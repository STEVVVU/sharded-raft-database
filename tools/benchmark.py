"""Real nine-process Raft benchmark with committed-return and readback oracles."""
import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import secrets
import socket
import sys
import threading
import time

from benchmark_support import Run, environment, load, quantiles, save
from client import Client, shard

ROOT=Path(__file__).resolve().parents[1]
WORKLOAD=dict(version=1,seed=421,keys=1024,value_bytes=[64,4096],reads_percent=[100,95,50,0],concurrency=[1,8,32],repeats=5,count=96,rates=[5,25,100],sweep_seconds=10,stability_seconds=120)


def value(index,size):
    prefix=f'{index:010d}|'
    return prefix+('abcdefghijklmnopqrstuvwxyz'*((size+25)//26))[:size-len(prefix)]


def cells():
    return [dict(value_bytes=size,reads_percent=mix,concurrency=concurrency) for size in WORKLOAD['value_bytes'] for mix in WORKLOAD['reads_percent'] for concurrency in WORKLOAD['concurrency']]


def trial(path,cell,count,rate=None,fault=None,smoke=False,profile=False):
    run=Run(path)
    result=dict(passed=False,cell=cell,rate=rate,fault=fault,history=[],phases=[])
    config=dict(Token=secrets.token_urlsafe(32),Nodes=[])
    reserved=[]
    processes={}
    observer_stop=threading.Event(); observer=None
    try:
        for group in ['controller','a','b']:
            for i in range(3):
                ports=[]
                for _ in range(2):
                    sock=socket.socket();sock.bind(('127.0.0.1',0));reserved.append(sock);ports.append(sock.getsockname()[1])
                identity=group+str(i)
                config['Nodes'].append(dict(ID=identity,Group=group,HTTP=f'127.0.0.1:{ports[0]}',Raft=f'127.0.0.1:{ports[1]}',Dir=str(run.path/'state'/identity)))
        configpath=run.path/'cluster.json';configpath.write_text(json.dumps(config))
        for sock in reserved:sock.close()
        env=dict(os.environ)
        if profile:env['GODEBUG']='schedtrace=1000,gctrace=1'
        def launch(node):
            process=run.launch(node['ID'],[str(ROOT/'sharded.exe'),'-config',str(configpath),'-node',node['ID']],env)
            processes[node['ID']]=process
            return process
        for node in config['Nodes']:launch(node)
        control=Client(config,'control')
        def leader(group):
            deadline=time.perf_counter()+20
            while time.perf_counter()<deadline:
                for node in config['Nodes']:
                    if node['Group']!=group:continue
                    try:
                        if control.call(node,'/status')['state']=='Leader':return node
                    except OSError:pass
                time.sleep(.05)
            raise TimeoutError('no leader '+group)
        for group in ['controller','a','b']:leader(group)
        result['before']={node['ID']:control.call(node,'/status') for node in config['Nodes']}
        keycount=4 if smoke else WORKLOAD['keys']
        def seed(index):
            client=Client(config,f'seed-{index}')
            expected=value(index,cell['value_bytes'])
            assert client.op('put',f'read-{index}',expected)==expected
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
            list(pool.map(seed,range(keycount)))
        assert control.op('get','read-0')==value(0,cell['value_bytes'])
        result['input_sha256']=hashlib.sha256(json.dumps([value(i,cell['value_bytes']) for i in range(keycount)]).encode()).hexdigest()
        trigger=threading.Lock(); injected=False; victim=None
        def observe():
            while not observer_stop.is_set():
                try:
                    state=control.group('controller','/config',{})['state']
                    result['phases'].append(dict(at=time.perf_counter(),move=state['Move'],owners=state['Owners']))
                except Exception as error:result['phases'].append(dict(at=time.perf_counter(),error=repr(error)))
                observer_stop.wait(.02)
        if fault=='migration':
            observer=threading.Thread(target=observe,daemon=True);observer.start()
        def action(index):
            nonlocal injected,victim
            if fault and index>=count//3:
                with trigger:
                    if not injected:
                        injected=True;result['fault_at']=time.perf_counter()
                        if fault=='migration':
                            state=control.group('controller','/config',{})['state']
                            index_shard=shard('read-0');owner=state['Owners'][index_shard]
                            move=dict(Shard=index_shard,From=owner['Group'],To='b' if owner['Group']=='a' else 'a',Epoch=owner['Epoch']+1,Phase=0)
                            control.group('controller','/move',dict(Shard=index_shard,Move=move))
                            result['move']=move
                        else:
                            victim=leader('a');run.stop(processes[victim['ID']]);result['killed']=victim['ID']
            client=Client(config,f'bench-{index}',result['history'].append)
            isread=((index*37+WORKLOAD['seed'])%100)<cell['reads_percent']
            key=f'read-{index%keycount}' if isread else f'write-{index}'
            expected=value(index%keycount if isread else index,cell['value_bytes'])
            actual=client.op('get' if isread else 'put',key,'' if isread else expected,timeout=30)
            assert actual==expected,(key,actual,expected)
            return dict(op='get' if isread else 'put',key=key,sha256=hashlib.sha256(actual.encode()).hexdigest(),expected_index=index%keycount if isread else index)
        result['load']=load(action,count,cell['concurrency'],rate)
        if fault=='migration':
            deadline=time.perf_counter()+30
            while control.group('controller','/config',{})['state']['Move'] is not None:
                if time.perf_counter()>=deadline:raise TimeoutError('migration did not settle')
                time.sleep(.02)
            result['migration_settled_at']=time.perf_counter()
        if victim:
            result['restart_at']=time.perf_counter();launch(victim);leader('a')
            deadline=time.perf_counter()+10
            while True:
                try:
                    control.call(victim,'/status');break
                except OSError:
                    if time.perf_counter()>=deadline:raise
                    time.sleep(.02)
        writes=[s for s in result['load']['samples'] if s['outcome']=='finished' and s['proof']['op']=='put']
        def readback(sample):
            proof=sample['proof'];client=Client(config,f'verify-{sample["index"]}')
            actual=client.op('get',proof['key'])
            assert hashlib.sha256(actual.encode()).hexdigest()==proof['sha256'],'acknowledged write readback mismatch'
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:list(pool.map(readback,writes))
        result['verified_writes']=len(writes)
        result['after']={node['ID']:control.call(node,'/status') for node in config['Nodes'] if processes[node['ID']].poll() is None}
        for kind in ['get','put']:
            result[kind+'_latency_ms']=quantiles([s['latency_ms'] for s in result['load']['samples'] if s['outcome']=='finished' and s['proof']['op']==kind])
        result['passed']=result['load']['finished']==count
        if profile:result['profile_scope']='Generator cProfile and Go scheduler/GC only. No native CPU hot-path profile yet; required before product optimization.'
    except Exception as error:result['error']=repr(error)
    finally:
        for sock in reserved:sock.close()
        observer_stop.set()
        if observer:observer.join(timeout=22)
        result['observer_stopped']=observer is None or not observer.is_alive()
        result['cleanup']=run.cleanup()
        result['passed']=result['passed'] and result['observer_stopped'] and result['cleanup']['all_exited'] and not result['cleanup']['errors']
        save(run.path/'result.json',result)
    return result


def main():
    parser=argparse.ArgumentParser();parser.add_argument('mode',choices=['smoke','benchmark','profile'],nargs='?',default='smoke');args=parser.parse_args()
    assert args.mode=='smoke' or sys.platform=='linux','isolated Linux required'
    directory=ROOT/'evidence'/('benchmark-'+args.mode+'-'+str(time.time_ns()));directory.mkdir(parents=True)
    summary=dict(passed=False,mode=args.mode,environment=environment(ROOT),workload=WORKLOAD,trials=[])
    def run(cell,count=WORKLOAD['count'],rate=None,fault=None):
        result=trial(directory/('trial-'+str(len(summary['trials']))),cell,count,rate,fault,args.mode=='smoke',args.mode=='profile')
        summary['trials'].append(result);save(directory/'summary.json',summary)
        print(json.dumps(dict(passed=result['passed'],evidence=str(directory))),flush=True)
        if result.get('error') or not result['cleanup']['all_exited']:raise RuntimeError(result.get('error','cleanup failure'))
        return result
    try:
        if args.mode=='smoke':run(dict(value_bytes=64,reads_percent=50,concurrency=2),4)
        elif args.mode=='profile':
            import cProfile
            profiler=cProfile.Profile();profiler.enable();run(dict(value_bytes=4096,reads_percent=50,concurrency=8));profiler.disable();profiler.dump_stats(str(directory/'generator.pstats'))
        else:
            matrix=cells()
            for repeat in range(WORKLOAD['repeats']):
                for cell in matrix if repeat%2==0 else reversed(matrix):run(dict(cell,repeat=repeat))
            base=dict(value_bytes=64,reads_percent=50,concurrency=32)
            sweeps=[run(base,rate*WORKLOAD['sweep_seconds'],rate) for rate in WORKLOAD['rates']]
            stable=[v for v in sweeps if v['passed'] and v['load']['dispatch_lag_ms']['p99']<100 and v['load']['latency_ms']['p99']<5000]
            if stable:
                rate=stable[-1]['rate'];run(base,rate*WORKLOAD['stability_seconds'],rate)
            else:summary['stability_blocked']='No zero-loss qualifying rate; retain full sweep and expand lower rates.'
            for fault in ['migration','leader-loss']:run(base,96,fault=fault)
        summary['passed']=all(v['passed'] for v in summary['trials']) and not summary.get('stability_blocked')
    except Exception as error:summary['error']=repr(error)
    finally:save(directory/'summary.json',summary)
    print(directory/'summary.json');return 0 if summary['passed'] else 1


if __name__=='__main__':raise SystemExit(main())
