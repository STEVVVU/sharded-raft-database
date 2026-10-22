"""Own real Raft nodes and fault oracles; k6 owns all request load and statistics."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import socket
import subprocess
import sys
import threading
import time

from benchmark_support import Run, environment, save
from client import Client, shard

ROOT=Path(__file__).resolve().parents[1]
WORKLOAD=dict(version=2,seed=421,read_keys=128,write_keys=32,value_bytes=[64,1024],reads_percent=[100,95,50,0],concurrency=[1,8,32],repeats=5,matrix_rate=25,rates=[5,25,100],sweep_seconds=10,stability_seconds=120,k6_version='2.3.0')


def value(index,size):
    prefix=f'{index:010d}|'
    return prefix+('abcdefghijklmnopqrstuvwxyz'*((size+25)//26))[:size-len(prefix)]


def cells():
    return [dict(value_bytes=size,reads_percent=mix,concurrency=concurrency) for size in WORKLOAD['value_bytes'] for mix in WORKLOAD['reads_percent'] for concurrency in WORKLOAD['concurrency']]


def metric(summary,name,field='count',default=0):
    return summary.get('metrics',{}).get(name,{}).get('values',{}).get(field,default)


def outcomes(summary,planned):
    started=metric(summary,'transaction_started')
    finished=metric(summary,'transaction_finished')
    errors={name:metric(summary,'transaction_'+name) for name in ['failed','rejected','uncertain']}
    dropped=metric(summary,'dropped_iterations')
    unresolved=started-finished-sum(errors.values())
    return dict(planned_offers=planned,native_scheduled_offers=started+dropped,boundary_offer_delta=started+dropped-planned,started=started,finished=finished,**errors,dropped_iterations=dropped,unresolved_started=unresolved,not_started=max(0,planned-started-dropped),extra_started=max(0,started+dropped-planned))


def complete_native_load(counts):
    # Pinned k6 v2.3.0 can select its final timer before duration cancellation.
    return counts['native_scheduled_offers'] in {counts['planned_offers'],counts['planned_offers']+1} and counts['started']>0 and counts['finished']==counts['started'] and not any(counts[name] for name in ['failed','rejected','uncertain','dropped_iterations','unresolved_started','not_started'])


def receipts(path):
    """Read k6 JSON-formatted console events, preserving incomplete native operations."""
    pending={};latest={};returns=0
    for line in path.read_text().splitlines():
        envelope=json.loads(line)
        if envelope.get('source','console')!='console':continue
        event=json.loads(envelope['msg'])
        if event.get('phase') not in ['invoke','return']:continue
        identity=(event['client'],event['seq'])
        if event['phase']=='invoke':
            assert identity not in pending,'duplicate logical invocation'
            pending[identity]=event
        else:
            previous=pending.pop(identity)
            assert all(previous[k]==event[k] for k in ['client','seq','op','key','input','invoke']),'receipt identity changed'
            if event['op']=='put':
                assert event['value']==event['input'],'committed write echo mismatch'
                latest[event['key']]=event['value']
            returns+=1
    return dict(pending=list(pending.values()),latest_writes=latest,returned_operations=returns)


def trial(path,cell,rate=25,seconds=10,fault=None,profile=False,k6='k6'):
    run=Run(path);result=dict(passed=False,cell=cell,rate=rate,seconds=seconds,fault=fault,phases=[])
    config=dict(Token=secrets.token_urlsafe(32),Nodes=[])
    reserved=[];processes={};observer_stop=threading.Event();observer=None;victim=None
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
        node_env=dict(os.environ)
        if profile:node_env['GODEBUG']='schedtrace=1000,gctrace=1'
        def launch(node):
            processes[node['ID']]=run.launch(node['ID'],[str(ROOT/'sharded.exe'),'-config',str(configpath),'-node',node['ID']],node_env)
        for node in config['Nodes']:launch(node)
        control=Client(config,'control-v2')
        def leader(group):
            deadline=time.monotonic()+20
            while time.monotonic()<deadline:
                for node in config['Nodes']:
                    if node['Group']!=group or processes[node['ID']].poll() is not None:continue
                    try:
                        if control.call(node,'/status')['state']=='Leader':return node
                    except OSError:pass
                time.sleep(.05)
            raise TimeoutError('no leader '+group)
        for group in ['controller','a','b']:leader(group)
        result['before']={node['ID']:control.call(node,'/status') for node in config['Nodes']}
        fixtures=[(f'read-{i}',value(i,cell['value_bytes'])) for i in range(128)]+[(f'write-{i}',value(i,cell['value_bytes'])) for i in range(32)]
        result['input_sha256']=hashlib.sha256(json.dumps(fixtures,separators=(',',':')).encode()).hexdigest()
        samples=run.path/'k6-samples.json';console=run.path/'receipts.log';native=run.path/'k6-summary.json'
        k6_env=dict(os.environ,CLUSTER_CONFIG=str(configpath),K6_SUMMARY=str(native),VALUE_BYTES=str(cell['value_bytes']),READS_PERCENT=str(cell['reads_percent']),LOAD_VUS=str(cell['concurrency']),RATE=str(rate),SECONDS=str(seconds),K6_NO_USAGE_REPORT='true')
        command=[k6,'run','--log-format','json','--out','json='+str(samples),'--console-output',str(console),str(ROOT/'tools/benchmark-k6.js')]
        result['command']=command
        process=run.launch('k6',command,k6_env)
        if fault:
            def observe():
                nonlocal victim
                seen=0;injected=False;fragment=''
                try:
                    while not observer_stop.is_set():
                        if samples.exists():
                            with samples.open() as source:
                                source.seek(seen);chunk=source.read();seen=source.tell()
                            fragment+=chunk
                            while '\n' in fragment:
                                line,fragment=fragment.split('\n',1);event=json.loads(line)
                                if event.get('type')=='Point' and event.get('metric')=='transaction_started':
                                    result['observed_starts']=result.get('observed_starts',0)+int(event['data']['value'])
                        if not injected and result.get('observed_starts',0)>=rate*seconds//3:
                            injected=True;result['fault_at_unix_ms']=time.time_ns()//1_000_000
                            if fault=='migration':
                                state=control.group('controller','/config',{})['state']
                                part=shard('read-0');owner=state['Owners'][part]
                                move=dict(Shard=part,From=owner['Group'],To='b' if owner['Group']=='a' else 'a',Epoch=owner['Epoch']+1,Phase=0)
                                control.group('controller','/move',dict(Shard=part,Move=move));result['move']=move
                            else:
                                victim=leader('a');run.stop(processes[victim['ID']]);result['killed']=victim['ID']
                        if injected and fault=='migration':
                            state=control.group('controller','/config',{})['state']
                            result['phases'].append(dict(at_unix_ms=time.time_ns()//1_000_000,move=state['Move'],owners=state['Owners']))
                        if process.poll() is not None:break
                        observer_stop.wait(.02)
                except Exception as error:result['observer_error']=repr(error)
            observer=threading.Thread(target=observe,daemon=True);observer.start()
        result['k6_exit_code']=process.wait(timeout=seconds+400)
        observer_stop.set()
        if observer:observer.join(timeout=35)
        assert observer is None or not observer.is_alive(),'fault observer did not stop'
        assert not result.get('observer_error'),result.get('observer_error')
        data=json.loads(native.read_text())
        result['outcomes']=outcomes(data,rate*seconds)
        result['native_offer_scope']='Requested rate-duration target and actual native scheduled offers are separate; only the pinned k6 v2.3.0 zero-or-one final-boundary difference is permitted. Every native start must finish; drops/deficits/errors remain failures.'
        result['native_summary']='k6-summary.json'
        ledger=receipts(console);result['receipt_operations_returned']=ledger['returned_operations'];result['pending_operations']=ledger['pending']
        if fault:
            assert 'fault_at_unix_ms' in result,'fault never injected under measured traffic'
        if fault=='migration':
            deadline=time.monotonic()+30
            while True:
                state=control.group('controller','/config',{})['state']
                if state['Move'] is None:break
                if time.monotonic()>=deadline:raise TimeoutError('migration did not settle')
                time.sleep(.02)
            move=result['move'];owner=state['Owners'][move['Shard']]
            assert owner['Group']==move['To'] and owner['Epoch']==move['Epoch'],'migration owner/epoch oracle'
            result['migration_settled_unix_ms']=time.time_ns()//1_000_000
        if victim:
            result['restart_at_unix_ms']=time.time_ns()//1_000_000;launch(victim);leader('a')
            deadline=time.monotonic()+10
            while True:
                try:control.call(victim,'/status');break
                except OSError:
                    if time.monotonic()>=deadline:raise
                    time.sleep(.02)
        if not ledger['pending']:
            verifier=Client(config,'verify-v2')
            for key,expected in ledger['latest_writes'].items():
                assert verifier.op('get',key)==expected,'latest acknowledged write readback mismatch'
            for i in range(128):
                assert ledger['latest_writes'].get(f'read-{i}')==value(i,cell['value_bytes']),'immutable read fixture changed'
            result['verified_acknowledged_keys']=len(ledger['latest_writes']);result['verified_read_keys']=128
            result['verified_write_keys']=sum(key.startswith('write-') for key in ledger['latest_writes'])
        else:result['final_readback_blocked']='Unresolved native operation may still commit; retain its identity before any readback claim.'
        result['after']={node['ID']:control.call(node,'/status') for node in config['Nodes']}
        counts=result['outcomes']
        result['correctness_passed']=not ledger['pending'] and counts['started']>0 and counts['unresolved_started']==0 and not any(counts[name] for name in ['failed','rejected','uncertain']) and counts['finished']==counts['started'] and metric(data,'checks','rate',None)==1
        result['passed']=result['k6_exit_code'] in [0,99] and result['correctness_passed'] and complete_native_load(counts)
        tails=[metric(data,'group_duration{group:::'+kind+'}','p(99)',None) for kind in (['get'] if cell['reads_percent']==100 else ['put_get'] if cell['reads_percent']==0 else ['get','put_get'])]
        result['load_slo_passed']=result['passed'] and all(tail is not None and tail<5000 for tail in tails)
        if profile:result['profile_scope']='Existing Go scheduler/GC traces only; native hot-path CPU profile remains unavailable. k6 owns load/statistics.'
        if result['k6_exit_code'] not in [0,99]:result['error']='k6 failed; inspect native summary, safe receipts and k6.log'
    except Exception as error:result['error']=repr(error)
    finally:
        observer_stop.set()
        if observer:observer.join(timeout=35)
        for sock in reserved:sock.close()
        result['observer_stopped']=observer is None or not observer.is_alive()
        result['cleanup']=run.cleanup()
        result['passed']=result['passed'] and result['observer_stopped'] and result['cleanup']['all_exited'] and not result['cleanup']['errors']
        result['load_slo_passed']=result.get('load_slo_passed',False) and result['passed']
        save(run.path/'result.json',result)
    return result


def main():
    parser=argparse.ArgumentParser();parser.add_argument('mode',choices=['smoke','benchmark','profile'],nargs='?',default='smoke');parser.add_argument('--k6',default='k6');args=parser.parse_args()
    assert args.mode=='smoke' or sys.platform=='linux','isolated Linux required'
    directory=ROOT/'evidence'/('benchmark-v2-'+args.mode+'-'+str(time.time_ns()));directory.mkdir(parents=True)
    summary=dict(passed=False,mode=args.mode,environment=environment(ROOT),workload=WORKLOAD,trials=[])
    def run(cell,rate=25,seconds=10,fault=None):
        result=trial(directory/('trial-'+str(len(summary['trials']))),cell,rate,seconds,fault,args.mode=='profile',args.k6)
        summary['trials'].append(result);save(directory/'summary.json',summary)
        print(json.dumps(dict(passed=result['passed'],evidence=str(directory))),flush=True)
        if result.get('error') or not result['cleanup']['all_exited']:raise RuntimeError(result.get('error','cleanup failure'))
        return result
    try:
        summary['environment']['k6']=subprocess.check_output([args.k6,'version'],text=True).strip()
        assert 'v2.3.0' in summary['environment']['k6'],'require pinned k6 v2.3.0'
        summary['environment']['k6_binary_sha256']=hashlib.sha256(Path(shutil.which(args.k6) or args.k6).read_bytes()).hexdigest()
        if args.mode=='smoke':run(dict(value_bytes=64,reads_percent=50,concurrency=8),5)
        elif args.mode=='profile':run(dict(value_bytes=1024,reads_percent=50,concurrency=32),100)
        else:
            matrix=cells()
            for repeat in range(WORKLOAD['repeats']):
                for cell in matrix if repeat%2==0 else reversed(matrix):run(dict(cell,repeat=repeat))
            base=dict(value_bytes=64,reads_percent=95,concurrency=32)
            sweeps=[run(base,rate) for rate in WORKLOAD['rates']]
            stable=[v for v in sweeps if v['load_slo_passed']]
            if stable:summary['stability_load_slo_passed']=run(base,stable[-1]['rate'],WORKLOAD['stability_seconds'])['load_slo_passed']
            else:summary['stability_blocked']='No zero-loss qualifying rate; preserve the full native sweep.'
            for fault in ['migration','leader-loss']:run(base,fault=fault)
        summary['passed']=all(v['passed'] for v in summary['trials']) and (args.mode!='benchmark' or summary.get('stability_load_slo_passed',False))
    except Exception as error:summary['error']=repr(error)
    finally:save(directory/'summary.json',summary)
    print(directory/'summary.json');return 0 if summary['passed'] else 1


if __name__=='__main__':raise SystemExit(main())
