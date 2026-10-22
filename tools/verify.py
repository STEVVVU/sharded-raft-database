"""Real-process verifier. All process handles and evidence belong to this run."""
import argparse, concurrent.futures, json, os, pathlib, socket, subprocess, time, urllib.request
from client import Client, shard

ROOT = pathlib.Path(__file__).resolve().parents[1]

def request(port, path, body=None, token=None, timeout=4):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(f'http://127.0.0.1:{port}{path}', data=data,
                                 headers={'Authorization': 'Bearer '+(token or ''), 'Content-Type':'application/json'})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.load(response)

def freeport():
    with socket.socket() as s:
        s.bind(('127.0.0.1',0))
        return s.getsockname()[1]

def baseline():
    run = ROOT/'evidence'/('baseline-'+str(time.time_ns()))
    run.mkdir(parents=True)
    port, raft = freeport(), freeport()
    log = (run/'node.log').open('wb')
    p = subprocess.Popen([str(ROOT/'baseline.exe'), '-haddr', f'127.0.0.1:{port}', '-raddr',f'127.0.0.1:{raft}',str(run/'state')],stdout=log,stderr=log)
    try:
        deadline=time.monotonic()+20
        while True:
            try:
                req=urllib.request.Request(f'http://127.0.0.1:{port}/key',data=b'{"baseline":"verified"}',method='POST')
                urllib.request.urlopen(req,timeout=2).read()
                with urllib.request.urlopen(f'http://127.0.0.1:{port}/key/baseline') as r:
                    value=json.load(r)
                assert value=={'baseline':'verified'},value
                (run/'result.json').write_text(json.dumps({'passed':True,'value':value,'pid':p.pid}))
                break
            except Exception:
                if time.monotonic()>deadline: raise
                time.sleep(.1)
    finally:
        p.terminate(); p.wait(timeout=10); log.close()
    assert (run/'result.json').exists()
    print(run)

def check_history(history):
    """Exhaustive per-key register checker with real-time precedence."""
    collapsed={}
    for item in history:
        k=(item['client'],item['seq'])
        if k in collapsed:
            old=collapsed[k]
            assert (old['key'],old['op'],old['input'])==(item['key'],item['op'],item['input'])
            if old.get('return') is not None and item.get('return') is not None: assert old['value']==item['value']
            returns=[x['return'] for x in (old,item) if x.get('return') is not None]
            item=dict(item,invoke=min(old['invoke'],item['invoke']),**({'return':min(returns)} if returns else {}))
        collapsed[k]=item
    assert all(x.get('return') is not None for x in collapsed.values()),'unresolved operations require an expanded optional-completion search'
    for key in {x['key'] for x in collapsed.values()}:
        operations=[x for x in collapsed.values() if x['key']==key]
        predecessors=[{j for j,y in enumerate(operations) if y['return']<x['invoke']} for x in operations]
        failed=set()
        def search(done, value):
            if len(done)==len(operations): return True
            signature=(frozenset(done),value)
            if signature in failed: return False
            for i,x in enumerate(operations):
                if i in done or not predecessors[i]<=done: continue
                after=value if x['op']=='get' else x['input'] if x['op']=='put' else value+x['input']
                if after==x['value'] and search(done|{i},after): return True
            failed.add(signature); return False
        assert search(set(),''),'nonlinearizable history for '+key
    return len(collapsed)

def journey():
    run=ROOT/'evidence'/('journey-'+str(time.time_ns()));run.mkdir(parents=True)
    config={'Token':os.urandom(24).hex(),'Nodes':[]}
    reserved=[]
    for group in ['controller','a','b']:
        for i in range(3):
            ports=[]
            for _ in range(2):
                s=socket.socket();s.bind(('127.0.0.1',0));reserved.append(s);ports.append(s.getsockname()[1])
            identity=group+str(i)
            config['Nodes'].append({'ID':identity,'Group':group,'HTTP':'127.0.0.1:'+str(ports[0]),'Raft':'127.0.0.1:'+str(ports[1]),'Dir':str(run/'state'/identity)})
    configpath=run/'cluster.json';configpath.write_text(json.dumps(config))
    for s in reserved:s.close()
    processes={};handles=[];history=[];events=[]
    client=Client(config,'oracle',history.append)
    def event(name,**data):
        events.append({'event':name,'at':time.monotonic_ns(),**data})
        (run/'events.json').write_text(json.dumps(events,indent=2))
    def start(node):
        log=(run/(node['ID']+'-'+str(time.time_ns())+'.log')).open('wb');handles.append(log)
        p=subprocess.Popen([str(ROOT/'sharded.exe'),'-config',str(configpath),'-node',node['ID']],stdout=log,stderr=log)
        processes[node['ID']]=p;event('start',node=node['ID'],pid=p.pid)
    def kill(node):
        p=processes[node['ID']];p.kill();p.wait(timeout=10);event('kill',node=node['ID'],pid=p.pid)
    def leader(group):
        deadline=time.monotonic()+20
        while time.monotonic()<deadline:
            for node in config['Nodes']:
                if node['Group']!=group:continue
                try:
                    if client.call(node,'/status')['state']=='Leader':return node
                except OSError:pass
            time.sleep(.05)
        raise AssertionError('no leader '+group)
    def move(key,to):
        state=client.group('controller','/config',{})['state'];index=shard(key);owner=state['Owners'][index]
        migration={'Shard':index,'From':owner['Group'],'To':to,'Epoch':owner['Epoch']+1,'Phase':0}
        client.group('controller','/move',{'Shard':index,'Move':migration})
        return migration
    def settled():
        deadline=time.monotonic()+30
        while time.monotonic()<deadline:
            if client.group('controller','/config',{})['state']['Move'] is None:return
            time.sleep(.05)
        raise AssertionError('migration did not finish')
    try:
        for node in config['Nodes']:start(node)
        for group in ['controller','a','b']:leader(group)
        client.group('controller','/config',{})
        assert all(p.poll() is None for p in processes.values())
        event('doctor',nodes=9)
        keys={}
        for i in range(100):keys.setdefault(shard('key'+str(i)),'key'+str(i))
        for key in keys.values():assert client.op('put',key,'seed')=='seed'
        assert {x['group'] for x in history if x['op']=='put'}=={'a','b'}
        event('both_data_groups_executed')
        moving=keys[0];dedup=Client(config,'duplicate',history.append)
        assert dedup.op('append',moving,'D',identity=1)=='seedD'
        migration=move(moving,'b');event('migration_intent',migration=migration)
        competing=dict(migration,Shard=2)
        response=client.call(leader('controller'),'/move',{'Shard':2,'Move':competing})
        assert response.get('error')=='migration busy';event('competing_controller_rejected',response=response)
        writer=Client(config,'writer',history.append)
        writer2=Client(config,'writer2',history.append)
        reader=Client(config,'reader',history.append)
        def writes():
            for _ in range(12):writer.op('append',moving,'W');time.sleep(.04)
        def writes2():
            for _ in range(6):writer2.op('append',moving,'Z');time.sleep(.07)
        def reads():
            for _ in range(6):reader.op('get',moving);time.sleep(.06)
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            task=pool.submit(writes)
            task2=pool.submit(writes2);task3=pool.submit(reads)
            deadline=time.monotonic()+15
            while time.monotonic()<deadline:
                current=client.group('controller','/config',{})['state']['Move']
                if current and 1<=current['Phase']<=4:
                    victim=leader('controller');kill(victim);event('controller_intermediate_crash',phase=current['Phase']);break
                time.sleep(.02)
            else:raise AssertionError('did not observe durable intermediate phase')
            settled();task.result();task2.result();task3.result()
        start(victim)
        assert dedup.op('append',moving,'D',identity=1)=='seedD'
        final_value=client.op('get',moving)
        assert final_value.startswith('seedD') and final_value[5:].count('W')==12 and final_value[5:].count('Z')==6 and len(final_value)==23
        move(moving,'a');settled()
        stale=client.call(leader('a'),'/internal',{'Op':'cleanup','Shard':0,'Epoch':migration['Epoch']})
        assert stale.get('error');event('stale_cleanup_rejected',response=stale)
        staleop=client.call(leader('a'),'/op',{'Op':'put','Shard':0,'Epoch':1,'Key':moving,'Value':'BAD','Client':'stale','Seq':1})
        assert staleop.get('error');event('stale_config_rejected',response=staleop)
        oldleader=leader('a');kill(oldleader)
        assert client.op('get',moving)==final_value
        start(oldleader)
        for group in ['controller','a','b']:
            client.group(group,'/snapshot',{})
        event('snapshots_committed')
        for node in config['Nodes']:kill(node)
        for node in config['Nodes']:start(node)
        assert client.op('get',moving)==final_value
        assert dedup.op('append',moving,'D',identity=1)=='seedD'
        restored={node['Group'] for node in config['Nodes'] for path in run.glob(node['ID']+'-*.log') if 'restored from snapshot' in path.read_text(errors='replace')}
        assert restored=={'controller','a','b'},restored
        event('snapshot_restore_observed',groups=sorted(restored))
        bnodes=[n for n in config['Nodes'] if n['Group']=='b'];bleader=leader('b');followers=[n for n in bnodes if n!=bleader]
        for node in followers:kill(node)
        uncertain=Client(config,'uncertain',history.append);key=keys[1]
        try:uncertain.op('append',key,'Q',identity=1,timeout=5);raise AssertionError('acknowledged without quorum')
        except TimeoutError:event('quorum_unavailable_uncertain')
        for node in followers:start(node)
        assert uncertain.op('append',key,'Q',identity=1)=='seedQ'
        assert uncertain.op('append',key,'Q',identity=1)=='seedQ'
        for key in keys.values():client.op('get',key)
        count=check_history(history);event('bounded_linearizability_pass',logical_operations=count)
        (run/'result.json').write_text(json.dumps({'passed':True,'logical_operations':count,'nodes':9,'scope':'per-key bounded completed history with retries collapsed'},indent=2))
    finally:
        (run/'history.json').write_text(json.dumps(history,indent=2))
        for p in processes.values():
            if p.poll() is None:p.terminate()
        for p in processes.values():p.wait(timeout=10)
        for h in handles:h.close()
        event('cleanup',all_owned_processes_stopped=all(p.poll() is not None for p in processes.values()))
    assert (run/'result.json').exists();print(run)

if __name__=='__main__':
    parser=argparse.ArgumentParser(); parser.add_argument('--baseline',action='store_true')
    args=parser.parse_args()
    if args.baseline: baseline()
    else: journey()
