"""Bounded retry client. Serialize operations for each client identity."""
import json, time, urllib.error, urllib.request

def shard(key):
    value=2166136261
    for byte in key.encode(): value=((value^byte)*16777619)&0xffffffff
    return value%4

class Client:
    def __init__(self, config, identity, record=lambda item: None):
        self.config=config; self.identity=identity; self.seq=0; self.record=record

    def call(self, node, path, body=None, timeout=3):
        req=urllib.request.Request('http://'+node['HTTP']+path,
            data=None if body is None else json.dumps(body).encode(),
            headers={'Authorization':'Bearer '+self.config['Token'],'Content-Type':'application/json'})
        with urllib.request.urlopen(req,timeout=timeout) as response: return json.load(response)

    def group(self, group, path, body, deadline=None):
        deadline=deadline or time.monotonic()+20
        last=None
        while time.monotonic()<deadline:
            for node in self.config['Nodes']:
                if node['Group']!=group: continue
                try:
                    response=self.call(node,path,body)
                    if response.get('error'): raise RuntimeError(response['error'])
                    return response
                except (OSError,ValueError,RuntimeError) as error: last=error
            time.sleep(.05)
        raise TimeoutError(str(last))

    def op(self, op, key, value='', identity=None, timeout=30):
        if identity is None: self.seq+=1; identity=self.seq
        start=time.monotonic_ns(); deadline=time.monotonic()+timeout
        command={'Op':op,'Key':key,'Value':value,'Client':self.identity,'Seq':identity,'Shard':shard(key)}
        while time.monotonic()<deadline:
            try:
                state=self.group('controller','/config',{},deadline)['state']
                owner=state['Owners'][command['Shard']]
                command['Epoch']=owner['Epoch']
                result=self.group(owner['Group'],'/op',command,min(deadline,time.monotonic()+4))
                self.record({'client':self.identity,'seq':identity,'op':op,'key':key,'input':value,'invoke':start,'return':time.monotonic_ns(),'value':result.get('value',''),'group':owner['Group'],'epoch':owner['Epoch']})
                return result.get('value','')
            except TimeoutError: pass
        self.record({'client':self.identity,'seq':identity,'op':op,'key':key,'input':value,'invoke':start,'return':None,'indeterminate':True})
        raise TimeoutError('uncertain outcome; retry the same identity and sequence')
