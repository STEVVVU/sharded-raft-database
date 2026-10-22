import json
from pathlib import Path
import tempfile
import unittest

from benchmark import cells,value,outcomes,receipts,WORKLOAD
from client import shard


class BenchmarkTests(unittest.TestCase):
    def test_frozen_axes_fit_native_capacity(self):
        self.assertEqual(WORKLOAD['version'],2)
        self.assertEqual(len(cells()),24)
        self.assertEqual({c['reads_percent'] for c in cells()},{0,50,95,100})
        self.assertEqual({c['value_bytes'] for c in cells()},{64,1024})
        self.assertEqual(len(value(421,1024).encode()),1024)
        occupancy=[0]*4
        for prefix,count in [('read-',128),('write-',32)]:
            for index in range(count):occupancy[shard(prefix+str(index))]+=1
        self.assertEqual(occupancy,[39,41,41,39])
        self.assertLessEqual(max(occupancy),64)
        self.assertLessEqual(32+1+1,64) # Load, seed, final-readback identities.

    def test_native_outcome_accounting_retains_drops_and_unresolved_starts(self):
        metrics={name:dict(values=dict(count=count)) for name,count in {
            'transaction_started':8,'transaction_finished':4,'transaction_failed':1,
            'transaction_rejected':1,'transaction_uncertain':1,'dropped_iterations':2}.items()}
        result=outcomes(dict(metrics=metrics),12)
        self.assertEqual(result['unresolved_started'],1)
        self.assertEqual(result['not_started'],2)
        self.assertEqual(result['dropped_iterations'],2)
        self.assertEqual(result['planned_offers'],result['started']+result['dropped_iterations']+result['not_started'])

    def test_native_receipts_keep_pending_identity_and_latest_acknowledged_value(self):
        first=dict(version=2,client='seed-v2',seq=1,op='put',key='write-0',input='old',invoke=1)
        second=dict(version=2,client='bench-v2-0',seq=1,op='put',key='write-0',input='new',invoke=3)
        pending=dict(version=2,client='bench-v2-0',seq=2,op='get',key='write-0',input='',invoke=5)
        events=[dict(first,phase='invoke'),dict(first,phase='return',value='old',**{'return':2}),dict(second,phase='invoke'),dict(second,phase='return',value='new',**{'return':4}),dict(pending,phase='invoke')]
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'receipts.log'
            # k6 v2.3.0 newFileConsole omits the normal logger's source field.
            path.write_text('\n'.join(json.dumps(dict(msg=json.dumps(event))) for event in events))
            result=receipts(path)
            self.assertEqual(result['latest_writes'],{'write-0':'new'})
            self.assertEqual(result['returned_operations'],2)
            self.assertEqual(result['pending'][0]['seq'],2)
            events[-1]=dict(second,phase='return',value='wrong',**{'return':6})
            path.write_text('\n'.join(json.dumps(dict(source='console',msg=json.dumps(event))) for event in events))
            with self.assertRaises((KeyError,AssertionError)):receipts(path)
