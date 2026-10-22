import json
from pathlib import Path
import tempfile
import unittest

from benchmark import cells,value,outcomes,receipts,WORKLOAD,complete_native_load
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

    def test_pinned_native_boundary_retains_actual_51_and_251_offers(self):
        for planned,actual in [(50,51),(250,251),(250,250)]:
            with self.subTest(planned=planned,actual=actual):
                native=dict(metrics={name:dict(values=dict(count=actual)) for name in ['transaction_started','transaction_finished']})
                counts=outcomes(native,planned)
                self.assertEqual(counts['native_scheduled_offers'],actual)
                self.assertEqual(counts['planned_offers'],planned)
                self.assertEqual(counts['boundary_offer_delta'],actual-planned)
                self.assertTrue(complete_native_load(counts))

    def test_boundary_exception_does_not_hide_drops_missing_extra_or_unfinished_work(self):
        for name,started,finished,dropped,failed in [
            ('dropped',250,250,1,0),('missing',249,249,0,0),
            ('plus_two',252,252,0,0),('unresolved',251,250,0,0),
            ('failed',251,250,0,1),('rejected',251,250,0,1),('uncertain',251,250,0,1)]:
            with self.subTest(name=name):
                native=dict(metrics={key:dict(values=dict(count=count)) for key,count in {
                    'transaction_started':started,'transaction_finished':finished,
                    'dropped_iterations':dropped,'transaction_'+(name if name in ['failed','rejected','uncertain'] else 'failed'):failed}.items()})
                self.assertFalse(complete_native_load(outcomes(native,250)))

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
