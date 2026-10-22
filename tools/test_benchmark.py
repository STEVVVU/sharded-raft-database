import unittest
from benchmark import cells,value
from benchmark_support import load


class BenchmarkTests(unittest.TestCase):
    def test_frozen_axes_and_values(self):
        self.assertEqual(len(cells()),24)
        self.assertEqual({c['reads_percent'] for c in cells()},{0,50,95,100})
        self.assertEqual(len(value(421,4096).encode()),4096)
        self.assertNotEqual(value(0,64),value(1,64))

    def test_offered_failure_accounting(self):
        def action(index):
            if index==1:raise TimeoutError('controlled uncertain write')
            return {'value':index}
        result=load(action,2,1)
        self.assertEqual(result['offered'],result['finished']+result['timed_out'])
