import unittest
from verify import check_history

class HistoryTests(unittest.TestCase):
    def operation(self, seq, op, value, start, end, input=''):
        return dict(client='c',seq=seq,op=op,key='k',input=input,invoke=start,**{'return':end},value=value)

    def test_rejects_impossible_read(self):
        history=[self.operation(1,'put','A',1,2,'A'),self.operation(2,'get','B',3,4)]
        with self.assertRaisesRegex(AssertionError,'nonlinearizable'):check_history(history)

    def test_overlap_can_linearize(self):
        history=[self.operation(1,'put','A',1,4,'A'),self.operation(2,'get','',2,3)]
        self.assertEqual(check_history(history),2)

    def test_retry_keeps_first_acknowledgement(self):
        history=[self.operation(1,'put','A',1,2,'A'),self.operation(2,'get','',3,4),self.operation(1,'put','A',5,6,'A')]
        with self.assertRaisesRegex(AssertionError,'nonlinearizable'):check_history(history)

    def test_uncertain_is_resolved_by_same_identity(self):
        history=[self.operation(1,'append','',1,None,'X'),self.operation(1,'append','X',4,5,'X')]
        self.assertEqual(check_history(history),1)

    def test_unresolved_is_not_discarded(self):
        with self.assertRaisesRegex(AssertionError,'unresolved'):check_history([self.operation(1,'append','',1,None,'X')])

if __name__=='__main__':unittest.main()
