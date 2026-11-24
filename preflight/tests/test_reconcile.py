import importlib.util
import pathlib
import sqlite3
import unittest

HERE=pathlib.Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('reconcile',HERE/'reconcile.py'); mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)

def plans():
    return [dict(run_id='r',worker_id=0,worker_count=1,target_value_payload_bytes=2000,
        target_tolerance_fraction=0,duration_seconds=60,drain_limit_seconds=10,
        streams=[dict(name='t',budget_bytes=2000)],consumer_sessions=[],
        consumer_groups=[dict(group_id='g',topics=['t'],require_full_run=True)])]

class LedgerTests(unittest.TestCase):
    def setUp(self): self.db=sqlite3.connect(':memory:');self.l=mod.Ledger(self.db,plans())
    def tearDown(self): self.db.close()
    def event(self,kind,**kw):
        self.l.add(dict(type=kind,run_id='r',worker_id=0,at_ms=1,**kw))
    def record(self,id='r/t/0/0',ack=True):
        self.event('attempt',id=id,topic='t',value_bytes=1000,checksum='abc')
        if ack:self.event('acknowledged',id=id,topic='t',value_bytes=1000,checksum='abc',boundary='generation')
    def receipt(self,id='r/t/0/0',checksum='abc'):
        self.event('receipt',id=id,group_id='g',topic='t',value_bytes=1000,checksum=checksum,valid=True)
    def test_missing_duplicate_corrupt_unexpected_distinct(self):
        self.record();self.record('r/t/0/1');self.receipt();self.receipt()
        r=self.l.report();self.assertEqual(r['groups']['g']['missing_acknowledged_ids'],1)
        self.assertEqual(r['groups']['g']['duplicate_deliveries'],1)
        self.assertEqual(r['invalid_receipts'],0)
        self.receipt('r/t/0/1','wrong');self.receipt('unexpected')
        r=self.l.report();self.assertEqual(r['invalid_receipts'],1);self.assertEqual(r['unexpected_receipts'],1)
        self.assertEqual(r['integrity'],'FAILED')
    def test_retry_not_double_counted_and_error_does_not_erase_ack(self):
        self.record();self.event('attempt',id='r/t/0/0',topic='t',value_bytes=1000,checksum='abc')
        self.event('ambiguous',id='r/t/0/0',topic='t',value_bytes=1000)
        r=self.l.report();self.assertEqual(r['attempt_count'],2)
        self.assertEqual(r['acknowledged_value_bytes_t1'],1000);self.assertEqual(r['unresolved_value_bytes'],0)
    def test_ambiguous_late_and_unsent_are_separate(self):
        self.record(ack=False);r=self.l.report();self.assertEqual(r['unresolved_value_bytes'],1000)
        self.assertEqual(r['not_attempted_value_bytes'],1000)
        self.event('acknowledged',id='r/t/0/0',topic='t',value_bytes=1000,checksum='abc',boundary='late')
        r=self.l.report();self.assertEqual(r['acknowledged_value_bytes_t1'],0)
        self.assertEqual(r['acknowledged_value_bytes_late_only'],1000);self.assertEqual(r['unresolved_value_bytes'],0)
        self.assertEqual(r['observed_target'],'NOT ACHIEVED')
    def test_incomplete_collection_cannot_pass(self):
        self.record();self.receipt();r=self.l.report()
        self.assertFalse(r['evidence_markers_complete']);self.assertEqual(r['integrity'],'INCONCLUSIVE')
    def test_conflicting_identity(self):
        self.record();self.event('attempt',id='r/t/0/0',topic='t',value_bytes=1001,checksum='abc')
        self.assertEqual(self.l.report()['malformed_or_conflicting_events'],1)
    def test_missing_worker_plans_rejected(self):
        p=plans();p[0]['worker_count']=2
        with self.assertRaises(ValueError):mod.Ledger(sqlite3.connect(':memory:'),p)

if __name__=='__main__': unittest.main()
