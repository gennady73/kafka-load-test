import copy
import importlib.util
import json
import pathlib
import unittest

HERE=pathlib.Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('plan',HERE/'plan.py'); mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
CONTRACT=json.loads((HERE.parents[1]/'config/preflight-profiles.json').read_text())
FIXTURE=json.loads((HERE/'examples/offline-fixture.json').read_text())

class PlannerTests(unittest.TestCase):
    def compile(self,s=None,c=None,offline=True): return mod.compile_plan(c or CONTRACT,s or FIXTURE,offline)
    def test_reproducible(self): self.assertEqual(self.compile(),self.compile())
    def test_budget_preserved_with_scaling(self):
        for workers in (1,2,3,7,61):
            s=copy.deepcopy(FIXTURE);s['workers']=workers
            for w in s['producer_windows']: w['max_value_bytes_per_second_per_worker']=10000
            p=self.compile(s)
            streams=[x for w in p for x in w['streams']]
            self.assertEqual(sum(t['budget_bytes'] for t in streams),120000)
            self.assertEqual(sum(t['record_count'] for t in streams),60)
            for t in streams:
                self.assertLessEqual(t['record_count']*t['minimum_payload_bytes'],t['budget_bytes'])
                self.assertGreaterEqual(t['record_count']*t['maximum_payload_bytes'],t['budget_bytes'])
    def test_consumer_change_does_not_change_producer(self):
        s=copy.deepcopy(FIXTURE);s['consumer_groups'][0]['group_id']='different'
        self.assertEqual([w['streams'] for w in self.compile()],[w['streams'] for w in self.compile(s)])
    def test_unachievable_count(self):
        s=copy.deepcopy(FIXTURE);s['topics'][0]['record_count']=121
        with self.assertRaises(ValueError):self.compile(s)
    def test_reject_missing_inputs(self):
        for key in ('topics','seed','workers','required_acks','consumer_groups'):
            s=copy.deepcopy(FIXTURE);s[key]=None
            with self.assertRaises((ValueError,TypeError)):self.compile(s)
    def test_fixture_never_executable(self):
        with self.assertRaises(ValueError):self.compile(offline=False)
    def test_daily_profile_and_environment_gates(self):
        for profile,budget in [('WP-A',2000000000000),('WP-B',20000000000000)]:
            s=copy.deepcopy(FIXTURE);s.update(fixture=False,profile_id=profile,duration_seconds=86400,target_value_payload_bytes=budget)
            t=s['topics'][0];t.update(budget_bytes=budget,minimum_payload_bytes=10000,maximum_payload_bytes=300000,record_count=budget//100000)
            s['consumer_groups'][0]['schedule'].update(active_seconds=[600,1200],idle_seconds=[60,300])
            s['producer_windows']=[dict(start_seconds=0,end_seconds=86400,weight=1,max_value_bytes_per_second_per_worker=1000000000)]
            self.assertEqual(sum(t['budget_bytes'] for w in self.compile(s) for t in w['streams']),budget)
            with self.assertRaises(ValueError):self.compile(s,offline=False)
    def test_bad_schedules_and_zero_ack(self):
        for change in ('ack','overlap','rate'):
            s=copy.deepcopy(FIXTURE)
            if change=='ack':s['required_acks']=0
            elif change=='overlap':s['producer_windows'][1]['start_seconds']=10
            else:s['producer_windows'][0]['max_value_bytes_per_second_per_worker']=1
            with self.assertRaises(ValueError):self.compile(s)
    def test_short_execution_plan_presence_gate_only(self):
        s=copy.deepcopy(FIXTURE);s.update(fixture=False,short_validation=True)
        c=copy.deepcopy(CONTRACT)
        for key in ('namespace','kafka_cluster_reference','security_profile_reference','component_inventory_reference','test_image_digest','observability_reference','resource_manifest_reference'):
            c['environment'][key]='STUB-REFERENCE-NOT-RUNTIME-EVIDENCE'
        c['environment']['bootstrap_endpoints']=['STUB-NO-NETWORK']
        s['runtime_evidence']={key:'STUB-REFERENCE-NOT-RUNTIME-EVIDENCE' for key in ('effective_topic_settings_reference','binary_ack_semantics_reference','worker_budget_reference','evidence_collection_reference','stop_policy_reference','security_adapter_reference','initial_group_offsets_reference')}
        plans=self.compile(s,c,offline=False)
        self.assertTrue(all(p['execution_ready'] for p in plans))
        self.assertTrue(all(p['qualification']=='short_validation' for p in plans))
    def test_random_vectors(self):
        rng=mod.Random(1)
        self.assertEqual([int(rng.next()*4294967296) for _ in range(3)],[270369,67634689,2647435461])

if __name__=='__main__': unittest.main()
