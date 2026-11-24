"""Offline-only continuity contract/CLI validation; never imports a Kafka client."""
import copy,importlib.util,json,pathlib,subprocess,sys,tempfile,unittest
HERE=pathlib.Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('planner_continuity_checks',HERE/'plan.py');mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
ROOT=HERE.parents[1];CONTRACT=json.loads((ROOT/'config/preflight-profiles.json').read_text())
SCENARIOS=ROOT/'kaas-demo/tests/continuity/scenarios'
class ContinuityOfflineChecks(unittest.TestCase):
    def scenario(self,stage='24h'):return json.loads((SCENARIOS/(stage+'.json')).read_text())
    def compile(self,s):return mod.compile_plan(CONTRACT,s,offline=True)[0]
    def test_all_stages_exact_disabled_and_reproducible(self):
        for stage,duration,count in [('15m',900,1040),('60m',3600,4168),('24h',86400,100000)]:
            s=self.scenario(stage);p=self.compile(s)
            self.assertEqual(p,self.compile(s));self.assertFalse(p['execution_ready']);self.assertFalse(p['execution_enabled'])
            self.assertEqual(p['duration_seconds'],duration);self.assertEqual(p['record_count'],count)
            self.assertEqual(sum(t['budget_bytes'] for t in p['streams']),count*1000)
            self.assertEqual(sum(t['record_count'] for t in p['streams']),count)
            self.assertEqual(len(p['consumer_sessions']),4)
            self.assertTrue(all(x['end_seconds']==duration+300 for x in p['consumer_sessions']))
            self.assertEqual(p['maximum_total_job_seconds'],duration+480)
    def test_wrong_volume_duration_policy_and_targets_rejected(self):
        for field,value in [('duration_seconds',3600),('target_value_payload_bytes',2000000000000),('record_value_bytes',1001),('record_count',True),('workers',2),('required_acks',1),('max_application_retries',1),('target_tolerance_fraction',0.01),('short_validation',True),('fixture',True)]:
            s=self.scenario();s[field]=value
            with self.subTest(field=field),self.assertRaises(ValueError):self.compile(s)
        for env_key,value in [('namespace','production'),('evidence_sink_url','http://collector.invalid'),('test_image','latest'),('binary_sha256','unknown')]:
            s=self.scenario();s['environment'][env_key]=value
            with self.subTest(field=env_key),self.assertRaises(ValueError):self.compile(s)
    def test_no_bypass_of_full_volume_mode(self):
        s=self.scenario();s['mode']='preflight';s['profile_id']='WP-A'
        with self.assertRaisesRegex(ValueError,'full profile'):mod.compile_plan(CONTRACT,s,offline=True)
        s['mode']='unknown'
        with self.assertRaisesRegex(ValueError,'unsupported mode'):mod.compile_plan(CONTRACT,s,offline=True)
    def test_nonoffline_disabled_and_missing_evidence_rejected(self):
        s=self.scenario()
        with self.assertRaisesRegex(ValueError,'execution disabled'):mod.compile_plan(CONTRACT,s,offline=False)
        s.update(execution_enabled=True,run_id='future-unique-run')
        with self.assertRaisesRegex(ValueError,'collector URL'):mod.compile_plan(CONTRACT,s,offline=False)
        self.assertFalse(self.compile(s)['execution_ready'])
    def test_cli_all_stages_does_not_overwrite(self):
        with tempfile.TemporaryDirectory() as tmp:
            for stage in ['15m','60m','24h']:
                cmd=[sys.executable,str(HERE/'plan.py'),'--contract',str(ROOT/'config/preflight-profiles.json'),'--scenario',str(SCENARIOS/(stage+'.json')),'--offline','--output',str(pathlib.Path(tmp)/stage)]
                a=subprocess.run(cmd,capture_output=True,text=True);self.assertEqual(a.returncode,0,a.stderr)
                self.assertFalse(json.loads(a.stdout)['execution_ready'])
                b=subprocess.run(cmd,capture_output=True,text=True);self.assertNotEqual(b.returncode,0)
if __name__=='__main__':unittest.main()
