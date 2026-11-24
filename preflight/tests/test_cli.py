import json
import pathlib
import subprocess
import sys
import tempfile
import unittest

HERE=pathlib.Path(__file__).resolve().parents[1]
CONTRACT=HERE.parents[1]/'config/preflight-profiles.json'
class CliTests(unittest.TestCase):
    def test_repeatability_reject_overwrite_and_reconcile_incomplete_logs(self):
        with tempfile.TemporaryDirectory(prefix='c2-cli-') as tmp:
            base=pathlib.Path(tmp)
            cmd=[sys.executable,str(HERE/'plan.py'),'--contract',str(CONTRACT),'--scenario',str(HERE/'examples/offline-fixture.json'),'--offline','--output']
            for name in ('a','b'):
                r=subprocess.run(cmd+[str(base/name)],capture_output=True,text=True)
                self.assertEqual(r.returncode,0,r.stderr)
            self.assertEqual((base/'a/manifest.json').read_bytes(),(base/'b/manifest.json').read_bytes())
            r=subprocess.run(cmd+[str(base/'a')],capture_output=True,text=True)
            self.assertNotEqual(r.returncode,0);self.assertIn('output already exists',r.stderr)
            events=base/'events.raw';events.write_text('PF_EVENT {broken\n')
            r=subprocess.run([sys.executable,str(HERE/'reconcile.py'),'--plans',str(base/'a'),'--events',str(events),'--database',str(base/'db.sqlite')],capture_output=True,text=True)
            self.assertEqual(r.returncode,0,r.stderr)
            report=json.loads(r.stdout)
            self.assertEqual(report['malformed_or_conflicting_events'],1)
            self.assertEqual(report['accounting'],'INCOMPLETE')
            self.assertEqual(report['integrity'],'INCONCLUSIVE')
    def test_empty_contract_template_rejected(self):
        with tempfile.TemporaryDirectory(prefix='c2-reject-') as tmp:
            r=subprocess.run([sys.executable,str(HERE/'plan.py'),'--contract',str(CONTRACT),'--scenario',str(HERE.parents[1]/'config/preflight-c2-scenario.template.json'),'--offline','--output',str(pathlib.Path(tmp)/'plan')],capture_output=True,text=True)
            self.assertNotEqual(r.returncode,0)
            self.assertFalse((pathlib.Path(tmp)/'plan').exists())

if __name__=='__main__':unittest.main()
