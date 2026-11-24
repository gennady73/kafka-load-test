#!/usr/bin/env python3
"""Offline cross-language audit/mutation checks for Node's fake collector export."""
import copy,hashlib,importlib.util,json,pathlib,sqlite3,sys
HERE=pathlib.Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('continuity_ledger_checks',HERE/'continuity_reconcile.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
f=json.loads(pathlib.Path(sys.argv[1]).read_text());plan=f['plan'];plan['_file_sha256']='stub-plan'
def audit(events,seal):
    db=sqlite3.connect(':memory:');ledger=m.Ledger(plan,db)
    for env in events:
        try:ledger.add(env)
        except (ValueError,KeyError,TypeError,sqlite3.IntegrityError) as error:ledger.issues.append(type(error).__name__)
    r=ledger.report(seal);db.close();return r
positive=audit(f['events'],f['seal']);assert positive['status']=='ACCOUNTING_COMPLETE',positive
assert positive['records_in_ledger']==1040
without=[x for x in f['events'] if json.loads(x['event_json'])['type']!='receipt'];assert audit(without,f['seal'])['status']=='FAIL'
assert audit(f['events'],None)['status']=='INCONCLUSIVE'
corrupt=copy.deepcopy(f['events'])
for item in corrupt:
    e=json.loads(item['event_json'])
    if e['type']=='receipt':
        e['checksum']='0'*64;item['event_json']=json.dumps(e,separators=(',',':'));item['event_sha256']=hashlib.sha256(item['event_json'].encode()).hexdigest();break
assert audit(corrupt,f['seal'])['status']=='FAIL'
wrong=copy.deepcopy(f['events'])
for item in wrong:
    e=json.loads(item['event_json'])
    if e['type']=='commit':
        e['receipt_event_id']='not-a-durable-receipt';item['event_json']=json.dumps(e,separators=(',',':'));item['event_sha256']=hashlib.sha256(item['event_json'].encode()).hexdigest();break
assert audit(wrong,f['seal'])['status']=='FAIL'
print(json.dumps({'offline_only':True,'independent_python_ledger':positive['status'],'records':1040,'mutation_checks':{'removed_receipts':'FAIL_AS_EXPECTED','missing_seal':'INCONCLUSIVE_AS_EXPECTED','forged_checksum_with_valid_export_hash':'FAIL_AS_EXPECTED','commit_before_or_unrelated_receipt':'FAIL_AS_EXPECTED'}}))
