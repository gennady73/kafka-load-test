#!/usr/bin/env python3
"""Stream PF_EVENT JSON records into a fresh SQLite ledger. Never declares production acceptance."""
import argparse
import json
import hashlib
import sqlite3
from pathlib import Path

class Ledger:
    def __init__(self, db, plans):
        self.db=db; self.plans=plans
        self.run=plans[0]['run_id']
        if len(plans)!=plans[0]['worker_count'] or {p['worker_id'] for p in plans}!=set(range(len(plans))):
            raise ValueError('all unique worker plans required')
        common=['run_id','target_value_payload_bytes','duration_seconds','drain_limit_seconds','consumer_groups']
        if any(any(p[k]!=plans[0][k] for k in common) for p in plans): raise ValueError('mixed plans')
        self.expected={(p['worker_id'],s['name']):s for p in plans for s in p['streams']}
        self.sessions={s['session_id'] for p in plans for s in p['consumer_sessions']}
        self.bad=0; self.ignored=0
        db.executescript('''
          CREATE TABLE records(id TEXT PRIMARY KEY,topic TEXT,worker INTEGER,bytes INTEGER,checksum TEXT,
            attempts INTEGER DEFAULT 0,acks_t1 INTEGER DEFAULT 0,acks_late INTEGER DEFAULT 0,ambiguous INTEGER DEFAULT 0);
          CREATE TABLE receipts(id TEXT,grp TEXT,bytes INTEGER,checksum TEXT,valid INTEGER,n INTEGER DEFAULT 1,
            PRIMARY KEY(id,grp));
          CREATE TABLE terminal(worker INTEGER,topic TEXT,at INTEGER,PRIMARY KEY(worker,topic));
          CREATE TABLE workers(worker INTEGER PRIMARY KEY,started INTEGER DEFAULT 0,ended INTEGER DEFAULT 0);
          CREATE TABLE sessions(id TEXT PRIMARY KEY,started INTEGER DEFAULT 0,ended INTEGER DEFAULT 0);
          CREATE TABLE issues(kind TEXT,n INTEGER DEFAULT 1);
        ''')
    def add(self,e):
        if e.get('run_id')!=self.run: self.ignored+=1; return
        kind=e['type']; wid=e['worker_id']; at=e['at_ms']
        if type(wid) is not int or not 0<=wid<len(self.plans) or type(at) is not int:
            raise ValueError('invalid worker/timestamp')
        expected_hash=self.plans[wid].get('_file_sha256')
        if expected_hash and e.get('plan_sha256')!=expected_hash: raise ValueError('event plan hash mismatch')
        if kind in ('attempt','acknowledged','ambiguous'):
            id=e['id']; topic=e['topic']; size=e['value_bytes']
            if (wid,topic) not in self.expected: raise ValueError('unplanned producer stream')
            if type(size) is not int or size<=0: raise ValueError('invalid byte size')
            prior=self.db.execute('SELECT topic,worker,bytes,checksum FROM records WHERE id=?',(id,)).fetchone()
            checksum=e.get('checksum')
            if prior and (tuple(prior[:3])!=(topic,wid,size) or (checksum and prior[3]!=checksum)):
                self.bad+=1; return
            if not prior:
                if kind!='attempt': self.bad+=1; return
                self.db.execute('INSERT INTO records(id,topic,worker,bytes,checksum) VALUES(?,?,?,?,?)',(id,topic,wid,size,checksum))
            column={'attempt':'attempts','ambiguous':'ambiguous','acknowledged':'acks_t1' if e.get('boundary')=='generation' else 'acks_late'}[kind]
            self.db.execute(f'UPDATE records SET {column}={column}+1 WHERE id=?',(id,))
        elif kind=='receipt':
            group=next((g for g in self.plans[0]['consumer_groups'] if g['group_id']==e['group_id']),None)
            if not group or e['topic'] not in group['topics']: self.bad+=1; return
            old=self.db.execute('SELECT bytes,checksum,valid FROM receipts WHERE id=? AND grp=?',(e['id'],e['group_id'])).fetchone()
            valid=bool(e['valid'])
            if old:
                valid=valid and bool(old[2]) and old[0]==e['value_bytes'] and old[1]==e['checksum']
                self.db.execute('UPDATE receipts SET n=n+1,valid=? WHERE id=? AND grp=?',(valid,e['id'],e['group_id']))
            else:
                self.db.execute('INSERT INTO receipts(id,grp,bytes,checksum,valid) VALUES(?,?,?,?,?)',
                    (e['id'],e['group_id'],e['value_bytes'],e['checksum'],valid))
        elif kind=='generation_end':
            if (wid,e['topic']) not in self.expected: raise ValueError('unplanned terminal')
            self.db.execute('INSERT OR REPLACE INTO terminal VALUES(?,?,?)',(wid,e['topic'],at))
        elif kind in ('worker_start','worker_end'):
            self.db.execute('INSERT OR IGNORE INTO workers(worker) VALUES(?)',(wid,))
            self.db.execute(f'UPDATE workers SET {"started" if kind=="worker_start" else "ended"}=1 WHERE worker=?',(wid,))
        elif kind in ('consumer_start','consumer_end'):
            sid=e['session_id']
            if sid not in self.sessions: raise ValueError('unknown consumer session')
            self.db.execute('INSERT OR IGNORE INTO sessions(id) VALUES(?)',(sid,))
            self.db.execute(f'UPDATE sessions SET {"started" if kind=="consumer_start" else "ended"}=1 WHERE id=?',(sid,))
        elif kind.endswith('error') or kind=='foreign_record':
            self.db.execute('INSERT INTO issues(kind) VALUES(?)',(kind,))
    def report(self):
        def scalar(q,args=()): return self.db.execute(q,args).fetchone()[0] or 0
        attempted=scalar('SELECT sum(bytes) FROM records')
        attempts=scalar('SELECT sum(attempts) FROM records')
        ack=scalar('SELECT sum(bytes) FROM records WHERE acks_t1>0')
        late=scalar('SELECT sum(bytes) FROM records WHERE acks_t1=0 AND acks_late>0')
        unresolved=scalar('SELECT sum(bytes) FROM records WHERE acks_t1=0 AND acks_late=0')
        invalid=scalar('SELECT count(*) FROM receipts r LEFT JOIN records p ON p.id=r.id WHERE r.valid=0 OR (p.id IS NOT NULL AND (r.bytes!=p.bytes OR r.checksum!=p.checksum))')
        unexpected=scalar('SELECT count(*) FROM receipts r LEFT JOIN records p ON p.id=r.id WHERE p.id IS NULL')
        streams=scalar('SELECT count(*) FROM terminal')
        workers=scalar('SELECT count(*) FROM workers WHERE started=1 AND ended=1')
        sessions=scalar('SELECT count(*) FROM sessions WHERE started=1 AND ended=1')
        issues=scalar('SELECT count(*) FROM issues')
        coverage=streams==len(self.expected) and workers==len(self.plans) and sessions==len(self.sessions) and not self.bad and not issues
        groups={}
        for g in self.plans[0]['consumer_groups']:
            placeholders=','.join('?' for _ in g['topics'])
            missing=scalar(f'''SELECT count(*) FROM records p LEFT JOIN receipts r ON p.id=r.id AND r.grp=?
                WHERE (p.acks_t1>0 OR p.acks_late>0) AND p.topic IN ({placeholders}) AND r.id IS NULL''',
                (g['group_id'],*g['topics']))
            groups[g['group_id']]={'required_full_run':g['require_full_run'],
                'missing_acknowledged_ids':missing if g['require_full_run'] else None,
                'duplicate_deliveries':scalar('SELECT sum(n-1) FROM receipts WHERE grp=?',(g['group_id'],)),
                'status':'INCOMPLETE' if g['require_full_run'] and missing else 'INCONCLUSIVE'}
        target=self.plans[0]['target_value_payload_bytes']; tolerance=self.plans[0]['target_tolerance_fraction']
        # Evidence streams have not been independently attested complete. Even balanced logs
        # cannot prove every record/event survived the evidence collection path.
        return dict(run_id=self.run,attempted_unique_value_bytes=attempted,attempt_count=attempts,
            acknowledged_value_bytes_t1=ack,acknowledged_value_bytes_late_only=late,
            unresolved_value_bytes=unresolved,not_attempted_value_bytes=target-attempted,
            target_value_payload_bytes=target,target_tolerance_fraction=tolerance,
            observed_target='ACHIEVED' if abs(ack-target)<=target*tolerance else 'NOT ACHIEVED',
            evidence_markers_complete=coverage,malformed_or_conflicting_events=self.bad,
            ignored_other_run_events=self.ignored,invalid_receipts=invalid,unexpected_receipts=unexpected,
            groups=groups,integrity='FAILED' if invalid else 'INCONCLUSIVE',
            accounting='INCOMPLETE' if not coverage or unresolved or unexpected else 'OBSERVED LOGS BALANCE; COLLECTION COVERAGE UNATTESTED',
            production_acceptance='NOT ASSESSED')

def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--plans',type=Path,required=True)
    ap.add_argument('--events',type=Path,nargs='+',required=True)
    ap.add_argument('--database',type=Path,required=True)
    args=ap.parse_args()
    try:
        if args.database.exists(): raise ValueError('database exists; use a fresh evidence database')
        plans=[]
        manifest=json.loads((args.plans/'manifest.json').read_text())
        for p in sorted(args.plans.glob('worker-*.json')):
            raw=p.read_bytes(); digest=hashlib.sha256(raw).hexdigest()
            if manifest.get(p.name)!=digest: raise ValueError('plan manifest hash mismatch')
            plan=json.loads(raw);plan['_file_sha256']=digest;plans.append(plan)
        if not plans: raise ValueError('no worker plans')
        db=sqlite3.connect(args.database); ledger=Ledger(db,plans)
        n=0
        for path in args.events:
            with path.open() as f:
                for line in f:
                    marker=line.find('PF_EVENT ')
                    if marker<0: continue
                    payload=line[marker+len('PF_EVENT '):]
                    # Accept raw k6 logs or k6 JSON logs; default escaped text logs are rejected.
                    try:
                        if line.lstrip().startswith('{'):
                            try:
                                wrapper=json.loads(line)
                                if 'msg' in wrapper: payload=wrapper['msg'].split('PF_EVENT ',1)[1]
                            except json.JSONDecodeError: pass
                        e,_=json.JSONDecoder().raw_decode(payload)
                        ledger.add(e)
                    except (ValueError,KeyError,TypeError): ledger.bad+=1
                    n+=1
                    if n%10000==0: db.commit()
        db.commit(); print(json.dumps(ledger.report(),indent=2)); db.close()
    except (ValueError,OSError,sqlite3.Error) as err: ap.exit(2,f'reconciliation rejected: {err}\n')

if __name__=='__main__': main()
