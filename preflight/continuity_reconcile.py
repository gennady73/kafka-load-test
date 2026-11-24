#!/usr/bin/env python3
"""Offline audit of collector exports for continuity plans. Does not connect or execute clients."""
import argparse,hashlib,json,sqlite3,math
from pathlib import Path

def expected_hash(plan,topic,sequence):
    body=''.join(hashlib.sha256(f'{plan["seed"]}:{topic}:{sequence}:{b}'.encode('ascii')).hexdigest() for b in range(16))[:1000]
    return hashlib.sha256(body.encode('ascii')).hexdigest()

class Ledger:
    def __init__(self,plan,db):
        self.plan=plan;self.db=db;self.issues=[];self.epoch=None;self.events=0
        self.streams={s['name']:s for s in plan['streams']}
        db.executescript('''CREATE TABLE events(id TEXT PRIMARY KEY,client TEXT,n INTEGER,kind TEXT,at REAL);
          CREATE TABLE records(id TEXT PRIMARY KEY,topic TEXT,seq INTEGER,attempt INTEGER DEFAULT 0,ack INTEGER DEFAULT 0,receipt INTEGER DEFAULT 0,commits INTEGER DEFAULT 0,receipt_event TEXT,offset INTEGER);
          CREATE TABLE clients(id TEXT PRIMARY KEY,started INTEGER DEFAULT 0,ready INTEGER DEFAULT 0,close_start INTEGER DEFAULT 0,closed INTEGER DEFAULT 0,ended INTEGER DEFAULT 0,complete INTEGER DEFAULT 0);
          CREATE TABLE heartbeats(client TEXT,at REAL);
          CREATE TABLE progress(client TEXT,kind TEXT,at REAL);
          CREATE TABLE locations(topic TEXT,partition INTEGER,offset INTEGER,PRIMARY KEY(topic,partition,offset));''')
    def add(self,envelope):
        p=self.plan
        raw=envelope['event_json'];digest=hashlib.sha256(raw.encode()).hexdigest()
        if digest!=envelope['event_sha256']:raise ValueError('export hash mismatch')
        e=json.loads(raw)
        if e['run_id']!=p['run_id']:raise ValueError('wrong run')
        if p.get('_file_sha256') and e['plan_sha256']!=p['_file_sha256']:raise ValueError('plan digest mismatch')
        client=e['client_id'];kind=e['type'];at=e['at_ms'];epoch=e['epoch_ms']
        if client not in p['clients'] or type(at) not in (int,float) or not math.isfinite(at) or type(epoch) is not int:raise ValueError('invalid client/time')
        if self.epoch is not None and self.epoch!=epoch:raise ValueError('epoch mismatch')
        self.epoch=epoch
        prefix=f'{p["run_id"]}/{client}/';eid=e['event_id']
        if not eid.startswith(prefix) or not eid[len(prefix):].isdigit():raise ValueError('invalid event identity')
        self.db.execute('INSERT INTO events VALUES(?,?,?,?,?)',(eid,client,int(eid[len(prefix):]),kind,at));self.events+=1
        self.db.execute('INSERT OR IGNORE INTO clients(id) VALUES(?)',(client,))
        topic=e['topic'];stream=self.streams.get(topic);ordinal=int(client.split('_')[1])
        if not stream or stream['ordinal']!=ordinal or e['group_id']!=stream['group_id']:raise ValueError('unplanned topic/group')
        role=client.split('_')[0]
        if kind in ['attempt','acknowledged','receipt','commit','unresolved']:
            rid=e['id'];prefix=f'{p["run_id"]}:{topic}:'
            if not rid.startswith(prefix):raise ValueError('unplanned record ID')
            text=rid[len(prefix):]
            if not text.isdigit() or str(int(text))!=text:raise ValueError('noncanonical sequence')
            seq=int(text)
            if not 0<=seq<stream['record_count']:raise ValueError('record outside budget')
            self.db.execute('INSERT OR IGNORE INTO records(id,topic,seq) VALUES(?,?,?)',(rid,topic,seq))
            due=epoch+(4*seq+stream['ordinal']+0.5)*p['duration_seconds']*1000/p['record_count']
            if kind in ['attempt','acknowledged','receipt']:
                if e.get('value_bytes')!=1000 or e.get('checksum')!=expected_hash(p,topic,seq):raise ValueError('byte/checksum mismatch')
            if kind=='attempt':
                if role!='producer' or not due<=at<=due+p['max_start_lateness_seconds']*1000 or at>=epoch+p['duration_seconds']*1000:raise ValueError('attempt timing/role')
                self.db.execute('UPDATE records SET attempt=attempt+1 WHERE id=?',(rid,))
            elif kind=='acknowledged':
                native=e['acknowledged_at_ms']
                if role!='producer' or e['boundary']!='generation' or not due<=native<=epoch+p['duration_seconds']*1000 or at<native:raise ValueError('late/invalid ACK')
                self.db.execute('UPDATE records SET ack=ack+1 WHERE id=?',(rid,));self.db.execute('INSERT INTO progress VALUES(?,?,?)',(client,kind,native))
            elif kind=='receipt':
                native=e['received_at_ms']
                if role!='consumer' or not epoch<=native<=epoch+(p['duration_seconds']+p['drain_limit_seconds'])*1000 or native-due>p['max_receive_latency_seconds']*1000 or at<native or e['partition']!=0 or type(e['offset']) is not int or e['offset']<0:raise ValueError('receipt timing/location')
                self.db.execute('INSERT INTO locations VALUES(?,?,?)',(topic,e['partition'],e['offset']))
                self.db.execute('UPDATE records SET receipt=receipt+1,receipt_event=?,offset=? WHERE id=?',(eid,e['offset'],rid));self.db.execute('INSERT INTO progress VALUES(?,?,?)',(client,kind,native))
            elif kind=='commit':
                if role!='consumer':raise ValueError('commit role')
                self.db.execute('UPDATE records SET commits=commits+1 WHERE id=?',(rid,))
                # Store link independent of export interleaving; validate after the full stream.
                self.db.execute('CREATE TABLE IF NOT EXISTS commit_links(id TEXT,receipt_event TEXT,client_n INTEGER,at REAL,offset INTEGER)')
                self.db.execute('INSERT INTO commit_links VALUES(?,?,?,?,?)',(rid,e['receipt_event_id'],int(eid[len(f'{p["run_id"]}/{client}/'):]),at,e['offset']))
            else:self.issues.append('unresolved')
        if kind=='client_start':self.db.execute('UPDATE clients SET started=started+1 WHERE id=?',(client,))
        elif kind=='client_ready':
            if at>=epoch:raise ValueError('late readiness')
            if role=='consumer' and (type(e.get('initial_position')) is not int or e['initial_position']<0):raise ValueError('missing assignment evidence')
            self.db.execute('UPDATE clients SET ready=ready+1 WHERE id=?',(client,))
        elif kind=='close_start':self.db.execute('UPDATE clients SET close_start=close_start+1 WHERE id=?',(client,))
        elif kind=='close_end':self.db.execute('UPDATE clients SET closed=closed+? WHERE id=?',(int(e.get('closed') is True),client))
        elif kind=='client_end':
            self.db.execute('UPDATE clients SET ended=ended+1,complete=complete+? WHERE id=?',(int(e.get('complete') is True and e.get('closed') is True and e.get('ready') is True),client))
            expected=stream['record_count']
            if role=='producer' and (e['attempts']!=expected or e['acks']!=expected):raise ValueError('producer terminal count')
            if role=='consumer' and (e['receipts']!=expected or e['commits']!=expected):raise ValueError('consumer terminal count')
        elif kind=='heartbeat':self.db.execute('INSERT INTO heartbeats VALUES(?,?)',(client,at))
        elif kind=='client_error':self.issues.append(e.get('reason','client_error'))
    def report(self,seal=None):
        p=self.plan;db=self.db;issues=list(self.issues);epoch=self.epoch;end=None if epoch is None else epoch+p['duration_seconds']*1000
        rows=db.execute('SELECT id,client,n,kind,at FROM events ORDER BY client,n').fetchall()
        for client in p['clients']:
            ev=[r for r in rows if r[1]==client]
            if [r[2] for r in ev]!=list(range(len(ev))):issues.append('missing/reordered event sequence '+client)
            if any(a[4]>b[4] for a,b in zip(ev,ev[1:])):issues.append('clock order '+client)
            state=db.execute('SELECT started,ready,close_start,closed,ended,complete FROM clients WHERE id=?',(client,)).fetchone()
            if state!=(1,1,1,1,1,1):issues.append('incomplete lifecycle '+client)
            if epoch is not None and ev:
                if ev[0][4]<epoch-p['setup_limit_seconds']*1000 or ev[-1][4]>end+(p['drain_limit_seconds']+p['shutdown_limit_seconds'])*1000:issues.append('outer client envelope '+client)
                phase_end=end if client.startswith('producer') else end+p['drain_limit_seconds']*1000
                close=[r for r in ev if r[3]=='close_start']
                if not close or close[0][4]<phase_end:issues.append('client ended phase early '+client)
                hb=[epoch]+[r[0] for r in db.execute('SELECT at FROM heartbeats WHERE client=? AND at>=? AND at<=? ORDER BY at',(client,epoch,phase_end))]+[phase_end]
                if any(b-a>60000 for a,b in zip(hb,hb[1:])):issues.append('heartbeat gap '+client)
                kind='acknowledged' if client.startswith('producer') else 'receipt'
                progress=[epoch]+[r[0] for r in db.execute('SELECT at FROM progress WHERE client=? AND kind=? AND at>=? AND at<=? ORDER BY at',(client,kind,epoch,end))]+[end]
                if any(b-a>60000 for a,b in zip(progress,progress[1:])):issues.append('success gap '+client)
        records=db.execute('SELECT id,topic,seq,attempt,ack,receipt,commits,receipt_event,offset FROM records').fetchall()
        if len(records)!=p['record_count'] or any(tuple(r[3:7])!=(1,1,1,1) for r in records):issues.append('incomplete/duplicate record ledger')
        if db.execute("SELECT count(*) FROM sqlite_master WHERE name='commit_links'").fetchone()[0]:
            for rid,receipt_event,n,at,offset in db.execute('SELECT * FROM commit_links'):
                r=db.execute('SELECT receipt_event,offset FROM records WHERE id=?',(rid,)).fetchone()
                receipt=db.execute('SELECT n,at FROM events WHERE id=?',(receipt_event,)).fetchone()
                if not r or tuple(r)!=(receipt_event,offset) or not receipt or n<=receipt[0] or at<receipt[1]:issues.append('commit before/mismatched durable receipt')
        complete_seal=bool(seal and seal.get('run_id')==p['run_id'] and seal.get('plan_sha256')==p.get('_file_sha256') and seal.get('complete') is True and seal.get('event_count')==self.events and seal.get('client_event_counts')=={c:sum(r[1]==c for r in rows) for c in p['clients']})
        return {'status':'FAIL' if issues else 'ACCOUNTING_COMPLETE' if complete_seal else 'INCONCLUSIVE','run_id':p['run_id'],'records_in_ledger':len(records),'expected_records':p['record_count'],'value_bytes':len(records)*1000,'event_count':self.events,'collector_export_seal_matches':complete_seal,'issues':issues,'limitations':['A collector seal is an external durability assertion; verify its owner/storage and complete Job/restart/metric evidence before declaring live continuity PASS.','No producer offset matching, capacity or recovery qualification is inferred.']}

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--plan',type=Path,required=True);p.add_argument('--events',type=Path,required=True);p.add_argument('--database',type=Path,required=True);p.add_argument('--seal',type=Path);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    try:
        if a.database.exists() or a.output.exists():raise ValueError('use fresh database and output paths')
        body=a.plan.read_bytes();plan=json.loads(body)
        if plan.get('plan_version')!='continuity-1.0':raise ValueError('continuity plan required')
        plan['_file_sha256']=hashlib.sha256(body).hexdigest()
        db=sqlite3.connect(a.database);ledger=Ledger(plan,db)
        with a.events.open() as f:
            for line in f:
                try:ledger.add(json.loads(line))
                except (ValueError,KeyError,TypeError,sqlite3.IntegrityError) as e:ledger.issues.append('malformed/conflicting export: '+type(e).__name__)
        result=ledger.report(json.loads(a.seal.read_text()) if a.seal else None);db.commit();db.close();a.output.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result));return 0 if result['status']=='ACCOUNTING_COMPLETE' else 1
    except (ValueError,OSError,KeyError,TypeError) as e:p.exit(2,str(e)+'\n')
if __name__=='__main__':raise SystemExit(main())
