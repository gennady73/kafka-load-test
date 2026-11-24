// Node-only fake-clock/fake-Kafka checks. No imports of k6, HTTP, sockets or native Kafka.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {createHash} from 'node:crypto';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import vm from 'node:vm';
import {validatePlan,dueMilliseconds,valueFor,receiptFor} from '../continuity-core.mjs';
import {makeSession} from '../continuity-engine.mjs';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'../../..');
const sha=s=>createHash('sha256').update(s).digest('hex');
function planned(stage){
 const py='import json,sys;sys.path.insert(0,"kafka-load-test/preflight");from plan import compile_plan; print(json.dumps(compile_plan(json.load(open("config/preflight-profiles.json")),json.load(open("kaas-demo/tests/continuity/scenarios/'+stage+'.json")),True)[0]))';
 const r=spawnSync('python3',['-c',py],{cwd:root,encoding:'utf8'});assert.equal(r.status,0,r.stderr);return JSON.parse(r.stdout);
}
function armed(stage){
 const p=planned(stage);p.run_id='offline-stub-'+stage; // A stub below opts out of live-ID checks by using a separate explicit name.
 p.run_id='stub-validation-'+stage;p.execution_ready=p.execution_enabled=true;
 for(const s of p.streams)s.group_id=`${s.name}-continuity-${p.run_id}`;
 p.consumer_groups.forEach((g,i)=>{g.group_id=p.streams[i].group_id;});
 p.environment.evidence_sink_url='https://collector.invalid';p.owner=p.stop_control_reference='OFFLINE_STUB_ONLY';p.planned_start_utc='1970-01-01T00:00:30Z';
 for(const k of ['effective_topic_settings_reference','binary_ack_semantics_reference','worker_budget_reference','evidence_collection_reference','stop_policy_reference','security_adapter_reference','initial_group_offsets_reference','native_commit_close_reference','job_deadline_reference','collector_durability_reference'])p.runtime_evidence[k]='OFFLINE_STUB_ONLY';
 return p;
}
function simulate(p,id,opts={}){
 let now=0,native=0,constructs=0,closes=0,commitCount=0,index=0,lastDurableReceipt=null;
 const events=[],envelopes=[],epoch=30000,stream=p.streams[Number(id.split('_')[1])];
 const keep=opts.keep!==false;let receiptAck=0,positionCalls=0;
 const deps={now:()=>now,clockHealthy:()=>!opts.clockDrift,sleep:s=>{now+=s*1000;},sha256:sha,
  post:body=>{
   if(opts.collectorDown)throw Error('offline fake collector unavailable');
   const envelope=JSON.parse(body),e=JSON.parse(envelope.event_json);assert.equal(sha(envelope.event_json),envelope.event_sha256);
   if(opts.slowAttempt&&e.type==='attempt')now+=6000;
   if(e.type==='receipt'&&opts.rejectReceipt)return {status:201,json:()=>({durable:false,event_id:e.event_id,event_sha256:envelope.event_sha256})};
   if(e.type==='receipt'){lastDurableReceipt=e.id;receiptAck++;}
   if(keep){events.push(e);envelopes.push(envelope);}
   return {status:201,json:()=>({durable:true,event_id:e.event_id,event_sha256:opts.wrongCollectorHash?'0'.repeat(64):envelope.event_sha256})};
  },
  readiness:()=>({run_id:p.run_id,plan_sha256:'stub-plan',durable:true,ready_clients:opts.noBarrier?[]:p.clients}),
  log:()=>{},unpersisted:()=>{},abort:()=>{},
  makeProducer:()=>{constructs++;return {produce:r=>{native++;if(opts.ambiguous)throw Error('stub native ambiguity');assert.equal(r.messages[0].value.length,1000);},close:()=>{closes++;if(opts.slowClose)now+=61000;}};},
  makeConsumer:()=>{constructs++;return {
   consume:()=>{
    if(opts.clockGap&&now>=epoch)now+=61000;
    if(index>=stream.record_count){now+=1500;return [];}
    const due=epoch+dueMilliseconds(p,stream,index);
    if(now<due){now=Math.min(due,now+1500);if(now<due)return [];}
    const seq=opts.duplicate&&index===1?0:index;
    const message={topic:stream.name,key:`${p.run_id}:${stream.name}:${seq}`,value:valueFor(p,stream,seq,sha),partition:0,offset:seq};index++;
    if(opts.corrupt)message.value='x'.repeat(1000);
    return [message];
   },position:()=>{positionCalls++;if(opts.assignmentPending&&positionCalls<3)throw Error('offline pending assignment');return 0;},commitOffsets:()=>{assert.ok(lastDurableReceipt,'native commit before a durable receipt ACK');lastDurableReceipt=null;commitCount++;},close:()=>{closes++;}
  };}
 };
 const result=makeSession(p,deps,id,epoch,'stub-plan').run();
 return {result,events,envelopes,native,constructs,closes,commitCount,receiptAck,now};
}
// Check the actual live entrypoint's offline gate with all native/HTTP imports removed.
function checkEntryGate(p){
 let credentialReads=0,constructs=0;const raw=JSON.stringify(p),env={CONTINUITY_PLAN:'candidate.json',CONTINUITY_PLAN_SHA256:sha(raw),CONTINUITY_EXECUTE:'true'};
 const context={__ENV:env,validatePlan,crypto:{sha256:sha},open:name=>{if(name==='candidate.json')return raw;credentialReads++;throw Error('credential/source read before offline gate');},Producer:class{constructor(){constructs++;}},Consumer:class{constructor(){constructs++;}}};
 let source=fs.readFileSync(new URL('../continuity.js',import.meta.url),'utf8').replace(/^import .*;\n/gm,'').replaceAll('export const ','const ').replaceAll('export function ','function ');
 assert.throws(()=>vm.runInNewContext(source,context),/armed/);assert.equal(credentialReads,0);assert.equal(constructs,0);
}
const summary=[];
for(const stage of ['15m','60m','24h']){
 const offline=planned(stage);validatePlan(offline);checkEntryGate(offline);assert.throws(()=>validatePlan(offline,true),/armed/);
 // Exercise every due time across the full logical day without wall-clock waiting.
 let prior=-1;const seen=new Set();
 for(let i=0;i<offline.record_count;i++){
  const t=offline.streams[i%4],seq=Math.floor(i/4),due=dueMilliseconds(offline,t,seq);
  assert.ok(due>prior&&due<offline.duration_seconds*1000);prior=due;seen.add(`${t.name}:${seq}`);
 }
 assert.equal(seen.size,offline.record_count);
 const p=armed(stage);let acks=0,receipts=0,commits=0;
 for(const id of p.clients){
  const s=simulate(p,id,{keep:false});assert.equal(s.result.failed,false,JSON.stringify(s.result));assert.equal(s.constructs,1);assert.equal(s.closes,1);
  acks+=s.result.acks;receipts+=s.result.receipts;commits+=s.result.commits;
 }
 assert.equal(acks,p.record_count);assert.equal(receipts,p.record_count);assert.equal(commits,p.record_count);
 summary.push({stage,logical_seconds:p.duration_seconds,record_count:p.record_count,acks,receipts,commits,mode:'OFFLINE_FAKE_CLOCK_FAKE_KAFKA'});
}
const p=armed('15m');
const pending=simulate(p,'consumer_0',{assignmentPending:true});assert.equal(pending.result.failed,false);assert.equal(pending.constructs,1);
for(const [id,opts] of [['producer_0',{noBarrier:true}],['producer_0',{slowAttempt:true}],['producer_0',{ambiguous:true}],['producer_0',{slowClose:true}],['consumer_0',{rejectReceipt:true}],['consumer_0',{duplicate:true}],['consumer_0',{corrupt:true}],['consumer_0',{clockGap:true}],['producer_0',{clockDrift:true}],['producer_0',{wrongCollectorHash:true}],['producer_0',{collectorDown:true}]]){
 const s=simulate(p,id,opts);assert.equal(s.result.failed,true,JSON.stringify(opts));
 if(opts.rejectReceipt||opts.corrupt)assert.equal(s.commitCount,0);
 if(opts.slowAttempt||opts.noBarrier)assert.equal(s.native,0);
 if(opts.ambiguous)assert.equal(s.native,1);
}
const fixture={plan:p,events:[],seal:{run_id:p.run_id,plan_sha256:'stub-plan',complete:true,event_count:0,client_event_counts:{}}};
for(const id of p.clients){const s=simulate(p,id);fixture.events.push(...s.envelopes);fixture.seal.client_event_counts[id]=s.envelopes.length;}
fixture.seal.event_count=fixture.events.length;
// Optional local file output for the independent Python auditor, never a network export.
if(process.env.CONTINUITY_OFFLINE_FIXTURE)fs.writeFileSync(process.env.CONTINUITY_OFFLINE_FIXTURE,JSON.stringify(fixture));
console.log(JSON.stringify({offline_only:true,simulations:summary,negative_cases:11,fixture_events:fixture.events.length}));
