import test from 'node:test';
import assert from 'node:assert/strict';
import {auditFixture} from '../audit.mjs';
import {simulate,sha,clone,rechain} from './helpers.mjs';
const good=simulate();
function mutated(fn,{sealChange=null,raw=false}={}){
 const events=clone(good.result.events);fn(events);
 let {envelopes,seal}=rechain(events,good.plan);
 if(sealChange)sealChange(seal);return auditFixture(good.plan,envelopes,seal,sha);
}
const expectIncomplete=a=>assert.equal(a.status,'OFFLINE_INCOMPLETE');
test('independent event checker accepts a complete explicitly simulated fixture',()=>{
 assert.equal(good.audit.status,'OFFLINE_COMPLETE');assert.equal(good.audit.stats.confirmed_commit_records,16);
 assert.equal(good.audit.matched_positive_cases,4);assert.equal(good.audit.matched_negative_cases,8);
 assert.equal(good.audit.runtime_acceptance,'NOT_ASSESSED');
});
for(const type of ['attempt_intent','send_outcome','receipt','durable_receipt_ack','commit_intent','commit','client_close','case_end','denial_result','fixture_terminal','case_start'])test('required event cannot disappear: '+type,()=>{
 expectIncomplete(mutated(events=>events.splice(events.findIndex(e=>e.type===type),1)));
});
test('removing a negative case start alone is incomplete even with valid export hashes',()=>{
 expectIncomplete(mutated(e=>e.splice(e.findIndex(x=>x.type==='case_start'&&x.case_kind==='negative'),1)));
});
for(const [name,change] of Object.entries({
 tenant:e=>{e.case_id='positive-kaas-sbx-b-events';},
 topic:e=>{e.topic='kaas-sbx-b-events';},group:e=>{e.group_id='kaas-sbx-b-events-group';},
 bytes:e=>{e.value_bytes=1001;},checksum:e=>{e.checksum='0'.repeat(64);},partition:e=>{e.partition=1;},offset_gap:e=>{e.offset+=1;}
}))test('receipt mutation rejected: '+name,()=>{
 const a=mutated(events=>change(events.find(e=>e.type==='receipt')));expectIncomplete(a);assert.equal(a.dimensions.accounting,'INCOMPLETE');
});
test('duplicate record evidence is not double-counted or accepted',()=>{
 const a=mutated(events=>{const i=events.findIndex(e=>e.type==='receipt');events.splice(i+1,0,clone(events[i]));});expectIncomplete(a);
});
test('commit before a matching receipt/durable acknowledgement is incomplete',()=>{
 const a=mutated(events=>{const i=events.findIndex(e=>e.type==='commit'),j=events.findIndex(e=>e.type==='durable_receipt_ack');const e=events.splice(i,1)[0];events.splice(j,0,e);});expectIncomplete(a);
});
test('late ACK cannot be relabeled on-time',()=>{
 const a=mutated(events=>{const ack=events.find(e=>e.type==='send_outcome'&&!e.negative);const cutoff=events.find(e=>e.type==='phase_start'&&e.phase==='positive_delivery').positive_cutoff_ms;ack.acknowledged_at_ms=cutoff+1;ack.ack_bucket='on_time';});
 expectIncomplete(a);assert.ok(a.issues.some(i=>i.code==='ack_time_bucket'));
});
test('a later receipt proves presence but does not manufacture a producer ACK',()=>{
 const a=mutated(events=>{const e=events.find(e=>e.type==='send_outcome'&&!e.negative);e.outcome='unresolved';delete e.acknowledged_at_ms;delete e.ack_bucket;e.category='timeout';e.failure_stage='produce';e.typed=false;});
 expectIncomplete(a);assert.equal(a.stats.acknowledged_value_bytes,15000);assert.equal(a.stats.unresolved_value_bytes,1000);assert.equal(a.stats.present_without_producer_ack_value_bytes,1000);
});
test('conflicting duplicate ACK cannot inflate counted bytes',()=>{
 const a=mutated(events=>{const i=events.findIndex(e=>e.type==='send_outcome'&&!e.negative);events.splice(i+1,0,clone(events[i]));});
 expectIncomplete(a);assert.equal(a.stats.acknowledged_value_bytes,16000);
});
test('wrong denial category/stage cannot preserve matrix acceptance',()=>{
 for(const key of ['category','failure_stage']){
  const a=mutated(events=>{events.find(e=>e.type==='denial_result')[key]=key==='category'?'timeout':'consume';});expectIncomplete(a);assert.equal(a.dimensions.isolation,'INCONCLUSIVE');
 }
});
test('unallowlisted error reason/severity is rejected without reflecting it in report',()=>{
 const canary='PRIVATE_FAKE_ERROR_CANARY';
 const a=mutated(events=>{events.splice(-1,0,{...events.at(-1),type:'fixture_stop',reason:canary,status:canary});});expectIncomplete(a);assert.ok(!JSON.stringify(a).includes(canary));
});
test('arbitrary event fields are rejected without exporting raw values',()=>{
 const canary='PRIVATE_FAKE_FIELD_CANARY';const a=mutated(events=>{events[0].password=canary;});expectIncomplete(a);assert.ok(!JSON.stringify(a).includes(canary));
});
test('missing or forged seal, count, export hash and elapsed evidence cannot pass',()=>{
 for(const change of [s=>{s.complete=false;},s=>{s.event_count--;},s=>{s.export_sha256='0'.repeat(64);},s=>{s.observed_job_elapsed_ms=300001;},s=>{delete s.observed_job_elapsed_ms;}])expectIncomplete(mutated(()=>{},{sealChange:change}));
 expectIncomplete(auditFixture(good.plan,good.result.envelopes,null,sha));
});
test('truncated raw export and envelope digest corruption cannot pass',()=>{
 const bad=clone(good.result.envelopes);bad.pop();expectIncomplete(auditFixture(good.plan,bad,good.result.seal,sha));
 const changed=clone(good.result.envelopes);changed[0].event_sha256='0'.repeat(64);expectIncomplete(auditFixture(good.plan,changed,good.result.seal,sha));
});
test('forced-terminal omission cannot be hidden by recalculating a seal',()=>{
 const a=mutated(events=>events.pop());expectIncomplete(a);assert.equal(a.dimensions.cleanup,'INCOMPLETE');
});
test('negative forbidden ACK remains isolation failure outside positive bytes',()=>{
 const x=simulate({unexpectedNegativeAck:true});expectIncomplete(x.audit);assert.equal(x.audit.dimensions.isolation,'FAILED');
 assert.equal(x.audit.stats.positive_acknowledged_value_bytes_on_time,16000);assert.equal(x.audit.stats.acknowledged_value_bytes,17000);
});
