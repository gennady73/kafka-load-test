import test from 'node:test';
import assert from 'node:assert/strict';
import {compileContract,validatePlan,validateContract,expectedNegatives,canonical,SAFE_REASONS,SAFE_SEVERITIES} from '../policy.mjs';
import {runTenantFixture} from '../engine.mjs';
import {contract,plan,clone,simulate,sha} from './helpers.mjs';

test('deterministic compile freezes exact positive/negative record and credential sets',()=>{
 const p=plan();assert.equal(canonical(p),canonical(plan()));assert.equal(p.records.length,16);assert.equal(p.negative.length,8);
 assert.equal(new Set([...p.records,...p.negative.filter(n=>n.record).map(n=>n.record)].map(r=>r.id)).size,20);
 assert.ok(Object.isFrozen(p.bindings[0]));assert.ok(p.bindings.every(b=>b.principal===b.id&&!b.principal.includes('smoke')));
});
const inputMutations={
 live:c=>{c.execution_enabled=true;},tls:c=>{c.security.hostname_verification=false;},smoke:c=>{c.tenants[0].roles.producer.principal='kaas-smoke-client';},
 path:c=>{c.tenants[0].roles.producer.proposed_password_mount_path='/credentials/password';},
 extra_role:c=>{c.tenants[0].roles.admin=c.tenants[0].roles.producer;},
 wildcard:c=>{c.tenants[0].roles.producer.acls_declared[0].resource.name='*';},
 group_suffix:c=>{c.tenants[0].topics[0].group+='-run';},
 seventeen_positive:c=>{c.tenants[0].topics[0].record_count=5;},
 fifth_negative_write:c=>{c.negative_cases.push(clone(c.negative_cases[0]));},
 oversize:c=>{c.tenants[0].topics[0].original_value_bytes_per_record=2097153;},
 fake_idempotence:c=>{c.candidate_client.idempotence_effective_setting=false;},
 unknown_password:c=>{c.tenants[0].roles.producer.password='DO_NOT_ACCEPT';},
 native_retry_count:c=>{c.candidate_client.native_retry_count=1;}
};
for(const [name,mutate] of Object.entries(inputMutations))test('input rejects '+name,()=>{const c=contract();mutate(c);assert.throws(()=>validateContract(c));});
const planMutations={
 I02_smoke_binding:p=>{p.bindings[0].principal='kaas-smoke-client';p.bindings[0].password_path='/credentials/password';},
 swapped_roles:p=>{p.bindings[0].principal=p.bindings[1].principal;},duplicate_binding:p=>{p.bindings[1]=p.bindings[0];},
 widened_acl:p=>{p.bindings[0].acls[0].resource.name='*';},wrong_positive_caller:p=>{p.positive[0].producer=p.bindings[2].id;},
 missing_controls:p=>{p.negative[0].control_ids=[];},wrong_denial:p=>{p.negative[0].expected_category='cluster_authorization_denied';},
 group_suffix:p=>{p.positive[0].group+='-run';},unknown_topic:p=>{p.negative[0].topic='unapproved';},extra_record:p=>{p.records.push(clone(p.records[0]));},
 record_bytes:p=>{p.records[0].value_bytes=2000;},record_checksum:p=>{p.records[0].checksum='0'.repeat(64);},
 native_mapping_forged:p=>{p.native_mapping_status='VERIFIED';},negative_reserve:p=>{p.call_reserve_ms=-1;},
 nonfinite_reserve:p=>{p.sink_reserve_ms=NaN;},close_reserve:p=>{p.close_reserve_ms=0;},
 settings_signature:p=>{p.write_settings_signature='a'.repeat(64);},unknown_key:p=>{p.principal_override='kaas-smoke-client';},
 live_flag:p=>{p.execution_ready=true;}
};
for(const [name,mutate] of Object.entries(planMutations))test('engine boundary rejects '+name+' before credential or native access',()=>{
 const p=clone(plan());mutate(p);let reads=0;
 assert.throws(()=>runTenantFixture(p,{mode:'OFFLINE_STUB',sha256:sha,readCredential:()=>{reads++;}}));assert.equal(reads,0);
});
test('either own literal group may be paired with either own topic; pairing is not an extra ACL rule',()=>{
 const c=contract();c.tenants[0].topics[0].group=c.tenants[0].topics[1].group;
 const expected=expectedNegatives(c.tenants);c.negative_cases.forEach((n,i)=>Object.assign(n,expected[i]));
 const p=plan(c);validatePlan(JSON.parse(JSON.stringify(p)));assert.equal(simulate({},null,p).audit.status,'OFFLINE_COMPLETE');
});
test('known-good minimal fixture accounts for 16 positive IDs and four denied writes',()=>{
 const x=simulate();assert.equal(x.result.status,'OFFLINE_COMPLETE');assert.equal(x.audit.status,'OFFLINE_COMPLETE');
 assert.equal(x.world.calls.filter(c=>c.type==='produce').length,20);assert.equal(x.world.commits.length,16);
 assert.ok(x.world.commits.every(c=>!c.negative));assert.equal(x.result.stats.acknowledged_value_bytes,16000);
 assert.equal(x.result.stats.definitive_not_accepted_value_bytes,4000);assert.equal(x.result.stats.unresolved_value_bytes,0);
 assert.equal(x.audit.matched_negative_cases,8);assert.equal(x.audit.runtime_acceptance,'NOT_ASSESSED');
});
for(const category of ['cluster_authorization_denied','authentication_failed','tls_failed','network_failed','timeout','unknown','group_authorization_denied'])test('wrong denial layer cannot pass: '+category,()=>{
 const p=plan(),id=p.negative[0].id,x=simulate({negativeFailure:{[id]:category}},null,p);
 assert.equal(x.result.cases[id],'INCONCLUSIVE');assert.notEqual(x.audit.status,'OFFLINE_COMPLETE');
});
test('wrong operation/stage and misleading authorization text never pass',()=>{
 const p=plan(),id=p.negative[0].id,x=simulate({negativeFailure:{[id]:{category:'topic_authorization_denied',stage:'consume'}}},null,p);
 assert.equal(x.result.cases[id],'INCONCLUSIVE');assert.equal(x.result.stats.unresolved_value_bytes,1000);
});
test('consumer-write IdempotentWrite confound is INCONCLUSIVE despite a typed cluster denial',()=>{
 const x=simulate({consumerWriteClusterDenied:true});const c=x.plan.negative.find(n=>n.consumer_write_confounded);
 assert.equal(x.result.cases[c.id],'INCONCLUSIVE');assert.equal(x.result.first_failure.reason,'consumer_write_layer_confounded');
 assert.equal(x.audit.dimensions.isolation,'INCONCLUSIVE');
});
test('empty forbidden consume is not denial',()=>{
 const p=plan(),id=p.negative.find(n=>n.operation==='consume').id,x=simulate({negativeFailure:{[id]:'empty'}},null,p);
 assert.equal(x.result.cases[id],'INCONCLUSIVE');assert.notEqual(x.audit.status,'OFFLINE_COMPLETE');assert.ok(x.world.commits.every(c=>!c.negative));
});
for(const fault of ['unexpectedNegativeAck','unexpectedNegativeRecord','unexpectedJoin'])test('unexpected access stops subsequent probes: '+fault,()=>{
 const x=simulate({[fault]:true});assert.equal(x.result.first_failure.severity,'FAIL');assert.equal(x.audit.dimensions.isolation,'FAILED');
 assert.ok(x.world.commits.every(c=>!c.negative));assert.ok(Object.values(x.result.cases).includes('SKIPPED'));
});
test('unexpected group join captured before a negative read; close and final snapshot retained',()=>{
 const x=simulate({unexpectedJoin:true});const id=x.plan.negative.find(n=>n.operation==='consume').id;
 assert.ok(!x.world.calls.some(c=>c.type==='consume'&&c.negative));
 assert.ok(x.result.events.some(e=>e.type==='group_state'&&e.case_id===id&&e.stage==='after_stop'));
});
for(const fault of ['foreignPositive','corruptPositive','offsetGap','wrongPartition','duplicateReceipt'])test('invalid receipt never permits a later commit: '+fault,()=>{
 const x=simulate({[fault]:true});assert.notEqual(x.audit.status,'OFFLINE_COMPLETE');assert.equal(x.result.first_failure.severity,'FAIL');
 assert.ok(x.world.commits.length<16);if(fault!=='duplicateReceipt')assert.equal(x.world.commits.length,0);
});
for(const fault of ['nonFixtureGroup','sharedGroup','unobservedMembership'])test('unknown group ownership/membership blocks clients: '+fault,()=>{
 const x=simulate({[fault]:true});assert.equal(x.world.calls.length,0);assert.notEqual(x.audit.status,'OFFLINE_COMPLETE');
});
test('native auto-commit capability failure blocks credential reads',()=>{const x=simulate({autoCommitEnabled:true});assert.equal(x.world.reads.length,0);assert.equal(x.world.calls.length,0);});
test('unexpected implicit negative commit is detected, never cleared by no explicit commit',()=>{const x=simulate({implicitNegativeCommit:true});assert.equal(x.result.first_failure.severity,'FAIL');assert.equal(x.audit.dimensions.isolation,'FAILED');});
test('debug gate precedes all credential loading',()=>{const x=simulate({debugUnsafe:true});assert.equal(x.world.reads.length,0);assert.equal(x.world.calls.length,0);});
test('missing password and missing CA never fall back to smoke credentials',()=>{
 for(const opts of [{emptyCredential:'/credentials/kaas-sbx-a-producer/password'},{emptyCA:true}]){const x=simulate(opts);assert.equal(x.world.calls.length,0);assert.notEqual(x.audit.status,'OFFLINE_COMPLETE');}
});
test('sink failure before attempted send prevents native invocation',()=>{const x=simulate({sinkThrowType:'attempt_intent'});assert.equal(x.world.calls.filter(c=>c.type==='produce').length,0);assert.notEqual(x.audit.status,'OFFLINE_COMPLETE');});
for(const key of ['refuseDurableType','wrongAckType'])test('receipt '+key+' prevents commit',()=>{const x=simulate({[key]:'receipt'});assert.equal(x.world.commits.length,0);assert.notEqual(x.audit.status,'OFFLINE_COMPLETE');});
test('duplicate matching durable acknowledgements are idempotent',()=>{
 const x=simulate({},w=>{const persist=w.deps.persist;w.deps.persist=e=>{persist(e);return persist(e);};});
 assert.equal(x.audit.status,'OFFLINE_COMPLETE');assert.equal(x.world.sink.length,x.result.envelopes.length);assert.equal(x.world.commits.length,16);
});
test('commit timeout preserves uncertainty, not fabricated confirmation',()=>{
 const x=simulate({commitTimeout:true});assert.equal(x.world.commits.length,1);assert.equal(x.audit.stats.confirmed_commit_records,0);
 assert.equal(x.audit.stats.uncertain_commit_records,1);assert.notEqual(x.audit.status,'OFFLINE_COMPLETE');
});
test('startup time is charged to the one global envelope',()=>{const x=simulate({initialTimeMs:50000});assert.equal(x.world.reads.length,0);assert.equal(x.world.calls.length,0);assert.equal(x.result.first_failure.reason,'budget_exhausted');});
test('evidence latency recheck refuses a send after its intent when reserve no longer fits',()=>{
 const x=simulate({sinkLatencyByType:{attempt_intent:85000}});assert.equal(x.world.calls.filter(c=>c.type==='produce').length,0);
 assert.ok(x.result.events.some(e=>e.type==='not_sent'));assert.equal(x.result.stats.attempted_unique_value_bytes,0);
});
test('late ACK is preserved but excluded from positive target',()=>{
 const x=simulate({produceDelayMs:100000});assert.equal(x.result.stats.positive_acknowledged_value_bytes_on_time,0);
 assert.equal(x.result.stats.late_acknowledged_value_bytes,1000);assert.notEqual(x.audit.status,'OFFLINE_COMPLETE');
});
test('terminal sink overrun cannot produce a complete seal or report',()=>{
 const x=simulate({sinkLatencyByType:{fixture_terminal:300001}});assert.equal(x.result.status,'OFFLINE_INCOMPLETE');assert.equal(x.result.seal.complete,false);assert.equal(x.audit.dimensions.duration,'INCOMPLETE');
});
test('unknown positive send outcome stays unresolved, with no application retry',()=>{
 const x=simulate({positiveTimeout:true,acceptBeforeTimeout:true});assert.equal(x.result.stats.unresolved_value_bytes,1000);
 assert.equal(x.world.calls.filter(c=>c.type==='produce').length,1);assert.equal(x.result.stats.acknowledged_value_bytes,0);
});
test('original send failure survives close failure and every initialized client receives one cleanup offer',()=>{
 const x=simulate({positiveTimeout:true,closeThrows:true});assert.equal(x.result.first_failure.reason,'positive_send_unresolved');
 const constructed=x.world.calls.filter(c=>c.type.startsWith('construct')).length,offers=x.world.calls.filter(c=>c.type.startsWith('close')).length;
 assert.equal(offers,constructed);assert.equal(x.result.cleanup_complete,false);
});
test('external stop preserves partial outcomes and launches no later work',()=>{
 const x=simulate({},w=>{w.deps.shouldStop=()=>w.calls.filter(c=>c.type==='produce').length>=1;});
 assert.equal(x.world.calls.filter(c=>c.type==='produce').length,1);assert.equal(x.result.first_failure.reason,'stop_requested');assert.notEqual(x.audit.status,'OFFLINE_COMPLETE');
});
for(const path of ['read','factory','consume','sink','close'])test('I01 external exception properties/cause/getters are never exported: '+path,()=>{
 const canary='SYNTHETIC_PRIVATE_CANARY_I01';
 const x=simulate({},w=>{
  const evil=()=>{const e=new Error(canary);e.severity=canary;e.cause={token:canary};Object.defineProperty(e,'adapterCode',{get(){throw Error(canary);}});return e;};
  if(path==='read')w.deps.readCredential=()=>{throw evil();};
  if(path==='factory')w.deps.makeProducer=()=>{throw evil();};
  if(path==='sink')w.deps.persist=()=>{throw evil();};
  if(['consume','close'].includes(path)){const make=w.deps.makeConsumer;w.deps.makeConsumer=o=>{const c=make(o);c[path==='consume'?'consume':'close']=()=>{throw evil();};return c;};}
 });
 assert.ok(!JSON.stringify(x.result).includes(canary));assert.ok(!JSON.stringify(x.audit).includes(canary));
 if(x.result.first_failure){assert.ok(SAFE_REASONS.includes(x.result.first_failure.reason));assert.ok(SAFE_SEVERITIES.includes(x.result.first_failure.severity));}
});
test('external adapter-like properties cannot forge PASS, an internal error, or a reason canary',()=>{
 const canary='SYNTHETIC_ADAPTER_CODE_CANARY';const x=simulate({},w=>{w.deps.readCredential=()=>{throw {adapterCode:canary,severity:'PASS',cause:{password:canary}};};});
 assert.equal(x.result.first_failure.reason,'client_or_evidence_failure');assert.equal(x.result.first_failure.severity,'INCONCLUSIVE');
 assert.ok(!JSON.stringify(x.result).includes(canary));assert.notEqual(x.audit.status,'OFFLINE_COMPLETE');
});
test('topic denial attributed to the wrong group-join stage is inconclusive',()=>{
 const p=plan(),id=p.negative.find(n=>n.id.endsWith('cross-topic-read')).id;
 const x=simulate({negativeFailure:{[id]:{category:'topic_authorization_denied',stage:'group_join'}}},null,p);
 assert.equal(x.result.cases[id],'INCONCLUSIVE');assert.notEqual(x.audit.status,'OFFLINE_COMPLETE');
});
test('slow native commit and close remain nonpassing rather than claiming cancellation',()=>{
 for(const opts of [{commitDelayMs:100000},{closeDelayMs:2000}]){const x=simulate(opts);assert.notEqual(x.result.status,'OFFLINE_COMPLETE');assert.notEqual(x.audit.status,'OFFLINE_COMPLETE');}
});
test('typed errors with unreviewed mapping or generic text remain inconclusive',()=>{
 const p=plan(),id=p.negative[0].id;
 const x=simulate({},w=>{w.deps.normalizeNativeError=e=>({category:e.offline_category,stage:e.offline_stage,typed:true,mapping_id:'UNREVIEWED'});},p);
 assert.equal(x.result.cases[id],'INCONCLUSIVE');assert.equal(x.audit.runtime_acceptance,'NOT_ASSESSED');
});
test('missing positive control cannot be forged by a serialized plan',()=>{const p=clone(plan());p.negative[0].control_ids=[];assert.throws(()=>validatePlan(p));});
test('unsupported native return values never manufacture ACK or commit confirmation',()=>{
 const send=simulate({},w=>{const make=w.deps.makeProducer;w.deps.makeProducer=o=>{const c=make(o);c.produce=()=>false;return c;};});
 assert.equal(send.result.stats.acknowledged_value_bytes,0);assert.equal(send.result.stats.unresolved_value_bytes,1000);
 const commit=simulate({},w=>{const make=w.deps.makeConsumer;w.deps.makeConsumer=o=>{const c=make(o);c.commitOffsets=()=>false;return c;};});
 assert.equal(commit.audit.stats.confirmed_commit_records,0);assert.equal(commit.audit.stats.uncertain_commit_records,1);
});
test('nonfinite or string job baseline rejected before evidence/credentials can reflect it',()=>{
 for(const baseline of [NaN,Infinity,-1,'PRIVATE_CLOCK_CANARY']){
  const p=plan();let reads=0;
  assert.throws(()=>runTenantFixture(p,{mode:'OFFLINE_STUB',sha256:sha,jobStartMs:baseline,readCredential:()=>{reads++;}}),/invalid_clock_baseline/);
  assert.equal(reads,0);
 }
});
test('last negative call cannot borrow a later phase after a native overrun',()=>{
 const x=simulate({},w=>{const make=w.deps.makeProducer;w.deps.makeProducer=o=>{const c=make(o),send=c.produce;c.produce=r=>{if(o.principal==='kaas-sbx-b-consumer')w.deps.sleep(100);return send(r);};return c;};});
 assert.equal(x.result.status,'OFFLINE_INCOMPLETE');assert.notEqual(x.audit.status,'OFFLINE_COMPLETE');
});
test('last positive evidence write cannot silently overrun its phase',()=>{
 const x=simulate({},w=>{const persist=w.deps.persist;w.deps.persist=e=>{const out=persist(e),event=JSON.parse(e.event_json);if(event.type==='case_end'&&event.case_id==='positive-kaas-sbx-b-audit')w.deps.sleep(100);return out;};});
 assert.equal(x.result.status,'OFFLINE_INCOMPLETE');assert.notEqual(x.audit.status,'OFFLINE_COMPLETE');
});
test('last negative evidence write cannot silently overrun its phase',()=>{
 const x=simulate({},w=>{const persist=w.deps.persist;w.deps.persist=e=>{const out=persist(e),event=JSON.parse(e.event_json);if(event.type==='denial_result'&&event.case_id==='kaas-sbx-b-consumer-write')w.deps.sleep(100);return out;};});
 assert.equal(x.result.status,'OFFLINE_INCOMPLETE');assert.notEqual(x.audit.status,'OFFLINE_COMPLETE');
});
