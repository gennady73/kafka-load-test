// Additive tenant adapter. Dependency-injected only: this module imports no k6, HTTP, fs or Kafka.
// No live entrypoint is supplied. The bundled driver provides in-memory fakes exclusively.
import {canonical,freeze,validatePlan,valueFor,ERROR_CATEGORIES,STAGES,SAFE_REASONS,SAFE_SEVERITIES,require} from './policy.mjs';
import {ascii} from '../core.mjs';

const internalFailures=new WeakMap();
export function runTenantFixture(plan,deps){
 validatePlan(plan);
 require(deps.mode==='OFFLINE_STUB'&&typeof deps.sha256==='function','offline_stub_dependencies_required');
 const sha=deps.sha256,planHash=sha(canonical(plan));
 const events=[],envelopes=[],unpersisted=[],handles=[],credentials=new Map(),reserved=new Set(),controls=new Set();
 const recordStates=new Map([...plan.records,...plan.negative.filter(n=>n.record).map(n=>n.record)].map(r=>[r.id,{id:r.id,case_id:r.case_id,value_bytes:r.value_bytes,intent:false,native_called:false,outcome:'not_attempted',ack_bucket:null,presence:false,receipt:false,committed:false,commit_uncertain:false}]));
 const cases=Object.fromEntries([...plan.positive,...plan.negative].map(c=>[c.id,'SKIPPED']));
 let start;try{start=deps.jobStartMs??deps.now();}catch(_){throw new Error('invalid_clock_baseline');}
 require(Number.isFinite(start)&&start>=0,'invalid_clock_baseline');let lastNow=start,phase='startup_and_controls',phaseEnd=start+45000;
 const end=start+300000;let serial=0,previous=null,reservedBytes=0,firstFailure=null,positiveCutoff=null,closed=true;
 const phaseEnds={};
 const normalizeCategory=e=>{
  let x;try{x=deps.normalizeNativeError?.(e);}catch(_){return {category:'unknown',stage:'unknown',typed:false};}
  if(!x||x.typed!==true||x.mapping_id!=='OFFLINE_STUB_TYPED_V1'||!ERROR_CATEGORIES.includes(x.category)||!STAGES.includes(x.stage))return {category:'unknown',stage:'unknown',typed:false};
  return {category:x.category,stage:x.stage,typed:true};
 };
 function issue(code,severity='INCONCLUSIVE'){const reason=SAFE_REASONS.includes(code)?code:'internal_policy_failure';const level=SAFE_SEVERITIES.includes(severity)?severity:'INCONCLUSIVE';const e=new Error(reason);internalFailures.set(e,{reason,severity:level});throw e;}
 function now(){let n;try{n=deps.now();}catch(_){issue('clock_invalid','FAIL');}if(!Number.isFinite(n)||n<lastNow)issue('clock_invalid','FAIL');lastNow=n;return n;}
 function budget(ms=0,deadline=phaseEnd){
  if(deps.shouldStop?.()===true)issue('stop_requested');
  const cleanup=handles.filter(h=>!h.closed).length*plan.close_reserve_ms+2*plan.sink_reserve_ms;
  if(now()+ms+cleanup>Math.min(deadline,end))issue('budget_exhausted');
 }
 function rawEvent(type,fields){return {version:1,run_id:plan.run_id,plan_sha256:planHash,client_id:'tenant_fixture',
  event_id:`${plan.run_id}/tenant_fixture/${serial++}`,type,at_ms:now(),previous_event_sha256:previous,...fields};}
 function emit(type,fields={}){
  const e=rawEvent(type,fields),event_json=JSON.stringify(e),event_sha256=sha(event_json),envelope={event_json,event_sha256};
  // Always construct only allowlisted fields at call sites; never spread native errors/options/records.
  try{
   const response=deps.persist(envelope),a=response?.ack;
   if(![200,201].includes(response?.status)||!a||a.durable!==true||a.event_id!==e.event_id||a.event_sha256!==event_sha256)issue('durable_evidence_unavailable');
  }catch(_){unpersisted.push({type,event_id:e.event_id,reason:'durable_evidence_unavailable'});issue('durable_evidence_unavailable');}
  previous=event_sha256;events.push(e);envelopes.push(envelope);return e.event_id;
 }
 function enter(name){if(now()>phaseEnd)issue('phase_overrun');phase=name;phaseEnd=Math.min(now()+plan.phase_seconds[name]*1000,end);phaseEnds[name]=phaseEnd;
  if(name==='positive_delivery')positiveCutoff=phaseEnd;
  emit('phase_start',{phase,deadline_ms:phaseEnd,positive_cutoff_ms:positiveCutoff});budget();}
 function binding(id){const b=plan.bindings.find(x=>x.id===id);if(!b)issue('unknown_binding','FAIL');return b;}
 function options(b,topic,group){
  const password=credentials.get(b.id);
  if(typeof password!=='string'||password.length===0)issue('credential_unavailable');
  return freeze({binding_id:b.id,tenant:b.tenant,role:b.role,principal:b.principal,password_path:b.password_path,
   password,ca:credentials.get('public_ca'),protocol:'SASL_SSL',mechanism:'SCRAM-SHA-512',hostname_verification:true,
   topic,group:group??null,auto_create_topic:false,auto_commit:false,commit_on_close:false,required_acks:-1,
   native_message_max_bytes:3145728,consumer_fetch_max_bytes:4194304,compression:'none',
   maximum_application_attempts:1,delivery_timeout_seconds_candidate:10,idempotence_effective:null,transactional:false});
 }
 function make(role,id,topic,group,caseId){
  budget(plan.call_reserve_ms+2*plan.sink_reserve_ms);
  const b=binding(id),o=options(b,topic,group);
  const client=role==='producer'?deps.makeProducer(o):deps.makeConsumer(o);
  const h={client,role,binding:b.id,case_id:caseId,closed:false};handles.push(h);
  emit('client_open',{case_id:caseId,binding_id:b.id,client_role:role,topic,group_id:group??null,auto_commit:false,commit_on_close:false,write_settings_signature:role==='producer'?plan.write_settings_signature:null});
  budget();return h;
 }
 function close(h){
  if(h.closed||h.close_offered)return;
  h.close_offered=true;
  if(now()+plan.close_reserve_ms+2*plan.sink_reserve_ms>end){closed=false;emit('close_skipped',{case_id:h.case_id,binding_id:h.binding,reason:'budget_exhausted'});return;}
  let closeOk=false;const at=now();
  try{const result=h.client.close();if(result!==undefined)throw new Error('close_return_unverified');h.closed=true;closeOk=now()-at<=plan.close_reserve_ms&&now()<=end;}
  catch(_){/* Do not serialize an exception; preserve the original stop reason. */}
  if(!closeOk)closed=false;
  emit('client_close',{case_id:h.case_id,binding_id:h.binding,client_role:h.role,closed:h.closed,within_reserve:closeOk});
  if(!closeOk)issue('close_failed_or_overrun');
 }
 function state(group,topic,caseId,stage){
  budget(plan.sink_reserve_ms);
  const s=deps.groupState(group,topic);
  if(!s||s.fixture_only!==true||s.exclusive!==true||s.members!==0||s.membership_observed!==true||!(s.committed_offset===null||Number.isSafeInteger(s.committed_offset)&&s.committed_offset>=0)||!Number.isSafeInteger(s.log_end_offset)||s.log_end_offset<0)issue('group_ownership_or_membership_unverified');
  emit('group_state',{case_id:caseId,group_id:group,topic,stage,committed_offset:s.committed_offset,log_end_offset:s.log_end_offset,members:0,membership_observed:true});
  return {committed_offset:s.committed_offset,log_end_offset:s.log_end_offset};
 }
 function reserve(r){
  if(reserved.has(r.id)||reservedBytes+r.value_bytes>plan.maximum_logical_value_bytes||r.value_bytes!==1000)issue('record_budget','FAIL');
  const known=recordStates.get(r.id);if(!known)issue('unknown_record','FAIL');
  reserved.add(r.id);reservedBytes+=r.value_bytes;return known;
 }
 function send(h,r,isNegative=false){
  budget(plan.call_reserve_ms+3*plan.sink_reserve_ms);
  const s=reserve(r),body=valueFor(plan.seed,r,sha);
  if(body.length!==1000||sha(body)!==r.checksum)issue('generated_payload_mismatch','FAIL');
  const intent=emit('attempt_intent',{case_id:r.case_id,id:r.id,topic:r.topic,value_bytes:1000,checksum:r.checksum,negative:isNegative});s.intent=true;
  // A persisted intent is not proof of native invocation; recheck after evidence latency.
  try{budget(plan.call_reserve_ms+2*plan.sink_reserve_ms);}catch(e){emit('not_sent',{case_id:r.case_id,id:r.id,reason:'budget_exhausted'});throw e;}
  s.native_called=true;let nativeReturn;
  try{
   nativeReturn=h.client.produce({messages:[{topic:r.topic,key:r.id,value:body}]});
  }catch(error){
   const c=normalizeCategory(error);
   const definitive=c.typed&&['topic_authorization_denied','group_authorization_denied','cluster_authorization_denied'].includes(c.category)&&c.stage==='produce';
   s.outcome=definitive?'definitive_not_accepted':'unresolved';
   emit('send_outcome',{case_id:r.case_id,id:r.id,topic:r.topic,value_bytes:1000,checksum:r.checksum,
    outcome:s.outcome,negative:isNegative,category:c.category,failure_stage:c.stage,typed:c.typed,intent_event_id:intent});
   return {returned:false,classification:c};
  }
  if(nativeReturn!==undefined){s.outcome='unresolved';emit('send_outcome',{case_id:r.case_id,id:r.id,topic:r.topic,value_bytes:1000,checksum:r.checksum,outcome:'unresolved',negative:isNegative,category:'unknown',failure_stage:'produce',typed:false,intent_event_id:intent});return {returned:false,classification:{category:'unknown',stage:'produce',typed:false}};}
  const ackAt=now();s.outcome='acknowledged';s.ack_bucket=ackAt<=positiveCutoff?'on_time':'late';
  emit('send_outcome',{case_id:r.case_id,id:r.id,topic:r.topic,value_bytes:1000,checksum:r.checksum,outcome:'acknowledged',
   negative:isNegative,acknowledged_at_ms:ackAt,ack_bucket:s.ack_bucket,intent_event_id:intent});
  if(isNegative)issue('unexpected_negative_ack','FAIL');
  if(s.ack_bucket==='late')issue('late_positive_ack');
  return {returned:true};
 }
 function positive(p){
  emit('case_start',{case_id:p.id,case_kind:'positive',topic:p.topic,group_id:p.group});
  const initial=state(p.group,p.topic,p.id,'before');
  if(!((initial.committed_offset===null&&initial.log_end_offset===0)||initial.committed_offset===initial.log_end_offset))issue('unprocessed_initial_records');
  let consumer,producer;
  try{
   consumer=make('consumer',p.consumer,p.topic,p.group,p.id);
   if(consumer.client.position()!==initial.log_end_offset)issue('initial_position_unverified');
   producer=make('producer',p.producer,p.topic,null,p.id);
   const expected=plan.records.filter(r=>r.case_id===p.id),seen=new Set();
   for(const r of expected){const sent=send(producer,r);if(!sent.returned)issue('positive_send_unresolved');}
   let nextOffset=initial.log_end_offset;
   while(seen.size<expected.length){
    budget(plan.call_reserve_ms+5*plan.sink_reserve_ms);
    const messages=consumer.client.consume({limit:1});
    if(!Array.isArray(messages)||messages.length>1)issue('consume_shape','FAIL');
    for(const m of messages){
     const key=ascii(m.key??''),r=expected.find(x=>x.id===key),body=ascii(m.value??''),digest=sha(body);
     if(!r){emit('unexpected_record',{case_id:p.id,topic:p.topic,observed_key_sha256:sha(key),checksum:digest});issue('foreign_or_unplanned_record','FAIL');}
     if(m.topic!==p.topic||m.partition!==0||!Number.isSafeInteger(m.offset)||m.offset!==nextOffset||body.length!==1000||digest!==r.checksum||seen.has(r.id))issue('receipt_integrity_or_offset_gap','FAIL');
     const s=recordStates.get(r.id);s.presence=true;
     const receipt=emit('receipt',{case_id:p.id,id:r.id,topic:p.topic,group_id:p.group,value_bytes:1000,checksum:digest,partition:0,offset:m.offset});s.receipt=true;
     const durable=emit('durable_receipt_ack',{case_id:p.id,id:r.id,receipt_event_id:receipt});
     budget(plan.call_reserve_ms+3*plan.sink_reserve_ms);
     emit('commit_intent',{case_id:p.id,id:r.id,receipt_event_id:receipt,durable_event_id:durable,group_id:p.group,partition:0,offset:m.offset});
     // Recheck immediately before the native offset mutation after sink latency.
     budget(plan.call_reserve_ms+2*plan.sink_reserve_ms);
     try{const result=consumer.client.commitOffsets();if(result!==undefined)throw new Error('commit_return_unverified');}catch(_){s.commit_uncertain=true;emit('commit_uncertain',{case_id:p.id,id:r.id,receipt_event_id:receipt});issue('commit_result_unresolved');}
     s.committed=true;emit('commit',{case_id:p.id,id:r.id,receipt_event_id:receipt,durable_event_id:durable,group_id:p.group,partition:0,offset:m.offset});
     nextOffset++;seen.add(r.id);
    }
    if(!messages.length)deps.sleep(0.05);
   }
   close(consumer);close(producer);
   const final=state(p.group,p.topic,p.id,'after');
   if(final.committed_offset!==nextOffset)issue('committed_offset_unverified');
   budget();cases[p.id]='PASS';controls.add(p.id);emit('case_end',{case_id:p.id,status:'PASS'});
  }finally{
   for(const h of [consumer,producer])if(h&&!h.closed){try{close(h);}catch(_){/* Global cleanup reports remaining handles. */}}
  }
 }
 function negative(n){
  if(!n.control_ids.every(id=>controls.has(id)))issue('positive_control_missing');
  if(handles.some(h=>h.role==='consumer'&&!h.closed))issue('positive_consumer_still_open');
  emit('case_start',{case_id:n.id,case_kind:'negative',binding_id:n.binding,topic:n.topic,group_id:n.group,control_ids:n.control_ids});
  let h,before=null,classification=null,finalStateRecorded=false;
  try{
   if(n.operation==='produce'){
    h=make('producer',n.binding,n.topic,null,n.id);
    classification=send(h,n.record,true).classification;
   }else{
    before=state(n.group,n.topic,n.id,'before');h=make('consumer',n.binding,n.topic,n.group,n.id);
    const membership=h.client.probeState?.();
    if(membership?.broker_confirmed===true&&((n.expected_category==='group_authorization_denied'&&membership.group_joined===true)||membership.forbidden_assignment===true)){
     emit('unexpected_access',{case_id:n.id,topic:n.topic,group_id:n.group,kind:'broker_confirmed_membership'});issue('unexpected_negative_join','FAIL');
    }
    budget(plan.call_reserve_ms+2*plan.sink_reserve_ms);
    try{
     const messages=h.client.consume({limit:1});
     if(!Array.isArray(messages))issue('consume_shape','FAIL');
     if(messages.length){const known=recordStates.get(ascii(messages[0].key??''));if(known)known.presence=true;emit('unexpected_access',{case_id:n.id,topic:n.topic,group_id:n.group,kind:'record',observed_key_sha256:sha(ascii(messages[0].key??'')),checksum:sha(ascii(messages[0].value??''))});issue('unexpected_negative_record','FAIL');}
     classification={category:'unknown',stage:'consume',typed:false};
    }catch(error){if(error&&typeof error==='object'&&internalFailures.has(error))throw error;classification=normalizeCategory(error);}
   }
   close(h);
   if(before){const after=state(n.group,n.topic,n.id,'after');finalStateRecorded=true;if(before.committed_offset!==after.committed_offset)issue('negative_offset_changed','FAIL');}
   const stageOk=n.expected_stages.includes(classification?.stage);
   const pass=classification?.typed===true&&classification.category===n.expected_category&&stageOk;
   budget(plan.sink_reserve_ms);
   cases[n.id]=pass?'PASS':'INCONCLUSIVE';
   emit('denial_result',{case_id:n.id,status:cases[n.id],category:classification?.category??'unknown',failure_stage:classification?.stage??'unknown',typed:classification?.typed===true,control_ids:n.control_ids,write_path_control_id:n.write_path_control_id,consumer_write_confounded:n.consumer_write_confounded,confound_resolution:n.consumer_write_confounded&&pass?'topic_stage_observed_in_stub':(n.consumer_write_confounded?'not_resolved':'not_applicable')});
   if(!pass)issue(n.consumer_write_confounded?'consumer_write_layer_confounded':'negative_failure_layer_unproven');
  }finally{
   if(h&&!h.closed){try{close(h);}catch(_){/* Do not overwrite original failure with cleanup. */}}
   if(before&&!finalStateRecorded){try{const after=state(n.group,n.topic,n.id,'after_stop');if(before.committed_offset!==after.committed_offset)emit('negative_state_violation',{case_id:n.id,reason:'offset_changed'});}catch(_){try{emit('group_state_unavailable',{case_id:n.id,topic:n.topic,group_id:n.group,reason:'final_snapshot_unavailable'});}catch(_){/* Missing external evidence remains incomplete. */}}}
   // No negative code path calls commitOffsets; selected native auto-commit behavior remains unverified.
  }
 }
 function summary(){
  const values=[...recordStates.values()],positiveIds=new Set(plan.records.map(r=>r.id));
  return {planned_original_value_bytes:20000,reserved_original_value_bytes:reservedBytes,
   attempted_unique_value_bytes:values.filter(s=>s.native_called).length*1000,
   acknowledged_value_bytes:values.filter(s=>s.outcome==='acknowledged').length*1000,
   positive_acknowledged_value_bytes_on_time:values.filter(s=>positiveIds.has(s.id)&&s.outcome==='acknowledged'&&s.ack_bucket==='on_time').length*1000,
   late_acknowledged_value_bytes:values.filter(s=>s.outcome==='acknowledged'&&s.ack_bucket==='late').length*1000,
   definitive_not_accepted_value_bytes:values.filter(s=>s.outcome==='definitive_not_accepted').length*1000,
   unresolved_value_bytes:values.filter(s=>s.outcome==='unresolved').length*1000,
   not_attempted_value_bytes:values.filter(s=>!s.native_called).length*1000};
 }
 try{
  if(deps.debugConfigurationSafe?.()!==true||deps.capabilities?.manual_commit_only!==true||deps.capabilities?.no_commit_on_close!==true)issue('unsafe_client_or_debug_configuration');
  emit('fixture_start',{job_start_ms:start,deadline_ms:end,mode:'OFFLINE_STUB_ONLY'});
  for(const b of plan.bindings){budget(plan.sink_reserve_ms);const password=deps.readCredential(b.password_path);if(typeof password!=='string'||!password.trim())issue('credential_unavailable');credentials.set(b.id,password);}
  const ca=deps.readPublicCA('/credentials/cluster-ca/ca.crt');if(typeof ca!=='string'||!ca.trim())issue('trust_unavailable');credentials.set('public_ca',ca);
  budget();enter('positive_delivery');for(const p of plan.positive)positive(p);
  enter('negative_cases');for(const n of plan.negative)negative(n);
  enter('final_reconciliation_drain');budget();emit('reconciliation_boundary',{positive_cutoff_ms:positiveCutoff});budget();
 }catch(e){
  const own=e&&typeof e==='object'?internalFailures.get(e):null;
  firstFailure=firstFailure??own??{reason:'client_or_evidence_failure',severity:'INCONCLUSIVE'};
  const active=events.filter(e=>e.type==='case_start').at(-1)?.case_id;
  if(active&&cases[active]==='SKIPPED')cases[active]=firstFailure.severity;
  try{emit('fixture_stop',{reason:firstFailure.reason,status:firstFailure.severity});}catch(_){/* Unpersisted evidence keeps collection incomplete. */}
 }finally{
  phase='shutdown';phaseEnd=end;
  for(const h of handles)if(!h.closed){try{close(h);}catch(_){closed=false;}}
  credentials.clear();
 }
 const stats=summary(),duration=now()-start;
 if(duration>300000)firstFailure=firstFailure??{reason:'total_deadline_exceeded',severity:'INCONCLUSIVE'};
 const allCases=Object.values(cases).every(s=>s==='PASS');
 let terminal=false;
 try{emit('fixture_terminal',{status:!firstFailure&&allCases&&closed?'OFFLINE_COMPLETE':'OFFLINE_INCOMPLETE',elapsed_ms:duration,closed:closed&&handles.every(h=>h.closed),positive_cutoff_ms:positiveCutoff});terminal=true;}catch(_){firstFailure=firstFailure??{reason:'terminal_evidence_missing',severity:'INCONCLUSIVE'};}
 const finalDuration=now()-start;if(finalDuration>300000)firstFailure=firstFailure??{reason:'total_deadline_exceeded',severity:'INCONCLUSIVE'};
 const exportText=envelopes.map(x=>JSON.stringify(x)+'\n').join('');
 return {mode:'OFFLINE_STUB_ONLY',run_id:plan.run_id,plan_sha256:planHash,live_execution:false,
  status:!firstFailure&&allCases&&closed&&terminal?'OFFLINE_COMPLETE':'OFFLINE_INCOMPLETE',
  first_failure:firstFailure,cases,stats,records:[...recordStates.values()],duration_ms:finalDuration,
  cleanup_complete:closed&&handles.every(h=>h.closed),terminal_recorded:terminal,unpersisted,events,envelopes,
  seal:{mode:'OFFLINE_STUB_ONLY',run_id:plan.run_id,plan_sha256:planHash,complete:terminal&&unpersisted.length===0&&finalDuration<=300000,observed_job_elapsed_ms:finalDuration,export_sha256:sha(exportText),event_count:envelopes.length,client_event_counts:{tenant_fixture:envelopes.length}}};
}
