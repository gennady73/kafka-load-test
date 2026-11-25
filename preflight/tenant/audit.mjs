// Offline evidence checker, separate from adapter control flow. No Kafka, native-error or engine imports.
import {canonical,validatePlan,SAFE_REASONS,SAFE_SEVERITIES} from './policy.mjs';
const common=['version','run_id','plan_sha256','client_id','event_id','type','at_ms','previous_event_sha256'];
const fields={fixture_start:['job_start_ms','deadline_ms','mode'],phase_start:['phase','deadline_ms','positive_cutoff_ms'],
 case_start:['case_id','case_kind','topic','group_id','binding_id','control_ids'],group_state:['case_id','group_id','topic','stage','committed_offset','log_end_offset','members','membership_observed'],
 client_open:['case_id','binding_id','client_role','topic','group_id','auto_commit','commit_on_close','write_settings_signature'],
 client_close:['case_id','binding_id','client_role','closed','within_reserve'],close_skipped:['case_id','binding_id','reason'],
 attempt_intent:['case_id','id','topic','value_bytes','checksum','negative'],not_sent:['case_id','id','reason'],
 send_outcome:['case_id','id','topic','value_bytes','checksum','outcome','negative','category','failure_stage','typed','intent_event_id','acknowledged_at_ms','ack_bucket'],
 receipt:['case_id','id','topic','group_id','value_bytes','checksum','partition','offset'],durable_receipt_ack:['case_id','id','receipt_event_id'],
 commit_intent:['case_id','id','receipt_event_id','durable_event_id','group_id','partition','offset'],
 commit:['case_id','id','receipt_event_id','durable_event_id','group_id','partition','offset'],commit_uncertain:['case_id','id','receipt_event_id'],
 case_end:['case_id','status'],denial_result:['case_id','status','category','failure_stage','typed','control_ids','write_path_control_id','consumer_write_confounded','confound_resolution'],
 unexpected_record:['case_id','topic','observed_key_sha256','checksum'],unexpected_access:['case_id','topic','group_id','kind','observed_key_sha256','checksum'],
 group_state_unavailable:['case_id','topic','group_id','reason'],negative_state_violation:['case_id','reason'],reconciliation_boundary:['positive_cutoff_ms'],fixture_stop:['reason','status'],
 fixture_terminal:['status','elapsed_ms','closed','positive_cutoff_ms']};
export function auditFixture(plan,envelopes,seal,sha){
 validatePlan(plan);const issues=[],parsed=[],hash=sha(canonical(plan));
 const problem=(code,case_id=null)=>issues.push({code,...(case_id?{case_id}:{})});
 let previous=null,lastTime=-Infinity;
 const eventIds=new Set();
 for(const [i,x] of envelopes.entries()){
  if(!x||Object.keys(x).sort().join(',')!=='event_json,event_sha256'||typeof x.event_json!=='string'||sha(x.event_json)!==x.event_sha256){problem('envelope_hash_or_shape');continue;}
  let e;try{e=JSON.parse(x.event_json);}catch(_){problem('event_json');continue;}
  if(!fields[e.type]||Object.keys(e).some(k=>![...common,...(fields[e.type]??[])].includes(k))){problem('event_field_allowlist');continue;}
  if(e.version!==1||e.run_id!==plan.run_id||e.plan_sha256!==hash||e.client_id!=='tenant_fixture'||e.event_id!==`${plan.run_id}/tenant_fixture/${i}`||eventIds.has(e.event_id))problem('event_identity_or_sequence');
  if(e.previous_event_sha256!==previous)problem('event_chain');
  if(!Number.isFinite(e.at_ms)||e.at_ms<lastTime)problem('event_time');
  previous=x.event_sha256;lastTime=e.at_ms;eventIds.add(e.event_id);parsed.push(e);
 }
 const cases=new Map([...plan.positive,...plan.negative].map(p=>[p.id,p]));
 const states=new Map([...plan.records,...plan.negative.filter(n=>n.record).map(n=>n.record)].map(r=>[r.id,{expected:r,intent:null,outcome:null,not_sent:false,present:false,receipt:null,durable:null,commit_intent:null,commit:false,commit_uncertain:false}]));
 const positiveCases=new Set(),negativeCases=new Set(),open=new Set(),opened=new Set(),snapshots=new Map(),caseStarts=new Set(),lastOffsets=new Map();
 const phaseDeadlines=new Map();let phaseIndex=0,priorDeadline=null;let cutoff=null,start=null,terminal=null,unexpected=false,integrityFailure=false,cleanupFailure=false,stopped=false;
 const recordCheck=e=>{
  const s=states.get(e.id);
  if(!s){problem('unknown_record',e.case_id);return null;}
  const r=s.expected;
  if(e.case_id!==r.case_id||e.topic!==r.topic||e.value_bytes!==1000||e.checksum!==r.checksum){problem('record_identity_bytes_or_checksum',e.case_id);integrityFailure=true;return null;}
  return s;
 };
 for(const e of parsed){
  const c=e.case_id?cases.get(e.case_id):null;
  if(e.case_id&&!c){problem('unknown_case');continue;}
  if(e.type==='fixture_start'){
   if(start!==null||e.deadline_ms!==e.job_start_ms+300000||e.mode!=='OFFLINE_STUB_ONLY')problem('fixture_start');start=e.job_start_ms;priorDeadline=start+45000;
  }else if(e.type==='phase_start'){
   if(e.phase!==['positive_delivery','negative_cases','final_reconciliation_drain'][phaseIndex++]||e.at_ms>priorDeadline)problem('phase_order_or_overrun');priorDeadline=e.deadline_ms;
   if(!Object.hasOwn(plan.phase_seconds,e.phase)||!Number.isFinite(e.deadline_ms)||e.deadline_ms>e.at_ms+plan.phase_seconds[e.phase]*1000||e.deadline_ms>start+300000)problem('phase_deadline');
   phaseDeadlines.set(e.phase,e.deadline_ms);if(e.phase==='positive_delivery')cutoff=e.positive_cutoff_ms;
  }else if(e.type==='case_start'){
   if(caseStarts.has(e.case_id))problem('duplicate_case',e.case_id);caseStarts.add(e.case_id);
   if(e.case_kind==='negative'){
    if(open.size)problem('positive_or_prior_client_not_closed',e.case_id);
    if(canonical(e.control_ids)!==canonical(c.control_ids)||!c.control_ids.every(id=>positiveCases.has(id)))problem('positive_controls_missing',e.case_id);
   }
  }else if(e.type==='client_open'){
   const k=e.case_id+'|'+e.binding_id+'|'+e.client_role;
   if(!['producer','consumer'].includes(e.client_role)||opened.has(k))problem('client_role_or_duplicate',e.case_id);opened.add(k);
   const positive=plan.positive.find(p=>p.id===e.case_id);
   if(!positive&&e.client_role!==(c.operation==='produce'?'producer':'consumer'))problem('negative_client_role',e.case_id);
   const expected=positive?(e.client_role==='producer'?positive.producer:positive.consumer):c.binding;
   if(e.binding_id!==expected||e.topic!==c.topic||e.group_id!==(e.client_role==='consumer'?c.group:null)||e.auto_commit!==false||e.commit_on_close!==false||e.write_settings_signature!==(e.client_role==='producer'?plan.write_settings_signature:null)||open.has(k))problem('client_binding_or_commit_policy',e.case_id);
   open.add(k);
  }else if(e.type==='client_close'){
   const k=e.case_id+'|'+e.binding_id+'|'+e.client_role;
   if(!open.has(k)||e.closed!==true||e.within_reserve!==true){problem('client_close',e.case_id);cleanupFailure=true;}
   if(e.closed)open.delete(k);
  }else if(e.type==='close_skipped'){cleanupFailure=true;problem('close_skipped',e.case_id);
  }else if(e.type==='group_state'){
   if(!['before','after','after_stop'].includes(e.stage)||e.topic!==c.topic||e.group_id!==c.group||e.members!==0||e.membership_observed!==true)problem('group_state',e.case_id);
   const k=e.case_id;if(e.stage==='before'){
    if(snapshots.has(k))problem('duplicate_group_baseline',e.case_id);snapshots.set(k,{before:e});
    if(plan.positive.some(p=>p.id===e.case_id)){if(!((e.committed_offset===null&&e.log_end_offset===0)||e.committed_offset===e.log_end_offset))problem('unprocessed_initial_records',k);lastOffsets.set(k,e.log_end_offset-1);}
   }else{
    const s=snapshots.get(k);if(!s)problem('missing_group_baseline',e.case_id);else{
     s.after=e;
     if(plan.negative.some(n=>n.id===k)&&s.before.committed_offset!==e.committed_offset){problem('negative_offset_changed',k);unexpected=true;}
     if(plan.positive.some(p=>p.id===k)&&e.committed_offset!==lastOffsets.get(k)+1)problem('positive_final_offset',k);
    }
   }
  }else if(e.type==='attempt_intent'){
   const s=recordCheck(e);if(!s)continue;if(s.intent)problem('duplicate_attempt',e.case_id);s.intent=e;
   const negative=plan.negative.some(n=>n.id===e.case_id);
   if(e.negative!==negative||stopped||!phaseDeadlines.has(negative?'negative_cases':'positive_delivery')||e.at_ms>=phaseDeadlines.get(negative?'negative_cases':'positive_delivery'))problem('send_outside_phase_or_after_stop',e.case_id);
  }else if(e.type==='not_sent'){
   const s=states.get(e.id);if(!s||!s.intent||s.outcome)problem('not_sent_state',e.case_id);else s.not_sent=true;
  }else if(e.type==='send_outcome'){
   const s=recordCheck(e);if(!s)continue;
   if(!s.intent||s.outcome||s.not_sent||s.intent.event_id!==e.intent_event_id)problem('outcome_without_unique_intent',e.case_id);
   s.outcome=e;
   if(e.at_ms>phaseDeadlines.get(plan.negative.some(n=>n.id===e.case_id)?'negative_cases':'positive_delivery'))problem('returned_native_phase_overrun',e.case_id);
   const negative=plan.negative.some(n=>n.id===e.case_id);
   if(e.negative!==negative)problem('outcome_case_kind',e.case_id);
   if(e.outcome==='acknowledged'){
    if(!Number.isFinite(e.acknowledged_at_ms)||e.acknowledged_at_ms>e.at_ms||e.ack_bucket!==(e.acknowledged_at_ms<=cutoff?'on_time':'late'))problem('ack_time_bucket',e.case_id);
    if(negative){unexpected=true;problem('negative_acknowledgement',e.case_id);}
    if(!negative&&e.acknowledged_at_ms>cutoff)problem('late_positive_ack',e.case_id);
   }else if(e.outcome==='definitive_not_accepted'){
    if(e.typed!==true||!['topic_authorization_denied','group_authorization_denied','cluster_authorization_denied'].includes(e.category)||e.failure_stage!=='produce')problem('unproven_definitive_rejection',e.case_id);
   }else if(e.outcome!=='unresolved')problem('unknown_outcome',e.case_id);
  }else if(e.type==='receipt'){
   const s=recordCheck(e);if(!s)continue;s.present=true;
   if(plan.negative.some(n=>n.id===e.case_id)){unexpected=true;problem('negative_receipt',e.case_id);continue;}
   if(s.receipt||e.group_id!==c.group||e.partition!==0||!Number.isSafeInteger(e.offset)||e.offset!==lastOffsets.get(e.case_id)+1){problem('receipt_duplicate_group_partition_or_gap',e.case_id);integrityFailure=true;}
   s.receipt=e;lastOffsets.set(e.case_id,e.offset);
  }else if(e.type==='durable_receipt_ack'){
   const s=states.get(e.id);if(!s||!s.receipt||s.durable||s.receipt.event_id!==e.receipt_event_id)problem('durable_ack_correlation',e.case_id);else s.durable=e;
  }else if(e.type==='commit_intent'||e.type==='commit'){
   const s=states.get(e.id);
   if(!s||!s.receipt||!s.durable||s.receipt.event_id!==e.receipt_event_id||s.durable.event_id!==e.durable_event_id||s.receipt.offset!==e.offset||e.partition!==0||e.group_id!==c.group||plan.negative.some(n=>n.id===e.case_id))problem('commit_before_durable_or_wrong_link',e.case_id);
   if(s){if(e.type==='commit_intent'){if(s.commit_intent)problem('duplicate_commit_intent',e.case_id);s.commit_intent=e;}
    else {if(!s.commit_intent||s.commit||s.commit_uncertain)problem('commit_result_conflict',e.case_id);s.commit=true;}}
  }else if(e.type==='commit_uncertain'){
   const s=states.get(e.id);if(!s||!s.commit_intent||s.commit)problem('commit_uncertainty_state',e.case_id);else s.commit_uncertain=true;
  }else if(e.type==='case_end'){
   const selected=[...states.values()].filter(s=>s.expected.case_id===e.case_id);
   if(!caseStarts.has(e.case_id)||positiveCases.has(e.case_id)||e.status!=='PASS'||selected.length!==4||!selected.every(s=>s.outcome?.outcome==='acknowledged'&&s.outcome.ack_bucket==='on_time'&&s.receipt&&s.durable&&s.commit&&!s.commit_uncertain)||snapshots.get(e.case_id)?.after?.stage!=='after')problem('positive_case_incomplete',e.case_id);
   else positiveCases.add(e.case_id);
  }else if(e.type==='denial_result'){
   const n=plan.negative.find(n=>n.id===e.case_id);
   const expectedStages=n?.expected_stages??[];
   if(!caseStarts.has(e.case_id)||!n||e.write_path_control_id!==n.write_path_control_id||e.consumer_write_confounded!==n.consumer_write_confounded||(n.consumer_write_confounded&&e.confound_resolution!=='topic_stage_observed_in_stub')||negativeCases.has(e.case_id)||e.status!=='PASS'||e.typed!==true||e.category!==n.expected_category||!expectedStages.includes(e.failure_stage)||canonical(e.control_ids)!==canonical(n.control_ids)||!n.control_ids.every(id=>positiveCases.has(id)))problem('negative_denial_unproven',e.case_id);
   else {
    if(n.record&&states.get(n.record.id)?.outcome?.outcome!=='definitive_not_accepted')problem('negative_write_rejection_missing',n.id);
    if(!n.record&&snapshots.get(n.id)?.after?.stage!=='after')problem('negative_membership_snapshot_missing',n.id);
    negativeCases.add(n.id);
   }
  }else if(e.type==='group_state_unavailable'){problem('negative_final_state_unavailable',e.case_id);
  }else if(['unexpected_record','unexpected_access','negative_state_violation'].includes(e.type)){unexpected=true;problem(e.type,e.case_id);
  }else if(e.type==='fixture_stop'){if(!SAFE_REASONS.includes(e.reason)||!SAFE_SEVERITIES.includes(e.status))problem('unsafe_reason_or_severity');stopped=true;problem('fixture_stopped');
  }else if(e.type==='fixture_terminal'){
   if(terminal)problem('duplicate_terminal');terminal=e;
   if(e.status!=='OFFLINE_COMPLETE'||e.closed!==true||e.positive_cutoff_ms!==cutoff||e.elapsed_ms!==e.at_ms-start)problem('terminal_incomplete');
  }
 }
 if(!terminal||parsed.at(-1)!==terminal||open.size||opened.size!==16){problem('missing_terminal_or_open_clients');cleanupFailure=true;}
 if(phaseIndex!==3)problem('required_phase_denominator');
 if(positiveCases.size!==4||negativeCases.size!==8)problem('required_case_denominator');
 const all=[...states.values()],unsettled=all.filter(s=>s.intent&&!s.not_sent&&!s.outcome);
 const sum=f=>all.filter(f).length*1000;
 const stats={attempt_intent_value_bytes:sum(s=>!!s.intent),acknowledged_value_bytes:sum(s=>s.outcome?.outcome==='acknowledged'),
  positive_acknowledged_value_bytes_on_time:sum(s=>plan.records.some(r=>r.id===s.expected.id)&&s.outcome?.outcome==='acknowledged'&&s.outcome.ack_bucket==='on_time'),
  late_acknowledged_value_bytes:sum(s=>s.outcome?.outcome==='acknowledged'&&s.outcome.ack_bucket==='late'),
  definitive_not_accepted_value_bytes:sum(s=>s.outcome?.outcome==='definitive_not_accepted'),
  unresolved_value_bytes:sum(s=>s.outcome?.outcome==='unresolved')+unsettled.length*1000,
  not_attempted_value_bytes:sum(s=>!s.intent||s.not_sent),present_without_producer_ack_value_bytes:sum(s=>s.present&&s.outcome?.outcome!=='acknowledged'),
  confirmed_commit_records:all.filter(s=>s.commit).length,uncertain_commit_records:all.filter(s=>s.commit_uncertain).length};
 if(all.some(s=>s.present&&s.outcome?.outcome==='definitive_not_accepted'))problem('definitive_rejection_conflicts_with_receipt');
 if(stats.positive_acknowledged_value_bytes_on_time!==16000||stats.acknowledged_value_bytes+stats.definitive_not_accepted_value_bytes+stats.unresolved_value_bytes+stats.not_attempted_value_bytes!==20000||stats.unresolved_value_bytes||stats.uncertain_commit_records)problem('record_outcome_accounting_incomplete');
 if(stats.attempt_intent_value_bytes>20000)problem('byte_budget_exceeded');
 const exportText=envelopes.map(x=>JSON.stringify(x)+'\n').join('');
 const evidenceComplete=seal?.mode==='OFFLINE_STUB_ONLY'&&seal.run_id===plan.run_id&&seal.plan_sha256===hash&&seal.complete===true&&seal.event_count===envelopes.length&&seal.client_event_counts?.tenant_fixture===envelopes.length&&seal.export_sha256===sha(exportText);
 if(!evidenceComplete)problem('export_or_seal_incomplete');
 const duration=seal?.observed_job_elapsed_ms;
 const durationOk=Number.isFinite(duration)&&duration>=terminal?.elapsed_ms&&duration<=300000&&!issues.some(i=>/phase.*overrun|late_positive_ack/.test(i.code));
 if(!durationOk)problem('duration_unverified_or_exceeded');
 return {mode:'OFFLINE_STUB_ONLY',live_execution:false,run_id:plan.run_id,status:issues.length?'OFFLINE_INCOMPLETE':'OFFLINE_COMPLETE',
  dimensions:{duration:durationOk?'MET_IN_STUB':'INCOMPLETE',budget:stats.attempt_intent_value_bytes<=20000?'WITHIN_LOGICAL_LIMIT':'EXCEEDED',
   accounting:issues.some(x=>/accounting|commit|record|receipt|outcome|intent/.test(x.code))?'INCOMPLETE':'MATCHED_STUB_RECORDS',
   isolation:unexpected?'FAILED':positiveCases.size===4&&negativeCases.size===8?'MINIMAL_STUB_MATRIX_MATCHED':'INCONCLUSIVE',
   evidence:evidenceComplete&&!issues.some(x=>/event|envelope|durable|seal/.test(x.code))?'COMPLETE_STUB_EXPORT':'INCOMPLETE',
   cleanup:cleanupFailure?'INCOMPLETE':'COMPLETE_IN_STUB',integrity:integrityFailure?'FAILED':'STUB_ONLY'},
  required_positive_cases:4,matched_positive_cases:positiveCases.size,required_negative_cases:8,matched_negative_cases:negativeCases.size,
  stats,issues,runtime_acceptance:'NOT_ASSESSED',production_acceptance:'NOT_ASSESSED'};
}
