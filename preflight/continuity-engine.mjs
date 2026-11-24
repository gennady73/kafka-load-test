// Dependency-injected continuity lifecycle. This file never imports Kafka or HTTP.
import {validatePlan,recordId,dueMilliseconds,valueFor,receiptFor,durableAck,gapExceeded} from './continuity-core.mjs';
import {ascii} from './core.mjs';

export function makeSession(plan,deps,clientId,epoch,planHash){
 validatePlan(plan,true);
 if(!plan.clients.includes(clientId)||!Number.isSafeInteger(epoch)||!Number.isFinite(deps.now()))throw Error('invalid session identity/epoch');
 const role=clientId.split('_')[0],stream=plan.streams[Number(clientId.split('_')[1])];
 const end=epoch+plan.duration_seconds*1000,finalEnd=end+plan.drain_limit_seconds*1000;
 const stopAt=finalEnd+plan.shutdown_limit_seconds*1000;
 let serial=0,nextHeartbeat=deps.now()+plan.heartbeat_seconds*1000,lastSuccess=epoch,failed=false,reason=null;
 let attempts=0,acks=0,receipts=0,commits=0,foreign=0,client,closed=false,ready=false;
 const seen=new Set();let lastOffset=-1;
 function issue(code){const e=new Error(code);e.continuityCode=code;throw e;}
 function now(){const n=deps.now();if(deps.clockHealthy&&!deps.clockHealthy())issue('clock_drift');if(n>stopAt)issue('shutdown_deadline');return n;}
 function emit(type,fields={}){
  const event={...fields,version:1,event_id:`${plan.run_id}/${clientId}/${serial++}`,type,run_id:plan.run_id,
   client_id:clientId,topic:stream.name,group_id:stream.group_id,at_ms:now(),epoch_ms:epoch,plan_sha256:planHash};
  const eventJSON=JSON.stringify(event),hash=deps.sha256(eventJSON);
  durableAck(deps.post(JSON.stringify({event_json:eventJSON,event_sha256:hash}),plan.collector_timeout_seconds),event,hash);
  deps.log({...event,event_sha256:hash,collector_acknowledged:true});return event.event_id;
 }
 function progress(){
  const n=now();
  if(n<end&&gapExceeded(n,epoch,lastSuccess,role==='producer'?plan.max_ack_gap_seconds:plan.max_receipt_gap_seconds))issue(role==='producer'?'ack_gap':'receipt_gap');
  if(n>=nextHeartbeat){emit('heartbeat',{attempts,acks,receipts,commits,foreign,last_success_ms:lastSuccess});nextHeartbeat=now()+plan.heartbeat_seconds*1000;}
 }
 function wait(until){while(now()<until){progress();deps.sleep(Math.min(1,(until-now())/1000));}}
 function consumerMessages(messages,beforeEpoch=false){
  for(const message of messages){
   const readAt=now(),r=receiptFor(plan,stream,message,deps.sha256,ascii);
   if(!r){foreign++;continue;}
   if(beforeEpoch||readAt<epoch)issue('current_run_record_before_epoch');
   if(seen.has(r.id))issue('duplicate_receipt');
   if(r.offset<=lastOffset)issue('nonincreasing_offset');
   if(readAt-(epoch+dueMilliseconds(plan,stream,r.sequence))>plan.max_receive_latency_seconds*1000)issue('receive_latency');
   const evidenceId=emit('receipt',{...r,received_at_ms:readAt}); // durable ACK is checked before any commit
   seen.add(r.id);lastOffset=r.offset;receipts++;lastSuccess=readAt;
   client.commitOffsets();commits++;emit('commit',{id:r.id,receipt_event_id:evidenceId,partition:r.partition,offset:r.offset});
  }
 }
 function runProducer(){
  client=deps.makeProducer(stream);emit('client_ready');ready=true;
  while(now()<epoch){
   const status=deps.readiness();
   if(status&&status.run_id===plan.run_id&&status.plan_sha256===planHash&&status.durable===true&&Array.isArray(status.ready_clients)&&plan.clients.every(id=>status.ready_clients.includes(id))){wait(epoch);break;}
   deps.sleep(0.25);progress();
  }
  // Recheck the barrier: epoch expiry cannot silently bypass missing consumers.
  const status=deps.readiness();
  if(!status||status.run_id!==plan.run_id||status.plan_sha256!==planHash||status.durable!==true||!Array.isArray(status.ready_clients)||!plan.clients.every(id=>status.ready_clients.includes(id)))issue('readiness_barrier');
  for(let sequence=0;sequence<stream.record_count;sequence++){
   const due=epoch+dueMilliseconds(plan,stream,sequence);wait(due);
   if(now()>=end||now()-due>plan.max_start_lateness_seconds*1000)issue('send_schedule');
   const id=recordId(plan,stream,sequence),value=valueFor(plan,stream,sequence,deps.sha256),checksum=deps.sha256(value);
   emit('attempt',{id,sequence,value_bytes:1000,checksum,due_ms:due});
   if(now()>=end||now()-due>plan.max_start_lateness_seconds*1000)issue('send_schedule');
   attempts++;
   try{client.produce({messages:[{topic:stream.name,key:id,value}]});}
   catch(_){emit('unresolved',{id,sequence,value_bytes:1000,checksum});issue('produce_unresolved');}
   const ackAt=now();acks++;lastSuccess=ackAt;
   emit('acknowledged',{id,sequence,value_bytes:1000,checksum,acknowledged_at_ms:ackAt,boundary:ackAt<=end?'generation':'late'});
   if(ackAt>end)issue('late_acknowledgement');progress();
  }
  wait(end);emit('generation_end',{attempts,acks,value_bytes:acks*1000});
 }
 function runConsumer(){
  client=deps.makeConsumer(stream);
  if(typeof client.position!=='function'||typeof client.commitOffsets!=='function')issue('consumer_api');
  // Prove a usable partition position before announcing readiness. A constructor is not assignment evidence.
  while(now()<epoch){
   consumerMessages(client.consume({limit:1,expectTimeout:true}),true);
   let position=-1;try{position=client.position(0);}catch(_){/* Assignment may still be pending; the setup deadline remains fixed. */}
   if(Number.isSafeInteger(position)&&position>=0){emit('client_ready',{initial_position:position});ready=true;break;}
   progress();deps.sleep(0.05);
  }
  if(!ready)issue('consumer_assignment_deadline');
  while(now()<finalEnd){progress();consumerMessages(client.consume({limit:1,expectTimeout:true}));progress();}
  if(receipts!==stream.record_count||commits!==stream.record_count)issue('missing_receipts_or_commits');
  emit('consumer_drained',{receipts,commits,value_bytes:receipts*1000});
 }
 function run(){
  try{
   if(now()>=epoch)issue('setup_deadline');
   emit('client_start',{role,measurement_end_ms:end,drain_end_ms:finalEnd,shutdown_end_ms:stopAt});
   if(role==='producer')runProducer();else runConsumer();
  }catch(e){
   failed=true;reason=e.continuityCode||'client_or_collector_failure';
   try{emit('client_error',{reason});}catch(_){deps.unpersisted({run_id:plan.run_id,client_id:clientId,reason:'evidence_unavailable'});}
  }finally{
   if(client){
    try{emit('close_start');}catch(_){failed=true;reason=reason||'evidence_unavailable';}
    try{const closeAt=deps.now();client.close();if(deps.now()-closeAt>plan.shutdown_limit_seconds*1000)throw Error('close overran reserve');closed=true;}catch(_){failed=true;reason=reason||'close_failed';}
    try{emit('close_end',{closed});}catch(_){failed=true;reason=reason||'close_or_evidence_deadline';}
   }
  }
  try{emit('client_end',{complete:!failed&&closed&&ready,closed,ready,attempts,acks,receipts,commits,foreign,reason});}
  catch(_){failed=true;reason=reason||'terminal_evidence_missing';deps.unpersisted({run_id:plan.run_id,client_id:clientId,reason});}
  if(failed)deps.abort(reason);
  return {failed,reason,attempts,acks,receipts,commits,closed};
 }
 return {run};
}
