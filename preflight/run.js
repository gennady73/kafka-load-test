// Experimental PF adapter: no topic/schema mutation; execution requires reviewed inputs.
import { sleep } from 'k6';
import exec from 'k6/execution';
import crypto from 'k6/crypto';
import { Counter, Trend } from 'k6/metrics';
import { Producer, Consumer } from 'k6/x/kafka';
import { Random, nextSize, payload, dueSeconds, idFor, ascii } from './core.mjs';

const path=__ENV.PREFLIGHT_PLAN;
if(!path) throw new Error('PREFLIGHT_PLAN required');
const raw=open(path), plan=JSON.parse(raw);
if(plan.plan_version!=='1.0' || plan.algorithm!=='pf-xorshift32-v1' || !plan.execution_ready || plan.fixture)
  throw new Error('requires a reviewed execution-ready non-fixture plan');
const digest=crypto.sha256(raw,'hex');
if(__ENV.PREFLIGHT_PLAN_SHA256!==digest) throw new Error('PREFLIGHT_PLAN_SHA256 mismatch');
// Supply only selected-extension sasl/tls settings through existing Secret injection.
const security=JSON.parse(__ENV.PREFLIGHT_SECURITY_JSON || 'null');
if(!security || Object.keys(security).some(k=>!['sasl','tls'].includes(k)))
  throw new Error('PREFLIGHT_SECURITY_JSON must explicitly contain only selected sasl/tls settings ({} allowed for reviewed plaintext)');
const epoch=Number(__ENV.PREFLIGHT_START_UNIX_MS), waitMs=epoch-Date.now();
if(!Number.isSafeInteger(epoch) || waitMs<1000 || waitMs>300000)
  throw new Error('shared PREFLIGHT_START_UNIX_MS must be 1s..5min in future; no automatic resume/restart');
const end=epoch+plan.duration_seconds*1000;
const finalEnd=end+plan.drain_limit_seconds*1000;
const scenarios={}, tasks={};
plan.streams.forEach((s,i)=>{ const name=`producer_${i}`; tasks[name]=s;
  scenarios[name]={executor:'shared-iterations',vus:1,iterations:1,exec:'produce',maxDuration:`${Math.ceil(waitMs/1000)+plan.duration_seconds+2}s`,gracefulStop:'0s'};
});
plan.consumer_sessions.forEach((s,i)=>{const name=`consumer_${i}`; tasks[name]=s;
  scenarios[name]={executor:'shared-iterations',vus:1,iterations:1,exec:'consume',
    startTime:`${s.start_seconds}s`,maxDuration:`${Math.ceil(waitMs/1000)+s.end_seconds-s.start_seconds+2}s`,gracefulStop:'0s'};
});
export const options={scenarios,setupTimeout:'10s',teardownTimeout:'10s'};
const attempted=new Counter('pf_attempt_value_bytes');
const acknowledged=new Counter('pf_acknowledged_value_bytes_t1');
const late=new Counter('pf_acknowledged_value_bytes_late');
const unknown=new Counter('pf_unresolved_value_bytes');
const read=new Counter('pf_consumer_value_bytes');
const errors=new Counter('pf_errors');
const latency=new Trend('pf_produce_call_ms',true);
const receivedLatency=new Trend('pf_receive_latency_ms',true);
function event(type, fields={}) {
  // No raw exception text, credentials or payload bodies in evidence.
  console.log('PF_EVENT '+JSON.stringify({version:1,type,run_id:plan.run_id,worker_id:plan.worker_id,
    plan_sha256:digest,at_ms:Date.now(),...fields}));
}
function waitUntil(ms) { const delay=ms-Date.now(); if(delay>0) sleep(delay/1000); }
function rateSlot(earliest,stream,workerBudget) {
  // Retry sends also consume spacing. No send in idle windows, no final spike.
  for(const w of plan.producer_windows) {
    const at=Math.max(earliest,epoch+w.start_seconds*1000);
    if(at<epoch+w.end_seconds*1000 && at<end)
      return {at,ceiling:w.max_value_bytes_per_second_per_worker*stream.budget_bytes/workerBudget};
  }
  return null;
}
export function setup() {
  event('worker_start',{generation_start_ms:epoch,generation_end_ms:end,final_end_ms:finalEnd,
    global_target_bytes:plan.target_value_payload_bytes,stream_count:plan.streams.length,
    consumer_session_count:plan.consumer_sessions.length});
  return {};
}
export function produce() {
  const stream=tasks[exec.scenario.name];
  const tags={topic:stream.name,worker:String(plan.worker_id)};
  let remaining=stream.budget_bytes,count=stream.record_count,sequence=0,planned=0,nextAllowed=epoch;
  let ackBytes=0,lateBytes=0,unknownBytes=0,firstAttemptBytes=0;
  const sizeRandom=new Random(stream.size_seed),contentRandom=new Random(stream.content_seed);
  const workerBudget=plan.streams.reduce((s,t)=>s+t.budget_bytes,0);
  let producer;
  event('stream_start',{topic:stream.name,budget_bytes:remaining,record_count:count});
  try {
    producer=new Producer({...security,brokers:plan.environment.bootstrap_endpoints,
      requiredAcks:plan.required_acks,compression:stream.compression==='none'?'':stream.compression,
      batchSize:1,batchTimeout:1000000,writeTimeout:1000000000,readTimeout:1000000000});
    while(count && Date.now()<end) {
      const size=nextSize(remaining,count,stream.minimum_payload_bytes,stream.maximum_payload_bytes,sizeRandom);
      const id=idFor(plan.run_id,stream.name,plan.worker_id,sequence);
      // Pace the start of each value; a final record may finish after t1 and is reported late.
      const due=epoch+dueSeconds(planned/stream.budget_bytes,plan.producer_windows)*1000;
      const body=payload(size,stream.content_distribution,contentRandom),checksum=crypto.sha256(body,'hex');
      let state='not_attempted',attempts=0;
      for(let attempt=0;attempt<=plan.max_application_retries;attempt++) {
        const slot=rateSlot(Math.max(Date.now(),due,nextAllowed),stream,workerBudget);
        if(!slot) break;
        waitUntil(slot.at);
        if(Date.now()>=end) break;
        const sentAt=Date.now();
        nextAllowed=sentAt+size/slot.ceiling*1000;
        if(!attempts) firstAttemptBytes+=size;
        attempts++;
        event('attempt',{id,topic:stream.name,sequence,value_bytes:size,checksum,attempt:attempts,
          generation_end_ms:end});
        attempted.add(size,tags);
        try {
          producer.produce({messages:[{topic:stream.name,key:id,value:body,
            headers:{pf_run:plan.run_id,pf_bytes:String(size),pf_sha256:checksum,pf_sent_ms:String(sentAt)}}]});
          const finished=Date.now();
          latency.add(finished-sentAt,tags);
          state='acknowledged';
          if(finished<=end) {ackBytes+=size;acknowledged.add(size,tags);}
          else {lateBytes+=size;late.add(size,tags);}
          event('acknowledged',{id,topic:stream.name,value_bytes:size,checksum,attempt:attempts,
            boundary:finished<=end?'generation':'late'});
          break;
        } catch (_) {
          state='unresolved'; errors.add(1,{...tags,operation:'produce'});
          event('ambiguous',{id,topic:stream.name,value_bytes:size,attempt:attempts});
        }
      }
      if(state==='unresolved') {unknownBytes+=size;unknown.add(size,tags);}
      if(state==='not_attempted') break;
      remaining-=size;planned+=size;count--;sequence++;
    }
    // Wait for the generation boundary; no producer traffic occurs in drain.
    waitUntil(end);
  } catch (_) {
    errors.add(1,{...tags,operation:'producer_lifecycle'});event('stream_error',{topic:stream.name});
  } finally {
    // May be missing if k6/worker is killed: reconciler treats absent terminal evidence as incomplete.
    event('generation_end',{topic:stream.name,budget_bytes:stream.budget_bytes,
      first_attempt_bytes:firstAttemptBytes,acknowledged_bytes_t1:ackBytes,acknowledged_bytes_late:lateBytes,
      unresolved_bytes:unknownBytes,not_attempted_bytes:stream.budget_bytes-firstAttemptBytes,
      remaining_records:count,in_flight:'not_exposed',backlog:'external_group_lag_required'});
    if(producer) {try {producer.close();} catch(_){event('close_error',{topic:stream.name});}}
  }
}
export function consume() {
  const session=tasks[exec.scenario.name], tags={group:session.group_id,phase:session.phase};
  const start=epoch+session.start_seconds*1000,stop=epoch+session.end_seconds*1000;
  waitUntil(start);
  let consumer;
  event('consumer_start',{session_id:session.session_id,group_id:session.group_id,phase:session.phase,
    expected_end_ms:stop});
  try {
    consumer=new Consumer({...security,brokers:plan.environment.bootstrap_endpoints,groupId:session.group_id,
      groupTopics:session.topics,startOffset:session.offset_reset==='earliest'?'start_offsets_first_offset':'start_offsets_last_offset',
      maxWait:'100ms',maxBytes:plan.streams.reduce((m,s)=>Math.max(m,s.maximum_payload_bytes*2+4096),20004096)});
    while(Date.now()<stop && Date.now()<finalEnd) {
      const messages=consumer.consume({maxMessages:1,expectTimeout:true});
      let safe=true;
      for(const msg of messages) {
        const headers=msg.headers||{};
        if(ascii(headers.pf_run||'')!==plan.run_id) {event('foreign_record',{group_id:session.group_id,topic:msg.topic});continue;}
        const id=ascii(msg.key),value=ascii(msg.value),expected=ascii(headers.pf_sha256||'');
        const size=Number(ascii(headers.pf_bytes||''));
        const actual=crypto.sha256(value,'hex');
        const valid=value.length===size && actual===expected;
        event('receipt',{id,group_id:session.group_id,topic:msg.topic,partition:msg.partition,
          offset:msg.offset,value_bytes:value.length,checksum:actual,valid,phase:session.phase});
        if(!valid) {safe=false;errors.add(1,{...tags,operation:'integrity'});}
        read.add(value.length,tags);
        const sent=Number(ascii(headers.pf_sent_ms||''));
        if(Number.isFinite(sent)) receivedLatency.add(Date.now()-sent,tags);
      }
      if(!safe) {exec.test.abort('PF integrity verification failed');throw new Error('integrity failure');}
      if(messages.length) {consumer.commitOffsets(); event('commit',{group_id:session.group_id,session_id:session.session_id,phase:session.phase});}
    }
  } catch (_) {
    errors.add(1,{...tags,operation:'consume'});event('consumer_error',{group_id:session.group_id,session_id:session.session_id});
  } finally {
    if(consumer) {try{consumer.close();}catch(_){event('consumer_close_error',{session_id:session.session_id});}}
    event('consumer_end',{group_id:session.group_id,session_id:session.session_id,phase:session.phase});
  }
}
export function teardown() {event('worker_end',{generation_end_ms:end,final_end_ms:finalEnd});}
