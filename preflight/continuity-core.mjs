// Pure planning/verification helpers; importing this module never creates Kafka clients.
export const stages={'15m':[900,1040],'60m':[3600,4168],'24h':[86400,100000]};
export function validatePlan(p,armed=false){
 const shape=stages[p.stage];
 if(p.plan_version!=='continuity-1.0'||p.algorithm!=='sha256-hex-ascii-v1'||p.mode!=='sandbox_continuity'||p.profile_id!=='SANDBOX-CONTINUITY-100MB-24H'||p.fixture!==false||!shape)throw Error('not a continuity plan');
 const [duration,count]=shape;
 if(p.duration_seconds!==duration||p.record_count!==count||p.record_value_bytes!==1000||p.target_value_payload_bytes!==count*1000||p.worker_id!==0||p.worker_count!==1||p.required_acks!==-1||p.max_application_retries!==0||p.target_tolerance_fraction!==0||p.compression!=='none'||p.schedule!=='uniform_interleaved_midpoints')throw Error('continuity budget/policy mismatch');
 if(!Number.isInteger(p.seed)||p.seed<1||p.seed>2147483647||!/^[-_A-Za-z0-9]{1,64}$/.test(p.run_id))throw Error('invalid identity');
 if(p.setup_limit_seconds!==120||p.drain_limit_seconds!==300||p.shutdown_limit_seconds!==60||p.maximum_total_job_seconds!==duration+480||p.delivery_timeout_seconds!==10||p.collector_timeout_seconds!==2||p.native_message_max_bytes!==3145728||p.consumer_fetch_max_bytes!==4194304||p.heartbeat_seconds!==30||p.maximum_clock_drift_ms!==2000||p.max_ack_gap_seconds!==60||p.max_receipt_gap_seconds!==60||p.max_receive_latency_seconds!==60||p.max_start_lateness_seconds!==5)throw Error('continuity time/limit mismatch');
 if(p.streams.length!==4||p.consumer_groups.length!==4||p.clients.length!==8)throw Error('four producer/consumer pairs required');
 const clients=[];
 for(let i=0;i<4;i++){
  const t=p.streams[i],name=`kaas-dev-smoke-0${i+1}`,group=`${name}-continuity-${p.run_id}`,g=p.consumer_groups[i];
  if(t.name!==name||t.ordinal!==i||t.group_id!==group||t.record_count!==count/4||t.budget_bytes!==count*1000/4||t.minimum_payload_bytes!==1000||t.maximum_payload_bytes!==1000||g.group_id!==group||g.topics.length!==1||g.topics[0]!==name||g.offset_reset!=='earliest'||g.commit_policy!=='manual_after_durable_receipt'||g.require_full_run!==true)throw Error('topic/group budget mismatch');
 }
 for(const role of ['producer','consumer'])for(let i=0;i<4;i++)clients.push(`${role}_${i}`);
 if(JSON.stringify(p.clients)!==JSON.stringify(clients))throw Error('client roster mismatch');
 const e=p.environment;
 if(e.namespace!=='kaas-demo'||e.kafka_namespace!=='kaas-kafka'||e.cluster!=='kaas-dev'||JSON.stringify(e.bootstrap_endpoints)!==JSON.stringify(['kaas-dev-kafka-bootstrap.kaas-kafka.svc:9093'])||!/^image-registry\.openshift-image-registry\.svc:5000\/kaas-demo\/[^@\s]+@sha256:[a-f0-9]{64}$/.test(e.test_image)||!/^[a-f0-9]{64}$/.test(e.binary_sha256))throw Error('unapproved target/build');
 for(const name of ['continuity.js','continuity-core.mjs','continuity-engine.mjs','core.mjs'])if(!/^[a-f0-9]{64}$/.test((p.runner_source_sha256||{})[name]||''))throw Error('missing runner source fingerprint');
 if(armed){
  if(p.execution_ready!==true||p.execution_enabled!==true||/^offline|^candidate|^fixture/i.test(p.run_id))throw Error('requires explicitly armed live plan');
  if(!/^https:\/\/[A-Za-z0-9.-]+(?::[0-9]+)?(?:\/[A-Za-z0-9_/-]*)?$/.test(e.evidence_sink_url||''))throw Error('reviewed HTTPS collector required');
  for(const k of ['owner','planned_start_utc','stop_control_reference'])if(typeof p[k]!=='string'||!p[k].trim())throw Error('missing '+k);
  if(!/(Z|\+00:00)$/.test(p.planned_start_utc)||!Number.isFinite(Date.parse(p.planned_start_utc)))throw Error('valid UTC planned start required');
  for(const k of ['effective_topic_settings_reference','binary_ack_semantics_reference','worker_budget_reference','evidence_collection_reference','stop_policy_reference','security_adapter_reference','initial_group_offsets_reference','native_commit_close_reference','job_deadline_reference','collector_durability_reference'])if(typeof p.runtime_evidence[k]!=='string'||!p.runtime_evidence[k].trim())throw Error('missing runtime evidence '+k);
 }
 return p;
}
export function recordId(plan,stream,sequence){return `${plan.run_id}:${stream.name}:${sequence}`;}
export function dueMilliseconds(plan,stream,sequence){
 if(!Number.isInteger(sequence)||sequence<0||sequence>=stream.record_count)throw Error('invalid sequence');
 return (4*sequence+stream.ordinal+0.5)*plan.duration_seconds*1000/plan.record_count;
}
export function sequenceFor(plan,stream,id){
 const prefix=`${plan.run_id}:${stream.name}:`;
 if(typeof id!=='string'||!id.startsWith(prefix))return null;
 const text=id.slice(prefix.length);if(!/^(0|[1-9][0-9]*)$/.test(text))return null;
 const n=Number(text);return Number.isSafeInteger(n)&&n<stream.record_count?n:null;
}
export function valueFor(plan,stream,sequence,sha256){
 const chunks=[];for(let block=0;block<16;block++)chunks.push(sha256(`${plan.seed}:${stream.name}:${sequence}:${block}`));
 return chunks.join('').slice(0,1000);
}
export function receiptFor(plan,stream,message,sha256,asAscii){
 if(message.topic!==stream.name)throw Error('wrong receipt topic');
 const id=asAscii(message.key||''),sequence=sequenceFor(plan,stream,id);
 if(sequence===null){if(id.startsWith(plan.run_id+':'))throw Error('malformed current-run ID');return null;}
 const body=asAscii(message.value),actual=sha256(body),expected=sha256(valueFor(plan,stream,sequence,sha256));
 if(body.length!==1000||actual!==expected||!Number.isInteger(message.partition)||!Number.isSafeInteger(message.offset)||message.partition!==0||message.offset<0)throw Error('receipt integrity/location mismatch');
 return {id,sequence,topic:stream.name,group_id:stream.group_id,value_bytes:1000,checksum:actual,partition:message.partition,offset:message.offset};
}
export function durableAck(response,event,hash){
 if(!response||![200,201].includes(response.status))throw Error('collector did not acknowledge');
 let a;try{a=response.json();}catch(_){throw Error('invalid collector acknowledgement');}
 if(a.durable!==true||a.event_id!==event.event_id||a.event_sha256!==hash)throw Error('collector durability acknowledgement mismatch');
}
export function gapExceeded(now,epoch,last,limitSeconds){return now>=epoch&&now-Math.max(epoch,last)>limitSeconds*1000;}
