import {createHash} from 'node:crypto';
import {CONTRACT_KEYS} from './contract-keys.mjs';
// Source-locked sandbox policy. No I/O, Kafka, network, credentials or live-mode switch.
export const RESOURCE_REVISION='3211df93ae40d7d6f47c6334966ae6d1f658fee7';
export const EXECUTION_FLAGS=['execution_enabled','deployment_enabled','tests_enabled','execution_ready'];
export const PHASES={startup_and_controls:45,positive_delivery:90,negative_cases:90,final_reconciliation_drain:45,shutdown:30};
export const ERROR_CATEGORIES=['topic_authorization_denied','group_authorization_denied','cluster_authorization_denied','authentication_failed','tls_failed','network_failed','timeout','unknown'];
export const STAGES=['produce','consume','group_join'];
export const SAFE_SEVERITIES=Object.freeze(['FAIL','INCONCLUSIVE']);
export const SAFE_REASONS=Object.freeze(['internal_policy_failure','clock_invalid','budget_exhausted','durable_evidence_unavailable','unknown_binding','credential_unavailable','trust_unavailable','close_failed_or_overrun','group_ownership_or_membership_unverified','record_budget','unknown_record','generated_payload_mismatch','unexpected_negative_ack','late_positive_ack','unprocessed_initial_records','initial_position_unverified','positive_send_unresolved','consume_shape','foreign_or_unplanned_record','receipt_integrity_or_offset_gap','commit_result_unresolved','committed_offset_unverified','positive_control_missing','positive_consumer_still_open','unexpected_negative_join','unexpected_negative_record','negative_offset_changed','negative_failure_layer_unproven','consumer_write_layer_confounded','unsafe_client_or_debug_configuration','client_or_evidence_failure','total_deadline_exceeded','terminal_evidence_missing','stop_requested','phase_overrun']);
const standardSha=s=>createHash('sha256').update(s).digest('hex');
export function require(ok,code){if(!ok)throw new Error(code);}
export function canonical(x){
 if(x===null||typeof x!=='object')return JSON.stringify(x);
 if(Array.isArray(x))return '['+x.map(canonical).join(',')+']';
 return '{'+Object.keys(x).sort().map(k=>JSON.stringify(k)+':'+canonical(x[k])).join(',')+'}';
}
export function freeze(x){if(x&&typeof x==='object'){Object.values(x).forEach(freeze);Object.freeze(x);}return x;}
export function expectedACLs(tenant,role){
 const topics=['events','audit'].map(s=>`${tenant}-${s}`);
 const resource=(type,name)=>({type,...(name?{name,patternType:'literal'}:{})});
 const a=topics.map(name=>({resource:resource('topic',name),operations:[role==='producer'?'Write':'Read','Describe']}));
 return role==='producer'?[...a,{resource:resource('cluster'),operations:['IdempotentWrite']}]:
  [...a,...topics.map(t=>({resource:resource('group',t+'-group'),operations:['Read','Describe']}))];
}
export function permissions(binding,kind,name,operation){
 return binding.acls.some(a=>a.resource.type===kind&&a.resource.name===name&&a.resource.patternType==='literal'&&a.operations.includes(operation));
}
export function expectedNegatives(tenants){
 const cases=[];
 for(const src of tenants){
  const dst=tenants.find(t=>t.tenant_id!==src.tenant_id),own=src.topics[0],other=dst.topics[0],id=src.tenant_id;
  const add=(suffix,role,operation,topic,group,category)=>cases.push({id:id+'-'+suffix,principal:src.roles[role].principal,operation,topic,group,expected_failure_category:category});
  add('cross-topic-write','producer','produce',other.topic,null,'topic_authorization_denied');
  add('cross-topic-read','consumer','consume',other.topic,own.group,'topic_authorization_denied');
  add('cross-group-read','consumer','consume',own.topic,other.group,'group_authorization_denied');
  add('consumer-write','consumer','produce',own.topic,null,'topic_authorization_denied');
 }
 return cases;
}
export function validateContract(c){
 const allowed=(value,path='$')=>{if(value&&typeof value==='object'&&!Array.isArray(value)){require(CONTRACT_KEYS[path]&&Object.keys(value).every(k=>CONTRACT_KEYS[path].includes(k)),'unknown_contract_key');for(const [k,v] of Object.entries(value))allowed(v,path+'.'+k);}else if(Array.isArray(value))value.forEach(v=>allowed(v,path+'[]'));};
 allowed(c);
 require(c&&['c2-tenant-r2-0.1','c2-tenant-r2-0.2'].includes(c.contract_version),'contract_version');
 require(EXECUTION_FLAGS.every(k=>c[k]===false)&&c.result==='NOT_RUN','live_execution_disabled');
 require(c.source.tenant_resource_revision===RESOURCE_REVISION,'resource_pin');
 require(c.target.cluster==='kaas-dev'&&c.target.kafka_namespace==='kaas-kafka'&&c.target.proposed_client_namespace==='kaas-demo'&&c.target.environment==='sandbox3088','target');
 require(canonical(c.target.bootstrap_endpoints_from_saved_evidence)===canonical(['kaas-dev-kafka-bootstrap.kaas-kafka.svc:9093']),'bootstrap');
 require(c.security.credential_values_in_contract===false,'no_credential_values');
 require(c.security.protocol==='SASL_SSL'&&c.security.mechanism==='SCRAM-SHA-512'&&c.security.hostname_verification===true,'security_downgrade');
 require(c.security.expected_ca_source_reference==='kaas-kafka/kaas-dev-cluster-ca-cert'&&c.security.proposed_ca_mount_path==='/credentials/cluster-ca/ca.crt','trust_binding');
 require(c.tenants.length===2&&c.tenants.every((t,i)=>t.tenant_id===`kaas-sbx-${i?'b':'a'}`),'tenant_roster');
 const paths=new Set();
 for(const t of c.tenants){
  require(t.topics.length===2&&t.topics.every((s,i)=>s.topic===t.tenant_id+'-'+['events','audit'][i]),'topic_roster');
  require(canonical(Object.keys(t.roles).sort())===canonical(['consumer','producer']),'role_roster');
  for(const role of ['producer','consumer']){
   const r=t.roles[role],name=t.tenant_id+'-'+role;
   require(r.principal===name&&r.expected_source_secret_reference==='kaas-kafka/'+name,'principal_binding');
   require(r.proposed_password_mount_path===`/credentials/${name}/password`&&!paths.has(r.proposed_password_mount_path),'credential_path_binding');paths.add(r.proposed_password_mount_path);
   require(canonical(r.acls_declared)===canonical(expectedACLs(t.tenant_id,role)),'acl_drift');
   require(canonical(r.quotas_declared)===canonical({producerByteRate:65536,consumerByteRate:65536,requestPercentage:5}),'quota_drift');
  }
  const groups=t.roles.consumer.acls_declared.filter(a=>a.resource.type==='group').map(a=>a.resource.name);
  for(const s of t.topics){
   require(groups.includes(s.group),'unapproved_group'); // Both own groups may read either own topic.
   require(s.record_count===4&&s.original_value_bytes_per_record===1000&&s.expected_original_value_bytes===4000,'positive_budget');
   require(s.replicas===2&&s.retention_ms===21600000&&s.minimum_isr===2&&s.max_message_bytes===3145728,'topic_policy');
  }
 }
 require(c.negative_cases.length===8,'negative_roster');
 const expected=expectedNegatives(c.tenants);
 c.negative_cases.forEach((n,i)=>{
  require(Object.entries(expected[i]).every(([k,v])=>n[k]===v),'negative_policy');
  require(n.maximum_application_attempts===1&&n.offset_commit_allowed===false&&n.application_retry_allowed===false&&n.runtime_result==='NOT_RUN','negative_call_policy');
  require(n.maximum_original_value_bytes===(n.operation==='produce'?1000:0),'negative_budget');
 });
 const b=c.proposed_budget;
 require(b.positive_records===16&&b.positive_original_value_bytes===16000&&b.maximum_negative_produce_attempts===4&&b.maximum_negative_attempted_value_bytes===4000&&b.maximum_total_attempted_logical_value_bytes===20000&&b.negative_consume_attempts===4,'global_budget');
 require(b.maximum_total_client_job_seconds===300&&canonical(b.phase_caps_seconds)===canonical(PHASES)&&b.fixture_value_bytes===1000&&b.maximum_value_payload_bytes_ceiling===2097152,'deadline_or_size');
 const g=c.group_and_offset_policy;
 require(g.literal_group_ids_required===true&&g.append_run_suffix_to_group===false&&g.reset_or_delete_existing_offsets===false&&g.negative_case_commit_offsets===false&&g.skip_unprocessed_application_records_and_commit_past_them===false,'offset_policy');
 const n=c.candidate_client;
 require(n.native_retry_count===null&&n.idempotence_effective_setting===null&&n.native_delivery_timeout_seconds_candidate===10,'unknown_native_mapping');
 require(n.k6_version_from_tested_smoke==='1.7.1'&&n.binary_sha256_from_tested_smoke==='6c705c09c17c9773158ef08d4446531855d37ab0b20ad5aaee41decd949604ec'&&n.image_reference_from_tested_smoke==='image-registry.openshift-image-registry.svc:5000/kaas-demo/kaas-xk6-verified-20261124@sha256:f66c3f86f2365b219c590d1d3c580ea41fcc4ab2ba74fc1efb2ae72d93be0df2','candidate_build_pin');
 require(n.producer_required_acks===-1&&n.maximum_application_attempts_per_id===1&&n.native_message_max_bytes===3145728&&n.consumer_fetch_max_bytes===4194304&&n.compression==='none'&&n.raw_utf8_values===true&&n.producer_ack_partition_offset_exposed===false,'native_candidate');
 require(c.evidence_contract.timeout_only_is_denial_proof===false&&c.evidence_contract.receipt_is_producer_acknowledgement===false&&c.evidence_contract.offset_commit_is_payload_integrity_proof===false,'evidence_policy');
 return c;
}
export function valueFor(seed,record,sha){
 let value='';for(let n=0;value.length<1000;n++)value+=sha(`${seed}:${record.tenant}:${record.topic}:${record.sequence}:${record.case_id}:${n}`);
 return value.slice(0,1000);
}
export function compileContract(contract,{run_id,seed,sha256,contract_sha256,review_hashes}={}){
 const c=validateContract(JSON.parse(JSON.stringify(contract)));
 require(/^offline-[A-Za-z0-9_-]{1,48}$/.test(run_id)&&Number.isInteger(seed)&&seed>0&&seed<=2147483647,'offline_identity');
 require(typeof sha256==='function'&&/^[a-f0-9]{64}$/.test(contract_sha256||'')&&review_hashes&&canonical(Object.keys(review_hashes).sort())===canonical(['C3','C6'])&&['C3','C6'].every(k=>/^[a-f0-9]{64}$/.test(review_hashes[k]||'')),'review_and_input_hashes');
 const bindings=[];for(const t of c.tenants)for(const role of ['producer','consumer']){
  const r=t.roles[role];bindings.push({id:r.principal,tenant:t.tenant_id,role,principal:r.principal,password_path:r.proposed_password_mount_path,acls:r.acls_declared});
 }
 const positive=c.tenants.flatMap(t=>t.topics.map(s=>({id:'positive-'+s.topic,tenant:t.tenant_id,topic:s.topic,group:s.group,
  producer:t.roles.producer.principal,consumer:t.roles.consumer.principal,record_count:4})));
 const records=positive.flatMap(p=>Array.from({length:4},(_,sequence)=>{
  const r={id:`${run_id}/${p.tenant}/${p.topic}/${sequence}/${p.id}`,tenant:p.tenant,topic:p.topic,group:p.group,case_id:p.id,sequence,value_bytes:1000};
  return {...r,checksum:sha256(valueFor(seed,r,sha256))};
 }));
 const negatives=c.negative_cases.map(n=>{
  const binding=bindings.find(b=>b.id===n.principal),own=positive.filter(p=>p.tenant===binding.tenant),dest=positive.find(p=>p.topic===n.topic),peerGroup=positive.find(p=>p.group===n.group);
  const control_ids=[own[0].id,dest.id,...(peerGroup?[peerGroup.id]:[])].filter((x,i,a)=>a.indexOf(x)===i);
  const r=n.operation==='produce'?{id:`${run_id}/${binding.tenant}/${n.topic}/0/${n.id}`,tenant:binding.tenant,topic:n.topic,case_id:n.id,sequence:0,value_bytes:1000}:null;
  return {id:n.id,binding:n.principal,tenant:binding.tenant,operation:n.operation,topic:n.topic,group:n.group,
   expected_category:n.expected_failure_category,expected_stages:n.operation==='produce'?['produce']:(n.expected_failure_category==='group_authorization_denied'?['consume','group_join']:['consume']),control_ids,write_path_control_id:n.operation==='produce'?own[0].id:null,consumer_write_confounded:n.id.endsWith('-consumer-write'),
   record:r?{...r,checksum:sha256(valueFor(seed,r,sha256))}:null};
 });
 return freeze({plan_version:'tenant-offline-1.0',mode:'OFFLINE_ONLY',run_id,seed,execution_enabled:false,deployment_enabled:false,tests_enabled:false,execution_ready:false,
  contract_sha256,review_hashes,source_contract:c,resource_revision:RESOURCE_REVISION,bindings,positive,negative:negatives,records,
  phase_seconds:{...PHASES},maximum_total_seconds:300,maximum_logical_value_bytes:20000,positive_value_bytes:16000,
  write_settings_signature:standardSha(canonical({auto_create_topic:false,required_acks:-1,native_message_max_bytes:3145728,compression:'none',delivery_timeout_seconds_candidate:10,idempotence_effective:null,transactional:false})),
  call_reserve_ms:10000,sink_reserve_ms:2000,close_reserve_ms:1000,native_mapping_status:'UNVERIFIED_OFFLINE_STUB_ONLY',
  candidate_client:{...c.candidate_client},group_creation_flag_is_intent_only:true});
}
export function validatePlan(p){
 require(p&&p.plan_version==='tenant-offline-1.0'&&p.mode==='OFFLINE_ONLY'&&EXECUTION_FLAGS.every(k=>p[k]===false),'offline_only_plan');
 // Rehydrated JSON plans are recompiled at the engine boundary. A caller-supplied hash
 // is not authority to change identity, ACLs, controls, roster, settings or reserves.
 const expected=compileContract(p.source_contract,{run_id:p.run_id,seed:p.seed,sha256:standardSha,
  contract_sha256:p.contract_sha256,review_hashes:p.review_hashes});
 require(canonical(p)===canonical(expected),'compiled_plan_policy_mismatch');
 return freeze(p);
}
