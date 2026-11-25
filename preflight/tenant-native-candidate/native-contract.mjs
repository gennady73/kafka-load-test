// Offline integration candidate. No k6, sockets, filesystem, credentials or live entrypoint.
import {validatePlan,canonical} from '../tenant/policy.mjs';
export const MAPPING='R2_NATIVE_BOUNDARY_CANDIDATE_1';
const fail=()=>{throw new Error('native_candidate_contract_rejected');};
const safeInt=x=>Number.isSafeInteger(x)&&x>=0;
export function asciiBytes(value){
 if(typeof value==='string'){if(!/^[\x00-\x7f]*$/.test(value))fail();return value;}
 if(value instanceof ArrayBuffer)value=new Uint8Array(value);
 if(!Array.isArray(value)&&!(value instanceof Uint8Array))fail();
 let result='';for(const x of value){if(!Number.isInteger(x)||x<0||x>127)fail();result+=String.fromCharCode(x);}return result;
}
export function compileNativeOptions(plan,bindingId,role,topic,group=null){
 validatePlan(plan);const b=plan.bindings.find(x=>x.id===bindingId);
 if(!b||!['producer','consumer'].includes(role))fail();
 const positive=plan.positive.some(p=>p.topic===topic&&(role==='producer'?p.producer:p.consumer)===bindingId&&(role==='producer'?group===null:p.group===group));
 const negative=plan.negative.some(n=>n.binding===bindingId&&n.topic===topic&&n.group===group&&n.operation===(role==='producer'?'produce':'consume'));
 if(!positive&&!negative)fail();
 const security={brokers:[...plan.source_contract.target.bootstrap_endpoints_from_saved_evidence],topic,
  sasl:{username:b.principal,password_file:b.password_path,algorithm:'SASL_SCRAM_SHA512'},
  tls:{enableTls:true,insecureSkipTlsVerify:false,serverCaPem_file:'/credentials/cluster-ca/ca.crt'}};
 // Files are references, not xk6 options. A future reviewed runtime must replace them in memory.
 return role==='producer'?{...security,requiredAcks:-1,batchSize:1,batchBytes:3145728,batchTimeout:1000000,
  writeTimeout:10000000000,readTimeout:10000000000,compression:'',autoCreateTopic:false}:
 {...security,groupId:group,startOffset:'START_OFFSETS_FIRST_OFFSET',maxBytes:4194304,maxWait:'250ms',
  sessionTimeout:10000000000,heartbeatInterval:3000000000,commitInterval:0};
}
export function adaptMessage(m){
 if(!m||typeof m.topic!=='string'||m.partition!==0||!safeInt(m.offset))fail();
 return {topic:m.topic,key:asciiBytes(m.key),value:asciiBytes(m.value),partition:m.partition,offset:m.offset};
}
export function budgetAdmission({elapsed_ms,phase_elapsed_ms,phase_cap_ms,call_ms=10000,evidence_ms=4000,cleanup_ms=2000}){
 const values=[elapsed_ms,phase_elapsed_ms,phase_cap_ms,call_ms,evidence_ms,cleanup_ms];
 if(!values.every(safeInt)||phase_cap_ms>300000||phase_elapsed_ms>elapsed_ms||call_ms<10000||evidence_ms<4000||cleanup_ms<2000)fail();
 return elapsed_ms+call_ms+evidence_ms+cleanup_ms<=300000&&phase_elapsed_ms+call_ms+evidence_ms+cleanup_ms<=phase_cap_ms;
}
// Only a native boundary with errors.As(ckafka.Error) may supply these codes. Never parse Error() text.
// This pure decoder is not proof that the existing binary exposes such a boundary.
export function decodeBoundaryError(record){
 const unknown={category:'unknown',stage:'unknown',typed:false,mapping_id:MAPPING};
 if(!record||typeof record!=='object')return unknown;
 const descriptors=Object.getOwnPropertyDescriptors(record),keys=Object.keys(descriptors).sort();
 if(canonical(keys)!==canonical(['api','code','schema']))return unknown;
 if(keys.some(k=>!('value' in descriptors[k])))return unknown;
 const r=Object.fromEntries(keys.map(k=>[k,descriptors[k].value]));
 if(r.schema!=='r2-native-error-1'||!Number.isInteger(r.code)||!['produce','consume','group_join'].includes(r.api))return unknown;
 const categories={29:'topic_authorization_denied',30:'group_authorization_denied',31:'cluster_authorization_denied'};
 const category=categories[r.code];
 if(!category||r.code===29&&r.api==='group_join'||r.code===30&&r.api==='produce'||r.code===31&&r.api!=='produce')return unknown;
 return {category,stage:r.api,typed:true,mapping_id:MAPPING};
}
export function requireCurrentBuildReady(receipt){
 // The existing binary cannot be promoted by supplying booleans; only this offline assessment exists.
 if(receipt?.status!=='OFFLINE_ONLY_NATIVE_GAPS_RECORDED')fail();
 throw new Error('live_native_binding_not_implemented');
}
export function compileIntegration(plan){
 validatePlan(plan);
 return {schema:'r2-native-candidate-1',mode:'OFFLINE_ONLY',execution_enabled:false,deployment_enabled:false,
  workloads_enabled:false,execution_ready:false,resource_revision:plan.resource_revision,
  positive:plan.positive.map(p=>({id:p.id,producer:compileNativeOptions(plan,p.producer,'producer',p.topic),consumer:compileNativeOptions(plan,p.consumer,'consumer',p.topic,p.group)})),
  negative:plan.negative.map(n=>({id:n.id,expected_category:n.expected_category,expected_stages:n.expected_stages,
   options:compileNativeOptions(plan,n.binding,n.operation==='produce'?'producer':'consumer',n.topic,n.group)})),
  required_native_work:['typed_error_boundary_and_safe_logs','group_state_and_initial_assignment_without_offset_reset','bounded_commit_and_close','k6_compatible_runtime_without_node_imports','collector_transport_and_durable_receipt_binding'],
  limits:{positive_originals:16,positive_value_bytes:16000,attempted_original_value_bytes:20000,negative_operations:8,whole_job_seconds:300},
  runtime_status:'NOT_RUN',native_binding_status:'BLOCKED_CURRENT_IMAGE_NOT_QUALIFIED'};
}
