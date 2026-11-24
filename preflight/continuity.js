// Live entrypoint. Offline candidate plans are rejected before opening credentials or constructing clients.
import {sleep} from 'k6';
import exec from 'k6/execution';
import crypto from 'k6/crypto';
import http from 'k6/http';
import {Counter} from 'k6/metrics';
import {Producer,Consumer,SASL_SCRAM_SHA512,START_OFFSETS_FIRST_OFFSET} from 'k6/x/kafka';
import {validatePlan} from './continuity-core.mjs';
import {makeSession} from './continuity-engine.mjs';

if(!__ENV.CONTINUITY_PLAN)throw Error('CONTINUITY_PLAN required');
const raw=open(__ENV.CONTINUITY_PLAN),plan=JSON.parse(raw),sha=s=>crypto.sha256(s,'hex');
validatePlan(plan,true);
const digest=sha(raw);
if(__ENV.CONTINUITY_EXECUTE!=='true'||__ENV.CONTINUITY_PLAN_SHA256!==digest)throw Error('execution and exact plan hash must be explicitly confirmed');
for(const name of ['continuity.js','continuity-core.mjs','continuity-engine.mjs','core.mjs'])if(sha(open('./'+name))!==plan.runner_source_sha256[name])throw Error('runner source fingerprint mismatch: '+name);
if(__ENV.K6_HTTP_DEBUG)throw Error('HTTP debug must be disabled');
if(__ENV.CONTINUITY_OBSERVED_BINARY_SHA256!==plan.environment.binary_sha256)throw Error('runtime binary fingerprint does not match plan');
if(Number(__ENV.CONTINUITY_JOB_DEADLINE_SECONDS)!==plan.maximum_total_job_seconds)throw Error('reviewed external Job deadline required');
const initAt=Date.now(),epoch=Number(__ENV.CONTINUITY_START_UNIX_MS),wait=epoch-initAt;
if(!Number.isSafeInteger(epoch)||wait<5000||wait>plan.setup_limit_seconds*1000)throw Error('shared future epoch must fit setup envelope');
if(Date.parse(plan.planned_start_utc)!==epoch)throw Error('epoch differs from reviewed planned start');
const security={sasl:{username:'kaas-smoke-client',password:open('/credentials/password').trim(),algorithm:SASL_SCRAM_SHA512},
 tls:{enableTls:true,insecureSkipTlsVerify:false,serverCaPem:open('/credentials/ca.crt')}};
if(open('/script/kafka-client-limits.txt').trim()!==String(plan.native_message_max_bytes))throw Error('mounted native size limit differs from plan');
const token=__ENV.CONTINUITY_EVIDENCE_TOKEN_FILE?open(__ENV.CONTINUITY_EVIDENCE_TOKEN_FILE).trim():null;
const headers={'Content-Type':'application/json'};if(token)headers.Authorization='Bearer '+token;
const base=plan.environment.evidence_sink_url.replace(/\/$/,'');
const scenarios={};
for(const id of plan.clients)scenarios[id]={executor:'shared-iterations',exec:'continuityClient',vus:1,iterations:1,maxDuration:`${plan.maximum_total_job_seconds}s`,gracefulStop:'0s'};
export const options={scenarios,setupTimeout:'10s',teardownTimeout:'10s',thresholds:{continuity_client_errors:['count==0'],continuity_completed_clients:['count==8']}};
const errors=new Counter('continuity_client_errors'),completed=new Counter('continuity_completed_clients');
export function continuityClient(){
 errors.add(0);completed.add(0);
 const wall=Date.now(),mono=exec.instance.currentTestRunDuration;
 const monotonicNow=()=>wall+exec.instance.currentTestRunDuration-mono;
 const clockHealthy=()=>Math.abs(Date.now()-monotonicNow())<=plan.maximum_clock_drift_ms;
 const deps={now:monotonicNow,clockHealthy,sleep,sha256:sha,
  post:(body,seconds)=>http.post(base+'/events',body,{headers,timeout:`${seconds}s`,redirects:0,tags:{name:'continuity-event'}}),
  readiness:()=>{const r=http.get(base+'/runs/'+plan.run_id+'/readiness?plan_sha256='+digest,{headers,timeout:`${plan.collector_timeout_seconds}s`,redirects:0,tags:{name:'continuity-readiness'}});try{return r.status===200?r.json():null;}catch(_){return null;}},
  log:e=>console.log('CONTINUITY_EVENT '+JSON.stringify(e)),
  unpersisted:e=>console.error('CONTINUITY_UNPERSISTED_ERROR '+JSON.stringify(e)),
  abort:reason=>{errors.add(1);exec.test.abort('continuity stopped: '+reason);},
  makeProducer:stream=>new Producer({...security,brokers:plan.environment.bootstrap_endpoints,topic:stream.name,requiredAcks:-1,
   batchSize:1,batchBytes:plan.native_message_max_bytes,batchTimeout:1000000,writeTimeout:10000000000,readTimeout:10000000000,compression:'',autoCreateTopic:false}),
  makeConsumer:stream=>new Consumer({...security,brokers:plan.environment.bootstrap_endpoints,topic:stream.name,groupId:stream.group_id,
   startOffset:START_OFFSETS_FIRST_OFFSET,maxWait:'250ms',maxBytes:plan.consumer_fetch_max_bytes,sessionTimeout:10000000000,heartbeatInterval:3000000000})};
 const r=makeSession(plan,deps,exec.scenario.name,epoch,digest).run();
 if(!r.failed)completed.add(1);
}
