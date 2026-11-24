import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import fs from 'node:fs';
import {createHash} from 'node:crypto';
import * as core from '../core.mjs';

function harness({failFirst=false,delay=0,ready=true}={}) {
  let now=0;const events=[],calls=[];
  const stream={name:'t',budget_bytes:2000,record_count:2,minimum_payload_bytes:1000,maximum_payload_bytes:1000,
    size_seed:1,content_seed:2,content_distribution:'seeded_ascii',compression:'none'};
  const plan={plan_version:'1.0',algorithm:'pf-xorshift32-v1',execution_ready:ready,fixture:false,
    run_id:'stub',worker_id:0,worker_count:1,duration_seconds:2,drain_limit_seconds:1,
    target_value_payload_bytes:2000,required_acks:-1,max_application_retries:1,
    environment:{bootstrap_endpoints:['STUB-NO-NETWORK']},streams:[stream],consumer_sessions:[],
    producer_windows:[{start_seconds:0,end_seconds:2,weight:1,max_value_bytes_per_second_per_worker:1000}]};
  const raw=JSON.stringify(plan),sha=s=>createHash('sha256').update(s).digest('hex');
  const context={...core,__ENV:{PREFLIGHT_PLAN:'stub.json',PREFLIGHT_PLAN_SHA256:sha(raw),
    PREFLIGHT_SECURITY_JSON:'{}',PREFLIGHT_START_UNIX_MS:'10000'},open:()=>raw,
    sleep:s=>{now+=s*1000;},Date:{now:()=>now},crypto:{sha256:sha},exec:{scenario:{name:'producer_0'}},
    Counter:class{add(){}},Trend:class{add(){}},console:{log:s=>events.push(JSON.parse(s.slice('PF_EVENT '.length)))},
    Producer:class{
      produce(x){calls.push({at:now,message:x.messages[0]});now+=delay;if(failFirst&&calls.length===1)throw new Error('stub ambiguity');}
      close(){}
    },Consumer:class{constructor(){throw new Error('consumer must not be created by producer');}}};
  let source=fs.readFileSync(new URL('../run.js',import.meta.url),'utf8');
  source=source.replace(/^import .*;\n/gm,'').replaceAll('export const ','const ').replaceAll('export function ','function ');
  source+='\nglobalThis.adapter={setup,produce,teardown,options};';
  vm.runInNewContext(source,context);
  return {context,events,calls};
}
test('adapter conserves original bytes and emits t1 snapshot with a stub',()=>{
  const h=harness();h.context.adapter.setup();h.context.adapter.produce();h.context.adapter.teardown();
  assert.equal(h.calls.length,2);assert.equal(h.calls[0].message.value.length,1000);
  const end=h.events.find(e=>e.type==='generation_end');assert.equal(end.acknowledged_bytes_t1,2000);
  assert.equal(end.not_attempted_bytes,0);assert.equal(end.at_ms,12000);
  assert.ok(h.calls.every(c=>c.at<12000));
});
test('ambiguous retry reuses ID and consumes rate budget; unsent shortfall stays visible',()=>{
  const h=harness({failFirst:true});h.context.adapter.produce();
  assert.equal(h.calls.length,2);assert.equal(h.calls[0].message.key,h.calls[1].message.key);
  assert.equal(h.calls[0].message.value,h.calls[1].message.value);
  const end=h.events.find(e=>e.type==='generation_end');assert.equal(end.acknowledged_bytes_t1,1000);
  assert.equal(end.not_attempted_bytes,1000);assert.equal(end.unresolved_bytes,0);
  assert.equal(h.calls[1].at-h.calls[0].at,1000);
});
test('late acknowledgement is not included in generation target',()=>{
  const h=harness({delay:2001});h.context.adapter.produce();
  const end=h.events.find(e=>e.type==='generation_end');assert.equal(end.acknowledged_bytes_t1,0);
  assert.equal(end.acknowledged_bytes_late,1000);assert.equal(end.not_attempted_bytes,1000);
});
test('offline plans are rejected before client construction',()=>assert.throws(()=>harness({ready:false}),/execution-ready/));
