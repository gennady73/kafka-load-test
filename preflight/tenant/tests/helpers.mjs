import fs from 'node:fs';
import {createHash} from 'node:crypto';
import {compileContract} from '../policy.mjs';
import {runTenantFixture} from '../engine.mjs';
import {fakeWorld} from '../offline-fakes.mjs';
import {auditFixture} from '../audit.mjs';
export const sha=s=>createHash('sha256').update(s).digest('hex');
export const clone=x=>JSON.parse(JSON.stringify(x));
export const contract=()=>JSON.parse(fs.readFileSync(new URL('./contract-fixture.json',import.meta.url),'utf8'));
export function plan(c=contract()){
 const raw=JSON.stringify(c);return compileContract(c,{run_id:'offline-tenant-test',seed:2511,sha256:sha,contract_sha256:sha(raw),review_hashes:{C3:sha('OFFLINE_REVIEW_FIXTURE_C3'),C6:sha('OFFLINE_REVIEW_FIXTURE_C6')}});
}
export function simulate(opts={},modify=null,p=plan()){
 const world=fakeWorld(p,sha,opts);if(modify)modify(world,p);
 const result=runTenantFixture(p,world.deps);return {plan:p,world,result,audit:auditFixture(p,result.envelopes,result.seal,sha)};
}
export function rechain(events,p,{complete=true}={}){
 let previous=null;
 const envelopes=events.map((event,i)=>{const e={...event,event_id:`${p.run_id}/tenant_fixture/${i}`,previous_event_sha256:previous};
  const x={event_json:JSON.stringify(e)};x.event_sha256=sha(x.event_json);previous=x.event_sha256;return x;});
 // Mutations intentionally preserve original reference IDs unless specifically changing links.
 const text=envelopes.map(x=>JSON.stringify(x)+'\n').join('');
 const terminal=events.at(-1);
 return {envelopes,seal:{mode:'OFFLINE_STUB_ONLY',run_id:p.run_id,plan_sha256:sha((fixtureCanonical(p))),complete,
  event_count:envelopes.length,client_event_counts:{tenant_fixture:envelopes.length},export_sha256:sha(text),observed_job_elapsed_ms:(terminal?.elapsed_ms??0)+1}};
}
function fixtureCanonical(x){
 if(x===null||typeof x!=='object')return JSON.stringify(x);
 if(Array.isArray(x))return '['+x.map(fixtureCanonical).join(',')+']';
 return '{'+Object.keys(x).sort().map(k=>JSON.stringify(k)+':'+fixtureCanonical(x[k])).join(',')+'}';
}
