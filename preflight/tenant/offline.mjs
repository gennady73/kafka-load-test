#!/usr/bin/env node
// Only bundled in-memory fakes are selectable. No live transport, Secret reader, native import or arming path.
import fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {compileContract} from './policy.mjs';
import {runTenantFixture} from './engine.mjs';
import {fakeWorld} from './offline-fakes.mjs';
import {auditFixture} from './audit.mjs';
export const sha256=s=>createHash('sha256').update(s).digest('hex');
export function simulate(plan,opts={}){const world=fakeWorld(plan,sha256,opts);const result=runTenantFixture(plan,world.deps);return {world,result,audit:auditFixture(plan,result.envelopes,result.seal,sha256)};}
function main(args){
 const names=['--contract','--c3-review','--c6-review','--output','--run-id','--seed'];const o={};
 if(args.length!==12)throw Error('usage: offline.mjs --contract FILE --c3-review FILE --c6-review FILE --output NEW_DIR --run-id offline-NAME --seed INT');
 for(let i=0;i<args.length;i+=2){if(!names.includes(args[i])||Object.hasOwn(o,args[i]))throw Error('unknown or duplicate argument');o[args[i]]=args[i+1];}
 if(names.some(k=>!o[k]))throw Error('missing argument');
 const raw=fs.readFileSync(o['--contract'],'utf8');
 const plan=compileContract(JSON.parse(raw),{run_id:o['--run-id'],seed:Number(o['--seed']),sha256,contract_sha256:sha256(raw),
  review_hashes:{C3:sha256(fs.readFileSync(o['--c3-review'])),C6:sha256(fs.readFileSync(o['--c6-review']))}});
 const report=simulate(plan);const destination=path.resolve(o['--output']);if(fs.existsSync(destination))throw Error('output already exists; choose a fresh immutable offline evidence directory');
 fs.mkdirSync(destination,{recursive:true});
 const save=(name,data)=>fs.writeFileSync(path.join(destination,name),typeof data==='string'?data:JSON.stringify(data,null,2)+'\n');
 const moduleDir=path.dirname(fileURLToPath(import.meta.url));
 save('driver-source-manifest.json',Object.fromEntries(['policy.mjs','contract-keys.mjs','engine.mjs','audit.mjs','offline-fakes.mjs','offline.mjs','../core.mjs'].map(name=>[name,sha256(fs.readFileSync(path.resolve(moduleDir,name)))])));
 save('plan.json',plan);save('events.ndjson',report.result.envelopes.map(e=>JSON.stringify(e)+'\n').join(''));
 save('seal.json',report.result.seal);save('adapter-report.json',{...report.result,events:undefined,envelopes:undefined});save('audit.json',report.audit);
 const files=fs.readdirSync(destination).sort();save('manifest.json',Object.fromEntries(files.map(n=>[n,sha256(fs.readFileSync(path.join(destination,n)))])));
 console.log(JSON.stringify({mode:'OFFLINE_STUB_ONLY',adapter_status:report.result.status,auditor_status:report.audit.status,live_execution:false,output:destination}));
 if(report.audit.status!=='OFFLINE_COMPLETE')process.exitCode=2;
}
if(process.argv[1]&&import.meta.url===pathToFileURL(path.resolve(process.argv[1])).href){try{main(process.argv.slice(2));}catch(_){console.error('Offline fixture rejected: check explicit disabled contract, review files, offline identity and fresh output path.');process.exitCode=2;}}
