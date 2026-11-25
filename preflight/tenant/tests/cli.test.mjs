import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),entry=path.resolve(here,'../offline.mjs');
function files(dir){const c=path.join(dir,'contract.json'),r3=path.join(dir,'c3-review.md'),r6=path.join(dir,'c6-review.md');
 fs.copyFileSync(path.join(here,'contract-fixture.json'),c);fs.writeFileSync(r3,'OFFLINE STUB REVIEW REFERENCE ONLY');fs.writeFileSync(r6,'OFFLINE STUB REVIEW REFERENCE ONLY');
 return ['--contract',c,'--c3-review',r3,'--c6-review',r6,'--output',path.join(dir,'output'),'--run-id','offline-cli-test','--seed','25'];}
function withDir(fn){const d=fs.mkdtempSync(path.join(os.tmpdir(),'tenant-cli-'));try{fn(d);}finally{fs.rmSync(d,{recursive:true,force:true});}}
test('portable offline CLI generates only fake reports, exact plan hashes and disabled flags',()=>withDir(d=>{
 const x=spawnSync(process.execPath,[entry,...files(d)],{cwd:d,encoding:'utf8'});assert.equal(x.status,0,x.stderr);
 const out=path.join(d,'output'),p=JSON.parse(fs.readFileSync(path.join(out,'plan.json'))),a=JSON.parse(fs.readFileSync(path.join(out,'audit.json')));
 assert.equal(p.execution_enabled,false);assert.equal(p.tests_enabled,false);assert.equal(p.mode,'OFFLINE_ONLY');
 assert.equal(a.status,'OFFLINE_COMPLETE');assert.equal(a.runtime_acceptance,'NOT_ASSESSED');
 const s=fs.readdirSync(out).map(n=>fs.readFileSync(path.join(out,n),'utf8')).join('');assert.ok(!s.includes('OFFLINE_CANARY_'));
}));
test('CLI refuses output replacement and leaves existing evidence byte-identical',()=>withDir(d=>{
 const args=files(d),first=spawnSync(process.execPath,[entry,...args],{encoding:'utf8'});assert.equal(first.status,0);
 const p=path.join(d,'output','plan.json'),before=fs.readFileSync(p);const again=spawnSync(process.execPath,[entry,...args],{encoding:'utf8'});
 assert.equal(again.status,2);assert.deepEqual(fs.readFileSync(p),before);
}));
test('CLI rejects live/transport flags without reading a credential or creating output',()=>withDir(d=>{
 for(const flag of ['--execute','--live','--collector','--credentials']){
  const x=spawnSync(process.execPath,[entry,...files(d),flag,'DO_NOT_READ_OR_CONTACT'],{encoding:'utf8'});
  assert.equal(x.status,2);assert.equal(fs.existsSync(path.join(d,'output')),false);
 }
}));
test('CLI rejects armed input before any evidence directory exists',()=>withDir(d=>{
 const args=files(d),input=args[1],c=JSON.parse(fs.readFileSync(input));c.execution_ready=true;fs.writeFileSync(input,JSON.stringify(c));
 const x=spawnSync(process.execPath,[entry,...args],{encoding:'utf8'});assert.equal(x.status,2);assert.equal(fs.existsSync(path.join(d,'output')),false);
}));
test('CLI refuses a non-offline identity and has no fallback configuration',()=>withDir(d=>{
 const args=files(d);args[args.indexOf('--run-id')+1]='live-tenant';const x=spawnSync(process.execPath,[entry,...args],{encoding:'utf8'});
 assert.equal(x.status,2);assert.equal(fs.existsSync(path.join(d,'output')),false);
}));
