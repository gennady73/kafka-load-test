import test from 'node:test';
import assert from 'node:assert/strict';
import {plan,clone} from '../../tenant/tests/helpers.mjs';
import {compileIntegration,compileNativeOptions,adaptMessage,asciiBytes,decodeBoundaryError,budgetAdmission,requireCurrentBuildReady} from '../native-contract.mjs';
const p=plan();
test('full two-tenant fixture compiles with disabled execution',()=>{const c=compileIntegration(p);assert.equal(c.positive.length,4);assert.equal(c.negative.length,8);assert.equal(c.execution_ready,false);assert.equal(c.limits.attempted_original_value_bytes,20000);});
for(const row of p.positive){
 test(row.id+' exact producer native options',()=>{const o=compileNativeOptions(p,row.producer,'producer',row.topic);assert.equal(o.requiredAcks,-1);assert.equal(o.writeTimeout,1e10);assert.equal(o.compression,'');assert.equal(o.sasl.username,row.producer);assert.equal(o.tls.insecureSkipTlsVerify,false);assert.equal('password' in o.sasl,false);});
 test(row.id+' consumer literal group and manual commit',()=>{const o=compileNativeOptions(p,row.consumer,'consumer',row.topic,row.group);assert.equal(o.groupId,row.group);assert.equal(o.commitInterval,0);assert.equal(o.maxBytes,4194304);});
}
for(const mutation of [q=>q.bindings[0].password_path='/credentials/smoke/password',q=>q.execution_enabled=true,q=>q.negative.pop(),q=>q.maximum_total_seconds=301])test('mutated accepted plan rejected '+mutation.toString(),()=>{const q=clone(p);mutation(q);assert.throws(()=>compileIntegration(q));});
for(const value of ['abc',new Uint8Array([97,98,99]),new Uint8Array([97,98,99]).buffer,[97,98,99]])test('byte codec '+Object.prototype.toString.call(value),()=>assert.equal(asciiBytes(value),'abc'));
for(const value of [[256],[-1],[1.5],[true],'é',{},null])test('invalid byte codec '+JSON.stringify(value),()=>assert.throws(()=>asciiBytes(value)));
test('message retains safe numeric location',()=>assert.deepEqual(adaptMessage({topic:'t',key:[107],value:[118],partition:0,offset:7}),{topic:'t',key:'k',value:'v',partition:0,offset:7}));
for(const offset of [-1,NaN,2**54,true])test('invalid offset '+offset,()=>assert.throws(()=>adaptMessage({topic:'t',key:[],value:[],partition:0,offset})));
for(const [code,api,category] of [[29,'produce','topic_authorization_denied'],[29,'consume','topic_authorization_denied'],[30,'group_join','group_authorization_denied'],[31,'produce','cluster_authorization_denied']])test('structured native error '+code+api,()=>assert.equal(decodeBoundaryError({schema:'r2-native-error-1',code,api}).category,category));
for(const record of [{message:'Topic authorization failed'}, {schema:'r2-native-error-1',code:'29',api:'produce'},{schema:'r2-native-error-1',code:29,api:'group_join'}, {schema:'r2-native-error-1',code:30,api:'produce'}, {schema:'r2-native-error-1',code:31,api:'consume'}, {schema:'r2-native-error-1',code:29,api:'produce',password:'CANARY'}])test('untrusted native error never passes '+JSON.stringify(record),()=>assert.equal(decodeBoundaryError(record).typed,false));
test('exception getter never evaluated',()=>assert.equal(decodeBoundaryError(Object.defineProperty({},'schema',{enumerable:true,get(){throw Error('CANARY');}})).typed,false));
test('deadline includes two evidence and two close reservations',()=>{assert.equal(budgetAdmission({elapsed_ms:284000,phase_elapsed_ms:0,phase_cap_ms:30000}),true);assert.equal(budgetAdmission({elapsed_ms:284001,phase_elapsed_ms:0,phase_cap_ms:30000}),false);assert.equal(budgetAdmission({elapsed_ms:0,phase_elapsed_ms:0,phase_cap_ms:12000}),false);assert.throws(()=>budgetAdmission({elapsed_ms:0,phase_elapsed_ms:0,phase_cap_ms:30000,cleanup_ms:0}));});
for(const x of [-1,true,NaN])test('invalid clock '+x,()=>assert.throws(()=>budgetAdmission({elapsed_ms:x,phase_elapsed_ms:0,phase_cap_ms:30000})));
test('current image cannot be armed with receipt booleans',()=>assert.throws(()=>requireCurrentBuildReady({status:'OFFLINE_ONLY_NATIVE_GAPS_RECORDED',ready:true}),/not_implemented/));
