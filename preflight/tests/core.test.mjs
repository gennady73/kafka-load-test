import test from 'node:test';
import assert from 'node:assert/strict';
import {Random,nextSize,payload,dueSeconds,activeWindow,idFor,outcome,ascii} from '../core.mjs';
test('PRNG cross-language golden vector',()=>{
  const r=new Random(1);assert.deepEqual([r.next(),r.next(),r.next()].map(x=>x*4294967296),[270369,67634689,2647435461]);
});
test('bounded sizes conserve exact bytes over many feasible tails',()=>{
  for(let seed=1;seed<200;seed++) {
    const r=new Random(seed); let remaining=100001, count=51, sum=0;
    while(count) {const n=nextSize(remaining,count,1000,3000,r);assert.ok(n>=1000&&n<=3000);remaining-=n;sum+=n;count--;}
    assert.equal(remaining,0);assert.equal(sum,100001);
  }
  assert.throws(()=>nextSize(999,1,1000,3000,new Random(1)));
});
test('ASCII content exact lengths and repeatability including WP-A maximum',()=>{
  for(const n of [1000,10000,300000,10000000]) {
    const s=payload(n,'repeated_ascii',new Random(2));assert.equal(Buffer.byteLength(s),n);
  }
  const a=payload(10000,'seeded_ascii',new Random(2));
  assert.equal(a,payload(10000,'seeded_ascii',new Random(2)));assert.equal(Buffer.byteLength(a),10000);
  assert.notEqual(a,payload(10000,'seeded_ascii',new Random(3)));
});
test('pacing respects independent weighted windows and idle gap',()=>{
  const w=[{start_seconds:0,end_seconds:20,weight:1},{start_seconds:30,end_seconds:60,weight:2}];
  assert.equal(dueSeconds(0,w),0);assert.equal(dueSeconds(1/3,w),20);assert.equal(dueSeconds(1,w),60);
  assert.equal(activeWindow(25,w),undefined);assert.equal(activeWindow(30,w),w[1]);
});
test('retry identity and ambiguity cannot erase an acknowledgement',()=>{
  assert.equal(idFor('r','t',2,5),'r/t/2/5');
  assert.equal(outcome('acknowledged','unresolved'),'acknowledged');
  assert.equal(outcome('unresolved','failed'),'unresolved');
  assert.equal(outcome('unresolved','acknowledged'),'acknowledged');
});
test('byte decoding preserves exact ASCII and rejects unsupported data',()=>{
  assert.equal(ascii(new Uint8Array([65,66])),'AB');assert.equal(ascii(new Uint8Array([65]).buffer),'A');
  assert.throws(()=>ascii([255]));assert.throws(()=>ascii({}));
});
