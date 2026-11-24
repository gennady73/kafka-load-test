// Offline k6 VM check. Does not import or construct Kafka clients.
import {Random,nextSize,payload,dueSeconds,ascii} from '../core.mjs';
export const options={vus:1,iterations:1};
export default function() {
  const r=new Random(1);
  if(r.next()*4294967296!==270369) throw new Error('PRNG mismatch');
  let left=10001,n=5,total=0;
  while(n) {const size=nextSize(left,n,1000,3000,r);total+=size;left-=size;n--;}
  if(total!==10001 || left!==0) throw new Error('budget mismatch');
  if(payload(1000,'seeded_ascii',new Random(4)).length!==1000) throw new Error('length mismatch');
  if(ascii(new Uint8Array([65,66]))!=='AB') throw new Error('byte conversion mismatch');
  if(dueSeconds(0.5,[{start_seconds:0,end_seconds:10,weight:1}])!==5) throw new Error('pacing mismatch');
  console.log('C2_OFFLINE_CORE_PASS');
}
