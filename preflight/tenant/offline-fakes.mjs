// In-memory test doubles only. No fs, sockets, HTTP, k6 or real credential reads.
import {permissions,canonical} from './policy.mjs';
export function fakeWorld(plan,sha,opts={}){
 let time=opts.initialTimeMs??0,groupGeneration=0;
 const log=[],sink=[],calls=[],reads=[],commits=[],groups=new Map(),broker=new Map(),members=new Map();
 const credentials=new Map(plan.bindings.map(b=>[b.password_path,`OFFLINE_CANARY_${b.principal}_not_a_credential`]));
 const canary='OFFLINE_SECRET_CANARY_DO_NOT_EXPORT';
 function err(category,stage,typed=true){const e=new Error(canary+' authorization password=DO_NOT_EXPORT');e.cause={token:canary};e.offline_category=category;e.offline_stage=stage;e.offline_typed=typed;return e;}
 const key=(g,t)=>g+'|'+t;
 const getOffset=(g,t)=>groups.has(key(g,t))?groups.get(key(g,t)):null;
 const expectedBinding=o=>{
  const b=plan.bindings.find(b=>b.id===o.binding_id);
  if(!b||o.principal!==b.principal||o.password_path!==b.password_path||o.password!==credentials.get(b.password_path))throw err('authentication_failed','produce');
  if(o.auto_commit!==false||o.commit_on_close!==false||o.protocol!=='SASL_SSL'||!o.hostname_verification)throw err('unknown','consume');
  return b;
 };
 const negativeFor=(b,o,operation)=>plan.negative.find(n=>n.binding===b.id&&n.topic===o.topic&&n.group===(operation==='produce'?null:o.group)&&n.operation===operation);
 function failFor(n,defaultCategory,defaultStage){
  const custom=opts.negativeFailure?.[n.id];
  if(custom==='empty')return null;
  if(custom)return err(custom.category??custom,custom.stage??defaultStage,custom.typed!==false);
  return err(defaultCategory,defaultStage);
 }
 const deps={mode:'OFFLINE_STUB',jobStartMs:0,now:()=>time,sleep:seconds=>{time+=seconds*1000;},sha256:sha,
  debugConfigurationSafe:()=>!opts.debugUnsafe,
  capabilities:{manual_commit_only:!opts.autoCommitEnabled,no_commit_on_close:!opts.autoCommitEnabled},
  readCredential:path=>{reads.push(path);return opts.emptyCredential===path?'':credentials.get(path);},
  readPublicCA:()=>opts.emptyCA?'':'OFFLINE_PUBLIC_CA_FIXTURE',
  normalizeNativeError:e=>({category:e.offline_category,stage:e.offline_stage,typed:e.offline_typed===true,mapping_id:'OFFLINE_STUB_TYPED_V1'}),
  persist:envelope=>{
   const e=JSON.parse(envelope.event_json);log.push(e);time+=opts.sinkLatencyByType?.[e.type]??1;
   if(opts.sinkThrowType===e.type)throw new Error(canary);
   const existing=sink.find(x=>JSON.parse(x.event_json).event_id===e.event_id);
   if(existing&&canonical(existing)!==canonical(envelope))return {status:409};
   if(!existing)sink.push(envelope);
   return {status:existing?200:201,ack:{event_id:e.event_id,event_sha256:opts.wrongAckType===e.type?'0'.repeat(64):envelope.event_sha256,durable:opts.refuseDurableType!==e.type}};
  },
  groupState:(group,topic)=>({fixture_only:!opts.nonFixtureGroup,exclusive:!opts.sharedGroup,
   members:members.get(group)??0,membership_observed:!opts.unobservedMembership,
   committed_offset:getOffset(group,topic),log_end_offset:(broker.get(topic)??[]).length}),
  makeProducer:o=>{
   const b=expectedBinding(o),n=negativeFor(b,o,'produce');calls.push({type:'construct_producer',binding:b.id,topic:o.topic,negative:!!n});
   return {
    produce:request=>{
     time+=opts.produceDelayMs??1;const r=request.messages[0];calls.push({type:'produce',binding:b.id,topic:o.topic,id:r.key,negative:!!n});
     if(r.topic!==o.topic)throw err('unknown','produce');
     if(n&&!opts.unexpectedNegativeAck){
      const error=opts.consumerWriteClusterDenied&&n.consumer_write_confounded?err('cluster_authorization_denied','produce'):failFor(n,'topic_authorization_denied','produce');
      throw error??err('unknown','produce',false);
     }
     if(opts.positiveTimeout&&!n){
      if(opts.acceptBeforeTimeout){const q=broker.get(o.topic)??[];q.push({...r,partition:0,offset:q.length});broker.set(o.topic,q);}
      throw err('timeout','produce');
     }
     const q=broker.get(o.topic)??[];q.push({...r,partition:0,offset:q.length});broker.set(o.topic,q);
    },
    close:()=>{time+=opts.closeDelayMs??1;calls.push({type:'close_producer',binding:b.id,negative:!!n});if(opts.closeThrows)throw err('unknown','produce');}
   };
  },
  makeConsumer:o=>{
   const b=expectedBinding(o),n=negativeFor(b,o,'consume');calls.push({type:'construct_consumer',binding:b.id,topic:o.topic,group:o.group,negative:!!n});
   let offset=getOffset(o.group,o.topic)??0,closed=false,joined=!n;
   if(joined)members.set(o.group,(members.get(o.group)??0)+1);
   return {
    position:()=>offset,
    probeState:()=>opts.unexpectedJoin&&n?{broker_confirmed:true,group_joined:true,forbidden_assignment:true}:{broker_confirmed:false},
    consume:()=>{
     time+=opts.consumeDelayMs??1;calls.push({type:'consume',binding:b.id,group:o.group,negative:!!n});
     if(n){
      if(opts.unexpectedNegativeRecord)return [{topic:o.topic,key:canary,value:canary,partition:0,offset:0}];
      const category=permissions(b,'group',o.group,'Read')?'topic_authorization_denied':'group_authorization_denied';
      const error=failFor(n,category,category==='group_authorization_denied'?'group_join':'consume');if(error)throw error;
      return [];
     }
     if(opts.emptyPositive)return [];
     const q=broker.get(o.topic)??[];if(offset>=q.length)return [];
     let m={...q[offset],topic:o.topic};offset++;
     if(opts.duplicateReceipt&&offset===2)m={...q[0],topic:o.topic};
     if(opts.foreignPositive)m.key=canary;
     if(opts.corruptPositive)m.value='x'.repeat(1000);
     if(opts.offsetGap)m.offset+=1;
     if(opts.wrongPartition)m.partition=1;
     return [m];
    },
    commitOffsets:()=>{
     time+=opts.commitDelayMs??1;
     const last=log.filter(e=>e.type==='durable_receipt_ack').at(-1);
     if(!last||!sink.some(x=>JSON.parse(x.event_json).event_id===last.event_id))throw err('unknown','consume');
     groups.set(key(o.group,o.topic),offset);commits.push({group:o.group,topic:o.topic,offset,negative:!!n});
     if(opts.commitTimeout)throw err('timeout','consume');
    },
    close:()=>{
     time+=opts.closeDelayMs??1;calls.push({type:'close_consumer',binding:b.id,group:o.group,negative:!!n});
     if(!closed&&joined)members.set(o.group,(members.get(o.group)??1)-1);closed=true;
     if(n&&opts.implicitNegativeCommit){groups.set(key(o.group,o.topic),999);commits.push({group:o.group,negative:true,implicit:true});}
     if(opts.closeThrows)throw err('unknown','consume');
    }
   };
  }
 };
 return {deps,calls,reads,commits,sink,log,canary,time:()=>time,credentials};
}
