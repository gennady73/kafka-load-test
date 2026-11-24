// Pure deterministic logic, shared by Node tests and the k6 adapter.
export class Random {
  constructor(seed) { this.state = seed >>> 0 || 1; }
  next() { let x=this.state; x^=x<<13; x^=x>>>17; x^=x<<5; this.state=x>>>0; return this.state/4294967296; }
  between(a,b) { return a+Math.floor(this.next()*(b-a+1)); }
}
export function nextSize(remaining, count, minimum, maximum, random) {
  if (!Number.isSafeInteger(remaining) || count<1 || remaining<count*minimum || remaining>count*maximum)
    throw new Error('infeasible stream remainder');
  const lo=Math.max(minimum,remaining-(count-1)*maximum);
  const hi=Math.min(maximum,remaining-(count-1)*minimum);
  return random.between(lo,hi);
}
export function payload(size, distribution, random) {
  if (distribution==='repeated_ascii') return 'x'.repeat(size);
  if (distribution!=='seeded_ascii') throw new Error('unsupported content distribution');
  const alphabet='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  const chunks=[];
  for(let offset=0;offset<size;offset+=4096) {
    let part='';
    for(let n=0;n<Math.min(4096,size-offset);n++) part+=alphabet[random.between(0,alphabet.length-1)];
    chunks.push(part);
  }
  return chunks.join(''); // ASCII: original value byte count equals string length.
}
export function dueSeconds(fraction, windows) {
  let mass=fraction*windows.reduce((s,w)=>s+w.weight,0);
  for(const w of windows) {
    if(mass<=w.weight) return w.start_seconds+(w.end_seconds-w.start_seconds)*mass/w.weight;
    mass-=w.weight;
  }
  return windows[windows.length-1].end_seconds;
}
export function activeWindow(seconds, windows) {
  return windows.find(w=>seconds>=w.start_seconds && seconds<w.end_seconds);
}
export function idFor(run,topic,worker,sequence) { return `${run}/${topic}/${worker}/${sequence}`; }
export function outcome(previous, result) {
  if(previous==='acknowledged' || result==='acknowledged') return 'acknowledged';
  if(previous==='unresolved' || result==='unresolved') return 'unresolved';
  return result;
}
export function ascii(value) {
  if(typeof value==='string') return value;
  if(value instanceof ArrayBuffer) value=new Uint8Array(value);
  if(!value || typeof value.length!=='number') throw new Error('unsupported extension byte representation');
  const chunks=[];
  for(let i=0;i<value.length;i+=4096) {
    const end=Math.min(i+4096,value.length); let s='';
    for(let j=i;j<end;j++) { if(!Number.isInteger(value[j]) || value[j]<0 || value[j]>127) throw new Error('non-ASCII bytes'); s+=String.fromCharCode(value[j]); }
    chunks.push(s);
  }
  return chunks.join('');
}
