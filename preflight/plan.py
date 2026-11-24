#!/usr/bin/env python3
"""Compile compact deterministic PF plans. No network, subprocesses or credentials."""
import argparse
import hashlib
import json
import math
import re
from pathlib import Path

ALGORITHM = 'pf-xorshift32-v1'

def integer(v, name, low=0):
    if type(v) is not int or not low <= v <= 9007199254740991:
        raise ValueError(f'{name}: expected safe integer >= {low}')
    return v

def number(v, name, low=0):
    if type(v) not in (int, float) or not math.isfinite(v) or not low <= v <= 9007199254740991:
        raise ValueError(f'{name}: expected finite number >= {low}')
    return v

def text(v, name):
    if not isinstance(v, str) or not v.strip():
        raise ValueError(f'{name}: nonempty string required')
    return v

def derive(seed, label):
    return int.from_bytes(hashlib.sha256(f'{ALGORITHM}:{seed}:{label}'.encode()).digest()[:4], 'big') or 1

class Random:
    def __init__(self, seed): self.state = seed
    def next(self):
        x = self.state
        x ^= (x << 13) & 0xffffffff
        x ^= x >> 17
        x ^= (x << 5) & 0xffffffff
        self.state = x & 0xffffffff
        return self.state / 4294967296
    def between(self, a, b): return a + int(self.next() * (b - a + 1))

def compile_plan(contract, scenario, offline=False):
    if scenario.get('mode') == 'sandbox_continuity':
        # Import by location as this file is also loaded directly by offline tooling.
        import importlib.util
        spec=importlib.util.spec_from_file_location('continuity_plan', Path(__file__).with_name('continuity_plan.py'))
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        return module.compile_continuity(contract,scenario,offline)
    if scenario.get('mode') not in (None,'preflight'):
        raise ValueError('unsupported mode')
    if contract.get('contract_version') != '1.0': raise ValueError('unsupported contract version')
    if contract['synthetic_conventions']['volume_boundary'] != 'original_generated_value_payload_bytes':
        raise ValueError('unsupported byte boundary')
    common = contract['common']
    if common['topic_replication_factor'] != 2 or common['topic_retention_ms'] != 21600000:
        raise ValueError('expected RF=2 and selected 6h retention')
    p = next((p for p in contract['profiles'] if p['id'] == scenario.get('profile_id')), None)
    if p is None: raise ValueError('select one profile')
    seed = integer(scenario.get('seed'), 'seed', 1)
    workers = integer(scenario.get('workers'), 'workers', 1)
    if workers > 10000: raise ValueError('worker count exceeds planner bound')
    run_id = text(scenario.get('run_id'), 'run_id')
    if any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_' for c in run_id):
        raise ValueError('run_id: only alphanumeric, dash and underscore allowed')
    fixture = scenario.get('fixture') is True
    if fixture and not offline: raise ValueError('fixtures cannot be execution-ready')
    duration = integer(scenario.get('duration_seconds'), 'duration_seconds', 1)
    target = integer(scenario.get('target_value_payload_bytes'), 'target_value_payload_bytes', 1)
    short = scenario.get('short_validation') is True
    if short and (duration > 3600 or target > p['target_value_payload_bytes']):
        raise ValueError('short validation must be <= 1 hour and within profile budget')
    if not fixture and not short and (duration != 86400 or target != p['target_value_payload_bytes']):
        raise ValueError('full profile requires selected daily budget and 86400 seconds')
    tolerance = number(scenario.get('target_tolerance_fraction'), 'target_tolerance_fraction')
    if tolerance >= 1: raise ValueError('tolerance must be < 1')
    drain = integer(scenario.get('drain_limit_seconds'), 'drain_limit_seconds')
    retries = integer(scenario.get('max_application_retries'), 'max_application_retries')
    if retries > 10: raise ValueError('max_application_retries must be <= 10')
    acks = scenario.get('required_acks')
    if type(acks) is not int or acks not in (-1, 1):
        raise ValueError('explicit acknowledged policy required: required_acks -1 or 1; policy not chosen by planner')
    windows = scenario.get('producer_windows')
    if not isinstance(windows, list) or not windows: raise ValueError('producer_windows required')
    end = 0
    for w in windows:
        start = integer(w['start_seconds'], 'window start')
        stop = integer(w['end_seconds'], 'window end', 1)
        weight = number(w['weight'], 'window weight', 0.000001)
        number(w['max_value_bytes_per_second_per_worker'], 'rate ceiling', 0.000001)
        if not end <= start < stop <= duration: raise ValueError('overlapping/out-of-range producer window')
        end = stop
    topics = scenario.get('topics')
    if not isinstance(topics, list) or not topics: raise ValueError('topics required')
    if sum(integer(t['budget_bytes'], 'topic budget', 1) for t in topics) != target:
        raise ValueError('topic budgets must sum to global target')
    if len({t['name'] for t in topics}) != len(topics): raise ValueError('duplicate topic')
    streams = [[] for _ in range(workers)]
    total_weight = number(sum(w['weight'] for w in windows), 'total window weight', 0.000001)
    for t in topics:
        name = text(t['name'], 'topic name')
        if not re.fullmatch(r'[a-zA-Z0-9._-]{1,249}', name) or name in ('.','..'):
            raise ValueError('invalid Kafka topic name')
        lo = integer(t['minimum_payload_bytes'], 'minimum_payload_bytes', p['minimum_payload_bytes'])
        hi = integer(t['maximum_payload_bytes'], 'maximum_payload_bytes', lo)
        if hi > p['maximum_payload_bytes']: raise ValueError('topic bounds outside profile')
        if t.get('size_distribution') != 'uniform_feasible_fixed_count':
            raise ValueError('supported size distribution: uniform_feasible_fixed_count')
        if t.get('content_distribution') not in ('seeded_ascii', 'repeated_ascii'):
            raise ValueError('explicit content_distribution required')
        if t.get('key_distribution') != 'record_id': raise ValueError('supported key_distribution: record_id')
        if t.get('serialization') != 'raw_utf8': raise ValueError('supported serialization: raw_utf8')
        if t.get('compression') not in ('none', 'snappy', 'gzip', 'lz4', 'zstd'):
            raise ValueError('explicit supported compression required')
        # Record count is explicit so size distribution is not forced near maximum.
        n = integer(t['record_count'], 'record_count', 1)
        budget = t['budget_bytes']
        if not n * lo <= budget <= n * hi: raise ValueError('record_count cannot tile topic budget')
        counts = [n // workers + (i < n % workers) for i in range(workers)]
        budgets = [c * lo for c in counts]
        extra = budget - sum(budgets)
        caps = [c * (hi-lo) for c in counts]
        cap = sum(caps)
        additions = [extra*c//cap if cap else 0 for c in caps]
        tail = extra - sum(additions)
        for i in range(workers):
            v = min(tail, caps[i]-additions[i]); additions[i] += v; tail -= v
        for i in range(workers):
            b = budgets[i]+additions[i]
            if not b: continue
            streams[i].append(dict(t, budget_bytes=b, record_count=counts[i],
                                   size_seed=derive(seed, f'size:{name}:{i}'),
                                   content_seed=derive(seed, f'content:{name}:{i}')))
    for worker_streams in streams:
        for w in windows:
            rate = sum(s['budget_bytes'] for s in worker_streams) * w['weight'] / total_weight / (w['end_seconds']-w['start_seconds'])
            if rate > w['max_value_bytes_per_second_per_worker']:
                raise ValueError('planned worker aggregate rate exceeds declared ceiling')
    groups = scenario.get('consumer_groups')
    if not isinstance(groups, list) or not groups: raise ValueError('consumer_groups required')
    sessions = [[] for _ in range(workers)]
    group_ids = set()
    for g in groups:
        gid = text(g['group_id'], 'group_id')
        if gid in group_ids: raise ValueError('duplicate group definition')
        group_ids.add(gid)
        subs = g['topics']
        if not subs or not set(subs) <= {t['name'] for t in topics}: raise ValueError('unknown subscription')
        if g.get('offset_reset') not in ('earliest', 'latest'): raise ValueError('offset_reset required')
        if g.get('commit_policy') != 'manual_after_verified_batch': raise ValueError('explicit manual commit policy required')
        if type(g.get('require_full_run')) is not bool: raise ValueError('require_full_run required')
        schedule = g['schedule']
        bounds = {}
        for key in ('start_seconds', 'active_seconds', 'idle_seconds', 'concurrency'):
            pair = schedule[key]
            if not isinstance(pair, list) or len(pair)!=2: raise ValueError(f'invalid {key} bounds')
            low = 1 if key in ('active_seconds','idle_seconds','concurrency') else 0
            a,b = (integer(v, key, low) for v in pair)
            if a>b: raise ValueError(f'inverted {key} bounds')
            bounds[key]=(a,b)
        if bounds['start_seconds'][1] >= duration: raise ValueError('consumer starts outside generation')
        if bounds['concurrency'][1] > 100: raise ValueError('consumer concurrency exceeds planner bound')
        rng=Random(derive(seed,f'consumer:{gid}'))
        at=rng.between(*bounds['start_seconds']); sequence=0
        while at<duration:
            active=min(rng.between(*bounds['active_seconds']), duration-at)
            for member in range(rng.between(*bounds['concurrency'])):
                wid=(sequence+member)%workers
                sessions[wid].append(dict(group_id=gid, topics=subs, offset_reset=g['offset_reset'],
                    start_seconds=at,end_seconds=at+active,phase='generation',session_id=f'{gid}:{sequence}:{member}'))
            at+=active+rng.between(*bounds['idle_seconds']); sequence+=1
            if sum(map(len,sessions))>10000: raise ValueError('too many sessions; widen scheduling bounds')
        if drain:
            for member in range(bounds['concurrency'][0]):
                sessions[member%workers].append(dict(group_id=gid, topics=subs,offset_reset=g['offset_reset'],
                    start_seconds=duration,end_seconds=duration+drain,phase='drain',session_id=f'{gid}:drain:{member}'))
    ready = not offline
    if ready:
        env=contract['environment']
        for key in ('namespace','kafka_cluster_reference','security_profile_reference','component_inventory_reference',
                    'test_image_digest','observability_reference','resource_manifest_reference'):
            text(env.get(key),f'environment.{key}')
        if not env.get('bootstrap_endpoints'): raise ValueError('bootstrap endpoints required')
        for key in ('effective_topic_settings_reference','binary_ack_semantics_reference','worker_budget_reference',
                    'evidence_collection_reference','stop_policy_reference','security_adapter_reference','initial_group_offsets_reference'):
            text(scenario.get('runtime_evidence',{}).get(key),f'runtime_evidence.{key}')
    plans=[]
    for i in range(workers):
        plans.append(dict(plan_version='1.0',algorithm=ALGORITHM,run_id=run_id,profile_id=p['id'],
            fixture=fixture,qualification='offline_fixture' if fixture else ('short_validation' if short else 'full_profile'),execution_ready=ready,worker_id=i,worker_count=workers,seed=seed,
            duration_seconds=duration,drain_limit_seconds=drain,target_value_payload_bytes=target,
            target_tolerance_fraction=tolerance,producer_windows=windows,required_acks=acks,
            max_application_retries=retries,streams=streams[i],consumer_sessions=sessions[i],
            consumer_groups=groups,environment=contract['environment'],runtime_evidence=scenario.get('runtime_evidence',{})))
    return plans

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--contract',type=Path,required=True)
    parser.add_argument('--scenario',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--offline',action='store_true',help='non-executable plan for code/fixture validation')
    args=parser.parse_args()
    try:
        raw_contract=args.contract.read_bytes(); raw_scenario=args.scenario.read_bytes()
        plans=compile_plan(json.loads(raw_contract),json.loads(raw_scenario),args.offline)
        if args.output.exists(): raise ValueError('output already exists; use a new immutable plan directory')
        args.output.mkdir(parents=True)
        hashes={}
        for p in plans:
            p['input_hashes']={'contract':hashlib.sha256(raw_contract).hexdigest(),'scenario':hashlib.sha256(raw_scenario).hexdigest()}
            body=(json.dumps(p,sort_keys=True,indent=2)+'\n').encode()
            name=f'worker-{p["worker_id"]}.json'
            (args.output/name).write_bytes(body); hashes[name]=hashlib.sha256(body).hexdigest()
        (args.output/'manifest.json').write_text(json.dumps(hashes,indent=2)+'\n')
        print(json.dumps({'workers':len(plans),'execution_ready':not args.offline,'output':str(args.output)}))
    except (ValueError,KeyError,TypeError,OSError) as err:
        parser.exit(2,f'plan rejected: {err}\n')

if __name__=='__main__': main()
