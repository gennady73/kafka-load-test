"""Strict opt-in continuity planning. Pure data transformation; no network or clients."""
import copy
from datetime import datetime
import hashlib
from pathlib import Path
import re

MODE='sandbox_continuity'
PROFILE='SANDBOX-CONTINUITY-100MB-24H'
STAGES={'15m':(900,1040),'60m':(3600,4168),'24h':(86400,100000)}
TOPICS=[f'kaas-dev-smoke-{i:02d}' for i in range(1,5)]
EVIDENCE_KEYS=('effective_topic_settings_reference','binary_ack_semantics_reference',
 'worker_budget_reference','evidence_collection_reference','stop_policy_reference',
 'security_adapter_reference','initial_group_offsets_reference','native_commit_close_reference',
 'job_deadline_reference','collector_durability_reference')

def compile_continuity(contract,scenario,offline):
    def require(ok,message):
        if not ok:raise ValueError('continuity: '+message)
    require(contract.get('contract_version')=='1.0','unsupported parent contract')
    require(contract.get('synthetic_conventions',{}).get('volume_boundary')=='original_generated_value_payload_bytes','value accounting required')
    require(contract.get('common',{}).get('topic_replication_factor')==2 and contract['common'].get('topic_retention_ms')==21600000,'RF2/6h baseline required')
    require(scenario.get('mode')==MODE and scenario.get('profile_id')==PROFILE,'explicit independent profile required')
    require(scenario.get('fixture') is False and scenario.get('short_validation') is False,'fixture/short-validation bypass forbidden')
    require(type(scenario.get('execution_enabled')) is bool,'explicit execution_enabled flag required')
    require(scenario.get('stage') in STAGES,'select 15m, 60m or 24h')
    duration,count=STAGES[scenario['stage']]
    for field,value in [('duration_seconds',duration),('record_count',count),('target_value_payload_bytes',count*1000),
                        ('record_value_bytes',1000),('workers',1),('required_acks',-1),('max_application_retries',0),
                        ('target_tolerance_fraction',0),('setup_limit_seconds',120),('drain_limit_seconds',300),('shutdown_limit_seconds',60)]:
        require(type(scenario.get(field)) is int and scenario[field]==value,'invalid '+field)
    seed=scenario.get('seed');run=scenario.get('run_id')
    require(type(seed) is int and 1<=seed<=2147483647,'positive 31-bit seed required')
    require(isinstance(run,str) and re.fullmatch(r'[A-Za-z0-9_-]{1,64}',run),'safe run ID required')
    require(scenario.get('topics')==TOPICS,'exactly the four approved smoke topics in order required')
    require(scenario.get('schedule')=='uniform_interleaved_midpoints','explicit centered schedule required')
    env=copy.deepcopy(scenario.get('environment'))
    require(isinstance(env,dict),'environment required')
    require(env.get('namespace')=='kaas-demo' and env.get('kafka_namespace')=='kaas-kafka' and env.get('cluster')=='kaas-dev','sandbox-only target')
    require(env.get('bootstrap_endpoints')==['kaas-dev-kafka-bootstrap.kaas-kafka.svc:9093'],'approved internal bootstrap required')
    require(isinstance(env.get('test_image'),str) and re.fullmatch(r'image-registry\.openshift-image-registry\.svc:5000/kaas-demo/[^@\s]+@sha256:[a-f0-9]{64}',env['test_image']),'pinned sandbox image required')
    require(isinstance(env.get('binary_sha256'),str) and re.fullmatch(r'[a-f0-9]{64}',env['binary_sha256']),'binary fingerprint required')
    url=env.get('evidence_sink_url')
    require(url is None or isinstance(url,str) and re.fullmatch(r'https://[A-Za-z0-9.-]+(?::[0-9]+)?(?:/[A-Za-z0-9_/-]*)?',url),'collector must be HTTPS without credentials/query/fragment')
    evidence=copy.deepcopy(scenario.get('runtime_evidence',{}));require(isinstance(evidence,dict),'runtime evidence object required')
    ready=False
    if not offline:
        require(scenario['execution_enabled'] is True,'execution disabled; compile with --offline')
        require(not run.lower().startswith(('offline','candidate','fixture')),'fresh live run ID required')
        require(bool(url),'reviewed collector URL required')
        require(all(isinstance(scenario.get(k),str) and scenario[k].strip() for k in ['owner','planned_start_utc','stop_control_reference']),'owner, planned start, stop control required')
        try:
            stamp=datetime.fromisoformat(scenario['planned_start_utc'].replace('Z','+00:00'))
            require(stamp.utcoffset() is not None and stamp.utcoffset().total_seconds()==0,'planned start must be UTC')
        except (ValueError,TypeError):raise ValueError('continuity: valid UTC planned start required')
        for key in EVIDENCE_KEYS:
            require(isinstance(evidence.get(key),str) and evidence[key].strip(),'runtime_evidence.'+key+' required')
        ready=True
    streams=[];groups=[]
    for ordinal,topic in enumerate(TOPICS):
        group=f'{topic}-continuity-{run}'
        streams.append({'name':topic,'ordinal':ordinal,'budget_bytes':count*1000//4,'record_count':count//4,
                        'minimum_payload_bytes':1000,'maximum_payload_bytes':1000,'group_id':group})
        groups.append({'group_id':group,'topics':[topic],'offset_reset':'earliest','commit_policy':'manual_after_durable_receipt','require_full_run':True})
    return [{'plan_version':'continuity-1.0','algorithm':'sha256-hex-ascii-v1','mode':MODE,'profile_id':PROFILE,
       'runner_entrypoint':'preflight/continuity.js','runner_source_sha256':{name:hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest() for name in ['continuity.js','continuity-core.mjs','continuity-engine.mjs','core.mjs']},'stage':scenario['stage'],'qualification':'low_volume_continuity',
       'execution_ready':ready,'execution_enabled':ready,'fixture':False,'worker_id':0,'worker_count':1,'run_id':run,'seed':seed,
       'duration_seconds':duration,'target_value_payload_bytes':count*1000,'record_count':count,'record_value_bytes':1000,
       'target_tolerance_fraction':0,'required_acks':-1,'max_application_retries':0,'compression':'none',
       'setup_limit_seconds':120,'drain_limit_seconds':300,'shutdown_limit_seconds':60,'maximum_total_job_seconds':duration+480,
       'native_message_max_bytes':3145728,'consumer_fetch_max_bytes':4194304,'delivery_timeout_seconds':10,
       'max_start_lateness_seconds':5,'max_ack_gap_seconds':60,'max_receipt_gap_seconds':60,'max_receive_latency_seconds':60,
       'heartbeat_seconds':30,'maximum_clock_drift_ms':2000,'collector_timeout_seconds':2,'schedule':'uniform_interleaved_midpoints',
       'streams':streams,'consumer_groups':groups,'consumer_sessions':[{'session_id':g['group_id']+':continuous',
        'group_id':g['group_id'],'topics':g['topics'],'start_seconds':0,'end_seconds':duration+300,'phase':'continuous_generation_and_drain'} for g in groups],
       'clients':[f'{role}_{i}' for role in ['producer','consumer'] for i in range(4)],
       'environment':env,'runtime_evidence':evidence,'owner':scenario.get('owner'),'planned_start_utc':scenario.get('planned_start_utc'),
       'stop_control_reference':scenario.get('stop_control_reference'),'source_scenario_execution_enabled':scenario['execution_enabled']}]
