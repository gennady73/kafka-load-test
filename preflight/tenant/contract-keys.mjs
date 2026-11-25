// Allowed data-only contract fields. Unknown nested credential/debug/override keys are rejected.
export const CONTRACT_KEYS={
  "$": [
    "candidate_client",
    "contract_version",
    "date",
    "deployment_enabled",
    "entry_gates",
    "evidence_contract",
    "excluded_changes",
    "execution_enabled",
    "execution_ready",
    "group_and_offset_policy",
    "negative_cases",
    "proposed_budget",
    "result",
    "review_disposition",
    "scope",
    "security",
    "source",
    "status",
    "target",
    "tenants",
    "tests_enabled"
  ],
  "$.candidate_client": [
    "adapter_status",
    "binary_sha256_from_tested_smoke",
    "commit_policy",
    "compression",
    "consumer_fetch_max_bytes",
    "current_image_and_pod_pull_verification_reference",
    "idempotence_effective_setting",
    "image_reference_from_tested_smoke",
    "k6_version_from_tested_smoke",
    "maximum_application_attempts_per_id",
    "native_delivery_timeout_seconds_candidate",
    "native_limit_file_path",
    "native_message_max_bytes",
    "native_retry_count",
    "native_timeout_commit_close_mapping_reference",
    "producer_ack_partition_offset_exposed",
    "producer_required_acks",
    "raw_utf8_values",
    "typed_authorization_failure_mapping_reference"
  ],
  "$.entry_gates": [
    "c3_acceptance_review_reference",
    "c6_identity_network_review_reference",
    "current_health_headroom_reference",
    "execution_owner_window_stop_reference",
    "fixture_state_positive_controls_reference",
    "literal_group_membership_evidence_reference",
    "r1_tenant_reconciliation_ownership_ready_reference"
  ],
  "$.evidence_contract": [
    "durability_reference",
    "events",
    "export_seal_custody_reference",
    "late_acknowledgements_separate",
    "negative_writes_excluded_from_positive_ingestion_target",
    "offset_commit_is_payload_integrity_proof",
    "receipt_is_producer_acknowledgement",
    "redaction_and_no_debug_reference",
    "retries_reuse_logical_id",
    "sink_url",
    "timeout_only_is_denial_proof",
    "unique_record_id_fields"
  ],
  "$.group_and_offset_policy": [
    "append_run_suffix_to_group",
    "automatic_group_creation_flag_is_intent_not_enforcement",
    "automatic_group_creation_for_denied_cases",
    "initial_reset_policy_candidate",
    "literal_group_ids_required",
    "negative_case_commit_offsets",
    "reset_or_delete_existing_offsets",
    "reuse_requires_fixture_only_ownership_and_initial_committed_offset_evidence",
    "run_identity_location",
    "skip_unprocessed_application_records_and_commit_past_them"
  ],
  "$.negative_cases[]": [
    "application_retry_allowed",
    "classification",
    "expected_failure_category",
    "group",
    "id",
    "maximum_application_attempts",
    "maximum_original_value_bytes",
    "offset_commit_allowed",
    "operation",
    "principal",
    "runtime_result",
    "topic"
  ],
  "$.proposed_budget": [
    "evidence_capacity_custody_reference",
    "fixture_value_bytes",
    "maximum_negative_attempted_value_bytes",
    "maximum_negative_produce_attempts",
    "maximum_total_attempted_logical_value_bytes",
    "maximum_total_client_job_seconds",
    "maximum_value_payload_bytes_ceiling",
    "negative_consume_attempts",
    "phase_caps_seconds",
    "positive_original_value_bytes",
    "positive_records",
    "producer_rate_and_catchup_policy",
    "status",
    "worker_cpu_memory_allocation_reference"
  ],
  "$.proposed_budget.phase_caps_seconds": [
    "final_reconciliation_drain",
    "negative_cases",
    "positive_delivery",
    "shutdown",
    "startup_and_controls"
  ],
  "$.review_disposition": [
    "C3_conditions",
    "C3_review",
    "C6_conditions",
    "C6_in_progress_findings",
    "C6_review",
    "live_error_mapping_verified",
    "live_transport_binding_implemented",
    "scope"
  ],
  "$.security": [
    "ca_chain_san_expiry_evidence_reference",
    "ca_destination_reference",
    "credential_values_in_contract",
    "expected_ca_source_reference",
    "hostname_verification",
    "mechanism",
    "proposed_ca_mount_path",
    "protocol"
  ],
  "$.source": [
    "registered_metadata_package_revision",
    "review",
    "srs_revision",
    "tenant_resource_revision",
    "tenant_resource_sha256",
    "utility_revision"
  ],
  "$.source.tenant_resource_sha256": [
    "environments/sandbox-tenants/a/resources.yaml",
    "environments/sandbox-tenants/b/resources.yaml"
  ],
  "$.target": [
    "advertised_broker_endpoint_evidence_reference",
    "bootstrap_endpoints_from_saved_evidence",
    "cluster",
    "effective_network_policy_reference",
    "environment",
    "external_listener_required_for_this_case",
    "kafka_namespace",
    "proposed_client_namespace"
  ],
  "$.tenants[]": [
    "fixture_only_group_ownership_reference",
    "initial_group_offsets_reference",
    "roles",
    "tenant_id",
    "topics"
  ],
  "$.tenants[].roles": [
    "consumer",
    "producer"
  ],
  "$.tenants[].roles.consumer": [
    "acls_declared",
    "credential_lifecycle_reference",
    "delivery_authority_reference",
    "destination_secret_reference",
    "expected_source_secret_reference",
    "principal",
    "proposed_password_mount_path",
    "quotas_declared",
    "source_secret_verified"
  ],
  "$.tenants[].roles.consumer.acls_declared[]": [
    "operations",
    "resource"
  ],
  "$.tenants[].roles.consumer.acls_declared[].resource": [
    "name",
    "patternType",
    "type"
  ],
  "$.tenants[].roles.consumer.quotas_declared": [
    "consumerByteRate",
    "producerByteRate",
    "requestPercentage"
  ],
  "$.tenants[].roles.producer": [
    "acls_declared",
    "credential_lifecycle_reference",
    "delivery_authority_reference",
    "destination_secret_reference",
    "expected_source_secret_reference",
    "principal",
    "proposed_password_mount_path",
    "quotas_declared",
    "source_secret_verified"
  ],
  "$.tenants[].roles.producer.acls_declared[]": [
    "operations",
    "resource"
  ],
  "$.tenants[].roles.producer.acls_declared[].resource": [
    "name",
    "patternType",
    "type"
  ],
  "$.tenants[].roles.producer.quotas_declared": [
    "consumerByteRate",
    "producerByteRate",
    "requestPercentage"
  ],
  "$.tenants[].topics[]": [
    "expected_original_value_bytes",
    "group",
    "max_message_bytes",
    "minimum_isr",
    "original_value_bytes_per_record",
    "record_count",
    "replicas",
    "retention_ms",
    "topic"
  ]
};
