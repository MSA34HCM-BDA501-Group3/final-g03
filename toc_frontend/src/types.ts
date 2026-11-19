export type AlertCategory =
  | 'ONSET_A_D_TO_SEVERE'
  | 'SEVERE_WORSENING_E_TO_F'
  | 'WORSENING_TRANSITION'
  | 'RECOVERING_TRANSITION'
  | 'PERSISTENT_SEVERE'
  | 'STABLE_FLOW';

export type CoordsTuple = [number, number, number, number];

export interface WindowSemantics {
  ict_date: string;
  issue_date_ict: string;
  slot_start_ict: string;
  issue_time_ict: string;
  collect_window: string;
  forecast_window: string;
  target_eval_time_ict: string;
  headline: string;
}

export interface ScenarioFrame extends WindowSemantics {
  vn_hour: number;
  date: string;
  period: string;
  active_segments: number;
  early_warnings: number;
  transitions_any: number;
  severe_now: number;
}

export interface ScenarioItem {
  id: string;
  title: string;
  short_label: string;
  split_badge: string;
  ict_date: string;
  default_vn_hour: number;
  spotlight_segment_id: number;
  description: string;
  frames: ScenarioFrame[];
}

export interface MapSegmentItem {
  segment_id: number;
  street_name: string;
  street_level: number;
  coords: CoordsTuple;
  mean_velocity: number;
  max_velocity: number;
  obs_count: number;
  current_los: string;
  rf_pred_los: string;
  rf_cal_los: string;
  effective_forecast_los?: string;
  actual_next_los: string | null;
  severe_prob: number;
  change_prob: number;
  alert_type: AlertCategory;
  pagerank_score: number;
  lifecycle_status?: string;
}

export interface AlertRow {
  segment_id: number;
  street_name: string;
  street_level: number;
  obs_count: number;
  mean_velocity: number;
  max_velocity: number;
  current_los: string;
  persistence_los: string;
  rf_pred_los: string;
  rf_cal_los: string;
  effective_forecast_los?: string;
  change_threshold?: number;
  actual_next_los: string | null;
  severe_prob: number;
  change_prob: number;
  alert_type: AlertCategory;
  pagerank_score: number;
  lifecycle_status: string;
}

export interface MongoTopDoc {
  event_id?: string;
  segment_id: number;
  street_name: string;
  street_level?: number;
  date: string;
  ict_date?: string;
  period: string;
  vn_hour?: number;
  current_los: string;
  predicted_next_los: string;
  severe_prob: number;
  lifecycle_status: string;
}

export interface CityFrameResponse {
  is_empty_slot?: boolean;
  window_semantics: WindowSemantics;
  summary: {
    active_segments: number;
    early_warning_worsening_count: number;
    recovering_count: number;
    persistent_severe_count: number;
    stable_flow_count: number;
    live_matched_segments?: number;
  };
  map_segments: MapSegmentItem[];
  alerts: AlertRow[];
  mongo_benchmark: {
    mode: string;
    total_docs: number;
    live_query_ms: number | null;
    benchmark_median_ms?: number | null;
    benchmark_p95_ms: number | null;
    collscan_warm_median_ms?: number | null;
    speedup_vs_collscan_x?: number | null;
    top_docs: MongoTopDoc[];
  };
}

export interface RawPingPoint {
  ping_id: number;
  time_ict: string;
  minutes_from_issue: number;
  velocity_kmh: number;
  bucket: string;
}

export interface HistoryWindowPoint {
  window_label: string;
  close_time_ict: string;
  minutes_from_issue: number;
  mean_velocity: number;
  obs_count: number;
  los: string;
}

export interface NeighborSegment {
  segment_id: number;
  street_name: string;
  relation: string;
  coords: CoordsTuple;
  current_los: string;
}

export interface SegmentTimelineResponse {
  segment_id: number;
  is_cold_start?: boolean;
  street_name: string;
  street_level: number;
  split_set: string;
  max_velocity: number;
  coords: CoordsTuple | null;
  neighbors: NeighborSegment[];
  window_semantics: WindowSemantics;
  history_windows: HistoryWindowPoint[];
  raw_pings_past: RawPingPoint[];
  raw_pings_future: RawPingPoint[];
  forecast_comparison: {
    current_los: string;
    current_velocity: number;
    persistence_los: string;
    persistence_velocity: number;
    rf_raw_los: string;
    rf_calibrated_los: string;
    effective_forecast_los: string;
    actual_next_los: string | null;
    actual_next_velocity: number | null;
    severe_prob: number;
    change_prob: number;
  };
  feature_drivers: {
    obs_count_in_window: number;
    lag1_los: string;
    neighbor_los: string;
    pagerank_score: number;
    cluster_id: number;
    street_severe_prior: number;
  };
}

export interface LiveScoreRow {
  event_id: string;
  seq_id?: number;
  received_at_ict?: string;
  kafka_partition?: number;
  segment_id: number;
  street_name: string;
  street_level?: number;
  date?: string;
  ict_date?: string;
  period?: string;
  vn_hour?: number;
  mean_velocity: number;
  max_velocity: number;
  current_los: string;
  predicted_next_los: string;
  cal_predicted_los?: string;
  severe_prob: number;
  change_prob: number;
  ewma_hotspot_score: number;
  lifecycle_status?: string;
  coords?: [number, number, number, number] | null;
}

export interface StreamStatusResponse {
  telemetry: {
    status?: string;
    topic?: string;
    batches_processed?: number;
    messages_consumed?: number;
    partition_counts?: Record<string, number>;
    duplicates_caught_bloom?: number;
    late_dropped_for_tier2?: number;
    sink2_scored_v1?: number;
    severe_alerts_v1_count?: number;
    v2_guard_protected_count?: number;
    hll_estimated_segments?: number;
    exact_distinct_segments?: number;
    mongo_connected?: boolean;
    last_updated_ict?: string;
    story_cursor?: {
      scenario: string;
      frame_idx: number;
      total_frames: number;
      ict_date: string;
      vn_hour: number;
      spotlight_segment_id: number;
    } | null;
  };
  recent_scores: LiveScoreRow[];
}

export interface HotspotItem {
  segment_id: number;
  street_name: string;
  decaying_hotspot_score: number;
  latest_velocity_kmh: number;
  latest_los: string;
  alert_severity?: string;
  coords?: CoordsTuple | null;
}

export interface QuarantineRuleItem {
  rule_order: number;
  rule_id: string;
  edge_case: string;
  source_table: string;
  flagged_records: number;
  first_failing_quarantined: number;
  train_windows_affected: number;
  action: string;
}

export interface DatasetKpis {
  nodes_count?: number;
  active_intersection_nodes_count?: number;
  pruned_orphan_nodes_count?: number;
  streets_count?: number;
  segments_count?: number;
  line_graph_transition_edges?: number;
  dangling_nodes_count?: number;
  segment_status_events?: number;
  labeled_windows_count?: number;
  exact_t30m_supervised_windows_count?: number;
  distinct_active_segments?: number;
  cold_start_segments_count?: number;
  distinct_streets?: number;
}

export interface QualityFunnel {
  quarantined_segment_records?: number;
  zero_length_segments?: number;
  non_road_osm_segments?: number;
  status_input_pings?: number;
  status_dedup_dropped_pings?: number;
  status_accepted_pings?: number;
  status_rejected_pings?: number;
  raw_distinct_street_names?: number;
  canonical_nfc_street_names?: number;
  street_entities_merged_by_nfc?: number;
}

export interface TransitionAnalysis {
  test_samples?: number;
  steady_state_samples?: number;
  steady_state_ratio_pct?: number;
  transition_samples?: number;
  transition_ratio_pct?: number;
  persistence_transition_6class_acc?: number;
  rf_23d_transition_6class_acc?: number;
  rf_23d_val_calibrated_transition_6class_acc?: number;
  val_selected_change_threshold?: number;
}

export interface EvaluationKpis {
  train_samples?: number;
  train_purged_boundary_samples?: number;
  val_samples?: number;
  val_purged_boundary_samples?: number;
  test_samples?: number;
  positive_severe_test_samples?: number;
  roc_auc?: number;
  average_precision?: number;
  accuracy_6class?: number;
  macro_f1_6class?: number;
  severe_f1?: number;
  transition_analysis?: TransitionAnalysis;
}

export interface OverviewResponse {
  is_demo_sample: boolean;
  dataset_kpis: DatasetKpis;
  evaluation_kpis: EvaluationKpis;
  quality_funnel: QualityFunnel;
  quarantine_rules: QuarantineRuleItem[];
  los_distribution: Record<string, number>;
  ewma_hotspots: HotspotItem[];
}

export interface PageRankTopItem {
  segment_id: number;
  s_node_id: number;
  e_node_id: number;
  street_name: string;
  street_level: number;
  pagerank_score: number;
  in_degree: number;
  out_degree: number;
  upstream_segment_id?: number | null;
  coords?: CoordsTuple | null;
  is_cold_start?: boolean;
  obs_windows?: number;
  mean_velocity?: number;
  latest_ict_date?: string | null;
  latest_vn_hour?: number | null;
}

export interface CardB2Row {
  test_category: string;
  entity_id: string;
  name_or_scope: string;
  pagerank_or_size: number;
  observed_windows: number;
  mean_velocity_kmh: number;
  max_velocity_kmh: number;
  velocity_ratio: number;
  empirical_los_or_span: string;
  raw_vs_data_crosscheck: string;
  revised_interpretation: string;
}

export interface CoCongestionRuleItem {
  antecedent_street: string;
  consequent_street: string;
  co_congested_windows?: number;
  support_count?: number;
  support: number;
  confidence: number;
  lift: number;
  rule_type?: string;
}

export interface LaggedRuleItem {
  antecedent_street_t?: string;
  source_street_t?: string;
  antecedent_street?: string;
  consequent_street_t30m?: string;
  target_street_t30m?: string;
  consequent_street?: string;
  lagged_transition_windows?: number;
  support_count?: number;
  support: number;
  confidence: number;
  lift: number;
}

export interface GraphRulesResponse {
  pagerank_top: PageRankTopItem[];
  card_b2_falsification: CardB2Row[];
  co_congestion_rules: CoCongestionRuleItem[];
  lagged_rules: LaggedRuleItem[];
}

export interface ClusterSummaryItem {
  cluster_id: number;
  total_segments: number;
  observed_segments: number;
  cold_start_segments: number;
  mean_velocity_kmh: number;
  mean_pagerank: number;
}

export interface ColdStartCandidate {
  segment_id: number;
  street_name: string;
  street_level: number;
  cluster_id: number;
  pagerank_score: number;
}

export interface SelectedSegmentColdStart {
  segment_id: number;
  street_name: string;
  street_level: number;
  cluster_id: number;
  is_cold_start: boolean;
  pagerank_score: number;
  max_velocity: number;
  structural_prior_predicted_los: string;
  structural_prior_severe_prob: number;
  badge: string;
  coords: CoordsTuple | null;
}

export interface PeerSegmentItem {
  segment_id: number;
  street_name: string;
  street_level?: number;
  street_type?: string;
  length_m?: number;
  cluster_id: number;
  mean_velocity: number;
  max_velocity?: number;
  pagerank_score: number;
  similarity_score?: number;
}

export interface ClusterMapPoint {
  segment_id: number;
  street_name: string;
  cluster_id: number;
  is_cold_start: boolean;
  pagerank_score: number;
  coords: CoordsTuple | null;
}

export interface ClustersColdStartResponse {
  total_network_segments: number;
  observed_segments_count: number;
  cold_start_segments_count: number;
  silhouette_score: number;
  train_segments_clustered?: number;
  cluster_features?: string[];
  clusters: ClusterSummaryItem[];
  cold_start_candidates: ColdStartCandidate[];
  selected_segment: SelectedSegmentColdStart;
  peers: PeerSegmentItem[];
  map_sample: ClusterMapPoint[];
}

export interface StreamingMmdsMetrics {
  total_stream_events_processed?: number;
  valid_unique_events?: number;
  duplicates_removed_exact?: number;
  reconciliation?: {
    in_watermark_like_for_like_match_pct?: number;
    tier1_exceptAll_diff_count?: number;
    affected_late_segment_windows_recomputed?: number;
    healed_downstream_lag_windows?: number;
    pre_reconciliation_completeness_pct?: number;
    post_reconciliation_completeness_pct?: number;
  };
}

export interface SparkCatalystJoinProof {
  baseline_median_ms?: number;
  baseline_iqr_ms?: number;
  optimized_median_ms?: number;
  optimized_iqr_ms?: number;
  join_speedup_pct?: number;
  baseline_exchange_stages_measured?: number;
  baseline_shuffle_joins_measured?: number;
  optimized_exchange_stages_measured?: number;
  optimized_broadcast_joins_measured?: number;
  broadcast_hash_join_present?: boolean;
  two_way_exceptAll_diff?: number;
}

export interface SparkMonoidSaltingProof {
  unsalted_windows?: number;
  stage1_salted_groups?: number;
  stage2_reduced_windows?: number;
  unsalted_median_ms?: number;
  salted_median_ms?: number;
  unsalted_iqr_ms?: number;
  salted_iqr_ms?: number;
  two_way_exceptAll_diff?: number;
}

export interface SparkRuntimeVerification {
  status?: string;
  spark_version?: string;
  catalyst_join_spark?: SparkCatalystJoinProof;
  monoid_salting_spark?: SparkMonoidSaltingProof;
}

export interface MonoidAndCatalystMetrics {
  pillar_i1_monoid_and_salting?: {
    salt_buckets?: number;
    unsalted_peak_to_median_skew_ratio?: number;
    peak_slot_salted_bucket_skew_ratio?: number;
    salt_bucket_cv?: number;
  };
  pillar_i2_catalyst_join_optimization?: {
    baseline_exchange_stages?: number;
    optimized_exchange_stages?: number;
    dataframe_vs_sql_exceptAll_diff?: number;
  };
  spark_runtime_verification?: SparkRuntimeVerification;
}

export interface AblationResultRow {
  model: string;
  task_type?: string;
  feature_set: string;
  accuracy_6class: number | null;
  macro_f1_6class: number | null;
  weighted_f1_6class?: number | null;
  severe_roc_auc: number | null;
  severe_pr_auc: number | null;
}

export interface TransitionEvaluationRow {
  regime: string;
  sample_count: number;
  persistence_6class_acc?: number | null;
  persistence_acc?: number | null;
  rf_23d_6class_acc?: number | null;
  rf_23d_acc?: number | null;
  dt_23d_6class_acc?: number | null;
  rf_23d_severe_pr_auc?: number | null;
  gbt_23d_severe_pr_auc?: number | null;
}

export interface SparkBenchmarkRow {
  run: string;
  configuration: string;
  input_size_partitions?: string;
  runtime_ms: number;
  iqr_ms?: number;
  exchange_shuffle_stages: number;
  peak_to_median_partition_skew: number;
  two_way_exceptAll_diff: number;
  observation?: string;
}

export interface ReconciliationAuditRow {
  date: string;
  period: string;
  batch_gold_events: number;
  dropped_late_events: number;
  pre_recon_completeness_pct: number;
  post_recon_completeness_pct: number;
}

export interface CardB1PitRow {
  forecast_origin_ict?: string;
  feature_idx: number;
  feature_name: string;
  feature_group?: string;
  source_window_rule: string;
  uses_target_interval_t_to_t30m: string;
  pass_fail: string;
}

export interface CardB3SketchRow {
  test_slice: string;
  exact_spark_sql_value: number;
  sketch_estimated_value: number;
  relative_error_or_fpr_pct: number;
  memory_footprint: string;
}

export interface WalProgressInfo {
  simulated_kill_after_batch?: number;
  restart_recovery_verified?: boolean;
  duplicate_side_effects_on_restart?: number;
  healed_downstream_rescored_windows?: number;
}

export interface MongoLatencyBenchmark {
  cold_query_ms?: number | null;
  warm_min_ms?: number | null;
  warm_median_ms?: number | null;
  warm_p95_ms?: number | null;
  warm_max_ms?: number | null;
  collscan_warm_median_ms?: number | null;
  collscan_warm_p95_ms?: number | null;
  ixscan_vs_collscan_speedup_x?: number | null;
  speedup_vs_collscan_x?: number | null;
  sla_under_10ms_verified?: boolean;
}

export interface MongoVerifyInfo {
  status?: string;
  verified_against_mongo_8_0?: boolean;
  mongo_server_version?: string;
  total_rows_upserted?: number;
  stale_v1_rejected_by_mongo?: number;
  idempotent_replay_verified?: boolean;
  index_used?: string;
  winning_stages?: string[];
  in_memory_sort?: boolean;
  execution_stats?: {
    nReturned?: number;
    totalKeysExamined?: number;
    totalDocsExamined?: number;
    executionTimeMillis?: number;
  };
  latency_benchmark_ms?: MongoLatencyBenchmark;
  uc2_arterial_query?: {
    total_matching_windows?: number;
    latency_benchmark_ms?: MongoLatencyBenchmark;
  };
}

export interface FrozenManifestInfo {
  sha256_digest?: string;
  model_version?: string;
  all_three_artifacts_immutable_verified?: boolean;
}

export interface InfrastructureAuditResponse {
  dataset?: DatasetKpis;
  evaluation?: EvaluationKpis;
  monoid_and_catalyst?: MonoidAndCatalystMetrics;
  cold_start_ratio_pct?: number;
  streaming_mmds: StreamingMmdsMetrics;
  quality_funnel: QualityFunnel;
  quarantine_rules: QuarantineRuleItem[];
  ablation_results: AblationResultRow[];
  transition_evaluation: TransitionEvaluationRow[];
  spark_benchmark: SparkBenchmarkRow[];
  reconciliation_audit: ReconciliationAuditRow[];
  card_b1_pit_matrix: CardB1PitRow[];
  card_b3_sketches: CardB3SketchRow[];
  wal_progress: WalProgressInfo;
  mongo_verify: MongoVerifyInfo;
  frozen_manifest: FrozenManifestInfo;
}

export interface PipelineTraceOption {
  id: string;
  segment_id: number;
  title: string;
  badge: string;
  slide_focus: string;
  ict_date?: string | null;
  vn_hour?: number | null;
  is_cold_start?: boolean;
}

export interface PipelineProbItem {
  los: string;
  prob: number;
}

export interface PipelineKeyValueItem {
  label: string;
  value: string;
}

export interface PipelineStageItem {
  stage_id: number;
  zone: string;
  slide_ref: string;
  title: string;
  subtitle: string;
  accent_color: string;
  status_badge: string;
  input_summary: PipelineKeyValueItem[];
  transform_steps: string[];
  output_summary: PipelineKeyValueItem[];
  key_metrics: PipelineKeyValueItem[];
  details?: Record<string, unknown>;
}

export interface PipelineTraceResponse {
  curated_traces: PipelineTraceOption[];
  active_trace_id: string;
  segment_id: number;
  street_name: string;
  ict_date: string | null;
  vn_hour: number | null;
  window_headline: string;
  is_cold_start: boolean;
  summary_verdict: string;
  stages: PipelineStageItem[];
}
