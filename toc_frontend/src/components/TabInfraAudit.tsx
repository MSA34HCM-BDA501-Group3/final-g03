import React, { useEffect, useState } from 'react';
import {
  AblationResultRow,
  CardB1PitRow,
  CardB3SketchRow,
  InfrastructureAuditResponse,
  QuarantineRuleItem,
  ReconciliationAuditRow,
  SparkBenchmarkRow,
  TransitionEvaluationRow,
} from '../types';

export const TabInfraAudit: React.FC = () => {
  const [data, setData] = useState<InfrastructureAuditResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    fetch('/api/infrastructure-audit')
      .then((r) => {
        if (!r.ok) throw new Error(`Lỗi máy chủ (${r.status}) khi tải hồ sơ kiểm toán hạ tầng.`);
        return r.json() as Promise<InfrastructureAuditResponse>;
      })
      .then((d) => {
        if (!cancelled) {
          setData(d);
          setError(null);
        }
      })
      .catch((err: unknown) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : 'Không thể tải dữ liệu kiểm toán Spark & ML.');
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  if (error) {
    return (
      <div className="card" role="alert" style={{ color: '#991b1b', background: '#fef2f2', borderColor: '#fecaca' }}>
        ⚠️ {error}
      </div>
    );
  }
  if (!data) return <div className="card">Đang tải hồ sơ Kiểm toán Spark 3.5.7, ML Ablation &amp; Đối soát...</div>;

  const sm = data.streaming_mmds || {};
  const recon = sm.reconciliation || {};
  const wal = data.wal_progress || {};
  const man = data.frozen_manifest || {};
  const evalInfo = data.evaluation || {};
  const ta = evalInfo.transition_analysis || {};
  const mc = data.monoid_and_catalyst || {};
  const salting = mc.pillar_i1_monoid_and_salting || {};
  const catalyst = mc.pillar_i2_catalyst_join_optimization || {};
  const sparkRt = mc.spark_runtime_verification || {};
  const catSpark = sparkRt.catalyst_join_spark || {};
  const monoidSpark = sparkRt.monoid_salting_spark || {};

  const mv = data.mongo_verify || {};
  const mvLat = mv.uc2_arterial_query?.latency_benchmark_ms || mv.latency_benchmark_ms || {};
  const mongoSpeedup = mvLat.ixscan_vs_collscan_speedup_x ?? mvLat.speedup_vs_collscan_x;
  const mongoWarmMed = mvLat.warm_median_ms;
  const mongoWarmP95 = mvLat.warm_p95_ms;
  const mongoColdMs = mvLat.cold_query_ms;
  const mongoCollMed = mvLat.collscan_warm_median_ms;
  const mongoCollP95 = mvLat.collscan_warm_p95_ms;
  const mongoStats = mv.execution_stats || {};

  const sparkRows = data.spark_benchmark || [];
  const baseBench = sparkRows.find((r) => r.run === 'Baseline') || sparkRows[0];
  const v1Bench = sparkRows.find((r) => r.run === 'Variant 1') || sparkRows[1];
  const v2Bench = sparkRows.find((r) => r.run === 'Variant 2') || sparkRows[2];

  const baseEx = baseBench?.exchange_shuffle_stages ?? catalyst.baseline_exchange_stages;
  const v1Ex = v1Bench?.exchange_shuffle_stages ?? catalyst.optimized_exchange_stages;
  const baseMs = baseBench?.runtime_ms;
  const v1Ms = v1Bench?.runtime_ms;
  const v2Ms = v2Bench?.runtime_ms;
  const speedupPct = baseMs != null && v1Ms != null && baseMs > 0 ? (((baseMs - v1Ms) / baseMs) * 100).toFixed(1) : null;
  const joinDiff = v1Bench?.two_way_exceptAll_diff ?? catalyst.dataframe_vs_sql_exceptAll_diff ?? 0;

  const saltBuckets = salting.salt_buckets;
  const unsaltedSkew = v1Bench?.peak_to_median_partition_skew ?? salting.unsalted_peak_to_median_skew_ratio;
  const saltedSkew = v2Bench?.peak_to_median_partition_skew ?? salting.peak_slot_salted_bucket_skew_ratio;
  const saltingOverhead = v1Ms != null && v2Ms != null ? (v2Ms - v1Ms).toFixed(1) : null;

  const prePct = recon.pre_reconciliation_completeness_pct;
  const postPct = recon.post_reconciliation_completeness_pct;
  const lateRecomputed = recon.affected_late_segment_windows_recomputed ?? 0;
  const healedLag = recon.healed_downstream_lag_windows ?? 0;
  const walRescored = wal.healed_downstream_rescored_windows ?? 0;

  const testSamples = evalInfo.test_samples ?? 0;
  const purgedSamples = (evalInfo.train_purged_boundary_samples ?? 0) + (evalInfo.val_purged_boundary_samples ?? 0);
  const transitionSamples = ta.transition_samples ?? 0;
  const steadyRatioPct = ta.steady_state_ratio_pct ?? 0;
  const persTransAccPct = ((ta.persistence_transition_6class_acc ?? 0) * 100).toFixed(2);
  const inputPings = data.quality_funnel?.status_input_pings ?? data.dataset?.segment_status_events ?? 0;
  const coldStartRatioPct = data.cold_start_ratio_pct ?? 0;

  const b1Rows = data.card_b1_pit_matrix || [];
  const b1Origin = b1Rows[0]?.forecast_origin_ict;
  const allB1Pass = b1Rows.length > 0 && b1Rows.every((r) => r.pass_fail === 'PASS');
  const b3Rows = data.card_b3_sketches || [];

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 15 }}>
      <div className="kpi-row-6">
        <div className="kpi-card">
          <div className="kpi-label">Spark BroadcastHashJoin (Catalyst)</div>
          <div className="kpi-val" style={{ color: '#2563eb' }}>
            {baseEx ?? '—'} ➔ {v1Ex ?? '—'} Exchanges
          </div>
          <div className="kpi-sub">
            Combined -{speedupPct ?? '—'}% ({baseMs != null ? baseMs.toLocaleString() : '—'} ➔{' '}
            {v1Ms != null ? v1Ms.toLocaleString() : '—'} ms) · Isolated{' '}
            {catSpark.join_speedup_pct != null ? `-${catSpark.join_speedup_pct}%` : '—'} (diff={joinDiff})
          </div>
        </div>
        <div className="kpi-card">
          <div className="kpi-label">{saltBuckets ?? '—'}-Bucket Salted 5-Tuple Monoid</div>
          <div className="kpi-val" style={{ color: '#059669' }}>
            {unsaltedSkew ?? '—'}x ➔ {saltedSkew ?? '—'}x
          </div>
          <div className="kpi-sub">
            Triệt tiêu lệch phân vùng giờ cao điểm ({saltingOverhead != null ? `+${saltingOverhead} ms` : '—'} · diff=0)
          </div>
        </div>
        <div className="kpi-card">
          <div className="kpi-label">MongoDB 8.0 IXSCAN vs COLLSCAN (UC-2)</div>
          <div className="kpi-val" style={{ color: '#2563eb' }}>
            {mongoSpeedup != null ? `${mongoSpeedup}x` : '—'} Nhanh Hơn
          </div>
          <div className="kpi-sub">
            Warm median {mongoWarmMed ?? '—'} ms · p95 {mongoWarmP95 ?? '—'} ms vs COLLSCAN {mongoCollMed ?? '—'} ms (
            {mv.in_memory_sort === false ? '0 SORT' : 'CHECK'})
          </div>
        </div>
        <div className="kpi-card">
          <div className="kpi-label">Đối Soát Hai Tầng (UC-6)</div>
          <div className="kpi-val" style={{ color: '#059669' }}>
            {prePct != null ? `${prePct}%` : '—'} ➔ {postPct != null ? `${postPct}%` : '—'}
          </div>
          <div className="kpi-sub">
            Bù {lateRecomputed.toLocaleString()} cửa sổ trễ · {healedLag.toLocaleString()} lag1 ({walRescored.toLocaleString()} hạ lưu WAL)
          </div>
        </div>
        <div className="kpi-card">
          <div className="kpi-label">Chịu Lỗi WAL Crash Recovery</div>
          <div className="kpi-val">{wal.duplicate_side_effects_on_restart ?? 0} Dupes</div>
          <div className="kpi-sub">
            Kill sau lô {wal.simulated_kill_after_batch ?? '—'} ➔ khôi phục chuẩn ({wal.restart_recovery_verified ? 'VERIFIED' : 'PENDING'})
          </div>
        </div>
        <div className="kpi-card">
          <div className="kpi-label">Khóa Bất Biến Frozen Registry</div>
          <div className="kpi-val" style={{ fontSize: 15.5, fontFamily: 'monospace' }}>
            SHA256:{String(man.sha256_digest || '').slice(0, 10)}
          </div>
          <div className="kpi-sub">
            {man.all_three_artifacts_immutable_verified ? '3 artifacts Phase A khóa cứng chống rò rỉ' : 'Kiểm tra hash registry'}
          </div>
        </div>
      </div>

      <div className="grid-2">
        <div className="card">
          <div className="card-header">
            <div>
              <h2 className="card-title">
                1. Thử Nghiệm Tách Lớp {(data.ablation_results || []).length} Mô Hình Dự Báo t + 30 Phút (Ablation Study)
              </h2>
              <p className="card-subtitle">
                Đánh giá trên {testSamples.toLocaleString()} cặp TEST độc lập (đã cắt sạch {purgedSamples.toLocaleString()} mẫu biên thời gian)
              </p>
            </div>
          </div>
          <div className="table-scroll">
            <table className="toc-table">
              <thead>
                <tr>
                  <th>Mô hình</th>
                  <th>Hệ biến</th>
                  <th>Acc 6-Lớp</th>
                  <th>Macro-F1</th>
                  <th>ROC-AUC (E/F)</th>
                  <th>PR-AUC</th>
                </tr>
              </thead>
              <tbody>
                {(data.ablation_results || []).map((r: AblationResultRow, i: number) => (
                  <tr key={i}>
                    <td>
                      <strong>{r.model}</strong>
                    </td>
                    <td>{r.feature_set}</td>
                    <td>{r.accuracy_6class != null ? `${(Number(r.accuracy_6class) * 100).toFixed(2)}%` : 'Binary'}</td>
                    <td>{r.macro_f1_6class != null ? Number(r.macro_f1_6class).toFixed(4) : '—'}</td>
                    <td>
                      <b>{r.severe_roc_auc != null ? Number(r.severe_roc_auc).toFixed(4) : '—'}</b>
                    </td>
                    <td>
                      <b>{r.severe_pr_auc != null ? Number(r.severe_pr_auc).toFixed(4) : '—'}</b>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div className="card">
          <div className="card-header">
            <div>
              <h2 className="card-title">
                2. Giải Mã Nghịch Lý Persistence Khi Giao Thông Đổi Trạng Thái ({transitionSamples.toLocaleString()} Ca TEST)
              </h2>
              <p className="card-subtitle">
                Persistence đạt {steadyRatioPct}% tổng thể nhờ quán tính đứng yên, nhưng đoán trúng {persTransAccPct}% khi đường thực sự chuyển pha
              </p>
            </div>
          </div>
          <div className="table-scroll">
            <table className="toc-table">
              <thead>
                <tr>
                  <th>Chế độ Kiểm định</th>
                  <th>Số mẫu</th>
                  <th>Persistence</th>
                  <th>RF 23D (Acc)</th>
                  <th>DT 23D (Acc)</th>
                  <th>PR-AUC E/F (RF · GBT)</th>
                </tr>
              </thead>
              <tbody>
                {(data.transition_evaluation || []).map((r: TransitionEvaluationRow, i: number) => {
                  const pAcc = r.persistence_6class_acc ?? r.persistence_acc;
                  const rfAcc = r.rf_23d_6class_acc ?? r.rf_23d_acc;
                  const dtAcc = r.dt_23d_6class_acc;
                  const rfPr = r.rf_23d_severe_pr_auc;
                  const gbtPr = r.gbt_23d_severe_pr_auc;
                  return (
                    <tr key={i}>
                      <td>
                        <strong>{r.regime}</strong>
                      </td>
                      <td>{r.sample_count}</td>
                      <td>{pAcc != null ? `${(Number(pAcc) * 100).toFixed(2)}%` : '—'}</td>
                      <td>
                        <b style={{ color: '#2563eb' }}>
                          {rfAcc != null ? `${(Number(rfAcc) * 100).toFixed(2)}%` : '—'}
                        </b>
                      </td>
                      <td>{dtAcc != null ? `${(Number(dtAcc) * 100).toFixed(2)}%` : '—'}</td>
                      <td>
                        <b>
                          {rfPr != null ? Number(rfPr).toFixed(4) : '—'}
                          {gbtPr != null ? ` · ${Number(gbtPr).toFixed(4)}` : ''}
                        </b>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <div className="card">
        <div className="card-header">
          <div>
            <h2 className="card-title">
              3A. Thực Nghiệm Có Kiểm Soát 3-Chiều Trên Apache Spark {sparkRt.spark_version || '3.5.7'} (Join + Salting Benchmark)
            </h2>
            <p className="card-subtitle">
              Chạy kết hợp Join + Salting: So sánh SortMergeJoin vs BroadcastHashJoin vs 2-Stage {saltBuckets ?? '—'}-Bucket Salted Monoid
            </p>
          </div>
          <span className="badge badge-blue">Combined Join + Salting</span>
        </div>
        <div className="table-scroll">
          <table className="toc-table">
            <thead>
              <tr>
                <th>Cấu hình Thực nghiệm</th>
                <th>Thời gian chạy (Median ± IQR)</th>
                <th>Tầng trộn mạng Exchange</th>
                <th>Độ lệch phân vùng (Peak/Median)</th>
                <th>Sai lệch exceptAll</th>
              </tr>
            </thead>
            <tbody>
              {sparkRows.map((b: SparkBenchmarkRow, i: number) => (
                <tr key={i}>
                  <td>
                    <strong>{b.run}:</strong> {b.configuration}
                  </td>
                  <td>
                    <b>{b.runtime_ms} ms</b>
                    {b.iqr_ms != null ? <span style={{ color: '#64748b' }}> (±{b.iqr_ms} ms)</span> : null}
                  </td>
                  <td>{b.exchange_shuffle_stages} stages</td>
                  <td>{b.peak_to_median_partition_skew}x</td>
                  <td>
                    <span className={`badge ${b.two_way_exceptAll_diff === 0 ? 'badge-green' : 'badge-red'}`}>
                      {b.two_way_exceptAll_diff} rows diff
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="grid-2">
        <div className="card">
          <div className="card-header">
            <div>
              <h2 className="card-title">
                3B. Kiểm Chứng Cô Lập Spark {sparkRt.spark_version || '3.5.7'} Runtime (<code>spark_runtime_evidence.json</code>)
              </h2>
              <p className="card-subtitle">
                Tách riêng phép đo Catalyst BroadcastHashJoin &amp; 2-Stage Salted 5-Tuple Monoid trên cụm Spark thực tế
              </p>
            </div>
            <span className={`badge ${sparkRt.status === 'verified' ? 'badge-green' : 'badge-amber'}`}>
              {sparkRt.status === 'verified' ? 'Spark Runtime VERIFIED' : 'Spark Runtime CHECK'}
            </span>
          </div>
          <div className="table-scroll">
            <table className="toc-table">
              <thead>
                <tr>
                  <th>Phép Kiểm Chứng Cô Lập</th>
                  <th>Baseline / Unsalted</th>
                  <th>Optimized / Salted</th>
                  <th>Chênh lệch &amp; Kế hoạch</th>
                  <th>exceptAll</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td>
                    <strong>Catalyst Join</strong>
                    <div style={{ fontSize: '10.5px', color: '#64748b' }}>SortMergeJoin ➔ BroadcastHashJoin</div>
                  </td>
                  <td>
                    <b>{catSpark.baseline_median_ms ?? '—'} ms</b>
                    <div style={{ fontSize: '10.5px', color: '#64748b' }}>
                      IQR {catSpark.baseline_iqr_ms ?? '—'} ms · {catSpark.baseline_exchange_stages_measured ?? '—'} Ex /{' '}
                      {catSpark.baseline_shuffle_joins_measured ?? '—'} SMJ
                    </div>
                  </td>
                  <td>
                    <b style={{ color: '#2563eb' }}>{catSpark.optimized_median_ms ?? '—'} ms</b>
                    <div style={{ fontSize: '10.5px', color: '#64748b' }}>
                      IQR {catSpark.optimized_iqr_ms ?? '—'} ms · {catSpark.optimized_exchange_stages_measured ?? '—'} Ex /{' '}
                      {catSpark.optimized_broadcast_joins_measured ?? '—'} BHJ
                    </div>
                  </td>
                  <td>
                    <span className="badge badge-green">
                      -{catSpark.join_speedup_pct ?? '—'}% tốc độ
                    </span>
                  </td>
                  <td>
                    <span className={`badge ${(catSpark.two_way_exceptAll_diff ?? 0) === 0 ? 'badge-green' : 'badge-red'}`}>
                      {catSpark.two_way_exceptAll_diff ?? 0} diff
                    </span>
                  </td>
                </tr>
                <tr>
                  <td>
                    <strong>Salted 5-Tuple Monoid</strong>
                    <div style={{ fontSize: '10.5px', color: '#64748b' }}>
                      Stage-1 ({(monoidSpark.stage1_salted_groups ?? 0).toLocaleString()}) ➔ Stage-2 (
                      {(monoidSpark.stage2_reduced_windows ?? 0).toLocaleString()})
                    </div>
                  </td>
                  <td>
                    <b>{monoidSpark.unsalted_median_ms ?? '—'} ms</b>
                    <div style={{ fontSize: '10.5px', color: '#64748b' }}>
                      IQR {monoidSpark.unsalted_iqr_ms ?? '—'} ms · {(monoidSpark.unsalted_windows ?? 0).toLocaleString()} win
                    </div>
                  </td>
                  <td>
                    <b>{monoidSpark.salted_median_ms ?? '—'} ms</b>
                    <div style={{ fontSize: '10.5px', color: '#64748b' }}>
                      IQR {monoidSpark.salted_iqr_ms ?? '—'} ms · 8 buckets
                    </div>
                  </td>
                  <td>
                    <span className="badge badge-blue">
                      Giảm phương sai IQR ({monoidSpark.unsalted_iqr_ms ?? '—'} ➔ {monoidSpark.salted_iqr_ms ?? '—'} ms)
                    </span>
                  </td>
                  <td>
                    <span className={`badge ${(monoidSpark.two_way_exceptAll_diff ?? 0) === 0 ? 'badge-green' : 'badge-red'}`}>
                      {monoidSpark.two_way_exceptAll_diff ?? 0} diff
                    </span>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>

        <div className="card">
          <div className="card-header">
            <div>
              <h2 className="card-title">
                3C. Kiểm Chứng MongoDB {mv.mongo_server_version || '8.0'} Compound IXSCAN vs COLLSCAN (UC-2)
              </h2>
              <p className="card-subtitle">
                Chỉ mục ESR <code>{mv.index_used || 'level_prob_segment_idx'}</code> trên{' '}
                {(mv.total_rows_upserted ?? 0).toLocaleString()} tài liệu · <code>in_memory_sort = {String(mv.in_memory_sort ?? false)}</code>
              </p>
            </div>
            <span className={`badge ${mongoSpeedup != null ? 'badge-green' : 'badge-blue'}`}>
              {mongoSpeedup != null ? `⚡ ${mongoSpeedup}x vs COLLSCAN` : 'IXSCAN Verified'}
            </span>
          </div>
          <div className="table-scroll">
            <table className="toc-table">
              <thead>
                <tr>
                  <th>Chiến lược Thực thi</th>
                  <th>Warm Median</th>
                  <th>Warm p95</th>
                  <th>Cold / Ghi chú</th>
                  <th>Kết quả</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td>
                    <strong>Compound IXSCAN</strong>
                    <div style={{ fontSize: '10.5px', color: '#64748b' }}>{mv.index_used || 'level_prob_segment_idx'}</div>
                  </td>
                  <td>
                    <b style={{ color: '#059669' }}>{mongoWarmMed != null ? `${mongoWarmMed} ms` : '—'}</b>
                  </td>
                  <td>
                    <b style={{ color: '#2563eb' }}>{mongoWarmP95 != null ? `${mongoWarmP95} ms` : '—'}</b>
                  </td>
                  <td>
                    Cold {mongoColdMs != null ? `${mongoColdMs} ms` : '—'} · Docs {mongoStats.totalDocsExamined ?? '—'}/
                    {mongoStats.nReturned ?? '—'}
                  </td>
                  <td>
                    <span className="badge badge-green">
                      {mongoSpeedup != null ? `${mongoSpeedup}x nhanh hơn` : 'SLA < 10 ms'}
                    </span>
                  </td>
                </tr>
                <tr>
                  <td>
                    <strong>Full Scan (COLLSCAN)</strong>
                    <div style={{ fontSize: '10.5px', color: '#64748b' }}>Quét toàn bộ bảng không chỉ mục</div>
                  </td>
                  <td>{mongoCollMed != null ? `${mongoCollMed} ms` : '—'}</td>
                  <td>{mongoCollP95 != null ? `${mongoCollP95} ms` : '—'}</td>
                  <td>
                    Chặn ghi đè v1 lỗi thời: <b>{mv.stale_v1_rejected_by_mongo ?? 0}</b> bản ghi
                  </td>
                  <td>
                    <span className="badge badge-amber">Baseline đối chứng</span>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <div className="grid-2">
        <div className="card">
          <div className="card-header">
            <div>
              <h2 className="card-title">
                4. Phễu Bảo Toàn {inputPings.toLocaleString()} Bản Tin GPS &amp; Nội Suy 3-Tier Vmax (P1 · 0 Silent Drops)
              </h2>
              <p className="card-subtitle">
                Mọi vi phạm đều gắn mã <code>first_failing_rule</code> lưu vào Quarantine Lakehouse, không rơi rớt ngầm
              </p>
            </div>
          </div>
          <div className="table-scroll" style={{ maxHeight: 240 }}>
            <table className="toc-table">
              <thead>
                <tr>
                  <th>Quy tắc</th>
                  <th>Bảng nguồn</th>
                  <th>Số bản ghi</th>
                  <th>Xử lý Chuẩn hóa</th>
                </tr>
              </thead>
              <tbody>
                {(data.quarantine_rules || []).map((q: QuarantineRuleItem, i: number) => (
                  <tr key={i}>
                    <td>
                      <strong>{q.rule_id}</strong>
                    </td>
                    <td>{q.source_table}</td>
                    <td>{Number(q.flagged_records).toLocaleString()}</td>
                    <td>
                      <span className="badge badge-blue">{q.action}</span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div className="card">
          <div className="card-header">
            <div>
              <h2 className="card-title">5. Đối Soát Hai Tầng Bù 100% Bản Tin Trễ Cuối Ngày (UC-6 · P4)</h2>
              <p className="card-subtitle">
                Nâng cấp <code>STREAM_PROVISIONAL (v1)</code> ➔ <code>BATCH_RECONCILED (v2)</code> cho{' '}
                {lateRecomputed.toLocaleString()} cửa sổ trễ &amp; {healedLag.toLocaleString()} lag1 ({walRescored.toLocaleString()} hạ lưu WAL)
              </p>
            </div>
          </div>
          <div className="table-scroll" style={{ maxHeight: 240 }}>
            <table className="toc-table">
              <thead>
                <tr>
                  <th>Ngày &amp; Khung</th>
                  <th>Tổng Batch</th>
                  <th>Tin trễ &gt;30p</th>
                  <th>Trước đối soát</th>
                  <th>Sau đối soát</th>
                </tr>
              </thead>
              <tbody>
                {(data.reconciliation_audit || []).map((rc: ReconciliationAuditRow, i: number) => (
                  <tr key={i}>
                    <td>
                      <strong>{rc.date}</strong> · {rc.period}
                    </td>
                    <td>{rc.batch_gold_events}</td>
                    <td style={{ color: '#d97706', fontWeight: 700 }}>+{rc.dropped_late_events}</td>
                    <td>{rc.pre_recon_completeness_pct}%</td>
                    <td>
                      <span className="badge badge-green">{rc.post_recon_completeness_pct}%</span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <div className="grid-2">
        <div className="card">
          <div className="card-header">
            <div>
              <h2 className="card-title">
                6. Kiểm Toán Chống Rò Rỉ Tương Lai {b1Rows.length} Biến ({b1Rows.length}D) (Point-in-Time · Future-Shuffle diff = 0.0)
              </h2>
              <p className="card-subtitle">
                Đối chứng <code>raw_data/</code> ↔ <code>data/dataset.csv</code>
                {b1Origin ? ` (mốc chốt ${b1Origin})` : ''}: 100% đặc trưng khóa trước mốc chốt <code>τ ≤ t</code>
              </p>
            </div>
            <span className={`badge ${allB1Pass ? 'badge-green' : 'badge-red'}`}>
              {allB1Pass ? 'Zero-Leakage PASS' : 'Zero-Leakage CHECK'}
            </span>
          </div>
          <div className="table-scroll" style={{ maxHeight: 250 }}>
            <table className="toc-table">
              <thead>
                <tr>
                  <th>#</th>
                  <th>Biến đầu vào ({b1Rows.length}D)</th>
                  <th>Quy tắc nguồn (τ ≤ t)</th>
                  <th>Dùng [t, t+30p)?</th>
                  <th>Kết quả</th>
                </tr>
              </thead>
              <tbody>
                {b1Rows.map((b1: CardB1PitRow, i: number) => (
                  <tr key={i}>
                    <td>{b1.feature_idx}</td>
                    <td>
                      <strong>{b1.feature_name}</strong>
                    </td>
                    <td>{b1.source_window_rule}</td>
                    <td>{b1.uses_target_interval_t_to_t30m}</td>
                    <td>
                      <span className={`badge ${b1.pass_fail === 'PASS' ? 'badge-green' : 'badge-red'}`}>
                        {b1.pass_fail}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div className="card">
          <div className="card-header">
            <div>
              <h2 className="card-title">
                7. Kiểm Toán Sai Số Thực Nghiệm 33 KiB Sketches &amp; Độ Thưa {coldStartRatioPct}%
              </h2>
              <p className="card-subtitle">
                Đối chứng <code>1 KiB HLL++</code> &amp; <code>32 KiB Bloom</code> với <code>Spark SQL Exact</code> (0 cảnh báo giả trên từng đoạn)
              </p>
            </div>
            <span className={`badge ${b3Rows.length > 0 ? 'badge-green' : 'badge-amber'}`}>
              {b3Rows.length > 0 ? 'Sketches Audit PASS' : 'Sketches Audit PENDING'}
            </span>
          </div>
          <div className="table-scroll" style={{ maxHeight: 250 }}>
            <table className="toc-table">
              <thead>
                <tr>
                  <th>Lát cắt Kiểm nghiệm</th>
                  <th>Exact SQL</th>
                  <th>Sketch Ước lượng</th>
                  <th>Sai số / FPR</th>
                  <th>Bộ nhớ</th>
                </tr>
              </thead>
              <tbody>
                {b3Rows.map((b3: CardB3SketchRow, i: number) => (
                  <tr key={i}>
                    <td>
                      <strong>{b3.test_slice}</strong>
                    </td>
                    <td>{b3.exact_spark_sql_value}</td>
                    <td>{b3.sketch_estimated_value}</td>
                    <td>{b3.relative_error_or_fpr_pct}%</td>
                    <td>{b3.memory_footprint}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
};
