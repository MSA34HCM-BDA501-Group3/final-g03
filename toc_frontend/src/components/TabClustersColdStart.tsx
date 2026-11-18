import React, { useEffect, useState } from 'react';
import {
  ClusterSummaryItem,
  ClustersColdStartResponse,
  ColdStartCandidate,
  PeerSegmentItem,
} from '../types';

export const TabClustersColdStart: React.FC = () => {
  const [data, setData] = useState<ClustersColdStartResponse | null>(null);
  const [selectedColdId, setSelectedColdId] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    const q = selectedColdId ? `?segment_id=${selectedColdId}&limit=8` : '?limit=8';
    fetch(`/api/clusters-coldstart${q}`)
      .then((r) => {
        if (!r.ok) throw new Error(`Lỗi máy chủ (${r.status}) khi tải dữ liệu phân cụm & Cold-Start.`);
        return r.json() as Promise<ClustersColdStartResponse>;
      })
      .then((d) => {
        if (!cancelled) {
          setData(d);
          setError(null);
        }
      })
      .catch((err: unknown) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : 'Không thể tải dữ liệu phân cụm K-Means.');
        }
      });
    return () => {
      cancelled = true;
    };
  }, [selectedColdId]);

  if (error) {
    return (
      <div className="card" role="alert" style={{ color: '#991b1b', background: '#fef2f2', borderColor: '#fecaca' }}>
        ⚠️ {error}
      </div>
    );
  }
  if (!data) return <div className="card">Đang tải cụm hành vi K-Means &amp; kho Cold-Start toàn mạng lưới...</div>;

  const sel = data.selected_segment;
  const totalSegments = Math.max(1, Number(data.total_network_segments) || 0);
  const obsPct = ((Number(data.observed_segments_count) / totalSegments) * 100).toFixed(2);
  const coldPct = ((Number(data.cold_start_segments_count) / totalSegments) * 100).toFixed(2);
  const clusterCount = data.clusters.length;
  const featDim = data.cluster_features?.length ?? 7;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 15 }}>
      <div className="kpi-row">
        <div className="kpi-card">
          <div className="kpi-label">Tổng mạng lưới TP.HCM</div>
          <div className="kpi-val">{Number(data.total_network_segments).toLocaleString()} đoạn</div>
          <div className="kpi-sub">100% có điểm PageRank &amp; nhóm {featDim}D K-Means</div>
        </div>
        <div className="kpi-card">
          <div className="kpi-label">Đoạn đã có cảm biến GPS</div>
          <div className="kpi-val" style={{ color: '#059669' }}>
            {Number(data.observed_segments_count).toLocaleString()} ({obsPct}%)
          </div>
          <div className="kpi-sub">Dùng làm nguồn tham chiếu hành vi thực tế</div>
        </div>
        <div className="kpi-card">
          <div className="kpi-label">Vùng mù Cold-Start (0 GPS)</div>
          <div className="kpi-val" style={{ color: '#d97706' }}>
            {Number(data.cold_start_segments_count).toLocaleString()} ({coldPct}%)
          </div>
          <div className="kpi-sub">Gắn nhãn minh bạch [COLD-START STRUCTURAL PRIOR]</div>
        </div>
        <div className="kpi-card">
          <div className="kpi-label">Số cụm hành vi {featDim}D K-Means</div>
          <div className="kpi-val" style={{ color: '#2563eb' }}>
            k = {clusterCount} Archetypes
          </div>
          <div className="kpi-sub">Không dùng tọa độ lat/lon (Phi liền kề địa lý — Card B2)</div>
        </div>
        <div className="kpi-card">
          <div className="kpi-label">Chất lượng Phân cụm TRAIN</div>
          <div className="kpi-val">{Number(data.silhouette_score).toFixed(4)}</div>
          <div className="kpi-sub">
            Học trên {data.train_segments_clustered != null ? data.train_segments_clustered.toLocaleString() : '—'} đoạn TRAIN (0 Data Leakage)
          </div>
        </div>
      </div>

      <div className="grid-2">
        <div className="card">
          <div className="card-header">
            <div>
              <h2 className="card-title">1. Thống Kê {clusterCount} Cụm Hình Thái &amp; Hành Vi Đường Phố ({featDim}D K-Means)</h2>
              <p className="card-subtitle">
                {featDim} đặc trưng: {(data.cluster_features || ['mean_los_ordinal', 'severe_rate', 'mean_velocity', 'velocity_ratio', 'length_m', 'max_velocity', 'pagerank_score']).join(', ')}
              </p>
            </div>
          </div>
          <div className="table-scroll">
            <table className="toc-table">
              <thead>
                <tr>
                  <th>Cụm Hành Vi</th>
                  <th>Tổng đoạn</th>
                  <th>Có GPS</th>
                  <th>Thiếu GPS (Cold-Start)</th>
                  <th>Vận tốc TB</th>
                  <th>PageRank TB</th>
                </tr>
              </thead>
              <tbody>
                {(data.clusters || []).map((c: ClusterSummaryItem) => (
                  <tr key={c.cluster_id}>
                    <td>
                      <strong>Archetype #{c.cluster_id}</strong>
                    </td>
                    <td>{Number(c.total_segments).toLocaleString()}</td>
                    <td>{Number(c.observed_segments).toLocaleString()}</td>
                    <td>{Number(c.cold_start_segments).toLocaleString()}</td>
                    <td>{c.mean_velocity_kmh} km/h</td>
                    <td>{c.mean_pagerank}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div className="card">
          <div className="card-header">
            <div>
              <h2 className="card-title">2. Suy Diễn Dự Báo &amp; Tra Cứu Đường Tương Đồng Cho Vùng Cold-Start (UC-5)</h2>
              <p className="card-subtitle">
                Trả lời câu hỏi bảo vệ 60s Card B: Hệ thống biết gì ở đoạn đường có 0 bản tin GPS?
              </p>
            </div>
            <span className="badge badge-amber">[{sel?.badge}]</span>
          </div>

          <div style={{ marginBottom: 10 }}>
            <label style={{ fontSize: '11.5px', fontWeight: 700, display: 'block', marginBottom: 4 }}>
              Chọn đoạn đường Cold-Start (0 bản tin GPS) cần tham chiếu:
            </label>
            <select
              style={{ width: '100%', padding: '7px 10px', borderRadius: 8, border: '1px solid #cbd5e1' }}
              value={sel?.segment_id || ''}
              onChange={(e) => setSelectedColdId(Number(e.target.value))}
            >
              {(data.cold_start_candidates || []).map((c: ColdStartCandidate) => (
                <option key={c.segment_id} value={c.segment_id}>
                  Đoạn #{c.segment_id} · {c.street_name} (Cấp {c.street_level} · PageRank {c.pagerank_score} · Cụm #
                  {c.cluster_id})
                </option>
              ))}
            </select>
          </div>

          {sel && (
            <div
              style={{
                background: '#fffbeb',
                border: '1px solid #fde68a',
                borderRadius: 9,
                padding: '10px 12px',
                marginBottom: 10,
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                flexWrap: 'wrap',
                gap: 8,
              }}
            >
              <div>
                <strong>
                  {sel.street_name} (Đoạn #{sel.segment_id})
                </strong>
                <div style={{ fontSize: '11.5px', color: '#92400e' }}>
                  Cấp đường {sel.street_level} · Vmax nội suy {sel.max_velocity} km/h · PageRank {sel.pagerank_score} ·
                  Cụm #{sel.cluster_id}
                </div>
              </div>
              <div style={{ textAlign: 'right' }}>
                <span className={`los-pill los-${sel.structural_prior_predicted_los}`}>
                  {sel.structural_prior_predicted_los}
                </span>
                <div style={{ fontSize: '11px', fontWeight: 700, color: '#b45309', marginTop: 2 }}>
                  Prior E/F: {(Number(sel.structural_prior_severe_prob) * 100).toFixed(1)}%
                </div>
              </div>
            </div>
          )}

          <div className="table-scroll" style={{ maxHeight: 205 }}>
            <table className="toc-table">
              <thead>
                <tr>
                  <th>Đoạn Tương Đồng (Có GPS)</th>
                  <th>Tuyến đường</th>
                  <th>Cụm</th>
                  <th>Vận tốc TB</th>
                  <th>PageRank</th>
                </tr>
              </thead>
              <tbody>
                {(data.peers || []).map((p: PeerSegmentItem) => (
                  <tr key={p.segment_id}>
                    <td>#{p.segment_id}</td>
                    <td>
                      <strong>{p.street_name}</strong>
                    </td>
                    <td>#{p.cluster_id}</td>
                    <td>{Number(p.mean_velocity ?? 0).toFixed(1)} km/h</td>
                    <td>{Number(p.pagerank_score ?? 0).toFixed(3)}</td>
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
