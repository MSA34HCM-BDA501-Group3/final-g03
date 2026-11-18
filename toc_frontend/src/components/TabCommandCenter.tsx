import React, { useState } from 'react';
import {
  AlertCategory,
  CityFrameResponse,
  HotspotItem,
  MongoTopDoc,
  SegmentTimelineResponse,
} from '../types';
import { InteractiveTrafficMap } from './InteractiveTrafficMap';
import { ForecastTimelineChart } from './ForecastTimelineChart';

interface Props {
  frameData: CityFrameResponse | null;
  timelineData: SegmentTimelineResponse | null;
  hotspots: HotspotItem[];
  selectedSegmentId: number;
  onSelectSegment: (segId: number) => void;
  alertFilter: string;
  onChangeAlertFilter: (f: string) => void;
}

const ALERT_BADGES: Record<AlertCategory, { text: string; cls: string }> = {
  ONSET_A_D_TO_SEVERE: { text: '🚨 Bùng Phát A-D ➔ E/F', cls: 'badge-red' },
  SEVERE_WORSENING_E_TO_F: { text: '🔴 Nặng Thêm E ➔ F', cls: 'badge-red' },
  WORSENING_TRANSITION: { text: '⚠️ Đang Xấu Đi', cls: 'badge-amber' },
  RECOVERING_TRANSITION: { text: '🟢 Đang Giảm Tải', cls: 'badge-green' },
  PERSISTENT_SEVERE: { text: '🔴 Đang Kẹt Cứng', cls: 'badge-red' },
  STABLE_FLOW: { text: '✓ Ổn Định', cls: 'badge-blue' },
};

function formatVnHourIct(vnHour: unknown): string {
  const num = Number(vnHour);
  if (!Number.isFinite(num)) return '—';
  const totalMins = ((Math.round(num * 60) % 1440) + 1440) % 1440;
  const hh = String(Math.floor(totalMins / 60)).padStart(2, '0');
  const mm = String(totalMins % 60).padStart(2, '0');
  return `${hh}:${mm} ICT`;
}

function lifecycleBadgeClass(status: string): string {
  if (status === 'STREAM_PROVISIONAL_V1') return 'badge-green';
  if (status === 'TRAIN_IN_SAMPLE') return 'badge-amber';
  return 'badge-blue';
}

export const TabCommandCenter: React.FC<Props> = ({
  frameData,
  timelineData,
  hotspots,
  selectedSegmentId,
  onSelectSegment,
  alertFilter,
  onChangeAlertFilter,
}) => {
  const [rightPanelMode, setRightPanelMode] = useState<'alerts' | 'ewma' | 'mongo'>('alerts');
  const sum = frameData?.summary;
  const sem = frameData?.window_semantics;
  const mb = frameData?.mongo_benchmark;
  const p95Label = mb?.benchmark_p95_ms != null ? `${mb.benchmark_p95_ms} ms` : '—';
  const medianLabel = mb?.benchmark_median_ms != null ? `${mb.benchmark_median_ms} ms` : '—';
  const collscanMedianLabel = mb?.collscan_warm_median_ms != null ? `${mb.collscan_warm_median_ms} ms` : '—';
  const speedupVal = mb?.speedup_vs_collscan_x;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 15 }}>
      <div className="kpi-row">
        <div className="kpi-card">
          <div className="kpi-label">Đoạn đường hoạt động tại khung {sem?.collect_window || 'hiện tại'}</div>
          <div className="kpi-val">{sum?.active_segments.toLocaleString() ?? '—'}</div>
          <div className="kpi-sub">
            Khóa chặt đúng 1 thời điểm t = {sem?.issue_time_ict ?? '—'}
            {sum?.live_matched_segments ? ` · 📡 ${sum.live_matched_segments} luồng Live` : ''}
          </div>
        </div>
        <div className="kpi-card">
          <div className="kpi-label">🚨 Cảnh báo Sớm Sắp Chuyển Nặng (+30p)</div>
          <div className="kpi-val" style={{ color: '#e11d48' }}>
            {sum?.early_warning_worsening_count ?? 0} đoạn
          </div>
          <div className="kpi-sub">Đang A–E nhưng dự báo xấu đi ở {sem?.forecast_window ?? '—'}</div>
        </div>
        <div className="kpi-card">
          <div className="kpi-label">🔴 Đang Ùn Tắc Kéo Dài (LOS E/F)</div>
          <div className="kpi-val" style={{ color: '#d97706' }}>
            {sum?.persistent_severe_count ?? 0} đoạn
          </div>
          <div className="kpi-sub">Đang kẹt LOS E/F tại khung t (khác EWMA UC-1)</div>
        </div>
        <div className="kpi-card">
          <div className="kpi-label">🟢 Dự Báo Sắp Giải Tỏa / Giảm Nhẹ</div>
          <div className="kpi-val" style={{ color: '#059669' }}>
            {sum?.recovering_count ?? 0} đoạn
          </div>
          <div className="kpi-sub">Hạ nhiệt trong 30 phút kế tiếp</div>
        </div>
        <div className="kpi-card">
          <div className="kpi-label">⚡ Tốc Độ Truy Vấn MongoDB 8.0 (UC-2)</div>
          <div className="kpi-val" style={{ color: '#2563eb', display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap' }}>
            <span>{mb?.live_query_ms != null ? `${mb.live_query_ms} ms` : p95Label}</span>
            {speedupVal != null && (
              <span className="badge badge-green" style={{ fontSize: '10.5px' }}>
                {speedupVal}x vs COLLSCAN
              </span>
            )}
          </div>
          <div className="kpi-sub">
            {mb?.mode ?? '—'} · IXSCAN median {medianLabel} · p95 {p95Label} ({mb?.total_docs.toLocaleString() ?? 0} docs)
          </div>
        </div>
      </div>

      <div className="grid-hero">
        <div className="card">
          <div className="card-header">
            <div>
              <h2 className="card-title">1. Bản Đồ Giao Thông Tương Tác Tại t = {sem?.issue_time_ict ?? '—'}</h2>
              <p className="card-subtitle">
                Nhấp vào bất kỳ đoạn đường hoặc nhánh giao lộ nào để xem chuỗi GPS thô &amp; nét đứt dự báo +30p bên dưới
              </p>
            </div>
            <span className="badge badge-blue">Đang chọn #{selectedSegmentId}</span>
          </div>
          <InteractiveTrafficMap
            segments={frameData?.map_segments || []}
            activeSegmentsTotal={sum?.active_segments}
            selectedSegmentId={selectedSegmentId}
            selectedTimeline={timelineData}
            hotspots={hotspots}
            onSelectSegment={onSelectSegment}
          />
        </div>

        <div className="card">
          <div className="card-header">
            <div>
              <h2 className="card-title">2. Bảng Điều Hành Cảnh Báo Sớm (+30 Phút · UC-2)</h2>
              <p className="card-subtitle">
                Mặc định ưu tiên <b>đường sắp chuyển nặng / đổi trạng thái</b> lên đầu thay vì chỉ liệt kê F ➔ F
              </p>
            </div>
          </div>

          <div className="filter-pills">
            <button
              className={`pill-btn ${alertFilter === 'early_warning' && rightPanelMode === 'alerts' ? 'active' : ''}`}
              onClick={() => {
                setRightPanelMode('alerts');
                onChangeAlertFilter('early_warning');
              }}
            >
              🚨 Cảnh báo sớm (+30p)
            </button>
            <button
              className={`pill-btn ${alertFilter === 'persistent_severe' && rightPanelMode === 'alerts' ? 'active' : ''}`}
              onClick={() => {
                setRightPanelMode('alerts');
                onChangeAlertFilter('persistent_severe');
              }}
            >
              🔴 Đang kẹt E/F ({sum?.persistent_severe_count ?? 0})
            </button>
            <button
              className={`pill-btn ${rightPanelMode === 'ewma' ? 'active' : ''}`}
              onClick={() => setRightPanelMode('ewma')}
            >
              🔥 Top {hotspots.length} EWMA (UC-1)
            </button>
            <button
              className={`pill-btn ${alertFilter === 'all' && rightPanelMode === 'alerts' ? 'active' : ''}`}
              onClick={() => {
                setRightPanelMode('alerts');
                onChangeAlertFilter('all');
              }}
            >
              Tất cả khung t
            </button>
            <button
              className={`pill-btn ${rightPanelMode === 'mongo' ? 'active' : ''}`}
              onClick={() => setRightPanelMode('mongo')}
            >
              ⚡ Mongo IXSCAN{speedupVal != null ? ` (${speedupVal}x)` : ''}
            </button>
          </div>

          {rightPanelMode === 'alerts' && (
            <div className="table-scroll">
              {frameData?.is_empty_slot || (frameData?.alerts || []).length === 0 ? (
                <div style={{ padding: '24px', textAlign: 'center', color: '#64748b' }}>
                  Không có đoạn đường quan sát ở khung giờ <b>{sem?.ict_date}</b> · <b>{sem?.collect_window}</b>.
                </div>
              ) : (
                <table className="toc-table">
                  <thead>
                    <tr>
                      <th>Tuyến đường</th>
                      <th>Hiện tại ➔ +30p</th>
                      <th>Thực tế</th>
                      <th>Đổi pha</th>
                      <th>Phân loại</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(frameData?.alerts || []).map((a) => {
                      const b = ALERT_BADGES[a.alert_type] || ALERT_BADGES.STABLE_FLOW;
                      const threshold = a.change_threshold ?? 0.3;
                      const predShow = a.effective_forecast_los ?? (a.change_prob >= threshold ? a.rf_cal_los : a.rf_pred_los);
                      return (
                        <tr
                          key={a.segment_id}
                          className={`clickable ${a.segment_id === selectedSegmentId ? 'selected-row' : ''}`}
                          onClick={() => onSelectSegment(a.segment_id)}
                        >
                          <td>
                            <strong>{a.street_name}</strong>{' '}
                            <span className={`badge ${lifecycleBadgeClass(a.lifecycle_status)}`} style={{ fontSize: '9.5px', padding: '1px 5px' }}>
                              {a.lifecycle_status}
                            </span>
                            <div style={{ fontSize: '10.5px', color: '#64748b' }}>
                              Đoạn #{a.segment_id} · {a.mean_velocity}/{a.max_velocity} km/h ({a.obs_count} pings)
                            </div>
                          </td>
                          <td>
                            <span className={`los-pill los-${a.current_los}`}>{a.current_los}</span>
                            <span style={{ margin: '0 5px', fontWeight: 800 }}>➔</span>
                            <span className={`los-pill los-${predShow}`}>{predShow}</span>
                          </td>
                          <td>
                            {a.actual_next_los ? (
                              <span className={`los-pill los-${a.actual_next_los}`}>{a.actual_next_los}</span>
                            ) : (
                              <span style={{ color: '#94a3b8' }}>—</span>
                            )}
                          </td>
                          <td>
                            <strong>{(a.change_prob * 100).toFixed(0)}%</strong>
                            <div style={{ fontSize: '10px', color: '#dc2626' }}>
                              E/F: {(a.severe_prob * 100).toFixed(0)}%
                            </div>
                          </td>
                          <td>
                            <span className={`badge ${b.cls}`}>{b.text}</span>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              )}
            </div>
          )}

          {rightPanelMode === 'ewma' && (
            <div className="table-scroll">
              <div style={{ fontSize: '11.5px', color: '#475569', marginBottom: 8 }}>
                <b>UC-1 — Top-{hotspots.length} Điểm Nóng Kẹt Dai Dẳng (EWMA α = 0.15):</b> Xếp hạng đoạn đường ùn tắc lặp lại nhiều khung liên tiếp để điều tiết lực lượng tại nút giao:
              </div>
              <table className="toc-table">
                <thead>
                  <tr>
                    <th>#</th>
                    <th>Tuyến đường &amp; Mã đoạn</th>
                    <th>Điểm EWMA</th>
                    <th>Vận tốc</th>
                    <th>LOS</th>
                  </tr>
                </thead>
                <tbody>
                  {hotspots.map((h, idx) => (
                    <tr
                      key={h.segment_id}
                      className={`clickable ${Number(h.segment_id) === selectedSegmentId ? 'selected-row' : ''}`}
                      onClick={() => onSelectSegment(Number(h.segment_id))}
                    >
                      <td>{idx + 1}</td>
                      <td>
                        <strong>{h.street_name}</strong> (#{h.segment_id})
                      </td>
                      <td>
                        <b style={{ color: '#dc2626' }}>{Number(h.decaying_hotspot_score).toFixed(2)}</b>
                      </td>
                      <td>{h.latest_velocity_kmh} km/h</td>
                      <td>
                        <span className={`los-pill los-${h.latest_los}`}>{h.latest_los}</span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {rightPanelMode === 'mongo' && (
            <div className="table-scroll">
              <div style={{ fontSize: '11.5px', color: '#475569', marginBottom: 8 }}>
                Truy vấn kiểm chứng tốc độ phục vụ trực tiếp trên <code>bda501.traffic_scores</code> với chỉ mục ESR{' '}
                <code>{'{street_level: 1, severe_prob: -1, segment_id: 1}'}</code> (0 SORT trên RAM · warm median ={' '}
                <b>{medianLabel}</b> · warm p95 = <b>{p95Label}</b>
                {speedupVal != null ? (
                  <>
                    {' '}
                    · tăng tốc <b style={{ color: '#059669' }}>{speedupVal}x</b> so với COLLSCAN {collscanMedianLabel}
                  </>
                ) : null}
                ):
              </div>
              <table className="toc-table">
                <thead>
                  <tr>
                    <th>Tuyến trục chính</th>
                    <th>Khung giờ (ICT)</th>
                    <th>LOS ➔ +30p</th>
                    <th>Nguy cơ E/F</th>
                    <th>Vòng đời</th>
                  </tr>
                </thead>
                <tbody>
                  {(mb?.top_docs || []).map((d: MongoTopDoc, i: number) => (
                    <tr key={d.event_id ?? `${d.segment_id}-${i}`} className="clickable" onClick={() => onSelectSegment(Number(d.segment_id))}>
                      <td>
                        <strong>{String(d.street_name)}</strong> (#{String(d.segment_id)})
                      </td>
                      <td>
                        {String(d.ict_date || d.date)} · {d.vn_hour != null ? formatVnHourIct(d.vn_hour) : String(d.period)}
                      </td>
                      <td>
                        {String(d.current_los)} ➔ <b>{String(d.predicted_next_los)}</b>
                      </td>
                      <td>{(Number(d.severe_prob) * 100).toFixed(1)}%</td>
                      <td>
                        <span className="badge badge-green">{String(d.lifecycle_status)}</span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>

      <ForecastTimelineChart timeline={timelineData} />
    </div>
  );
};
