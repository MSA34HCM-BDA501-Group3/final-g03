import React, { useEffect, useState } from 'react';
import { LiveScoreRow, ScenarioItem, StreamStatusResponse } from '../types';
import { getLiveRowKey, LiveStreamMap } from './LiveStreamMap';

interface Props {
  streamStatus: StreamStatusResponse | null;
  scenarios: ScenarioItem[];
  kafkaStoryScenario: string;
  onSelectKafkaStoryScenario: (id: string) => void;
  isStreaming: boolean;
  onStartStoryKafka: () => void;
  onStartStressKafka: () => void;
  onResetKafka: () => void;
}

function formatHourLabel(vnHour?: number): string {
  const num = Number(vnHour ?? 0);
  if (!Number.isFinite(num)) return '—';
  const totalMins = ((Math.round(num * 60) % 1440) + 1440) % 1440;
  const hh = String(Math.floor(totalMins / 60)).padStart(2, '0');
  const mm = String(totalMins % 60).padStart(2, '0');
  return `${hh}:${mm}`;
}

export const TabLiveKafka: React.FC<Props> = ({
  streamStatus,
  scenarios,
  kafkaStoryScenario,
  onSelectKafkaStoryScenario,
  isStreaming,
  onStartStoryKafka,
  onStartStressKafka,
  onResetKafka,
}) => {
  const [selectedRowKey, setSelectedRowKey] = useState<string | null>(null);

  const tel = streamStatus?.telemetry || {};
  const parts = tel.partition_counts || { '0': 0, '1': 0, '2': 0 };
  const p0 = Number(parts['0'] ?? 0);
  const p1 = Number(parts['1'] ?? 0);
  const p2 = Number(parts['2'] ?? 0);
  const pTotal = Math.max(1, p0 + p1 + p2);
  const cursor = tel.story_cursor;
  const rows = streamStatus?.recent_scores || [];

  useEffect(() => {
    if (rows.length === 0 && selectedRowKey !== null) {
      setSelectedRowKey(null);
    }
  }, [rows.length, selectedRowKey]);

  const resolvedSelectedKey =
    selectedRowKey && rows.some((row) => getLiveRowKey(row) === selectedRowKey)
      ? selectedRowKey
      : null;

  const highlightedKey = resolvedSelectedKey ?? (rows[0] ? getLiveRowKey(rows[0]) : null);
  const activeRow = rows.find((row) => getLiveRowKey(row) === highlightedKey) || null;

  const cursorScenarioLabel = cursor
    ? scenarios.find((s) => s.id === cursor.scenario)?.short_label || cursor.scenario
    : '';
  const storyFinished = Boolean(cursor && cursor.total_frames > 0 && cursor.frame_idx >= cursor.total_frames);

  const handleStartStory = () => {
    setSelectedRowKey(null);
    onStartStoryKafka();
  };

  const handleStartStress = () => {
    setSelectedRowKey(null);
    onStartStressKafka();
  };

  const handleReset = () => {
    setSelectedRowKey(null);
    onResetKafka();
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 15 }}>
      <div className="card">
        <div className="card-header" style={{ flexWrap: 'wrap' }}>
          <div>
            <h2 className="card-title">
              Luồng Phát Lại Thời Gian Thực Đầu–Cuối Qua Apache Kafka 4.1.0 KRaft (3 Partitions)
            </h2>
            <p className="card-subtitle">
              Tầng tốc độ độc lập (Speed Layer): chọn kịch bản <b>🎬 Story Replay</b> hoặc chạy{' '}
              <b>⚡ Stress-Test 200 tin</b> để kiểm chứng 32 KiB Bloom, 1 KiB HLL++, Watermark 30p và quan sát dự báo{' '}
              <code>STREAM_PROVISIONAL (v1)</code> chảy trực tiếp lên bản đồ Live bên dưới
            </p>
          </div>
          <div className="kafka-control-bar">
            <select
              className="select-light"
              aria-label="Chọn kịch bản phát Story qua Kafka"
              value={kafkaStoryScenario}
              onChange={(e) => onSelectKafkaStoryScenario(e.target.value)}
            >
              {scenarios.map((s) => (
                <option key={s.id} value={s.id}>
                  {s.short_label}
                </option>
              ))}
            </select>
            <button type="button" className="btn btn-emerald" onClick={handleStartStory}>
              🎬 {isStreaming ? 'Đang phát Story...' : 'Phát Story qua Kafka'}
            </button>
            <button type="button" className="btn btn-primary" onClick={handleStartStress}>
              ⚡ Phát 200 tin Stress-Test
            </button>
            <button type="button" className="btn btn-slate" onClick={handleReset}>
              🧹 Reset bộ đếm 0
            </button>
          </div>
        </div>

        {cursor && (
          <div className="live-story-banner">
            <span>
              🎬 <b>Đang đồng bộ Story qua Kafka:</b> Kịch bản <b>{cursorScenarioLabel}</b> · Khung{' '}
              <b>
                {cursor.frame_idx} / {cursor.total_frames}
              </b>{' '}
              ({cursor.ict_date} · giờ gốc {cursor.vn_hour}h)
            </span>
            <span className={`badge ${storyFinished ? 'badge-blue' : 'badge-green'}`}>
              {storyFinished ? '✓ Hoàn tất Story' : '● Đang stream v1'}
            </span>
          </div>
        )}

        <div className="kpi-row">
          <div className="kpi-card">
            <div className="kpi-label">Bản tin nhận qua Kafka</div>
            <div className="kpi-val">{(tel.messages_consumed ?? 0).toLocaleString()}</div>
            <div className="kpi-sub">
              P0: {p0} · P1: {p1} · P2: {p2}
            </div>
            <div style={{ display: 'flex', height: 6, borderRadius: 4, overflow: 'hidden', marginTop: 6, background: '#e2e8f0' }}>
              <div style={{ width: `${(p0 / pTotal) * 100}%`, background: '#2563eb' }} title={`Partition 0: ${p0}`} />
              <div style={{ width: `${(p1 / pTotal) * 100}%`, background: '#059669' }} title={`Partition 1: ${p1}`} />
              <div style={{ width: `${(p2 / pTotal) * 100}%`, background: '#f59e0b' }} title={`Partition 2: ${p2}`} />
            </div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">32.0 KiB Bit-Packed Bloom</div>
            <div className="kpi-val" style={{ color: '#d97706' }}>
              {tel.duplicates_caught_bloom ?? 0} tin trùng
            </div>
            <div className="kpi-sub">2 × 131,072 bits (32 KiB) · k=10 hashes (O(1) RAM)</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">Watermark 30 Phút (Sink 2)</div>
            <div className="kpi-val" style={{ color: '#6366f1' }}>
              {tel.late_dropped_for_tier2 ?? 0} tin trễ &gt;30p
            </div>
            <div className="kpi-sub">Giữ ở Sink 1 chờ Tier-2 Reconciliation bù 100%</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">1.0 KiB HyperLogLog++</div>
            <div className="kpi-val" style={{ color: '#0891b2' }}>
              {Math.round(tel.hll_estimated_segments ?? 0)} đoạn
            </div>
            <div className="kpi-sub">Đếm chính xác thực tế: {tel.exact_distinct_segments ?? 0} đoạn</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">Dự báo v1 ghi MongoDB 8.0</div>
            <div className="kpi-val" style={{ color: '#059669' }}>
              {tel.sink2_scored_v1 ?? 0} bản ghi
            </div>
            <div className="kpi-sub">
              Nguy cơ E/F: {tel.severe_alerts_v1_count ?? 0} · Khóa bảo vệ v2: {tel.v2_guard_protected_count ?? 0}
            </div>
          </div>
        </div>
      </div>

      <div className="grid-hero">
        <div className="card">
          <div className="card-header">
            <div>
              <h2 className="card-title">1. Bản Đồ Luồng Live Kafka (STREAM_PROVISIONAL v1)</h2>
              <p className="card-subtitle">
                Mỗi đoạn đường vừa được Kafka ➔ Sketches ➔ RF 23D chấm điểm sẽ hiển thị trực tiếp ngay tại đây
              </p>
            </div>
            <span className="badge badge-blue">
              {activeRow ? `Đang chọn #${activeRow.segment_id}` : 'Chờ luồng Live'}
            </span>
          </div>

          <LiveStreamMap
            rows={rows}
            selectedKey={resolvedSelectedKey}
            onSelectRow={setSelectedRowKey}
            isStreaming={isStreaming}
          />
        </div>

        <div className="card">
          <div className="card-header">
            <div>
              <h2 className="card-title">
                2. Bảng Sự Kiện STREAM_PROVISIONAL (v1) Đang Chảy Vào MongoDB 8.0
              </h2>
              <p className="card-subtitle">
                Nguồn <code>bda501.live_stream_scores</code> · Nhấp vào bất kỳ dòng nào để phóng ngay trên Bản đồ Live bên trái
              </p>
            </div>
            <span className="badge badge-green">Cập nhật lúc {tel.last_updated_ict || '—'} ICT</span>
          </div>

          {rows.length === 0 ? (
            <div style={{ padding: '36px 20px', textAlign: 'center', color: '#64748b' }}>
              Bộ đếm đang ở <b>0</b>. Hãy nhấn <b>🎬 Phát Story qua Kafka</b> hoặc <b>⚡ Phát 200 tin Stress-Test</b> ở trên để xem sự kiện đổ vào MongoDB 8.0!
            </div>
          ) : (
            <div className="table-scroll" style={{ maxHeight: 452 }}>
              <table className="toc-table">
                <thead>
                  <tr>
                    <th>STT · Part</th>
                    <th>Tuyến đường &amp; Vận tốc</th>
                    <th>LOS ➔ +30p</th>
                    <th>Xác suất RF</th>
                    <th>EWMA</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map((r: LiveScoreRow, idx: number) => {
                    const rowKey = getLiveRowKey(r);
                    const isSelected = rowKey === highlightedKey;
                    const curLos = String(r.current_los || 'A');
                    const predLos = String(r.cal_predicted_los || r.predicted_next_los || curLos);
                    const sevPct = (Number(r.severe_prob ?? 0) * 100).toFixed(1);
                    const chgPct = (Number(r.change_prob ?? 0) * 100).toFixed(1);
                    return (
                      <tr
                        key={`${rowKey}-${idx}`}
                        className={`clickable ${isSelected ? 'selected-row' : ''}`}
                        onClick={() => setSelectedRowKey(rowKey)}
                      >
                        <td style={{ whiteSpace: 'nowrap' }}>
                          <strong>#{String(r.seq_id ?? idx + 1)}</strong>{' '}
                          <span className="badge badge-blue" style={{ fontSize: '10px', padding: '1px 5px' }}>
                            P{String(r.kafka_partition ?? 0)}
                          </span>
                          <div style={{ fontSize: '10.5px', color: '#64748b' }}>
                            {String(r.received_at_ict || '—')}
                          </div>
                        </td>
                        <td>
                          <strong>{String(r.street_name)}</strong>{' '}
                          <span style={{ color: '#64748b', fontSize: '11px' }}>(#{String(r.segment_id)})</span>
                          <div style={{ fontSize: '10.5px', color: '#64748b' }}>
                            {String(r.ict_date || r.date || '—')} · {formatHourLabel(r.vn_hour)} ·{' '}
                            {Number(r.mean_velocity ?? 0).toFixed(1)}/{Number(r.max_velocity ?? 40)} km/h
                          </div>
                        </td>
                        <td style={{ whiteSpace: 'nowrap' }}>
                          <span className={`los-pill los-${curLos}`}>{curLos}</span>
                          <span style={{ margin: '0 4px', fontWeight: 800 }}>➔</span>
                          <span className={`los-pill los-${predLos}`}>{predLos}</span>
                        </td>
                        <td style={{ whiteSpace: 'nowrap' }}>
                          <strong style={{ color: Number(r.severe_prob ?? 0) >= 0.5 ? '#dc2626' : '#0f172a' }}>
                            E/F: {sevPct}%
                          </strong>
                          <div style={{ fontSize: '10.5px', color: '#475569' }}>Đổi pha: {chgPct}%</div>
                        </td>
                        <td style={{ whiteSpace: 'nowrap' }}>
                          <b>{Number(r.ewma_hotspot_score ?? 0).toFixed(2)}</b>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};
