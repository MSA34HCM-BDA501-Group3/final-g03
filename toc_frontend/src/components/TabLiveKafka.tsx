import React from 'react';
import { LiveScoreRow, StreamStatusResponse } from '../types';

interface Props {
  streamStatus: StreamStatusResponse | null;
  onStartStoryKafka: () => void;
  onStartStressKafka: () => void;
  onResetKafka: () => void;
  onJumpToSegment: (segId: number, ictDate?: string, vnHour?: number) => void;
}

export const TabLiveKafka: React.FC<Props> = ({
  streamStatus,
  onStartStoryKafka,
  onStartStressKafka,
  onResetKafka,
  onJumpToSegment,
}) => {
  const tel = streamStatus?.telemetry || {};
  const parts = tel.partition_counts || { '0': 0, '1': 0, '2': 0 };
  const p0 = Number(parts['0'] ?? 0);
  const p1 = Number(parts['1'] ?? 0);
  const p2 = Number(parts['2'] ?? 0);
  const pTotal = Math.max(1, p0 + p1 + p2);
  const cursor = tel.story_cursor;
  const rows = streamStatus?.recent_scores || [];

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 15 }}>
      <div className="card">
        <div className="card-header">
          <div>
            <h2 className="card-title">
              Luồng Phát Lại Thời Gian Thực Đầu–Cuối Qua Apache Kafka 4.1.0 KRaft (3 Partitions)
            </h2>
            <p className="card-subtitle">
              Hỗ trợ cả 2 chế độ: <b>🎬 Story-Driven Rush-Hour Replay</b> (phát tuần tự từng khung 30 phút làm đồng hồ TOC &amp; bản đồ chuyển pha động) và <b>⚡ Stress-Test 200 tin</b> kiểm chứng 32 KiB Bloom + 30m Watermark
            </p>
          </div>
          <div style={{ display: 'flex', gap: 8 }}>
            <button className="btn btn-emerald" onClick={onStartStoryKafka}>
              🎬 Phát Story qua Kafka
            </button>
            <button className="btn btn-primary" onClick={onStartStressKafka}>
              ⚡ Phát 200 tin Stress-Test
            </button>
            <button className="btn btn-slate" onClick={onResetKafka}>
              🧹 Reset bộ đếm 0
            </button>
          </div>
        </div>

        {cursor && (
          <div
            style={{
              background: '#ecfdf5',
              border: '1px solid #6ee7b7',
              borderRadius: 10,
              padding: '10px 14px',
              marginBottom: 12,
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              flexWrap: 'wrap',
              gap: 8,
            }}
          >
            <span>
              🎬 <b>Đang đồng bộ Story qua Kafka:</b> Kịch bản <code>{cursor.scenario}</code> · Khung{' '}
              <b>
                {cursor.frame_idx} / {cursor.total_frames}
              </b>{' '}
              ({cursor.ict_date} · giờ gốc {cursor.vn_hour}h)
            </span>
            <button
              className="btn btn-emerald"
              onClick={() => onJumpToSegment(cursor.spotlight_segment_id, cursor.ict_date, cursor.vn_hour)}
            >
              🔍 Mở ngay trên Bản đồ &amp; Biểu đồ dự báo
            </button>
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
            <div className="kpi-sub">262,144 bits · k=4 hashes (O(1) RAM)</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">Watermark 30 Phút (Sink 2)</div>
            <div className="kpi-val" style={{ color: '#6366f1' }}>
              {tel.late_dropped_for_tier2 ?? 0} tin trễ &gt;30p
            </div>
            <div className="kpi-sub">Giữ ở Sink 1 chờ Tier-2 Reconciliation bù 100%</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">1.0 KiB HyperLogLog++ (B3)</div>
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

      <div className="card">
        <div className="card-header">
          <div>
            <h2 className="card-title">
              Bảng Sự Kiện STREAM_PROVISIONAL (v1) Đang Chảy Vào MongoDB 8.0 (<code>bda501.live_stream_scores</code>)
            </h2>
            <p className="card-subtitle">
              Nhấp vào bất kỳ dòng nào để chuyển về Tab 1 định vị đoạn đường trên Bản đồ &amp; Biểu đồ dự báo +30p
            </p>
          </div>
          <span className="badge badge-green">Cập nhật lúc {tel.last_updated_ict || '—'} ICT</span>
        </div>

        {rows.length === 0 ? (
          <div style={{ padding: '24px', textAlign: 'center', color: '#64748b' }}>
            Bộ đếm đang ở <b>0</b>. Hãy nhấn <b>🎬 Phát Story qua Kafka</b> hoặc <b>⚡ Phát 200 tin Stress-Test</b> ở trên!
          </div>
        ) : (
          <div className="table-scroll" style={{ maxHeight: 440 }}>
            <table className="toc-table">
              <thead>
                <tr>
                  <th>STT</th>
                  <th>Giờ nhận</th>
                  <th>Part</th>
                  <th>Tuyến đường</th>
                  <th>Khung giờ gốc</th>
                  <th>Vận tốc</th>
                  <th>Hiện tại ➔ RF (+30p)</th>
                  <th>Nguy cơ E/F</th>
                  <th>Đổi pha</th>
                  <th>EWMA</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r: LiveScoreRow, idx: number) => {
                  const curLos = String(r.current_los || 'A');
                  const predLos = String(r.cal_predicted_los || r.predicted_next_los || curLos);
                  return (
                    <tr
                      key={`${r.event_id}-${idx}`}
                      className="clickable"
                      onClick={() => onJumpToSegment(Number(r.segment_id), String(r.ict_date || ''), Number(r.vn_hour || 12))}
                    >
                      <td>#{String(r.seq_id ?? idx + 1)}</td>
                      <td>{String(r.received_at_ict || '')}</td>
                      <td>P{String(r.kafka_partition ?? 0)}</td>
                      <td>
                        <strong>{String(r.street_name)}</strong> (#{String(r.segment_id)})
                      </td>
                      <td>
                        {String(r.ict_date || r.date)} · {Number(r.vn_hour ?? 0).toFixed(1)}h
                      </td>
                      <td>
                        {Number(r.mean_velocity ?? 0).toFixed(1)} / {Number(r.max_velocity ?? 40)} km/h
                      </td>
                      <td>
                        <span className={`los-pill los-${curLos}`}>{curLos}</span>
                        <span style={{ margin: '0 5px', fontWeight: 800 }}>➔</span>
                        <span className={`los-pill los-${predLos}`}>{predLos}</span>
                      </td>
                      <td>{(Number(r.severe_prob ?? 0) * 100).toFixed(1)}%</td>
                      <td>{(Number(r.change_prob ?? 0) * 100).toFixed(1)}%</td>
                      <td>{Number(r.ewma_hotspot_score ?? 0).toFixed(2)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
};
