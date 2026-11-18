import React, { useState } from 'react';
import { SegmentTimelineResponse } from '../types';

interface Props {
  timeline: SegmentTimelineResponse | null;
}

const LOS_LIST = ['A', 'B', 'C', 'D', 'E', 'F'];
const LOS_TO_IDX: Record<string, number> = { A: 0, B: 1, C: 2, D: 3, E: 4, F: 5 };

export const ForecastTimelineChart: React.FC<Props> = ({ timeline }) => {
  const [showActual, setShowActual] = useState<boolean>(true);

  if (!timeline) {
    return <div className="card">Đang tải chuỗi thời gian dự báo của đoạn đường...</div>;
  }

  const {
    segment_id,
    street_name,
    max_velocity,
    window_semantics: sem,
    history_windows,
    raw_pings_past,
    raw_pings_future,
    forecast_comparison: fc,
    feature_drivers: drv,
  } = timeline;

  const W = 980;
  const H = 342;
  const xMin = -125;
  const xMax = 35;
  const rightMargin = 232;
  const mapX = (minOffset: number) => {
    const clamped = Math.max(xMin, Math.min(xMax, minOffset));
    return 72 + ((clamped - xMin) / (xMax - xMin)) * (W - 72 - rightMargin);
  };

  // Top Band: Speed (km/h) in y=[38..154]
  const maxObservedVel = Math.max(
    0,
    ...history_windows.map((h) => h.mean_velocity),
    ...raw_pings_past.map((p) => p.velocity_kmh),
    ...raw_pings_future.map((p) => p.velocity_kmh)
  );
  const vCeil = Math.max(max_velocity * 1.05, maxObservedVel * 1.08, 45);
  const mapVelY = (v: number) => 154 - (Math.max(0, Math.min(vCeil, v)) / vCeil) * 114;

  // Bottom Band: LOS A (top=194) -> LOS F (bottom=296)
  const mapLosY = (los: string) => 194 + ((LOS_TO_IDX[los] ?? 0) / 5) * 102;

  const xWinStart = mapX(-30);
  const xIssue = mapX(0);
  const xTargetEnd = mapX(30);

  const histVelPath = history_windows
    .map((hw, i) => `${i === 0 ? 'M' : 'L'} ${mapX(hw.minutes_from_issue)} ${mapVelY(hw.mean_velocity)}`)
    .join(' ');

  const histLosPath = history_windows
    .map((hw, i) => `${i === 0 ? 'M' : 'L'} ${mapX(hw.minutes_from_issue)} ${mapLosY(hw.los)}`)
    .join(' ');

  const curY = mapLosY(fc.current_los);
  const persY = mapLosY(fc.persistence_los);
  const rfY = mapLosY(fc.effective_forecast_los);
  const actY = fc.actual_next_los ? mapLosY(fc.actual_next_los) : null;
  const sameRfAct = actY !== null && Math.abs(rfY - actY) < 4;
  const samePersRf = Math.abs(persY - rfY) < 4;

  return (
    <div className="card">
      <div className="card-header">
        <div>
          <h2 className="card-title">
            Biểu Đồ Chuỗi Thời Gian &amp; Phóng Dự Báo +30 Phút: {street_name} (Đoạn #{segment_id})
          </h2>
          <p className="card-subtitle">
            Trực quan hóa trọn vẹn từ <b>chấm bản tin GPS thô</b> trong cửa sổ {sem.collect_window} ➔{' '}
            <b>gom Monoid chốt tại t = {sem.issue_time_ict}</b> ➔ <b>phóng nét đứt dự báo sang {sem.forecast_window}</b>
          </p>
        </div>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
          <span className={`badge ${timeline.is_cold_start ? 'badge-amber' : 'badge-blue'}`}>{timeline.split_set}</span>
          <span className="badge badge-red">
            Nguy cơ E/F: {(fc.severe_prob * 100).toFixed(1)}% · Đổi pha: {(fc.change_prob * 100).toFixed(1)}%
          </span>
          <button className="btn btn-amber" onClick={() => setShowActual((v) => !v)}>
            {showActual ? '🙈 Ẩn nhãn thực tế t+30p (Đố Hội đồng)' : '👁️ Mở kết quả thực tế t+30p'}
          </button>
        </div>
      </div>

      <svg viewBox={`0 0 ${W} ${H}`} style={{ width: '100%', height: 'auto', background: '#f8fafc', borderRadius: 10, border: '1px solid #e2e8f0' }}>
        {/* Shaded Current Collection Window [-30m, 0] */}
        <rect x={xWinStart} y={24} width={xIssue - xWinStart} height={H - 46} fill="#dbeafe" opacity={0.45} />
        {/* Shaded Future Forecast Horizon [0, +30m] */}
        <rect x={xIssue} y={24} width={xTargetEnd - xIssue} height={H - 46} fill="#ffe4e6" opacity={0.42} />

        {/* Band titles */}
        <text x={76} y={33} fontSize={10.5} fontWeight={700} fill="#334155">
          TẦNG 1 · VẬN TỐC XE THỰC ĐO &amp; CÁC BẢN TIN GPS THÔ (km/h, Vmax = {max_velocity} km/h)
        </text>
        <text x={76} y={182} fontSize={10.5} fontWeight={700} fill="#334155">
          TẦNG 2 · DIỄN BIẾN MỨC PHỤC VỤ LOS (A ➔ F) &amp; ĐỐI ĐẦU DỰ BÁO TRƯỚC +30 PHÚT
        </text>

        {/* Speed Gridlines */}
        {[0, Math.round(max_velocity * 0.35), Math.round(max_velocity * 0.65), Math.round(max_velocity)].map((v) => (
          <g key={`vg-${v}`}>
            <line x1={68} y1={mapVelY(v)} x2={xTargetEnd + 16} y2={mapVelY(v)} stroke="#cbd5e1" strokeDasharray="3,3" />
            <text x={62} y={mapVelY(v) + 3.5} textAnchor="end" fontSize={9.5} fill="#64748b">
              {v} km/h
            </text>
          </g>
        ))}

        {/* LOS Gridlines A..F */}
        {LOS_LIST.map((lbl) => (
          <g key={`lg-${lbl}`}>
            <line
              x1={68}
              y1={mapLosY(lbl)}
              x2={xTargetEnd + 16}
              y2={mapLosY(lbl)}
              stroke={lbl === 'E' || lbl === 'F' ? '#fecaca' : '#e2e8f0'}
            />
            <text
              x={62}
              y={mapLosY(lbl) + 3.5}
              textAnchor="end"
              fontSize={10}
              fontWeight={700}
              fill={lbl === 'E' || lbl === 'F' ? '#dc2626' : '#475569'}
            >
              LOS {lbl}
            </text>
          </g>
        ))}

        {/* Vertical Issue Boundary Line at t = 0 */}
        <line x1={xWinStart} y1={24} x2={xWinStart} y2={H - 22} stroke="#3b82f6" strokeDasharray="4,4" />
        <line x1={xIssue} y1={24} x2={xIssue} y2={H - 22} stroke="#0f172a" strokeWidth={2.2} />
        <text x={(xWinStart + xIssue) / 2} y={H - 7} textAnchor="middle" fontSize={10.2} fontWeight={700} fill="#1d4ed8">
          Khung thu thập {sem.collect_window} ({drv.obs_count_in_window} pings)
        </text>
        <text x={xIssue} y={17} textAnchor="middle" fontSize={10.5} fontWeight={800} fill="#0f172a">
          Chốt t = {sem.issue_time_ict} (0 Leakage)
        </text>
        <text x={(xIssue + xTargetEnd) / 2} y={H - 7} textAnchor="middle" fontSize={10.2} fontWeight={700} fill="#be123c">
          Dự báo {sem.forecast_window}
        </text>

        {/* Historical 30m Window Speed Line */}
        {histVelPath && <path d={histVelPath} fill="none" stroke="#1e40af" strokeWidth={2.4} />}
        {history_windows.map((hw, idx) => (
          <g key={`hwv-${idx}`}>
            <circle cx={mapX(hw.minutes_from_issue)} cy={mapVelY(hw.mean_velocity)} r={4.5} fill="#1e40af" />
            <text
              x={mapX(hw.minutes_from_issue)}
              y={mapVelY(hw.mean_velocity) - 8}
              textAnchor="middle"
              fontSize={9.2}
              fontWeight={700}
              fill="#1e3a8a"
            >
              {hw.mean_velocity} km/h ({hw.close_time_ict})
            </text>
          </g>
        ))}

        {/* Individual Raw GPS Probe Pings (Past Observed) */}
        {raw_pings_past.map((p, idx) => {
          const yOff = idx % 2 === 0 ? -8 : 13;
          const xShift = idx > 0 && Math.abs(p.minutes_from_issue - raw_pings_past[idx - 1].minutes_from_issue) < 1.2
            ? (idx % 2 === 0 ? -14 : 14)
            : 0;
          return (
            <g key={`rp-${idx}`}>
              <circle
                cx={mapX(p.minutes_from_issue)}
                cy={mapVelY(p.velocity_kmh)}
                r={4.2}
                fill="#06b6d4"
                stroke="#0e7490"
                strokeWidth={1.2}
              >
                <title>{`GPS Ping #${p.ping_id} lúc ${p.time_ict} ICT: ${p.velocity_kmh} km/h`}</title>
              </circle>
              {raw_pings_past.length <= 8 && (
                <text
                  x={mapX(p.minutes_from_issue) + xShift}
                  y={mapVelY(p.velocity_kmh) + yOff}
                  textAnchor="middle"
                  fontSize={8.2}
                  fontWeight={600}
                  fill="#0e7490"
                >
                  {p.time_ict.slice(0, 5)} ({p.velocity_kmh})
                </text>
              )}
            </g>
          );
        })}

        {/* Future Raw GPS Pings (when revealed) */}
        {showActual &&
          raw_pings_future.map((p, idx) => (
            <circle
              key={`fp-${idx}`}
              cx={mapX(p.minutes_from_issue)}
              cy={mapVelY(p.velocity_kmh)}
              r={4.2}
              fill="#10b981"
              stroke="#047857"
              strokeWidth={1.2}
            >
              <title>{`GPS thực tế tương lai lúc ${p.time_ict} ICT: ${p.velocity_kmh} km/h`}</title>
            </circle>
          ))}

        {/* Historical LOS Trajectory */}
        {histLosPath && <path d={histLosPath} fill="none" stroke="#334155" strokeWidth={2.6} />}
        {history_windows.map((hw, idx) => (
          <circle key={`hwl-${idx}`} cx={mapX(hw.minutes_from_issue)} cy={mapLosY(hw.los)} r={5} fill="#0f172a" />
        ))}

        {/* 1. Persistence Baseline Flat Line (t -> t+30m) */}
        <line
          x1={xIssue}
          y1={curY}
          x2={xTargetEnd}
          y2={persY}
          stroke="#94a3b8"
          strokeWidth={2.5}
          strokeDasharray="3,4"
        />
        <circle cx={xTargetEnd} cy={persY} r={5} fill="#94a3b8" />
        <text
          x={xTargetEnd + 11}
          y={samePersRf ? persY - 12 : persY + 3}
          fontSize={10}
          fontWeight={650}
          fill="#64748b"
        >
          Persistence: LOS {fc.persistence_los}
        </text>

        {/* 2. Random Forest 23D Dashed Forecast Trajectory (t -> t+30m) */}
        <line
          x1={xIssue}
          y1={curY}
          x2={xTargetEnd}
          y2={rfY}
          stroke="#e11d48"
          strokeWidth={3.4}
          strokeDasharray="7,4"
        />
        <circle cx={xTargetEnd} cy={rfY} r={6.5} fill="#e11d48" stroke="#fff" strokeWidth={1.8} />
        <text
          x={xTargetEnd + 12}
          y={sameRfAct ? rfY - 4 : rfY + 4}
          fontSize={10.5}
          fontWeight={800}
          fill="#be123c"
        >
          RF (+30p): LOS {fc.effective_forecast_los} ({(fc.change_prob * 100).toFixed(0)}%)
        </text>

        {/* 3. Actual Ground Truth at t+30m */}
        {showActual && actY !== null && (
          <g>
            <circle cx={xTargetEnd} cy={actY} r={10} fill="none" stroke="#059669" strokeWidth={2.4} />
            <text
              x={xTargetEnd + 12}
              y={sameRfAct ? actY + 12 : actY + 4}
              fontSize={10.2}
              fontWeight={800}
              fill="#059669"
            >
              ★ Thực tế: LOS {fc.actual_next_los}
            </text>
          </g>
        )}
      </svg>

      <div style={{ display: 'flex', gap: 12, marginTop: 10, flexWrap: 'wrap', fontSize: '12px', color: '#334155' }}>
        <span>
          🔹 <b>Chấm xanh lam ({raw_pings_past.length} pings):</b> Từng xe gửi GPS thô trước mốc chốt
        </span>
        <span>
          🔸 <b>Vì sao RF phát hiện đổi pha?</b> Quá khứ t-1 = <b>{drv.lag1_los}</b> · Nhánh kề = <b>LOS {drv.neighbor_los}</b> ·
          PageRank = <b>{drv.pagerank_score}</b> · Cụm hành vi = <b>#{drv.cluster_id}</b> · Tỷ lệ kẹt lịch sử tuyến ={' '}
          <b>{(drv.street_severe_prior * 100).toFixed(1)}%</b>
        </span>
      </div>
    </div>
  );
};
