import React from 'react';
import { ScenarioItem, StreamStatusResponse, WindowSemantics } from '../types';

interface Props {
  showScenarioClock: boolean;
  scenarios: ScenarioItem[];
  activeScenarioId: string;
  onSelectScenario: (id: string) => void;
  currentHour: number;
  onSelectHour: (vnHour: number) => void;
  semantics: WindowSemantics | null;
  streamStatus: StreamStatusResponse | null;
  isStreaming: boolean;
  onOpenKafkaTab: () => void;
}

export const TopClockBar: React.FC<Props> = ({
  showScenarioClock,
  scenarios,
  activeScenarioId,
  onSelectScenario,
  currentHour,
  onSelectHour,
  semantics,
  streamStatus,
  isStreaming,
  onOpenKafkaTab,
}) => {
  const activeScenario = scenarios.find((s) => s.id === activeScenarioId) || scenarios[0];
  const frames = activeScenario?.frames || [];
  const curIdx = frames.findIndex((f) => Math.abs(f.vn_hour - currentHour) < 0.05);
  const tel = streamStatus?.telemetry || {};

  const statusPillClass = [
    'kafka-status-pill',
    tel.mongo_connected ? 'connected' : 'disconnected',
    isStreaming ? 'streaming' : '',
  ]
    .filter(Boolean)
    .join(' ');

  return (
    <header className="toc-header">
      <div className="toc-header-top">
        <div className="brand-group">
          <div className="brand-badge">TOC</div>
          <div>
            <h1 className="brand-title">
              Trung Tâm Điều Hành &amp; Dự Báo Ùn Tắc Giao Thông TP.HCM (+30 Phút)
            </h1>
            <p className="brand-sub">
              BDA501 · Apache Kafka 4.1 KRaft ➔ Spark 3.5.7 / 33 KiB Sketches ➔ Frozen 23D Random Forest ➔ MongoDB 8.0
            </p>
          </div>
        </div>

        <button
          type="button"
          className={statusPillClass}
          onClick={onOpenKafkaTab}
          title="Nhấp để mở Tab 02 · Live Kafka & Bản đồ Luồng Speed Layer"
        >
          ● Kafka: {tel.messages_consumed ?? 0} tin · Mongo v1: {tel.sink2_scored_v1 ?? 0} · Bloom chặn:{' '}
          {tel.duplicates_caught_bloom ?? 0}
        </button>
      </div>

      {showScenarioClock && (
        <>
          <div className="clock-bar">
            <div className="clock-controls">
              <span style={{ fontSize: '11.5px', color: '#94a3b8', fontWeight: 700 }}>KỊCH BẢN THỰC TẾ:</span>
              <select
                className="select-dark"
                value={activeScenarioId}
                onChange={(e) => onSelectScenario(e.target.value)}
              >
                {scenarios.map((s) => (
                  <option key={s.id} value={s.id}>
                    {s.short_label} [{s.split_badge}]
                  </option>
                ))}
              </select>

              <button
                type="button"
                className="btn btn-slate"
                disabled={curIdx <= 0}
                onClick={() => curIdx > 0 && onSelectHour(frames[curIdx - 1].vn_hour)}
              >
                ◀ Lùi 30p
              </button>
              <button
                type="button"
                className="btn btn-slate"
                disabled={curIdx < 0 || curIdx >= frames.length - 1}
                onClick={() => curIdx >= 0 && curIdx < frames.length - 1 && onSelectHour(frames[curIdx + 1].vn_hour)}
              >
                Tua +30p ▶
              </button>
            </div>

            {semantics && (
              <div className="window-pill-strip">
                <span className="window-step">
                  📅 <strong>{semantics.ict_date}</strong>
                </span>
                <span>|</span>
                <span className="window-step">
                  📡 Thu thập GPS: <strong>{semantics.collect_window}</strong>
                </span>
                <span>➔</span>
                <span className="window-step">
                  ⏱️ Chốt phát lệnh: <strong>t = {semantics.issue_time_ict}</strong>
                </span>
                <span>➔</span>
                <span className="window-step forecast">
                  🔮 Dự báo trước +30p cho: <strong>{semantics.forecast_window}</strong>
                </span>
              </div>
            )}
          </div>

          {frames.length > 1 && (
            <div className="frame-scrubber">
              <span style={{ fontSize: '11px', color: '#94a3b8', alignSelf: 'center', marginRight: 4 }}>
                Trục thời gian kịch bản (mỗi nấc = 30 phút):
              </span>
              {frames.map((f) => {
                const active = Math.abs(f.vn_hour - currentHour) < 0.05;
                return (
                  <button
                    key={`${f.ict_date}-${f.vn_hour}`}
                    type="button"
                    className={`frame-chip ${active ? 'active' : ''} ${f.early_warnings > 0 ? 'has-warn' : ''}`}
                    onClick={() => onSelectHour(f.vn_hour)}
                  >
                    Chốt {f.issue_time_ict.replace(' ICT', '')} → Đoán {f.forecast_window}
                    {f.early_warnings > 0 ? ` (🚨${f.early_warnings})` : ''}
                  </button>
                );
              })}
            </div>
          )}
        </>
      )}
    </header>
  );
};
