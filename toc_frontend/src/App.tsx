import React, { useCallback, useEffect, useRef, useState } from 'react';
import {
  CityFrameResponse,
  HotspotItem,
  OverviewResponse,
  ScenarioItem,
  SegmentTimelineResponse,
  StreamStatusResponse,
} from './types';
import { TopClockBar } from './components/TopClockBar';
import { TabCommandCenter } from './components/TabCommandCenter';
import { TabLiveKafka } from './components/TabLiveKafka';
import { TabGraphRules } from './components/TabGraphRules';
import { TabClustersColdStart } from './components/TabClustersColdStart';
import { TabInfraAudit } from './components/TabInfraAudit';
import { TabPipelineFlow } from './components/TabPipelineFlow';

type ActiveTab = 'command' | 'kafka' | 'graph' | 'clusters' | 'audit' | 'pipeline';

const DEFAULT_SCENARIO_ID = 'morning_rush_dien_bien_phu';
const DEFAULT_ICT_DATE = '2021-01-04';
const DEFAULT_VN_HOUR = 7.0;
const DEFAULT_SEGMENT_ID = 50026;
const STREAM_STATUS_URL = '/api/stream/status?limit=45';

async function fetchJson<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, init);
  if (!res.ok) {
    throw new Error(`Lỗi máy chủ (${res.status} ${res.statusText}) khi gọi ${url}`);
  }
  return (await res.json()) as T;
}

export const App: React.FC = () => {
  const [activeTab, setActiveTab] = useState<ActiveTab>('command');
  const [scenarios, setScenarios] = useState<ScenarioItem[]>([]);
  const [activeScenarioId, setActiveScenarioId] = useState<string>(DEFAULT_SCENARIO_ID);
  const [currentDate, setCurrentDate] = useState<string>(DEFAULT_ICT_DATE);
  const [currentHour, setCurrentHour] = useState<number>(DEFAULT_VN_HOUR);
  const [selectedSegmentId, setSelectedSegmentId] = useState<number>(DEFAULT_SEGMENT_ID);
  const [selectedColdSegmentId, setSelectedColdSegmentId] = useState<number | null>(null);
  const [coldJumpSourceNote, setColdJumpSourceNote] = useState<string | null>(null);
  const [alertFilter, setAlertFilter] = useState<string>('early_warning');

  const [kafkaStoryScenario, setKafkaStoryScenario] = useState<string>(DEFAULT_SCENARIO_ID);
  const [frameData, setFrameData] = useState<CityFrameResponse | null>(null);
  const [timelineData, setTimelineData] = useState<SegmentTimelineResponse | null>(null);
  const [hotspots, setHotspots] = useState<HotspotItem[]>([]);
  const [streamStatus, setStreamStatus] = useState<StreamStatusResponse | null>(null);
  const [isStreaming, setIsStreaming] = useState<boolean>(false);
  const [apiError, setApiError] = useState<string | null>(null);

  const stopTimerRef = useRef<number | null>(null);
  const streamStartedAtRef = useRef<number>(0);

  const clearStopTimer = useCallback(() => {
    if (stopTimerRef.current !== null) {
      window.clearTimeout(stopTimerRef.current);
      stopTimerRef.current = null;
    }
  }, []);

  useEffect(() => () => clearStopTimer(), [clearStopTimer]);

  // Initial load: scenarios (prefer morning_rush_dien_bien_phu), overview hotspots, stream status
  useEffect(() => {
    let cancelled = false;
    fetchJson<{ scenarios?: ScenarioItem[] }>('/api/scenarios')
      .then((d) => {
        if (cancelled) return;
        const list: ScenarioItem[] = d.scenarios || [];
        setScenarios(list);
        if (list.length > 0) {
          const preferred = list.find((s) => s.id === DEFAULT_SCENARIO_ID) || list[0];
          setActiveScenarioId(preferred.id);
          setCurrentDate(preferred.ict_date);
          setCurrentHour(preferred.default_vn_hour);
          setSelectedSegmentId(preferred.spotlight_segment_id);
          setKafkaStoryScenario(preferred.id);
        }
      })
      .catch((err: unknown) => {
        if (!cancelled) {
          setApiError(err instanceof Error ? err.message : 'Không thể tải danh sách kịch bản.');
        }
      });

    fetchJson<OverviewResponse>('/api/overview')
      .then((d) => {
        if (!cancelled) setHotspots(d.ewma_hotspots || []);
      })
      .catch((err: unknown) => {
        if (!cancelled) {
          setApiError(err instanceof Error ? err.message : 'Không thể tải dữ liệu tổng quan.');
        }
      });

    fetchJson<StreamStatusResponse>(STREAM_STATUS_URL)
      .then((st) => {
        if (!cancelled) setStreamStatus(st);
      })
      .catch((err: unknown) => {
        if (!cancelled) {
          setApiError(err instanceof Error ? err.message : 'Không thể kết nối trạng thái luồng Kafka.');
        }
      });

    return () => {
      cancelled = true;
    };
  }, []);

  // Load pure TEST/VAL Holdout City Frame for Tab 01 (strictly decoupled from Kafka live overlay)
  useEffect(() => {
    let cancelled = false;
    const q = `/api/frame?ict_date=${encodeURIComponent(currentDate)}&vn_hour=${currentHour}&alert_filter=${encodeURIComponent(alertFilter)}&include_live_overlay=false`;
    fetchJson<CityFrameResponse>(q)
      .then((d) => {
        if (!cancelled) {
          setFrameData(d);
          setApiError(null);
        }
      })
      .catch((err: unknown) => {
        if (!cancelled) {
          setApiError(err instanceof Error ? err.message : 'Không thể tải dữ liệu khung giờ TOC.');
        }
      });
    return () => {
      cancelled = true;
    };
  }, [currentDate, currentHour, alertFilter]);

  // Load Segment Timeline for Tab 01 whenever (selectedSegmentId, currentDate, currentHour) changes
  useEffect(() => {
    let cancelled = false;
    const q = `/api/segment/${selectedSegmentId}/timeline?ict_date=${encodeURIComponent(currentDate)}&vn_hour=${currentHour}`;
    fetchJson<SegmentTimelineResponse>(q)
      .then((d) => {
        if (!cancelled) {
          setTimelineData(d);
          setApiError(null);
        }
      })
      .catch((err: unknown) => {
        if (!cancelled) {
          setApiError(err instanceof Error ? err.message : `Không thể tải chuỗi thời gian đoạn #${selectedSegmentId}.`);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [selectedSegmentId, currentDate, currentHour]);

  // Poll Kafka Stream status purely for Tab 02 / Tab 06 & TopClockBar status pill (NEVER touches Tab 01)
  useEffect(() => {
    if (!isStreaming && activeTab !== 'kafka' && activeTab !== 'pipeline') return;
    let cancelled = false;

    const pollOnce = () => {
      fetchJson<StreamStatusResponse>(STREAM_STATUS_URL)
        .then((st) => {
          if (cancelled) return;
          setStreamStatus(st);
          const cur = st?.telemetry?.story_cursor;
          const elapsedMs = Date.now() - streamStartedAtRef.current;
          if (
            isStreaming &&
            cur &&
            cur.total_frames > 0 &&
            cur.frame_idx >= cur.total_frames &&
            elapsedMs > 2200 &&
            stopTimerRef.current === null
          ) {
            stopTimerRef.current = window.setTimeout(() => {
              stopTimerRef.current = null;
              setIsStreaming(false);
            }, 1800);
          }
        })
        .catch((err: unknown) => {
          if (!cancelled) {
            setApiError(err instanceof Error ? err.message : 'Mất kết nối tạm thời khi cập nhật luồng Kafka.');
          }
        });
    };

    pollOnce();
    const timer = window.setInterval(pollOnce, 1300);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [isStreaming, activeTab]);

  const handleSelectScenario = useCallback(
    (scenId: string) => {
      const found = scenarios.find((s) => s.id === scenId);
      setActiveScenarioId(scenId);
      if (found) {
        setCurrentDate(found.ict_date);
        setCurrentHour(found.default_vn_hour);
        setSelectedSegmentId(found.spotlight_segment_id);
      }
    },
    [scenarios]
  );

  const handleStartStoryKafka = useCallback(() => {
    clearStopTimer();
    streamStartedAtRef.current = Date.now();
    setApiError(null);
    setIsStreaming(true);
    fetchJson('/api/stream/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        mode: 'story',
        scenario: kafkaStoryScenario,
        reset_first: true,
      }),
    }).catch((err: unknown) => {
      setIsStreaming(false);
      setApiError(err instanceof Error ? err.message : 'Không thể khởi chạy Story Kafka.');
    });
  }, [clearStopTimer, kafkaStoryScenario]);

  const handleStartStressKafka = useCallback(() => {
    clearStopTimer();
    streamStartedAtRef.current = Date.now();
    setApiError(null);
    setIsStreaming(true);
    fetchJson('/api/stream/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        mode: 'stress',
        max_messages: 200,
        reset_first: true,
      }),
    }).catch((err: unknown) => {
      setIsStreaming(false);
      setApiError(err instanceof Error ? err.message : 'Không thể khởi chạy Stress-Test Kafka.');
    });
    stopTimerRef.current = window.setTimeout(() => {
      stopTimerRef.current = null;
      setIsStreaming(false);
    }, 14000);
  }, [clearStopTimer]);

  const handleResetKafka = useCallback(() => {
    clearStopTimer();
    setIsStreaming(false);
    setApiError(null);
    fetchJson('/api/stream/reset', { method: 'POST' })
      .then(() => fetchJson<StreamStatusResponse>(STREAM_STATUS_URL))
      .then((st) => {
        setStreamStatus(st);
      })
      .catch((err: unknown) => {
        setApiError(err instanceof Error ? err.message : 'Không thể reset trạng thái Kafka.');
      });
  }, [clearStopTimer]);

  const handleJumpToColdStart = useCallback((segId: number, streetName: string) => {
    setSelectedColdSegmentId(segId);
    setColdJumpSourceNote(`Đoạn #${segId} · ${streetName}`);
    setActiveTab('clusters');
  }, []);

  const handleJumpToSegment = useCallback(
    (segId: number, ictDate?: string, vnHour?: number) => {
      if (ictDate) {
        setCurrentDate(ictDate);
        const matched = scenarios.find((s) => s.ict_date === ictDate);
        if (matched) setActiveScenarioId(matched.id);
      }
      if (vnHour != null && !Number.isNaN(vnHour)) setCurrentHour(vnHour);
      setSelectedSegmentId(segId);
      setActiveTab('command');
    },
    [scenarios]
  );

  return (
    <div className="toc-shell">
      <TopClockBar
        showScenarioClock={activeTab === 'command'}
        scenarios={scenarios}
        activeScenarioId={activeScenarioId}
        onSelectScenario={handleSelectScenario}
        currentHour={currentHour}
        onSelectHour={setCurrentHour}
        semantics={frameData?.window_semantics || null}
        streamStatus={streamStatus}
        isStreaming={isStreaming}
        onOpenKafkaTab={() => setActiveTab('kafka')}
      />

      <nav className="nav-tabs">
        <button
          className={`nav-tab ${activeTab === 'command' ? 'active' : ''}`}
          onClick={() => setActiveTab('command')}
        >
          🚦 01 · Điều Hành &amp; Dự Báo +30p
        </button>
        <button
          className={`nav-tab ${activeTab === 'kafka' ? 'active' : ''}`}
          onClick={() => setActiveTab('kafka')}
        >
          📡 02 · Live Kafka &amp; Sketches 33 KiB
        </button>
        <button
          className={`nav-tab ${activeTab === 'graph' ? 'active' : ''}`}
          onClick={() => setActiveTab('graph')}
        >
          🕸️ 03 · PageRank &amp; Luật Đồng Kẹt
        </button>
        <button
          className={`nav-tab ${activeTab === 'clusters' ? 'active' : ''}`}
          onClick={() => setActiveTab('clusters')}
        >
          🧭 04 · 6 Cụm K-Means &amp; Cold-Start
        </button>
        <button
          className={`nav-tab ${activeTab === 'audit' ? 'active' : ''}`}
          onClick={() => setActiveTab('audit')}
        >
          🛡️ 05 · Spark 3.5.7 &amp; Ablation
        </button>
        <button
          className={`nav-tab ${activeTab === 'pipeline' ? 'active' : ''}`}
          onClick={() => setActiveTab('pipeline')}
        >
          ⚡ 06 · Luồng Thực Thi Pipeline (Live)
        </button>
      </nav>

      <main className="toc-main">
        {apiError && (
          <div
            role="alert"
            style={{
              background: '#fef2f2',
              border: '1px solid #fecaca',
              color: '#991b1b',
              borderRadius: 10,
              padding: '10px 14px',
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              gap: 12,
            }}
          >
            <span>⚠️ <strong>Cảnh báo kết nối API:</strong> {apiError}</span>
            <button className="btn btn-slate" onClick={() => setApiError(null)}>
              Đóng
            </button>
          </div>
        )}
        {activeTab === 'command' && (
          <TabCommandCenter
            frameData={frameData}
            timelineData={timelineData}
            hotspots={hotspots}
            selectedSegmentId={selectedSegmentId}
            onSelectSegment={setSelectedSegmentId}
            alertFilter={alertFilter}
            onChangeAlertFilter={setAlertFilter}
          />
        )}
        {activeTab === 'kafka' && (
          <TabLiveKafka
            streamStatus={streamStatus}
            scenarios={scenarios}
            kafkaStoryScenario={kafkaStoryScenario}
            onSelectKafkaStoryScenario={setKafkaStoryScenario}
            isStreaming={isStreaming}
            onStartStoryKafka={handleStartStoryKafka}
            onStartStressKafka={handleStartStressKafka}
            onResetKafka={handleResetKafka}
          />
        )}
        {activeTab === 'graph' && (
          <TabGraphRules
            onSelectSegmentOnMap={(sid, ictDate, vnHour) => handleJumpToSegment(sid, ictDate, vnHour)}
            onSelectColdStartSegment={handleJumpToColdStart}
          />
        )}
        {activeTab === 'clusters' && (
          <TabClustersColdStart
            selectedSegmentId={selectedColdSegmentId}
            onSelectSegmentId={setSelectedColdSegmentId}
            jumpSourceNote={coldJumpSourceNote}
            onClearJumpSource={() => setColdJumpSourceNote(null)}
          />
        )}
        {activeTab === 'audit' && <TabInfraAudit />}
        {activeTab === 'pipeline' && (
          <TabPipelineFlow
            streamStatus={streamStatus}
            isStreaming={isStreaming}
            onStartStressKafka={handleStartStressKafka}
            onJumpToSegment={handleJumpToSegment}
          />
        )}
      </main>
    </div>
  );
};
