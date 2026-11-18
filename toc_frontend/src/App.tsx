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

type ActiveTab = 'command' | 'kafka' | 'graph' | 'clusters' | 'audit';

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
  const [activeScenarioId, setActiveScenarioId] = useState<string>('noon_transition');
  const [currentDate, setCurrentDate] = useState<string>('2021-01-05');
  const [currentHour, setCurrentHour] = useState<number>(12.0);
  const [selectedSegmentId, setSelectedSegmentId] = useState<number>(21262);
  const [alertFilter, setAlertFilter] = useState<string>('early_warning');

  const [frameData, setFrameData] = useState<CityFrameResponse | null>(null);
  const [timelineData, setTimelineData] = useState<SegmentTimelineResponse | null>(null);
  const [hotspots, setHotspots] = useState<HotspotItem[]>([]);
  const [streamStatus, setStreamStatus] = useState<StreamStatusResponse | null>(null);
  const [isStreaming, setIsStreaming] = useState<boolean>(false);
  const [apiError, setApiError] = useState<string | null>(null);
  const [liveRefreshTick, setLiveRefreshTick] = useState<number>(0);
  const lastStoryFrameRef = useRef<string>('');
  const lastLiveScoredRef = useRef<number>(0);

  // Initial load: scenarios, overview hotspots, stream status
  useEffect(() => {
    let cancelled = false;
    fetchJson<{ scenarios?: ScenarioItem[] }>('/api/scenarios')
      .then((d) => {
        if (cancelled) return;
        const list: ScenarioItem[] = d.scenarios || [];
        setScenarios(list);
        if (list.length > 0) {
          const s0 = list[0];
          setActiveScenarioId(s0.id);
          setCurrentDate(s0.ict_date);
          setCurrentHour(s0.default_vn_hour);
          setSelectedSegmentId(s0.spotlight_segment_id);
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

    fetchJson<StreamStatusResponse>('/api/stream/status')
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

  // Load City Frame whenever (currentDate, currentHour, alertFilter, liveRefreshTick) changes
  useEffect(() => {
    let cancelled = false;
    const q = `/api/frame?ict_date=${encodeURIComponent(currentDate)}&vn_hour=${currentHour}&alert_filter=${encodeURIComponent(alertFilter)}`;
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
  }, [currentDate, currentHour, alertFilter, liveRefreshTick]);

  // Load Segment Timeline whenever (selectedSegmentId, currentDate, currentHour) changes
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

  // Poll Kafka Stream status when streaming or on Kafka tab
  useEffect(() => {
    if (!isStreaming && activeTab !== 'kafka') return;
    const timer = window.setInterval(() => {
      fetchJson<StreamStatusResponse>('/api/stream/status')
        .then((st) => {
          setStreamStatus(st);
          const scoredNow = Number(st?.telemetry?.sink2_scored_v1 ?? 0);
          if (scoredNow !== lastLiveScoredRef.current) {
            lastLiveScoredRef.current = scoredNow;
            if (isStreaming) {
              setLiveRefreshTick((t) => t + 1);
            }
          }
          const cur = st?.telemetry?.story_cursor;
          if (cur && isStreaming) {
            const key = `${cur.scenario}-${cur.frame_idx}-${cur.vn_hour}`;
            if (key !== lastStoryFrameRef.current) {
              lastStoryFrameRef.current = key;
              if (cur.scenario) setActiveScenarioId(cur.scenario);
              setCurrentDate(cur.ict_date);
              setCurrentHour(cur.vn_hour);
              if (cur.spotlight_segment_id) setSelectedSegmentId(cur.spotlight_segment_id);
              if (cur.frame_idx >= cur.total_frames) {
                window.setTimeout(() => setIsStreaming(false), 1800);
              }
            }
          }
        })
        .catch((err: unknown) => {
          setApiError(err instanceof Error ? err.message : 'Mất kết nối tạm thời khi cập nhật luồng Kafka.');
        });
    }, 1300);
    return () => window.clearInterval(timer);
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
    lastStoryFrameRef.current = '';
    setApiError(null);
    setIsStreaming(true);
    fetchJson('/api/stream/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        mode: 'story',
        scenario: activeScenarioId,
        reset_first: true,
      }),
    }).catch((err: unknown) => {
      setIsStreaming(false);
      setApiError(err instanceof Error ? err.message : 'Không thể khởi chạy Story Kafka.');
    });
  }, [activeScenarioId]);

  const handleStartStressKafka = useCallback(() => {
    lastStoryFrameRef.current = '';
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
    window.setTimeout(() => setIsStreaming(false), 14000);
  }, []);

  const handleResetKafka = useCallback(() => {
    setIsStreaming(false);
    lastStoryFrameRef.current = '';
    lastLiveScoredRef.current = 0;
    setApiError(null);
    fetchJson('/api/stream/reset', { method: 'POST' })
      .then(() => fetchJson<StreamStatusResponse>('/api/stream/status'))
      .then((st) => {
        setStreamStatus(st);
        setLiveRefreshTick((t) => t + 1);
      })
      .catch((err: unknown) => {
        setApiError(err instanceof Error ? err.message : 'Không thể reset trạng thái Kafka.');
      });
  }, []);

  const handleJumpToSegment = useCallback((segId: number, ictDate?: string, vnHour?: number) => {
    if (ictDate) setCurrentDate(ictDate);
    if (vnHour != null && !Number.isNaN(vnHour)) setCurrentHour(vnHour);
    setSelectedSegmentId(segId);
    setActiveTab('command');
  }, []);

  return (
    <div className="toc-shell">
      <TopClockBar
        scenarios={scenarios}
        activeScenarioId={activeScenarioId}
        onSelectScenario={handleSelectScenario}
        currentHour={currentHour}
        onSelectHour={setCurrentHour}
        semantics={frameData?.window_semantics || null}
        streamStatus={streamStatus}
        isStreaming={isStreaming}
        onStartStoryKafka={handleStartStoryKafka}
        onStartStressKafka={handleStartStressKafka}
        onResetKafka={handleResetKafka}
      />

      <nav className="nav-tabs">
        <button
          className={`nav-tab ${activeTab === 'command' ? 'active' : ''}`}
          onClick={() => setActiveTab('command')}
        >
          🚦 01 · Điều Hành &amp; Dự Báo +30p (UC-1, UC-2)
        </button>
        <button
          className={`nav-tab ${activeTab === 'kafka' ? 'active' : ''}`}
          onClick={() => setActiveTab('kafka')}
        >
          📡 02 · Live Kafka &amp; Sketches 33 KiB (P4)
        </button>
        <button
          className={`nav-tab ${activeTab === 'graph' ? 'active' : ''}`}
          onClick={() => setActiveTab('graph')}
        >
          🕸️ 03 · PageRank &amp; Luật Đồng Kẹt (UC-3, UC-4)
        </button>
        <button
          className={`nav-tab ${activeTab === 'clusters' ? 'active' : ''}`}
          onClick={() => setActiveTab('clusters')}
        >
          🧭 04 · 6 Cụm K-Means &amp; Cold-Start (UC-5)
        </button>
        <button
          className={`nav-tab ${activeTab === 'audit' ? 'active' : ''}`}
          onClick={() => setActiveTab('audit')}
        >
          🛡️ 05 · Spark 3.5.7, Ablation &amp; Đối Soát (UC-6)
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
            onStartStoryKafka={handleStartStoryKafka}
            onStartStressKafka={handleStartStressKafka}
            onResetKafka={handleResetKafka}
            onJumpToSegment={handleJumpToSegment}
          />
        )}
        {activeTab === 'graph' && <TabGraphRules onSelectSegmentOnMap={(sid) => handleJumpToSegment(sid)} />}
        {activeTab === 'clusters' && <TabClustersColdStart />}
        {activeTab === 'audit' && <TabInfraAudit />}
      </main>
    </div>
  );
};
