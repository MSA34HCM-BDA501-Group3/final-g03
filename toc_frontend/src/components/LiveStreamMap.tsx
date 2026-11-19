import React, { useEffect, useMemo, useRef, useState } from 'react';
import L from 'leaflet';
import { CoordsTuple, LiveScoreRow } from '../types';

export interface LiveStreamMapProps {
  rows: LiveScoreRow[];
  selectedKey: string | null;
  onSelectRow: (key: string) => void;
  isStreaming: boolean;
}

interface PlottedLiveItem {
  key: string;
  row: LiveScoreRow;
  coords: CoordsTuple;
}

const HCMC_CENTER: [number, number] = [10.778, 106.695];
const HCMC_DEFAULT_ZOOM = 13;

export function getLiveRowKey(row: LiveScoreRow): string {
  const base = row.event_id || `seg_${row.segment_id}`;
  return `${base}#${row.seq_id ?? 0}`;
}

function hasValidCoords(coords: unknown): coords is CoordsTuple {
  return (
    Array.isArray(coords) &&
    coords.length === 4 &&
    coords.every((val) => typeof val === 'number' && Number.isFinite(val))
  );
}

function escapeHtml(value: unknown): string {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function getEffectivePredLos(row: LiveScoreRow): string {
  return String(row.cal_predicted_los || row.predicted_next_los || row.current_los || 'A').toUpperCase();
}

function getLiveLosColor(los: string): string {
  const upper = los.toUpperCase();
  if (upper === 'E' || upper === 'F') return '#ef4444';
  if (upper === 'C' || upper === 'D') return '#f59e0b';
  return '#10b981';
}

function selectPlottedRows(rows: LiveScoreRow[], selectedKey: string | null): PlottedLiveItem[] {
  const bySegment = new Map<number, PlottedLiveItem>();

  for (const row of rows) {
    if (!hasValidCoords(row.coords)) continue;
    const key = getLiveRowKey(row);
    const existing = bySegment.get(row.segment_id);
    if (!existing || key === selectedKey) {
      bySegment.set(row.segment_id, { key, row, coords: row.coords });
    }
  }

  return Array.from(bySegment.values());
}

function buildRowTooltipHtml(row: LiveScoreRow): string {
  const street = escapeHtml(row.street_name || `Đoạn #${row.segment_id}`);
  const cur = escapeHtml(String(row.current_los || 'A').toUpperCase());
  const pred = escapeHtml(getEffectivePredLos(row));
  const sev = (Number(row.severe_prob ?? 0) * 100).toFixed(1);
  const part = Number(row.kafka_partition ?? 0);
  const ewma = Number(row.ewma_hotspot_score ?? 0).toFixed(2);
  return (
    `<b>📡 LIVE v1 · ${street} (#${row.segment_id})</b><br/>` +
    `Hiện tại: <b>LOS ${cur}</b> ➔ RF +30p: <b>LOS ${pred}</b><br/>` +
    `Nguy cơ E/F: <b>${sev}%</b> · Partition P${part} · EWMA ${ewma}`
  );
}

function buildSelectedPinHtml(row: LiveScoreRow): string {
  const street = escapeHtml(row.street_name || `Đoạn #${row.segment_id}`);
  const cur = escapeHtml(String(row.current_los || 'A').toUpperCase());
  const pred = escapeHtml(getEffectivePredLos(row));
  const sev = (Number(row.severe_prob ?? 0) * 100).toFixed(1);
  const part = Number(row.kafka_partition ?? 0);
  const ewma = Number(row.ewma_hotspot_score ?? 0).toFixed(2);
  return (
    `<div class="toc-focus-pin">` +
    `<div class="toc-focus-pin-title">📡 LIVE v1 · ${street} (#${row.segment_id})</div>` +
    `<div class="toc-focus-pin-sub">` +
    `Hiện tại: <b>LOS ${cur}</b> ➔ RF +30p: <b class="forecast-tag">LOS ${pred}</b>` +
    `</div>` +
    `<div class="toc-focus-pin-sub">` +
    `Nguy cơ E/F: <b class="forecast-tag">${sev}%</b> · Partition P${part} · EWMA <b>${ewma}</b>` +
    `</div>` +
    `</div>`
  );
}

function addNonSelectedLayer(
  group: L.LayerGroup,
  item: PlottedLiveItem,
  theme: 'dark' | 'light',
  onSelectRow: (key: string) => void
): void {
  const [sLat, sLon, eLat, eLon] = item.coords;
  const color = getLiveLosColor(getEffectivePredLos(item.row));
  const tooltip = buildRowTooltipHtml(item.row);
  const midLat = (sLat + eLat) * 0.5;
  const midLon = (sLon + eLon) * 0.5;

  L.polyline(
    [
      [sLat, sLon],
      [eLat, eLon],
    ],
    {
      color,
      weight: 4.5,
      opacity: 0.9,
      lineCap: 'round',
    }
  )
    .bindTooltip(tooltip, { sticky: true })
    .on('click', () => onSelectRow(item.key))
    .addTo(group);

  L.circleMarker([midLat, midLon], {
    radius: 4.5,
    color: theme === 'dark' ? '#0f172a' : '#ffffff',
    weight: 1.4,
    fillColor: color,
    fillOpacity: 0.95,
  })
    .bindTooltip(tooltip, { sticky: true })
    .on('click', () => onSelectRow(item.key))
    .addTo(group);
}

function addSelectedLayer(
  group: L.LayerGroup,
  item: PlottedLiveItem,
  onSelectRow: (key: string) => void
): void {
  const [sLat, sLon, eLat, eLon] = item.coords;
  const color = getLiveLosColor(getEffectivePredLos(item.row));
  const midLat = (sLat + eLat) * 0.5;
  const midLon = (sLon + eLon) * 0.5;

  L.polyline(
    [
      [sLat, sLon],
      [eLat, eLon],
    ],
    {
      color: '#38bdf8',
      weight: 14,
      opacity: 0.48,
      lineCap: 'round',
    }
  ).addTo(group);

  L.polyline(
    [
      [sLat, sLon],
      [eLat, eLon],
    ],
    {
      color,
      weight: 7,
      opacity: 1,
      lineCap: 'round',
    }
  )
    .on('click', () => onSelectRow(item.key))
    .addTo(group);

  L.circleMarker([midLat, midLon], {
    radius: 6.5,
    color: '#38bdf8',
    weight: 2.2,
    fillColor: color,
    fillOpacity: 1,
  })
    .on('click', () => onSelectRow(item.key))
    .addTo(group);

  L.marker([midLat, midLon], {
    interactive: false,
    zIndexOffset: 1000,
    icon: L.divIcon({
      className: 'toc-focus-pin-wrap',
      iconSize: [320, 72],
      iconAnchor: [160, 82],
      html: buildSelectedPinHtml(item.row),
    }),
  }).addTo(group);
}

function fitAllPlottedBounds(map: L.Map, items: PlottedLiveItem[]): void {
  if (items.length === 0) {
    map.flyTo(HCMC_CENTER, HCMC_DEFAULT_ZOOM, { duration: 0.5 });
    return;
  }
  const pts: Array<[number, number]> = [];
  items.forEach(({ coords }) => {
    pts.push([coords[0], coords[1]], [coords[2], coords[3]]);
  });
  const bounds = L.latLngBounds(pts);
  map.fitBounds(bounds, {
    paddingTopLeft: [46, 108],
    paddingBottomRight: [46, 54],
    maxZoom: 15,
    animate: true,
  });
}

function focusSingleItem(map: L.Map, item: PlottedLiveItem): void {
  const [sLat, sLon, eLat, eLon] = item.coords;
  const bounds = L.latLngBounds([
    [sLat, sLon],
    [eLat, eLon],
  ]);
  map.fitBounds(bounds, {
    paddingTopLeft: [48, 128],
    paddingBottomRight: [48, 56],
    maxZoom: 16,
    animate: true,
  });
}

export const LiveStreamMap: React.FC<LiveStreamMapProps> = ({
  rows,
  selectedKey,
  onSelectRow,
  isStreaming,
}) => {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<L.Map | null>(null);
  const baseLayerGroupRef = useRef<L.LayerGroup | null>(null);
  const streamGroupRef = useRef<L.LayerGroup | null>(null);
  const hasAutoFittedRef = useRef<boolean>(false);
  const lastFocusedKeyRef = useRef<string | null>(null);
  const lastMaxSeqRef = useRef<number>(0);
  const wasPopulatedRef = useRef<boolean>(false);

  const [theme, setTheme] = useState<'dark' | 'light'>('dark');
  const [cameraMode, setCameraMode] = useState<'overview' | 'focus'>('overview');

  const plottedItems = useMemo(
    () => selectPlottedRows(rows, selectedKey),
    [rows, selectedKey]
  );

  const activeItem = useMemo(() => {
    if (plottedItems.length === 0) return null;
    if (selectedKey) {
      const exact = plottedItems.find((item) => item.key === selectedKey);
      if (exact) return exact;
    }
    return plottedItems[0];
  }, [plottedItems, selectedKey]);

  // 1. Initialize Leaflet Map once with clean unmount teardown
  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;
    const map = L.map(containerRef.current, {
      center: HCMC_CENTER,
      zoom: HCMC_DEFAULT_ZOOM,
      zoomControl: true,
    });
    map.attributionControl.setPrefix(false);
    baseLayerGroupRef.current = L.layerGroup().addTo(map);
    streamGroupRef.current = L.layerGroup().addTo(map);
    mapRef.current = map;

    const rafId = window.requestAnimationFrame(() => {
      map.invalidateSize();
    });

    return () => {
      window.cancelAnimationFrame(rafId);
      map.remove();
      mapRef.current = null;
      baseLayerGroupRef.current = null;
      streamGroupRef.current = null;
    };
  }, []);

  // 2. Clean Dark / Light TOC Basemap matching InteractiveTrafficMap
  useEffect(() => {
    const baseGroup = baseLayerGroupRef.current;
    if (!baseGroup) return;
    baseGroup.clearLayers();
    const baseName = theme === 'dark' ? 'World_Dark_Gray_Base' : 'World_Light_Gray_Base';
    const refName = theme === 'dark' ? 'World_Dark_Gray_Reference' : 'World_Light_Gray_Reference';

    L.tileLayer(
      `https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/${baseName}/MapServer/tile/{z}/{y}/{x}`,
      {
        attribution: '© Esri · OSM',
        maxNativeZoom: 16,
        maxZoom: 19,
      }
    ).addTo(baseGroup);

    L.tileLayer(
      `https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/${refName}/MapServer/tile/{z}/{y}/{x}`,
      {
        maxNativeZoom: 16,
        maxZoom: 19,
        opacity: theme === 'dark' ? 0.78 : 0.88,
      }
    ).addTo(baseGroup);
  }, [theme]);

  // 3. Render Live Kafka Polylines + Selected Cyan Halo & Floating Pin
  useEffect(() => {
    const group = streamGroupRef.current;
    if (!group) return;
    group.clearLayers();

    const activeKey = activeItem?.key ?? null;
    plottedItems.forEach((item) => {
      if (item.key !== activeKey) {
        addNonSelectedLayer(group, item, theme, onSelectRow);
      }
    });

    if (activeItem) {
      addSelectedLayer(group, activeItem, onSelectRow);
    }
  }, [plottedItems, activeItem, theme, onSelectRow]);

  // 4. Auto-fit bounds on first arrival & smooth pan/zoom when selectedKey changes
  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;

    const maxSeqId = plottedItems.reduce((acc, item) => Math.max(acc, Number(item.row.seq_id ?? 0)), 0);
    if (maxSeqId > 0 && maxSeqId < lastMaxSeqRef.current) {
      hasAutoFittedRef.current = false;
      lastFocusedKeyRef.current = null;
    }

    if (plottedItems.length === 0) {
      hasAutoFittedRef.current = false;
      lastFocusedKeyRef.current = null;
      lastMaxSeqRef.current = 0;
      if (wasPopulatedRef.current) {
        wasPopulatedRef.current = false;
        setCameraMode('overview');
        map.stop();
        map.flyTo(HCMC_CENTER, HCMC_DEFAULT_ZOOM, { duration: 0.5 });
      }
      return;
    }

    lastMaxSeqRef.current = maxSeqId;
    wasPopulatedRef.current = true;

    if (selectedKey && selectedKey !== lastFocusedKeyRef.current) {
      const target = plottedItems.find((item) => item.key === selectedKey);
      if (target) {
        lastFocusedKeyRef.current = selectedKey;
        hasAutoFittedRef.current = true;
        setCameraMode('focus');
        map.stop();
        focusSingleItem(map, target);
        return;
      }
    }

    if (!hasAutoFittedRef.current) {
      hasAutoFittedRef.current = true;
      setCameraMode('overview');
      map.stop();
      fitAllPlottedBounds(map, plottedItems);
    }
  }, [plottedItems, selectedKey]);

  const handleFitOverview = () => {
    const map = mapRef.current;
    setCameraMode('overview');
    if (map) {
      map.stop();
      fitAllPlottedBounds(map, plottedItems);
    }
  };

  const handleFocusSelected = () => {
    const map = mapRef.current;
    if (!map || !activeItem) return;
    setCameraMode('focus');
    lastFocusedKeyRef.current = activeItem.key;
    map.stop();
    focusSingleItem(map, activeItem);
  };

  return (
    <div>
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          marginBottom: 8,
          gap: 6,
          flexWrap: 'wrap',
        }}
      >
        <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
          <button
            type="button"
            className={`pill-btn ${cameraMode === 'overview' ? 'active' : ''}`}
            onClick={handleFitOverview}
          >
            🗺️ Toàn TP.HCM
          </button>
          <button
            type="button"
            className={`pill-btn ${cameraMode === 'focus' ? 'active' : ''}`}
            disabled={!activeItem}
            onClick={handleFocusSelected}
          >
            🔍 Phóng vào sự kiện chọn
          </button>
        </div>

        <button
          type="button"
          className="pill-btn"
          onClick={() => setTheme((prev) => (prev === 'dark' ? 'light' : 'dark'))}
        >
          {theme === 'dark' ? '☀️ Đổi Nền Sáng' : '🌙 Đổi Nền Tối'}
        </button>
      </div>

      <div style={{ position: 'relative' }}>
        <div
          ref={containerRef}
          className={`leaflet-box live-map-box ${theme === 'dark' ? 'map-dark' : 'map-light'}`}
        />

        {plottedItems.length > 0 && (
          <div className="toc-map-hud-top">
            <div style={{ fontWeight: 750, fontSize: '11.5px', display: 'flex', alignItems: 'center', gap: 6 }}>
              <span className={`live-pulse-dot ${isStreaming ? 'active' : ''}`} />
              {isStreaming ? 'Đang nhận luồng Kafka Live' : 'Bản đồ Luồng Live v1'}
            </div>
            <div style={{ fontSize: '11px', opacity: 0.92, marginTop: 2 }}>
              Hiển thị <b>{plottedItems.length} đoạn</b> từ <b>{rows.length}</b> bản ghi mới nhất
            </div>
          </div>
        )}

        {rows.length === 0 && (
          <div className="live-map-empty-overlay">
            <div className="live-map-empty-pill">
              Chưa có sự kiện Live — nhấn <b>🎬 Phát Story</b> hoặc <b>⚡ Phát 200 tin Stress-Test</b> ở trên để xem
              luồng chảy trực tiếp lên bản đồ
            </div>
          </div>
        )}

        {rows.length > 0 && plottedItems.length === 0 && (
          <div className="live-map-empty-overlay">
            <div className="live-map-empty-pill">
              Đã nhận <b>{rows.length}</b> sự kiện Live — đang đồng bộ tọa độ đoạn đường để hiển thị lên bản đồ
            </div>
          </div>
        )}

        <div className="toc-map-legend">
          <span>
            <i style={{ background: '#38bdf8', border: '2px solid #ef4444' }} /> Sự kiện chọn
          </span>
          <span>
            <i style={{ background: '#ef4444' }} /> Kẹt E–F
          </span>
          <span>
            <i style={{ background: '#f59e0b' }} /> Đông C–D
          </span>
          <span>
            <i style={{ background: '#10b981' }} /> Thoáng A–B
          </span>
        </div>
      </div>
    </div>
  );
};
