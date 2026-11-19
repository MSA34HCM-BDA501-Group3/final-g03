import React, { useEffect, useRef, useState } from 'react';
import L from 'leaflet';
import { HotspotItem, MapSegmentItem, NeighborSegment, SegmentTimelineResponse } from '../types';

interface Props {
  segments: MapSegmentItem[];
  activeSegmentsTotal?: number;
  selectedSegmentId: number;
  selectedTimeline: SegmentTimelineResponse | null;
  hotspots: HotspotItem[];
  onSelectSegment: (segId: number) => void;
}

const MAX_CITY_BEACONS = 240;

const LOS_COLORS: Record<string, string> = {
  A: '#10b981',
  B: '#34d399',
  C: '#f59e0b',
  D: '#f97316',
  E: '#ef4444',
  F: '#dc2626',
};

function escapeHtml(value: unknown): string {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function bearingDeg(sLat: number, sLon: number, eLat: number, eLon: number): number {
  const dy = eLat - sLat;
  const dx = (eLon - sLon) * Math.cos(((sLat + eLat) * 0.5 * Math.PI) / 180);
  const rad = Math.atan2(dx, dy);
  return ((rad * 180) / Math.PI + 360) % 360;
}

function formatPinForecastLine(fc: SegmentTimelineResponse['forecast_comparison']): string {
  const safeCurLos = escapeHtml(fc.current_los);
  const safeNextLos = escapeHtml(fc.effective_forecast_los);
  const sevPct = (fc.severe_prob * 100).toFixed(1);
  const chgPct = (fc.change_prob * 100).toFixed(1);
  return (
    `<div class="toc-focus-pin-sub">` +
    `Hiện tại: <b>LOS ${safeCurLos}</b> (${fc.current_velocity} km/h) ➔ RF +30p: <b class="forecast-tag">LOS ${safeNextLos}</b>` +
    `</div>` +
    `<div class="toc-focus-pin-sub">` +
    `Nguy cơ E/F: <b class="forecast-tag">${sevPct}%</b> · Đổi pha: <b>${chgPct}%</b>` +
    `</div>`
  );
}

function makeArrowIcon(angleDeg: number, color: string, size = 14): L.DivIcon {
  return L.divIcon({
    className: 'toc-arrow-icon',
    iconSize: [size, size],
    iconAnchor: [size / 2, size / 2],
    html: `<div style="transform:rotate(${angleDeg.toFixed(1)}deg);color:${color};font-size:${size}px;line-height:1;text-shadow:0 0 4px rgba(0,0,0,0.85);">▲</div>`,
  });
}

export const InteractiveTrafficMap: React.FC<Props> = ({
  segments,
  activeSegmentsTotal,
  selectedSegmentId,
  selectedTimeline,
  hotspots,
  onSelectSegment,
}) => {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<L.Map | null>(null);
  const baseLayerGroupRef = useRef<L.LayerGroup | null>(null);
  const trafficGroupRef = useRef<L.LayerGroup | null>(null);

  const [viewMode, setViewMode] = useState<'corridor' | 'city'>('corridor');
  const [theme, setTheme] = useState<'dark' | 'light'>('dark');
  const [showHotspots, setShowHotspots] = useState<boolean>(false);

  const selectedCoords = selectedTimeline?.coords || null;
  const neighbors: NeighborSegment[] = selectedTimeline?.neighbors || [];
  const citySegments = segments.slice(0, MAX_CITY_BEACONS);

  // 1. Initialize Leaflet Map once
  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;
    const map = L.map(containerRef.current, {
      center: [10.7827, 106.6958],
      zoom: 16,
      zoomControl: true,
    });
    map.attributionControl.setPrefix(false);
    baseLayerGroupRef.current = L.layerGroup().addTo(map);
    trafficGroupRef.current = L.layerGroup().addTo(map);
    mapRef.current = map;
    return () => {
      map.remove();
      mapRef.current = null;
    };
  }, []);

  // 2. Switch Clean TOC Basemap (Dark Gray Radar vs Light Gray Minimal)
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

  // 3. Auto-switch back to Corridor Focus whenever user picks a specific segment
  useEffect(() => {
    setViewMode('corridor');
  }, [selectedSegmentId]);

  // 4. Render Focused Corridor View OR Clean City-Wide Radar Beacons
  useEffect(() => {
    const map = mapRef.current;
    const group = trafficGroupRef.current;
    if (!map || !group) return;
    group.clearLayers();

    if (viewMode === 'corridor') {
      const labeledCrossStreets = new Set<string>();
      const placedPillPoints: Array<[number, number]> = [];
      if (selectedCoords) {
        placedPillPoints.push([
          (selectedCoords[0] + selectedCoords[2]) * 0.5,
          (selectedCoords[1] + selectedCoords[3]) * 0.5,
        ]);
      }
      const mainStreet = selectedTimeline?.street_name || '';

      // A. Draw Upstream Inflow & Downstream Outflow Cross-Streets first
      neighbors
        .filter((nb) => nb.relation !== 'SAME_CORRIDOR')
        .forEach((nb, idx) => {
          const [sLat, sLon, eLat, eLon] = nb.coords;
          const isInflow = nb.relation === 'UPSTREAM_INFLOW';
          const color = isInflow ? '#38bdf8' : '#a78bfa';
          const safeStreet = escapeHtml(nb.street_name);
          const safeLos = escapeHtml(nb.current_los);

          const line = L.polyline(
            [
              [sLat, sLon],
              [eLat, eLon],
            ],
            {
              color,
              weight: isInflow ? 4.2 : 3.6,
              dashArray: isInflow ? undefined : '6, 6',
              opacity: 0.88,
              lineCap: 'round',
            }
          );
          line.bindTooltip(
            `<b>${safeStreet} (#${nb.segment_id})</b><br/>` +
              `${isInflow ? '🟦 Nhánh đổ vào nút giao (Upstream)' : '🟪 Nhánh thoát khỏi nút giao (Downstream)'}<br/>` +
              `Trạng thái: <b>${nb.current_los === 'COLD_START' ? 'Cold-Start (0 GPS khung này)' : `LOS ${safeLos}`}</b>`,
            { sticky: true }
          );
          line.on('click', () => onSelectSegment(nb.segment_id));
          line.addTo(group);

          if (idx % 5 === 1) {
            const midLat = (sLat + eLat) * 0.5;
            const midLon = (sLon + eLon) * 0.5;
            const ang = bearingDeg(sLat, sLon, eLat, eLon);
            L.marker([midLat, midLon], {
              icon: makeArrowIcon(ang, color, 11),
              interactive: false,
            }).addTo(group);
          }

          const pLat = (sLat + eLat) * 0.5;
          const pLon = (sLon + eLon) * 0.5;
          const farEnough = placedPillPoints.every(
            ([alat, alon]) => Math.hypot(pLat - alat, pLon - alon) >= 0.00105
          );
          if (
            nb.street_name &&
            nb.street_name !== mainStreet &&
            !nb.street_name.startsWith('Segment') &&
            !labeledCrossStreets.has(nb.street_name) &&
            labeledCrossStreets.size < 3 &&
            farEnough
          ) {
            labeledCrossStreets.add(nb.street_name);
            placedPillPoints.push([pLat, pLon]);
            L.marker([pLat, pLon], {
              interactive: false,
              icon: L.divIcon({
                className: 'toc-street-pill-wrap',
                iconSize: [140, 22],
                iconAnchor: [70, 11],
                html: `<span class="toc-street-pill ${isInflow ? 'inflow' : 'outflow'}">${isInflow ? '➔ ' : '↗ '}${safeStreet}</span>`,
              }),
            }).addTo(group);
          }
        });

      // B. Draw Full Target Street / Roundabout Corridor (`SAME_CORRIDOR`)
      neighbors
        .filter((nb) => nb.relation === 'SAME_CORRIDOR')
        .forEach((nb, idx) => {
          const [sLat, sLon, eLat, eLon] = nb.coords;
          const hasObs = nb.current_los !== 'COLD_START';
          const cColor = hasObs ? LOS_COLORS[nb.current_los] || '#f59e0b' : '#fb923c';
          const safeStreet = escapeHtml(nb.street_name);
          const safeLos = escapeHtml(nb.current_los);

          L.polyline(
            [
              [sLat, sLon],
              [eLat, eLon],
            ],
            {
              color: theme === 'dark' ? '#0f172a' : '#ffffff',
              weight: 9.5,
              opacity: 0.75,
              lineCap: 'round',
            }
          ).addTo(group);

          const cLine = L.polyline(
            [
              [sLat, sLon],
              [eLat, eLon],
            ],
            {
              color: cColor,
              weight: 6,
              opacity: 0.96,
              lineCap: 'round',
            }
          );
          cLine.bindTooltip(
            `<b>${safeStreet} (#${nb.segment_id})</b> · Cùng trục chính / vòng xoay<br/>` +
              `Trạng thái: <b>${hasObs ? `LOS ${safeLos}` : 'Nối liền trục đang chọn'}</b> (Nhấp để xem)`,
            { sticky: true }
          );
          cLine.on('click', () => onSelectSegment(nb.segment_id));
          cLine.addTo(group);

          if (idx % 4 === 0) {
            const ang = bearingDeg(sLat, sLon, eLat, eLon);
            L.marker([(sLat + eLat) * 0.5, (sLon + eLon) * 0.5], {
              icon: makeArrowIcon(ang, '#ffffff', 11),
              interactive: false,
            }).addTo(group);
          }
        });

      // C. Draw Exact Selected Segment with High-Contrast Glow + Floating Callout Pin
      if (selectedCoords && selectedTimeline) {
        const [sLat, sLon, eLat, eLon] = selectedCoords;
        const fc = selectedTimeline.forecast_comparison;
        const curLos = fc.current_los;
        const nextLos = fc.effective_forecast_los;
        const isWorsening = nextLos > curLos;
        const mainColor = isWorsening ? '#ff2e63' : LOS_COLORS[curLos] || '#ef4444';
        const safeMainStreet = escapeHtml(selectedTimeline.street_name);
        const pinForecastHtml = formatPinForecastLine(fc);

        L.polyline(
          [
            [sLat, sLon],
            [eLat, eLon],
          ],
          {
            color: '#22d3ee',
            weight: 16,
            opacity: 0.5,
            lineCap: 'round',
          }
        ).addTo(group);

        L.polyline(
          [
            [sLat, sLon],
            [eLat, eLon],
          ],
          {
            color: mainColor,
            weight: 8,
            opacity: 1,
            lineCap: 'round',
          }
        ).addTo(group);

        const midLat = (sLat + eLat) * 0.5;
        const midLon = (sLon + eLon) * 0.5;
        const ang = bearingDeg(sLat, sLon, eLat, eLon);
        L.marker([midLat, midLon], {
          icon: makeArrowIcon(ang, '#ffffff', 15),
          interactive: false,
        }).addTo(group);

        L.marker([midLat, midLon], {
          interactive: false,
          zIndexOffset: 1000,
          icon: L.divIcon({
            className: 'toc-focus-pin-wrap',
            iconSize: [286, 68],
            iconAnchor: [143, 76],
            html: `
              <div class="toc-focus-pin">
                <div class="toc-focus-pin-title">📍 ${safeMainStreet} (#${selectedSegmentId})</div>
                ${pinForecastHtml}
              </div>
            `,
          }),
        }).addTo(group);
      }
    } else {
      // CITY-WIDE RADAR MODE
      citySegments.forEach((seg) => {
        const [sLat, sLon, eLat, eLon] = seg.coords;
        const midLat = (sLat + eLat) * 0.5;
        const midLon = (sLon + eLon) * 0.5;
        const isSelected = seg.segment_id === selectedSegmentId;
        const isEarlyWarn =
          seg.alert_type === 'ONSET_A_D_TO_SEVERE' ||
          seg.alert_type === 'SEVERE_WORSENING_E_TO_F' ||
          seg.alert_type === 'WORSENING_TRANSITION';
        const isSevere = seg.current_los === 'E' || seg.current_los === 'F';
        const fill = isEarlyWarn ? '#ff2e63' : LOS_COLORS[seg.current_los] || '#10b981';
        const radius = isSelected ? 9 : isEarlyWarn ? 7.5 : isSevere ? 5.5 : 3.8;
        const safeStreet = escapeHtml(seg.street_name);
        const safeCurLos = escapeHtml(seg.current_los);
        const safeEffLos = escapeHtml(seg.effective_forecast_los || seg.rf_cal_los);

        if (isEarlyWarn || isSelected) {
          L.circleMarker([midLat, midLon], {
            radius: radius + 5,
            color: isSelected ? '#22d3ee' : '#ff2e63',
            weight: 1.5,
            fillColor: isSelected ? '#22d3ee' : '#ff2e63',
            fillOpacity: 0.24,
            interactive: false,
          }).addTo(group);
        }

        L.circleMarker([midLat, midLon], {
          radius,
          color: theme === 'dark' ? '#0f172a' : '#ffffff',
          weight: 1.5,
          fillColor: fill,
          fillOpacity: 0.95,
        })
          .bindTooltip(
            `<b>${safeStreet} (#${seg.segment_id})</b><br/>` +
              `Hiện tại: <b>LOS ${safeCurLos}</b> (${seg.mean_velocity}/${seg.max_velocity} km/h)<br/>` +
              `RF +30p: <b>LOS ${safeEffLos}</b> · Nguy cơ E/F: ${(seg.severe_prob * 100).toFixed(1)}% · Đổi pha: ${(seg.change_prob * 100).toFixed(1)}%<br/>` +
              `<i>Nhấp để phóng cận cảnh giao lộ</i>`,
            { sticky: true }
          )
          .on('click', () => {
            onSelectSegment(seg.segment_id);
            setViewMode('corridor');
          })
          .addTo(group);
      });
    }

    // D. Optional Top-25 EWMA Persistent Hotspots Overlay (UC-1)
    if (showHotspots) {
      hotspots.forEach((h, idx) => {
        if (!h.coords) return;
        const [sLat, sLon, eLat, eLon] = h.coords;
        const midLat = (sLat + eLat) * 0.5;
        const midLon = (sLon + eLon) * 0.5;
        const safeStreet = escapeHtml(h.street_name);
        const safeLos = escapeHtml(h.latest_los);
        L.marker([midLat, midLon], {
          icon: L.divIcon({
            className: 'toc-ewma-badge-wrap',
            iconSize: [26, 26],
            iconAnchor: [13, 13],
            html: `<div class="toc-ewma-badge">#${idx + 1}</div>`,
          }),
        })
          .bindTooltip(
            `🔥 <b>Top #${idx + 1} EWMA Hotspot: ${safeStreet} (#${h.segment_id})</b><br/>` +
              `Điểm EWMA: <b>${Number(h.decaying_hotspot_score).toFixed(2)}</b> · ${h.latest_velocity_kmh} km/h (LOS ${safeLos})`
          )
          .on('click', () => {
            onSelectSegment(h.segment_id);
            setViewMode('corridor');
          })
          .addTo(group);
      });
    }
  }, [viewMode, theme, citySegments, selectedSegmentId, selectedCoords, selectedTimeline, neighbors, hotspots, showHotspots, onSelectSegment]);

  // 5. Smooth Camera Framing for Corridor vs City-Wide
  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    map.stop();
    if (viewMode === 'city') {
      map.flyTo([10.786, 106.686], 12.5, { duration: 0.6 });
      return;
    }
    if (!selectedCoords || selectedTimeline?.segment_id !== selectedSegmentId) return;
    const pts: Array<[number, number]> = [
      [selectedCoords[0], selectedCoords[1]],
      [selectedCoords[2], selectedCoords[3]],
    ];
    neighbors.slice(0, 32).forEach((nb) => {
      pts.push([nb.coords[0], nb.coords[1]]);
      pts.push([nb.coords[2], nb.coords[3]]);
    });
    const bounds = L.latLngBounds(pts);
    map.fitBounds(bounds, {
      paddingTopLeft: [44, 128],
      paddingBottomRight: [44, 52],
      maxZoom: 16.5,
      animate: true,
    });
  }, [viewMode, selectedSegmentId, selectedCoords, selectedTimeline, neighbors]);

  const inflowCount = neighbors.filter((n) => n.relation === 'UPSTREAM_INFLOW').length;
  const outflowCount = neighbors.filter((n) => n.relation === 'DOWNSTREAM_OUTFLOW').length;
  const corridorCount = neighbors.filter((n) => n.relation === 'SAME_CORRIDOR').length + 1;
  const totalAtSlot = activeSegmentsTotal ?? segments.length;

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8, gap: 6, flexWrap: 'wrap' }}>
        <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
          <button
            className={`pill-btn ${viewMode === 'corridor' ? 'active' : ''}`}
            onClick={() => setViewMode('corridor')}
          >
            🎯 Cận cảnh Giao lộ (#{selectedSegmentId})
          </button>
          <button
            className={`pill-btn ${viewMode === 'city' ? 'active' : ''}`}
            onClick={() => setViewMode('city')}
          >
            🏙️ Toàn TP.HCM ({citySegments.length}
            {totalAtSlot > citySegments.length ? `/${totalAtSlot.toLocaleString()}` : ''} đoạn hiển thị)
          </button>
          <button
            className={`pill-btn ${showHotspots ? 'active' : ''}`}
            onClick={() => setShowHotspots((v) => !v)}
          >
            🔥 {showHotspots ? `Ẩn ${hotspots.length} Điểm nóng EWMA` : `${hotspots.length} Điểm nóng EWMA (UC-1)`}
          </button>
        </div>

        <button
          className="pill-btn"
          onClick={() => setTheme((t) => (t === 'dark' ? 'light' : 'dark'))}
          title="Chuyển nền bản đồ tối giản không nhiễu"
        >
          {theme === 'dark' ? '☀️ Đổi Nền Sáng Tối Giản' : '🌙 Đổi Nền Radar Tối'}
        </button>
      </div>

      <div style={{ position: 'relative' }}>
        <div ref={containerRef} className={`leaflet-box ${theme === 'dark' ? 'map-dark' : 'map-light'}`} />

        {viewMode === 'corridor' && selectedTimeline && (
          <div className="toc-map-hud-top">
            <div style={{ fontWeight: 750, fontSize: '11.5px', marginBottom: 2 }}>
              🧭 Cấu trúc Nút giao: {selectedTimeline.street_name}
            </div>
            <div style={{ fontSize: '11px', opacity: 0.92 }}>
              Trục chính: <b>{corridorCount} đoạn</b> · Đổ vào: <b style={{ color: '#38bdf8' }}>{inflowCount} nhánh</b> · Thoát ra:{' '}
              <b style={{ color: '#c4b5fd' }}>{outflowCount} nhánh</b>
            </div>
          </div>
        )}

        <div className="toc-map-legend">
          <span>
            <i style={{ background: '#22d3ee', border: '2px solid #ff2e63' }} /> Đoạn đang chọn
          </span>
          <span>
            <i style={{ background: '#ef4444' }} /> Sắp tắc / Kẹt E–F
          </span>
          <span>
            <i style={{ background: '#f59e0b' }} /> Đông C–D
          </span>
          <span>
            <i style={{ background: '#10b981' }} /> Thoáng A–B
          </span>
          {viewMode === 'corridor' && (
            <>
              <span>
                <i style={{ background: '#38bdf8' }} /> Nhánh đổ vào (▲)
              </span>
              <span>
                <i style={{ background: '#a78bfa' }} /> Nhánh thoát ra
              </span>
            </>
          )}
        </div>
      </div>
    </div>
  );
};
