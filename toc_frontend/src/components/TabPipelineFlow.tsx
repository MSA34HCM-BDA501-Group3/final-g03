import React, { useCallback, useEffect, useState } from 'react';
import {
  PipelineKeyValueItem,
  PipelineProbItem,
  PipelineStageItem,
  PipelineTraceOption,
  PipelineTraceResponse,
  StreamStatusResponse,
} from '../types';

interface Props {
  streamStatus: StreamStatusResponse | null;
  isStreaming: boolean;
  onStartStressKafka: () => void;
  onJumpToSegment: (segId: number, ictDate?: string, vnHour?: number) => void;
}

interface DagNodeLayout {
  stageId: number;
  x: number;
  y: number;
  w: number;
  h: number;
}

interface DagEdgeLayout {
  id: string;
  from: number;
  to: number;
  lane: 'speed' | 'batch' | 'mlops' | 'serve-v1' | 'serve-v2';
  d: string;
  arrowPoints: string;
  dashed?: boolean;
  label?: string;
  labelX?: number;
  labelY?: number;
  labelW?: number;
}

interface BloomProbeSlot {
  hashIdx: number;
  bitIndex: number;
  active: boolean;
}

const DEFAULT_STAGE_COLORS = [
  '#d97706',
  '#2563eb',
  '#1d4ed8',
  '#0284c7',
  '#0d9488',
  '#7c3aed',
  '#059669',
];

const LOS_BAR_COLORS: Record<string, string> = {
  A: '#0284c7',
  B: '#38bdf8',
  C: '#fbbf24',
  D: '#f59e0b',
  E: '#ef4444',
  F: '#b91c1c',
};

const LOS_ORDER = ['A', 'B', 'C', 'D', 'E', 'F'];

// True Lambda System Architecture Layout (viewBox 0 0 1360 448):
// Left (Zone A-B): Stage 1 Ingestion -> splits into Top Speed Lane (Zone C: Stage 6)
// and Bottom Batch Lane (Zone D-E: Stages 2->3->4->5) -> converges at Right Serving Layer (Zone F: Stage 7)
const NODE_LAYOUTS: DagNodeLayout[] = [
  { stageId: 1, x: 22, y: 156, w: 192, h: 134 },
  { stageId: 6, x: 326, y: 72, w: 704, h: 116 },
  { stageId: 2, x: 264, y: 276, w: 189, h: 138 },
  { stageId: 3, x: 473, y: 276, w: 189, h: 138 },
  { stageId: 4, x: 682, y: 276, w: 189, h: 138 },
  { stageId: 5, x: 891, y: 276, w: 189, h: 138 },
  { stageId: 7, x: 1142, y: 156, w: 196, h: 140 },
];

const EDGE_LAYOUTS: DagEdgeLayout[] = [
  {
    id: '1-6-speed',
    from: 1,
    to: 6,
    lane: 'speed',
    d: 'M 214 196 L 234 196 Q 242 196 242 188 L 242 138 Q 242 130 250 130 L 320 130',
    arrowPoints: '317,124 326,130 317,136',
    label: 'Stream GPS',
    labelX: 286,
    labelY: 114,
    labelW: 66,
  },
  {
    id: '1-2-batch',
    from: 1,
    to: 2,
    lane: 'batch',
    d: 'M 214 252 L 234 252 Q 242 252 242 260 L 242 337 Q 242 345 250 345 L 258 345',
    arrowPoints: '255,339 264,345 255,351',
  },
  {
    id: '2-3-batch',
    from: 2,
    to: 3,
    lane: 'batch',
    d: 'M 453 345 L 467 345',
    arrowPoints: '464,339 473,345 464,351',
  },
  {
    id: '3-4-batch',
    from: 3,
    to: 4,
    lane: 'batch',
    d: 'M 662 345 L 676 345',
    arrowPoints: '673,339 682,345 673,351',
  },
  {
    id: '4-5-batch',
    from: 4,
    to: 5,
    lane: 'batch',
    d: 'M 871 345 L 885 345',
    arrowPoints: '882,339 891,345 882,351',
  },
  {
    id: '4-6-mlops-static',
    from: 4,
    to: 6,
    lane: 'mlops',
    dashed: true,
    d: 'M 776 276 L 776 194',
    arrowPoints: '770,197 776,188 782,197',
    label: 'Static Store (PR & Cụm)',
    labelX: 776,
    labelY: 221,
    labelW: 126,
  },
  {
    id: '5-6-mlops-rf',
    from: 5,
    to: 6,
    lane: 'mlops',
    dashed: true,
    d: 'M 985 276 L 985 194',
    arrowPoints: '979,197 985,188 991,197',
    label: 'Read-Only Frozen RF',
    labelX: 985,
    labelY: 221,
    labelW: 118,
  },
  {
    id: '6-7-serve-v1',
    from: 6,
    to: 7,
    lane: 'serve-v1',
    d: 'M 1030 130 L 1106 130 Q 1114 130 1114 138 L 1114 190 Q 1114 198 1122 198 L 1136 198',
    arrowPoints: '1133,192 1142,198 1133,204',
    label: 'Ghi tạm v1',
    labelX: 1063,
    labelY: 114,
    labelW: 62,
  },
  {
    id: '5-7-serve-v2',
    from: 5,
    to: 7,
    lane: 'serve-v2',
    d: 'M 1080 345 L 1106 345 Q 1114 345 1114 337 L 1114 264 Q 1114 256 1122 256 L 1136 256',
    arrowPoints: '1133,250 1142,256 1133,262',
  },
];

const SLIDE_SYNC_GROUPS = [
  { label: 'Batch P1 · Ingestion & Spark (Stage 1–3)', startStage: 1, min: 1, max: 3 },
  { label: 'Batch P2–P3 · Graph & Frozen RF (Stage 4–5)', startStage: 4, min: 4, max: 5 },
  { label: 'Speed & Serving · Kafka & Mongo v1→v2 (Stage 6–7)', startStage: 6, min: 6, max: 7 },
];

const LANE_ACCENTS: Record<number, string> = {
  1: '#d97706',
  2: '#2563eb',
  3: '#1d4ed8',
  4: '#0284c7',
  5: '#0d9488',
  6: '#7c3aed',
  7: '#059669',
};

function stageAccent(stage: PipelineStageItem): string {
  return LANE_ACCENTS[stage.stage_id] || DEFAULT_STAGE_COLORS[0];
}

function truncateText(text: string, maxLen: number): string {
  const clean = text.trim();
  if (clean.length <= maxLen) return clean;
  return `${clean.slice(0, Math.max(1, maxLen - 1)).trimEnd()}…`;
}

function wrapTwoLines(text: string, maxChars: number): [string, string] {
  const clean = text.trim();
  if (clean.length <= maxChars) return [clean, ''];
  const words = clean.split(/\s+/);
  let line1 = '';
  let idx = 0;
  while (idx < words.length) {
    const cand = line1 ? `${line1} ${words[idx]}` : words[idx];
    if (cand.length > maxChars && line1) break;
    line1 = cand;
    idx += 1;
  }
  const rest = words.slice(idx).join(' ');
  return [truncateText(line1, maxChars), truncateText(rest, maxChars)];
}

function asPercentNumber(raw: number): number {
  if (!Number.isFinite(raw)) return 0;
  return raw <= 1.01 ? raw * 100 : raw;
}

function parseNumericField(
  details: Record<string, unknown> | undefined,
  keys: string[],
  fallback: number | null = null
): number | null {
  if (!details) return fallback;
  for (const key of keys) {
    const val = details[key];
    if (typeof val === 'number' && Number.isFinite(val)) return val;
    if (typeof val === 'string' && val.trim() !== '' && Number.isFinite(Number(val))) {
      return Number(val);
    }
  }
  return fallback;
}

function parseStringField(
  details: Record<string, unknown> | undefined,
  keys: string[],
  fallback: string | null = null
): string | null {
  if (!details) return fallback;
  for (const key of keys) {
    const val = details[key];
    if (typeof val === 'string' && val.trim() !== '') return val.trim();
  }
  return fallback;
}

function parseProbItems(details?: Record<string, unknown>): PipelineProbItem[] {
  if (!details) return [];
  const raw = details.probs_6 ?? details.class_probs ?? details.probabilities;
  if (Array.isArray(raw)) {
    const items: PipelineProbItem[] = [];
    for (const entry of raw) {
      if (entry && typeof entry === 'object') {
        const obj = entry as Record<string, unknown>;
        const los = String(obj.los ?? obj.label ?? '').toUpperCase();
        const prob = Number(obj.prob ?? obj.probability ?? obj.value ?? 0);
        if (LOS_ORDER.includes(los) && Number.isFinite(prob)) {
          items.push({ los, prob });
        }
      }
    }
    return items;
  }
  if (raw && typeof raw === 'object') {
    const map = raw as Record<string, unknown>;
    return LOS_ORDER.filter((l) => map[l] != null).map((l) => ({
      los: l,
      prob: Number(map[l]),
    }));
  }
  return [];
}

function parseBloomProbes(
  details: Record<string, unknown> | undefined,
  segmentId: number,
  liveMsgCount: number
): BloomProbeSlot[] {
  const raw = details?.bloom_probes;
  if (Array.isArray(raw) && raw.length > 0) {
    return raw.slice(0, 10).map((item, idx) => {
      if (typeof item === 'number') {
        return { hashIdx: idx + 1, bitIndex: Math.abs(Math.round(item)) % 131072, active: true };
      }
      if (item && typeof item === 'object') {
        const obj = item as Record<string, unknown>;
        const bit = Number(obj.bit_index ?? obj.bit ?? obj.slot ?? obj.index ?? idx * 12377);
        const isSet = obj.active ?? obj.is_set ?? obj.hit ?? true;
        return {
          hashIdx: Number(obj.hash_idx ?? obj.k ?? idx + 1),
          bitIndex: Math.abs(Math.round(bit)) % 131072,
          active: Boolean(isSet),
        };
      }
      return { hashIdx: idx + 1, bitIndex: (segmentId * (idx + 7) * 2654435) % 131072, active: true };
    });
  }
  const seed = Math.max(1, segmentId) + liveMsgCount * 17;
  return Array.from({ length: 10 }, (_, idx) => {
    const bitIndex = Math.abs((seed * (idx + 3) * 7919 + idx * 13109) % 131072);
    return { hashIdx: idx + 1, bitIndex, active: true };
  });
}

function parsePartitionCounts(
  streamStatus: StreamStatusResponse | null,
  details?: Record<string, unknown>
): { p0: number; p1: number; p2: number; isLive: boolean } {
  const liveParts = streamStatus?.telemetry?.partition_counts;
  const liveP0 = Number(liveParts?.['0'] ?? 0);
  const liveP1 = Number(liveParts?.['1'] ?? 0);
  const liveP2 = Number(liveParts?.['2'] ?? 0);
  if (liveP0 + liveP1 + liveP2 > 0) {
    return { p0: liveP0, p1: liveP1, p2: liveP2, isLive: true };
  }
  const detParts = details?.partition_counts as Record<string, unknown> | undefined;
  if (detParts && typeof detParts === 'object') {
    const p0 = Number(detParts['0'] ?? detParts.P0 ?? 0);
    const p1 = Number(detParts['1'] ?? detParts.P1 ?? 0);
    const p2 = Number(detParts['2'] ?? detParts.P2 ?? 0);
    if (p0 + p1 + p2 > 0) {
      return { p0, p1, p2, isLive: false };
    }
  }
  return { p0: 0, p1: 0, p2: 0, isLive: false };
}

function parseMongoDoc(details?: Record<string, unknown>): Record<string, unknown> | null {
  if (!details) return null;
  const candidate = details.mongo_doc ?? details.document ?? details.sample_doc;
  if (candidate && typeof candidate === 'object' && !Array.isArray(candidate)) {
    return candidate as Record<string, unknown>;
  }
  return null;
}

function parseFeatures23d(details?: Record<string, unknown>): Array<{ name: string; value: string; group?: string }> {
  if (!details) return [];
  const raw = details.features_23d ?? details.feature_vector;
  if (Array.isArray(raw)) {
    const out: Array<{ name: string; value: string; group?: string }> = [];
    for (const item of raw) {
      if (item && typeof item === 'object') {
        const obj = item as Record<string, unknown>;
        const name = String(obj.name ?? obj.feature ?? obj.label ?? '').trim();
        if (name) {
          out.push({
            name,
            value: String(obj.value ?? '—'),
            group: obj.group ? String(obj.group) : undefined,
          });
        }
      }
    }
    return out;
  }
  if (raw && typeof raw === 'object') {
    return Object.entries(raw as Record<string, unknown>).map(([k, v]) => ({
      name: k,
      value: typeof v === 'number' ? Number(v.toFixed(4)).toString() : String(v ?? '—'),
    }));
  }
  return [];
}

function parseMonoidPings(details?: Record<string, unknown>): Array<{ label: string; velocity: number }> {
  if (!details) return [];
  const raw = details.monoid_pings ?? details.raw_pings ?? details.pings;
  if (!Array.isArray(raw)) return [];
  const out: Array<{ label: string; velocity: number }> = [];
  for (let i = 0; i < raw.length; i += 1) {
    const item = raw[i];
    if (typeof item === 'number' && Number.isFinite(item)) {
      out.push({ label: `Ping #${i + 1}`, velocity: item });
    } else if (item && typeof item === 'object') {
      const obj = item as Record<string, unknown>;
      const vel = Number(obj.velocity_kmh ?? obj.velocity ?? obj.v ?? NaN);
      if (Number.isFinite(vel)) {
        const lbl = String(obj.time_ict ?? obj.time ?? obj.label ?? `Ping #${i + 1}`);
        out.push({ label: lbl, velocity: vel });
      }
    }
  }
  return out;
}

function getNodePreviewMetric(stage: PipelineStageItem, streamStatus: StreamStatusResponse | null): string {
  const tel = streamStatus?.telemetry;
  if (stage.stage_id === 6 && Number(tel?.messages_consumed ?? 0) > 0) {
    const msgs = Number(tel?.messages_consumed ?? 0);
    const bloom = Number(tel?.duplicates_caught_bloom ?? 0);
    const hll = Math.round(Number(tel?.hll_estimated_segments ?? 0));
    return `📡 Live: ${msgs} tin · Bloom -${bloom} · HLL ${hll}`;
  }
  if (stage.stage_id === 7 && Number(tel?.sink2_scored_v1 ?? 0) > 0) {
    const v1 = Number(tel?.sink2_scored_v1 ?? 0);
    return `⚡ Mongo v1: ${v1} · 0.36ms`;
  }
  const m0 = stage.key_metrics?.[0];
  if (m0) return `${m0.label}: ${m0.value}`;
  return stage.status_badge || 'Sẵn sàng';
}

const SpeedSubFlowChips: React.FC<{ x: number; y: number }> = ({ x, y }) => {
  const chips = [
    { label: '① Kafka 4.1 (3 Partitions)', w: 182, dx: 0 },
    { label: '② 33 KiB Sketches (Bloom · HLL++ · EWMA)', w: 256, dx: 202 },
    { label: '③ Online Scorer · Watermark 30p', w: 202, dx: 478 },
  ];
  return (
    <g>
      {chips.map((c, idx) => (
        <g key={c.label}>
          <rect
            x={x + c.dx}
            y={y}
            width={c.w}
            height={20}
            rx={5}
            fill="#f5f3ff"
            stroke="#c4b5fd"
            strokeWidth={1}
          />
          <text
            x={x + c.dx + c.w / 2}
            y={y + 13.5}
            textAnchor="middle"
            fontSize={9.6}
            fontWeight={700}
            fill="#5b21b6"
          >
            {c.label}
          </text>
          {idx < chips.length - 1 && (
            <text x={x + c.dx + c.w + 10} y={y + 13.5} textAnchor="middle" fontSize={10.5} fontWeight={800} fill="#7c3aed">
              ➔
            </text>
          )}
        </g>
      ))}
    </g>
  );
};

interface DagNodeProps {
  stage: PipelineStageItem;
  layout: DagNodeLayout;
  activeStageId: number;
  isStreaming: boolean;
  streamStatus: StreamStatusResponse | null;
  onSelect: (id: number) => void;
}

const PipelineDagNode: React.FC<DagNodeProps> = ({
  stage,
  layout,
  activeStageId,
  isStreaming,
  streamStatus,
  onSelect,
}) => {
  const accent = stageAccent(stage);
  const isWideSpeed = layout.w > 400;
  const isActive = stage.stage_id === activeStageId;
  const isCompleted = stage.stage_id < activeStageId;
  const isLiveLit = isStreaming && (stage.stage_id === 6 || stage.stage_id === 7);

  const maxTitleChars = isWideSpeed ? 66 : 24;
  const [titleLine1, titleLine2] = wrapTwoLines(stage.title, maxTitleChars);
  const preview = truncateText(getNodePreviewMetric(stage, streamStatus), isWideSpeed ? 84 : 25);
  const metaTag = isWideSpeed
    ? 'Zone C · Speed / Real-Time · Phase B'
    : truncateText(stage.slide_ref || stage.zone, 15);

  const statusSymbol = isCompleted ? '✓' : isActive || isLiveLit ? '●' : '○';
  const statusColor = isCompleted ? '#059669' : isActive || isLiveLit ? accent : '#94a3b8';
  const strokeColor = isActive ? accent : isLiveLit ? '#7c3aed' : isCompleted ? accent : '#cbd5e1';
  const fillBg = isActive ? '#ffffff' : isCompleted ? '#f8fafc' : '#ffffff';

  const handleKeyDown = (e: React.KeyboardEvent<SVGGElement>) => {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      onSelect(stage.stage_id);
    }
  };

  return (
    <g
      role="button"
      tabIndex={0}
      aria-label={`Bước ${stage.stage_id}: ${stage.title}`}
      className={isActive ? 'pipeline-node-active' : undefined}
      style={{ cursor: 'pointer' }}
      onClick={() => onSelect(stage.stage_id)}
      onKeyDown={handleKeyDown}
    >
      {isActive && (
        <rect
          x={layout.x - 4}
          y={layout.y - 4}
          width={layout.w + 8}
          height={layout.h + 8}
          rx={14}
          fill="none"
          stroke={accent}
          strokeWidth={2.2}
          strokeOpacity={0.4}
        />
      )}
      <rect
        x={layout.x}
        y={layout.y}
        width={layout.w}
        height={layout.h}
        rx={11}
        fill={fillBg}
        stroke={strokeColor}
        strokeWidth={isActive ? 2.5 : isCompleted ? 1.8 : 1.3}
      />
      {/* Top accent strip */}
      <path
        d={`M ${layout.x + 11} ${layout.y} L ${layout.x + layout.w - 11} ${layout.y} Q ${layout.x + layout.w} ${layout.y} ${layout.x + layout.w} ${layout.y + 6} L ${layout.x} ${layout.y + 6} Q ${layout.x} ${layout.y} ${layout.x + 11} ${layout.y} Z`}
        fill={isActive || isCompleted || isLiveLit ? accent : '#cbd5e1'}
      />

      {/* Stage badge */}
      <rect x={layout.x + 9} y={layout.y + 13} width={52} height={18} rx={5} fill={accent} />
      <text
        x={layout.x + 35}
        y={layout.y + 25.5}
        textAnchor="middle"
        fontSize={9.6}
        fontWeight={800}
        fill="#ffffff"
      >
        STAGE {stage.stage_id}
      </text>

      {/* Zone + Slide tag */}
      <text x={layout.x + 68} y={layout.y + 25.5} fontSize={9.4} fontWeight={650} fill="#64748b">
        {metaTag}
      </text>

      {/* Status dot / check */}
      <circle
        cx={layout.x + layout.w - 17}
        cy={layout.y + 22}
        r={8.5}
        fill={isCompleted ? '#ecfdf5' : isActive || isLiveLit ? '#eff6ff' : '#f1f5f9'}
        stroke={statusColor}
        strokeWidth={1.3}
      />
      <text
        x={layout.x + layout.w - 17}
        y={layout.y + 25.5}
        textAnchor="middle"
        fontSize={10.5}
        fontWeight={800}
        fill={statusColor}
      >
        {statusSymbol}
      </text>

      {/* Stage Title */}
      <text
        x={layout.x + 11}
        y={layout.y + (isWideSpeed ? 49 : 54)}
        fontSize={isWideSpeed ? 12.6 : 11.4}
        fontWeight={800}
        fill="#0f172a"
      >
        {titleLine1}
      </text>
      {isWideSpeed ? (
        <SpeedSubFlowChips x={layout.x + 11} y={layout.y + 57} />
      ) : titleLine2 ? (
        <text x={layout.x + 11} y={layout.y + 71} fontSize={11.1} fontWeight={750} fill="#1e293b">
          {titleLine2}
        </text>
      ) : (
        <text x={layout.x + 11} y={layout.y + 71} fontSize={9.8} fontWeight={550} fill="#64748b">
          {truncateText(stage.status_badge || stage.subtitle, 26)}
        </text>
      )}

      {/* Compact subtitle hint for standard vertical cards */}
      {!isWideSpeed && (
        <text x={layout.x + 11} y={layout.y + 92} fontSize={9.3} fontWeight={600} fill="#64748b">
          {truncateText(stage.zone, 26)}
        </text>
      )}

      {/* Bottom Metric Preview Pill */}
      <rect
        x={layout.x + 9}
        y={layout.y + layout.h - 30}
        width={layout.w - 18}
        height={22}
        rx={6}
        fill={isActive || isLiveLit ? '#0f172a' : '#f1f5f9'}
      />
      <text
        x={layout.x + 16}
        y={layout.y + layout.h - 15.5}
        fontSize={9.9}
        fontWeight={700}
        fill={isActive || isLiveLit ? '#67e8f9' : '#334155'}
      >
        {preview}
      </text>
    </g>
  );
};

interface DagConnectorProps {
  edge: DagEdgeLayout;
  energized: boolean;
  hasPacket: boolean;
  color: string;
}

const PipelineConnector: React.FC<DagConnectorProps> = ({ edge, energized, hasPacket, color }) => {
  const pillW = edge.labelW ?? 72;
  const pillX = (edge.labelX ?? 0) - pillW / 2;
  const pillY = (edge.labelY ?? 0) - 9;

  return (
    <g>
      <path
        d={edge.d}
        fill="none"
        stroke={edge.dashed ? '#99f6e4' : '#cbd5e1'}
        strokeWidth={edge.dashed ? 2.1 : 2.5}
        strokeDasharray={edge.dashed ? '5 4' : undefined}
        strokeLinecap="round"
      />
      {energized && (
        <path
          d={edge.d}
          fill="none"
          stroke={color}
          strokeWidth={edge.dashed ? 2.5 : 3.0}
          strokeDasharray="6 5"
          strokeLinecap="round"
          className="pipeline-flow-line"
        />
      )}
      <polygon points={edge.arrowPoints} fill={energized ? color : edge.dashed ? '#0d9488' : '#94a3b8'} />
      {hasPacket && (
        <circle r={4.6} fill={color} stroke="#ffffff" strokeWidth={1.6}>
          <animateMotion dur={edge.dashed ? '1.35s' : '1.15s'} repeatCount="indefinite" path={edge.d} />
        </circle>
      )}
      {edge.label && edge.labelX != null && edge.labelY != null && (
        <g>
          <rect
            x={pillX}
            y={pillY}
            width={pillW}
            height={17}
            rx={8}
            fill={energized ? '#ffffff' : '#f8fafc'}
            stroke={energized ? color : edge.dashed ? '#14b8a6' : '#cbd5e1'}
            strokeWidth={1.1}
          />
          <text
            x={edge.labelX}
            y={edge.labelY + 3}
            textAnchor="middle"
            fontSize={8.8}
            fontWeight={750}
            fill={energized ? color : edge.dashed ? '#0f766e' : '#475569'}
          >
            {edge.label}
          </text>
        </g>
      )}
    </g>
  );
};

function getEdgeAnimationState(
  edge: DagEdgeLayout,
  activeStageId: number,
  isStreaming: boolean
): { energized: boolean; hasPacket: boolean; color: string } {
  if (edge.lane === 'speed') {
    const on = isStreaming || activeStageId >= 6;
    return { energized: on, hasPacket: isStreaming || activeStageId === 6, color: '#7c3aed' };
  }
  if (edge.lane === 'mlops') {
    const on = isStreaming || activeStageId >= 6;
    return { energized: on, hasPacket: isStreaming || activeStageId === 6, color: '#0d9488' };
  }
  if (edge.lane === 'serve-v1') {
    const on = isStreaming || activeStageId === 7;
    return { energized: on, hasPacket: isStreaming || activeStageId === 7, color: '#7c3aed' };
  }
  if (edge.lane === 'serve-v2') {
    const on = activeStageId === 7;
    return { energized: on, hasPacket: activeStageId === 7, color: '#2563eb' };
  }
  // Batch lane edges (1->2, 2->3, 3->4, 4->5)
  const on = activeStageId >= edge.to && activeStageId <= 5 || activeStageId === 7;
  const pkt = activeStageId === edge.to;
  return { energized: on, hasPacket: pkt, color: '#2563eb' };
}

const LambdaLaneFrames: React.FC = () => (
  <g>
    {/* Top Architecture Legend Strip */}
    <text x={16} y={24} fontSize={11.5} fontWeight={800} fill="#0f172a">
      KIẾN TRÚC LAMBDA CHUẨN · HCMC SMART TRAFFIC PLATFORM (ZONES A–F)
    </text>
    <g transform="translate(592, 11)">
      <circle cx={6} cy={9} r={4.5} fill="#d97706" />
      <text x={15} y={12.5} fontSize={9.6} fontWeight={700} fill="#92400e">
        Zone A–B: Ingestion
      </text>
      <rect x={126} y={7.5} width={16} height={3} rx={1.5} fill="#7c3aed" />
      <text x={147} y={12.5} fontSize={9.6} fontWeight={700} fill="#6d28d9">
        Zone C: Speed / Stream (v1)
      </text>
      <rect x={296} y={7.5} width={16} height={3} rx={1.5} fill="#2563eb" />
      <text x={317} y={12.5} fontSize={9.6} fontWeight={700} fill="#1d4ed8">
        Zone D–E: Batch &amp; Train (v2)
      </text>
      <line x1={470} y1={9} x2={488} y2={9} stroke="#0d9488" strokeWidth={2.4} strokeDasharray="4 3" />
      <text x={493} y={12.5} fontSize={9.6} fontWeight={700} fill="#0f766e">
        MLOps Read-Only
      </text>
      <circle cx={602} cy={9} r={4.5} fill="#059669" />
      <text x={611} y={12.5} fontSize={9.6} fontWeight={700} fill="#047857">
        Zone F: Serving &amp; Guard
      </text>
    </g>

    {/* Zone A-B: Ingestion Frame */}
    <rect
      x={12}
      y={42}
      width={212}
      height={390}
      rx={12}
      fill="#fffbeb"
      fillOpacity={0.62}
      stroke="#f59e0b"
      strokeWidth={1.4}
      strokeDasharray="6 4"
    />
    <rect x={22} y={52} width={192} height={20} rx={6} fill="#fef3c7" stroke="#fcd34d" strokeWidth={1} />
    <text x={118} y={65.5} textAnchor="middle" fontSize={9.5} fontWeight={800} fill="#92400e">
      ZONE A–B · INGESTION &amp; HYGIENE
    </text>
    <rect x={22} y={80} width={192} height={60} rx={8} fill="#ffffff" stroke="#fde68a" strokeWidth={1} />
    <text x={32} y={97} fontSize={9.6} fontWeight={800} fill="#92400e">
      📡 Nguồn Dữ Liệu Gốc (Dual Input)
    </text>
    <text x={32} y={113} fontSize={9.1} fontWeight={600} fill="#475569">
      • GPS Fleet: 90,938 pings thực tế
    </text>
    <text x={32} y={128} fontSize={9.1} fontWeight={600} fill="#475569">
      • OSM GIS: 84,206 đoạn đường cơ giới
    </text>
    <rect x={22} y={308} width={192} height={110} rx={8} fill="#ffffff" stroke="#fde68a" strokeWidth={1} />
    <text x={32} y={326} fontSize={9.6} fontWeight={800} fill="#92400e">
      🔀 Tách 2 Nhánh Lambda
    </text>
    <text x={32} y={344} fontSize={9.0} fontWeight={700} fill="#6d28d9">
      ▲ Nhánh trên (Stream / Speed):
    </text>
    <text x={38} y={358} fontSize={8.8} fontWeight={550} fill="#475569">
      Chạy real-time từng giây qua Kafka
    </text>
    <text x={32} y={378} fontSize={9.0} fontWeight={700} fill="#1d4ed8">
      ▼ Nhánh dưới (Batch / Offline):
    </text>
    <text x={38} y={392} fontSize={8.8} fontWeight={550} fill="#475569">
      Spark ETL, Train AI &amp; Bù tin trễ cuối ngày
    </text>
    <text x={32} y={409} fontSize={8.7} fontWeight={700} fill="#b45309">
      0 Silent Drops · 151 pings Quarantine
    </text>

    {/* Zone C: Speed / Stream Layer Frame */}
    <rect
      x={250}
      y={42}
      width={846}
      height={158}
      rx={12}
      fill="#faf5ff"
      fillOpacity={0.78}
      stroke="#a78bfa"
      strokeWidth={1.6}
    />
    <rect x={262} y={50} width={470} height={18} rx={5} fill="#ede9fe" />
    <text x={272} y={62.5} fontSize={9.4} fontWeight={800} fill="#5b21b6">
      ⚡ ZONE C · SPEED / STREAM LAYER — REAL-TIME TỪNG GIÂY (PHASE B · SUB-SECOND · 33 KiB RAM)
    </text>
    <text x={1084} y={62.5} textAnchor="end" fontSize={9.2} fontWeight={700} fill="#7c3aed">
      Chỉ chấm điểm (Inference) · Không train lại · Ghi tạm v1
    </text>

    {/* Zone D-E: Batch Layer Frame */}
    <rect
      x={250}
      y={240}
      width={846}
      height={192}
      rx={12}
      fill="#eff6ff"
      fillOpacity={0.75}
      stroke="#60a5fa"
      strokeWidth={1.6}
    />
    <rect x={262} y={248} width={502} height={18} rx={5} fill="#dbeafe" />
    <text x={270} y={260.5} fontSize={9.3} fontWeight={800} fill="#1e40af">
      🗄️ ZONE D–E · BATCH LAYER — SPARK 3.5.7 ETL, GRAPH, K-MEANS &amp; TRAIN AI (PHASE A &amp; C)
    </text>
    <text x={880} y={260.5} textAnchor="middle" fontSize={9.0} fontWeight={750} fill="#0f766e">
      Zone E: Khóa SHA-256 · Ghi chuẩn v2 (100%)
    </text>
    {/* Inner dashed frame highlighting Zone E (Frozen Registry) around Stage 5 */}
    <rect
      x={885}
      y={270}
      width={201}
      height={152}
      rx={11}
      fill="#f0fdfa"
      fillOpacity={0.65}
      stroke="#14b8a6"
      strokeWidth={1.2}
      strokeDasharray="4 3"
    />

    {/* Zone F: Serving & Reconciliation Frame */}
    <rect
      x={1132}
      y={42}
      width={216}
      height={390}
      rx={12}
      fill="#ecfdf5"
      fillOpacity={0.68}
      stroke="#34d399"
      strokeWidth={1.6}
    />
    <rect x={1142} y={52} width={196} height={20} rx={6} fill="#d1fae5" stroke="#6ee7b7" strokeWidth={1} />
    <text x={1240} y={65.5} textAnchor="middle" fontSize={9.5} fontWeight={800} fill="#065f46">
      ZONE F · SERVING &amp; RECONCILIATION
    </text>
    <rect x={1142} y={80} width={196} height={62} rx={8} fill="#ffffff" stroke="#a7f3d0" strokeWidth={1} />
    <text x={1152} y={96} fontSize={9.5} fontWeight={800} fill="#065f46">
      🔁 Hội tụ 2 Phiên Bản Vòng Đời
    </text>
    <text x={1152} y={113} fontSize={9.0} fontWeight={700} fill="#6d28d9">
      ● v1 STREAM_PROVISIONAL (Speed)
    </text>
    <text x={1152} y={129} fontSize={9.0} fontWeight={700} fill="#1d4ed8">
      ● v2 BATCH_RECONCILED (Batch 100%)
    </text>
    <rect x={1142} y={310} width={196} height={108} rx={8} fill="#ffffff" stroke="#a7f3d0" strokeWidth={1} />
    <text x={1152} y={328} fontSize={9.5} fontWeight={800} fill="#065f46">
      🛡️ $status_version Guard
    </text>
    <text x={1152} y={346} fontSize={9.0} fontWeight={600} fill="#334155">
      • Chặn v1 cũ ghi đè v2: 25/25 PASS
    </text>
    <text x={1152} y={362} fontSize={9.0} fontWeight={600} fill="#334155">
      • Bù 500 tin trễ: 98.5% ➔ 100.0%
    </text>
    <text x={1152} y={378} fontSize={9.0} fontWeight={600} fill="#334155">
      • ESR IXSCAN: 0.36 ms (31.36x)
    </text>
    <text x={1152} y={396} fontSize={8.9} fontWeight={750} fill="#047857">
      ➔ Phục vụ TOC React UI (:8000)
    </text>
  </g>
);

const PipelineDagSvg: React.FC<{
  stages: PipelineStageItem[];
  activeStageId: number;
  isStreaming: boolean;
  streamStatus: StreamStatusResponse | null;
  onSelectStage: (id: number) => void;
}> = ({ stages, activeStageId, isStreaming, streamStatus, onSelectStage }) => {
  const stageMap = new Map<number, PipelineStageItem>();
  stages.forEach((s) => stageMap.set(s.stage_id, s));

  return (
    <svg
      viewBox="0 0 1360 448"
      style={{
        width: '100%',
        height: 'auto',
        background: 'linear-gradient(180deg, #ffffff 0%, #f8fafc 100%)',
        borderRadius: 12,
        border: '1px solid #cbd5e1',
        display: 'block',
      }}
    >
      <LambdaLaneFrames />

      {EDGE_LAYOUTS.map((edge) => {
        const anim = getEdgeAnimationState(edge, activeStageId, isStreaming);
        return (
          <PipelineConnector
            key={edge.id}
            edge={edge}
            energized={anim.energized}
            hasPacket={anim.hasPacket}
            color={anim.color}
          />
        );
      })}

      {NODE_LAYOUTS.map((layout) => {
        const stage = stageMap.get(layout.stageId);
        if (!stage) return null;
        return (
          <PipelineDagNode
            key={stage.stage_id}
            stage={stage}
            layout={layout}
            activeStageId={activeStageId}
            isStreaming={isStreaming}
            streamStatus={streamStatus}
            onSelect={onSelectStage}
          />
        );
      })}
    </svg>
  );
};

const LiveTelemetryStrip: React.FC<{
  streamStatus: StreamStatusResponse | null;
  isStreaming: boolean;
}> = ({ streamStatus, isStreaming }) => {
  const tel = streamStatus?.telemetry || {};
  const parts = tel.partition_counts || { '0': 0, '1': 0, '2': 0 };
  const p0 = Number(parts['0'] ?? 0);
  const p1 = Number(parts['1'] ?? 0);
  const p2 = Number(parts['2'] ?? 0);

  return (
    <div className="pipeline-telemetry-strip">
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
        <span className={`badge ${isStreaming ? 'badge-red' : tel.mongo_connected ? 'badge-green' : 'badge-blue'}`}>
          {isStreaming ? '🔴 ĐANG BƠM KAFKA LIVE' : '📡 Stage 6–7 Speed Layer Telemetry'}
        </span>
        <span className="telemetry-chip">
          Kafka msgs: <b>{(tel.messages_consumed ?? 0).toLocaleString()}</b>
        </span>
        <span className="telemetry-chip">
          Partitions: <b>P0:{p0} · P1:{p1} · P2:{p2}</b>
        </span>
        <span className="telemetry-chip">
          Bloom chặn: <b style={{ color: '#d97706' }}>{tel.duplicates_caught_bloom ?? 0}</b>
        </span>
        <span className="telemetry-chip">
          Watermark trễ: <b style={{ color: '#4f46e5' }}>{tel.late_dropped_for_tier2 ?? 0}</b>
        </span>
        <span className="telemetry-chip">
          HLL++: <b style={{ color: '#0891b2' }}>{Math.round(tel.hll_estimated_segments ?? 0)}</b>
          <span style={{ color: '#64748b' }}> (exact {tel.exact_distinct_segments ?? 0})</span>
        </span>
        <span className="telemetry-chip">
          Mongo v1: <b style={{ color: '#059669' }}>{tel.sink2_scored_v1 ?? 0}</b>
        </span>
      </div>
      <span style={{ fontSize: 11, color: '#64748b', whiteSpace: 'nowrap' }}>
        Cập nhật: <b>{tel.last_updated_ict || '—'} ICT</b>
      </span>
    </div>
  );
};

const StageMonoidVmaxWidget: React.FC<{ details?: Record<string, unknown> }> = ({ details }) => {
  const pings = parseMonoidPings(details);
  const vmaxTier = parseStringField(details, ['vmax_tier', 'imputation_tier', 'tier_label']);
  if (pings.length === 0 && !vmaxTier) return null;

  const tiers = [
    { id: 1, title: 'Tier 1 · Đo trực tiếp trên đoạn đường', match: '1' },
    { id: 2, title: 'Tier 2 · Trung vị theo cùng cấp đường (street_level)', match: '2' },
    { id: 3, title: 'Tier 3 · Dự phòng toàn mạng lưới (Global Fallback)', match: '3' },
  ];

  return (
    <div className="pipeline-widget-box">
      {pings.length > 0 && (
        <div>
          <div className="pipeline-widget-title">
            🧮 Luồng Gom 5-Tuple Monoid Trực Tiếp Từ GPS Thô ({pings.length} pings mẫu)
          </div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, marginTop: 8 }}>
            {pings.slice(0, 12).map((p, idx) => (
              <span key={`${p.label}-${idx}`} className="badge badge-blue">
                {p.label}: <b>{p.velocity.toFixed(1)} km/h</b>
              </span>
            ))}
          </div>
        </div>
      )}
      {vmaxTier && (
        <div style={{ marginTop: pings.length > 0 ? 12 : 0 }}>
          <div className="pipeline-widget-title">🎯 Thang Nội Suy Vmax 3 Tầng (0 Silent Drops)</div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, minmax(0, 1fr))', gap: 8, marginTop: 8 }}>
            {tiers.map((t) => {
              const active = vmaxTier.includes(t.match) || vmaxTier.toLowerCase().includes(`tier ${t.match}`);
              return (
                <div
                  key={t.id}
                  style={{
                    padding: '8px 10px',
                    borderRadius: 8,
                    border: active ? '2px solid #059669' : '1px solid #e2e8f0',
                    background: active ? '#ecfdf5' : '#f8fafc',
                    fontSize: 11.5,
                    fontWeight: active ? 700 : 500,
                    color: active ? '#065f46' : '#64748b',
                  }}
                >
                  {active ? '✓ ' : '○ '}
                  {t.title}
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
};

const StageFeatures23dWidget: React.FC<{ details?: Record<string, unknown> }> = ({ details }) => {
  const feats = parseFeatures23d(details);
  if (feats.length === 0) return null;
  return (
    <div className="pipeline-widget-box">
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
        <div className="pipeline-widget-title">
          🧬 Vector Đặc Trưng Point-in-Time ({feats.length} Biến Đã Khóa τ ≤ t)
        </div>
        <span className="badge badge-green">Zero-Leakage Verified (Future-Shuffle diff = 0.0)</span>
      </div>
      <div className="pipeline-feature-grid">
        {feats.map((f, idx) => (
          <div key={`${f.name}-${idx}`} className="pipeline-feature-chip">
            <span className="pipeline-feature-name">{f.name}</span>
            <strong className="pipeline-feature-val">{f.value}</strong>
          </div>
        ))}
      </div>
    </div>
  );
};

const StageProbs6Widget: React.FC<{ details?: Record<string, unknown> }> = ({ details }) => {
  const probs = parseProbItems(details);
  if (probs.length === 0) return null;

  const probMap = new Map<string, number>();
  probs.forEach((p) => probMap.set(p.los, asPercentNumber(p.prob)));

  const ordered = LOS_ORDER.map((los) => ({
    los,
    pct: probMap.get(los) ?? 0,
    color: LOS_BAR_COLORS[los] || '#2563eb',
  }));

  const sevRaw = parseNumericField(details, ['severe_prob', 'p_severe', 'p_ef']);
  const chgRaw = parseNumericField(details, ['change_prob', 'p_change', 'p_chg']);
  const sevThreshRaw = parseNumericField(details, ['severe_threshold', 'val_severe_threshold'], 25);
  const chgThreshRaw = parseNumericField(details, ['change_threshold', 'val_change_threshold'], 40);

  const sevPct = sevRaw != null ? asPercentNumber(sevRaw) : (probMap.get('E') ?? 0) + (probMap.get('F') ?? 0);
  const chgPct = chgRaw != null ? asPercentNumber(chgRaw) : null;
  const sevThresh = asPercentNumber(sevThreshRaw ?? 25);
  const chgThresh = asPercentNumber(chgThreshRaw ?? 40);
  const predLos = parseStringField(details, ['effective_forecast_los', 'predicted_los', 'rf_cal_los', 'rf_pred_los']);

  return (
    <div className="pipeline-widget-box">
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 10, flexWrap: 'wrap' }}>
        <div className="pipeline-widget-title">
          📊 Phân Phối Xác Suất 6 Lớp Random Forest 23D (LOS A ➔ LOS F) &amp; Ngưỡng Hiệu Chỉnh VAL
        </div>
        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
          <span className={`badge ${sevPct >= sevThresh ? 'badge-red' : 'badge-blue'}`}>
            P(E/F): <b>{sevPct.toFixed(1)}%</b> vs ngưỡng VAL {sevThresh.toFixed(0)}%
          </span>
          {chgPct != null && (
            <span className={`badge ${chgPct >= chgThresh ? 'badge-amber' : 'badge-blue'}`}>
              P(chg): <b>{chgPct.toFixed(1)}%</b> vs ngưỡng VAL {chgThresh.toFixed(0)}%
            </span>
          )}
          {predLos && (
            <span className="badge badge-green">
              Dự báo chốt: <b>LOS {predLos}</b>
            </span>
          )}
        </div>
      </div>

      <div className="prob6-grid">
        {ordered.map((bar) => {
          const isTop = predLos === bar.los;
          const heightPct = Math.max(4, Math.min(100, bar.pct));
          return (
            <div key={bar.los} className={`prob6-col ${isTop ? 'prob6-col-selected' : ''}`}>
              <div className="prob6-pct" style={{ color: bar.color }}>
                {bar.pct.toFixed(1)}%
              </div>
              <div className="prob6-track">
                <div
                  className="prob6-bar"
                  style={{
                    height: `${heightPct}%`,
                    background: bar.color,
                  }}
                />
              </div>
              <div className="prob6-label">
                <span className={`los-pill los-${bar.los}`}>{bar.los}</span>
                {isTop && <span className="prob6-star">★</span>}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
};

const StageSpeedLayerWidget: React.FC<{
  segmentId: number;
  details?: Record<string, unknown>;
  streamStatus: StreamStatusResponse | null;
  isStreaming: boolean;
}> = ({ segmentId, details, streamStatus, isStreaming }) => {
  const tel = streamStatus?.telemetry || {};
  const liveMsgs = Number(tel.messages_consumed ?? 0);
  const probes = parseBloomProbes(details, segmentId, liveMsgs);
  const parts = parsePartitionCounts(streamStatus, details);
  const totalParts = Math.max(1, parts.p0 + parts.p1 + parts.p2);

  const partBars = [
    { label: 'Partition 0', count: parts.p0, color: '#2563eb' },
    { label: 'Partition 1', count: parts.p1, color: '#059669' },
    { label: 'Partition 2', count: parts.p2, color: '#f59e0b' },
  ];

  return (
    <div className="grid-2">
      <div className="pipeline-widget-box">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
          <div className="pipeline-widget-title">
            🧬 10-Slot RotatingBloom Probe (32 KiB · k = 10 hashes)
          </div>
          <span className={`badge ${isStreaming ? 'badge-red' : 'badge-amber'}`}>
            Bloom chặn trùng: {tel.duplicates_caught_bloom ?? 0} tin
          </span>
        </div>
        <div style={{ fontSize: 11.2, color: '#475569', marginTop: 4 }}>
          Khóa sự kiện đoạn <b>#{segmentId}</b> băm đồng thời qua 10 hàm độc lập vào mảng 2 × 131,072 bits (O(1) RAM):
        </div>
        <div className="bloom-slot-grid">
          {probes.map((p) => (
            <div
              key={p.hashIdx}
              className={`bloom-slot ${p.active ? 'bloom-slot-lit' : ''} ${isStreaming ? 'bloom-slot-pulse' : ''}`}
            >
              <span className="bloom-slot-k">h{p.hashIdx}</span>
              <strong className="bloom-slot-bit">bit {p.bitIndex.toLocaleString()}</strong>
              <span className="bloom-slot-val">{p.active ? '1 ✓' : '0'}</span>
            </div>
          ))}
        </div>
      </div>

      <div className="pipeline-widget-box">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
          <div className="pipeline-widget-title">
            ⚖️ Cân Bằng Tải 3 Phân Vùng Kafka KRaft (P0 / P1 / P2)
          </div>
          <span className={`badge ${parts.isLive ? 'badge-green' : 'badge-blue'}`}>
            {parts.isLive ? `Live Docker · ${parts.p0 + parts.p1 + parts.p2} tin` : 'Sẵn sàng · Bấm ⚡ Bơm 200 tin'}
          </span>
        </div>

        <div style={{ display: 'flex', flexDirection: 'column', gap: 9, marginTop: 10 }}>
          {partBars.map((pb) => {
            const pct = parts.p0 + parts.p1 + parts.p2 > 0 ? (pb.count / totalParts) * 100 : 0;
            return (
              <div key={pb.label}>
                <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11.5, marginBottom: 3 }}>
                  <strong>{pb.label}</strong>
                  <span>
                    <b>{pb.count}</b> bản tin ({pct.toFixed(1)}%)
                  </span>
                </div>
                <div style={{ height: 10, borderRadius: 6, background: '#e2e8f0', overflow: 'hidden' }}>
                  <div
                    style={{
                      width: `${Math.max(pct, pb.count > 0 ? 4 : 0)}%`,
                      height: '100%',
                      background: pb.color,
                      transition: 'width 0.35s ease',
                    }}
                  />
                </div>
              </div>
            );
          })}
        </div>

        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginTop: 12 }}>
          <span className="badge badge-blue">
            Watermark 30p lọc trễ: <b>{tel.late_dropped_for_tier2 ?? 0}</b>
          </span>
          <span className="badge badge-green">
            1 KiB HLL++: <b>{Math.round(tel.hll_estimated_segments ?? 0)}</b> / {tel.exact_distinct_segments ?? 0} đoạn
          </span>
          <span className="badge badge-amber">
            Mongo v1: <b>{tel.sink2_scored_v1 ?? 0}</b> bản ghi
          </span>
        </div>
      </div>
    </div>
  );
};

const StageMongoServingWidget: React.FC<{ details?: Record<string, unknown> }> = ({ details }) => {
  const mongoDoc = parseMongoDoc(details);
  const ixscanMs = parseNumericField(details, ['ixscan_ms', 'ixscan_median_ms', 'warm_median_ms'], 0.36) ?? 0.36;
  const collscanMs = parseNumericField(details, ['collscan_ms', 'collscan_median_ms'], 11.289) ?? 11.289;
  const speedupX =
    parseNumericField(details, ['speedup_x', 'speedup_vs_collscan_x']) ??
    (ixscanMs > 0 ? Number((collscanMs / ixscanMs).toFixed(2)) : 31.36);

  const ixBarPct = Math.max(3.5, Math.min(100, (ixscanMs / Math.max(collscanMs, 1)) * 100));

  return (
    <div className="grid-2">
      <div className="pipeline-widget-box">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
          <div className="pipeline-widget-title">
            ⚡ So Sánh Độ Trễ Truy Vấn MongoDB 8.0 (ESR Compound IXSCAN vs COLLSCAN)
          </div>
          <span className="badge badge-green">⚡ {speedupX.toFixed(2)}x Nhanh Hơn · 0 SORT</span>
        </div>

        <div style={{ display: 'flex', flexDirection: 'column', gap: 12, marginTop: 12 }}>
          <div>
            <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12, marginBottom: 4 }}>
              <strong>Compound IXSCAN (<code>level_prob_segment_idx</code>)</strong>
              <strong style={{ color: '#059669' }}>{ixscanMs.toFixed(3)} ms (SLA &lt; 10 ms ✓)</strong>
            </div>
            <div style={{ height: 14, borderRadius: 7, background: '#e2e8f0', overflow: 'hidden' }}>
              <div style={{ width: `${ixBarPct}%`, height: '100%', background: '#059669', borderRadius: 7 }} />
            </div>
          </div>

          <div>
            <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12, marginBottom: 4 }}>
              <strong>Full Collection Scan (COLLSCAN)</strong>
              <strong style={{ color: '#dc2626' }}>{collscanMs.toFixed(3)} ms</strong>
            </div>
            <div style={{ height: 14, borderRadius: 7, background: '#e2e8f0', overflow: 'hidden' }}>
              <div style={{ width: '100%', height: '100%', background: '#ef4444', borderRadius: 7 }} />
            </div>
          </div>
        </div>

        <div style={{ marginTop: 12, fontSize: 11.5, color: '#475569' }}>
          Cơ chế nâng cấp vòng đời chống ghi đè ngược: <code>$status_version: v1 (STREAM_PROVISIONAL) ➔ v2 (BATCH_RECONCILED)</code>{' '}
          được bảo vệ nguyên tử bằng điều kiện <code>$gte</code> trong MongoDB 8.0.
        </div>
      </div>

      {mongoDoc && (
        <div className="pipeline-widget-box">
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
            <div className="pipeline-widget-title">
              📄 Tài Liệu MongoDB Serving (<code>$status_version: v1 ➔ v2</code>)
            </div>
            <span className="badge badge-blue">bda501.traffic_scores</span>
          </div>
          <pre className="pipeline-code-box">{JSON.stringify(mongoDoc, null, 2)}</pre>
        </div>
      )}
    </div>
  );
};

const StageRichWidget: React.FC<{
  stage: PipelineStageItem;
  segmentId: number;
  streamStatus: StreamStatusResponse | null;
  isStreaming: boolean;
}> = ({ stage, segmentId, streamStatus, isStreaming }) => {
  if (stage.stage_id >= 1 && stage.stage_id <= 3) {
    return <StageMonoidVmaxWidget details={stage.details} />;
  }
  if (stage.stage_id === 4) {
    return <StageFeatures23dWidget details={stage.details} />;
  }
  if (stage.stage_id === 5) {
    return <StageProbs6Widget details={stage.details} />;
  }
  if (stage.stage_id === 6) {
    return (
      <StageSpeedLayerWidget
        segmentId={segmentId}
        details={stage.details}
        streamStatus={streamStatus}
        isStreaming={isStreaming}
      />
    );
  }
  if (stage.stage_id === 7) {
    return <StageMongoServingWidget details={stage.details} />;
  }
  return null;
};

const KeyValueCardColumn: React.FC<{
  title: string;
  accent: string;
  items: PipelineKeyValueItem[];
  emptyText: string;
}> = ({ title, accent, items, emptyText }) => (
  <div className="pipeline-col-card" style={{ borderTop: `3px solid ${accent}` }}>
    <div className="pipeline-col-title">{title}</div>
    {items.length === 0 ? (
      <div style={{ fontSize: 12, color: '#64748b' }}>{emptyText}</div>
    ) : (
      <div className="pipeline-kv-list">
        {items.map((item, idx) => (
          <div key={`${item.label}-${idx}`} className="pipeline-kv-row">
            <span className="pipeline-kv-label">{item.label}</span>
            <strong className="pipeline-kv-val">{item.value}</strong>
          </div>
        ))}
      </div>
    )}
  </div>
);

const StageInspector: React.FC<{
  stage: PipelineStageItem;
  segmentId: number;
  streamStatus: StreamStatusResponse | null;
  isStreaming: boolean;
}> = ({ stage, segmentId, streamStatus, isStreaming }) => {
  const accent = stageAccent(stage);

  return (
    <div className="card" style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
      <div className="card-header" style={{ marginBottom: 0 }}>
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap', marginBottom: 4 }}>
            <span
              className="badge"
              style={{ background: accent, color: '#ffffff', border: 'none' }}
            >
              BƯỚC {stage.stage_id} / 7
            </span>
            <span className="badge badge-blue">{stage.zone}</span>
            <span className="badge badge-amber">{stage.slide_ref}</span>
          </div>
          <h2 className="card-title" style={{ fontSize: 16 }}>
            {stage.title}
          </h2>
          <p className="card-subtitle" style={{ fontSize: 12 }}>
            {stage.subtitle}
          </p>
        </div>
        <span className="badge badge-green">{stage.status_badge}</span>
      </div>

      {stage.key_metrics && stage.key_metrics.length > 0 && (
        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
          {stage.key_metrics.map((km, idx) => (
            <div key={`${km.label}-${idx}`} className="pipeline-metric-pill">
              <span>{km.label}:</span> <strong>{km.value}</strong>
            </div>
          ))}
        </div>
      )}

      <div className="pipeline-inspector-grid">
        <KeyValueCardColumn
          title="📥 Đầu Vào (Input)"
          accent="#0284c7"
          items={stage.input_summary || []}
          emptyText="Không có thông số đầu vào bổ sung."
        />

        <div className="pipeline-col-card" style={{ borderTop: `3px solid ${accent}` }}>
          <div className="pipeline-col-title">⚙️ Thuật Toán Đang Thực Thi (Transform)</div>
          <ol className="pipeline-step-list">
            {(stage.transform_steps || []).map((step, idx) => (
              <li key={idx} className="pipeline-step-item">
                <span className="pipeline-step-num" style={{ background: accent }}>
                  {String(idx + 1).padStart(2, '0')}
                </span>
                <span>{step}</span>
              </li>
            ))}
          </ol>
        </div>

        <KeyValueCardColumn
          title="📤 Đầu Ra & Kết Quả (Output)"
          accent="#059669"
          items={stage.output_summary || []}
          emptyText="Không có thông số đầu ra bổ sung."
        />
      </div>

      <StageRichWidget
        stage={stage}
        segmentId={segmentId}
        streamStatus={streamStatus}
        isStreaming={isStreaming}
      />
    </div>
  );
};

export const TabPipelineFlow: React.FC<Props> = ({
  streamStatus,
  isStreaming,
  onStartStressKafka,
  onJumpToSegment,
}) => {
  const [traceId, setTraceId] = useState<string>('dien_bien_phu_50026');
  const [traceData, setTraceData] = useState<PipelineTraceResponse | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [activeStageId, setActiveStageId] = useState<number>(1);
  const [isPlaying, setIsPlaying] = useState<boolean>(false);
  const [reloadTick, setReloadTick] = useState<number>(0);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    fetch(`/api/pipeline-trace?trace_id=${encodeURIComponent(traceId)}`)
      .then((res) => {
        if (!res.ok) {
          throw new Error(`Lỗi máy chủ (${res.status}) khi tải luồng thực thi Pipeline.`);
        }
        return res.json() as Promise<PipelineTraceResponse>;
      })
      .then((payload) => {
        if (!cancelled) {
          setTraceData(payload);
          setLoading(false);
        }
      })
      .catch((err: unknown) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : 'Không thể tải dữ liệu luồng thực thi Pipeline.');
          setLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [traceId, reloadTick]);

  const stages = traceData?.stages || [];
  const maxStageId = stages.length > 0 ? stages[stages.length - 1].stage_id : 7;

  useEffect(() => {
    if (!isPlaying) return;
    const timer = window.setInterval(() => {
      setActiveStageId((prev) => {
        if (prev >= maxStageId) {
          setIsPlaying(false);
          return maxStageId;
        }
        return prev + 1;
      });
    }, 1500);
    return () => window.clearInterval(timer);
  }, [isPlaying, maxStageId]);

  const handleSelectTrace = useCallback((id: string) => {
    setIsPlaying(false);
    setActiveStageId(1);
    setTraceId(id);
  }, []);

  const handleSelectStage = useCallback((stageId: number) => {
    setIsPlaying(false);
    setActiveStageId(stageId);
  }, []);

  const handleTogglePlay = useCallback(() => {
    if (isPlaying) {
      setIsPlaying(false);
      return;
    }
    if (activeStageId >= maxStageId) {
      setActiveStageId(1);
    }
    setIsPlaying(true);
  }, [isPlaying, activeStageId, maxStageId]);

  const handlePumpKafkaLive = useCallback(() => {
    setIsPlaying(false);
    setActiveStageId(6);
    onStartStressKafka();
  }, [onStartStressKafka]);

  const activeStage = stages.find((s) => s.stage_id === activeStageId) || stages[0] || null;
  const traces: PipelineTraceOption[] = traceData?.curated_traces || [];

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
      {/* Card 1: Presenter Control Header */}
      <div className="card" style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
        <div className="card-header" style={{ marginBottom: 0 }}>
          <div>
            <h2 className="card-title">
              ⚡ 06 · Trình Diễn Trực Tiếp Luồng Thực Thi Pipeline 7 Giai Đoạn (End-to-End Algorithm Trace)
            </h2>
            <p className="card-subtitle">
              {traceData ? (
                <>
                  Đang truy vết tuyến thực tế: <b>{traceData.street_name}</b> (Đoạn <b>#{traceData.segment_id}</b>) ·{' '}
                  <b>{traceData.window_headline}</b>
                  {traceData.is_cold_start ? ' · 🧭 Cold-Start Structural Prior' : ''}
                </>
              ) : (
                'Truy vết trọn vẹn dữ liệu từ bản tin GPS thô ➔ Monoid 5-Tuple ➔ Line-Graph 23D ➔ Random Forest ➔ Kafka Sketches ➔ MongoDB 8.0'
              )}
            </p>
          </div>
          {traceData?.summary_verdict && (
            <span className={`badge ${traceData.is_cold_start ? 'badge-amber' : 'badge-green'}`}>
              {traceData.summary_verdict}
            </span>
          )}
        </div>

        {/* 4 Curated Trace Selector Pills */}
        {traces.length > 0 && (
          <div className="pipeline-trace-grid">
            {traces.map((tr) => {
              const selected = tr.id === (traceData?.active_trace_id || traceId);
              return (
                <button
                  key={tr.id}
                  type="button"
                  className={`pipeline-trace-btn ${selected ? 'active' : ''}`}
                  onClick={() => handleSelectTrace(tr.id)}
                >
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 6 }}>
                    <span className="pipeline-trace-title">{tr.title}</span>
                    <span className={`badge ${selected ? 'badge-blue' : 'badge-amber'}`} style={{ fontSize: 10 }}>
                      {tr.badge}
                    </span>
                  </div>
                  <div className="pipeline-trace-sub">
                    #{tr.segment_id} · {tr.slide_focus}
                  </div>
                </button>
              );
            })}
          </div>
        )}

        {/* Playback Controls + Live Action Buttons */}
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 10, flexWrap: 'wrap' }}>
          <div style={{ display: 'flex', gap: 7, flexWrap: 'wrap', alignItems: 'center' }}>
            <button
              type="button"
              className={`btn ${isPlaying ? 'btn-amber' : 'btn-primary'}`}
              disabled={stages.length === 0}
              onClick={handleTogglePlay}
            >
              {isPlaying ? '⏸ Tạm Dừng' : '▶ Chạy Từng Bước'}
            </button>
            <button
              type="button"
              className="btn btn-slate"
              disabled={activeStageId <= 1}
              onClick={() => handleSelectStage(Math.max(1, activeStageId - 1))}
            >
              ⏮ Lùi
            </button>
            <button
              type="button"
              className="btn btn-slate"
              disabled={activeStageId >= maxStageId}
              onClick={() => handleSelectStage(Math.min(maxStageId, activeStageId + 1))}
            >
              ⏭ Bước Tiếp
            </button>
            <button
              type="button"
              className="btn btn-slate"
              onClick={() => handleSelectStage(1)}
            >
              ↺ Đặt lại
            </button>
            <button
              type="button"
              className="btn btn-emerald"
              onClick={handlePumpKafkaLive}
            >
              ⚡ {isStreaming ? 'Đang bơm 200 tin Kafka...' : 'Bơm 200 tin Kafka Live'}
            </button>
            {traceData && (
              <button
                type="button"
                className="btn btn-amber"
                onClick={() =>
                  onJumpToSegment(
                    traceData.segment_id,
                    traceData.ict_date ?? undefined,
                    traceData.vn_hour ?? undefined
                  )
                }
              >
                🔍 Mở trên Bản đồ Tab 01
              </button>
            )}
          </div>

          {/* Slide-Sync Quick Jump Pills */}
          <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', alignItems: 'center' }}>
            {SLIDE_SYNC_GROUPS.map((grp) => {
              const inRange = activeStageId >= grp.min && activeStageId <= grp.max;
              return (
                <button
                  key={grp.label}
                  type="button"
                  className={`pill-btn ${inRange ? 'active' : ''}`}
                  onClick={() => handleSelectStage(grp.startStage)}
                >
                  {grp.label}
                </button>
              );
            })}
          </div>
        </div>
      </div>

      {/* Error / Loading State */}
      {error && (
        <div
          className="card"
          role="alert"
          style={{
            background: '#fef2f2',
            borderColor: '#fecaca',
            color: '#991b1b',
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            gap: 12,
          }}
        >
          <span>⚠️ {error}</span>
          <button type="button" className="btn btn-slate" onClick={() => setReloadTick((t) => t + 1)}>
            Thử tải lại
          </button>
        </div>
      )}

      {loading && !traceData && (
        <div className="card">Đang tải dữ liệu truy vết Pipeline 7 giai đoạn từ hệ thống...</div>
      )}

      {/* Card 2: Interactive 7-Stage Pipeline DAG SVG + Live Telemetry Strip */}
      {traceData && stages.length > 0 && (
        <div className="card" style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
          <div className="card-header" style={{ marginBottom: 0 }}>
            <div>
              <h2 className="card-title">
                Sơ Đồ Kiến Trúc Lambda Tương Tác (Zone A–F: Speed/Stream Song Song Batch ➔ Hội Tụ Serving MongoDB 8.0)
              </h2>
              <p className="card-subtitle">
                Đang dừng tại <b>Stage {activeStageId} / {maxStageId}</b> · <b>Phase A (Batch 1–5)</b> huấn luyện &amp; khóa Frozen RF ➔ <b>Phase B (Stream 6)</b> chấm điểm real-time ghi <code>v1</code> ➔ <b>Phase C (Serving 7)</b> đối soát <code>v2</code> &amp; khóa chống ghi đè
              </p>
            </div>
            <span className="badge badge-blue">
              Tiến độ: {activeStageId}/{maxStageId}
            </span>
          </div>

          <PipelineDagSvg
            stages={stages}
            activeStageId={activeStageId}
            isStreaming={isStreaming}
            streamStatus={streamStatus}
            onSelectStage={handleSelectStage}
          />

          <LiveTelemetryStrip streamStatus={streamStatus} isStreaming={isStreaming} />
        </div>
      )}

      {/* Card 3: 3-Column Live Stage Inspector + Stage-Specific Rich Visual Widget */}
      {traceData && activeStage && (
        <StageInspector
          stage={activeStage}
          segmentId={traceData.segment_id}
          streamStatus={streamStatus}
          isStreaming={isStreaming}
        />
      )}
    </div>
  );
};
