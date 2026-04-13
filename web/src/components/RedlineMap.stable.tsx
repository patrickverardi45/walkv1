"use client";

import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE_URL?.replace(/\/+$/, "") ||
  "http://127.0.0.1:8000";

type CandidateRanking = {
  route_id?: string;
  route_name?: string;
  source_folder?: string;
  route_role?: string;
  route_length_ft?: number;
  expected_span_ft?: number;
  length_gap_ft?: number;
  score?: number;
  reason?: string;
};

type VerificationInfo = {
  confidence?: string;
  reason?: string;
  mapping_mode?: string;
  anchor_type?: string;
  print_present?: boolean;
  route_name?: string;
  route_length_ft?: number;
  source_file?: string;
  print?: string;
  candidate_rankings?: CandidateRanking[];
};

type StationPoint = {
  station?: string;
  station_ft?: number;
  mapped_station_ft?: number;
  lat?: number;
  lon?: number;
  depth_ft?: number | null;
  boc_ft?: number | null;
  notes?: string;
  date?: string;
  crew?: string;
  print?: string;
  source_file?: string;
  point_role?: string;
  verification?: VerificationInfo;
};

type RedlineSegment = {
  segment_id?: string;
  start_station?: string;
  end_station?: string;
  length_ft?: number;
  print?: string;
  source_file?: string;
  route_name?: string;
  coords?: number[][];
};

type GroupMatch = {
  route_name?: string;
  route_role?: string;
  confidence_label?: string;
  final_decision?: string;
  expected_span_ft?: number;
  length_gap_ft?: number;
  print?: string;
  candidate_rankings?: CandidateRanking[];
  print_filter?: {
    print_tokens?: string[];
  };
};

type KmzLineFeature = {
  feature_id?: string;
  route_id?: string;
  route_name?: string;
  source_folder?: string;
  role?: string;
  coords?: number[][];
  color?: string;
  width?: number;
  stroke?: string;
  stroke_width?: number;
};

type KmzPolygonFeature = {
  feature_id?: string;
  name?: string;
  coords?: number[][];
  fill_color?: string;
  stroke_color?: string;
  fill?: string;
  stroke?: string;
  fill_opacity?: number;
  stroke_width?: number;
};

type BackendState = {
  success?: boolean;
  message?: string;
  warning?: string;
  error?: string;
  route_name?: string | null;
  selected_route_name?: string | null;
  selected_route_match?: GroupMatch | null;
  route_coords?: number[][];
  loaded_field_data_files?: number;
  latest_structured_file?: string | null;
  redline_segments?: RedlineSegment[];
  station_points?: StationPoint[];
  active_route_redline_segments?: RedlineSegment[];
  active_route_station_points?: StationPoint[];
  verification_summary?: {
    status?: string;
    route_selection_reason?: string;
  };
  total_length_ft?: number;
  covered_length_ft?: number;
  completion_pct?: number;
  active_route_covered_length_ft?: number;
  active_route_completion_pct?: number;
  active_route_station_points_count?: number;
  active_route_redline_segments_count?: number;
  committed_rows?: Array<Record<string, unknown>>;
  bug_report_count?: number;
  suggested_route_id?: string | null;
  station_mapping_mode?: string | null;
  kmz_reference?: {
    line_features?: KmzLineFeature[];
    polygon_features?: KmzPolygonFeature[];
  };
};


type StationPhoto = {
  photo_id: string;
  station_identity: string;
  station_summary: string;
  original_filename: string;
  stored_filename: string;
  content_type?: string;
  uploaded_at: string;
  relative_url: string;
};

type ExceptionCost = {
  id: string;
  label: string;
  amount: string;
};

type NoteTone = "neutral" | "success" | "warning" | "error";

type Bounds = {
  minLat: number;
  maxLat: number;
  minLon: number;
  maxLon: number;
};

type ScreenPoint = { x: number; y: number };

type Viewport = {
  zoom: number;
  panX: number;
  panY: number;
};

const WORLD_WIDTH = 1000;
const WORLD_HEIGHT = 1000;
const MAP_HEIGHT = 620;
const MIN_ZOOM = 1;
const MAX_ZOOM = 300;
const FIT_PADDING = 72;
const WHEEL_IN = 1.18;
const WHEEL_OUT = 0.85;
const BUTTON_IN = 1.22;
const BUTTON_OUT = 1 / BUTTON_IN;
const LOW_ZOOM_LABEL_THRESHOLD = 6;
const MID_ZOOM_LABEL_THRESHOLD = 16;
const STATION_HIT_RADIUS_PX = 14;

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function formatNumber(value: number | null | undefined, digits = 2): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "--";
  return value.toFixed(digits);
}

function cleanDisplayText(value: unknown): string {
  const raw = String(value ?? "").trim();
  if (!raw) return "--";
  const lower = raw.toLowerCase();
  if (lower === "nan" || lower === "null" || lower === "undefined") return "--";
  return raw;
}

function formatDisplayDate(value: string | null | undefined): string {
  const raw = String(value || "").trim();
  if (!raw) return "--";
  const match = raw.match(/^(\d{4}-\d{2}-\d{2})/);
  if (match) return match[1];
  const parsed = new Date(raw);
  if (!Number.isNaN(parsed.getTime())) {
    const year = parsed.getFullYear();
    const month = String(parsed.getMonth() + 1).padStart(2, "0");
    const day = String(parsed.getDate()).padStart(2, "0");
    return `${year}-${month}-${day}`;
  }
  return raw.replace(/\s+\d{2}:\d{2}:\d{2}(?:\.\d+)?$/, "");
}


function stationIdentityPart(value: unknown, digits?: number): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "number") {
    if (!Number.isFinite(value)) return "";
    return digits !== undefined ? value.toFixed(digits) : String(value);
  }
  const raw = String(value).trim();
  return raw;
}

function buildStationIdentity(routeName: string | null | undefined, point: StationPoint | null | undefined): string {
  if (!point) return "";
  return [
    stationIdentityPart(routeName),
    stationIdentityPart(point.source_file),
    stationIdentityPart(point.station),
    stationIdentityPart(point.mapped_station_ft, 3),
    stationIdentityPart(point.lat, 8),
    stationIdentityPart(point.lon, 8),
  ].join("|");
}

function buildStationSummary(routeName: string | null | undefined, point: StationPoint | null | undefined): string {
  if (!point) return "--";
  const station = cleanDisplayText(point.station);
  const source = cleanDisplayText(point.source_file);
  const route = cleanDisplayText(routeName);
  return `${station} • ${route} • ${source}`;
}

function cleanCoords(coords: number[][] | undefined | null): number[][] {
  if (!Array.isArray(coords)) return [];
  return coords.filter(
    (pt): pt is number[] =>
      Array.isArray(pt) &&
      pt.length >= 2 &&
      typeof pt[0] === "number" &&
      typeof pt[1] === "number" &&
      Number.isFinite(pt[0]) &&
      Number.isFinite(pt[1])
  );
}

function getBoundsFromCoords(coords: number[][]): Bounds | null {
  if (!coords.length) return null;
  return {
    minLat: Math.min(...coords.map((p) => p[0])),
    maxLat: Math.max(...coords.map((p) => p[0])),
    minLon: Math.min(...coords.map((p) => p[1])),
    maxLon: Math.max(...coords.map((p) => p[1])),
  };
}

function expandBounds(bounds: Bounds, factor = 0.04): Bounds {
  const latPad = Math.max((bounds.maxLat - bounds.minLat) * factor, 0.00001);
  const lonPad = Math.max((bounds.maxLon - bounds.minLon) * factor, 0.00001);
  return {
    minLat: bounds.minLat - latPad,
    maxLat: bounds.maxLat + latPad,
    minLon: bounds.minLon - lonPad,
    maxLon: bounds.maxLon + lonPad,
  };
}

function projectWorldPoint(lat: number, lon: number, bounds: Bounds): ScreenPoint {
  const latSpan = Math.max(bounds.maxLat - bounds.minLat, 0.000001);
  const lonSpan = Math.max(bounds.maxLon - bounds.minLon, 0.000001);
  return {
    x: ((lon - bounds.minLon) / lonSpan) * WORLD_WIDTH,
    y: WORLD_HEIGHT - ((lat - bounds.minLat) / latSpan) * WORLD_HEIGHT,
  };
}

function buildWorldPath(coords: number[][], bounds: Bounds | null): string {
  if (!bounds || coords.length < 2) return "";
  return coords
    .map((pt, idx) => {
      const p = projectWorldPoint(pt[0], pt[1], bounds);
      return `${idx === 0 ? "M" : "L"} ${p.x.toFixed(2)} ${p.y.toFixed(2)}`;
    })
    .join(" ");
}

function viewBoxToString(widthPx: number, heightPx: number, viewport: Viewport): string {
  const worldWidth = widthPx / viewport.zoom;
  const worldHeight = heightPx / viewport.zoom;
  const x = -viewport.panX / viewport.zoom;
  const y = -viewport.panY / viewport.zoom;
  return `${x} ${y} ${worldWidth} ${worldHeight}`;
}

function screenToWorld(
  screenX: number,
  screenY: number,
  viewport: Viewport
): ScreenPoint {
  return {
    x: (screenX - viewport.panX) / viewport.zoom,
    y: (screenY - viewport.panY) / viewport.zoom,
  };
}

function worldToScreen(
  worldX: number,
  worldY: number,
  viewport: Viewport
): ScreenPoint {
  return {
    x: worldX * viewport.zoom + viewport.panX,
    y: worldY * viewport.zoom + viewport.panY,
  };
}

function distance(a: ScreenPoint, b: ScreenPoint): number {
  return Math.hypot(a.x - b.x, a.y - b.y);
}

function kmzLineStroke(feature: KmzLineFeature): string {
  return (
    feature.stroke ||
    feature.color ||
    (feature.role === "backbone"
      ? "rgba(186, 168, 96, 0.16)"
      : feature.role === "terminal_tail"
      ? "rgba(166, 178, 190, 0.14)"
      : "rgba(176, 188, 198, 0.12)")
  );
}

function kmzLineWidth(feature: KmzLineFeature): number {
  const raw = feature.stroke_width ?? feature.width;
  if (typeof raw === "number" && Number.isFinite(raw)) {
    return clamp(raw * 0.66, 0.85, 2.4);
  }
  return feature.role === "backbone" ? 1.5 : 1.05;
}

function kmzPolygonFill(feature: KmzPolygonFeature): string {
  return feature.fill_color || feature.fill || "rgba(95, 128, 110, 0.05)";
}

function kmzPolygonStroke(feature: KmzPolygonFeature): string {
  return feature.stroke_color || feature.stroke || "rgba(164, 174, 181, 0.22)";
}

function kmzPolygonOpacity(feature: KmzPolygonFeature): number {
  const raw = feature.fill_opacity;
  if (typeof raw === "number" && Number.isFinite(raw)) {
    return clamp(raw * 0.38, 0.015, 0.12);
  }
  return 0.038;
}

function SummaryCard({ title, value, subtitle }: { title: string; value: string; subtitle: string }) {
  return (
    <div
      style={{
        background: "linear-gradient(180deg, #ffffff 0%, #f8fbff 100%)",
        border: "1px solid #dbe4ee",
        borderRadius: 20,
        padding: 18,
        boxShadow: "0 10px 24px rgba(15, 23, 42, 0.04)",
      }}
    >
      <div style={{ fontSize: 12, color: "#5b6b7d", fontWeight: 700, textTransform: "uppercase", letterSpacing: 0.4 }}>{title}</div>
      <div style={{ marginTop: 10, fontSize: 28, fontWeight: 800, color: "#0f172a", lineHeight: 1.1 }}>{value}</div>
      <div style={{ marginTop: 8, fontSize: 12, color: "#6b7280" }}>{subtitle}</div>
    </div>
  );
}

function Section({ title, subtitle, children, actions }: { title: string; subtitle?: string; children: React.ReactNode; actions?: React.ReactNode }) {
  return (
    <div
      style={{
        background: "#fff",
        border: "1px solid #dbe4ee",
        borderRadius: 20,
        overflow: "hidden",
        boxShadow: "0 10px 24px rgba(15, 23, 42, 0.04)",
      }}
    >
      <div style={{ padding: 18, borderBottom: "1px solid #e8eef5", display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: 12, flexWrap: "wrap" }}>
        <div>
          <div style={{ fontSize: 18, fontWeight: 800, color: "#0f172a" }}>{title}</div>
          {subtitle ? <div style={{ marginTop: 6, fontSize: 13, color: "#64748b", maxWidth: 900 }}>{subtitle}</div> : null}
        </div>
        {actions ? <div>{actions}</div> : null}
      </div>
      <div style={{ padding: 18 }}>{children}</div>
    </div>
  );
}

function StatusBanner({ tone, text }: { tone: NoteTone; text: string }) {
  const styles: Record<NoteTone, { bg: string; border: string; color: string }> = {
    neutral: { bg: "#eef2f7", border: "#dbe4ee", color: "#334155" },
    success: { bg: "#ecfdf3", border: "#b7ebc8", color: "#166534" },
    warning: { bg: "#fffbeb", border: "#fcd34d", color: "#92400e" },
    error: { bg: "#fef2f2", border: "#fecaca", color: "#991b1b" },
  };
  const s = styles[tone];
  return (
    <div style={{ border: `1px solid ${s.border}`, background: s.bg, color: s.color, borderRadius: 16, padding: 14, fontSize: 14, whiteSpace: "pre-wrap", boxShadow: "0 6px 18px rgba(15, 23, 42, 0.03)" }}>
      {text}
    </div>
  );
}

function SmallRow({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div style={{ display: "grid", gridTemplateColumns: "160px 1fr", gap: 10, fontSize: 13, padding: "6px 0" }}>
      <div style={{ color: "#64748b", fontWeight: 700 }}>{label}</div>
      <div style={{ color: "#0f172a", wordBreak: "break-word" }}>{value}</div>
    </div>
  );
}

function TooltipRow({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "92px 1fr",
        gap: 10,
        alignItems: "start",
        fontSize: 12,
        lineHeight: 1.35,
      }}
    >
      <div style={{ color: "#64748b", fontWeight: 800, letterSpacing: 0.15 }}>{label}</div>
      <div style={{ color: "#0f172a", wordBreak: "break-word" }}>{value}</div>
    </div>
  );
}

function buttonStyle(background: string, color: string, borderColor: string, disabled: boolean): React.CSSProperties {
  return {
    background,
    color,
    border: "2px solid #000000",
    borderRadius: 14,
    padding: "12px 14px",
    fontWeight: 800,
    cursor: disabled ? "not-allowed" : "pointer",
    opacity: disabled ? 0.65 : 1,
    fontSize: 14,
  };
}

const miniMapButton: React.CSSProperties = {
  height: 36,
  borderRadius: 10,
  padding: "0 12px",
  border: "2px solid #000000",
  background: "rgba(15, 23, 42, 0.92)",
  color: "#f8fafc",
  fontWeight: 800,
  cursor: "pointer",
  boxShadow: "0 6px 16px rgba(0,0,0,0.28)",
};

function uploadCardStyle(disabled: boolean): React.CSSProperties {
  return {
    display: "block",
    border: "2px solid #000000",
    borderRadius: 16,
    padding: 16,
    background: disabled ? "#f3f4f6" : "#ffffff",
    cursor: disabled ? "not-allowed" : "pointer",
    opacity: disabled ? 0.7 : 1,
  };
}

function ShellCard({ title, description, children }: { title: string; description: string; children?: React.ReactNode }) {
  return (
    <div style={{ border: "1px solid #dbe4ee", borderRadius: 16, background: "#fbfdff", padding: 16 }}>
      <div style={{ fontSize: 15, fontWeight: 800, color: "#0f172a" }}>{title}</div>
      <div style={{ marginTop: 6, fontSize: 13, color: "#64748b", lineHeight: 1.55 }}>{description}</div>
      {children ? <div style={{ marginTop: 12 }}>{children}</div> : null}
    </div>
  );
}

function Pill({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div style={{ border: "1px solid #dbe4ee", background: "#f8fbfe", borderRadius: 999, padding: "8px 12px", fontSize: 12, color: "#334155" }}>
      <strong>{label}:</strong> {value}
    </div>
  );
}

function toMoney(value: number): string {
  return `$${value.toFixed(2)}`;
}

export default function RedlineMap() {
  const [state, setState] = useState<BackendState | null>(null);
  const [busy, setBusy] = useState(false);
  const [statusTone, setStatusTone] = useState<NoteTone>("neutral");
  const [statusText, setStatusText] = useState("Connecting to local beta backend...");
  const [jobLabel, setJobLabel] = useState("");
  const [notes, setNotes] = useState("");
  const [costPerFoot, setCostPerFoot] = useState("5.00");
  const [manualFootage, setManualFootage] = useState("");
  const [exceptions, setExceptions] = useState<ExceptionCost[]>([
    { id: "txdot", label: "TXDOT", amount: "" },
    { id: "railroad", label: "Railroad", amount: "" },
    { id: "restoration", label: "Restoration", amount: "" },
  ]);
  const [extraExceptionLabel, setExtraExceptionLabel] = useState("");
  const [extraExceptionAmount, setExtraExceptionAmount] = useState("");
  const [stationPhotos, setStationPhotos] = useState<StationPhoto[]>([]);
  const [stationPhotosLoading, setStationPhotosLoading] = useState(false);
  const [stationPhotoBusy, setStationPhotoBusy] = useState(false);
  const [viewport, setViewport] = useState<Viewport>({ zoom: 1, panX: 0, panY: 0 });
  const [didInitialFit, setDidInitialFit] = useState(false);
  const [isPanning, setIsPanning] = useState(false);
  const panStartRef = useRef<{ x: number; y: number; panX: number; panY: number } | null>(null);
  const mapContainerRef = useRef<HTMLDivElement | null>(null);
  const [containerSize, setContainerSize] = useState({ width: 1200, height: MAP_HEIGHT });
  const [boxZoom, setBoxZoom] = useState<{ startX: number; startY: number; endX: number; endY: number } | null>(null);
  const [selectedStationIndex, setSelectedStationIndex] = useState<number | null>(null);
  const [hoverStationIndex, setHoverStationIndex] = useState<number | null>(null);
  const [showStations, setShowStations] = useState(false);
  const userHasAdjustedViewportRef = useRef(false);
  const lastAutoFitSignatureRef = useRef<string>("");
  const initialFitRafRef = useRef<number | null>(null);
  const initialFitTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const routeCoords = useMemo(() => cleanCoords(state?.route_coords || []), [state]);
  const redlineSegments = state?.redline_segments || [];
  const stationPoints = state?.station_points || [];
  const activeRouteRedlineSegments = state?.active_route_redline_segments || [];
  const activeRouteStationPoints = state?.active_route_station_points || [];
  const selectedMatch = state?.selected_route_match || null;
  const verification = state?.verification_summary || {};
  const activeJob = jobLabel.trim() || state?.selected_route_name || state?.route_name || "No active job";

  const kmzLineFeatures = useMemo(
    () =>
      (state?.kmz_reference?.line_features || [])
        .map((f) => ({ ...f, coords: cleanCoords(f.coords) }))
        .filter((f) => f.coords.length > 1),
    [state]
  );

  const kmzPolygonFeatures = useMemo(
    () =>
      (state?.kmz_reference?.polygon_features || [])
        .map((f) => ({ ...f, coords: cleanCoords(f.coords) }))
        .filter((f) => f.coords.length > 2),
    [state]
  );

  const designCoords = useMemo(() => {
    const coords: number[][] = [];
    kmzLineFeatures.forEach((feature) => cleanCoords(feature.coords).forEach((pt) => coords.push(pt)));
    kmzPolygonFeatures.forEach((feature) => cleanCoords(feature.coords).forEach((pt) => coords.push(pt)));
    routeCoords.forEach((pt) => coords.push(pt));
    return coords;
  }, [kmzLineFeatures, kmzPolygonFeatures, routeCoords]);

  const allCoords = useMemo(() => {
    const coords: number[][] = [];
    designCoords.forEach((pt) => coords.push(pt));
    redlineSegments.forEach((segment) => cleanCoords(segment.coords).forEach((pt) => coords.push(pt)));
    stationPoints.forEach((point) => {
      if (typeof point.lat === "number" && typeof point.lon === "number") {
        coords.push([point.lat, point.lon]);
      }
    });
    return coords;
  }, [designCoords, redlineSegments, stationPoints]);

  const bounds = useMemo(() => {
    const raw = getBoundsFromCoords(allCoords);
    return raw ? expandBounds(raw, 0.12) : null;
  }, [allCoords]);

  const designBounds = useMemo(() => {
    const raw = getBoundsFromCoords(designCoords);
    return raw ? expandBounds(raw, 0.14) : null;
  }, [designCoords]);

  const stationOnlyBounds = useMemo(() => {
    const coords: number[][] = [];
    stationPoints.forEach((point) => {
      if (typeof point.lat === "number" && typeof point.lon === "number") {
        coords.push([point.lat, point.lon]);
      }
    });
    const raw = getBoundsFromCoords(coords);
    return raw ? expandBounds(raw, 0.12) : null;
  }, [stationPoints]);

  const initialFitBounds = useMemo(() => designBounds || bounds || stationOnlyBounds || null, [designBounds, bounds, stationOnlyBounds]);

  const autoFitSignature = useMemo(() => {
    if (!initialFitBounds) return "";
    return JSON.stringify({
      bounds: initialFitBounds,
      width: containerSize.width,
      height: containerSize.height,
      route: state?.selected_route_name || state?.route_name || "",
      designCoordCount: designCoords.length,
      routeCoordCount: routeCoords.length,
      redlineCount: redlineSegments.length,
      stationCount: stationPoints.length,
    });
  }, [
    initialFitBounds,
    containerSize.width,
    containerSize.height,
    state?.selected_route_name,
    state?.route_name,
    designCoords.length,
    routeCoords.length,
    redlineSegments.length,
    stationPoints.length,
  ]);

  const kmzLinePaths = useMemo(
    () =>
      kmzLineFeatures.map((feature) => ({
        id: feature.feature_id || feature.route_id || `${feature.route_name || "kmz"}-${Math.random()}`,
        path: buildWorldPath(feature.coords || [], bounds),
      })),
    [kmzLineFeatures, bounds]
  );

  const kmzPolygonPaths = useMemo(
    () =>
      kmzPolygonFeatures.map((feature) => ({
        id: feature.feature_id || `${feature.name || "polygon"}-${Math.random()}`,
        path: buildWorldPath([...(feature.coords || []), (feature.coords || [])[0]], bounds),
      })),
    [kmzPolygonFeatures, bounds]
  );

  const redlinePaths = useMemo(
    () =>
      redlineSegments.map((segment) => ({
        id: segment.segment_id || `${segment.start_station || "start"}-${segment.end_station || "end"}`,
        path: buildWorldPath(cleanCoords(segment.coords), bounds),
      })),
    [redlineSegments, bounds]
  );

  const projectedStations = useMemo(() => {
    if (!bounds) return [] as Array<{ idx: number; point: StationPoint; world: ScreenPoint }>;
    return stationPoints
      .map((point, idx) => {
        if (typeof point.lat !== "number" || typeof point.lon !== "number") return null;
        return {
          idx,
          point,
          world: projectWorldPoint(point.lat, point.lon, bounds),
        };
      })
      .filter((item): item is { idx: number; point: StationPoint; world: ScreenPoint } => Boolean(item));
  }, [stationPoints, bounds]);

  const visibleLabelIndices = useMemo(() => {
    const result = new Set<number>();
    if (!showStations || !projectedStations.length) return result;

    const thresholdPx =
      viewport.zoom < LOW_ZOOM_LABEL_THRESHOLD
        ? 999999
        : viewport.zoom < MID_ZOOM_LABEL_THRESHOLD
        ? 56
        : 28;

    const acceptedScreen: ScreenPoint[] = [];
    for (const station of projectedStations) {
      const screen = worldToScreen(station.world.x, station.world.y, viewport);
      const mustShow =
        selectedStationIndex === station.idx ||
        hoverStationIndex === station.idx ||
        station.idx === 0 ||
        station.idx === projectedStations.length - 1;

      if (mustShow) {
        result.add(station.idx);
        acceptedScreen.push(screen);
        continue;
      }

      if (viewport.zoom < LOW_ZOOM_LABEL_THRESHOLD) {
        continue;
      }

      const tooClose = acceptedScreen.some((existing) => distance(existing, screen) < thresholdPx);
      if (!tooClose) {
        result.add(station.idx);
        acceptedScreen.push(screen);
      }
    }

    return result;
  }, [showStations, projectedStations, viewport, selectedStationIndex, hoverStationIndex]);

  const topCandidateRankings =
    selectedMatch?.candidate_rankings || stationPoints[0]?.verification?.candidate_rankings || [];

  const selectedStation =
    selectedStationIndex !== null ? stationPoints[selectedStationIndex] || null : null;

  const hoverStation =
    hoverStationIndex !== null ? stationPoints[hoverStationIndex] || null : null;

  const tooltipStation = showStations ? (selectedStation || hoverStation) : null;


  const selectedStationIdentity = useMemo(
    () => buildStationIdentity(state?.selected_route_name || state?.route_name, selectedStation),
    [state?.selected_route_name, state?.route_name, selectedStation]
  );

  const selectedStationSummary = useMemo(
    () => buildStationSummary(state?.selected_route_name || state?.route_name, selectedStation),
    [state?.selected_route_name, state?.route_name, selectedStation]
  );

  const calculatedCoveredFootage = useMemo(() => {
    const fromSegments = redlineSegments.reduce((sum, segment) => {
      const len = typeof segment.length_ft === "number" && Number.isFinite(segment.length_ft) ? segment.length_ft : 0;
      return sum + len;
    }, 0);
    if (fromSegments > 0) return fromSegments;
    const backendCovered = typeof state?.covered_length_ft === "number" && Number.isFinite(state.covered_length_ft)
      ? state.covered_length_ft
      : 0;
    return backendCovered;
  }, [redlineSegments, state?.covered_length_ft]);

  const effectiveFootage = useMemo(() => {
    const manual = Number.parseFloat(manualFootage);
    if (Number.isFinite(manual) && manual > 0) return manual;
    return calculatedCoveredFootage;
  }, [manualFootage, calculatedCoveredFootage]);

  const numericCostPerFoot = useMemo(() => {
    const parsed = Number.parseFloat(costPerFoot);
    return Number.isFinite(parsed) && parsed >= 0 ? parsed : 0;
  }, [costPerFoot]);

  const exceptionTotal = useMemo(
    () =>
      exceptions.reduce((sum, item) => {
        const parsed = Number.parseFloat(item.amount);
        return sum + (Number.isFinite(parsed) ? parsed : 0);
      }, 0),
    [exceptions]
  );

  const baseBillingTotal = useMemo(() => effectiveFootage * numericCostPerFoot, [effectiveFootage, numericCostPerFoot]);
  const finalBillingTotal = useMemo(() => baseBillingTotal + exceptionTotal, [baseBillingTotal, exceptionTotal]);

  const drillPathRows = useMemo(() => {
    const rows: Array<{
      id: string;
      startStation: string;
      endStation: string;
      lengthFt: number;
      cost: number;
      print: string;
      sourceFile: string;
      routeName: string;
    }> = [];

    let current:
      | {
          id: string;
          startStation: string;
          endStation: string;
          lengthFt: number;
          cost: number;
          print: string;
          sourceFile: string;
          routeName: string;
          groupKey: string;
        }
      | null = null;

    redlineSegments.forEach((segment, idx) => {
      const lengthFt =
        typeof segment.length_ft === "number" && Number.isFinite(segment.length_ft) ? segment.length_ft : 0;
      const startStation = cleanDisplayText(segment.start_station);
      const endStation = cleanDisplayText(segment.end_station);
      const print = cleanDisplayText(segment.print);
      const sourceFile = cleanDisplayText(segment.source_file);
      const routeName = cleanDisplayText(segment.route_name);
      const groupKey = `${routeName}||${print}||${sourceFile}`;

      if (!current || current.groupKey !== groupKey) {
        if (current) {
          rows.push({
            id: current.id,
            startStation: current.startStation,
            endStation: current.endStation,
            lengthFt: current.lengthFt,
            cost: current.cost,
            print: current.print,
            sourceFile: current.sourceFile,
            routeName: current.routeName,
          });
        }
        current = {
          id: `drill-path-${idx + 1}`,
          startStation,
          endStation,
          lengthFt,
          cost: lengthFt * numericCostPerFoot,
          print,
          sourceFile,
          routeName,
          groupKey,
        };
        return;
      }

      current.endStation = endStation;
      current.lengthFt += lengthFt;
      current.cost += lengthFt * numericCostPerFoot;
    });

    if (current) {
      rows.push({
        id: current.id,
        startStation: current.startStation,
        endStation: current.endStation,
        lengthFt: current.lengthFt,
        cost: current.cost,
        print: current.print,
        sourceFile: current.sourceFile,
        routeName: current.routeName,
      });
    }

    return rows;
  }, [redlineSegments, numericCostPerFoot]);

  const handleAddException = useCallback(() => {
    const label = extraExceptionLabel.trim();
    if (!label) return;
    setExceptions((current) => [
      ...current,
      { id: `custom-${Date.now()}`, label, amount: extraExceptionAmount.trim() },
    ]);
    setExtraExceptionLabel("");
    setExtraExceptionAmount("");
  }, [extraExceptionLabel, extraExceptionAmount]);

  const handleRemoveException = useCallback((id: string) => {
    setExceptions((current) => current.filter((item) => item.id !== id));
  }, []);

  const handleExceptionChange = useCallback((id: string, field: "label" | "amount", value: string) => {
    setExceptions((current) =>
      current.map((item) => (item.id === id ? { ...item, [field]: value } : item))
    );
  }, []);

  const handlePrintReport = useCallback(() => {
    if (typeof window !== "undefined") {
      window.print();
    }
  }, []);

  const fitToBounds = useCallback((targetBounds: Bounds | null) => {
    const container = mapContainerRef.current;
    if (!container || !targetBounds) return;

    const width = Math.max(1, container.clientWidth);
    const height = Math.max(1, container.clientHeight);

    const topLeft = projectWorldPoint(targetBounds.maxLat, targetBounds.minLon, targetBounds);
    const bottomRight = projectWorldPoint(targetBounds.minLat, targetBounds.maxLon, targetBounds);

    const contentWidth = Math.max(1, bottomRight.x - topLeft.x);
    const contentHeight = Math.max(1, bottomRight.y - topLeft.y);
    const usableWidth = Math.max(1, width - FIT_PADDING * 2);
    const usableHeight = Math.max(1, height - FIT_PADDING * 2);

    const zoom = clamp(Math.min(usableWidth / contentWidth, usableHeight / contentHeight), MIN_ZOOM, MAX_ZOOM);
    const centerWorldX = (topLeft.x + bottomRight.x) / 2;
    const centerWorldY = (topLeft.y + bottomRight.y) / 2;

    setViewport({
      zoom,
      panX: width / 2 - centerWorldX * zoom,
      panY: height / 2 - centerWorldY * zoom,
    });
  }, []);

  const zoomAt = useCallback((nextZoom: number, anchorX: number, anchorY: number) => {
    setViewport((current) => {
      const zoom = clamp(nextZoom, MIN_ZOOM, MAX_ZOOM);
      if (zoom === current.zoom) return current;

      const world = screenToWorld(anchorX, anchorY, current);
      return {
        zoom,
        panX: anchorX - world.x * zoom,
        panY: anchorY - world.y * zoom,
      };
    });
  }, []);

  async function fetchState(message?: string) {
    if (message) {
      setStatusText(message);
      setStatusTone("neutral");
    }
    try {
      const response = await fetch(`${API_BASE}/api/current-state`);
      const data: BackendState = await response.json();
      if (!response.ok || data.success === false) throw new Error(data.error || "Unable to load current state.");
      setState(data);
      if (data.warning) {
        setStatusText(String(data.warning));
        setStatusTone("warning");
      } else if (data.message) {
        setStatusText(String(data.message));
        setStatusTone("success");
      } else if ((data.redline_segments || []).length > 0) {
        setStatusText("Local backend connected. KMZ, redlines, and stations loaded.");
        setStatusTone("success");
      } else if ((data.kmz_reference?.line_features || []).length > 0) {
        setStatusText("Local backend connected. KMZ loaded. Waiting for bore logs.");
        setStatusTone("success");
      } else {
        setStatusText("Local backend connected. Workspace is empty and ready.");
        setStatusTone("neutral");
      }
    } catch (error) {
      setStatusText(error instanceof Error ? error.message : "Backend connection failed.");
      setStatusTone("error");
    }
  }

  async function handleReset() {
    setBusy(true);
    try {
      const response = await fetch(`${API_BASE}/api/reset-state`, { method: "POST" });
      const data: BackendState = await response.json();
      if (!response.ok || data.success === false) throw new Error(data.error || "Reset failed.");
      setState(data);
      setDidInitialFit(false);
      userHasAdjustedViewportRef.current = false;
      lastAutoFitSignatureRef.current = "";
      if (initialFitRafRef.current !== null) {
        cancelAnimationFrame(initialFitRafRef.current);
        initialFitRafRef.current = null;
      }
      if (initialFitTimeoutRef.current) {
        clearTimeout(initialFitTimeoutRef.current);
        initialFitTimeoutRef.current = null;
      }
      setSelectedStationIndex(null);
      setHoverStationIndex(null);
      setStatusText(String(data.message || "Workspace reset successfully."));
      setStatusTone("success");
    } catch (error) {
      setStatusText(error instanceof Error ? error.message : "Reset failed.");
      setStatusTone("error");
    } finally {
      setBusy(false);
    }
  }

  async function handleDesignUpload(file: File) {
    setBusy(true);
    setStatusText(`Uploading design: ${file.name}`);
    setStatusTone("neutral");
    try {
      const form = new FormData();
      form.append("file", file);
      const response = await fetch(`${API_BASE}/api/upload-design`, { method: "POST", body: form });
      const data: BackendState = await response.json();
      if (!response.ok || data.success === false) throw new Error(data.error || "Design upload failed.");
      setState(data);
      setDidInitialFit(false);
      userHasAdjustedViewportRef.current = false;
      lastAutoFitSignatureRef.current = "";
      if (initialFitRafRef.current !== null) {
        cancelAnimationFrame(initialFitRafRef.current);
        initialFitRafRef.current = null;
      }
      if (initialFitTimeoutRef.current) {
        clearTimeout(initialFitTimeoutRef.current);
        initialFitTimeoutRef.current = null;
      }
      setStatusText(String(data.warning || data.message || "Design uploaded successfully."));
      setStatusTone(data.warning ? "warning" : "success");
    } catch (error) {
      setStatusText(error instanceof Error ? error.message : "Design upload failed.");
      setStatusTone("error");
    } finally {
      setBusy(false);
    }
  }

  async function handleBoreUpload(files: FileList | null) {
    if (!files || !files.length) return;
    setBusy(true);
    setStatusText(`Uploading ${files.length} structured bore file${files.length > 1 ? "s" : ""}...`);
    setStatusTone("neutral");
    try {
      const form = new FormData();
      Array.from(files).forEach((file) => form.append("files", file));
      const response = await fetch(`${API_BASE}/api/upload-structured-bore-files`, { method: "POST", body: form });
      const data: BackendState = await response.json();
      if (!response.ok || data.success === false) throw new Error(data.error || "Structured bore upload failed.");
      setState(data);
      setDidInitialFit(false);
      userHasAdjustedViewportRef.current = false;
      lastAutoFitSignatureRef.current = "";
      if (initialFitRafRef.current !== null) {
        cancelAnimationFrame(initialFitRafRef.current);
        initialFitRafRef.current = null;
      }
      if (initialFitTimeoutRef.current) {
        clearTimeout(initialFitTimeoutRef.current);
        initialFitTimeoutRef.current = null;
      }
      setStatusText(String(data.warning || data.message || "Structured bore files uploaded successfully."));
      setStatusTone(data.warning ? "warning" : "success");
    } catch (error) {
      setStatusText(error instanceof Error ? error.message : "Structured bore upload failed.");
      setStatusTone("error");
    } finally {
      setBusy(false);
    }
  }

  async function submitBugNote() {
    if (!notes.trim()) return;
    setBusy(true);
    try {
      const payload = {
        id: `beta-note-${Date.now()}`,
        timestamp: new Date().toISOString(),
        level: "info",
        category: "beta-test",
        message: notes.trim(),
        details: {
          enteredJobLabel: jobLabel,
          selectedRouteName: state?.selected_route_name || state?.route_name || "",
          redlineSegmentCount: (state?.redline_segments || []).length,
          stationPointCount: (state?.station_points || []).length,
        },
      };
      const response = await fetch(`${API_BASE}/api/report-bug`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const data = await response.json();
      if (!response.ok || data.success === false) throw new Error(data.error || "Note submission failed.");
      setStatusText(String(data.message || "Operator note submitted."));
      setStatusTone("success");
      setNotes("");
      await fetchState();
    } catch (error) {
      setStatusText(error instanceof Error ? error.message : "Note submission failed.");
      setStatusTone("error");
    } finally {
      setBusy(false);
    }
  }

  useEffect(() => {
    fetchState("Connecting to local beta backend...");
  }, []);

  useEffect(() => {
    if (!showStations) {
      setHoverStationIndex(null);
      setSelectedStationIndex(null);
    }
  }, [showStations]);


  async function fetchStationPhotos(stationIdentity: string) {
    if (!stationIdentity) {
      setStationPhotos([]);
      return;
    }
    setStationPhotosLoading(true);
    try {
      const response = await fetch(
        `${API_BASE}/api/station-photos?station_identity=${encodeURIComponent(stationIdentity)}`
      );
      const data = await response.json();
      if (!response.ok || data.success === false) {
        throw new Error(data.error || "Unable to load station photos.");
      }
      setStationPhotos(Array.isArray(data.photos) ? data.photos : []);
    } catch (error) {
      setStationPhotos([]);
      setStatusText(error instanceof Error ? error.message : "Unable to load station photos.");
      setStatusTone("error");
    } finally {
      setStationPhotosLoading(false);
    }
  }

  async function handleStationPhotoUpload(files: FileList | null) {
    if (!files || !files.length || !selectedStation || !selectedStationIdentity) return;
    setStationPhotoBusy(true);
    setStatusTone("neutral");
    setStatusText(`Uploading ${files.length} station photo${files.length > 1 ? "s" : ""}...`);
    try {
      const form = new FormData();
      form.append("station_identity", selectedStationIdentity);
      form.append("station_summary", selectedStationSummary);
      form.append("route_name", state?.selected_route_name || state?.route_name || "");
      form.append("source_file", selectedStation.source_file || "");
      form.append("station_label", selectedStation.station || "");
      form.append(
        "mapped_station_ft",
        stationIdentityPart(selectedStation.mapped_station_ft, 3)
      );
      form.append(
        "lat",
        stationIdentityPart(selectedStation.lat, 8)
      );
      form.append(
        "lon",
        stationIdentityPart(selectedStation.lon, 8)
      );
      Array.from(files).forEach((file) => form.append("files", file));

      const response = await fetch(`${API_BASE}/api/station-photos/upload`, {
        method: "POST",
        body: form,
      });
      const data = await response.json();
      if (!response.ok || data.success === false) {
        throw new Error(data.error || "Station photo upload failed.");
      }
      setStatusTone("success");
      setStatusText(data.message || "Station photo uploaded.");
      await fetchStationPhotos(selectedStationIdentity);
    } catch (error) {
      setStatusTone("error");
      setStatusText(error instanceof Error ? error.message : "Station photo upload failed.");
    } finally {
      setStationPhotoBusy(false);
    }
  }



  useEffect(() => {
    if (!selectedStation || !selectedStationIdentity) {
      setStationPhotos([]);
      return;
    }
    fetchStationPhotos(selectedStationIdentity);
  }, [selectedStation, selectedStationIdentity]);

  useEffect(() => {
    const container = mapContainerRef.current;
    if (!container) return;

    let resizeTimeout: ReturnType<typeof setTimeout> | null = null;

    const updateSize = () => {
      setContainerSize((prev) => {
        const newWidth = Math.max(1, Math.round(container.clientWidth));
        const newHeight = Math.max(1, Math.round(container.clientHeight));

        if (prev.width === newWidth && prev.height === newHeight) {
          return prev;
        }

        return {
          width: newWidth,
          height: newHeight,
        };
      });
    };

    updateSize();
    const observer = new ResizeObserver(() => {
      if (resizeTimeout) {
        clearTimeout(resizeTimeout);
      }
      resizeTimeout = setTimeout(updateSize, 100);
    });
    observer.observe(container);

    return () => {
      if (resizeTimeout) {
        clearTimeout(resizeTimeout);
      }
      observer.disconnect();
    };
  }, []);

  useEffect(() => {
    if (didInitialFit) return;
    if (userHasAdjustedViewportRef.current) return;
    if (containerSize.width <= 0 || containerSize.height <= 0) return;

    const targetBounds = initialFitBounds;
    if (!targetBounds) return;
    if (!(designCoords.length > 0 || routeCoords.length > 0 || stationPoints.length > 0)) return;
    if (!autoFitSignature) return;
    if (lastAutoFitSignatureRef.current === autoFitSignature) return;

    if (initialFitRafRef.current !== null) {
      cancelAnimationFrame(initialFitRafRef.current);
      initialFitRafRef.current = null;
    }
    if (initialFitTimeoutRef.current) {
      clearTimeout(initialFitTimeoutRef.current);
      initialFitTimeoutRef.current = null;
    }

    initialFitRafRef.current = window.requestAnimationFrame(() => {
      initialFitTimeoutRef.current = setTimeout(() => {
        if (userHasAdjustedViewportRef.current) return;
        if (didInitialFit) return;
        if (containerSize.width <= 0 || containerSize.height <= 0) return;
        if (!(designCoords.length > 0 || routeCoords.length > 0 || stationPoints.length > 0)) return;
        fitToBounds(targetBounds);
        lastAutoFitSignatureRef.current = autoFitSignature;
        setDidInitialFit(true);
      }, 0);
    });

    return () => {
      if (initialFitRafRef.current !== null) {
        cancelAnimationFrame(initialFitRafRef.current);
        initialFitRafRef.current = null;
      }
      if (initialFitTimeoutRef.current) {
        clearTimeout(initialFitTimeoutRef.current);
        initialFitTimeoutRef.current = null;
      }
    };
  }, [
    didInitialFit,
    initialFitBounds,
    autoFitSignature,
    containerSize.width,
    containerSize.height,
    designCoords.length,
    routeCoords.length,
    stationPoints.length,
    fitToBounds,
  ]);

  function handleWheel(e: React.WheelEvent<HTMLDivElement>) {
    e.preventDefault();
    e.stopPropagation();
    userHasAdjustedViewportRef.current = true;

    const rect = mapContainerRef.current?.getBoundingClientRect();
    if (!rect) return;

    const anchorX = e.clientX - rect.left;
    const anchorY = e.clientY - rect.top;
    zoomAt(viewport.zoom * (e.deltaY < 0 ? WHEEL_IN : WHEEL_OUT), anchorX, anchorY);
  }

  function handlePointerDown(e: React.PointerEvent<HTMLDivElement>) {
    if (e.button !== 0) return;
    const rect = mapContainerRef.current?.getBoundingClientRect();
    if (!rect) return;

    e.preventDefault();
    e.stopPropagation();

    if (e.shiftKey) {
      userHasAdjustedViewportRef.current = true;
      setBoxZoom({
        startX: e.clientX - rect.left,
        startY: e.clientY - rect.top,
        endX: e.clientX - rect.left,
        endY: e.clientY - rect.top,
      });
      e.currentTarget.setPointerCapture?.(e.pointerId);
      return;
    }

    userHasAdjustedViewportRef.current = true;
    panStartRef.current = {
      x: e.clientX,
      y: e.clientY,
      panX: viewport.panX,
      panY: viewport.panY,
    };
    setIsPanning(true);
    e.currentTarget.setPointerCapture?.(e.pointerId);
  }

  function handlePointerMove(e: React.PointerEvent<HTMLDivElement>) {
    const rect = mapContainerRef.current?.getBoundingClientRect();
    if (!rect) return;

    if (boxZoom) {
      setBoxZoom((current) =>
        current
          ? {
              ...current,
              endX: e.clientX - rect.left,
              endY: e.clientY - rect.top,
            }
          : null
      );
      return;
    }

    if (isPanning && panStartRef.current) {
      const dx = e.clientX - panStartRef.current.x;
      const dy = e.clientY - panStartRef.current.y;
      setViewport((current) => ({
        ...current,
        panX: panStartRef.current ? panStartRef.current.panX + dx : current.panX,
        panY: panStartRef.current ? panStartRef.current.panY + dy : current.panY,
      }));
      return;
    }

    if (!showStations) {
      setHoverStationIndex(null);
      return;
    }

    const localX = e.clientX - rect.left;
    const localY = e.clientY - rect.top;
    const nearest = projectedStations.reduce(
      (best, station) => {
        const screen = worldToScreen(station.world.x, station.world.y, viewport);
        const d = Math.hypot(screen.x - localX, screen.y - localY);
        if (d < best.distance) {
          return { idx: station.idx, distance: d };
        }
        return best;
      },
      { idx: -1, distance: Number.POSITIVE_INFINITY }
    );

    if (nearest.distance <= STATION_HIT_RADIUS_PX) {
      setHoverStationIndex(nearest.idx);
    } else {
      setHoverStationIndex(null);
    }
  }

  function handlePointerUp(e: React.PointerEvent<HTMLDivElement>) {
    if (boxZoom && mapContainerRef.current) {
      const width = Math.abs(boxZoom.endX - boxZoom.startX);
      const height = Math.abs(boxZoom.endY - boxZoom.startY);

      if (width > 18 && height > 18) {
        const boxLeft = Math.min(boxZoom.startX, boxZoom.endX);
        const boxTop = Math.min(boxZoom.startY, boxZoom.endY);
        const boxCenterX = boxLeft + width / 2;
        const boxCenterY = boxTop + height / 2;

        const currentWorldWidth = containerSize.width / viewport.zoom;
        const currentWorldHeight = containerSize.height / viewport.zoom;
        const selectedWorldWidth = currentWorldWidth * (width / containerSize.width);
        const selectedWorldHeight = currentWorldHeight * (height / containerSize.height);
        const targetZoom = clamp(
          Math.min(containerSize.width / selectedWorldWidth, containerSize.height / selectedWorldHeight),
          MIN_ZOOM,
          MAX_ZOOM
        );

        const centerWorld = screenToWorld(boxCenterX, boxCenterY, viewport);
        setViewport({
          zoom: targetZoom,
          panX: containerSize.width / 2 - centerWorld.x * targetZoom,
          panY: containerSize.height / 2 - centerWorld.y * targetZoom,
        });
      }

      setBoxZoom(null);
      return;
    }

    if (!isPanning || !panStartRef.current) {
      return;
    }

    if (!showStations) {
      setIsPanning(false);
      panStartRef.current = null;
      return;
    }

    const moved = Math.hypot(e.clientX - panStartRef.current.x, e.clientY - panStartRef.current.y);
    if (moved < 4) {
      const rect = mapContainerRef.current?.getBoundingClientRect();
      if (rect) {
        const localX = e.clientX - rect.left;
        const localY = e.clientY - rect.top;
        const nearest = projectedStations.reduce(
          (best, station) => {
            const screen = worldToScreen(station.world.x, station.world.y, viewport);
            const d = Math.hypot(screen.x - localX, screen.y - localY);
            if (d < best.distance) {
              return { idx: station.idx, distance: d };
            }
            return best;
          },
          { idx: -1, distance: Number.POSITIVE_INFINITY }
        );
        if (nearest.distance <= STATION_HIT_RADIUS_PX) {
          setSelectedStationIndex(nearest.idx);
        }
      }
    }

    setIsPanning(false);
    panStartRef.current = null;
  }

  const tooltipScreenPosition = useMemo(() => {
    if (hoverStationIndex === null && selectedStationIndex === null) return null;
    const idx = hoverStationIndex ?? selectedStationIndex;
    const station = projectedStations.find((item) => item.idx === idx);
    if (!station) return null;
    return worldToScreen(station.world.x, station.world.y, viewport);
  }, [hoverStationIndex, selectedStationIndex, projectedStations, viewport]);

  const redlineStroke = "rgba(255, 72, 72, 1)";
  const redlineCasing = "rgba(18, 4, 6, 0.82)";
  const hasDesign = (kmzLineFeatures.length || kmzPolygonFeatures.length) > 0;
  const hasBoreFiles = (state?.loaded_field_data_files || 0) > 0;
  const hasGeneratedOutput = redlineSegments.length > 0 || stationPoints.length > 0;

  return (
    <div style={{ minHeight: "100vh", background: "linear-gradient(180deg, #eef3f8 0%, #f6f9fc 100%)", fontFamily: "Inter, ui-sans-serif, system-ui, sans-serif", color: "#0f172a" }}>
      <style>{`
        @media print {
          body {
            background: #ffffff !important;
          }
          button, input[type="file"], textarea {
            -webkit-print-color-adjust: exact;
            print-color-adjust: exact;
          }
          .no-print {
            display: none !important;
          }
          .print-report {
            box-shadow: none !important;
            border-color: #d1d5db !important;
            break-inside: avoid;
          }
        }
      `}</style>
      <div style={{ maxWidth: 1520, margin: "0 auto", padding: 20 }}>
        <div style={{ display: "grid", gap: 18 }}>
          <div
            style={{
              background: "linear-gradient(135deg, #ffffff 0%, #f7fbff 52%, #eef6ff 100%)",
              border: "1px solid #dbe4ee",
              borderRadius: 24,
              padding: 24,
              boxShadow: "0 12px 28px rgba(15, 23, 42, 0.05)",
            }}
          >
            <div style={{ display: "flex", justifyContent: "space-between", gap: 16, alignItems: "flex-start", flexWrap: "wrap" }}>
              <div style={{ maxWidth: 860 }}>
                <div style={{ display: "inline-flex", alignItems: "center", gap: 8, padding: "6px 10px", borderRadius: 999, background: "#eff6ff", border: "1px solid #cfe0f5", fontSize: 12, fontWeight: 800, color: "#1d4ed8", marginBottom: 12 }}>
                  Phase 1 Safe UI Polish
                </div>
                <div style={{ fontSize: 34, fontWeight: 900, letterSpacing: -0.7 }}>OSP Redlining Operator Workspace</div>
                <div style={{ marginTop: 8, fontSize: 15, color: "#526173", lineHeight: 1.6 }}>
                  Upload design files, load bore logs, generate redlines, review the map, and stage reporting outputs in one cleaner top-to-bottom workflow.
                </div>
                <div style={{ marginTop: 14, display: "flex", gap: 10, flexWrap: "wrap" }}>
                  <Pill label="API" value={API_BASE} />
                  <Pill label="Active job" value={String(activeJob)} />
                  <Pill label="Status" value={String(verification?.status || "waiting")} />
                </div>
              </div>

              <div style={{ display: "grid", gap: 10, minWidth: 320, flex: "0 1 360px" }}>
                <input
                  value={jobLabel}
                  onChange={(e) => setJobLabel(e.target.value)}
                  placeholder="Optional local beta job label"
                  style={{ borderRadius: 14, border: "1px solid #cfd8e3", background: "#fff", padding: "12px 14px", outline: "none", fontSize: 14 }}
                />
                <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: 10 }}>
                  <button onClick={() => fetchState("Refreshing backend state...")} disabled={busy} style={buttonStyle("#ffffff", "#0f172a", "#cfd8e3", busy)}>Refresh State</button>
                  <button onClick={handleReset} disabled={busy} style={buttonStyle("#0f172a", "#ffffff", "#0f172a", busy)}>Clear Workspace</button>
                </div>
              </div>
            </div>
          </div>

          <StatusBanner tone={statusTone} text={statusText} />

          <div style={{ display: "grid", gridTemplateColumns: "repeat(4, minmax(0, 1fr))", gap: 16 }}>
            <SummaryCard title="Active Job" value={String(activeJob)} subtitle="Local label or backend-selected route" />
            <SummaryCard title="Files Loaded" value={String((hasDesign ? 1 : 0) + (state?.loaded_field_data_files || 0))} subtitle="Design + structured bore files" />
            <SummaryCard title="QA Status" value={String(verification?.status || "waiting")} subtitle="Real backend verification summary" />
            <SummaryCard title="Output Counts" value={`${stationPoints.length} pts / ${redlineSegments.length} segs`} subtitle="Station points and generated redline segments" />
          </div>

          <Section
            title="1. Upload"
            subtitle="Load the design first, then add one or more structured bore log files. This section stays tied to the current backend workflow."
          >
            <div style={{ display: "grid", gridTemplateColumns: "1.1fr 1.1fr 0.8fr", gap: 16, alignItems: "start" }}>
              <label style={uploadCardStyle(busy)}>
                <input
                  type="file"
                  accept=".kmz,.kml"
                  style={{ display: "none" }}
                  disabled={busy}
                  onChange={(e) => {
                    const file = e.target.files?.[0];
                    if (file) handleDesignUpload(file);
                    e.currentTarget.value = "";
                  }}
                />
                <div style={{ fontWeight: 800, fontSize: 16 }}>Upload KMZ Design</div>
                <div style={{ marginTop: 6, fontSize: 13, color: "#64748b", lineHeight: 1.55 }}>Loads KMZ layers and selected route geometry without changing map internals.</div>
                <div style={{ marginTop: 14, fontSize: 12, color: hasDesign ? "#166534" : "#64748b", fontWeight: 700 }}>
                  {hasDesign ? "Design appears loaded in backend state." : "No design currently loaded."}
                </div>
              </label>

              <label style={uploadCardStyle(busy)}>
                <input
                  type="file"
                  accept=".xlsx,.xls,.csv"
                  multiple
                  style={{ display: "none" }}
                  disabled={busy}
                  onChange={(e) => {
                    handleBoreUpload(e.target.files);
                    e.currentTarget.value = "";
                  }}
                />
                <div style={{ fontWeight: 800, fontSize: 16 }}>Upload Structured Bore Logs</div>
                <div style={{ marginTop: 6, fontSize: 13, color: "#64748b", lineHeight: 1.55 }}>Triggers the existing backend upload flow for route matching, station mapping, and generated redlines.</div>
                <div style={{ marginTop: 14, fontSize: 12, color: hasBoreFiles ? "#166534" : "#64748b", fontWeight: 700 }}>
                  {hasBoreFiles ? `${state?.loaded_field_data_files || 0} bore file(s) loaded.` : "No bore files currently loaded."}
                </div>
              </label>

              <div style={{ border: "1px solid #dbe4ee", borderRadius: 16, background: "#fbfdff", padding: 16 }}>
                <div style={{ fontWeight: 800, fontSize: 15 }}>File status</div>
                <div style={{ marginTop: 12, display: "grid", gap: 10 }}>
                  <Pill label="Design" value={hasDesign ? "Loaded" : "Waiting"} />
                  <Pill label="Bore files" value={String(state?.loaded_field_data_files || 0)} />
                  <Pill label="Latest file" value={state?.latest_structured_file || "--"} />
                  <Pill label="Output ready" value={hasGeneratedOutput ? "Yes" : "No"} />
                </div>
              </div>
            </div>
          </Section>

          <Section
            title="2. Actions"
            subtitle="Workspace controls and live backend facts. These controls use the existing execution flow exactly as-is."
          >
            <div style={{ display: "grid", gridTemplateColumns: "0.95fr 1.05fr", gap: 16, alignItems: "start" }}>
              <div style={{ display: "grid", gap: 12 }}>
                <div style={{ border: "1px solid #dbe4ee", borderRadius: 16, padding: 16, background: "#fbfdff" }}>
                  <div style={{ fontSize: 15, fontWeight: 800 }}>Workflow guidance</div>
                  <div style={{ marginTop: 8, fontSize: 13, color: "#64748b", lineHeight: 1.65 }}>
                    1. Upload a KMZ design.<br />
                    2. Upload structured bore logs.<br />
                    3. Review generated output on the map.<br />
                    4. Use the reporting shells below for staging pricing, crew, exceptions, and export workflow.
                  </div>
                </div>

                <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: 12 }}>
                  <button onClick={() => fetchState("Refreshing backend state...")} disabled={busy} style={buttonStyle("#ffffff", "#0f172a", "#cfd8e3", busy)}>Refresh Backend State</button>
                  <button onClick={handleReset} disabled={busy} style={buttonStyle("#0f172a", "#ffffff", "#0f172a", busy)}>Clear Workspace</button>
                </div>
              </div>

              <div style={{ border: "1px solid #dbe4ee", borderRadius: 16, padding: 16, background: "#ffffff" }}>
                <div style={{ fontSize: 15, fontWeight: 800, marginBottom: 6 }}>Current backend state</div>
                <SmallRow label="Selected route" value={state?.selected_route_name || state?.route_name || "--"} />
                <SmallRow label="Suggested route id" value={state?.suggested_route_id || "--"} />
                <SmallRow label="Latest bore file" value={state?.latest_structured_file || "--"} />
                <SmallRow label="Field data files" value={String(state?.loaded_field_data_files || 0)} />
                <SmallRow label="Route length" value={`${formatNumber(state?.total_length_ft)} ft`} />
                <SmallRow label="Covered length" value={`${formatNumber(state?.covered_length_ft)} ft`} />
                <SmallRow label="Completion %" value={`${formatNumber(state?.completion_pct)}%`} />
                <SmallRow label="Active-route covered" value={`${formatNumber(state?.active_route_covered_length_ft)} ft`} />
                <SmallRow label="Active-route completion" value={`${formatNumber(state?.active_route_completion_pct)}%`} />
                <SmallRow label="Mapping mode" value={state?.station_mapping_mode || "--"} />
                <SmallRow label="Committed rows" value={String((state?.committed_rows || []).length)} />
                <SmallRow label="Bug report count" value={String(state?.bug_report_count || 0)} />
              </div>
            </div>
          </Section>

          <Section
            title="3. Map Review"
            subtitle="Safe map polish only: smaller black stations, stronger redline readability, cleaner field-review callouts, and initial fit prioritized to the full KMZ design footprint."
            actions={
              <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                <button onClick={() => {
                  userHasAdjustedViewportRef.current = true;
                  zoomAt(viewport.zoom * BUTTON_IN, containerSize.width / 2, containerSize.height / 2);
                }} style={miniMapButton}>+</button>
                <button onClick={() => {
                  userHasAdjustedViewportRef.current = true;
                  zoomAt(viewport.zoom * BUTTON_OUT, containerSize.width / 2, containerSize.height / 2);
                }} style={miniMapButton}>-</button>
                <button onClick={() => {
                  userHasAdjustedViewportRef.current = true;
                  fitToBounds(designBounds || bounds);
                }} style={miniMapButton}>Fit All</button>
                <button onClick={() => {
                  userHasAdjustedViewportRef.current = true;
                  fitToBounds(stationOnlyBounds || bounds);
                }} style={miniMapButton}>Fit Stations</button>
                <button onClick={() => setShowStations((current) => !current)} style={miniMapButton}>{showStations ? "Hide Stations" : "Show Stations"}</button>
              </div>
            }
          >
            <div style={{ display: "grid", gap: 16 }}>
              <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                <Pill label="Matched route" value={selectedMatch?.route_name || state?.selected_route_name || state?.route_name || "--"} />
                <Pill label="Route role" value={selectedMatch?.route_role || "--"} />
                <Pill label="Confidence" value={selectedMatch?.confidence_label || stationPoints[0]?.verification?.confidence || "--"} />
                <Pill label="Decision" value={selectedMatch?.final_decision || stationPoints[0]?.verification?.reason || "--"} />
              </div>

              <div
                ref={mapContainerRef}
                style={{
                  position: "relative",
                  height: MAP_HEIGHT,
                  borderRadius: 18,
                  overflow: "hidden",
                  background: "linear-gradient(180deg, #102535 0%, #0a1824 58%, #07111a 100%)",
                  border: "1px solid #1a3650",
                  cursor: boxZoom ? "crosshair" : isPanning ? "grabbing" : "grab",
                  overscrollBehavior: "contain",
                  touchAction: "none",
                  userSelect: "none",
                  boxShadow: "inset 0 1px 0 rgba(255,255,255,0.04)",
                }}
                onWheel={handleWheel}
                onPointerDown={handlePointerDown}
                onPointerMove={handlePointerMove}
                onPointerUp={handlePointerUp}
                onPointerCancel={handlePointerUp}
                onPointerLeave={() => {
                  if (!isPanning) setHoverStationIndex(null);
                }}
              >
                {bounds && allCoords.length > 0 ? (
                  <svg
                    viewBox={viewBoxToString(containerSize.width, containerSize.height, viewport)}
                    preserveAspectRatio="none"
                    style={{ position: "absolute", inset: 0, width: "100%", height: "100%", display: "block", shapeRendering: "geometricPrecision" }}
                  >
                    <g id="kmz-design-layer">
                      <rect x={0} y={0} width={WORLD_WIDTH} height={WORLD_HEIGHT} fill="rgba(8,18,26,0.96)" />

                      {kmzPolygonPaths.map((poly, idx) => {
                        const feature = kmzPolygonFeatures[idx];
                        return poly.path ? (
                          <path
                            key={poly.id}
                            d={poly.path}
                            fill={kmzPolygonFill(feature)}
                            fillOpacity={kmzPolygonOpacity(feature)}
                            stroke={kmzPolygonStroke(feature)}
                            strokeWidth={feature?.stroke_width ?? 1.2}
                            vectorEffect="non-scaling-stroke"
                          />
                        ) : null;
                      })}

                      {kmzLinePaths.map((line, idx) => {
                        const feature = kmzLineFeatures[idx];
                        const stroke = kmzLineStroke(feature);
                        const width = kmzLineWidth(feature);
                        return line.path ? (
                          <g key={line.id}>
                            <path
                              d={line.path}
                              fill="none"
                              stroke="rgba(12,18,28,0.18)"
                              strokeWidth={width + 0.8}
                              strokeLinecap="round"
                              strokeLinejoin="round"
                              vectorEffect="non-scaling-stroke"
                            />
                            <path
                              d={line.path}
                              fill="none"
                              stroke={stroke}
                              strokeOpacity={0.78}
                              strokeWidth={width}
                              strokeLinecap="round"
                              strokeLinejoin="round"
                              vectorEffect="non-scaling-stroke"
                            />
                          </g>
                        ) : null;
                      })}
                    </g>

                    <g id="redline-layer">
                      {redlinePaths.map((line) =>
                        line.path ? (
                          <g key={line.id}>
                            <path
                              d={line.path}
                              fill="none"
                              stroke={redlineCasing}
                              strokeWidth={6.2}
                              strokeLinecap="round"
                              strokeLinejoin="round"
                              vectorEffect="non-scaling-stroke"
                            />
                            <path
                              d={line.path}
                              fill="none"
                              stroke={redlineStroke}
                              strokeWidth={4.35}
                              strokeLinecap="round"
                              strokeLinejoin="round"
                              vectorEffect="non-scaling-stroke"
                            />
                          </g>
                        ) : null
                      )}
                    </g>

                    {showStations ? (
                      <g id="station-layer">
                      {projectedStations.map(({ idx, world }) => {
                        const isSelected = selectedStationIndex === idx;
                        const isHovered = hoverStationIndex === idx;
                        const baseRadius = viewport.zoom < 4 ? 1.8 : viewport.zoom < 12 ? 1.5 : 1.25;
                        const radius = isSelected ? baseRadius + 0.8 : isHovered ? baseRadius + 0.45 : baseRadius;
                        const halo = isSelected ? radius + 3.2 : isHovered ? radius + 2.1 : radius + 0.7;

                        return (
                          <g key={`station-${idx}`}>
                            {(isSelected || isHovered) ? (
                              <circle
                                cx={world.x}
                                cy={world.y}
                                r={halo}
                                fill={isSelected ? "rgba(255, 214, 10, 0.24)" : "rgba(255,255,255,0.16)"}
                              />
                            ) : null}
                            <circle
                              cx={world.x}
                              cy={world.y}
                              r={radius + 0.45}
                              fill="rgba(255,255,255,0.82)"
                            />
                            <circle
                              cx={world.x}
                              cy={world.y}
                              r={radius}
                              fill={isSelected ? "#facc15" : isHovered ? "#dbeafe" : "#05070a"}
                              stroke={isSelected ? "rgba(255,255,255,0.96)" : isHovered ? "rgba(255,255,255,0.92)" : "rgba(255,255,255,0.78)"}
                              strokeWidth={isSelected ? 1.05 : isHovered ? 0.95 : 0.8}
                              vectorEffect="non-scaling-stroke"
                            />
                          </g>
                        );
                      })}
                      </g>
                    ) : null}
                  </svg>
                ) : (
                  <div style={{ height: "100%", display: "flex", alignItems: "center", justifyContent: "center", textAlign: "center", padding: 24, color: "#cbd5e1", fontWeight: 700 }}>
                    Upload a KMZ and structured bore logs to render real map output.
                  </div>
                )}

                {projectedStations
                  .filter((station) => visibleLabelIndices.has(station.idx))
                  .map((station) => {
                    const screen = worldToScreen(station.world.x, station.world.y, viewport);
                    return (
                      <div
                        key={`label-${station.idx}`}
                        style={{
                          position: "absolute",
                          left: screen.x + 10,
                          top: screen.y - 24,
                          background: "rgba(14, 24, 34, 0.88)",
                          color: "#f8fafc",
                          border: "1px solid rgba(255,255,255,0.08)",
                          borderRadius: 8,
                          padding: "2px 7px",
                          fontSize: 10.5,
                          fontWeight: 700,
                          lineHeight: 1.2,
                          pointerEvents: "none",
                          whiteSpace: "nowrap",
                        }}
                      >
                        {station.point.station || "--"}
                      </div>
                    );
                  })}

                {boxZoom ? (
                  <div
                    style={{
                      position: "absolute",
                      left: Math.min(boxZoom.startX, boxZoom.endX),
                      top: Math.min(boxZoom.startY, boxZoom.endY),
                      width: Math.abs(boxZoom.endX - boxZoom.startX),
                      height: Math.abs(boxZoom.endY - boxZoom.startY),
                      background: "rgba(56,189,248,0.12)",
                      border: "2px dashed #38bdf8",
                      pointerEvents: "none",
                      boxSizing: "border-box",
                    }}
                  />
                ) : null}

                {stationPoints.length === 0 ? (
                  <div style={{ position: "absolute", right: 18, top: 18, borderRadius: 16, background: "rgba(127,29,29,0.92)", border: "1px solid rgba(254, 202, 202, 0.24)", color: "#fee2e2", padding: 14, width: 290, boxShadow: "0 14px 34px rgba(0,0,0,0.28)" }}>
                    <div style={{ fontWeight: 800, fontSize: 14 }}>No stations returned</div>
                    <div style={{ marginTop: 8, fontSize: 12, lineHeight: 1.5 }}>
                      The frontend is ready to render stations, but the backend payload currently contains 0 station points. Trace this flow next: upload-structured-bore-files response → /api/current-state payload → station_points array length.
                    </div>
                  </div>
                ) : null}

                {tooltipStation && tooltipScreenPosition ? (
                  <div
                    style={{
                      position: "absolute",
                      left: Math.min(containerSize.width - 346, tooltipScreenPosition.x + 18),
                      top: Math.max(18, tooltipScreenPosition.y - 18),
                      transform: "translateY(-50%)",
                      width: 314,
                      maxWidth: "calc(100% - 24px)",
                      borderRadius: 18,
                      background: "linear-gradient(180deg, rgba(255,255,255,0.985) 0%, rgba(247,250,252,0.975) 100%)",
                      border: "1px solid rgba(15, 23, 42, 0.16)",
                      padding: 14,
                      boxShadow: "0 18px 42px rgba(0,0,0,0.24)",
                      pointerEvents: "none",
                      backdropFilter: "blur(10px)",
                    }}
                  >
                    <div
                      style={{
                        display: "flex",
                        alignItems: "flex-start",
                        justifyContent: "space-between",
                        gap: 10,
                        paddingBottom: 10,
                        borderBottom: "1px solid rgba(148, 163, 184, 0.18)",
                      }}
                    >
                      <div>
                        <div style={{ fontSize: 11, fontWeight: 800, letterSpacing: 0.55, textTransform: "uppercase", color: "#64748b" }}>
                          Field inspection
                        </div>
                        <div style={{ marginTop: 4, fontWeight: 900, fontSize: 17, color: "#0f172a", lineHeight: 1.15 }}>
                          {cleanDisplayText(tooltipStation.station)}
                        </div>
                      </div>
                      <div
                        style={{
                          padding: "5px 8px",
                          borderRadius: 999,
                          background: "rgba(15, 23, 42, 0.06)",
                          color: "#334155",
                          fontSize: 10.5,
                          fontWeight: 800,
                          letterSpacing: 0.25,
                          whiteSpace: "nowrap",
                        }}
                      >
                        Hover
                      </div>
                    </div>

                    <div style={{ marginTop: 12, display: "grid", gap: 8 }}>
                      <TooltipRow label="Station" value={cleanDisplayText(tooltipStation.station)} />
                      <TooltipRow label="Mapped FT" value={formatNumber(tooltipStation.mapped_station_ft, 3)} />
                      <TooltipRow label="Depth FT" value={formatNumber(tooltipStation.depth_ft)} />
                      <TooltipRow label="BOC FT" value={formatNumber(tooltipStation.boc_ft)} />
                      <TooltipRow label="Date" value={formatDisplayDate(tooltipStation.date)} />
                      <TooltipRow label="Crew" value={cleanDisplayText(tooltipStation.crew)} />
                      <TooltipRow label="Print" value={cleanDisplayText(tooltipStation.print)} />
                      <TooltipRow label="Source" value={cleanDisplayText(tooltipStation.source_file)} />
                      <TooltipRow label="Notes" value={cleanDisplayText(tooltipStation.notes)} />
                      <TooltipRow
                        label="Lat / Lon"
                        value={`${formatNumber(tooltipStation.lat, 8)}, ${formatNumber(tooltipStation.lon, 8)}`}
                      />
                    </div>
                  </div>
                ) : null}

                              </div>

              <div
                style={{
                  border: "1px solid #dbe4ee",
                  borderRadius: 16,
                  background: "#ffffff",
                  padding: 16,
                  display: "grid",
                  gap: 14,
                }}
              >
                <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "flex-start", flexWrap: "wrap" }}>
                  <div>
                    <div style={{ fontSize: 15, fontWeight: 800, color: "#0f172a" }}>Station photos</div>
                    <div style={{ marginTop: 6, fontSize: 13, color: "#64748b", lineHeight: 1.55 }}>
                      Manual attach only. Select a station first, then upload one or more photos to that exact station.
                    </div>
                  </div>
                  <div style={{ fontSize: 12, color: "#475569", fontWeight: 700 }}>
                    {selectedStation ? selectedStationSummary : "No station selected"}
                  </div>
                </div>

                {selectedStation ? (
                  <div style={{ display: "grid", gap: 12 }}>
                    <div style={{ display: "flex", gap: 12, flexWrap: "wrap", alignItems: "center" }}>
                      <label
                        style={{
                          display: "inline-flex",
                          alignItems: "center",
                          justifyContent: "center",
                          padding: "10px 14px",
                          borderRadius: 12,
                          border: "1px solid #0f172a",
                          background: stationPhotoBusy ? "#e5e7eb" : "#0f172a",
                          color: "#ffffff",
                          fontWeight: 800,
                          cursor: stationPhotoBusy ? "not-allowed" : "pointer",
                          opacity: stationPhotoBusy ? 0.7 : 1,
                        }}
                      >
                        <input
                          type="file"
                          accept="image/*"
                          multiple
                          style={{ display: "none" }}
                          disabled={stationPhotoBusy}
                          onChange={(e) => {
                            handleStationPhotoUpload(e.target.files);
                            e.currentTarget.value = "";
                          }}
                        />
                        {stationPhotoBusy ? "Uploading..." : "Upload Station Photos"}
                      </label>

                      <div style={{ fontSize: 12, color: "#64748b" }}>
                        Stable station key: <strong>{selectedStationIdentity || "--"}</strong>
                      </div>
                    </div>

                    {stationPhotosLoading ? (
                      <div style={{ fontSize: 13, color: "#64748b" }}>Loading station photos...</div>
                    ) : stationPhotos.length ? (
                      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(140px, 1fr))", gap: 12 }}>
                        {stationPhotos.map((photo) => (
                          <a
                            key={photo.photo_id}
                            href={`${API_BASE}${photo.relative_url}`}
                            target="_blank"
                            rel="noreferrer"
                            style={{
                              textDecoration: "none",
                              color: "inherit",
                              border: "1px solid #dbe4ee",
                              borderRadius: 14,
                              overflow: "hidden",
                              background: "#fbfdff",
                            }}
                          >
                            <div
                              style={{
                                height: 112,
                                backgroundImage: `url(${API_BASE}${photo.relative_url})`,
                                backgroundSize: "cover",
                                backgroundPosition: "center",
                                backgroundRepeat: "no-repeat",
                                backgroundColor: "#e5e7eb",
                              }}
                            />
                            <div style={{ padding: 10 }}>
                              <div style={{ fontSize: 12, fontWeight: 700, color: "#0f172a", wordBreak: "break-word" }}>
                                {photo.original_filename}
                              </div>
                              <div style={{ marginTop: 4, fontSize: 11, color: "#64748b" }}>
                                {formatDisplayDate(photo.uploaded_at)}
                              </div>
                            </div>
                          </a>
                        ))}
                      </div>
                    ) : (
                      <div style={{ fontSize: 13, color: "#64748b" }}>
                        No photos attached to this station yet.
                      </div>
                    )}
                  </div>
                ) : (
                  <div style={{ fontSize: 13, color: "#64748b" }}>
                    Select a station on the map first. Photos only attach to the currently selected station.
                  </div>
                )}
              </div>

            </div>
          </Section>

          <div style={{ display: "grid", gridTemplateColumns: "1.1fr 0.9fr", gap: 18, alignItems: "start" }}>
            
<Section title="4. Reports" subtitle="Real report output built from current job data, redline sections, pricing inputs, and exception totals.">
              <div className="print-report" style={{ display: "grid", gap: 14 }}>
                <ShellCard
                  title="Field-to-billing report"
                  description="This report uses current route, redline, completion, pricing, and exception values only. Browser print is enabled for clean export."
                >
                  <div style={{ display: "grid", gap: 8 }}>
                    <SmallRow label="Job / Route" value={activeJob} />
                    <SmallRow label="Matched route" value={selectedMatch?.route_name || state?.selected_route_name || state?.route_name || "--"} />
                    <SmallRow label="Total footage" value={`${formatNumber(effectiveFootage)} ft`} />
                    <SmallRow label="Completion %" value={`${formatNumber(state?.completion_pct)}%`} />
                    <SmallRow label="Drill paths" value={String(drillPathRows.length)} />
                    <SmallRow label="Base cost / ft" value={toMoney(numericCostPerFoot)} />
                    <SmallRow label="Exception total" value={toMoney(exceptionTotal)} />
                    <SmallRow label="Final total" value={toMoney(finalBillingTotal)} />
                  </div>
                </ShellCard>

                <ShellCard
                  title="Drill Path Summary"
                  description="Each row collapses adjacent redline segments into one continuous drilled path using the existing redline report data only."
                >
                  {drillPathRows.length ? (
                    <div style={{ overflowX: "auto" }}>
                      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
                        <thead>
                          <tr>
                            <th style={{ textAlign: "left", padding: "10px 8px", borderBottom: "1px solid #dbe4ee" }}>Start</th>
                            <th style={{ textAlign: "left", padding: "10px 8px", borderBottom: "1px solid #dbe4ee" }}>End</th>
                            <th style={{ textAlign: "left", padding: "10px 8px", borderBottom: "1px solid #dbe4ee" }}>Length (FT)</th>
                            <th style={{ textAlign: "left", padding: "10px 8px", borderBottom: "1px solid #dbe4ee" }}>Cost</th>
                            <th style={{ textAlign: "left", padding: "10px 8px", borderBottom: "1px solid #dbe4ee" }}>Print</th>
                            <th style={{ textAlign: "left", padding: "10px 8px", borderBottom: "1px solid #dbe4ee" }}>Source</th>
                          </tr>
                        </thead>
                        <tbody>
                          {drillPathRows.map((row) => (
                            <tr key={row.id}>
                              <td style={{ padding: "10px 8px", borderBottom: "1px solid #eef2f7" }}>{row.startStation}</td>
                              <td style={{ padding: "10px 8px", borderBottom: "1px solid #eef2f7" }}>{row.endStation}</td>
                              <td style={{ padding: "10px 8px", borderBottom: "1px solid #eef2f7" }}>{formatNumber(row.lengthFt)}</td>
                              <td style={{ padding: "10px 8px", borderBottom: "1px solid #eef2f7" }}>{toMoney(row.cost)}</td>
                              <td style={{ padding: "10px 8px", borderBottom: "1px solid #eef2f7" }}>{row.print}</td>
                              <td style={{ padding: "10px 8px", borderBottom: "1px solid #eef2f7" }}>{row.sourceFile}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <div style={{ fontSize: 13, color: "#64748b" }}>
                      No drill-path summary data is available yet. Upload data or enter manual footage for a billing estimate.
                    </div>
                  )}
                </ShellCard>
              </div>
            </Section>

            <div style={{ display: "grid", gap: 18 }}>
              <Section title="5. Pricing / Crews / Exceptions" subtitle="Real billing controls using actual footage plus editable exception costs.">
                <div style={{ display: "grid", gap: 14 }}>
                  <ShellCard
                    title="Footage calculator"
                    description="Uses summed redline segment lengths first, then covered_length_ft from the backend. Manual footage is optional when no backend value is available."
                  >
                    <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: 12 }}>
                      <label style={{ display: "grid", gap: 6, fontSize: 13, color: "#475569" }}>
                        <span>Detected footage (FT)</span>
                        <input value={formatNumber(calculatedCoveredFootage)} readOnly style={{ borderRadius: 12, border: "1px solid #cfd8e3", padding: "10px 12px", background: "#f8fafc", fontSize: 14 }} />
                      </label>
                      <label style={{ display: "grid", gap: 6, fontSize: 13, color: "#475569" }}>
                        <span>Manual footage override (FT)</span>
                        <input value={manualFootage} onChange={(e) => setManualFootage(e.target.value)} placeholder="Optional" style={{ borderRadius: 12, border: "1px solid #cfd8e3", padding: "10px 12px", background: "#ffffff", fontSize: 14 }} />
                      </label>
                      <label style={{ display: "grid", gap: 6, fontSize: 13, color: "#475569" }}>
                        <span>Cost per foot ($)</span>
                        <input value={costPerFoot} onChange={(e) => setCostPerFoot(e.target.value)} style={{ borderRadius: 12, border: "1px solid #cfd8e3", padding: "10px 12px", background: "#ffffff", fontSize: 14 }} />
                      </label>
                      <div style={{ display: "grid", gap: 6, fontSize: 13, color: "#475569" }}>
                        <span>Base total</span>
                        <div style={{ borderRadius: 12, border: "1px solid #cfd8e3", padding: "10px 12px", background: "#f8fafc", fontSize: 14, fontWeight: 800 }}>{toMoney(baseBillingTotal)}</div>
                      </div>
                    </div>
                  </ShellCard>

                  <ShellCard
                    title="Exceptions"
                    description="Add or remove manual cost rows for TXDOT, railroad, restoration, and other job-specific charges."
                  >
                    <div style={{ display: "grid", gap: 10 }}>
                      {exceptions.map((item) => (
                        <div key={item.id} style={{ display: "grid", gridTemplateColumns: "1.4fr 0.8fr auto", gap: 10, alignItems: "center" }}>
                          <input value={item.label} onChange={(e) => handleExceptionChange(item.id, "label", e.target.value)} style={{ borderRadius: 12, border: "1px solid #cfd8e3", padding: "10px 12px", background: "#ffffff", fontSize: 14 }} />
                          <input value={item.amount} onChange={(e) => handleExceptionChange(item.id, "amount", e.target.value)} placeholder="0.00" style={{ borderRadius: 12, border: "1px solid #cfd8e3", padding: "10px 12px", background: "#ffffff", fontSize: 14 }} />
                          <button onClick={() => handleRemoveException(item.id)} style={buttonStyle("#ffffff", "#0f172a", "#000000", false)}>Remove</button>
                        </div>
                      ))}
                      <div style={{ display: "grid", gridTemplateColumns: "1.4fr 0.8fr auto", gap: 10, alignItems: "center" }}>
                        <input value={extraExceptionLabel} onChange={(e) => setExtraExceptionLabel(e.target.value)} placeholder="Add exception label" style={{ borderRadius: 12, border: "1px solid #cfd8e3", padding: "10px 12px", background: "#ffffff", fontSize: 14 }} />
                        <input value={extraExceptionAmount} onChange={(e) => setExtraExceptionAmount(e.target.value)} placeholder="0.00" style={{ borderRadius: 12, border: "1px solid #cfd8e3", padding: "10px 12px", background: "#ffffff", fontSize: 14 }} />
                        <button onClick={handleAddException} style={buttonStyle("#0f172a", "#ffffff", "#000000", false)}>Add</button>
                      </div>
                    </div>
                  </ShellCard>

                  <ShellCard
                    title="Billing summary"
                    description="Usable billing totals built from current footage, cost per foot, and exception totals."
                  >
                    <SmallRow label="Footage used" value={`${formatNumber(effectiveFootage)} ft`} />
                    <SmallRow label="Cost / foot" value={toMoney(numericCostPerFoot)} />
                    <SmallRow label="Base total" value={toMoney(baseBillingTotal)} />
                    <SmallRow label="Exception total" value={toMoney(exceptionTotal)} />
                    <SmallRow label="Final total" value={toMoney(finalBillingTotal)} />
                  </ShellCard>
                </div>
              </Section>

              <Section title="6. Export / Print" subtitle="Real print/export via browser print with a clean report layout.">
                <div style={{ display: "grid", gap: 14 }}>
                  <ShellCard
                    title="Print / export report"
                    description="Use browser print to create a clean printed report or Save as PDF from the browser print dialog."
                  >
                    <button onClick={handlePrintReport} className="no-print" style={{ ...buttonStyle("#0f172a", "#ffffff", "#000000", false), width: "100%" }}>
                      Print / Export Report
                    </button>
                  </ShellCard>
                  <ShellCard
                    title="Operator notes"
                    description="Use this to capture what looked right or wrong during beta testing. Existing note submission behavior remains intact."
                  >
                    <textarea
                      value={notes}
                      onChange={(e) => setNotes(e.target.value)}
                      placeholder="Example: Route looked right but station spacing seemed compressed near sheet 14..."
                      style={{ width: "100%", minHeight: 140, borderRadius: 14, border: "1px solid #cfd8e3", padding: 12, outline: "none", resize: "vertical", fontSize: 14, background: "#ffffff" }}
                    />
                    <button
                      onClick={submitBugNote}
                      disabled={busy || !notes.trim()}
                      style={{ ...buttonStyle("#0f172a", "#ffffff", "#0f172a", busy || !notes.trim()), marginTop: 12, width: "100%" }}
                    >
                      Submit Operator Note
                    </button>
                  </ShellCard>
                </div>
              </Section>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
