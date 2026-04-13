from __future__ import annotations

import base64
import heapq
import io
import json
import math
import os
import re
import zipfile
import xml.etree.ElementTree as ET
from collections import defaultdict
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import FastAPI, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from PIL import Image, ExifTags
import pytesseract

app = FastAPI(title="OSP Redlining Software API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
OUTPUTS_DIR = os.path.join(PROJECT_ROOT, "outputs")
os.makedirs(OUTPUTS_DIR, exist_ok=True)
STATE_PATH = os.path.join(OUTPUTS_DIR, "phase53_state.json")
MATERIAL_RATE = 3.50
SPLICING_RATE = 1.50
NODE_TOLERANCE_FT = 35.0
SNAP_MAX_DISTANCE_FT = 250.0
MAX_REDLINE_GAP_FT = 650.0
MAX_REDLINE_LENGTH_FT = 900.0
MAX_PATH_STRETCH_RATIO = 2.0
MIN_REDLINE_LENGTH_FT = 15.0

STATE: Dict[str, Any] = {
    "route_name": None,
    "route_coords": [],
    "route_length_ft": 0.0,
    "design_segments": [],
    "design_summary": {},
    "graph_nodes": [],
    "photo_points": [],
    "redline_records": [],
    "billing_report_rows": [],
    "status_summary": {},
    "design_filters": {},
}


DESIGN_TYPE_COLORS = {
    "backbone": "#2563EB",
    "lateral": "#60A5FA",
    "terminal_tail": "#A78BFA",
    "vacant_conduit": "#F59E0B",
    "new_conduit": "#14B8A6",
    "fiber_path": "#10B981",
    "rod_rope": "#8B5CF6",
    "strand": "#0EA5E9",
    "splice": "#F97316",
    "structure": "#6B7280",
    "other": "#94A3B8",
}

STATUS_COLORS = {
    "planned": "#5B8FF9",
    "in_progress": "#F59E0B",
    "completed": "#10B981",
    "redlined": "#EF4444",
}

NETWORK_TYPES = {"backbone", "lateral", "terminal_tail", "vacant_conduit", "new_conduit", "fiber_path", "strand", "rod_rope"}
REDLINE_ELIGIBLE_TYPES = {"backbone", "lateral", "terminal_tail", "new_conduit", "fiber_path"}


# ---------- persistence ----------

def _save_state() -> None:
    try:
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(STATE, f, indent=2)
    except Exception:
        pass


def _load_state() -> None:
    if not os.path.exists(STATE_PATH):
        return
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if isinstance(payload, dict):
            STATE.update(payload)
    except Exception:
        pass


_load_state()


def _ok(**kwargs: Any) -> JSONResponse:
    return JSONResponse({"success": True, **kwargs})


def _err(message: str, status_code: int = 200, **kwargs: Any) -> JSONResponse:
    return JSONResponse({"success": False, "error": message, **kwargs}, status_code=status_code)


# ---------- geometry helpers ----------
def _haversine_feet(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r_m = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r_m * c * 3.28084


def _polyline_length_ft(points: Sequence[Sequence[float]]) -> float:
    total = 0.0
    for i in range(1, len(points)):
        total += _haversine_feet(points[i - 1][0], points[i - 1][1], points[i][0], points[i][1])
    return total


def _feet_per_degree(lat: float) -> Tuple[float, float]:
    feet_per_deg_lat = 364000.0
    feet_per_deg_lon = max(1.0, 364000.0 * math.cos(math.radians(lat)))
    return feet_per_deg_lat, feet_per_deg_lon


def _point_key(lat: float, lon: float, decimals: int = 6) -> str:
    return f"{round(lat, decimals):.{decimals}f}|{round(lon, decimals):.{decimals}f}"


def _point_key_tol(lat: float, lon: float, tol_ft: float = NODE_TOLERANCE_FT) -> str:
    # ~ 35 ft -> 4 decimals at this latitude, stable enough for local graphing
    decimals = 4 if tol_ft >= 25 else 5
    return _point_key(lat, lon, decimals=decimals)


def _nearest_point_on_polyline(points: List[List[float]], lat: float, lon: float) -> Optional[Dict[str, Any]]:
    if len(points) < 2:
        return None
    best: Optional[Dict[str, Any]] = None
    traveled = 0.0
    for idx in range(1, len(points)):
        a = points[idx - 1]
        b = points[idx]
        mean_lat = (a[0] + b[0]) / 2.0
        f_lat, f_lon = _feet_per_degree(mean_lat)
        bx = (b[1] - a[1]) * f_lon
        by = (b[0] - a[0]) * f_lat
        px = (lon - a[1]) * f_lon
        py = (lat - a[0]) * f_lat
        seg_sq = bx * bx + by * by
        if seg_sq <= 0:
            continue
        t = max(0.0, min(1.0, (px * bx + py * by) / seg_sq))
        proj_x = t * bx
        proj_y = t * by
        dx = px - proj_x
        dy = py - proj_y
        dist = math.hypot(dx, dy)
        proj_lon = a[1] + (proj_x / f_lon if f_lon else 0.0)
        proj_lat = a[0] + (proj_y / f_lat if f_lat else 0.0)
        seg_len = math.sqrt(seg_sq)
        candidate = {
            "lat": proj_lat,
            "lon": proj_lon,
            "distance_ft": dist,
            "offset_ft": traveled + seg_len * t,
            "segment_index": idx - 1,
            "t": t,
        }
        if best is None or dist < best["distance_ft"]:
            best = candidate
        traveled += seg_len
    return best


def _slice_polyline_between_offsets(points: List[List[float]], start_ft: float, end_ft: float) -> List[List[float]]:
    if len(points) < 2:
        return []
    lo, hi = sorted((max(0.0, start_ft), max(0.0, end_ft)))
    out: List[List[float]] = []
    traveled = 0.0
    for idx in range(1, len(points)):
        a = points[idx - 1]
        b = points[idx]
        seg_len = _haversine_feet(a[0], a[1], b[0], b[1])
        seg_start = traveled
        seg_end = traveled + seg_len
        if seg_end < lo:
            traveled = seg_end
            continue
        if seg_start > hi:
            break
        def interp(t: float) -> List[float]:
            return [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t]
        start_t = 0.0 if lo <= seg_start else (lo - seg_start) / seg_len if seg_len else 0.0
        end_t = 1.0 if hi >= seg_end else (hi - seg_start) / seg_len if seg_len else 1.0
        start_pt = interp(max(0.0, min(1.0, start_t)))
        end_pt = interp(max(0.0, min(1.0, end_t)))
        if not out or out[-1] != start_pt:
            out.append(start_pt)
        if start_t <= 0.0 and end_t >= 1.0:
            if out[-1] != b:
                out.append(b)
        elif out[-1] != end_pt:
            out.append(end_pt)
        traveled = seg_end
    return out


def _reverse_points(points: List[List[float]]) -> List[List[float]]:
    return [list(p) for p in reversed(points)]


def _join_polylines(parts: List[List[List[float]]]) -> List[List[float]]:
    out: List[List[float]] = []
    for part in parts:
        for pt in part:
            if not out or out[-1] != pt:
                out.append(pt)
    return out


# ---------- KML parsing ----------
def _extract_coordinate_candidates_from_text(text: str) -> List[Tuple[float, float]]:
    normalized = str(text or "")
    pattern = re.compile(r"(-?\d{1,3}\.\d{4,})[^\d-]+(-?\d{1,3}\.\d{4,})")
    found: List[Tuple[float, float]] = []
    for match in pattern.finditer(normalized):
        try:
            a = float(match.group(1))
            b = float(match.group(2))
        except Exception:
            continue
        for lat, lon in ((a, b), (b, a)):
            if 20 <= lat <= 40 and -110 <= lon <= -80:
                candidate = (round(lat, 7), round(lon, 7))
                if candidate not in found:
                    found.append(candidate)
    return found


def _make_preview_data_url(image: Image.Image, max_dim: int = 320) -> str:
    preview = image.copy()
    preview.thumbnail((max_dim, max_dim))
    buf = io.BytesIO()
    preview.save(buf, format="JPEG", quality=80)
    return f"data:image/jpeg;base64,{base64.b64encode(buf.getvalue()).decode('ascii')}"


def _extract_gps_from_exif(image: Image.Image) -> Optional[Tuple[float, float]]:
    try:
        exif = image.getexif()
    except Exception:
        exif = None
    if not exif:
        return None

    gps_info: Any = None
    try:
        if hasattr(exif, "get_ifd") and hasattr(ExifTags, "IFD"):
            gps_info = exif.get_ifd(ExifTags.IFD.GPSInfo)
    except Exception:
        gps_info = None

    if not gps_info:
        gps_tag = next((k for k, v in ExifTags.TAGS.items() if v == "GPSInfo"), None)
        gps_info = exif.get(gps_tag) if gps_tag is not None else None

    if not gps_info:
        return None

    gps_named: Dict[str, Any] = {}
    if hasattr(gps_info, "items"):
        items = gps_info.items()
    elif isinstance(gps_info, dict):
        items = gps_info.items()
    else:
        return None

    for key, value in items:
        gps_named[ExifTags.GPSTAGS.get(key, key)] = value

    def _ratio_to_float(part: Any) -> Optional[float]:
        try:
            if hasattr(part, "numerator") and hasattr(part, "denominator"):
                return float(part.numerator) / float(part.denominator)
            if isinstance(part, (tuple, list)) and len(part) == 2:
                return float(part[0]) / float(part[1])
            return float(part)
        except Exception:
            return None

    def _to_deg(value: Any) -> Optional[float]:
        if not isinstance(value, (tuple, list)) or len(value) < 3:
            return None
        d = _ratio_to_float(value[0])
        m = _ratio_to_float(value[1])
        s = _ratio_to_float(value[2])
        if d is None or m is None or s is None:
            return None
        return d + (m / 60.0) + (s / 3600.0)

    lat = _to_deg(gps_named.get("GPSLatitude"))
    lon = _to_deg(gps_named.get("GPSLongitude"))
    if lat is None or lon is None:
        return None
    if str(gps_named.get("GPSLatitudeRef", "N")).upper().startswith("S"):
        lat = -lat
    if str(gps_named.get("GPSLongitudeRef", "E")).upper().startswith("W"):
        lon = -lon
    return lat, lon


def _classify_design_type(name: str, description: str, style_url: str, length_ft: float) -> Tuple[str, str, bool]:
    blob = f"{name} {description} {style_url}".lower()
    if any(k in blob for k in ["splice", "slack/pull", "handhole", "hh", "cabinet", "flower pot", "terminal port"]):
        return ("splice" if "splice" in blob else "structure", "structure", False)
    if any(k in blob for k in ["vacant", "empty conduit"]):
        return ("vacant_conduit", "network", True)
    if any(k in blob for k in ["terminal tail", "tail"]):
        return ("terminal_tail", "network", True)
    if any(k in blob for k in ["backbone", "trunk", "mainline"]):
        return ("backbone", "network", True)
    if any(k in blob for k in ["lateral", "drop"]):
        return ("lateral", "network", True)
    if any(k in blob for k in ["rod", "rope"]):
        return ("rod_rope", "network", False)
    if "strand" in blob:
        return ("strand", "network", False)
    if any(k in blob for k in ["conduit", "dir. bore", "bore"]):
        return ("new_conduit", "network", True)
    if any(k in blob for k in ["fiber", "cable"]):
        return ("fiber_path", "network", True)
    if length_ft >= 350:
        return ("backbone", "network", True)
    if length_ft >= 120:
        return ("lateral", "network", True)
    return ("terminal_tail", "network", True)


def _parse_kmz_or_kml(file_bytes: bytes, filename: str) -> Dict[str, Any]:
    lower = filename.lower()
    if lower.endswith(".kmz"):
        with zipfile.ZipFile(io.BytesIO(file_bytes), "r") as zf:
            kml_name = next((name for name in zf.namelist() if name.lower().endswith(".kml")), None)
            if not kml_name:
                raise ValueError("No KML file was found inside the KMZ.")
            kml_data = zf.read(kml_name)
    elif lower.endswith(".kml"):
        kml_data = file_bytes
    else:
        raise ValueError("Design upload must be a KMZ or KML file.")

    root = ET.fromstring(kml_data)
    placemarks: List[Dict[str, Any]] = []
    ns_strip = lambda t: t.split("}")[-1].lower()
    for pm in root.iter():
        if ns_strip(pm.tag) != "placemark":
            continue
        name = ""
        description = ""
        style_url = ""
        coords_list: List[List[List[float]]] = []
        for child in pm.iter():
            tag = ns_strip(child.tag)
            if tag == "name" and child.text:
                name = child.text.strip()
            elif tag == "description" and child.text:
                description = child.text.strip()
            elif tag == "styleurl" and child.text:
                style_url = child.text.strip()
            elif tag == "coordinates" and child.text:
                coords: List[List[float]] = []
                for raw in child.text.strip().split():
                    parts = raw.split(",")
                    if len(parts) < 2:
                        continue
                    try:
                        lon = float(parts[0])
                        lat = float(parts[1])
                    except Exception:
                        continue
                    coords.append([lat, lon])
                if len(coords) >= 2:
                    coords_list.append(coords)
        for coords in coords_list:
            placemarks.append({
                "name": name,
                "description": description,
                "style_url": style_url,
                "coords": coords,
            })

    if not placemarks:
        raise ValueError("No route coordinates were found in the uploaded design.")

    # Detect obvious closed perimeter display geometry so it won't participate in redlining.
    design_segments: List[Dict[str, Any]] = []
    counts = defaultdict(int)
    feet = defaultdict(float)
    route_candidate_coords: List[List[List[float]]] = []

    bbox_lats = [pt[0] for pm in placemarks for pt in pm["coords"]]
    bbox_lons = [pt[1] for pm in placemarks for pt in pm["coords"]]
    min_lat, max_lat = min(bbox_lats), max(bbox_lats)
    min_lon, max_lon = min(bbox_lons), max(bbox_lons)

    for idx, pm in enumerate(placemarks, start=1):
        coords = pm["coords"]
        length_ft = _polyline_length_ft(coords)
        design_type, role, eligible = _classify_design_type(pm["name"], pm["description"], pm["style_url"], length_ft)
        start = coords[0]
        end = coords[-1]
        is_closed = _haversine_feet(start[0], start[1], end[0], end[1]) < NODE_TOLERANCE_FT
        lat_hits = sum(1 for lat, lon in coords if abs(lat - min_lat) < 0.0006 or abs(lat - max_lat) < 0.0006)
        lon_hits = sum(1 for lat, lon in coords if abs(lon - min_lon) < 0.0006 or abs(lon - max_lon) < 0.0006)
        edge_touch_ratio = (lat_hits + lon_hits) / max(1, len(coords) * 2)
        is_boundary = is_closed or (length_ft > 1200 and edge_touch_ratio > 0.35 and design_type in {"backbone", "lateral", "terminal_tail"})
        if is_boundary:
            eligible = False
            role = "boundary"
        seg = {
            "segment_id": f"SEG-{idx:04d}",
            "segment_type": design_type,
            "segment_role": role,
            "eligible_for_redline": eligible,
            "is_boundary": is_boundary,
            "label": pm["name"] or f"Segment {idx}",
            "coords": coords,
            "length_ft": round(length_ft, 2),
            "status": "planned",
            "display_color": DESIGN_TYPE_COLORS.get(design_type, DESIGN_TYPE_COLORS["other"]),
        }
        design_segments.append(seg)
        counts[design_type] += 1
        feet[design_type] += length_ft
        if role == "network" and eligible:
            route_candidate_coords.append(coords)

    if not route_candidate_coords:
        route_candidate_coords = [seg["coords"] for seg in design_segments if seg["segment_role"] == "network"]
    route_coords = max(route_candidate_coords, key=_polyline_length_ft)
    route_length_ft = sum(seg["length_ft"] for seg in design_segments if seg["segment_role"] == "network")

    graph_nodes = _refresh_graph_nodes_from_segments(design_segments)
    design_filters = {
        "available_types": sorted({seg["segment_type"] for seg in design_segments}),
        "eligible_types": sorted(REDLINE_ELIGIBLE_TYPES),
    }

    return {
        "route_name": os.path.basename(filename),
        "route_coords": route_coords,
        "route_length_ft": round(route_length_ft, 2),
        "design_segments": design_segments,
        "design_summary": {
            "counts": dict(counts),
            "feet": {k: round(v, 2) for k, v in feet.items()},
            "total_segments": len(design_segments),
        },
        "graph_nodes": graph_nodes,
        "design_filters": design_filters,
    }


# ---------- graph + snap ----------
def _build_graph(design_segments: List[Dict[str, Any]], eligible_only: bool = False) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, List[Tuple[str, float, str]]]]:
    nodes: Dict[str, Dict[str, Any]] = {}
    adjacency: Dict[str, List[Tuple[str, float, str]]] = defaultdict(list)
    for seg in design_segments:
        if eligible_only and not seg.get("eligible_for_redline"):
            continue
        if seg.get("segment_role") != "network":
            continue
        coords = seg["coords"]
        a = coords[0]
        b = coords[-1]
        akey = _point_key_tol(a[0], a[1])
        bkey = _point_key_tol(b[0], b[1])
        nodes.setdefault(akey, {"lat": a[0], "lon": a[1], "node_id": akey})
        nodes.setdefault(bkey, {"lat": b[0], "lon": b[1], "node_id": bkey})
        seg["start_node"] = akey
        seg["end_node"] = bkey
        length = float(seg.get("length_ft") or 0.0)
        adjacency[akey].append((bkey, length, seg["segment_id"]))
        adjacency[bkey].append((akey, length, seg["segment_id"]))
    return nodes, adjacency


def _shortest_path(adjacency: Dict[str, List[Tuple[str, float, str]]], start: str, end: str) -> Tuple[float, List[str], List[str]]:
    if start == end:
        return 0.0, [start], []
    heap = [(0.0, start)]
    dist = {start: 0.0}
    prev_node: Dict[str, str] = {}
    prev_seg: Dict[str, str] = {}
    while heap:
        cur_d, node = heapq.heappop(heap)
        if node == end:
            break
        if cur_d > dist.get(node, float("inf")):
            continue
        for nxt, weight, seg_id in adjacency.get(node, []):
            nd = cur_d + weight
            if nd < dist.get(nxt, float("inf")):
                dist[nxt] = nd
                prev_node[nxt] = node
                prev_seg[nxt] = seg_id
                heapq.heappush(heap, (nd, nxt))
    if end not in dist:
        return float("inf"), [], []
    nodes = [end]
    segs: List[str] = []
    cur = end
    while cur != start:
        segs.append(prev_seg[cur])
        cur = prev_node[cur]
        nodes.append(cur)
    nodes.reverse()
    segs.reverse()
    return dist[end], nodes, segs


def _segment_partial_to_endpoint(seg: Dict[str, Any], snap_offset_ft: float, endpoint: str) -> List[List[float]]:
    total_len = float(seg.get("length_ft") or _polyline_length_ft(seg["coords"]))
    if endpoint == "start":
        return _reverse_points(_slice_polyline_between_offsets(seg["coords"], 0.0, snap_offset_ft))
    return _slice_polyline_between_offsets(seg["coords"], snap_offset_ft, total_len)


def _segment_between_offsets(seg: Dict[str, Any], start_offset_ft: float, end_offset_ft: float) -> List[List[float]]:
    path = _slice_polyline_between_offsets(seg["coords"], start_offset_ft, end_offset_ft)
    return path if start_offset_ft <= end_offset_ft else _reverse_points(path)


def _snap_photo_points(photo_points: List[Dict[str, Any]], design_segments: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    snapped: List[Dict[str, Any]] = []
    route_coords = STATE.get("route_coords", []) or []
    for point in photo_points:
        lat = float(point["lat"])
        lon = float(point["lon"])
        best: Optional[Dict[str, Any]] = None
        for seg in design_segments:
            if not seg.get("eligible_for_redline"):
                continue
            nearest = _nearest_point_on_polyline(seg["coords"], lat, lon)
            if not nearest:
                continue
            candidate = {**nearest, "segment_id": seg["segment_id"], "segment_type": seg["segment_type"]}
            if best is None or candidate["distance_ft"] < best["distance_ft"]:
                best = candidate
        if best and best["distance_ft"] <= SNAP_MAX_DISTANCE_FT:
            confidence = max(0.0, min(1.0, 1.0 - (best["distance_ft"] / SNAP_MAX_DISTANCE_FT)))
            updated = dict(point)
            updated.update({
                "snapped_lat": round(best["lat"], 7),
                "snapped_lon": round(best["lon"], 7),
                "snapped_segment_id": best["segment_id"],
                "snapped_segment_type": best["segment_type"],
                "snap_offset_ft": round(best["distance_ft"], 2),
                "snap_confidence": round(confidence, 3),
                "segment_route_offset_ft": round(best["offset_ft"], 2),
            })
            route_nearest = _nearest_point_on_polyline(route_coords, best["lat"], best["lon"]) if route_coords else None
            updated["route_offset_ft"] = round(route_nearest["offset_ft"], 2) if route_nearest else 0.0
            snapped.append(updated)
    return snapped


def _build_redline_path(a: Dict[str, Any], b: Dict[str, Any], seg_lookup: Dict[str, Dict[str, Any]], adjacency: Dict[str, List[Tuple[str, float, str]]]) -> Tuple[List[List[float]], List[str]]:
    start_seg = seg_lookup.get(str(a.get("snapped_segment_id")))
    end_seg = seg_lookup.get(str(b.get("snapped_segment_id")))
    if not start_seg or not end_seg:
        return [], []
    if start_seg["segment_id"] == end_seg["segment_id"]:
        path = _segment_between_offsets(start_seg, float(a.get("segment_route_offset_ft") or 0.0), float(b.get("segment_route_offset_ft") or 0.0))
        return path, [start_seg["segment_id"]]

    candidates = []
    start_offset = float(a.get("segment_route_offset_ft") or 0.0)
    end_offset = float(b.get("segment_route_offset_ft") or 0.0)
    total_start = float(start_seg.get("length_ft") or 0.0)
    total_end = float(end_seg.get("length_ft") or 0.0)
    for s_ep, s_node, s_cost in [
        ("start", start_seg["start_node"], start_offset),
        ("end", start_seg["end_node"], max(0.0, total_start - start_offset)),
    ]:
        for e_ep, e_node, e_cost in [
            ("start", end_seg["start_node"], end_offset),
            ("end", end_seg["end_node"], max(0.0, total_end - end_offset)),
        ]:
            graph_cost, node_path, seg_path = _shortest_path(adjacency, s_node, e_node)
            total_cost = s_cost + graph_cost + e_cost
            candidates.append((total_cost, s_ep, e_ep, seg_path))
    if not candidates:
        return [], []
    _, s_ep, e_ep, seg_path = min(candidates, key=lambda x: x[0])

    parts: List[List[List[float]]] = []
    segment_ids: List[str] = [start_seg["segment_id"]]
    start_part = _segment_partial_to_endpoint(start_seg, start_offset, s_ep)
    if start_part:
        parts.append(start_part)

    for mid_seg_id in seg_path:
        if mid_seg_id not in segment_ids:
            segment_ids.append(mid_seg_id)
        mid_seg = seg_lookup[mid_seg_id]
        prev_pt = parts[-1][-1] if parts and parts[-1] else None
        coords = mid_seg["coords"]
        if prev_pt:
            d_start = _haversine_feet(prev_pt[0], prev_pt[1], coords[0][0], coords[0][1])
            d_end = _haversine_feet(prev_pt[0], prev_pt[1], coords[-1][0], coords[-1][1])
            coords = coords if d_start <= d_end else _reverse_points(coords)
        parts.append(coords)

    end_part = _segment_partial_to_endpoint(end_seg, end_offset, e_ep)
    if end_part:
        end_part = _reverse_points(end_part)
        parts.append(end_part)
    if end_seg["segment_id"] not in segment_ids:
        segment_ids.append(end_seg["segment_id"])

    path = _join_polylines(parts)
    return path, segment_ids


def _redline_reason(index: int) -> str:
    reasons = [
        "Utility conflict / obstacle avoidance",
        "Rock / bore depth adjustment",
        "Field deviation under review",
        "Alignment adjustment",
    ]
    return reasons[index % len(reasons)]


def _assign_segment_statuses(design_segments: List[Dict[str, Any]], snapped_points: List[Dict[str, Any]], redlines: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    evidence_counts: Dict[str, int] = defaultdict(int)
    redline_segment_ids = set()
    completion_lengths: Dict[str, float] = defaultdict(float)
    for p in snapped_points:
        evidence_counts[str(p.get("snapped_segment_id") or "")] += 1
    for red in redlines:
        for sid in red.get("segment_ids", []):
            redline_segment_ids.add(str(sid))
            completion_lengths[str(sid)] += float(red.get("length_ft") or 0.0) / max(1, len(red.get("segment_ids", [])))

    counts = defaultdict(int)
    feet = defaultdict(float)
    updated_segments: List[Dict[str, Any]] = []
    for seg in design_segments:
        updated = dict(seg)
        sid = seg["segment_id"]
        status = "planned"
        evidence = evidence_counts.get(sid, 0)
        coverage_ratio = min(1.0, completion_lengths.get(sid, 0.0) / max(1.0, float(seg.get("length_ft") or 1.0)))
        if sid in redline_segment_ids:
            status = "redlined"
        elif coverage_ratio >= 0.8:
            status = "completed"
        elif evidence > 0 or coverage_ratio > 0.05:
            status = "in_progress"
        updated["status"] = status
        updated_segments.append(updated)
        counts[status] += 1
        feet[status] += float(seg.get("length_ft") or 0.0)
    return updated_segments, {"counts": dict(counts), "feet": {k: round(v, 2) for k, v in feet.items()}}


def _refresh_graph_nodes_from_segments(design_segments: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    node_counts: Dict[str, int] = defaultdict(int)
    coord_lookup: Dict[str, Tuple[float, float]] = {}
    for seg in design_segments:
        if seg.get("segment_role") != "network":
            continue
        for lat, lon in (seg["coords"][0], seg["coords"][-1]):
            key = _point_key_tol(lat, lon)
            coord_lookup[key] = (lat, lon)
            node_counts[key] += 1
    graph_nodes: List[Dict[str, Any]] = []
    for idx, (key, count) in enumerate(node_counts.items(), start=1):
        lat, lon = coord_lookup[key]
        graph_nodes.append({"node_id": f"NODE-{idx:04d}", "lat": lat, "lon": lon, "degree_hint": count})
    return graph_nodes


def _generate_real_redlines() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any], List[Dict[str, Any]]]:
    design_segments = STATE.get("design_segments", []) or []
    photo_points = STATE.get("photo_points", []) or []
    if not design_segments:
        raise ValueError("Upload the KMZ/KML design first.")
    if len(photo_points) < 2:
        raise ValueError("Upload at least two GPS-enabled field photos first.")

    seg_lookup = {seg["segment_id"]: seg for seg in design_segments}
    _, adjacency = _build_graph(design_segments, eligible_only=True)
    snapped_points = _snap_photo_points(photo_points, design_segments)
    snapped_points = sorted(snapped_points, key=lambda p: float(p.get("route_offset_ft") or 0.0))
    if len(snapped_points) < 2:
        raise ValueError("Could not snap enough field photos onto eligible design segments.")
    STATE["photo_points"] = snapped_points

    redlines: List[Dict[str, Any]] = []
    billing_rows: List[Dict[str, Any]] = []
    rl_index = 1
    for idx in range(1, len(snapped_points)):
        a = snapped_points[idx - 1]
        b = snapped_points[idx]
        start_ft = float(a.get("route_offset_ft") or 0.0)
        end_ft = float(b.get("route_offset_ft") or 0.0)
        station_gap_ft = abs(end_ft - start_ft)
        if station_gap_ft < MIN_REDLINE_LENGTH_FT:
            continue
        # Redlines must be station-bounded local deviations, not long project traces.
        if station_gap_ft > MAX_REDLINE_GAP_FT:
            continue
        path, segment_ids = _build_redline_path(a, b, seg_lookup, adjacency)
        if len(path) < 2:
            continue
        if any(seg_lookup[sid].get("is_boundary") for sid in segment_ids if sid in seg_lookup):
            continue
        if any(not seg_lookup[sid].get("eligible_for_redline") for sid in segment_ids if sid in seg_lookup):
            continue
        length_ft = _polyline_length_ft(path)
        if length_ft < MIN_REDLINE_LENGTH_FT:
            continue
        # Prevent route-trace artifacts that run far beyond the start/end station span.
        if length_ft > MAX_REDLINE_LENGTH_FT:
            continue
        if length_ft > (station_gap_ft * MAX_PATH_STRETCH_RATIO) + 40.0:
            continue
        lo_ft, hi_ft = sorted((start_ft, end_ft))
        redline_id = f"RL-{rl_index:03d}"
        rl_index += 1
        rec = {
            "redline_id": redline_id,
            "start_station": _feet_to_station_text(lo_ft),
            "end_station": _feet_to_station_text(hi_ft),
            "start_station_ft": round(lo_ft, 2),
            "end_station_ft": round(hi_ft, 2),
            "length_ft": round(length_ft, 2),
            "reason": _redline_reason(idx - 1),
            "coords": path,
            "segment_ids": sorted(set(segment_ids)),
        }
        redlines.append(rec)
        material = round(length_ft * MATERIAL_RATE, 2)
        splicing = round(length_ft * SPLICING_RATE, 2)
        billing_rows.append({
            "selected": False,
            "redline_id": redline_id,
            "start_station": rec["start_station"],
            "end_station": rec["end_station"],
            "length_ft": round(length_ft, 2),
            "reason": rec["reason"],
            "material_cost": material,
            "splicing_cost": splicing,
            "total_cost": round(material + splicing, 2),
        })

    design_segments, status_summary = _assign_segment_statuses(design_segments, snapped_points, redlines)
    graph_nodes = _refresh_graph_nodes_from_segments(design_segments)
    return redlines, billing_rows, design_segments, status_summary, graph_nodes


# ---------- API ----------
def _summary_payload() -> Dict[str, Any]:
    return {
        "route_name": STATE.get("route_name"),
        "route_coords": STATE.get("route_coords", []),
        "route_length_ft": float(STATE.get("route_length_ft", 0.0) or 0.0),
        "design_segments": STATE.get("design_segments", []),
        "design_summary": STATE.get("design_summary", {}),
        "graph_nodes": STATE.get("graph_nodes", []),
        "photo_points": STATE.get("photo_points", []),
        "redline_records": STATE.get("redline_records", []),
        "billing_report_rows": STATE.get("billing_report_rows", []),
        "status_summary": STATE.get("status_summary", {}),
        "design_filters": STATE.get("design_filters", {}),
    }


@app.get("/api/health")
def health() -> Dict[str, Any]:
    return {"ok": True}


@app.post("/api/reset-demo")
def reset_demo() -> JSONResponse:
    STATE.clear()
    STATE.update({
        "route_name": None,
        "route_coords": [],
        "route_length_ft": 0.0,
        "design_segments": [],
        "design_summary": {},
        "graph_nodes": [],
        "photo_points": [],
        "redline_records": [],
        "billing_report_rows": [],
        "status_summary": {},
        "design_filters": {},
    })
    _save_state()
    return _ok(reset=True)


@app.post("/api/upload-design")
async def upload_design(file: UploadFile = File(...)) -> JSONResponse:
    try:
        parsed = _parse_kmz_or_kml(await file.read(), file.filename or "design.kmz")
        STATE.update(parsed)
        STATE["photo_points"] = []
        STATE["redline_records"] = []
        STATE["billing_report_rows"] = []
        STATE["status_summary"] = {"counts": {"planned": len(parsed["design_segments"])}, "feet": {"planned": parsed["route_length_ft"]}}
        _save_state()
        return _ok(**parsed)
    except Exception as exc:
        return _err(str(exc))


@app.post("/api/upload-photo-points")
async def upload_photo_points(files: List[UploadFile] = File(...)) -> JSONResponse:
    try:
        if not (STATE.get("design_segments") or []):
            return _err("Upload the KMZ/KML design first.")
        uploaded_points: List[Dict[str, Any]] = []
        for upload in files:
            payload = await upload.read()
            if not payload:
                continue
            image = Image.open(io.BytesIO(payload)).convert("RGB")
            gps = _extract_gps_from_exif(image)
            ocr_text = ""
            if not gps:
                try:
                    ocr_text = pytesseract.image_to_string(image)
                    coords = _extract_coordinate_candidates_from_text(ocr_text)
                    gps = coords[0] if coords else None
                except Exception:
                    gps = None
                    ocr_text = ""
            if not gps:
                continue
            lat, lon = gps
            preview_url = _make_preview_data_url(image)
            uploaded_points.append({
                "filename": str(upload.filename or "photo"),
                "lat": lat,
                "lon": lon,
                "caption": "Field evidence photo",
                "preview_url": preview_url,
                "ocr_text": ocr_text[:500],
            })
        STATE["photo_points"] = uploaded_points
        _save_state()
        return _ok(photo_points_added=len(uploaded_points), photo_points=uploaded_points)
    except Exception as exc:
        return _err(f"Could not load photo points: {exc}")


@app.post("/api/generate-real-redlines")
def generate_real_redlines() -> JSONResponse:
    try:
        redlines, billing_rows, design_segments, status_summary, graph_nodes = _generate_real_redlines()
        STATE["redline_records"] = redlines
        STATE["billing_report_rows"] = billing_rows
        STATE["design_segments"] = design_segments
        STATE["status_summary"] = status_summary
        STATE["graph_nodes"] = graph_nodes
        _save_state()
        return _ok(redline_count=len(redlines), report_rows=billing_rows, status_summary=status_summary, photo_points=STATE["photo_points"])
    except Exception as exc:
        return _err(str(exc))


@app.get("/api/current-state")
def current_state() -> Dict[str, Any]:
    return _summary_payload()


def _feet_to_station_text(value: Optional[float]) -> str:
    if value is None:
        return ""
    total = int(round(float(value)))
    return f"{total // 100:02d}+{total % 100:02d}"
