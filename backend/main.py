
from __future__ import annotations

import hashlib
import io
import json
import math
import os
import shutil
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd
from fastapi import Body, FastAPI, File, Form, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

BASE_DIR = Path(__file__).resolve().parent
UPLOADS_DIR = BASE_DIR / "uploads"
UPLOADS_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="OSP Redlining Mapping Layer")
app.mount("/uploads", StaticFiles(directory=str(UPLOADS_DIR)), name="uploads")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

KML_NS = {
    "kml": "http://www.opengis.net/kml/2.2",
    "gx": "http://www.google.com/kml/ext/2.2",
}

MAX_BUG_REPORTS = 200

STATE: Dict[str, Any] = {
    "route_name": None,
    "route_id": None,
    "route_coords": [],
    "route_length_ft": 0.0,
    "route_catalog": [],
    "map_points": [],
    "committed_rows": [],
    "station_points": [],
    "redline_segments": [],
    "loaded_field_data_files": 0,
    "latest_structured_file": None,
    "station_mapping_mode": None,
    "station_mapping_min_ft": None,
    "station_mapping_max_ft": None,
    "station_mapping_range_ft": None,
    "selected_route_match": None,
    "route_match_candidates": [],
    "verification_summary": {},
    "kmz_reference": {
        "folder_summary": [],
        "line_role_summary": [],
        "point_role_summary": [],
        "line_layers": [],
        "explicit_redline_layers": [],
        "visual_reference": {},
        "line_features": [],
        "polygon_features": [],
        "point_features": [],
    },
    "bug_reports": [],
    "matching_debug": [],
}


def _reset_workspace_state() -> None:
    preserved_bug_reports = list(STATE.get("bug_reports", []) or [])
    STATE.clear()
    STATE.update(
        {
            "route_name": None,
            "route_id": None,
            "route_coords": [],
            "route_length_ft": 0.0,
            "route_catalog": [],
            "map_points": [],
            "committed_rows": [],
            "station_points": [],
            "redline_segments": [],
            "loaded_field_data_files": 0,
            "latest_structured_file": None,
            "station_mapping_mode": None,
            "station_mapping_min_ft": None,
            "station_mapping_max_ft": None,
            "station_mapping_range_ft": None,
            "selected_route_match": None,
            "route_match_candidates": [],
            "verification_summary": {},
            "kmz_reference": {
                "folder_summary": [],
                "line_role_summary": [],
                "point_role_summary": [],
                "line_layers": [],
                "explicit_redline_layers": [],
                "visual_reference": {},
                "line_features": [],
                "polygon_features": [],
                "point_features": [],
            },
            "bug_reports": preserved_bug_reports,
            "matching_debug": [],
        }
    )


CURRENT_PACKET_PRINT_SHEET_INDEX: Dict[str, Dict[str, Any]] = {
    # Calibrated from the detailed engineering sheets in the 07-15-25 Brenham Phase 5 design set.
    # The new Fieldwire report becomes useful starting at its page 24 because that is where the
    # embedded engineering plan pages begin showing street-level route geometry, matchlines, and
    # sheet continuity. We use those plan sheets as the print-to-street truth layer.
    #
    # Route-id calibration against the current KMZ underground-cable lines:
    # route_476 -> E Stone St corridor
    # route_477 -> E Tom Green St corridor
    # route_478 -> E Mansfield St corridor
    # route_479 / route_480 -> Niebuhr St corridor
    # route_475 -> Glenda Blvd corridor
    "1": {"sheet": 1, "streets": ["E STONE ST"], "route_ids": ["route_476"]},
    "2": {"sheet": 2, "streets": ["E STONE ST"], "route_ids": ["route_476"]},
    "3": {"sheet": 3, "streets": ["E STONE ST"], "route_ids": ["route_476"]},
    "4": {"sheet": 4, "streets": ["E STONE ST", "NIEBUHR ST"], "route_ids": ["route_476", "route_479"]},
    "5": {"sheet": 5, "streets": ["NIEBUHR ST"], "route_ids": ["route_479", "route_480"]},
    "6": {"sheet": 6, "streets": ["NIEBUHR ST"], "route_ids": ["route_479", "route_480"]},
    # For the paired 7,15 bore-log context the design truth is the E Stone St corridor.
    "7": {"sheet": 7, "streets": ["E STONE ST"], "route_ids": ["route_476"]},
    "8": {"sheet": 8, "streets": ["E MANSFIELD ST"], "route_ids": ["route_478"]},
    "9": {"sheet": 9, "streets": ["E TOM GREEN ST"], "route_ids": ["route_477"]},
    "10": {"sheet": 10, "streets": ["E TOM GREEN ST"], "route_ids": ["route_477"]},
    "11": {"sheet": 11, "streets": ["E TOM GREEN ST"], "route_ids": ["route_477"]},
    "12": {"sheet": 12, "streets": ["E TOM GREEN ST"], "route_ids": ["route_477"]},
    "13": {"sheet": 13, "streets": ["E TOM GREEN ST", "BRUCE ST"], "route_ids": ["route_477"]},
    "14": {"sheet": 14, "streets": ["E MANSFIELD ST"], "route_ids": ["route_478"]},
    "15": {"sheet": 15, "streets": ["E STONE ST"], "route_ids": ["route_476"]},
    "16": {"sheet": 16, "streets": ["NIEBUHR ST"], "route_ids": ["route_479", "route_480"]},
    "17": {"sheet": 17, "streets": ["NIEBUHR ST"], "route_ids": ["route_479", "route_480"]},
    "18": {"sheet": 18, "streets": ["NIEBUHR ST", "E TOM GREEN ST"], "route_ids": ["route_477", "route_479", "route_480"]},
    "19": {"sheet": 19, "streets": ["NIEBUHR ST"], "route_ids": ["route_479", "route_480"]},
    "20": {"sheet": 20, "streets": ["NIEBUHR ST"], "route_ids": ["route_479", "route_480"]},
    "21": {"sheet": 21, "streets": ["NIEBUHR ST"], "route_ids": ["route_479", "route_480"]},
    "22": {"sheet": 22, "streets": ["NIEBUHR ST", "E TOM GREEN ST"], "route_ids": ["route_477", "route_479", "route_480"]},
    "23": {"sheet": 23, "streets": ["CARLEE DR"], "route_ids": ["route_478"]},
    "24": {"sheet": 24, "streets": ["POST OAK CT"], "route_ids": ["route_478"]},
    "25": {"sheet": 25, "streets": ["GLENDA BLVD"], "route_ids": ["route_475"]},
    "26": {"sheet": 26, "streets": ["GLENDA BLVD"], "route_ids": ["route_475"]},
    "27": {"sheet": 27, "streets": ["GLENDA BLVD"], "route_ids": ["route_475"]},
    "28": {"sheet": 28, "streets": ["GLENDA BLVD"], "route_ids": ["route_475"]},
    "29": {"sheet": 29, "streets": ["GLENDA BLVD"], "route_ids": ["route_475"]},
    "30": {"sheet": 30, "streets": ["E STONE ST"], "route_ids": ["route_476"]},
}

def _print_sheet_hints(print_tokens: Sequence[str]) -> Dict[str, Any]:
    tokens = [str(token).strip() for token in print_tokens if str(token).strip()]
    streets: List[str] = []
    sheet_numbers: List[int] = []
    route_ids: List[str] = []

    for token in tokens:
        entry = CURRENT_PACKET_PRINT_SHEET_INDEX.get(token)
        if not entry:
            continue
        sheet = entry.get("sheet")
        if isinstance(sheet, int) and sheet not in sheet_numbers:
            sheet_numbers.append(sheet)
        for street in entry.get("streets", []) or []:
            if street not in streets:
                streets.append(street)
        for route_id in entry.get("route_ids", []) or []:
            if route_id not in route_ids:
                route_ids.append(route_id)

    return {
        "print_tokens": tokens,
        "sheet_numbers": sheet_numbers,
        "street_hints": streets,
        "allowed_route_ids": route_ids,
    }




def _store_bug_report(report: Dict[str, Any]) -> Dict[str, Any]:
    reports = STATE.setdefault("bug_reports", [])
    fingerprint = str(report.get("fingerprint") or "").strip()
    if fingerprint:
        for existing in reports:
            if str(existing.get("fingerprint") or "").strip() == fingerprint:
                existing["count"] = int(existing.get("count") or 1) + 1
                existing["timestamp"] = report.get("timestamp") or existing.get("timestamp")
                if report.get("details") is not None:
                    existing["details"] = report.get("details")
                if report.get("context") is not None:
                    existing["context"] = report.get("context")
                return existing
    reports.insert(0, dict(report))
    del reports[MAX_BUG_REPORTS:]
    return report

def _ok(**kwargs: Any) -> JSONResponse:
    return JSONResponse({"success": True, **kwargs})


def _err(message: str, status_code: int = 200, **kwargs: Any) -> JSONResponse:
    return JSONResponse({"success": False, "error": message, **kwargs}, status_code=status_code)


def _safe_filename(value: Any) -> str:
    try:
        return str(value or "").strip()
    except Exception:
        return ""


def _haversine_feet(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r_m = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r_m * c * 3.28084


def _route_length_ft(coords: Sequence[Sequence[float]]) -> float:
    total = 0.0
    for i in range(1, len(coords)):
        total += _haversine_feet(
            float(coords[i - 1][0]),
            float(coords[i - 1][1]),
            float(coords[i][0]),
            float(coords[i][1]),
        )
    return total

def _route_bbox(coords: Sequence[Sequence[float]]) -> Optional[Dict[str, float]]:
    if not coords:
        return None
    lats = [float(pt[0]) for pt in coords if len(pt) >= 2]
    lons = [float(pt[1]) for pt in coords if len(pt) >= 2]
    if not lats or not lons:
        return None
    return {
        "min_lat": min(lats),
        "max_lat": max(lats),
        "min_lon": min(lons),
        "max_lon": max(lons),
    }


def _route_centroid(coords: Sequence[Sequence[float]]) -> Optional[Tuple[float, float]]:
    if not coords:
        return None
    pts = [(float(pt[0]), float(pt[1])) for pt in coords if len(pt) >= 2]
    if not pts:
        return None
    lat = sum(pt[0] for pt in pts) / len(pts)
    lon = sum(pt[1] for pt in pts) / len(pts)
    return (lat, lon)


def _bbox_contains_with_buffer(
    outer_bbox: Optional[Dict[str, float]],
    inner_bbox: Optional[Dict[str, float]],
    lat_buffer_deg: float,
    lon_buffer_deg: float,
) -> bool:
    if not outer_bbox or not inner_bbox:
        return True
    return (
        inner_bbox["max_lat"] >= outer_bbox["min_lat"] - lat_buffer_deg
        and inner_bbox["min_lat"] <= outer_bbox["max_lat"] + lat_buffer_deg
        and inner_bbox["max_lon"] >= outer_bbox["min_lon"] - lon_buffer_deg
        and inner_bbox["min_lon"] <= outer_bbox["max_lon"] + lon_buffer_deg
    )



def _build_route_chainage(coords: Sequence[Sequence[float]]) -> List[float]:
    if not coords:
        return []
    chainage = [0.0]
    running = 0.0
    for idx in range(1, len(coords)):
        prev = coords[idx - 1]
        curr = coords[idx]
        if len(prev) < 2 or len(curr) < 2:
            chainage.append(running)
            continue
        running += _haversine_feet(float(prev[0]), float(prev[1]), float(curr[0]), float(curr[1]))
        chainage.append(running)
    return chainage





def _densify_route_coords(coords: Sequence[Sequence[float]], step_ft: float = 60.0) -> List[List[float]]:
    if not coords:
        return []
    cleaned = _dedupe_consecutive(coords)
    if len(cleaned) < 2:
        return [list(cleaned[0])] if cleaned else []

    chainage = _build_route_chainage(cleaned)
    total_ft = float(chainage[-1] or 0.0)
    if total_ft <= 0.0:
        return cleaned

    step = max(15.0, float(step_ft))
    densified: List[List[float]] = [list(cleaned[0])]
    distance_ft = step
    while distance_ft < total_ft - 1e-6:
        interpolated = _interpolate_point_on_route(cleaned, chainage, distance_ft)
        if interpolated:
            point = [float(interpolated["lat"]), float(interpolated["lon"])]
            if abs(densified[-1][0] - point[0]) > 1e-9 or abs(densified[-1][1] - point[1]) > 1e-9:
                densified.append(point)
        distance_ft += step

    end_point = [float(cleaned[-1][0]), float(cleaned[-1][1])]
    if abs(densified[-1][0] - end_point[0]) > 1e-9 or abs(densified[-1][1] - end_point[1]) > 1e-9:
        densified.append(end_point)

    return densified

def _virtual_segment_chunks(chainage: Sequence[float], target_virtual_ft: float = 60.0) -> List[int]:
    if not chainage or len(chainage) < 2:
        return [1]
    chunks: List[int] = []
    for idx in range(1, len(chainage)):
        seg_len = max(0.0, float(chainage[idx]) - float(chainage[idx - 1]))
        chunk_count = max(1, int(math.ceil(seg_len / max(1.0, float(target_virtual_ft)))))
        chunks.append(chunk_count)
    return chunks


def _route_segment_denominator(route_coords: Sequence[Sequence[float]], chainage: Optional[Sequence[float]] = None) -> int:
    active_chainage = list(chainage) if chainage is not None else _build_route_chainage(route_coords)
    chunks = _virtual_segment_chunks(active_chainage)
    return max(1, sum(chunks))


def _virtualize_segment_index(chainage: Sequence[float], actual_segment_index: int, ratio: float) -> Dict[str, Any]:
    chunks = _virtual_segment_chunks(chainage)
    if not chunks:
        return {
            "virtual_segment_index": 0,
            "virtual_segment_ratio": max(0.0, min(1.0, float(ratio))),
            "virtual_segment_count": 1,
        }

    actual_index = max(0, min(int(actual_segment_index), len(chunks) - 1))
    bounded_ratio = max(0.0, min(1.0, float(ratio)))
    chunk_count = max(1, int(chunks[actual_index]))
    chunk_position = min(chunk_count - 1, int(math.floor(bounded_ratio * chunk_count)))
    local_start = chunk_position / chunk_count
    local_ratio_span = 1.0 / chunk_count
    local_ratio = 0.0 if local_ratio_span <= 0.0 else (bounded_ratio - local_start) / local_ratio_span
    local_ratio = max(0.0, min(1.0, local_ratio))
    virtual_index = sum(chunks[:actual_index]) + chunk_position

    return {
        "virtual_segment_index": int(virtual_index),
        "virtual_segment_ratio": float(local_ratio),
        "virtual_segment_count": int(sum(chunks)),
    }


def _interpolate_point_on_route(coords: Sequence[Sequence[float]], chainage: Sequence[float], target_ft: float) -> Optional[Dict[str, Any]]:
    if not coords or not chainage or len(coords) != len(chainage):
        return None
    if len(coords) == 1:
        return {
            "lat": float(coords[0][0]),
            "lon": float(coords[0][1]),
            "segment_index": 0,
            "segment_ratio": 0.0,
            "actual_segment_index": 0,
            "actual_segment_ratio": 0.0,
            "virtual_segment_count": 1,
            "target_ft": round(float(target_ft), 2),
        }

    total_ft = float(chainage[-1] or 0.0)
    target = max(0.0, min(float(target_ft), total_ft))

    for idx in range(1, len(chainage)):
        start_ft = float(chainage[idx - 1])
        end_ft = float(chainage[idx])
        if target <= end_ft or idx == len(chainage) - 1:
            start_pt = coords[idx - 1]
            end_pt = coords[idx]
            span = max(end_ft - start_ft, 1e-9)
            ratio = max(0.0, min(1.0, (target - start_ft) / span))
            lat = float(start_pt[0]) + (float(end_pt[0]) - float(start_pt[0])) * ratio
            lon = float(start_pt[1]) + (float(end_pt[1]) - float(start_pt[1])) * ratio
            virtual_meta = _virtualize_segment_index(chainage, idx - 1, ratio)
            return {
                "lat": lat,
                "lon": lon,
                "segment_index": int(virtual_meta["virtual_segment_index"]),
                "segment_ratio": float(virtual_meta["virtual_segment_ratio"]),
                "actual_segment_index": idx - 1,
                "actual_segment_ratio": ratio,
                "virtual_segment_count": int(virtual_meta["virtual_segment_count"]),
                "target_ft": round(target, 2),
            }

    last = coords[-1]
    last_actual_index = max(0, len(coords) - 2)
    virtual_meta = _virtualize_segment_index(chainage, last_actual_index, 1.0)
    return {
        "lat": float(last[0]),
        "lon": float(last[1]),
        "segment_index": int(virtual_meta["virtual_segment_index"]),
        "segment_ratio": float(virtual_meta["virtual_segment_ratio"]),
        "actual_segment_index": last_actual_index,
        "actual_segment_ratio": 1.0,
        "virtual_segment_count": int(virtual_meta["virtual_segment_count"]),
        "target_ft": round(target, 2),
    }


def _generate_segment_windows(route_coords: Sequence[Sequence[float]], span_ft: float) -> List[Dict[str, Any]]:
    chainage = _build_route_chainage(route_coords)
    if not chainage:
        return []

    total_ft = float(chainage[-1] or 0.0)
    if total_ft <= 0.0:
        return []

    span = max(1.0, float(span_ft or 0.0))
    if total_ft <= span:
        return [{
            "start_ft": 0.0,
            "end_ft": total_ft,
            "window_type": "full_route_window",
            "chainage": chainage,
        }]

    windows = []
    seen = set()

    def add_window(start_ft: float, end_ft: float, window_type: str) -> None:
        start_val = max(0.0, min(float(start_ft), total_ft))
        end_val = max(start_val, min(float(end_ft), total_ft))
        key = (round(start_val, 2), round(end_val, 2), window_type)
        if key in seen:
            return
        seen.add(key)
        windows.append({
            "start_ft": round(start_val, 2),
            "end_ft": round(end_val, 2),
            "window_type": window_type,
            "chainage": chainage,
        })

    coarse_step = max(10.0, min(40.0, span / 8.0))
    fine_step = max(5.0, min(20.0, span / 16.0))

    current = 0.0
    while current + span <= total_ft + 1e-6:
        add_window(current, current + span, "coarse_window")
        current += coarse_step

    current = 0.0
    while current + span <= total_ft + 1e-6:
        add_window(current, current + span, "fine_window")
        current += fine_step

    add_window(0.0, span, "origin_window")
    add_window(max(0.0, total_ft - span), total_ft, "tail_window")
    add_window(max(0.0, (total_ft - span) / 2.0), min(total_ft, (total_ft - span) / 2.0 + span), "mid_window")

    for vertex_ft in chainage:
        add_window(vertex_ft, vertex_ft + span, "vertex_forward")
        add_window(vertex_ft - span, vertex_ft, "vertex_backward")

    return windows


def _score_segment_window(
    route_coords: Sequence[Sequence[float]],
    normalized_group: Dict[str, Any],
    window: Dict[str, Any],
) -> Dict[str, Any]:
    chainage = window.get("chainage") or _build_route_chainage(route_coords)
    if not chainage:
        return {
            "window_score": 0.0,
            "window_reasons": ["no_chainage"],
            "window_profile": {"projected_points": []},
            "mapping": _resolve_station_mapping(normalized_group.get("station_rows") or [], 0.0),
        }

    start_ft = float(window.get("start_ft") or 0.0)
    end_ft = float(window.get("end_ft") or start_ft)
    span_ft = max(1.0, float(normalized_group.get("span_ft") or 0.0))
    segment_length_ft = max(0.0, end_ft - start_ft)

    mapping = _resolve_station_mapping(normalized_group.get("station_rows") or [], float(chainage[-1]))
    mapping["anchor_offset_ft"] = round(start_ft, 2)
    mapping["anchored_start_ft"] = round(start_ft, 2)
    mapping["anchored_end_ft"] = round(end_ft, 2)
    mapping["anchor_strategy"] = "true_sliding_window_segment_scorer"

    projected_points = []
    covered_segments = []
    min_station = float(normalized_group.get("min_station_ft") or 0.0)

    for row in normalized_group.get("station_rows") or []:
        station_ft = float(row.get("station_ft") or 0.0)
        relative_ft = max(0.0, station_ft - min_station)
        route_ft = start_ft + relative_ft
        projected = _interpolate_point_on_route(route_coords, chainage, route_ft)
        if not projected:
            continue
        covered_segments.append(int(projected["segment_index"]))
        projected_points.append({
            "station_ft": round(station_ft, 2),
            "route_ft": round(route_ft, 2),
            "lat": round(float(projected["lat"]), 8),
            "lon": round(float(projected["lon"]), 8),
            "segment_index": int(projected["segment_index"]),
            "segment_ratio": round(float(projected["segment_ratio"]), 4),
            "actual_segment_index": int(projected.get("actual_segment_index", projected["segment_index"])),
            "actual_segment_ratio": round(float(projected.get("actual_segment_ratio", projected["segment_ratio"])), 4),
            "virtual_segment_count": int(projected.get("virtual_segment_count", 1)),
        })

    exact_span_fit = max(0.0, 1.0 - (abs(segment_length_ft - span_ft) / max(span_ft, 1.0)))
    segment_diversity = min(1.0, len(set(covered_segments)) / max(1, _route_segment_denominator(route_coords, chainage)))
    edge_clearance = min(start_ft, max(0.0, float(chainage[-1]) - end_ft)) / max(span_ft, 1.0)
    edge_fit = min(1.0, edge_clearance)

    shape_bonus = 0.08 if len(route_coords) >= 4 else 0.0

    window_score = exact_span_fit * 0.6 + segment_diversity * 0.2 + edge_fit * 0.12 + shape_bonus
    window_score = max(0.0, min(1.0, window_score))

    mapping["anchor_basis"] = {
        "window_type": window.get("window_type"),
        "window_start_ft": round(start_ft, 2),
        "window_end_ft": round(end_ft, 2),
        "segment_length_ft": round(segment_length_ft, 2),
        "exact_span_fit": round(exact_span_fit, 6),
        "segment_diversity": round(segment_diversity, 6),
        "edge_fit": round(edge_fit, 6),
    }

    return {
        "window_score": round(window_score, 6),
        "window_reasons": [
            f"segment_length_ft={round(segment_length_ft, 2)} vs span_ft={round(span_ft, 2)}",
            f"exact_span_fit={round(exact_span_fit, 4)}",
            f"segment_diversity={round(segment_diversity, 4)}",
            f"edge_fit={round(edge_fit, 4)}",
        ],
        "window_profile": {
            "window_type": window.get("window_type"),
            "start_ft": round(start_ft, 2),
            "end_ft": round(end_ft, 2),
            "segment_length_ft": round(segment_length_ft, 2),
            "projected_points": projected_points[:25],
            "unique_segments_covered": len(set(covered_segments)),
            "score_components": {
                "exact_span_fit": round(exact_span_fit, 6),
                "segment_diversity": round(segment_diversity, 6),
                "edge_fit": round(edge_fit, 6),
                "shape_bonus": round(shape_bonus, 6),
            },
        },
        "mapping": mapping,
    }


def _infer_group_spatial_context(normalized_group: Dict[str, Any]) -> Dict[str, Any]:
    inferred_points: List[Tuple[float, float]] = []

    for row in normalized_group.get("rows") or []:
        lat = row.get("lat")
        lon = row.get("lon")
        if lat is None or lon is None:
            continue
        try:
            inferred_points.append((float(lat), float(lon)))
        except Exception:
            continue

    if not inferred_points:
        return {
            "has_spatial_context": False,
            "point_count": 0,
            "bbox": None,
            "centroid": None,
            "lat_buffer_deg": 0.0,
            "lon_buffer_deg": 0.0,
        }

    lats = [pt[0] for pt in inferred_points]
    lons = [pt[1] for pt in inferred_points]
    bbox = {
        "min_lat": min(lats),
        "max_lat": max(lats),
        "min_lon": min(lons),
        "max_lon": max(lons),
    }
    centroid = (sum(lats) / len(lats), sum(lons) / len(lons))

    lat_span = max(0.0, bbox["max_lat"] - bbox["min_lat"])
    lon_span = max(0.0, bbox["max_lon"] - bbox["min_lon"])

    # About 150 ft minimum buffer, plus some extra slack for sparse field capture.
    lat_buffer_deg = max(0.00042, lat_span * 0.75)
    lon_buffer_deg = max(0.00052, lon_span * 0.75)

    return {
        "has_spatial_context": True,
        "point_count": len(inferred_points),
        "bbox": bbox,
        "centroid": centroid,
        "lat_buffer_deg": lat_buffer_deg,
        "lon_buffer_deg": lon_buffer_deg,
    }



def _normalize_station_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().upper()
    if not text:
        return None
    if "+" in text:
        left, right = text.split("+", 1)
        left = "".join(ch for ch in left if ch.isdigit())
        right = "".join(ch for ch in right if ch.isdigit())
        if not left or not right:
            return None
        return f"{int(left)}+{int(right):02d}"
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) < 3:
        return None
    return f"{int(digits[:-2])}+{int(digits[-2:]):02d}"


def _station_to_feet(value: Any) -> Optional[float]:
    normalized = _normalize_station_text(value)
    if not normalized:
        return None
    left, right = normalized.split("+", 1)
    return float(int(left) * 100 + int(right))


def _parse_coordinate_text(text: str) -> List[List[float]]:
    coords: List[List[float]] = []
    for raw in (text or "").strip().split():
        parts = raw.split(",")
        if len(parts) < 2:
            continue
        try:
            lon = float(parts[0])
            lat = float(parts[1])
        except Exception:
            continue
        coords.append([lat, lon])
    return coords


def _extract_kml_bytes(file_bytes: bytes, filename: str) -> bytes:
    lower = _safe_filename(filename).lower()
    if lower.endswith(".kml"):
        return file_bytes
    if lower.endswith(".kmz"):
        with zipfile.ZipFile(io.BytesIO(file_bytes), "r") as zf:
            kml_names = [name for name in zf.namelist() if name.lower().endswith(".kml")]
            if not kml_names:
                raise ValueError("No KML file found inside KMZ.")
            preferred = next((name for name in kml_names if name.lower().endswith("doc.kml")), kml_names[0])
            return zf.read(preferred)
    raise ValueError("Design upload must be .kmz or .kml")


def _dedupe_consecutive(coords: Sequence[Sequence[float]]) -> List[List[float]]:
    cleaned: List[List[float]] = []
    for pt in coords:
        lat = float(pt[0])
        lon = float(pt[1])
        if not cleaned or abs(cleaned[-1][0] - lat) > 1e-9 or abs(cleaned[-1][1] - lon) > 1e-9:
            cleaned.append([lat, lon])
    return cleaned


def _parent_map(root: ET.Element) -> Dict[int, ET.Element]:
    result: Dict[int, ET.Element] = {}
    for elem in root.iter():
        for child in elem:
            result[id(child)] = elem
    return result


def _folder_path(elem: ET.Element, parent_map: Dict[int, ET.Element]) -> List[str]:
    names: List[str] = []
    current = elem
    while id(current) in parent_map:
        current = parent_map[id(current)]
        tag = current.tag.split("}")[-1]
        if tag in {"Folder", "Document"}:
            name = (current.findtext("kml:name", default="", namespaces=KML_NS) or "").strip()
            if name:
                names.append(name)
    names.reverse()
    return names




def _infer_route_role(role_hint: str) -> str:
    role = "other"
    if "backbone" in role_hint:
        role = "backbone"
    elif "terminal" in role_hint and "tail" in role_hint:
        role = "terminal_tail"
    elif "house" in role_hint and "drop" in role_hint:
        role = "house_drop"
    elif "vacant" in role_hint:
        role = "vacant_pipe"
    elif "underground" in role_hint and "cable" in role_hint:
        role = "underground_cable"
    return role


def _polyline_color_for_role(role: str) -> str:
    palette = {
        "underground_cable": "#3b82f6",
        "terminal_tail": "#f59e0b",
        "backbone": "#22c55e",
        "house_drop": "#eab308",
        "vacant_pipe": "#84cc16",
        "other": "#10b981",
    }
    return palette.get(str(role or "other"), "#10b981")


def _polygon_style_for_role(role: str) -> Dict[str, Any]:
    if role == "underground_cable":
        return {"fill": "#22c55e", "fill_opacity": 0.24, "stroke": "#22c55e", "stroke_width": 2}
    if role == "terminal_tail":
        return {"fill": "#f59e0b", "fill_opacity": 0.12, "stroke": "#f59e0b", "stroke_width": 2}
    if role == "backbone":
        return {"fill": "#38bdf8", "fill_opacity": 0.10, "stroke": "#38bdf8", "stroke_width": 2}
    return {"fill": "#22c55e", "fill_opacity": 0.16, "stroke": "#22c55e", "stroke_width": 2}


def _extract_point_coords(text: str) -> Optional[List[float]]:
    coords = _parse_coordinate_text(text or "")
    if not coords:
        return None
    return [float(coords[0][0]), float(coords[0][1])]


def _build_kmz_reference(file_bytes: bytes, filename: str) -> Dict[str, Any]:
    kml_bytes = _extract_kml_bytes(file_bytes, filename)
    root = ET.fromstring(kml_bytes)
    parent_map = _parent_map(root)

    line_features: List[Dict[str, Any]] = []
    polygon_features: List[Dict[str, Any]] = []
    point_features: List[Dict[str, Any]] = []
    folder_summary: Dict[str, int] = {}
    line_role_summary: Dict[str, int] = {}
    point_role_summary: Dict[str, int] = {}

    feature_counter = 0

    for placemark in root.findall(".//kml:Placemark", KML_NS):
        placemark_name = (placemark.findtext("kml:name", default="", namespaces=KML_NS) or "").strip() or "Unnamed Feature"
        folder_names = _folder_path(placemark, parent_map)
        folder_path = " / ".join(folder_names[1:]) if len(folder_names) > 1 else (folder_names[0] if folder_names else "")
        role_hint = f"{folder_path} {placemark_name}".strip().lower()
        role = _infer_route_role(role_hint)

        folder_summary[folder_path or "root"] = folder_summary.get(folder_path or "root", 0) + 1

        line_nodes = placemark.findall(".//kml:LineString/kml:coordinates", KML_NS)
        for node in line_nodes:
            coords = _dedupe_consecutive(_parse_coordinate_text(node.text or ""))
            if len(coords) < 2:
                continue
            feature_counter += 1
            line_features.append(
                {
                    "feature_id": f"line_{feature_counter}",
                    "name": placemark_name,
                    "folder_path": folder_path,
                    "role": role,
                    "coords": coords,
                    "stroke": _polyline_color_for_role(role),
                    "stroke_width": 4 if role == "underground_cable" else 3,
                    "length_ft": round(_route_length_ft(coords), 2),
                }
            )
            line_role_summary[role] = line_role_summary.get(role, 0) + 1

        polygon_nodes = placemark.findall(".//kml:Polygon", KML_NS)
        for poly in polygon_nodes:
            outer = poly.find(".//kml:outerBoundaryIs/kml:LinearRing/kml:coordinates", KML_NS)
            if outer is None:
                continue
            coords = _dedupe_consecutive(_parse_coordinate_text(outer.text or ""))
            if len(coords) < 3:
                continue
            feature_counter += 1
            style = _polygon_style_for_role(role)
            polygon_features.append(
                {
                    "feature_id": f"polygon_{feature_counter}",
                    "name": placemark_name,
                    "folder_path": folder_path,
                    "role": role,
                    "coords": coords,
                    **style,
                }
            )

        point_nodes = placemark.findall(".//kml:Point/kml:coordinates", KML_NS)
        for point_node in point_nodes:
            point = _extract_point_coords(point_node.text or "")
            if not point:
                continue
            feature_counter += 1
            point_features.append(
                {
                    "feature_id": f"point_{feature_counter}",
                    "name": placemark_name,
                    "folder_path": folder_path,
                    "role": role,
                    "lat": point[0],
                    "lon": point[1],
                }
            )
            point_role_summary[role] = point_role_summary.get(role, 0) + 1

    line_layers = [
        {
            "layer_id": f"role::{role}",
            "label": role.replace("_", " ").title(),
            "role": role,
            "feature_count": count,
            "stroke": _polyline_color_for_role(role),
        }
        for role, count in sorted(line_role_summary.items(), key=lambda item: (-item[1], item[0]))
    ]

    visual_reference = {
        "design_bbox_hint": {},
        "has_polygons": bool(polygon_features),
        "has_lines": bool(line_features),
        "line_feature_count": len(line_features),
        "polygon_feature_count": len(polygon_features),
        "point_feature_count": len(point_features),
    }

    return {
        "folder_summary": [
            {"folder_path": folder, "feature_count": count}
            for folder, count in sorted(folder_summary.items(), key=lambda item: (-item[1], item[0]))
        ],
        "line_role_summary": [
            {"role": role, "feature_count": count}
            for role, count in sorted(line_role_summary.items(), key=lambda item: (-item[1], item[0]))
        ],
        "point_role_summary": [
            {"role": role, "feature_count": count}
            for role, count in sorted(point_role_summary.items(), key=lambda item: (-item[1], item[0]))
        ],
        "line_layers": line_layers,
        "explicit_redline_layers": [],
        "visual_reference": visual_reference,
        "line_features": line_features,
        "polygon_features": polygon_features,
        "point_features": point_features,
    }

def _build_route_catalog(file_bytes: bytes, filename: str) -> List[Dict[str, Any]]:
    kml_bytes = _extract_kml_bytes(file_bytes, filename)
    root = ET.fromstring(kml_bytes)
    parent_map = _parent_map(root)

    routes: List[Dict[str, Any]] = []
    route_counter = 0

    for placemark in root.findall(".//kml:Placemark", KML_NS):
        placemark_name = (placemark.findtext("kml:name", default="", namespaces=KML_NS) or "").strip() or "Unnamed Route"
        folder_names = _folder_path(placemark, parent_map)
        source_folder = " / ".join(folder_names[1:]) if len(folder_names) > 1 else (folder_names[0] if folder_names else "")
        role_hint = f"{source_folder} {placemark_name}".strip().lower()

        for node in placemark.findall(".//kml:LineString/kml:coordinates", KML_NS):
            raw_coords = _dedupe_consecutive(_parse_coordinate_text(node.text or ""))
            if len(raw_coords) < 2:
                continue

            coords = _densify_route_coords(raw_coords)
            route_counter += 1
            route_length_ft = round(_route_length_ft(coords), 2)
            role = _infer_route_role(role_hint)

            routes.append(
                {
                    "route_id": f"route_{route_counter}",
                    "route_name": placemark_name,
                    "name": placemark_name,
                    "source_folder": source_folder,
                    "coords": coords,
                    "length_ft": route_length_ft,
                    "point_count": len(coords),
                    "route_role": role,
                }
            )

    if not routes:
        raise ValueError("No valid LineString routes found in design file.")

    routes.sort(key=lambda route: (-float(route.get("length_ft", 0.0) or 0.0), route.get("route_name", "")))
    return routes


def _choose_default_route(route_catalog: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    if not route_catalog:
        raise ValueError("Route catalog is empty.")
    return max(route_catalog, key=lambda route: float(route.get("length_ft", 0.0) or 0.0))


def _find_route_by_id(route_id: Any) -> Optional[Dict[str, Any]]:
    target = str(route_id or "").strip()
    for route in STATE.get("route_catalog", []) or []:
        if str(route.get("route_id", "")).strip() == target:
            return route
    return None


def _set_active_route(route: Optional[Dict[str, Any]]) -> None:
    if not route:
        STATE["route_id"] = None
        STATE["route_name"] = None
        STATE["route_coords"] = []
        STATE["route_length_ft"] = 0.0
        STATE["map_points"] = []
        return

    STATE["route_id"] = route.get("route_id")
    STATE["route_name"] = route.get("route_name") or route.get("name")
    STATE["route_coords"] = route.get("coords", []) or []
    STATE["route_length_ft"] = float(route.get("length_ft", 0.0) or 0.0)
    STATE["map_points"] = route.get("coords", []) or []


def _route_chainage(coords: Sequence[Sequence[float]]) -> List[float]:
    chainage = [0.0]
    for i in range(1, len(coords)):
        chainage.append(
            chainage[-1]
            + _haversine_feet(
                float(coords[i - 1][0]),
                float(coords[i - 1][1]),
                float(coords[i][0]),
                float(coords[i][1]),
            )
        )
    return chainage


def _latlon_to_local_xy_feet(lat: float, lon: float, lat0: float, lon0: float) -> Tuple[float, float]:
    lat_scale = 364000.0
    lon_scale = 364000.0 * math.cos(math.radians(lat0))
    x = (lon - lon0) * lon_scale
    y = (lat - lat0) * lat_scale
    return x, y


def _project_point_to_segment_ft(
    point_xy: Tuple[float, float],
    start_xy: Tuple[float, float],
    end_xy: Tuple[float, float],
) -> Tuple[float, float, Tuple[float, float]]:
    px, py = point_xy
    ax, ay = start_xy
    bx, by = end_xy
    dx = bx - ax
    dy = by - ay
    seg_len_sq = dx * dx + dy * dy
    if seg_len_sq <= 1e-9:
        dist = math.hypot(px - ax, py - ay)
        return 0.0, dist, (ax, ay)
    t = ((px - ax) * dx + (py - ay) * dy) / seg_len_sq
    t = max(0.0, min(1.0, t))
    qx = ax + t * dx
    qy = ay + t * dy
    dist = math.hypot(px - qx, py - qy)
    return t, dist, (qx, qy)


def _route_segment_bearings(route_coords: Sequence[Sequence[float]]) -> List[float]:
    if len(route_coords) < 2:
        return []
    lat0 = float(route_coords[0][0])
    lon0 = float(route_coords[0][1])
    pts = [_latlon_to_local_xy_feet(float(lat), float(lon), lat0, lon0) for lat, lon in route_coords]
    bearings: List[float] = []
    for i in range(1, len(pts)):
        dx = pts[i][0] - pts[i - 1][0]
        dy = pts[i][1] - pts[i - 1][1]
        bearings.append(math.atan2(dy, dx))
    return bearings


def _bearing_delta_degrees(a: float, b: float) -> float:
    delta = abs(math.degrees(b - a))
    while delta > 180.0:
        delta = abs(delta - 360.0)
    return delta


def _project_chainage_to_route(route_coords: Sequence[Sequence[float]], chainage: Sequence[float], distance_ft: float) -> Dict[str, Any]:
    if not route_coords:
        return {"segment_index": 0, "segment_ratio": 0.0, "actual_segment_index": 0, "actual_segment_ratio": 0.0, "virtual_segment_count": 1, "lat": 0.0, "lon": 0.0}
    d = max(0.0, min(float(distance_ft), float(chainage[-1])))
    if len(route_coords) == 1:
        return {"segment_index": 0, "segment_ratio": 0.0, "actual_segment_index": 0, "actual_segment_ratio": 0.0, "virtual_segment_count": 1, "lat": float(route_coords[0][0]), "lon": float(route_coords[0][1])}
    for idx in range(1, len(chainage)):
        seg_start = float(chainage[idx - 1])
        seg_end = float(chainage[idx])
        if d <= seg_end or idx == len(chainage) - 1:
            seg_len = max(seg_end - seg_start, 1e-9)
            ratio = (d - seg_start) / seg_len
            lat, lon = _interpolate_point(route_coords[idx - 1], route_coords[idx], ratio)
            virtual_meta = _virtualize_segment_index(chainage, idx - 1, ratio)
            return {
                "segment_index": int(virtual_meta["virtual_segment_index"]),
                "segment_ratio": float(virtual_meta["virtual_segment_ratio"]),
                "actual_segment_index": idx - 1,
                "actual_segment_ratio": ratio,
                "virtual_segment_count": int(virtual_meta["virtual_segment_count"]),
                "lat": float(lat),
                "lon": float(lon),
            }
    lat, lon = route_coords[-1]
    last_actual_index = max(0, len(route_coords) - 2)
    virtual_meta = _virtualize_segment_index(chainage, last_actual_index, 1.0)
    return {"segment_index": int(virtual_meta["virtual_segment_index"]), "segment_ratio": float(virtual_meta["virtual_segment_ratio"]), "actual_segment_index": last_actual_index, "actual_segment_ratio": 1.0, "virtual_segment_count": int(virtual_meta["virtual_segment_count"]), "lat": float(lat), "lon": float(lon)}

def _route_shape_signature(route_coords: Sequence[Sequence[float]], chainage: Sequence[float]) -> Dict[str, Any]:
    bearings = _route_segment_bearings(route_coords)
    bend_positions: List[float] = []
    bend_strengths: List[float] = []
    for i in range(1, len(bearings)):
        delta = _bearing_delta_degrees(bearings[i - 1], bearings[i])
        if delta >= 12.0:
            bend_positions.append(float(chainage[i]))
            bend_strengths.append(delta)
    return {
        "bend_positions": bend_positions,
        "bend_strengths": bend_strengths,
    }


def _interpolate_point(a: Sequence[float], b: Sequence[float], ratio: float) -> List[float]:
    ratio = max(0.0, min(1.0, float(ratio)))
    return [
        float(a[0]) + (float(b[0]) - float(a[0])) * ratio,
        float(a[1]) + (float(b[1]) - float(a[1])) * ratio,
    ]


def _point_at_distance(route_coords: Sequence[Sequence[float]], chainage: Sequence[float], distance_ft: float) -> List[float]:
    if not route_coords:
        raise ValueError("Route is empty.")
    if len(route_coords) == 1:
        return [float(route_coords[0][0]), float(route_coords[0][1])]

    d = max(0.0, min(float(distance_ft), float(chainage[-1])))
    for idx in range(1, len(chainage)):
        seg_start = float(chainage[idx - 1])
        seg_end = float(chainage[idx])
        if d <= seg_end or idx == len(chainage) - 1:
            seg_len = max(seg_end - seg_start, 1e-9)
            ratio = (d - seg_start) / seg_len
            return _interpolate_point(route_coords[idx - 1], route_coords[idx], ratio)

    last = route_coords[-1]
    return [float(last[0]), float(last[1])]


def _clip_route_segment(route_coords: Sequence[Sequence[float]], start_ft: float, end_ft: float) -> List[List[float]]:
    if len(route_coords) < 2:
        return []
    chainage = _route_chainage(route_coords)
    total = float(chainage[-1])
    start_d = max(0.0, min(float(start_ft), total))
    end_d = max(0.0, min(float(end_ft), total))
    if end_d <= start_d:
        return []

    segment = [_point_at_distance(route_coords, chainage, start_d)]
    for idx in range(1, len(chainage) - 1):
        current_d = float(chainage[idx])
        if start_d < current_d < end_d:
            segment.append([float(route_coords[idx][0]), float(route_coords[idx][1])])
    segment.append(_point_at_distance(route_coords, chainage, end_d))

    cleaned: List[List[float]] = []
    for pt in segment:
        if not cleaned or abs(cleaned[-1][0] - pt[0]) > 1e-9 or abs(cleaned[-1][1] - pt[1]) > 1e-9:
            cleaned.append(pt)
    return cleaned if len(cleaned) >= 2 else []





def _station_offsets_from_rows(rows: Sequence[Dict[str, Any]]) -> List[float]:
    station_values = [float(row.get("station_ft")) for row in rows if row.get("station_ft") is not None]
    if not station_values:
        return []
    origin = min(station_values)
    return [max(0.0, float(value) - origin) for value in station_values]


def _distance_to_nearest(target_ft: float, candidates_ft: Sequence[float]) -> float:
    if not candidates_ft:
        return float("inf")
    return min(abs(float(target_ft) - float(candidate)) for candidate in candidates_ft)


def _candidate_anchor_starts(
    route_coords: Sequence[Sequence[float]],
    route_total_ft: float,
    span_ft: float,
    mapping: Dict[str, Any],
    rows: Sequence[Dict[str, Any]],
) -> List[float]:
    if route_total_ft <= 0.0:
        return [0.0]

    usable_span = max(0.0, min(float(span_ft or 0.0), float(route_total_ft)))
    max_start = max(0.0, float(route_total_ft) - usable_span)
    chainage = _route_chainage(route_coords) if route_coords else [0.0]
    station_offsets = _station_offsets_from_rows(rows)

    candidates = {0.0, round(max_start, 2)}
    if max_start > 0.0:
        candidates.add(round(max_start / 2.0, 2))

    min_station = mapping.get("min_station_ft")
    max_station = mapping.get("max_station_ft")
    if min_station is not None and max_station is not None:
        min_station = float(min_station)
        max_station = float(max_station)
        if 0.0 <= min_station <= route_total_ft and 0.0 <= max_station <= route_total_ft and max_station > min_station:
            candidates.add(round(max(0.0, min(min_station, max_start)), 2))

    probe_offsets = {0.0}
    if station_offsets:
        probe_offsets.update(station_offsets)
        probe_offsets.add(round(station_offsets[-1] / 2.0, 2))
        if len(station_offsets) >= 3:
            probe_offsets.add(round(station_offsets[len(station_offsets) // 2], 2))

    # Vertex-aligned probes
    for vertex_ft in chainage:
        for offset_ft in probe_offsets:
            start_ft = max(0.0, min(float(vertex_ft) - float(offset_ft), max_start))
            candidates.add(round(start_ft, 2))

    # Segment interior probes at quarter points to support real projection-based anchoring.
    for idx in range(1, len(chainage)):
        seg_start = float(chainage[idx - 1])
        seg_end = float(chainage[idx])
        for frac in (0.25, 0.5, 0.75):
            probe_chain = seg_start + (seg_end - seg_start) * frac
            for offset_ft in probe_offsets:
                start_ft = max(0.0, min(probe_chain - float(offset_ft), max_start))
                candidates.add(round(start_ft, 2))

    if max_start > 0.0:
        step = max(8.0, min(25.0, usable_span / 10.0 if usable_span > 0.0 else route_total_ft / 30.0))
        probe = 0.0
        while probe <= max_start + 1e-9:
            candidates.add(round(min(probe, max_start), 2))
            probe += step

    return sorted(candidates)


def _anchor_profile_for_start(
    route_coords: Sequence[Sequence[float]],
    route_total_ft: float,
    span_ft: float,
    start_ft: float,
    rows: Sequence[Dict[str, Any]],
    mapping: Dict[str, Any],
) -> Dict[str, Any]:
    usable_span = max(0.0, min(float(span_ft or 0.0), float(route_total_ft)))
    max_start = max(0.0, float(route_total_ft) - usable_span)
    start_ft = max(0.0, min(float(start_ft), max_start))
    end_ft = max(0.0, min(start_ft + usable_span, float(route_total_ft)))

    chainage = _route_chainage(route_coords)
    station_offsets = _station_offsets_from_rows(rows)
    mapped_positions = [max(0.0, min(start_ft + offset, route_total_ft)) for offset in station_offsets]

    projected_points = [_project_chainage_to_route(route_coords, chainage, pos) for pos in mapped_positions]
    segment_indices = [int(p["segment_index"]) for p in projected_points]
    segment_ratios = [float(p["segment_ratio"]) for p in projected_points]

    distinct_segment_count = len(set(segment_indices))
    row_count = max(len(rows), 1)

    edge_clearance_ft = min(start_ft, max(0.0, route_total_ft - end_ft))
    start_vertex_distance = _distance_to_nearest(start_ft, list(chainage))
    end_vertex_distance = _distance_to_nearest(end_ft, list(chainage))

    segment_balance = 0.0
    if projected_points:
        interior_hits = sum(1 for r in segment_ratios if 0.08 <= r <= 0.92)
        segment_balance = interior_hits / len(projected_points)

    segment_steps: List[int] = []
    for i in range(1, len(segment_indices)):
        segment_steps.append(abs(segment_indices[i] - segment_indices[i - 1]))
    max_segment_jump = max(segment_steps) if segment_steps else 0
    jump_penalty = min(1.0, max_segment_jump / 3.0) if max_segment_jump > 0 else 0.0

    shape_sig = _route_shape_signature(route_coords, chainage)
    bend_positions = list(shape_sig["bend_positions"])
    bend_strengths = list(shape_sig["bend_strengths"])
    window_bend_strength = 0.0
    covered_bends = 0
    for pos, strength in zip(bend_positions, bend_strengths):
        if start_ft <= pos <= end_ft:
            covered_bends += 1
            window_bend_strength += float(strength)
    bend_density = 0.0
    if usable_span > 0.0:
        bend_density = min(1.0, window_bend_strength / max(35.0, usable_span * 0.18))

    endpoint_alignment = 0.0
    if projected_points:
        endpoint_alignment = 1.0 - min(1.0, ((start_vertex_distance + end_vertex_distance) / max(30.0, usable_span * 0.15)) / 2.0)

    return {
        "start_ft": round(start_ft, 2),
        "end_ft": round(end_ft, 2),
        "mapped_positions": [round(value, 2) for value in mapped_positions],
        "projected_points": [
            {
                "segment_index": int(point["segment_index"]),
                "segment_ratio": round(float(point["segment_ratio"]), 4),
                "actual_segment_index": int(point.get("actual_segment_index", point["segment_index"])),
                "actual_segment_ratio": round(float(point.get("actual_segment_ratio", point["segment_ratio"])), 4),
                "virtual_segment_count": int(point.get("virtual_segment_count", 1)),
                "lat": round(float(point["lat"]), 8),
                "lon": round(float(point["lon"]), 8),
            }
            for point in projected_points
        ],
        "segment_indices": segment_indices,
        "segment_ratios": [round(v, 4) for v in segment_ratios],
        "start_vertex_distance_ft": round(start_vertex_distance if math.isfinite(start_vertex_distance) else 999999.0, 2),
        "end_vertex_distance_ft": round(end_vertex_distance if math.isfinite(end_vertex_distance) else 999999.0, 2),
        "edge_clearance_ft": round(edge_clearance_ft, 2),
        "distinct_segment_count": distinct_segment_count,
        "row_count": row_count,
        "segment_balance_fit": round(segment_balance, 6),
        "max_segment_jump": int(max_segment_jump),
        "jump_penalty": round(jump_penalty, 6),
        "covered_bends": int(covered_bends),
        "bend_density_fit": round(bend_density, 6),
        "endpoint_alignment_fit": round(max(0.0, endpoint_alignment), 6),
    }


def _score_anchor_start(
    start_ft: float,
    route_coords: Sequence[Sequence[float]],
    route_total_ft: float,
    span_ft: float,
    mapping: Dict[str, Any],
    ranking: Dict[str, Any],
    rows: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    usable_span = max(0.0, min(float(span_ft or 0.0), float(route_total_ft)))
    route_score = float(ranking.get("score", 0.0) or 0.0)
    profile = _anchor_profile_for_start(route_coords, route_total_ft, span_ft, start_ft, rows, mapping)

    start_ft = float(profile["start_ft"])
    end_ft = float(profile["end_ft"])

    span_fit = 1.0
    if span_ft > 0.0 and route_total_ft > 0.0:
        span_fit = min(span_ft, route_total_ft) / max(span_ft, route_total_ft)

    route_length_fit = 0.0
    if route_total_ft > 0.0 and span_ft > 0.0:
        relative_gap = abs(route_total_ft - span_ft) / max(span_ft, 1.0)
        route_length_fit = max(0.0, 1.0 - relative_gap)

    endpoint_alignment_fit = float(profile.get("endpoint_alignment_fit", 0.0) or 0.0)
    segment_balance_fit = float(profile.get("segment_balance_fit", 0.0) or 0.0)
    bend_density_fit = float(profile.get("bend_density_fit", 0.0) or 0.0)
    edge_clearance_ft = float(profile["edge_clearance_ft"])
    if route_total_ft <= usable_span + 1.0:
        edge_fit = 1.0
    else:
        edge_fit = min(1.0, edge_clearance_ft / max(35.0, usable_span * 0.12))

    jump_penalty = float(profile.get("jump_penalty", 0.0) or 0.0)

    anchor_method = "projection_window_search"
    anchor_reasons: List[str] = []
    absolute_station_fit = 0.0

    min_station = mapping.get("min_station_ft")
    max_station = mapping.get("max_station_ft")
    if min_station is not None and max_station is not None:
        min_station = float(min_station)
        max_station = float(max_station)
        if 0.0 <= min_station <= route_total_ft and 0.0 <= max_station <= route_total_ft and max_station > min_station:
            expected_start = min_station
            expected_end = max_station
            tolerance = max(20.0, usable_span * 0.08)
            start_fit = max(0.0, 1.0 - (abs(start_ft - expected_start) / tolerance))
            end_fit = max(0.0, 1.0 - (abs(end_ft - expected_end) / tolerance))
            absolute_station_fit = (start_fit + end_fit) / 2.0
            if absolute_station_fit >= 0.85:
                anchor_method = "absolute_station_projection"
                anchor_reasons.append("Absolute station feet aligned closely with the projected route window.")

    anchor_fit = (
        0.30 * endpoint_alignment_fit
        + 0.25 * segment_balance_fit
        + 0.15 * bend_density_fit
        + 0.15 * edge_fit
        + 0.15 * absolute_station_fit
    )

    subsection_score = (
        0.35 * span_fit
        + 0.25 * route_length_fit
        + 0.40 * anchor_fit
        - 0.18 * jump_penalty
    )
    subsection_score = max(0.0, min(1.0, subsection_score))

    combined_score = max(0.0, min(1.0, (0.55 * route_score) + (0.45 * subsection_score)))

    if not anchor_reasons:
        anchor_reasons.extend(
            [
                f"Projected station offsets across {int(profile.get('distinct_segment_count', 0) or 0)} route segment(s).",
                f"Segment-balance fit {round(segment_balance_fit, 3)} and endpoint-alignment fit {round(endpoint_alignment_fit, 3)} drove anchor selection.",
            ]
        )
        if bend_density_fit > 0.0:
            anchor_reasons.append(f"Window bend-density fit {round(bend_density_fit, 3)} favored geometry that matched the bore span shape.")
        if jump_penalty > 0.0:
            anchor_reasons.append(f"Large segment jumps were penalized ({round(jump_penalty, 3)}).")

    return {
        "start_ft": round(start_ft, 2),
        "end_ft": round(end_ft, 2),
        "anchor_fit": round(anchor_fit, 6),
        "anchor_method": anchor_method,
        "anchor_reasons": anchor_reasons,
        "subsection_score": round(subsection_score, 6),
        "combined_score": round(combined_score, 6),
        "score_components": {
            "route_score": round(route_score, 6),
            "span_fit": round(span_fit, 6),
            "route_length_fit": round(route_length_fit, 6),
            "endpoint_alignment_fit": round(endpoint_alignment_fit, 6),
            "segment_balance_fit": round(segment_balance_fit, 6),
            "bend_density_fit": round(bend_density_fit, 6),
            "edge_fit": round(edge_fit, 6),
            "absolute_station_fit": round(absolute_station_fit, 6),
            "jump_penalty": round(jump_penalty, 6),
        },
        "anchor_profile": profile,
    }
def _coerce_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        text = "".join(ch for ch in str(value) if ch.isdigit() or ch in ".-")
        if not text:
            return None
        try:
            return float(text)
        except Exception:
            return None


def _read_bore_log_rows(file_bytes: bytes, filename: str) -> List[Dict[str, Any]]:
    df = pd.read_excel(io.BytesIO(file_bytes))
    df.columns = [str(col).strip().lower() for col in df.columns]

    required = {"station", "depth", "boc"}
    if not required.issubset(set(df.columns)):
        raise ValueError(f"{filename} must contain columns: station, depth, boc")

    rows: List[Dict[str, Any]] = []
    for _, rec in df.iterrows():
        station_text = _normalize_station_text(rec.get("station"))
        station_ft = _station_to_feet(station_text)
        if station_ft is None:
            continue
        rows.append(
            {
                "station": station_text,
                "station_ft": float(station_ft),
                "depth_ft": _coerce_float(rec.get("depth")),
                "boc_ft": _coerce_float(rec.get("boc")),
                "date": str(rec.get("date") or "").strip(),
                "crew": str(rec.get("crew") or "").strip(),
                "print": str(rec.get("print") or "").strip(),
                "notes": str(rec.get("notes") or "").strip(),
                "source_file": _safe_filename(filename),
            }
        )

    rows.sort(key=lambda r: float(r["station_ft"]))
    return rows


def _group_rows_for_matching(rows: Sequence[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    if not rows:
        return []

    def _normalized_text(value: Any) -> str:
        return str(value or "").strip()

    def _group_key(row: Dict[str, Any]) -> Tuple[str, Tuple[str, ...]]:
        source_file = _normalized_text(row.get("source_file"))
        print_tokens = tuple(sorted(_parse_print_tokens(row.get("print"))))
        return source_file, print_tokens

    def _step_history(group: Sequence[Dict[str, Any]]) -> List[float]:
        steps: List[float] = []
        for idx in range(1, len(group)):
            prev_ft = group[idx - 1].get("station_ft")
            curr_ft = group[idx].get("station_ft")
            if prev_ft is None or curr_ft is None:
                continue
            delta = float(curr_ft) - float(prev_ft)
            if delta > 0.0:
                steps.append(delta)
        return steps

    def _median_step(group: Sequence[Dict[str, Any]]) -> float:
        steps = sorted(_step_history(group))
        if not steps:
            return 50.0
        mid = len(steps) // 2
        if len(steps) % 2 == 1:
            return float(steps[mid])
        return float((steps[mid - 1] + steps[mid]) / 2.0)

    def _is_new_group(previous: Dict[str, Any], current: Dict[str, Any], active_group: Sequence[Dict[str, Any]]) -> bool:
        if _group_key(previous) != _group_key(current):
            return True

        prev_station = previous.get("station_ft")
        curr_station = current.get("station_ft")
        if prev_station is None or curr_station is None:
            return True

        station_delta = float(curr_station) - float(prev_station)
        if station_delta <= 0.0:
            return True

        previous_crew = _normalized_text(previous.get("crew"))
        current_crew = _normalized_text(current.get("crew"))
        if previous_crew and current_crew and previous_crew != current_crew:
            return True

        previous_date = _normalized_text(previous.get("date"))
        current_date = _normalized_text(current.get("date"))
        if previous_date and current_date and previous_date != current_date:
            return True

        median_step = _median_step(active_group)
        max_expected_gap = max(150.0, median_step * 3.5)
        if station_delta > max_expected_gap:
            return True

        return False

    sorted_rows = sorted(
        [dict(row) for row in rows],
        key=lambda row: (
            _normalized_text(row.get("source_file")),
            tuple(sorted(_parse_print_tokens(row.get("print")))),
            float(row.get("station_ft") or 0.0),
            _normalized_text(row.get("date")),
            _normalized_text(row.get("crew")),
        ),
    )

    groups: List[List[Dict[str, Any]]] = []
    current_group: List[Dict[str, Any]] = [sorted_rows[0]]

    for row in sorted_rows[1:]:
        previous = current_group[-1]
        if _is_new_group(previous, row, current_group):
            groups.append(current_group)
            current_group = [row]
        else:
            current_group.append(row)

    groups.append(current_group)
    return groups


def _infer_expected_roles(group_rows: Sequence[Dict[str, Any]], expected_length_ft: float) -> List[str]:
    notes_blob = " ".join(str(row.get("notes") or "") for row in group_rows).lower()
    source_blob = " ".join(
        [
            str(group_rows[0].get("source_file") or ""),
            str(group_rows[0].get("print") or ""),
            notes_blob,
        ]
    ).lower()

    expected: List[str] = []
    if "vacant" in source_blob:
        expected.append("vacant_pipe")
    if "drop" in source_blob and "house" in source_blob:
        expected.append("house_drop")
    if "tail" in source_blob:
        expected.append("terminal_tail")
    if "backbone" in source_blob:
        expected.append("backbone")
    if "cable" in source_blob or "fiber" in source_blob:
        expected.append("underground_cable")

    if expected_length_ft <= 160:
        expected.extend(["house_drop", "vacant_pipe", "terminal_tail"])
    elif expected_length_ft <= 1200:
        expected.extend(["terminal_tail", "underground_cable", "vacant_pipe"])
    else:
        expected.extend(["underground_cable", "backbone", "terminal_tail"])

    seen = set()
    ordered: List[str] = []
    for item in expected:
        if item not in seen:
            ordered.append(item)
            seen.add(item)
    return ordered


def _route_type_bonus(route_role: str, expected_roles: Sequence[str]) -> float:
    normalized = str(route_role or "other").strip().lower()
    if not expected_roles:
        return 0.0
    if normalized == expected_roles[0]:
        return 0.18
    if normalized in expected_roles[:2]:
        return 0.10
    if normalized in expected_roles:
        return 0.04
    return 0.0




def _parse_print_tokens(value: Any) -> List[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    parts = [part.strip() for part in raw.replace(";", ",").split(",")]
    return [part for part in parts if part]


def _collect_group_print_tokens(group_rows: Sequence[Dict[str, Any]]) -> List[str]:
    seen: List[str] = []
    for row in group_rows:
        for token in _parse_print_tokens(row.get("print")):
            if token not in seen:
                seen.append(token)
    return seen



def _route_filter_for_print_tokens(print_tokens: Sequence[str], route_catalog: Sequence[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if not print_tokens:
        return list(route_catalog), {
            "applied": False,
            "mode": "none",
            "print_tokens": [],
            "sheet_numbers": [],
            "street_hints": [],
            "allowed_route_ids": [],
            "reason": "No print tokens were present on the bore-log group.",
        }

    hint_meta = _print_sheet_hints(print_tokens)
    allowed_route_ids = list(hint_meta.get("allowed_route_ids") or [])
    street_hints = list(hint_meta.get("street_hints") or [])
    sheet_numbers = list(hint_meta.get("sheet_numbers") or [])

    if not allowed_route_ids:
        return list(route_catalog), {
            "applied": False,
            "mode": "none",
            "print_tokens": list(print_tokens),
            "sheet_numbers": sheet_numbers,
            "street_hints": street_hints,
            "allowed_route_ids": [],
            "reason": "No print-to-street extraction hints were available for this print set.",
        }

    allowed_set = set(allowed_route_ids)
    filtered = [route for route in route_catalog if str(route.get("route_id") or "") in allowed_set]

    if not filtered:
        return list(route_catalog), {
            "applied": False,
            "mode": "print_to_street_extraction",
            "print_tokens": list(print_tokens),
            "sheet_numbers": sheet_numbers,
            "street_hints": street_hints,
            "allowed_route_ids": allowed_route_ids,
            "reason": "Print-to-street extraction resolved to route ids, but none were present in the current KMZ catalog.",
        }

    return filtered, {
        "applied": True,
        "mode": "print_to_street_extraction",
        "print_tokens": list(print_tokens),
        "sheet_numbers": sheet_numbers,
        "street_hints": street_hints,
        "allowed_route_ids": allowed_route_ids,
        "reason": "Candidate routes were narrowed by print-to-street extraction calibrated from the detailed engineering sheets.",
    }


def _decorate_route_id_disambiguation(
    plausible_routes: Sequence[Dict[str, Any]],
    span_ft: float,
    filter_meta: Dict[str, Any],
) -> List[Dict[str, Any]]:
    decorated: List[Dict[str, Any]] = [dict(route) for route in plausible_routes]
    if not decorated:
        return decorated

    allowed_route_ids = [str(value or "").strip() for value in (filter_meta.get("allowed_route_ids") or []) if str(value or "").strip()]
    allowed_route_id_set = set(allowed_route_ids)

    family_map: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for route in decorated:
        family_key = (
            str(route.get("route_name") or "").strip().lower(),
            str(route.get("route_role") or "").strip().lower(),
        )
        family_map.setdefault(family_key, []).append(route)

    for family_routes in family_map.values():
        if not family_routes:
            continue

        best_length_gap = min(abs(float(route.get("length_ft", 0.0) or 0.0) - float(span_ft or 0.0)) for route in family_routes)
        best_spatial_hint = max(float(route.get("_spatial_hint_score", 0.0) or 0.0) for route in family_routes)
        shortest_length = min(float(route.get("length_ft", 0.0) or 0.0) for route in family_routes)
        longest_length = max(float(route.get("length_ft", 0.0) or 0.0) for route in family_routes)

        for route in family_routes:
            route_length_ft = float(route.get("length_ft", 0.0) or 0.0)
            length_gap = abs(route_length_ft - float(span_ft or 0.0))
            spatial_hint = float(route.get("_spatial_hint_score", 0.0) or 0.0)
            route_id = str(route.get("route_id") or "").strip()

            exact_allowed_bonus = 0.0
            if len(allowed_route_id_set) == 1 and route_id in allowed_route_id_set:
                exact_allowed_bonus = 0.14

            family_length_bonus = 0.0
            if len(family_routes) > 1 and best_length_gap >= 0.0:
                tolerance_ft = max(60.0, float(span_ft or 0.0) * 0.20)
                family_length_bonus = max(0.0, 1.0 - ((length_gap - best_length_gap) / tolerance_ft)) * 0.10

            family_spatial_bonus = 0.0
            if len(family_routes) > 1 and best_spatial_hint > 0.0:
                family_spatial_bonus = max(0.0, min(spatial_hint, best_spatial_hint) / best_spatial_hint) * 0.05

            corridor_fit_bonus = 0.0
            if len(family_routes) > 1 and longest_length > shortest_length and float(span_ft or 0.0) > 0.0:
                # Prefer the corridor whose total length is proportionally closest to the bore span
                relative_fit = min(route_length_ft, float(span_ft)) / max(route_length_ft, float(span_ft))
                corridor_fit_bonus = max(0.0, min(1.0, relative_fit)) * 0.04

            total_bonus = exact_allowed_bonus + family_length_bonus + family_spatial_bonus + corridor_fit_bonus
            route["_route_id_disambiguation_bonus"] = round(total_bonus, 6)
            route["_route_id_disambiguation_meta"] = {
                "family_size": len(family_routes),
                "best_length_gap_ft": round(best_length_gap, 2),
                "route_length_gap_ft": round(length_gap, 2),
                "exact_allowed_bonus": round(exact_allowed_bonus, 6),
                "family_length_bonus": round(family_length_bonus, 6),
                "family_spatial_bonus": round(family_spatial_bonus, 6),
                "corridor_fit_bonus": round(corridor_fit_bonus, 6),
            }

    return decorated

def _score_route_for_group(group_rows: Sequence[Dict[str, Any]], route: Dict[str, Any]) -> Dict[str, Any]:
    start_ft = float(group_rows[0].get("station_ft") or 0.0)
    end_ft = float(group_rows[-1].get("station_ft") or start_ft)
    expected_length_ft = max(0.0, end_ft - start_ft)

    route_length_ft = float(route.get("length_ft", 0.0) or 0.0)
    length_gap = abs(route_length_ft - expected_length_ft)

    if expected_length_ft <= 0.0 or route_length_ft <= 0.0:
        closeness_ratio = 0.0
        length_score = 0.0
        oversize_penalty = 0.0
    else:
        shorter = min(expected_length_ft, route_length_ft)
        longer = max(expected_length_ft, route_length_ft)
        closeness_ratio = shorter / longer

        # Make route length fit the dominant signal.
        # Exact or near-exact routes should rise hard.
        # Oversized routes should get hit much harder than before.
        length_score = closeness_ratio ** 2.35

        if route_length_ft > expected_length_ft:
            oversize_ratio = route_length_ft / max(expected_length_ft, 1.0)
            oversize_penalty = min(0.42, max(0.0, (oversize_ratio - 1.0) * 0.18))
        else:
            oversize_penalty = 0.0

        length_score = max(0.0, length_score - oversize_penalty)

    expected_roles = _infer_expected_roles(group_rows, expected_length_ft)
    type_bonus = _route_type_bonus(str(route.get("route_role") or ""), expected_roles)

    point_count = float(route.get("point_count", 0) or 0)
    geometry_bonus = 0.02 if point_count >= 3 else 0.0

    score = round(min(1.0, length_score + type_bonus + geometry_bonus), 6)
    reason_parts = [
        f"Expected span {round(expected_length_ft, 2)} ft vs route length {round(route_length_ft, 2)} ft",
        f"Length closeness ratio {round(closeness_ratio, 4)}",
        f"Route role {route.get('route_role', 'other')}",
    ]
    if route_length_ft > expected_length_ft and expected_length_ft > 0.0:
        reason_parts.append("Oversized route was penalized to avoid loose span matches.")
    if expected_roles:
        reason_parts.append(f"Expected roles {', '.join(expected_roles)}")

    return {
        "route_id": route.get("route_id"),
        "route_name": route.get("route_name"),
        "source_folder": route.get("source_folder"),
        "route_role": route.get("route_role"),
        "route_length_ft": round(route_length_ft, 2),
        "expected_span_ft": round(expected_length_ft, 2),
        "length_gap_ft": round(length_gap, 2),
        "score": score,
        "reason": " | ".join(reason_parts),
    }


def _normalize_bore_group(group_rows: Sequence[Dict[str, Any]], group_idx: int) -> Dict[str, Any]:
    rows = [dict(row) for row in group_rows]
    station_values = [float(row["station_ft"]) for row in rows if row.get("station_ft") is not None]
    warnings: List[str] = []
    if not station_values:
        warnings.append("No normalized station values were available for this bore-log group.")
    monotonic_breaks = 0
    duplicate_count = 0
    for idx in range(1, len(station_values)):
        if station_values[idx] < station_values[idx - 1]:
            monotonic_breaks += 1
        if abs(station_values[idx] - station_values[idx - 1]) < 1e-9:
            duplicate_count += 1
    if monotonic_breaks:
        warnings.append(f"Station order contains {monotonic_breaks} non-monotonic break(s).")
    if duplicate_count:
        warnings.append(f"Station order contains {duplicate_count} duplicate station value(s).")

    min_station = min(station_values) if station_values else None
    max_station = max(station_values) if station_values else None
    span_ft = (max_station - min_station) if (min_station is not None and max_station is not None) else None

    return {
        "group_id": f"group_{group_idx + 1}",
        "group_index": group_idx,
        "source_file": str(rows[0].get("source_file") or "") if rows else "",
        "print_tokens": list(_collect_group_print_tokens(rows)),
        "row_count": len(rows),
        "min_station_ft": round(float(min_station), 2) if min_station is not None else None,
        "max_station_ft": round(float(max_station), 2) if max_station is not None else None,
        "span_ft": round(float(span_ft), 2) if span_ft is not None else None,
        "station_rows": [dict(row) for row in rows],
        "normalization_warnings": warnings,
    }


def _build_candidate_pool_for_group(normalized_group: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    route_catalog = STATE.get("route_catalog", []) or []
    if not route_catalog:
        raise ValueError("No route catalog loaded.")

    print_tokens = list(normalized_group.get("print_tokens") or [])
    filtered_routes, filter_meta = _route_filter_for_print_tokens(print_tokens, route_catalog)
    span_ft = float(normalized_group.get("span_ft") or 0.0)
    source_file = str(normalized_group.get("source_file") or "").lower()
    spatial_context = _infer_group_spatial_context(normalized_group)

    plausible_routes: List[Dict[str, Any]] = []
    rejected_length_routes: List[Dict[str, Any]] = []

    for route in filtered_routes:
        route_length_ft = float(route.get("length_ft", 0.0) or 0.0)
        deferred_oversized_reason = None

        if span_ft > 0.0 and route_length_ft > 0.0:
            min_allowed_ft = span_ft * 0.70
            max_allowed_ft = span_ft * 3.50

            if route_length_ft < min_allowed_ft:
                rejected_length_routes.append({
                    "route_id": route.get("route_id"),
                    "route_name": route.get("route_name"),
                    "route_length_ft": round(route_length_ft, 2),
                    "reason": f"undersized_hard_gate_lt_{round(min_allowed_ft, 2)}",
                })
                continue

            if route_length_ft > max_allowed_ft:
                # Oversized corridors can still contain a valid anchored subsection.
                # Keep them in the candidate pool and let subsection anchoring +
                # downstream validation decide whether the match is truly usable.
                deferred_oversized_reason = f"oversized_hard_gate_gt_{round(max_allowed_ft, 2)}"
            else:
                deferred_oversized_reason = None

        plausible_route = dict(route)
        route_tokens = f"{plausible_route.get('route_name', '')} {plausible_route.get('source_folder', '')}".lower()
        name_hint_score = 0.0
        if source_file and route_tokens:
            for token in [tok for tok in source_file.replace('-', ' ').replace('_', ' ').split() if len(tok) >= 4]:
                if token in route_tokens:
                    name_hint_score += 0.03

        route_coords = plausible_route.get("coords") or []
        route_bbox = _route_bbox(route_coords)
        route_centroid = _route_centroid(route_coords)

        spatial_filter = {
            "applied": False,
            "passed": True,
            "reason": "no_spatial_context",
            "centroid_distance_ft": None,
        }

        spatial_hint_score = 0.0
        if spatial_context.get("has_spatial_context"):
            spatial_filter["applied"] = True
            passes_bbox = _bbox_contains_with_buffer(
                spatial_context.get("bbox"),
                route_bbox,
                float(spatial_context.get("lat_buffer_deg") or 0.0),
                float(spatial_context.get("lon_buffer_deg") or 0.0),
            )

            centroid_distance_ft = None
            if route_centroid and spatial_context.get("centroid"):
                centroid_distance_ft = _haversine_feet(
                    float(route_centroid[0]),
                    float(route_centroid[1]),
                    float(spatial_context["centroid"][0]),
                    float(spatial_context["centroid"][1]),
                )
                spatial_filter["centroid_distance_ft"] = round(centroid_distance_ft, 2)

            if not passes_bbox:
                if centroid_distance_ft is None or centroid_distance_ft > 700.0:
                    continue
                spatial_filter["passed"] = True
                spatial_filter["reason"] = "centroid_fallback"
            else:
                spatial_filter["reason"] = "bbox_overlap"

            if centroid_distance_ft is not None:
                spatial_hint_score = max(0.0, 1.0 - (centroid_distance_ft / 900.0)) * 0.18

        plausible_route["_name_hint_score"] = round(min(name_hint_score, 0.12), 3)
        plausible_route["_spatial_hint_score"] = round(spatial_hint_score, 6)
        plausible_route["_spatial_filter"] = spatial_filter
        plausible_route["_hard_length_gate"] = {
            "applied": span_ft > 0.0 and route_length_ft > 0.0,
            "min_allowed_ft": round(span_ft * 0.70, 2) if span_ft > 0.0 else None,
            "max_allowed_ft": round(span_ft * 3.50, 2) if span_ft > 0.0 else None,
            "passed": deferred_oversized_reason is None,
            "deferred_to_subsection_anchor": deferred_oversized_reason is not None,
            "reason": deferred_oversized_reason,
        }
        plausible_routes.append(plausible_route)

    if not plausible_routes:
        plausible_routes = [dict(route) for route in filtered_routes]
        for route in plausible_routes:
            route["_name_hint_score"] = 0.0
            route["_spatial_hint_score"] = 0.0
            route["_spatial_filter"] = {
                "applied": False,
                "passed": True,
                "reason": "fallback_no_plausible_routes",
                "centroid_distance_ft": None,
            }
            route["_hard_length_gate"] = {
                "applied": False,
                "min_allowed_ft": None,
                "max_allowed_ft": None,
                "passed": True,
            }

    plausible_routes = _decorate_route_id_disambiguation(plausible_routes, span_ft, filter_meta)

    filter_meta = dict(filter_meta or {})
    filter_meta["spatial_context"] = spatial_context
    filter_meta["hard_length_gate"] = {
        "applied": span_ft > 0.0,
        "span_ft": round(span_ft, 2),
        "min_allowed_ft": round(span_ft * 0.70, 2) if span_ft > 0.0 else None,
        "max_allowed_ft": round(span_ft * 1.80, 2) if span_ft > 0.0 else None,
        "rejected_count": len(rejected_length_routes),
        "rejected_sample": rejected_length_routes[:25],
    }

    plausible_routes.sort(
        key=lambda route: (
            abs(float(route.get("length_ft", 0.0) or 0.0) - span_ft),
            -float(route.get("_spatial_hint_score", 0.0) or 0.0),
            -float(route.get("_name_hint_score", 0.0) or 0.0),
            float(route.get("length_ft", 0.0) or 0.0),
        )
    )

    return plausible_routes, filter_meta


def _score_route_candidate(group_rows: Sequence[Dict[str, Any]], route: Dict[str, Any], filter_meta: Dict[str, Any], normalized_group: Dict[str, Any]) -> Dict[str, Any]:
    base = _score_route_for_group(group_rows, route)
    route_role = str(route.get("route_role") or "other")
    print_bonus = 0.10 if filter_meta.get("applied") and str(route.get("route_id") or "") in set(filter_meta.get("allowed_route_ids") or []) else 0.0
    route_length_ft = float(route.get("length_ft", 0.0) or 0.0)
    span_ft = float(normalized_group.get("span_ft") or 0.0)

    subsection_plausibility = 0.0
    exact_length_bonus = 0.0
    oversize_penalty = 0.0
    if span_ft > 0.0 and route_length_ft > 0.0:
        ratio = min(span_ft, route_length_ft) / max(span_ft, route_length_ft)
        subsection_plausibility = max(0.0, min(1.0, ratio)) * 0.08

        relative_gap = abs(route_length_ft - span_ft) / max(span_ft, 1.0)
        exact_length_bonus = max(0.0, 1.0 - relative_gap) * 0.16

        if route_length_ft > span_ft:
            oversize_ratio = route_length_ft / max(span_ft, 1.0)
            oversize_penalty = min(0.30, max(0.0, (oversize_ratio - 1.15) * 0.10))

    role_score = 0.04 if route_role in {"underground_cable", "backbone", "terminal_tail"} else 0.0
    name_hint = float(route.get("_name_hint_score", 0.0) or 0.0)
    spatial_hint = float(route.get("_spatial_hint_score", 0.0) or 0.0)
    route_id_disambiguation_bonus = float(route.get("_route_id_disambiguation_bonus", 0.0) or 0.0)

    total_score = (
        float(base.get("score", 0.0) or 0.0)
        + print_bonus
        + subsection_plausibility
        + exact_length_bonus
        + role_score
        + name_hint
        + spatial_hint
        + route_id_disambiguation_bonus
        - oversize_penalty
    )
    total_score = min(1.0, max(0.0, total_score))

    return {
        **base,
        "score": round(total_score, 6),
        "score_breakdown": {
            "base_score": round(float(base.get("score", 0.0) or 0.0), 6),
            "print_bonus": round(print_bonus, 6),
            "subsection_plausibility": round(subsection_plausibility, 6),
            "exact_length_bonus": round(exact_length_bonus, 6),
            "oversize_penalty": round(oversize_penalty, 6),
            "role_score": round(role_score, 6),
            "name_hint_score": round(name_hint, 6),
            "spatial_hint_score": round(spatial_hint, 6),
            "route_id_disambiguation_bonus": round(route_id_disambiguation_bonus, 6),
        },
        "route_id_disambiguation_meta": dict(route.get("_route_id_disambiguation_meta") or {}),
    }


def _candidate_rankings_for_group_v2(group_rows: Sequence[Dict[str, Any]], normalized_group: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any], List[Dict[str, Any]]]:
    candidate_routes, filter_meta = _build_candidate_pool_for_group(normalized_group)
    rankings = [_score_route_candidate(group_rows, route, filter_meta, normalized_group) for route in candidate_routes]
    rankings.sort(key=lambda item: (-float(item.get("score", 0.0) or 0.0), float(item.get("length_gap_ft", 0.0) or 0.0), float(item.get("route_length_ft", 0.0) or 0.0), str(item.get("route_name", ""))))
    return rankings[:8], filter_meta, rankings


def _route_sheet_sequence(route_id: Any) -> List[int]:
    target = str(route_id or "").strip()
    if not target:
        return []
    sheets: List[int] = []
    for token, entry in CURRENT_PACKET_PRINT_SHEET_INDEX.items():
        route_ids = [str(value or "").strip() for value in (entry.get("route_ids") or [])]
        if target not in route_ids:
            continue
        sheet = entry.get("sheet")
        if isinstance(sheet, int) and sheet not in sheets:
            sheets.append(sheet)
    sheets.sort()
    return sheets


def _print_aware_window_bias(route_id: Any, filter_meta: Dict[str, Any], start_ft: float, end_ft: float, route_total_ft: float) -> Dict[str, Any]:
    route_id_text = str(route_id or "").strip()
    sheet_numbers = [int(value) for value in (filter_meta.get("sheet_numbers") or []) if str(value).strip().isdigit()]
    allowed_route_ids = [str(value or "").strip() for value in (filter_meta.get("allowed_route_ids") or []) if str(value or "").strip()]
    if not route_id_text or route_total_ft <= 0.0 or not sheet_numbers:
        return {"bonus": 0.0, "applied": False, "reason": "no_print_sheet_numbers"}
    if allowed_route_ids and route_id_text not in set(allowed_route_ids):
        return {"bonus": 0.0, "applied": False, "reason": "route_not_in_allowed_set"}

    route_sheets = _route_sheet_sequence(route_id_text)
    if not route_sheets:
        return {"bonus": 0.0, "applied": False, "reason": "no_route_sheet_sequence"}

    preferred_fractions: List[float] = []
    if len(route_sheets) == 1:
        preferred_fractions = [0.5]
    else:
        denom = max(1, len(route_sheets) - 1)
        for sheet in sheet_numbers:
            nearest_index = min(range(len(route_sheets)), key=lambda idx: abs(route_sheets[idx] - sheet))
            preferred_fractions.append(nearest_index / denom)
    if not preferred_fractions:
        return {"bonus": 0.0, "applied": False, "reason": "no_preferred_fractions"}

    window_center_ft = max(0.0, min((float(start_ft) + float(end_ft)) / 2.0, float(route_total_ft)))
    window_fraction = window_center_ft / max(float(route_total_ft), 1.0)
    distance = min(abs(window_fraction - fraction) for fraction in preferred_fractions)
    tolerance = 0.18
    normalized_fit = max(0.0, 1.0 - (distance / tolerance))
    bonus = normalized_fit * 0.12
    return {
        "bonus": round(bonus, 6),
        "applied": True,
        "reason": "print_sheet_fraction_bias",
        "window_fraction": round(window_fraction, 6),
        "preferred_fractions": [round(value, 6) for value in preferred_fractions],
        "fraction_distance": round(distance, 6),
        "route_sheets": route_sheets,
    }


def _anchor_route_subsection(route: Dict[str, Any], normalized_group: Dict[str, Any], ranking: Dict[str, Any], filter_meta: Dict[str, Any]) -> Dict[str, Any]:
    route_coords = route.get("coords", []) or []
    route_total_ft = _route_length_ft(route_coords) if route_coords else 0.0
    span_ft = float(normalized_group.get("span_ft") or 0.0)

    if route_total_ft <= 0.0:
        return {
            "route_id": route.get("route_id"),
            "route_name": route.get("route_name"),
            "route_score": float(ranking.get("score", 0.0) or 0.0),
            "subsection_start_ft": 0.0,
            "subsection_end_ft": 0.0,
            "subsection_score": 0.0,
            "combined_score": round(float(ranking.get("score", 0.0) or 0.0), 6),
            "anchor_method": "invalid_route_geometry",
            "anchor_reasons": ["Route geometry length was zero."],
            "mapping": _resolve_station_mapping(normalized_group.get("station_rows") or [], 0.0),
            "score_breakdown": dict(ranking.get("score_breakdown") or {}),
        }

    windows = _generate_segment_windows(route_coords, span_ft)
    if not windows:
        fallback_mapping = _resolve_station_mapping(normalized_group.get("station_rows") or [], route_total_ft)
        return {
            "route_id": route.get("route_id"),
            "route_name": route.get("route_name"),
            "route_score": round(float(ranking.get("score", 0.0) or 0.0), 6),
            "subsection_start_ft": 0.0,
            "subsection_end_ft": round(min(route_total_ft, span_ft or route_total_ft), 2),
            "subsection_score": 0.0,
            "combined_score": round(float(ranking.get("score", 0.0) or 0.0), 6),
            "anchor_method": "no_segment_windows",
            "anchor_reasons": ["No sliding-window segment hypotheses were generated."],
            "mapping": fallback_mapping,
            "score_breakdown": dict(ranking.get("score_breakdown") or {}),
        }

    scored_windows = []
    for window in windows:
        scored = {
            **window,
            **_score_segment_window(route_coords, normalized_group, window),
        }
        bias_meta = _print_aware_window_bias(route.get("route_id"), filter_meta, float(window.get("start_ft") or 0.0), float(window.get("end_ft") or 0.0), float(route_total_ft))
        print_bias_bonus = float(bias_meta.get("bonus", 0.0) or 0.0)
        scored["print_aware_window_bias"] = bias_meta
        scored["window_score_base"] = round(float(scored.get("window_score", 0.0) or 0.0), 6)
        scored["window_score"] = round(min(1.0, max(0.0, float(scored.get("window_score", 0.0) or 0.0) + print_bias_bonus)), 6)
        scored_windows.append(scored)

    scored_windows.sort(
        key=lambda item: (
            -float(item.get("window_score", 0.0) or 0.0),
            -float((item.get("print_aware_window_bias") or {}).get("bonus", 0.0) or 0.0),
            abs(float(item.get("end_ft", 0.0) or 0.0) - float(item.get("start_ft", 0.0) or 0.0) - span_ft),
            float(item.get("start_ft", 0.0) or 0.0),
        )
    )
    best_window = scored_windows[0]

    mapping = dict(best_window.get("mapping") or {})
    anchor_reasons = list(best_window.get("window_reasons") or [])
    if filter_meta.get("applied"):
        anchor_reasons.append("Print-aware filtering narrowed the route family before sliding-window segment scoring.")
    else:
        anchor_reasons.append("No print-aware narrowing was available, so sliding-window scoring relied on KMZ route geometry and span fit.")

    mapping["anchor_strategy"] = "true_sliding_window_segment_scorer"
    mapping["anchor_basis"] = {
        **dict(mapping.get("anchor_basis") or {}),
        "print_tokens": list(normalized_group.get("print_tokens") or []),
        "filter_applied": bool(filter_meta.get("applied")),
        "route_total_ft": round(float(route_total_ft), 2),
        "group_span_ft": round(float(span_ft), 2),
        "segment_window_count": len(scored_windows),
        "segment_window_preview": [
            {
                "start_ft": round(float(item.get("start_ft", 0.0) or 0.0), 2),
                "end_ft": round(float(item.get("end_ft", 0.0) or 0.0), 2),
                "window_type": item.get("window_type"),
                "window_score": round(float(item.get("window_score", 0.0) or 0.0), 6),
            }
            for item in scored_windows[:12]
        ],
    }

    combined_score = min(
        1.0,
        float(ranking.get("score", 0.0) or 0.0) + float(best_window.get("window_score", 0.0) or 0.0) * 0.35,
    )

    return {
        "route_id": route.get("route_id"),
        "route_name": route.get("route_name"),
        "route_score": round(float(ranking.get("score", 0.0) or 0.0), 6),
        "subsection_start_ft": round(float(best_window.get("start_ft", 0.0) or 0.0), 2),
        "subsection_end_ft": round(float(best_window.get("end_ft", 0.0) or 0.0), 2),
        "subsection_score": round(float(best_window.get("window_score", 0.0) or 0.0), 6),
        "combined_score": round(combined_score, 6),
        "anchor_method": "true_sliding_window_segment_scorer",
        "anchor_reasons": anchor_reasons,
        "anchor_profile": dict(best_window.get("window_profile") or {}),
        "mapping": mapping,
        "score_breakdown": dict(ranking.get("score_breakdown") or {}),
    }


def _build_validation_checks(
    normalized_group: Dict[str, Any],
    anchored_hypotheses: Sequence[Dict[str, Any]],
    mapping: Dict[str, Any],
    mapped_station_points: Sequence[Dict[str, Any]],
    matched_route: Dict[str, Any],
) -> Dict[str, Any]:
    checks: List[Dict[str, Any]] = []

    if anchored_hypotheses:
        best = anchored_hypotheses[0]
        second = anchored_hypotheses[1] if len(anchored_hypotheses) > 1 else None
        gap = float(best.get("combined_score", 0.0) or 0.0) - float(second.get("combined_score", 0.0) or 0.0) if second else 1.0
        status = "pass"
        message = f"Top candidate score gap is {round(gap, 4)}."
        if gap < 0.02:
            status = "fail"
            message = f"Top two anchored candidates are nearly tied with a score gap of {round(gap, 4)}."
        elif gap < 0.05:
            status = "warn"
            message = f"Top two anchored candidates are close with a score gap of {round(gap, 4)}."
        checks.append({"check": "route_ambiguity", "status": status, "message": message})

    mapped_values = [float(point.get("mapped_station_ft") or 0.0) for point in mapped_station_points if point.get("mapped_station_ft") is not None]
    monotonic_status = "pass"
    monotonic_message = "Mapped stations are strictly increasing."
    for idx in range(1, len(mapped_values)):
        if mapped_values[idx] <= mapped_values[idx - 1]:
            monotonic_status = "fail"
            monotonic_message = "Mapped station feet are not strictly increasing."
            break
    checks.append({"check": "mapped_station_monotonicity", "status": monotonic_status, "message": monotonic_message})

    source_values = [float(row.get("station_ft") or 0.0) for row in normalized_group.get("station_rows") or [] if row.get("station_ft") is not None]
    source_span = (max(source_values) - min(source_values)) if len(source_values) >= 2 else 0.0
    mapped_span = (mapped_values[-1] - mapped_values[0]) if len(mapped_values) >= 2 else 0.0
    span_status = "pass"
    span_message = "Mapped span is consistent with source station span."
    if source_span > 0.0:
        span_ratio = abs(mapped_span - source_span) / source_span
        if span_ratio > 0.35:
            span_status = "fail"
            span_message = f"Mapped span deviates from source span by {round(span_ratio * 100.0, 2)}%."
        elif span_ratio > 0.15:
            span_status = "warn"
            span_message = f"Mapped span deviates from source span by {round(span_ratio * 100.0, 2)}%."
    checks.append({"check": "span_integrity", "status": span_status, "message": span_message})

    spacing_ratios: List[float] = []
    for idx in range(1, min(len(source_values), len(mapped_values))):
        src_delta = source_values[idx] - source_values[idx - 1]
        mapped_delta = mapped_values[idx] - mapped_values[idx - 1]
        if src_delta > 0.0:
            spacing_ratios.append(mapped_delta / src_delta)
    spacing_status = "pass"
    spacing_message = "Mapped station spacing tracks source station spacing."
    if spacing_ratios:
        min_ratio = min(spacing_ratios)
        max_ratio = max(spacing_ratios)
        if min_ratio < 0.50 or max_ratio > 1.75:
            spacing_status = "fail"
            spacing_message = f"Mapped station spacing is distorted (ratio range {round(min_ratio, 3)} to {round(max_ratio, 3)})."
        elif min_ratio < 0.80 or max_ratio > 1.25:
            spacing_status = "warn"
            spacing_message = f"Mapped station spacing is somewhat distorted (ratio range {round(min_ratio, 3)} to {round(max_ratio, 3)})."
    checks.append({"check": "spacing_distortion", "status": spacing_status, "message": spacing_message})

    route_total_ft = float(matched_route.get("length_ft", 0.0) or 0.0)
    edge_status = "pass"
    edge_message = "Mapped stations are not clamped to the route edges."
    if mapped_values and route_total_ft > 0.0:
        near_start = sum(1 for value in mapped_values if value <= 5.0)
        near_end = sum(1 for value in mapped_values if abs(route_total_ft - value) <= 5.0)
        if near_start >= 2 or near_end >= 2:
            edge_status = "warn"
            edge_message = "Multiple mapped stations fall very close to the route start or end, which may indicate an anchor issue."
    checks.append({"check": "edge_clamp", "status": edge_status, "message": edge_message})

    anchor_strategy = str(mapping.get("anchor_strategy") or "")
    anchor_status = "pass"
    anchor_message = f"Anchor strategy used: {anchor_strategy or 'unspecified'}."
    if anchor_strategy in {"group_relative_origin", "full_route_fallback", "none", ""}:
        anchor_status = "warn"
        anchor_message = f"Anchor strategy '{anchor_strategy or 'unspecified'}' is still a fallback-style anchor and should be treated cautiously."
    elif anchor_strategy == "balanced_span_search":
        anchor_status = "warn"
        anchor_message = "Anchor strategy 'balanced_span_search' is an inferred subsection anchor. Better than route-origin fallback, but still not absolute proof."
    elif anchor_strategy == "absolute_station_window":
        anchor_status = "pass"
        anchor_message = "Anchor strategy 'absolute_station_window' aligned the bore span to a plausible absolute station window on the selected route."
    checks.append({"check": "anchor_confidence", "status": anchor_status, "message": anchor_message})

    overall = "pass"
    if any(check["status"] == "fail" for check in checks):
        overall = "fail"
    elif any(check["status"] == "warn" for check in checks):
        overall = "warn"

    probable_failure_class = None
    failed_or_warned = {check["check"]: check["status"] for check in checks if check["status"] in {"warn", "fail"}}
    if failed_or_warned.get("route_ambiguity") == "fail":
        probable_failure_class = "AMBIGUOUS_MATCH"
    elif failed_or_warned.get("mapped_station_monotonicity") == "fail":
        probable_failure_class = "STATION_NORMALIZATION_ISSUE"
    elif failed_or_warned.get("anchor_confidence") in {"warn", "fail"}:
        probable_failure_class = "BAD_ANCHOR"
    elif failed_or_warned.get("spacing_distortion") == "fail":
        probable_failure_class = "RIGHT_ROUTE_WRONG_SCALING"
    elif failed_or_warned.get("span_integrity") == "fail":
        probable_failure_class = "RIGHT_ROUTE_WRONG_POSITION"

    confidence_label = "HIGH"
    if overall == "fail":
        confidence_label = "LOW"
    elif overall == "warn":
        confidence_label = "MEDIUM"

    return {
        "validation_status": overall,
        "confidence_label": confidence_label,
        "probable_failure_class": probable_failure_class,
        "checks": checks,
    }


def _build_matching_debug_record(
    normalized_group: Dict[str, Any],
    filter_meta: Dict[str, Any],
    rankings: Sequence[Dict[str, Any]],
    anchored_hypotheses: Sequence[Dict[str, Any]],
    selected_hypothesis: Dict[str, Any],
    validation: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "group_id": normalized_group.get("group_id"),
        "source_file": normalized_group.get("source_file"),
        "normalized_group": normalized_group,
        "print_filter": dict(filter_meta),
        "candidate_routes": list(rankings),
        "anchored_hypotheses": list(anchored_hypotheses),
        "selected_hypothesis": dict(selected_hypothesis),
        "validation": dict(validation),
    }



def _candidate_is_billable(
    validation: Dict[str, Any],
    hypothesis: Dict[str, Any],
    normalized_group: Dict[str, Any],
) -> Tuple[bool, List[str]]:
    reasons: List[str] = []
    checks = {str(check.get("check") or ""): str(check.get("status") or "") for check in validation.get("checks") or []}

    for required_check in ("mapped_station_monotonicity", "span_integrity", "spacing_distortion"):
        if checks.get(required_check) != "pass":
            reasons.append(f"{required_check}={checks.get(required_check) or 'missing'}")

    if checks.get("edge_clamp") == "fail":
        reasons.append("edge_clamp=fail")

    profile = dict(hypothesis.get("anchor_profile") or {})
    segment_length_ft = float(profile.get("segment_length_ft") or 0.0)
    source_span_ft = float(normalized_group.get("span_ft") or 0.0)
    if source_span_ft > 0.0:
        coverage_ratio = segment_length_ft / source_span_ft
        if coverage_ratio < 0.90:
            reasons.append(f"segment_coverage_ratio={round(coverage_ratio, 4)}")

    projected_points = list(profile.get("projected_points") or [])
    if projected_points:
        unique_projected = {
            (round(float(point.get("lat") or 0.0), 7), round(float(point.get("lon") or 0.0), 7))
            for point in projected_points
        }
        if len(unique_projected) < max(3, int(len(projected_points) * 0.65)):
            reasons.append("projected_points_clamped")

    return (len(reasons) == 0, reasons)


def _authoritative_selection_bundle(
    selected_hypothesis: Dict[str, Any],
    matched_route: Dict[str, Any],
    selected_ranking: Dict[str, Any],
    mapping: Dict[str, Any],
    evaluated_hypotheses: Sequence[Dict[str, Any]],
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any], str]:
    selected_copy = dict(selected_hypothesis or {})
    matched_route_copy = dict(matched_route or {})
    ranking_copy = dict(selected_ranking or {})
    mapping_copy = dict(mapping or {})

    consensus_gate = dict(selected_copy.get("route_consensus_gate") or {})
    authoritative_route_id = str(consensus_gate.get("consensus_route_id") or selected_copy.get("route_id") or "").strip()
    if not authoritative_route_id:
        authoritative_route_id = str(matched_route_copy.get("route_id") or "").strip()

    authoritative_bundle: Optional[Dict[str, Any]] = None
    for item in evaluated_hypotheses or []:
        hypothesis = dict(item.get("hypothesis") or {})
        item_route_id = str(hypothesis.get("route_id") or "").strip()
        commit_meta = dict(hypothesis.get("authoritative_route_commit") or {})
        committed = bool(commit_meta.get("committed"))
        if item_route_id and item_route_id == authoritative_route_id and (committed or authoritative_route_id):
            authoritative_bundle = dict(item)
            break

    if authoritative_bundle:
        selected_copy = dict(authoritative_bundle.get("hypothesis") or selected_copy)
        matched_route_copy = dict(authoritative_bundle.get("matched_route") or matched_route_copy)
        ranking_copy = dict(authoritative_bundle.get("ranking") or ranking_copy)
        mapping_copy = dict(authoritative_bundle.get("mapping") or mapping_copy)

    selected_copy["authoritative_route_id"] = authoritative_route_id or None
    mapping_copy["authoritative_route_id"] = authoritative_route_id or None
    if authoritative_route_id:
        matched_route_copy["route_id"] = authoritative_route_id
        ranking_copy["route_id"] = authoritative_route_id

    return selected_copy, matched_route_copy, ranking_copy, mapping_copy, authoritative_route_id


def _select_best_hypothesis_with_gate(
    group: Sequence[Dict[str, Any]],
    normalized_group: Dict[str, Any],
    rankings: Sequence[Dict[str, Any]],
    filter_meta: Dict[str, Any],
    anchored_hypotheses: Sequence[Dict[str, Any]],
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any], List[Dict[str, Any]]]:
    evaluated: List[Dict[str, Any]] = []

    for hypothesis in anchored_hypotheses:
        route_id = str(hypothesis.get("route_id") or "")
        matched_route = _find_route_by_id(route_id)
        if not matched_route:
            continue

        ranking = next((item for item in rankings if str(item.get("route_id") or "") == route_id), {})
        mapping = dict(hypothesis.get("mapping") or _resolve_station_mapping(group, float(matched_route.get("length_ft", 0.0) or 0.0)))
        station_points, mapping = _build_station_points_for_group(group, matched_route, rankings, filter_meta, mapping)
        validation = _build_validation_checks(normalized_group, anchored_hypotheses, mapping, station_points, matched_route)
        is_billable, gate_reasons = _candidate_is_billable(validation, hypothesis, normalized_group)

        enriched = dict(hypothesis)
        enriched["route_length_ft"] = round(float(matched_route.get("length_ft", 0.0) or 0.0), 2)
        enriched["mapping"] = mapping
        enriched["preselection_validation"] = dict(validation)
        enriched["billable_candidate"] = bool(is_billable)
        enriched["billable_gate_reasons"] = list(gate_reasons)

        evaluated.append({
            "hypothesis": enriched,
            "matched_route": dict(matched_route),
            "ranking": dict(ranking),
            "mapping": dict(mapping),
            "validation": dict(validation),
            "is_billable": bool(is_billable),
            "gate_reasons": list(gate_reasons),
        })

    if not evaluated:
        raise ValueError("No anchored hypotheses could be evaluated.")

    evaluated = _apply_physical_feasibility_gate(evaluated, normalized_group)
    evaluated = _apply_segment_fit_gate(evaluated)
    evaluated = _apply_boundary_exactness_gate(evaluated, normalized_group)
    evaluated = _apply_continuity_gate(evaluated)
    evaluated = _apply_chain_gate(evaluated, normalized_group)
    evaluated = _apply_node_resolution_gate(evaluated, normalized_group)
    evaluated = _apply_route_uniqueness_gate(evaluated)
    evaluated = _apply_route_consensus_gate(evaluated, rankings, anchored_hypotheses)
    evaluated = _apply_geometry_lock_gate(evaluated)

    authoritative_candidates: List[Dict[str, Any]] = []
    for item in evaluated:
        item_copy = dict(item)
        hypothesis_copy = dict(item_copy.get("hypothesis") or {})
        consensus_gate = dict(hypothesis_copy.get("route_consensus_gate") or {})
        consensus_route_id = str(consensus_gate.get("consensus_route_id") or "").strip()
        route_id = str(hypothesis_copy.get("route_id") or "").strip()
        is_authoritative = bool(consensus_route_id) and route_id == consensus_route_id
        hypothesis_copy["authoritative_route_commit"] = {
            "committed": is_authoritative,
            "reason": "route_consensus_authoritative_commit" if is_authoritative else "not_consensus_route",
            "consensus_route_id": consensus_route_id or None,
        }
        item_copy["hypothesis"] = hypothesis_copy
        if is_authoritative:
            authoritative_candidates.append(item_copy)

    if authoritative_candidates:
        authoritative_candidates.sort(
            key=lambda item: (
                0 if item["is_billable"] else 1,
                -float(item["hypothesis"].get("route_score", 0.0) or 0.0),
                -float(item["hypothesis"].get("combined_score", 0.0) or 0.0),
                -float(item["hypothesis"].get("subsection_score", 0.0) or 0.0),
                str(item["hypothesis"].get("route_name", "")),
            )
        )
        authoritative_route_id = str((authoritative_candidates[0].get("hypothesis") or {}).get("route_id") or "").strip()
        resorted: List[Dict[str, Any]] = []
        for item in evaluated:
            item_copy = dict(item)
            hypothesis_copy = dict(item_copy.get("hypothesis") or {})
            route_id = str(hypothesis_copy.get("route_id") or "").strip()
            commit_meta = dict(hypothesis_copy.get("authoritative_route_commit") or {})
            commit_meta["committed"] = route_id == authoritative_route_id
            commit_meta["reason"] = "route_consensus_authoritative_commit" if route_id == authoritative_route_id else "authoritative_route_commit_superseded"
            commit_meta["consensus_route_id"] = authoritative_route_id or None
            hypothesis_copy["authoritative_route_commit"] = commit_meta
            item_copy["hypothesis"] = hypothesis_copy
            resorted.append(item_copy)
        evaluated = sorted(
            resorted,
            key=lambda item: (
                0 if bool((item.get("hypothesis") or {}).get("authoritative_route_commit", {}).get("committed")) else 1,
                0 if item["is_billable"] else 1,
                -float(item["hypothesis"].get("route_score", 0.0) or 0.0),
                -float(item["hypothesis"].get("combined_score", 0.0) or 0.0),
                -float(item["hypothesis"].get("subsection_score", 0.0) or 0.0),
                str(item["hypothesis"].get("route_name", "")),
            )
        )
    else:
        evaluated.sort(
            key=lambda item: (
                0 if item["is_billable"] else 1,
                -float(item["hypothesis"].get("subsection_score", 0.0) or 0.0),
                -float(item["hypothesis"].get("combined_score", 0.0) or 0.0),
                -float(item["hypothesis"].get("route_score", 0.0) or 0.0),
                str(item["hypothesis"].get("route_name", "")),
            )
        )

    winner = evaluated[0]
    return (
        dict(winner["hypothesis"]),
        dict(winner["matched_route"]),
        dict(winner["ranking"]),
        dict(winner["mapping"]),
        evaluated,
    )



def _apply_route_uniqueness_gate(
    evaluated: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    billable_items = [dict(item) for item in evaluated if bool(item.get("is_billable"))]
    if not billable_items:
        return [dict(item) for item in evaluated]

    billable_items.sort(
        key=lambda item: (
            -float(item.get("hypothesis", {}).get("subsection_score", 0.0) or 0.0),
            -float(item.get("hypothesis", {}).get("combined_score", 0.0) or 0.0),
            -float(item.get("hypothesis", {}).get("route_score", 0.0) or 0.0),
            str(item.get("hypothesis", {}).get("route_id", "")),
        )
    )

    winner = billable_items[0]
    winner_hypothesis = dict(winner.get("hypothesis") or {})
    winner_subsection = float(winner_hypothesis.get("subsection_score", 0.0) or 0.0)
    winner_combined = float(winner_hypothesis.get("combined_score", 0.0) or 0.0)

    competing_billable = []
    for other in billable_items[1:]:
        other_hypothesis = dict(other.get("hypothesis") or {})
        other_subsection = float(other_hypothesis.get("subsection_score", 0.0) or 0.0)
        other_combined = float(other_hypothesis.get("combined_score", 0.0) or 0.0)

        subsection_gap = winner_subsection - other_subsection
        combined_gap = winner_combined - other_combined

        if subsection_gap < 0.08 or combined_gap < 0.06:
            competing_billable.append({
                "route_id": other_hypothesis.get("route_id"),
                "route_name": other_hypothesis.get("route_name"),
                "subsection_score": round(other_subsection, 6),
                "combined_score": round(other_combined, 6),
                "subsection_gap_vs_winner": round(subsection_gap, 6),
                "combined_gap_vs_winner": round(combined_gap, 6),
            })

    if not competing_billable:
        winner_hypothesis["route_uniqueness_gate"] = {
            "passed": True,
            "reason": "single_clear_billable_candidate",
            "competing_billable_candidates": [],
        }
        winner["hypothesis"] = winner_hypothesis

        updated = []
        winner_route_id = str(winner_hypothesis.get("route_id") or "")
        for item in evaluated:
            item_copy = dict(item)
            hypothesis_copy = dict(item_copy.get("hypothesis") or {})
            if str(hypothesis_copy.get("route_id") or "") == winner_route_id:
                hypothesis_copy["route_uniqueness_gate"] = dict(winner_hypothesis["route_uniqueness_gate"])
            item_copy["hypothesis"] = hypothesis_copy
            updated.append(item_copy)
        return updated

    updated: List[Dict[str, Any]] = []
    competing_ids = {str(item.get("route_id") or "") for item in competing_billable}
    winner_route_id = str(winner_hypothesis.get("route_id") or "")

    for item in evaluated:
        item_copy = dict(item)
        hypothesis_copy = dict(item_copy.get("hypothesis") or {})
        route_id = str(hypothesis_copy.get("route_id") or "")
        if route_id == winner_route_id:
            hypothesis_copy["billable_candidate"] = False
            reasons = list(hypothesis_copy.get("billable_gate_reasons") or [])
            reasons.append("route_uniqueness_failed_winner")
            hypothesis_copy["billable_gate_reasons"] = reasons
            hypothesis_copy["route_uniqueness_gate"] = {
                "passed": False,
                "reason": "multiple_billable_routes",
                "competing_billable_candidates": list(competing_billable),
            }
            item_copy["is_billable"] = False
            item_copy["gate_reasons"] = list(reasons)
        elif route_id in competing_ids:
            hypothesis_copy["billable_candidate"] = False
            reasons = list(hypothesis_copy.get("billable_gate_reasons") or [])
            reasons.append("route_uniqueness_failed_competitor")
            hypothesis_copy["billable_gate_reasons"] = reasons
            hypothesis_copy["route_uniqueness_gate"] = {
                "passed": False,
                "reason": "multiple_billable_routes",
                "competing_billable_candidates": list(competing_billable),
            }
            item_copy["is_billable"] = False
            item_copy["gate_reasons"] = list(reasons)
        item_copy["hypothesis"] = hypothesis_copy
        updated.append(item_copy)

    return updated



def _point_to_segment_distance_feet(
    point_lat: float,
    point_lon: float,
    a_lat: float,
    a_lon: float,
    b_lat: float,
    b_lon: float,
) -> float:
    # Local planar approximation is good enough at this scale.
    mean_lat = math.radians((point_lat + a_lat + b_lat) / 3.0)
    feet_per_deg_lat = 364000.0
    feet_per_deg_lon = 364000.0 * max(0.2, math.cos(mean_lat))

    px = point_lon * feet_per_deg_lon
    py = point_lat * feet_per_deg_lat
    ax = a_lon * feet_per_deg_lon
    ay = a_lat * feet_per_deg_lat
    bx = b_lon * feet_per_deg_lon
    by = b_lat * feet_per_deg_lat

    abx = bx - ax
    aby = by - ay
    apx = px - ax
    apy = py - ay
    denom = abx * abx + aby * aby
    if denom <= 1e-9:
        dx = px - ax
        dy = py - ay
        return math.sqrt(dx * dx + dy * dy)

    t = max(0.0, min(1.0, (apx * abx + apy * aby) / denom))
    cx = ax + abx * t
    cy = ay + aby * t
    dx = px - cx
    dy = py - cy
    return math.sqrt(dx * dx + dy * dy)


def _point_to_route_distance_feet(point_lat: float, point_lon: float, route_coords: Sequence[Sequence[float]]) -> float:
    if not route_coords:
        return float("inf")
    if len(route_coords) == 1:
        only = route_coords[0]
        return _haversine_feet(point_lat, point_lon, float(only[0]), float(only[1]))

    best = float("inf")
    for idx in range(1, len(route_coords)):
        a = route_coords[idx - 1]
        b = route_coords[idx]
        if len(a) < 2 or len(b) < 2:
            continue
        dist = _point_to_segment_distance_feet(
            point_lat,
            point_lon,
            float(a[0]),
            float(a[1]),
            float(b[0]),
            float(b[1]),
        )
        if dist < best:
            best = dist
    return best


def _apply_route_consensus_gate(
    evaluated: Sequence[Dict[str, Any]],
    rankings: Sequence[Dict[str, Any]],
    anchored_hypotheses: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    top_ranked_route_id = str((rankings[0] or {}).get("route_id") or "") if rankings else ""
    top_anchored_route_id = str((anchored_hypotheses[0] or {}).get("route_id") or "") if anchored_hypotheses else ""

    consensus_route_id = ""
    if top_ranked_route_id and top_ranked_route_id == top_anchored_route_id:
        consensus_route_id = top_ranked_route_id

    if not consensus_route_id:
        return [dict(item) for item in evaluated]

    top_ranked_score = float((rankings[0] or {}).get("score", 0.0) or 0.0) if rankings else 0.0
    second_ranked_score = float((rankings[1] or {}).get("score", 0.0) or 0.0) if len(rankings) > 1 else 0.0
    ranked_gap = top_ranked_score - second_ranked_score

    top_anchored_score = float((anchored_hypotheses[0] or {}).get("combined_score", 0.0) or 0.0) if anchored_hypotheses else 0.0
    second_anchored_score = float((anchored_hypotheses[1] or {}).get("combined_score", 0.0) or 0.0) if len(anchored_hypotheses) > 1 else 0.0
    anchored_gap = top_anchored_score - second_anchored_score

    if ranked_gap < 0.03 and anchored_gap < 0.03:
        return [dict(item) for item in evaluated]

    updated: List[Dict[str, Any]] = []
    for item in evaluated:
        item_copy = dict(item)
        hypothesis_copy = dict(item_copy.get("hypothesis") or {})
        route_id = str(hypothesis_copy.get("route_id") or "")

        consensus_gate = {
            "passed": route_id == consensus_route_id,
            "reason": "top_ranked_and_top_anchored_route_agree",
            "consensus_route_id": consensus_route_id,
            "top_ranked_route_id": top_ranked_route_id,
            "top_anchored_route_id": top_anchored_route_id,
            "ranked_gap": round(ranked_gap, 6),
            "anchored_gap": round(anchored_gap, 6),
        }

        if bool(item_copy.get("is_billable")) and route_id != consensus_route_id:
            reasons = list(hypothesis_copy.get("billable_gate_reasons") or [])
            reasons.append("route_consensus_failed")
            hypothesis_copy["billable_candidate"] = False
            hypothesis_copy["billable_gate_reasons"] = reasons
            item_copy["is_billable"] = False
            item_copy["gate_reasons"] = list(reasons)

        hypothesis_copy["route_consensus_gate"] = consensus_gate
        item_copy["hypothesis"] = hypothesis_copy
        updated.append(item_copy)

    return updated


def _apply_geometry_lock_gate(
    evaluated: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    winner_candidates = [dict(item) for item in evaluated if bool(item.get("is_billable"))]
    if not winner_candidates:
        return [dict(item) for item in evaluated]

    winner_candidates.sort(
        key=lambda item: (
            -float(item.get("hypothesis", {}).get("subsection_score", 0.0) or 0.0),
            -float(item.get("hypothesis", {}).get("combined_score", 0.0) or 0.0),
            str(item.get("hypothesis", {}).get("route_id", "")),
        )
    )
    winner = winner_candidates[0]
    winner_hypothesis = dict(winner.get("hypothesis") or {})
    winner_route_id = str(winner_hypothesis.get("route_id") or "")
    winner_points = list((winner_hypothesis.get("anchor_profile") or {}).get("projected_points") or [])

    if len(winner_points) < 3:
        winner_hypothesis["geometry_lock_gate"] = {
            "passed": False,
            "reason": "insufficient_projected_points",
            "competing_parallel_routes": [],
        }
        winner_hypothesis["billable_candidate"] = False
        reasons = list(winner_hypothesis.get("billable_gate_reasons") or [])
        reasons.append("geometry_lock_insufficient_points")
        winner_hypothesis["billable_gate_reasons"] = reasons

        updated = []
        for item in evaluated:
            item_copy = dict(item)
            hypothesis_copy = dict(item_copy.get("hypothesis") or {})
            if str(hypothesis_copy.get("route_id") or "") == winner_route_id:
                hypothesis_copy.update(winner_hypothesis)
                item_copy["hypothesis"] = hypothesis_copy
                item_copy["is_billable"] = False
                item_copy["gate_reasons"] = list(reasons)
            updated.append(item_copy)
        return updated

    parallel_conflicts = []
    for other in winner_candidates[1:]:
        other_hypothesis = dict(other.get("hypothesis") or {})
        other_route = dict(other.get("matched_route") or {})
        other_coords = list(other_route.get("coords") or [])
        if not other_coords:
            continue

        distances = []
        for pt in winner_points:
            lat = float(pt.get("lat") or 0.0)
            lon = float(pt.get("lon") or 0.0)
            distances.append(_point_to_route_distance_feet(lat, lon, other_coords))

        if not distances:
            continue

        avg_dist = sum(distances) / len(distances)
        max_dist = max(distances)
        near_count = sum(1 for d in distances if d <= 18.0)
        near_ratio = near_count / max(1, len(distances))

        if near_ratio >= 0.75 and avg_dist <= 15.0 and max_dist <= 28.0:
            parallel_conflicts.append({
                "route_id": other_hypothesis.get("route_id"),
                "route_name": other_hypothesis.get("route_name"),
                "avg_distance_ft": round(avg_dist, 3),
                "max_distance_ft": round(max_dist, 3),
                "near_ratio": round(near_ratio, 4),
                "subsection_score": round(float(other_hypothesis.get("subsection_score", 0.0) or 0.0), 6),
                "combined_score": round(float(other_hypothesis.get("combined_score", 0.0) or 0.0), 6),
            })

    updated = []
    if parallel_conflicts:
        winner_hypothesis["geometry_lock_gate"] = {
            "passed": False,
            "reason": "parallel_route_conflict",
            "competing_parallel_routes": parallel_conflicts,
        }
        winner_hypothesis["billable_candidate"] = False
        reasons = list(winner_hypothesis.get("billable_gate_reasons") or [])
        reasons.append("geometry_lock_parallel_conflict")
        winner_hypothesis["billable_gate_reasons"] = reasons

        for item in evaluated:
            item_copy = dict(item)
            hypothesis_copy = dict(item_copy.get("hypothesis") or {})
            if str(hypothesis_copy.get("route_id") or "") == winner_route_id:
                hypothesis_copy.update(winner_hypothesis)
                item_copy["hypothesis"] = hypothesis_copy
                item_copy["is_billable"] = False
                item_copy["gate_reasons"] = list(reasons)
            updated.append(item_copy)
        return updated

    winner_hypothesis["geometry_lock_gate"] = {
        "passed": True,
        "reason": "no_parallel_route_conflict_detected",
        "competing_parallel_routes": [],
    }

    for item in evaluated:
        item_copy = dict(item)
        hypothesis_copy = dict(item_copy.get("hypothesis") or {})
        if str(hypothesis_copy.get("route_id") or "") == winner_route_id:
            hypothesis_copy["geometry_lock_gate"] = dict(winner_hypothesis["geometry_lock_gate"])
        item_copy["hypothesis"] = hypothesis_copy
        updated.append(item_copy)

    return updated



def _apply_physical_feasibility_gate(
    evaluated: Sequence[Dict[str, Any]],
    normalized_group: Dict[str, Any],
) -> List[Dict[str, Any]]:
    source_span_ft = float(normalized_group.get("span_ft") or 0.0)
    min_span_ratio = 0.85
    max_span_ratio = 3.50

    if source_span_ft <= 0.0:
        return [dict(item) for item in evaluated]

    min_valid_ft = source_span_ft * min_span_ratio
    max_valid_ft = source_span_ft * max_span_ratio

    updated: List[Dict[str, Any]] = []
    for item in evaluated:
        item_copy = dict(item)
        hypothesis = dict(item_copy.get("hypothesis") or {})
        matched_route = dict(item_copy.get("matched_route") or {})
        route_length_ft = float(matched_route.get("length_ft") or hypothesis.get("route_length_ft") or 0.0)

        gate = {
            "passed": True,
            "reason": "within_physical_span_bounds",
            "route_length_ft": round(route_length_ft, 2),
            "source_span_ft": round(source_span_ft, 2),
            "min_valid_ft": round(min_valid_ft, 2),
            "max_valid_ft": round(max_valid_ft, 2),
            "min_span_ratio": min_span_ratio,
            "max_span_ratio": max_span_ratio,
        }

        reasons = list(hypothesis.get("billable_gate_reasons") or [])

        if route_length_ft < min_valid_ft:
            gate["passed"] = False
            gate["reason"] = "route_too_short_for_bore_span"
            reasons.append("physical_feasibility_route_too_short")
            hypothesis["billable_candidate"] = False
            item_copy["is_billable"] = False
            item_copy["gate_reasons"] = reasons

        elif route_length_ft > max_valid_ft:
            gate["passed"] = False
            gate["reason"] = "route_too_long_for_bore_span"
            reasons.append("physical_feasibility_route_too_long")
            hypothesis["billable_candidate"] = False
            item_copy["is_billable"] = False
            item_copy["gate_reasons"] = reasons

        hypothesis["billable_gate_reasons"] = reasons
        hypothesis["physical_feasibility_gate"] = gate
        item_copy["hypothesis"] = hypothesis
        updated.append(item_copy)

    return updated



def _apply_segment_fit_gate(
    evaluated: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    updated: List[Dict[str, Any]] = []

    for item in evaluated:
        item_copy = dict(item)
        hypothesis = dict(item_copy.get("hypothesis") or {})
        profile = dict(hypothesis.get("anchor_profile") or {})
        projected_points = list(profile.get("projected_points") or [])

        gate = {
            "passed": True,
            "reason": "segment_fit_valid",
            "min_point_count": 4,
            "min_unique_segment_ratio": 0.20,
            "min_route_progress_ft": 250.0,
            "max_segment_jump": 2,
            "details": {},
        }

        reasons = list(hypothesis.get("billable_gate_reasons") or [])

        if len(projected_points) < 4:
            gate["passed"] = False
            gate["reason"] = "insufficient_projected_points"
            reasons.append("segment_fit_insufficient_points")
            hypothesis["billable_candidate"] = False
            item_copy["is_billable"] = False
            item_copy["gate_reasons"] = reasons
        else:
            route_fts = [float(point.get("route_ft") or 0.0) for point in projected_points]
            segment_indices = [int(point.get("segment_index") or 0) for point in projected_points]
            actual_segment_indices = [int(point.get("actual_segment_index", point.get("segment_index") or 0)) for point in projected_points]
            virtual_segment_count = max(1, max(int(point.get("virtual_segment_count") or 1) for point in projected_points))

            route_progress_ft = max(route_fts) - min(route_fts) if route_fts else 0.0
            unique_segments = len(set(segment_indices))
            unique_actual_segments = len(set(actual_segment_indices))
            segment_ratio = unique_segments / max(1, len(projected_points) - 1)
            route_coverage_ratio = unique_segments / max(1, virtual_segment_count)

            segment_jumps = [
                abs(segment_indices[idx] - segment_indices[idx - 1])
                for idx in range(1, len(segment_indices))
            ]
            max_segment_jump = max(segment_jumps) if segment_jumps else 0

            monotonic_forward = all(
                route_fts[idx] > route_fts[idx - 1]
                for idx in range(1, len(route_fts))
            )

            gate["details"] = {
                "projected_point_count": len(projected_points),
                "route_progress_ft": round(route_progress_ft, 2),
                "unique_segments": unique_segments,
                "unique_actual_segments": unique_actual_segments,
                "virtual_segment_count": int(virtual_segment_count),
                "unique_segment_ratio": round(segment_ratio, 4),
                "route_coverage_ratio": round(route_coverage_ratio, 4),
                "max_segment_jump": int(max_segment_jump),
                "monotonic_forward": bool(monotonic_forward),
            }

            if not monotonic_forward:
                gate["passed"] = False
                gate["reason"] = "non_monotonic_route_progress"
                reasons.append("segment_fit_non_monotonic_route_progress")
            elif route_progress_ft < gate["min_route_progress_ft"]:
                gate["passed"] = False
                gate["reason"] = "insufficient_route_progress"
                reasons.append("segment_fit_insufficient_route_progress")
            elif unique_actual_segments <= 1 and virtual_segment_count <= 1:
                gate["passed"] = True
                gate["reason"] = "single_segment_route_geometry"
            elif segment_ratio < gate["min_unique_segment_ratio"] and route_coverage_ratio < gate["min_unique_segment_ratio"]:
                gate["passed"] = False
                gate["reason"] = "low_unique_segment_ratio"
                reasons.append("segment_fit_low_unique_segment_ratio")
            elif max_segment_jump > gate["max_segment_jump"]:
                gate["passed"] = False
                gate["reason"] = "segment_jump_too_large"
                reasons.append("segment_fit_segment_jump_too_large")

            if not gate["passed"]:
                hypothesis["billable_candidate"] = False
                item_copy["is_billable"] = False
                item_copy["gate_reasons"] = reasons

        hypothesis["billable_gate_reasons"] = reasons
        hypothesis["segment_fit_gate"] = gate
        item_copy["hypothesis"] = hypothesis
        updated.append(item_copy)

    return updated



def _apply_boundary_exactness_gate(
    evaluated: Sequence[Dict[str, Any]],
    normalized_group: Dict[str, Any],
) -> List[Dict[str, Any]]:
    updated: List[Dict[str, Any]] = []
    source_span_ft = float(normalized_group.get("span_ft") or 0.0)
    span_tolerance_ft = 10.0
    endpoint_tolerance_ft = 10.0
    allowed_boundary_overrun_ft = 5.0

    for item in evaluated:
        item_copy = dict(item)
        hypothesis = dict(item_copy.get("hypothesis") or {})
        profile = dict(hypothesis.get("anchor_profile") or {})
        projected_points = list(profile.get("projected_points") or [])

        gate = {
            "passed": True,
            "reason": "boundary_exactness_valid",
            "source_span_ft": round(source_span_ft, 2),
            "span_tolerance_ft": span_tolerance_ft,
            "endpoint_tolerance_ft": endpoint_tolerance_ft,
            "allowed_boundary_overrun_ft": allowed_boundary_overrun_ft,
            "details": {},
        }

        reasons = list(hypothesis.get("billable_gate_reasons") or [])

        if len(projected_points) < 2 or source_span_ft <= 0.0:
            gate["passed"] = False
            gate["reason"] = "insufficient_boundary_points"
            reasons.append("boundary_exactness_insufficient_points")
            hypothesis["billable_candidate"] = False
            item_copy["is_billable"] = False
            item_copy["gate_reasons"] = reasons
        else:
            route_fts = [float(point.get("route_ft") or 0.0) for point in projected_points]
            station_fts = [float(point.get("station_ft") or 0.0) for point in projected_points]

            projected_start_ft = min(route_fts)
            projected_end_ft = max(route_fts)
            projected_span_ft = projected_end_ft - projected_start_ft

            source_start_ft = min(station_fts)
            source_end_ft = max(station_fts)

            expected_start_ft = float(hypothesis.get("subsection_start_ft") or 0.0)
            expected_end_ft = float(hypothesis.get("subsection_end_ft") or expected_start_ft)

            start_alignment_error_ft = abs(route_fts[0] - expected_start_ft)
            end_alignment_error_ft = abs(route_fts[-1] - expected_end_ft)
            span_error_ft = abs(projected_span_ft - source_span_ft)

            lower_bound = expected_start_ft - allowed_boundary_overrun_ft
            upper_bound = expected_end_ft + allowed_boundary_overrun_ft
            out_of_bounds_count = sum(
                1 for value in route_fts
                if value < lower_bound or value > upper_bound
            )

            gate["details"] = {
                "source_start_ft": round(source_start_ft, 2),
                "source_end_ft": round(source_end_ft, 2),
                "expected_start_ft": round(expected_start_ft, 2),
                "expected_end_ft": round(expected_end_ft, 2),
                "projected_start_ft": round(projected_start_ft, 2),
                "projected_end_ft": round(projected_end_ft, 2),
                "projected_span_ft": round(projected_span_ft, 2),
                "span_error_ft": round(span_error_ft, 2),
                "start_alignment_error_ft": round(start_alignment_error_ft, 2),
                "end_alignment_error_ft": round(end_alignment_error_ft, 2),
                "out_of_bounds_count": int(out_of_bounds_count),
            }

            if span_error_ft > span_tolerance_ft:
                gate["passed"] = False
                gate["reason"] = "projected_span_out_of_tolerance"
                reasons.append("boundary_exactness_span_out_of_tolerance")
            elif start_alignment_error_ft > endpoint_tolerance_ft:
                gate["passed"] = False
                gate["reason"] = "start_boundary_out_of_tolerance"
                reasons.append("boundary_exactness_start_out_of_tolerance")
            elif end_alignment_error_ft > endpoint_tolerance_ft:
                gate["passed"] = False
                gate["reason"] = "end_boundary_out_of_tolerance"
                reasons.append("boundary_exactness_end_out_of_tolerance")
            elif out_of_bounds_count > 0:
                gate["passed"] = False
                gate["reason"] = "projected_points_outside_segment_bounds"
                reasons.append("boundary_exactness_points_outside_bounds")

            if not gate["passed"]:
                hypothesis["billable_candidate"] = False
                item_copy["is_billable"] = False
                item_copy["gate_reasons"] = reasons

        hypothesis["billable_gate_reasons"] = reasons
        hypothesis["boundary_exactness_gate"] = gate
        item_copy["hypothesis"] = hypothesis
        updated.append(item_copy)

    return updated



def _apply_continuity_gate(
    evaluated: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    updated: List[Dict[str, Any]] = []

    for item in evaluated:
        item_copy = dict(item)
        hypothesis = dict(item_copy.get("hypothesis") or {})
        profile = dict(hypothesis.get("anchor_profile") or {})
        projected_points = list(profile.get("projected_points") or [])

        gate = {
            "passed": True,
            "reason": "continuity_valid",
            "max_gap_ft": 80.0,
            "min_gap_ft": 5.0,
            "max_repeat_ratio": 0.20,
            "max_gap_ratio": 1.75,
            "details": {},
        }

        reasons = list(hypothesis.get("billable_gate_reasons") or [])

        if len(projected_points) < 3:
            gate["passed"] = False
            gate["reason"] = "insufficient_points_for_continuity"
            reasons.append("continuity_insufficient_points")
            hypothesis["billable_candidate"] = False
            item_copy["is_billable"] = False
            item_copy["gate_reasons"] = reasons
        else:
            route_fts = [float(point.get("route_ft") or 0.0) for point in projected_points]
            station_fts = [float(point.get("station_ft") or 0.0) for point in projected_points]

            route_steps = [
                route_fts[idx] - route_fts[idx - 1]
                for idx in range(1, len(route_fts))
            ]
            station_steps = [
                station_fts[idx] - station_fts[idx - 1]
                for idx in range(1, len(station_fts))
            ]

            positive_route_steps = [step for step in route_steps if step > 0]
            positive_station_steps = [step for step in station_steps if step > 0]

            max_route_gap = max(positive_route_steps) if positive_route_steps else 0.0
            min_route_gap = min(positive_route_steps) if positive_route_steps else 0.0
            zero_or_repeat_steps = sum(1 for step in route_steps if step <= 0.01)
            repeat_ratio = zero_or_repeat_steps / max(1, len(route_steps))

            gap_ratios = []
            for r_step, s_step in zip(route_steps, station_steps):
                if s_step > 0:
                    gap_ratios.append(r_step / s_step)

            max_gap_ratio = max(gap_ratios) if gap_ratios else 0.0
            min_gap_ratio = min(gap_ratios) if gap_ratios else 0.0

            overlap_count = sum(1 for step in route_steps if step < -0.01)

            gate["details"] = {
                "projected_point_count": len(projected_points),
                "max_route_gap_ft": round(max_route_gap, 2),
                "min_route_gap_ft": round(min_route_gap, 2),
                "repeat_ratio": round(repeat_ratio, 4),
                "max_gap_ratio": round(max_gap_ratio, 4),
                "min_gap_ratio": round(min_gap_ratio, 4),
                "overlap_count": int(overlap_count),
                "route_steps_preview": [round(v, 2) for v in route_steps[:12]],
                "station_steps_preview": [round(v, 2) for v in station_steps[:12]],
            }

            if overlap_count > 0:
                gate["passed"] = False
                gate["reason"] = "route_overlap_detected"
                reasons.append("continuity_route_overlap_detected")
            elif repeat_ratio > gate["max_repeat_ratio"]:
                gate["passed"] = False
                gate["reason"] = "too_many_repeated_steps"
                reasons.append("continuity_too_many_repeated_steps")
            elif max_route_gap > gate["max_gap_ft"]:
                gate["passed"] = False
                gate["reason"] = "route_gap_too_large"
                reasons.append("continuity_route_gap_too_large")
            elif positive_route_steps and min_route_gap < gate["min_gap_ft"]:
                gate["passed"] = False
                gate["reason"] = "route_gap_too_small"
                reasons.append("continuity_route_gap_too_small")
            elif gap_ratios and max_gap_ratio > gate["max_gap_ratio"]:
                gate["passed"] = False
                gate["reason"] = "route_station_gap_ratio_too_large"
                reasons.append("continuity_route_station_gap_ratio_too_large")
            elif gap_ratios and min_gap_ratio < 0.25:
                gate["passed"] = False
                gate["reason"] = "route_station_gap_ratio_too_small"
                reasons.append("continuity_route_station_gap_ratio_too_small")

            if not gate["passed"]:
                hypothesis["billable_candidate"] = False
                item_copy["is_billable"] = False
                item_copy["gate_reasons"] = reasons

        hypothesis["billable_gate_reasons"] = reasons
        hypothesis["continuity_gate"] = gate
        item_copy["hypothesis"] = hypothesis
        updated.append(item_copy)

    return updated



def _endpoint_distance_feet(a_lat: float, a_lon: float, b_lat: float, b_lon: float) -> float:
    return _haversine_feet(float(a_lat), float(a_lon), float(b_lat), float(b_lon))


def _build_route_endpoint_index(route_catalog: Sequence[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    for route in route_catalog or []:
        route_id = str(route.get("route_id") or "")
        coords = list(route.get("coords") or [])
        if not route_id or len(coords) < 2:
            continue
        start = coords[0]
        end = coords[-1]
        index[route_id] = {
            "start_lat": float(start[0]),
            "start_lon": float(start[1]),
            "end_lat": float(end[0]),
            "end_lon": float(end[1]),
        }
    return index


def _infer_chain_neighbors(
    hypothesis: Dict[str, Any],
    route_catalog: Sequence[Dict[str, Any]],
    max_link_distance_ft: float = 3.0,
) -> Dict[str, Any]:
    route_id = str(hypothesis.get("route_id") or "")
    endpoint_index = _build_route_endpoint_index(route_catalog)
    current = endpoint_index.get(route_id)
    if not current:
        return {
            "upstream_candidates": [],
            "downstream_candidates": [],
            "closest_upstream_ft": None,
            "closest_downstream_ft": None,
        }

    upstream = []
    downstream = []

    for other_route_id, other in endpoint_index.items():
        if other_route_id == route_id:
            continue

        upstream_ft = _endpoint_distance_feet(
            current["start_lat"], current["start_lon"],
            other["end_lat"], other["end_lon"],
        )
        downstream_ft = _endpoint_distance_feet(
            current["end_lat"], current["end_lon"],
            other["start_lat"], other["start_lon"],
        )

        if upstream_ft <= max_link_distance_ft:
            upstream.append({
                "route_id": other_route_id,
                "distance_ft": round(upstream_ft, 3),
            })
        if downstream_ft <= max_link_distance_ft:
            downstream.append({
                "route_id": other_route_id,
                "distance_ft": round(downstream_ft, 3),
            })

    upstream.sort(key=lambda item: (float(item["distance_ft"]), str(item["route_id"])))
    downstream.sort(key=lambda item: (float(item["distance_ft"]), str(item["route_id"])))

    return {
        "upstream_candidates": upstream[:10],
        "downstream_candidates": downstream[:10],
        "closest_upstream_ft": upstream[0]["distance_ft"] if upstream else None,
        "closest_downstream_ft": downstream[0]["distance_ft"] if downstream else None,
    }



def _route_catalog_lookup(route_catalog: Sequence[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {
        str(route.get("route_id") or ""): dict(route)
        for route in (route_catalog or [])
        if str(route.get("route_id") or "")
    }


def _apply_chain_gate(
    evaluated: Sequence[Dict[str, Any]],
    normalized_group: Dict[str, Any],
) -> List[Dict[str, Any]]:
    route_catalog = STATE.get("route_catalog", []) or []
    route_lookup = _route_catalog_lookup(route_catalog)
    updated: List[Dict[str, Any]] = []

    valid_transitions = {
        "underground_cable": {"underground_cable", "backbone", "terminal_tail"},
        "backbone": {"underground_cable"},
        "terminal_tail": {"underground_cable"},
    }

    for item in evaluated:
        item_copy = dict(item)
        hypothesis = dict(item_copy.get("hypothesis") or {})
        matched_route = dict(item_copy.get("matched_route") or {})
        profile = dict(hypothesis.get("anchor_profile") or {})
        projected_points = list(profile.get("projected_points") or [])
        route_id = str(hypothesis.get("route_id") or "")

        gate = {
            "passed": True,
            "reason": "chain_valid",
            "max_link_distance_ft": 3.0,
            "max_chain_ambiguity_count": 1,
            "details": {},
        }

        reasons = list(hypothesis.get("billable_gate_reasons") or [])
        neighbors = _infer_chain_neighbors(hypothesis, route_catalog, max_link_distance_ft=gate["max_link_distance_ft"])

        route_role = str(matched_route.get("route_role") or "")
        source_span_ft = float(normalized_group.get("span_ft") or 0.0)
        subsection_start_ft = float(hypothesis.get("subsection_start_ft") or 0.0)
        subsection_end_ft = float(hypothesis.get("subsection_end_ft") or subsection_start_ft)
        route_length_ft = float(matched_route.get("length_ft") or hypothesis.get("route_length_ft") or 0.0)

        near_route_start = subsection_start_ft <= 15.0
        near_route_end = (route_length_ft - subsection_end_ft) <= 15.0 if route_length_ft > 0 else False

        upstream_candidates = list(neighbors.get("upstream_candidates") or [])
        downstream_candidates = list(neighbors.get("downstream_candidates") or [])
        chain_ambiguity_count = len(upstream_candidates) + len(downstream_candidates)

        gate["details"] = {
            "route_role": route_role,
            "route_length_ft": round(route_length_ft, 2),
            "source_span_ft": round(source_span_ft, 2),
            "subsection_start_ft": round(subsection_start_ft, 2),
            "subsection_end_ft": round(subsection_end_ft, 2),
            "near_route_start": bool(near_route_start),
            "near_route_end": bool(near_route_end),
            "closest_upstream_ft": neighbors["closest_upstream_ft"],
            "closest_downstream_ft": neighbors["closest_downstream_ft"],
            "upstream_candidates": upstream_candidates,
            "downstream_candidates": downstream_candidates,
            "chain_ambiguity_count": int(chain_ambiguity_count),
            "projected_point_count": len(projected_points),
            "bidirectional_checks": [],
            "type_checks": [],
        }

        if near_route_start or near_route_end:
            if len(projected_points) < 3:
                gate["passed"] = False
                gate["reason"] = "insufficient_points_for_chain_validation"
                reasons.append("chain_insufficient_points")
            elif chain_ambiguity_count > gate["max_chain_ambiguity_count"]:
                gate["passed"] = False
                gate["reason"] = "multiple_possible_chain_links"
                reasons.append("chain_not_unique")
            elif near_route_start and not upstream_candidates:
                gate["passed"] = False
                gate["reason"] = "missing_upstream_chain_link"
                reasons.append("chain_missing_upstream_link")
            elif near_route_end and not downstream_candidates:
                gate["passed"] = False
                gate["reason"] = "missing_downstream_chain_link"
                reasons.append("chain_missing_downstream_link")

            if gate["passed"] and near_route_start:
                for up in upstream_candidates:
                    neighbor_id = str(up.get("route_id") or "")
                    reverse = _infer_chain_neighbors(
                        {"route_id": neighbor_id},
                        route_catalog,
                        max_link_distance_ft=gate["max_link_distance_ft"],
                    )
                    reverse_down = [str(r.get("route_id") or "") for r in (reverse.get("downstream_candidates") or [])]
                    bidirectional_ok = route_id in reverse_down
                    gate["details"]["bidirectional_checks"].append({
                        "direction": "upstream",
                        "neighbor_route_id": neighbor_id,
                        "reverse_contains_current": bidirectional_ok,
                    })
                    if not bidirectional_ok:
                        gate["passed"] = False
                        gate["reason"] = "chain_not_bidirectional"
                        reasons.append("chain_break_in_topology")
                        break

                    neighbor_route = route_lookup.get(neighbor_id, {})
                    neighbor_role = str(neighbor_route.get("route_role") or "")
                    type_ok = neighbor_role in valid_transitions.get(route_role, set())
                    gate["details"]["type_checks"].append({
                        "direction": "upstream",
                        "neighbor_route_id": neighbor_id,
                        "neighbor_role": neighbor_role,
                        "type_ok": type_ok,
                    })
                    if not type_ok:
                        gate["passed"] = False
                        gate["reason"] = "invalid_chain_type_transition"
                        reasons.append("chain_invalid_topology_type")
                        break

            if gate["passed"] and near_route_end:
                for down in downstream_candidates:
                    neighbor_id = str(down.get("route_id") or "")
                    reverse = _infer_chain_neighbors(
                        {"route_id": neighbor_id},
                        route_catalog,
                        max_link_distance_ft=gate["max_link_distance_ft"],
                    )
                    reverse_up = [str(r.get("route_id") or "") for r in (reverse.get("upstream_candidates") or [])]
                    bidirectional_ok = route_id in reverse_up
                    gate["details"]["bidirectional_checks"].append({
                        "direction": "downstream",
                        "neighbor_route_id": neighbor_id,
                        "reverse_contains_current": bidirectional_ok,
                    })
                    if not bidirectional_ok:
                        gate["passed"] = False
                        gate["reason"] = "chain_not_bidirectional"
                        reasons.append("chain_break_in_topology")
                        break

                    neighbor_route = route_lookup.get(neighbor_id, {})
                    neighbor_role = str(neighbor_route.get("route_role") or "")
                    type_ok = neighbor_role in valid_transitions.get(route_role, set())
                    gate["details"]["type_checks"].append({
                        "direction": "downstream",
                        "neighbor_route_id": neighbor_id,
                        "neighbor_role": neighbor_role,
                        "type_ok": type_ok,
                    })
                    if not type_ok:
                        gate["passed"] = False
                        gate["reason"] = "invalid_chain_type_transition"
                        reasons.append("chain_invalid_topology_type")
                        break

        if not gate["passed"]:
            hypothesis["billable_candidate"] = False
            item_copy["is_billable"] = False
            item_copy["gate_reasons"] = reasons

        hypothesis["billable_gate_reasons"] = reasons
        hypothesis["chain_gate"] = gate
        item_copy["hypothesis"] = hypothesis
        updated.append(item_copy)

    return updated



def _bearing_degrees(a_lat: float, a_lon: float, b_lat: float, b_lon: float) -> float:
    lat1 = math.radians(float(a_lat))
    lat2 = math.radians(float(b_lat))
    dlon = math.radians(float(b_lon) - float(a_lon))
    y = math.sin(dlon) * math.cos(lat2)
    x = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
    bearing = math.degrees(math.atan2(y, x))
    return (bearing + 360.0) % 360.0


def _angle_difference_degrees(a: float, b: float) -> float:
    diff = abs(float(a) - float(b)) % 360.0
    return min(diff, 360.0 - diff)


def _route_terminal_bearings(route: Dict[str, Any]) -> Dict[str, Optional[float]]:
    coords = list(route.get("coords") or [])
    if len(coords) < 2:
        return {
            "start_outbound_bearing_deg": None,
            "end_inbound_bearing_deg": None,
        }

    start_a = coords[0]
    start_b = coords[1]
    end_a = coords[-2]
    end_b = coords[-1]

    return {
        "start_outbound_bearing_deg": _bearing_degrees(float(start_a[0]), float(start_a[1]), float(start_b[0]), float(start_b[1])),
        "end_inbound_bearing_deg": _bearing_degrees(float(end_a[0]), float(end_a[1]), float(end_b[0]), float(end_b[1])),
    }


def _resolve_node_candidates(
    current_route: Dict[str, Any],
    current_role: str,
    candidate_list: Sequence[Dict[str, Any]],
    direction: str,
    route_lookup: Dict[str, Dict[str, Any]],
    valid_transitions: Dict[str, set],
) -> Dict[str, Any]:
    current_bearings = _route_terminal_bearings(current_route)
    if direction == "upstream":
        current_reference = current_bearings.get("start_outbound_bearing_deg")
    else:
        current_reference = current_bearings.get("end_inbound_bearing_deg")

    scored = []
    for candidate in candidate_list or []:
        candidate_id = str(candidate.get("route_id") or "")
        neighbor_route = dict(route_lookup.get(candidate_id) or {})
        if not neighbor_route:
            continue

        neighbor_role = str(neighbor_route.get("route_role") or "")
        type_ok = neighbor_role in valid_transitions.get(current_role, set())

        neighbor_bearings = _route_terminal_bearings(neighbor_route)
        if direction == "upstream":
            neighbor_reference = neighbor_bearings.get("end_inbound_bearing_deg")
        else:
            neighbor_reference = neighbor_bearings.get("start_outbound_bearing_deg")

        if current_reference is None or neighbor_reference is None:
            angle_diff = 999.0
        else:
            angle_diff = _angle_difference_degrees(float(current_reference), float(neighbor_reference))

        distance_ft = float(candidate.get("distance_ft") or 0.0)
        score = angle_diff + distance_ft * 2.0 + (0.0 if type_ok else 1000.0)

        scored.append({
            "route_id": candidate_id,
            "route_name": neighbor_route.get("route_name"),
            "route_role": neighbor_role,
            "distance_ft": round(distance_ft, 3),
            "angle_diff_deg": round(angle_diff, 3),
            "type_ok": bool(type_ok),
            "node_score": round(score, 3),
        })

    scored.sort(key=lambda item: (float(item["node_score"]), float(item["distance_ft"]), str(item["route_id"])))

    if not scored:
        return {
            "selected": None,
            "resolved": [],
            "resolution_status": "no_candidates",
            "ambiguity": False,
            "ambiguity_reason": None,
        }

    best = scored[0]
    second = scored[1] if len(scored) > 1 else None

    ambiguity = False
    ambiguity_reason = None
    if not best["type_ok"]:
        ambiguity = True
        ambiguity_reason = "best_candidate_has_invalid_transition"
    elif second is not None:
        node_gap = float(second["node_score"]) - float(best["node_score"])
        angle_gap = float(second["angle_diff_deg"]) - float(best["angle_diff_deg"])
        if node_gap < 12.0 or angle_gap < 10.0:
            ambiguity = True
            ambiguity_reason = "multiple_directionally_plausible_neighbors"

    return {
        "selected": best,
        "resolved": scored[:10],
        "resolution_status": "resolved" if not ambiguity else "ambiguous",
        "ambiguity": ambiguity,
        "ambiguity_reason": ambiguity_reason,
    }


def _apply_node_resolution_gate(
    evaluated: Sequence[Dict[str, Any]],
    normalized_group: Dict[str, Any],
) -> List[Dict[str, Any]]:
    route_catalog = STATE.get("route_catalog", []) or []
    route_lookup = _route_catalog_lookup(route_catalog)
    updated: List[Dict[str, Any]] = []

    valid_transitions = {
        "underground_cable": {"underground_cable", "backbone", "terminal_tail"},
        "backbone": {"underground_cable"},
        "terminal_tail": {"underground_cable"},
    }

    for item in evaluated:
        item_copy = dict(item)
        hypothesis = dict(item_copy.get("hypothesis") or {})
        matched_route = dict(item_copy.get("matched_route") or {})
        chain_gate = dict(hypothesis.get("chain_gate") or {})
        route_role = str(matched_route.get("route_role") or "")
        subsection_start_ft = float(hypothesis.get("subsection_start_ft") or 0.0)
        subsection_end_ft = float(hypothesis.get("subsection_end_ft") or subsection_start_ft)
        route_length_ft = float(matched_route.get("length_ft") or hypothesis.get("route_length_ft") or 0.0)

        near_route_start = subsection_start_ft <= 15.0
        near_route_end = (route_length_ft - subsection_end_ft) <= 15.0 if route_length_ft > 0 else False

        gate = {
            "passed": True,
            "reason": "node_resolution_valid",
            "details": {
                "near_route_start": bool(near_route_start),
                "near_route_end": bool(near_route_end),
                "upstream_resolution": None,
                "downstream_resolution": None,
            },
        }

        reasons = list(hypothesis.get("billable_gate_reasons") or [])

        if chain_gate.get("passed") is False:
            gate["passed"] = False
            gate["reason"] = "chain_gate_failed_first"
            reasons.append("node_resolution_blocked_by_chain_failure")
        else:
            if near_route_start:
                upstream_candidates = list(((chain_gate.get("details") or {}).get("upstream_candidates")) or [])
                upstream_resolution = _resolve_node_candidates(
                    matched_route,
                    route_role,
                    upstream_candidates,
                    "upstream",
                    route_lookup,
                    valid_transitions,
                )
                gate["details"]["upstream_resolution"] = upstream_resolution
                if upstream_resolution.get("ambiguity"):
                    gate["passed"] = False
                    gate["reason"] = "upstream_node_ambiguous"
                    reasons.append("node_resolution_upstream_ambiguous")
                elif upstream_resolution.get("selected") is None:
                    gate["passed"] = False
                    gate["reason"] = "upstream_node_unresolved"
                    reasons.append("node_resolution_upstream_unresolved")

            if gate["passed"] and near_route_end:
                downstream_candidates = list(((chain_gate.get("details") or {}).get("downstream_candidates")) or [])
                downstream_resolution = _resolve_node_candidates(
                    matched_route,
                    route_role,
                    downstream_candidates,
                    "downstream",
                    route_lookup,
                    valid_transitions,
                )
                gate["details"]["downstream_resolution"] = downstream_resolution
                if downstream_resolution.get("ambiguity"):
                    gate["passed"] = False
                    gate["reason"] = "downstream_node_ambiguous"
                    reasons.append("node_resolution_downstream_ambiguous")
                elif downstream_resolution.get("selected") is None:
                    gate["passed"] = False
                    gate["reason"] = "downstream_node_unresolved"
                    reasons.append("node_resolution_downstream_unresolved")

        if not gate["passed"]:
            hypothesis["billable_candidate"] = False
            item_copy["is_billable"] = False
            item_copy["gate_reasons"] = reasons

        hypothesis["billable_gate_reasons"] = reasons
        hypothesis["node_resolution_gate"] = gate
        item_copy["hypothesis"] = hypothesis
        updated.append(item_copy)

    return updated


def _candidate_rankings_for_group(group_rows: Sequence[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    normalized_group = _normalize_bore_group(group_rows, 0)
    rankings, filter_meta, _ = _candidate_rankings_for_group_v2(group_rows, normalized_group)
    return rankings[:5], filter_meta


def _select_route_for_group(group_rows: Sequence[Dict[str, Any]]) -> Tuple[Dict[str, Any], List[Dict[str, Any]], Dict[str, Any]]:
    rankings, filter_meta = _candidate_rankings_for_group(group_rows)
    best = rankings[0]
    matched_route = _find_route_by_id(best.get("route_id"))
    if not matched_route:
        raise ValueError("Matched route could not be resolved.")

    return matched_route, rankings, filter_meta


def _resolve_station_mapping(rows: Sequence[Dict[str, Any]], route_total_ft: float) -> Dict[str, Any]:
    station_values = [float(row["station_ft"]) for row in rows if row.get("station_ft") is not None]
    if not station_values:
        return {
            "mode": "absolute",
            "min_station_ft": None,
            "max_station_ft": None,
            "station_range_ft": None,
            "anchor_offset_ft": 0.0,
            "anchored_start_ft": None,
            "anchored_end_ft": None,
        }

    min_station = min(station_values)
    max_station = max(station_values)
    station_range = max_station - min_station

    if route_total_ft <= 0 or station_range <= 0:
        mode = "absolute"
    else:
        mode = "group_relative"

    return {
        "mode": mode,
        "min_station_ft": round(min_station, 2),
        "max_station_ft": round(max_station, 2),
        "station_range_ft": round(station_range, 2),
        "anchor_offset_ft": 0.0,
        "anchored_start_ft": 0.0 if mode == "group_relative" else round(min_station, 2),
        "anchored_end_ft": round(station_range, 2) if mode == "group_relative" else round(max_station, 2),
    }


def _map_station_to_route_distance(station_ft: float, route_total_ft: float, mapping: Dict[str, Any]) -> float:
    if route_total_ft <= 0:
        return 0.0

    mode = str(mapping.get("mode") or "absolute")
    anchor_offset_ft = float(mapping.get("anchor_offset_ft") or 0.0)

    if mode == "group_relative":
        min_station = float(mapping.get("min_station_ft") or 0.0)
        mapped = anchor_offset_ft + max(0.0, float(station_ft) - min_station)
        return max(0.0, min(mapped, route_total_ft))

    mapped = float(station_ft) + anchor_offset_ft
    return max(0.0, min(mapped, route_total_ft))


def _print_order_key(group_rows: Sequence[Dict[str, Any]], filter_meta: Dict[str, Any]) -> Tuple[int, str, str]:
    sheet_numbers = [int(value) for value in (filter_meta.get("sheet_numbers") or []) if str(value).strip().isdigit()]
    print_tokens = [str(token).strip() for token in _collect_group_print_tokens(group_rows) if str(token).strip()]
    numeric_tokens = [int(token) for token in print_tokens if token.isdigit()]

    if sheet_numbers:
        sheet_order = min(sheet_numbers)
    elif numeric_tokens:
        sheet_order = min(numeric_tokens)
    else:
        sheet_order = 10**9

    source_file = str(group_rows[0].get("source_file") or "").strip().lower()
    first_station = str(group_rows[0].get("station") or "").strip()
    return sheet_order, source_file, first_station


def _sheet_anchor_key(group_rows: Sequence[Dict[str, Any]], filter_meta: Dict[str, Any]) -> str:
    sheet_numbers = [int(value) for value in (filter_meta.get("sheet_numbers") or []) if str(value).strip().isdigit()]
    if sheet_numbers:
        return f"sheet::{min(sheet_numbers)}"

    print_tokens = [str(token).strip() for token in _collect_group_print_tokens(group_rows) if str(token).strip()]
    numeric_tokens = [int(token) for token in print_tokens if token.isdigit()]
    if numeric_tokens:
        return f"sheet::{min(numeric_tokens)}"

    if print_tokens:
        return f"print::{sorted(print_tokens)[0]}"

    return "fallback::unknown"


def _apply_non_overlapping_group_anchors(
    prepared_groups: Sequence[Dict[str, Any]],
    route_total_ft: float,
) -> Dict[int, Dict[str, Any]]:
    adjusted_mappings: Dict[int, Dict[str, Any]] = {}
    for item in prepared_groups:
        group_idx = int(item["group_idx"])
        mapping = dict(item["mapping"])
        group_rows = item["group"]
        mapping["anchor_offset_ft"] = 0.0
        mapping["anchor_strategy"] = "true_station_position_no_fabrication"
        mapping["anchor_basis"] = {
            "source_file": str(group_rows[0].get("source_file") or ""),
            "print_tokens": list(_collect_group_print_tokens(group_rows)),
            "sheet_numbers": list(item["filter_meta"].get("sheet_numbers") or []),
            "route_total_ft": round(float(route_total_ft or 0.0), 2),
        }
        if str(mapping.get("mode") or "") == "group_relative":
            station_range_ft = max(0.0, float(mapping.get("station_range_ft") or 0.0))
            mapping["anchored_start_ft"] = 0.0
            mapping["anchored_end_ft"] = round(station_range_ft, 2)
        else:
            mapping["anchored_start_ft"] = mapping.get("min_station_ft")
            mapping["anchored_end_ft"] = mapping.get("max_station_ft")
        adjusted_mappings[group_idx] = mapping
    return adjusted_mappings

def _confidence_from_rankings(mapping_mode: str, rankings: Sequence[Dict[str, Any]]) -> Tuple[str, str]:
    top = float(rankings[0].get("score", 0.0)) if rankings else 0.0
    second = float(rankings[1].get("score", 0.0)) if len(rankings) > 1 else 0.0
    margin = top - second

    if top >= 0.90 and margin >= 0.14:
        return "MEDIUM", "Best candidate selected by independent route scoring with a clear lead over alternate paths."
    if top >= 0.78 and margin >= 0.07:
        return "MEDIUM", "Best candidate selected by independent route scoring, but competing paths remain plausible."
    return "LOW", "Candidate route was selected independently, but the score spread is still too narrow for high trust."


def _build_station_points_for_group(
    rows: Sequence[Dict[str, Any]],
    matched_route: Dict[str, Any],
    rankings: Sequence[Dict[str, Any]],
    filter_meta: Dict[str, Any],
    mapping_override: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    route_coords = matched_route.get("coords", []) or []
    if len(route_coords) < 2:
        return [], {
            "mode": "absolute",
            "min_station_ft": None,
            "max_station_ft": None,
            "station_range_ft": None,
        }

    chainage = _route_chainage(route_coords)
    total = float(chainage[-1])
    mapping = dict(mapping_override or _resolve_station_mapping(rows, total))
    confidence, reason = _confidence_from_rankings(str(mapping.get("mode") or "absolute"), rankings)

    points: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows):
        mapped_ft = _map_station_to_route_distance(float(row["station_ft"]), total, mapping)
        lat, lon = _point_at_distance(route_coords, chainage, mapped_ft)
        role = "station"
        if idx == 0:
            role = "start"
        elif idx == len(rows) - 1:
            role = "end"

        points.append(
            {
                "station": row["station"],
                "station_ft": float(row["station_ft"]),
                "mapped_station_ft": round(mapped_ft, 2),
                "lat": round(float(lat), 8),
                "lon": round(float(lon), 8),
                "depth_ft": row.get("depth_ft"),
                "boc_ft": row.get("boc_ft"),
                "notes": row.get("notes", ""),
                "date": row.get("date", ""),
                "crew": row.get("crew", ""),
                "print": row.get("print", ""),
                "job": row.get("source_file", ""),
                "source_file": row.get("source_file", ""),
                "point_role": role,
                "route_id": matched_route.get("route_id"),
                "matched_route_id": matched_route.get("route_id"),
                "matched_route_name": matched_route.get("route_name"),
                "verification": {
                    "entity_type": "station",
                    "confidence": confidence,
                    "reason": reason,
                    "route_selection_method": "independent_candidate_scoring",
                    "mapping_mode": mapping.get("mode"),
                    "anchor_type": "print_filtered_route_pool" if filter_meta.get("applied") else ("print_included_in_group_scoring" if str(row.get("print") or "").strip() else "station_range_group_scoring"),
                    "print_present": bool(str(row.get("print") or "").strip()),
                    "route_name": matched_route.get("route_name", ""),
                    "route_length_ft": round(total, 2),
                    "source_file": str(row.get("source_file") or ""),
                    "print": str(row.get("print") or ""),
                    "candidate_rankings": list(rankings),
                    "print_filter": dict(filter_meta),
                },
            }
        )

    return points, mapping


def _build_redline_segments_for_group(
    rows: Sequence[Dict[str, Any]],
    matched_route: Dict[str, Any],
    rankings: Sequence[Dict[str, Any]],
    mapping: Dict[str, Any],
    filter_meta: Dict[str, Any],
) -> List[Dict[str, Any]]:
    route_coords = matched_route.get("coords", []) or []
    if len(route_coords) < 2 or len(rows) < 2:
        return []

    chainage = _route_chainage(route_coords)
    total = float(chainage[-1])
    confidence, reason = _confidence_from_rankings(str(mapping.get("mode") or "absolute"), rankings)

    segments: List[Dict[str, Any]] = []
    for idx in range(len(rows) - 1):
        start_row = rows[idx]
        end_row = rows[idx + 1]

        start_ft = _map_station_to_route_distance(float(start_row["station_ft"]), total, mapping)
        end_ft = _map_station_to_route_distance(float(end_row["station_ft"]), total, mapping)
        if end_ft <= start_ft:
            continue

        coords = _clip_route_segment(route_coords, start_ft, end_ft)
        if len(coords) < 2:
            continue

        segments.append(
            {
                "segment_id": f"{matched_route.get('route_id', 'route')}_redline_{idx + 1}_{str(start_row.get('print') or 'no_print').replace(' ', '_')}",
                "row_index": idx + 1,
                "start_station": start_row["station"],
                "end_station": end_row["station"],
                "source_start_ft": round(float(start_row["station_ft"]), 2),
                "source_end_ft": round(float(end_row["station_ft"]), 2),
                "start_ft": round(start_ft, 2),
                "end_ft": round(end_ft, 2),
                "length_ft": round(end_ft - start_ft, 2),
                "depth_ft": start_row.get("depth_ft"),
                "boc_ft": start_row.get("boc_ft"),
                "notes": start_row.get("notes", ""),
                "date": start_row.get("date", ""),
                "crew": start_row.get("crew", ""),
                "print": start_row.get("print", ""),
                "source_file": start_row.get("source_file", ""),
                "coords": coords,
                "route_id": matched_route.get("route_id"),
                "route_name": matched_route.get("route_name"),
                "matched_route_id": matched_route.get("route_id"),
                "matched_route_name": matched_route.get("route_name"),
                "verification": {
                    "entity_type": "redline",
                    "confidence": confidence,
                    "reason": reason,
                    "route_selection_method": "independent_candidate_scoring",
                    "mapping_mode": mapping.get("mode"),
                    "anchor_type": "print_filtered_route_pool" if filter_meta.get("applied") else ("print_included_in_group_scoring" if str(start_row.get("print") or "").strip() else "station_range_group_scoring"),
                    "print_present": bool(str(start_row.get("print") or "").strip()),
                    "route_name": matched_route.get("route_name", ""),
                    "route_length_ft": round(total, 2),
                    "source_file": str(start_row.get("source_file") or ""),
                    "print": str(start_row.get("print") or ""),
                    "mapped_start_ft": round(start_ft, 2),
                    "mapped_end_ft": round(end_ft, 2),
                    "source_start_station": start_row["station"],
                    "source_end_station": end_row["station"],
                    "candidate_rankings": list(rankings),
                    "print_filter": dict(filter_meta),
                },
            }
        )

    return segments


def _group_render_is_allowed(validation: Dict[str, Any], selected_hypothesis: Dict[str, Any]) -> Tuple[bool, List[str]]:
    hard_block_reasons: List[str] = []
    soft_block_reasons: List[str] = []

    def _collect(target: List[str], values: Sequence[Any]) -> None:
        for value in values:
            text = str(value or "").strip()
            if text:
                target.append(text)

    validation_status = str(validation.get("validation_status") or "").strip().lower()
    if validation_status == "fail":
        hard_block_reasons.append("validation_status:fail")

    for gate_name in (
        "route_uniqueness_gate",
        "geometry_lock_gate",
        "chain_gate",
        "node_resolution_gate",
    ):
        gate = dict(validation.get(gate_name) or {})
        if gate and gate.get("passed") is False:
            hard_block_reasons.append(f"{gate_name}:{gate.get('reason') or 'failed'}")

    billing_gate = dict(validation.get("billing_gate") or {})
    _collect(soft_block_reasons, billing_gate.get("gate_reasons") or [])

    # Preview-safe render behavior:
    # keep stations/redlines visible on the map when only soft quality heuristics fail,
    # but preserve those failures in block_reasons so the match is still clearly non-billable.
    for gate_name in (
        "physical_feasibility_gate",
        "segment_fit_gate",
        "boundary_exactness_gate",
        "continuity_gate",
    ):
        gate = dict(validation.get(gate_name) or {})
        if gate and gate.get("passed") is False:
            soft_block_reasons.append(f"{gate_name}:{gate.get('reason') or 'failed'}")

    _collect(soft_block_reasons, selected_hypothesis.get("billable_gate_reasons") or [])

    render_allowed = len(hard_block_reasons) == 0
    reasons = hard_block_reasons if hard_block_reasons else soft_block_reasons

    deduped: List[str] = []
    seen = set()
    for reason in reasons:
        if reason not in seen:
            deduped.append(reason)
            seen.add(reason)

    return (render_allowed, deduped)


def _chain_ambiguity_preview_safe(validation: Dict[str, Any], selected_hypothesis: Dict[str, Any]) -> Tuple[bool, List[str]]:
    chain_gate = dict(validation.get("chain_gate") or {})
    node_gate = dict(validation.get("node_resolution_gate") or {})
    route_consensus_gate = dict(selected_hypothesis.get("route_consensus_gate") or {})
    authoritative_commit = dict(selected_hypothesis.get("authoritative_route_commit") or {})
    physical_gate = dict(validation.get("physical_feasibility_gate") or {})
    continuity_gate = dict(validation.get("continuity_gate") or {})
    segment_fit_gate = dict(validation.get("segment_fit_gate") or {})
    boundary_gate = dict(validation.get("boundary_exactness_gate") or {})

    if str(validation.get("validation_status") or "").strip().lower() != "pass":
        return (False, [])
    if not bool(chain_gate):
        return (False, [])
    if bool(chain_gate.get("passed", True)):
        return (False, [])
    if str(chain_gate.get("reason") or "") != "multiple_possible_chain_links":
        return (False, [])
    if not bool(node_gate) or bool(node_gate.get("passed", True)):
        return (False, [])
    if str(node_gate.get("reason") or "") != "chain_gate_failed_first":
        return (False, [])
    if not bool(physical_gate.get("passed", True)):
        return (False, [])
    if not bool(continuity_gate.get("passed", True)):
        return (False, [])
    if not bool(segment_fit_gate.get("passed", True)):
        return (False, [])
    if not bool(boundary_gate.get("passed", True)):
        return (False, [])

    details = dict(chain_gate.get("details") or {})
    near_route_start = bool(details.get("near_route_start"))
    near_route_end = bool(details.get("near_route_end"))
    if not near_route_start and not near_route_end:
        return (False, [])

    authoritative_route_id = str(
        selected_hypothesis.get("authoritative_route_id")
        or authoritative_commit.get("consensus_route_id")
        or route_consensus_gate.get("consensus_route_id")
        or ""
    ).strip()
    if not authoritative_route_id:
        return (False, [])
    consensus_route_id = str(route_consensus_gate.get("consensus_route_id") or authoritative_route_id).strip()
    if consensus_route_id and consensus_route_id != authoritative_route_id:
        return (False, [])
    if authoritative_commit and not bool(authoritative_commit.get("committed", False)):
        return (False, [])

    preview_reasons = [
        "chain_gate:multiple_possible_chain_links",
        "node_resolution_gate:chain_gate_failed_first",
        "endpoint_chain_ambiguity_preview_only",
    ]
    return (True, preview_reasons)



def _window_overlap_ft(start_a: Any, end_a: Any, start_b: Any, end_b: Any) -> float:
    try:
        a0 = float(start_a or 0.0)
        a1 = float(end_a or 0.0)
        b0 = float(start_b or 0.0)
        b1 = float(end_b or 0.0)
    except Exception:
        return 0.0
    left = max(min(a0, a1), min(b0, b1))
    right = min(max(a0, a1), max(b0, b1))
    return max(0.0, right - left)


def _print_zone_distance(current_sheets: Sequence[int], prior_sheets: Sequence[int]) -> Optional[int]:
    current_vals = [int(value) for value in current_sheets if str(value).strip().isdigit()]
    prior_vals = [int(value) for value in prior_sheets if str(value).strip().isdigit()]
    if not current_vals or not prior_vals:
        return None
    return min(abs(curr - prev) for curr in current_vals for prev in prior_vals)


def _same_print_zone(current_filter_meta: Dict[str, Any], prior_filter_meta: Dict[str, Any]) -> Dict[str, Any]:
    current_sheets = [int(value) for value in (current_filter_meta.get('sheet_numbers') or []) if str(value).strip().isdigit()]
    prior_sheets = [int(value) for value in (prior_filter_meta.get('sheet_numbers') or []) if str(value).strip().isdigit()]
    current_streets = {str(value or '').strip().upper() for value in (current_filter_meta.get('street_hints') or []) if str(value or '').strip()}
    prior_streets = {str(value or '').strip().upper() for value in (prior_filter_meta.get('street_hints') or []) if str(value or '').strip()}

    sheet_distance = _print_zone_distance(current_sheets, prior_sheets)
    shared_streets = sorted(current_streets & prior_streets)
    same_zone = False
    reason = 'no_print_zone_evidence'

    if sheet_distance is not None and sheet_distance <= 1:
        same_zone = True
        reason = 'adjacent_or_same_sheet'
    elif shared_streets and sheet_distance is not None and sheet_distance <= 2:
        same_zone = True
        reason = 'shared_street_and_near_sheet'
    elif shared_streets and not current_sheets and not prior_sheets:
        same_zone = True
        reason = 'shared_street_only'

    return {
        'same_zone': same_zone,
        'reason': reason,
        'sheet_distance': sheet_distance,
        'shared_streets': shared_streets,
        'current_sheets': current_sheets,
        'prior_sheets': prior_sheets,
    }


def _apply_within_route_anchor_separation(
    selected_hypothesis: Dict[str, Any],
    matched_route: Dict[str, Any],
    selected_ranking: Dict[str, Any],
    mapping: Dict[str, Any],
    evaluated_hypotheses: Sequence[Dict[str, Any]],
    rendered_matches: Sequence[Dict[str, Any]],
    normalized_group: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    route_id = str(matched_route.get("route_id") or "")
    current_group_id = str(normalized_group.get("group_id") or "")
    current_filter_meta = _print_sheet_hints(normalized_group.get("print_tokens") or [])
    current_start = float(selected_hypothesis.get("subsection_start_ft", 0.0) or 0.0)
    current_end = float(selected_hypothesis.get("subsection_end_ft", 0.0) or 0.0)
    current_span = max(1.0, abs(current_end - current_start))
    current_center = (current_start + current_end) / 2.0

    def _overlap_conflicts_for_window(window_start: float, window_end: float) -> List[Dict[str, Any]]:
        window_span = max(1.0, abs(window_end - window_start))
        window_center = (window_start + window_end) / 2.0
        conflicts: List[Dict[str, Any]] = []
        for prior in rendered_matches:
            if str(prior.get("route_id") or "") != route_id:
                continue
            if current_group_id and str(prior.get("group_id") or "") == current_group_id:
                continue

            prior_hypothesis = dict(prior.get("selected_hypothesis") or {})
            prior_start = float(prior_hypothesis.get("subsection_start_ft", 0.0) or 0.0)
            prior_end = float(prior_hypothesis.get("subsection_end_ft", 0.0) or 0.0)
            prior_span = max(1.0, abs(prior_end - prior_start))
            prior_center = (prior_start + prior_end) / 2.0
            prior_filter_meta = dict(prior.get("print_filter") or {})
            print_zone_meta = _same_print_zone(current_filter_meta, prior_filter_meta)

            overlap_ft = _window_overlap_ft(window_start, window_end, prior_start, prior_end)
            overlap_ratio = overlap_ft / max(1.0, min(window_span, prior_span))
            center_gap_ft = abs(window_center - prior_center)
            span_similarity = min(window_span, prior_span) / max(window_span, prior_span)

            hard_overlap_tolerance_ft = 5.0
            if overlap_ft > hard_overlap_tolerance_ft:
                conflicts.append({
                    "source_file": str(prior.get("source_file") or ""),
                    "route_id": route_id,
                    "overlap_ft": round(overlap_ft, 2),
                    "overlap_ratio": round(overlap_ratio, 6),
                    "center_gap_ft": round(center_gap_ft, 2),
                    "prior_start_ft": round(prior_start, 2),
                    "prior_end_ft": round(prior_end, 2),
                    "print_zone_same": bool(print_zone_meta.get("same_zone")),
                    "print_zone_reason": str(print_zone_meta.get("reason") or ""),
                    "sheet_distance": print_zone_meta.get("sheet_distance"),
                    "shared_streets": list(print_zone_meta.get("shared_streets") or []),
                })
        return conflicts

    overlap_conflicts = _overlap_conflicts_for_window(current_start, current_end)

    route_coords = matched_route.get("coords", []) or []
    route_total_ft = float(matched_route.get("length_ft", 0.0) or 0.0)

    if not overlap_conflicts:
        # Edge-clamped full-span windows on short corridors can appear "conflict free"
        # while still being poor ownership candidates for review. When the chosen window
        # nearly consumes the entire route, prefer a near-equal interior alternative if one exists.
        edge_escape_candidates: List[Dict[str, Any]] = []
        if route_coords and route_total_ft > 0.0 and current_span > 0.0:
            route_consumption_ratio = current_span / max(route_total_ft, 1.0)
            edge_locked = current_start <= 12.0 or (route_total_ft - current_end) <= 12.0
            if route_consumption_ratio >= 0.94 and edge_locked:
                windows = _generate_segment_windows(route_coords, float(normalized_group.get("span_ft") or 0.0))
                current_subsection_score = float(selected_hypothesis.get("subsection_score", 0.0) or 0.0)
                for window in windows:
                    alt_start = float(window.get("start_ft", 0.0) or 0.0)
                    alt_end = float(window.get("end_ft", 0.0) or 0.0)
                    if abs(alt_start - current_start) < 1e-6 and abs(alt_end - current_end) < 1e-6:
                        continue
                    scored = {
                        **window,
                        **_score_segment_window(route_coords, normalized_group, window),
                    }
                    bias_meta = _print_aware_window_bias(route_id, current_filter_meta, alt_start, alt_end, route_total_ft)
                    print_bias_bonus = float(bias_meta.get("bonus", 0.0) or 0.0)
                    scored["print_aware_window_bias"] = bias_meta
                    scored["window_score_base"] = round(float(scored.get("window_score", 0.0) or 0.0), 6)
                    scored["window_score"] = round(min(1.0, max(0.0, float(scored.get("window_score", 0.0) or 0.0) + print_bias_bonus)), 6)
                    alt_score = float(scored.get("window_score", 0.0) or 0.0)
                    alt_edge_clearance = min(alt_start, max(0.0, route_total_ft - alt_end))
                    if alt_edge_clearance <= 4.0:
                        continue
                    if alt_score + 0.03 < current_subsection_score:
                        continue
                    edge_escape_candidates.append(scored)
        if edge_escape_candidates:
            edge_escape_candidates.sort(
                key=lambda item: (
                    -min(float(item.get("start_ft", 0.0) or 0.0), max(0.0, route_total_ft - float(item.get("end_ft", 0.0) or 0.0))),
                    -float(item.get("window_score", 0.0) or 0.0),
                    abs(((float(item.get("start_ft", 0.0) or 0.0) + float(item.get("end_ft", 0.0) or 0.0)) / 2.0) - current_center),
                    float(item.get("start_ft", 0.0) or 0.0),
                )
            )
            best_window = edge_escape_candidates[0]
            alt_mapping = dict(best_window.get("mapping") or mapping or {})
            alt_mapping["anchor_strategy"] = "true_sliding_window_segment_scorer"
            alt_mapping["anchor_basis"] = {
                **dict(alt_mapping.get("anchor_basis") or {}),
                "print_tokens": list(normalized_group.get("print_tokens") or []),
                "filter_applied": bool(current_filter_meta.get("applied")),
                "route_total_ft": round(route_total_ft, 2),
                "group_span_ft": round(float(normalized_group.get("span_ft") or 0.0), 2),
                "segment_window_count": len(edge_escape_candidates),
                "segment_window_preview": [
                    {
                        "start_ft": round(float(item.get("start_ft", 0.0) or 0.0), 2),
                        "end_ft": round(float(item.get("end_ft", 0.0) or 0.0), 2),
                        "window_type": item.get("window_type"),
                        "window_score": round(float(item.get("window_score", 0.0) or 0.0), 6),
                    }
                    for item in edge_escape_candidates[:12]
                ],
            }
            alt_hypothesis = dict(selected_hypothesis)
            alt_hypothesis["subsection_start_ft"] = round(float(best_window.get("start_ft", 0.0) or 0.0), 2)
            alt_hypothesis["subsection_end_ft"] = round(float(best_window.get("end_ft", 0.0) or 0.0), 2)
            alt_hypothesis["subsection_score"] = round(float(best_window.get("window_score", 0.0) or 0.0), 6)
            alt_hypothesis["combined_score"] = round(
                min(1.0, float(selected_ranking.get("score", 0.0) or 0.0) + float(best_window.get("window_score", 0.0) or 0.0) * 0.35),
                6,
            )
            alt_hypothesis["anchor_method"] = "true_sliding_window_segment_scorer"
            alt_reasons = list(best_window.get("window_reasons") or [])
            if current_filter_meta.get("applied"):
                alt_reasons.append("Print-aware filtering narrowed the route family before sliding-window segment scoring.")
            alt_reasons.append("Edge-clamped full-span anchor was nudged inward to improve within-route ownership stability.")
            alt_hypothesis["anchor_reasons"] = alt_reasons
            alt_hypothesis["anchor_profile"] = dict(best_window.get("window_profile") or {})
            alt_hypothesis["mapping"] = alt_mapping
            gate = {
                "passed": True,
                "reason": "edge_locked_window_reselected_inward",
                "conflicts": [],
                "reselected": True,
                "reselected_route_id": route_id,
                "reselected_subsection_start_ft": round(float(best_window.get("start_ft", 0.0) or 0.0), 2),
                "reselected_subsection_end_ft": round(float(best_window.get("end_ft", 0.0) or 0.0), 2),
                "mode": "edge_escape_same_route",
            }
            alt_hypothesis["within_route_anchor_separation_gate"] = gate
            return alt_hypothesis, matched_route, selected_ranking, alt_mapping, gate

        gate = {
            "passed": True,
            "reason": "no_within_route_overlap_conflict",
            "conflicts": [],
            "reselected": False,
        }
        selected_hypothesis = dict(selected_hypothesis)
        selected_hypothesis["within_route_anchor_separation_gate"] = gate
        return selected_hypothesis, matched_route, selected_ranking, mapping, gate
    same_route_candidates: List[Dict[str, Any]] = []
    if route_coords and route_total_ft > 0.0:
        windows = _generate_segment_windows(route_coords, float(normalized_group.get("span_ft") or 0.0))
        for window in windows:
            alt_start = float(window.get("start_ft", 0.0) or 0.0)
            alt_end = float(window.get("end_ft", 0.0) or 0.0)
            if abs(alt_start - current_start) < 1e-6 and abs(alt_end - current_end) < 1e-6:
                continue
            scored = {
                **window,
                **_score_segment_window(route_coords, normalized_group, window),
            }
            bias_meta = _print_aware_window_bias(route_id, current_filter_meta, alt_start, alt_end, route_total_ft)
            print_bias_bonus = float(bias_meta.get("bonus", 0.0) or 0.0)
            scored["print_aware_window_bias"] = bias_meta
            scored["window_score_base"] = round(float(scored.get("window_score", 0.0) or 0.0), 6)
            scored["window_score"] = round(min(1.0, max(0.0, float(scored.get("window_score", 0.0) or 0.0) + print_bias_bonus)), 6)
            conflicts = _overlap_conflicts_for_window(alt_start, alt_end)
            if conflicts:
                continue
            same_route_candidates.append(scored)

    if same_route_candidates:
        same_route_candidates.sort(
            key=lambda item: (
                -float(item.get("window_score", 0.0) or 0.0),
                -float((item.get("print_aware_window_bias") or {}).get("bonus", 0.0) or 0.0),
                abs(float(item.get("end_ft", 0.0) or 0.0) - float(item.get("start_ft", 0.0) or 0.0) - current_span),
                abs(((float(item.get("start_ft", 0.0) or 0.0) + float(item.get("end_ft", 0.0) or 0.0)) / 2.0) - current_center),
                float(item.get("start_ft", 0.0) or 0.0),
            )
        )
        best_window = same_route_candidates[0]
        alt_mapping = dict(best_window.get("mapping") or mapping or {})
        alt_mapping["anchor_strategy"] = "true_sliding_window_segment_scorer"
        alt_mapping["anchor_basis"] = {
            **dict(alt_mapping.get("anchor_basis") or {}),
            "print_tokens": list(normalized_group.get("print_tokens") or []),
            "filter_applied": bool(current_filter_meta.get("applied")),
            "route_total_ft": round(route_total_ft, 2),
            "group_span_ft": round(float(normalized_group.get("span_ft") or 0.0), 2),
            "segment_window_count": len(same_route_candidates),
            "segment_window_preview": [
                {
                    "start_ft": round(float(item.get("start_ft", 0.0) or 0.0), 2),
                    "end_ft": round(float(item.get("end_ft", 0.0) or 0.0), 2),
                    "window_type": item.get("window_type"),
                    "window_score": round(float(item.get("window_score", 0.0) or 0.0), 6),
                }
                for item in same_route_candidates[:12]
            ],
        }
        alt_hypothesis = dict(selected_hypothesis)
        alt_hypothesis["subsection_start_ft"] = round(float(best_window.get("start_ft", 0.0) or 0.0), 2)
        alt_hypothesis["subsection_end_ft"] = round(float(best_window.get("end_ft", 0.0) or 0.0), 2)
        alt_hypothesis["subsection_score"] = round(float(best_window.get("window_score", 0.0) or 0.0), 6)
        alt_hypothesis["combined_score"] = round(
            min(
                1.0,
                float(selected_ranking.get("score", 0.0) or 0.0) + float(best_window.get("window_score", 0.0) or 0.0) * 0.35,
            ),
            6,
        )
        alt_hypothesis["anchor_method"] = "true_sliding_window_segment_scorer"
        alt_reasons = list(best_window.get("window_reasons") or [])
        if current_filter_meta.get("applied"):
            alt_reasons.append("Print-aware filtering narrowed the route family before sliding-window segment scoring.")
        alt_reasons.append("Within-route anchor reselection avoided overlap with an already rendered same-corridor group.")
        alt_hypothesis["anchor_reasons"] = alt_reasons
        alt_hypothesis["anchor_profile"] = dict(best_window.get("window_profile") or {})
        alt_hypothesis["mapping"] = alt_mapping

        gate = {
            "passed": True,
            "reason": "reselected_to_non_overlapping_subsection_same_route",
            "conflicts": overlap_conflicts,
            "reselected": True,
            "reselected_route_id": route_id,
            "reselected_subsection_start_ft": round(float(best_window.get("start_ft", 0.0) or 0.0), 2),
            "reselected_subsection_end_ft": round(float(best_window.get("end_ft", 0.0) or 0.0), 2),
            "mode": "within_route_batch_anchor_coordination",
        }
        alt_hypothesis["within_route_anchor_separation_gate"] = gate
        return alt_hypothesis, matched_route, selected_ranking, alt_mapping, gate

    for item in evaluated_hypotheses:
        hypothesis = dict(item.get("hypothesis") or {})
        alt_route = dict(item.get("matched_route") or {})
        alt_ranking = dict(item.get("ranking") or {})
        alt_mapping = dict(item.get("mapping") or {})
        if not hypothesis or not alt_route:
            continue
        if str(hypothesis.get("route_id") or "") != route_id:
            gate = {
                "passed": True,
                "reason": "reselected_to_non_conflicting_route",
                "conflicts": overlap_conflicts,
                "reselected": True,
                "reselected_route_id": str(hypothesis.get("route_id") or ""),
            }
            hypothesis["within_route_anchor_separation_gate"] = gate
            return hypothesis, alt_route, alt_ranking, alt_mapping, gate

        alt_start = float(hypothesis.get("subsection_start_ft", 0.0) or 0.0)
        alt_end = float(hypothesis.get("subsection_end_ft", 0.0) or 0.0)
        if _overlap_conflicts_for_window(alt_start, alt_end):
            continue

        gate = {
            "passed": True,
            "reason": "reselected_to_non_overlapping_subsection",
            "conflicts": overlap_conflicts,
            "reselected": True,
            "reselected_route_id": str(hypothesis.get("route_id") or ""),
            "reselected_subsection_start_ft": round(alt_start, 2),
            "reselected_subsection_end_ft": round(alt_end, 2),
            "mode": "print_zone_overlap_suppression",
        }
        hypothesis["within_route_anchor_separation_gate"] = gate
        return hypothesis, alt_route, alt_ranking, alt_mapping, gate

    gate = {
        "passed": False,
        "reason": "within_route_overlap_conflict_no_safe_alternative",
        "conflicts": overlap_conflicts,
        "reselected": False,
        "mode": "within_route_batch_anchor_coordination",
    }
    selected_hypothesis = dict(selected_hypothesis)
    selected_hypothesis["within_route_anchor_separation_gate"] = gate
    return selected_hypothesis, matched_route, selected_ranking, mapping, gate



def _batch_conflict_meta(current: Dict[str, Any], prior: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not bool(current.get("render_allowed")) or not bool(prior.get("render_allowed")):
        return None
    if str(current.get("route_id") or "") != str(prior.get("route_id") or ""):
        return None
    if str(current.get("group_id") or "") and str(current.get("group_id") or "") == str(prior.get("group_id") or ""):
        return None

    current_hypothesis = dict(current.get("selected_hypothesis") or {})
    prior_hypothesis = dict(prior.get("selected_hypothesis") or {})
    current_start = float(current_hypothesis.get("subsection_start_ft", 0.0) or 0.0)
    current_end = float(current_hypothesis.get("subsection_end_ft", 0.0) or 0.0)
    prior_start = float(prior_hypothesis.get("subsection_start_ft", 0.0) or 0.0)
    prior_end = float(prior_hypothesis.get("subsection_end_ft", 0.0) or 0.0)
    current_span = max(1.0, abs(current_end - current_start))
    prior_span = max(1.0, abs(prior_end - prior_start))
    overlap_ft = _window_overlap_ft(current_start, current_end, prior_start, prior_end)
    if overlap_ft <= 0.0:
        return None

    current_filter_meta = dict(current.get("print_filter") or {})
    prior_filter_meta = dict(prior.get("print_filter") or {})
    print_zone_meta = _same_print_zone(current_filter_meta, prior_filter_meta)
    overlap_ratio = overlap_ft / max(1.0, min(current_span, prior_span))
    current_center = (current_start + current_end) / 2.0
    prior_center = (prior_start + prior_end) / 2.0
    center_gap_ft = abs(current_center - prior_center)
    span_similarity = min(current_span, prior_span) / max(current_span, prior_span)

    # Keep true duplicate / materially overlapping windows blocked, but do not let
    # tiny or edge-adjacent subsection nibbling kill otherwise distinct same-route groups.
    hard_overlap_tolerance_ft = min(30.0, max(5.0, min(current_span, prior_span) * 0.06))
    if overlap_ft <= hard_overlap_tolerance_ft:
        return None

    material_overlap = overlap_ratio >= 0.12
    near_duplicate_window = center_gap_ft <= max(25.0, min(current_span, prior_span) * 0.10)
    if not material_overlap and not near_duplicate_window:
        return None

    return {
        "route_id": str(current.get("route_id") or ""),
        "overlap_ft": round(overlap_ft, 2),
        "overlap_ratio": round(overlap_ratio, 6),
        "center_gap_ft": round(center_gap_ft, 2),
        "span_similarity": round(span_similarity, 6),
        "hard_overlap_tolerance_ft": round(hard_overlap_tolerance_ft, 2),
        "material_overlap": bool(material_overlap),
        "near_duplicate_window": bool(near_duplicate_window),
        "print_zone_same": bool(print_zone_meta.get("same_zone")),
        "print_zone_reason": str(print_zone_meta.get("reason") or ""),
        "sheet_distance": print_zone_meta.get("sheet_distance"),
        "shared_streets": list(print_zone_meta.get("shared_streets") or []),
    }

def _apply_batch_level_conflict_resolution(group_matches: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Hard no-overlap ownership engine.

    Single uploads remain unchanged.
    For batches, rendered groups are processed in deterministic priority order and each later
    same-route group must either re-anchor to a non-overlapping subsection or be blocked.
    """
    final_matches = [dict(match) for match in group_matches]
    rendered = [match for match in final_matches if bool(match.get("render_allowed"))]
    if len(rendered) <= 1:
        for match in final_matches:
            validation = dict(match.get("validation") or {})
            validation.setdefault(
                "batch_conflict_resolution_gate",
                {
                    "passed": True,
                    "reason": "single_or_zero_rendered_group",
                    "conflicts": [],
                    "mode": "hard_no_overlap_single_safe",
                },
            )
            match["validation"] = validation
        return final_matches

    rendered.sort(
        key=lambda item: (
            -float((item.get("selected_hypothesis") or {}).get("combined_score", 0.0) or 0.0),
            -float(item.get("confidence", 0.0) or 0.0),
            -float((item.get("selected_hypothesis") or {}).get("subsection_score", 0.0) or 0.0),
            -float(item.get("expected_span_ft", 0.0) or 0.0),
            str(item.get("source_file") or ""),
        )
    )

    accepted: List[Dict[str, Any]] = []
    updated_by_group: Dict[str, Dict[str, Any]] = {}

    for candidate in rendered:
        updated = dict(candidate)
        group_id = str(updated.get("group_id") or "")
        validation = dict(updated.get("validation") or {})
        rankings = list(updated.get("candidate_rankings") or [])
        selected_hypothesis = dict(updated.get("selected_hypothesis") or {})
        matched_route = dict(updated.get("_matched_route") or {})
        mapping = dict(updated.get("mapping") or {})
        normalized_group = dict(updated.get("_normalized_group") or {})
        evaluated_hypotheses = list(updated.get("_evaluated_hypotheses") or [])

        selected_ranking = next(
            (dict(item) for item in rankings if str(item.get("route_id") or "") == str(selected_hypothesis.get("route_id") or "")),
            dict(rankings[0]) if rankings else {},
        )

        if accepted and matched_route and normalized_group:
            selected_hypothesis, matched_route, selected_ranking, mapping, within_gate = _apply_within_route_anchor_separation(
                selected_hypothesis,
                matched_route,
                selected_ranking,
                mapping,
                evaluated_hypotheses,
                accepted,
                normalized_group,
            )
            validation["within_route_anchor_separation_gate"] = dict(within_gate)

            group_rows = list(normalized_group.get("station_rows") or [])
            group_station_points, mapping = _build_station_points_for_group(
                group_rows,
                matched_route,
                rankings,
                dict(updated.get("print_filter") or {}),
                mapping_override=mapping,
            )
            group_redline_segments = _build_redline_segments_for_group(
                group_rows,
                matched_route,
                rankings,
                mapping,
                dict(updated.get("print_filter") or {}),
            )
            updated["group_station_points"] = list(group_station_points)
            updated["group_redline_segments"] = list(group_redline_segments)
            updated["mapping"] = dict(mapping)
            updated["selected_hypothesis"] = dict(selected_hypothesis)
            updated["route_id"] = matched_route.get("route_id")
            updated["route_name"] = matched_route.get("route_name")
            updated["source_folder"] = matched_route.get("source_folder")
            updated["route_role"] = matched_route.get("route_role")

        candidate_conflicts = []
        for prior in accepted:
            conflict = _batch_conflict_meta(updated, prior)
            if conflict:
                candidate_conflicts.append({
                    **conflict,
                    "conflicts_with_source_file": str(prior.get("source_file") or ""),
                })

        group_station_points = list(updated.get("group_station_points") or [])
        group_redline_segments = list(updated.get("group_redline_segments") or [])
        has_built_geometry = bool(group_station_points) or bool(group_redline_segments)

        hard_conflicts = []
        salvageable_conflicts = []
        for conflict in candidate_conflicts:
            overlap_ratio = float(conflict.get("overlap_ratio", 0.0) or 0.0)
            overlap_ft = float(conflict.get("overlap_ft", 0.0) or 0.0)
            tolerance_ft = float(conflict.get("hard_overlap_tolerance_ft", 0.0) or 0.0)
            span_similarity = float(conflict.get("span_similarity", 0.0) or 0.0)
            near_duplicate_window = bool(conflict.get("near_duplicate_window"))

            true_duplicate = (
                near_duplicate_window
                or (overlap_ratio >= 0.5 and span_similarity >= 0.7)
                or overlap_ft >= max(80.0, tolerance_ft * 3.0)
            )
            if true_duplicate:
                hard_conflicts.append({**conflict, "true_duplicate": True})
            else:
                salvageable_conflicts.append({**conflict, "true_duplicate": False})

        if candidate_conflicts and (not has_built_geometry or hard_conflicts):
            updated["render_allowed"] = False
            validation["batch_conflict_resolution_gate"] = {
                "passed": False,
                "reason": "hard_no_overlap_conflict_no_safe_alternative",
                "conflicts": hard_conflicts or candidate_conflicts,
                "salvageable_conflicts": salvageable_conflicts,
                "mode": "hard_no_overlap_authoritative",
            }
            render_block_reasons = [reason for reason in list(updated.get("render_block_reasons") or []) if str(reason)]
            if "batch_level_conflict_resolution" not in render_block_reasons:
                render_block_reasons.append("batch_level_conflict_resolution")
            updated["render_block_reasons"] = render_block_reasons
            validation["render_gate"] = {
                "render_allowed": False,
                "block_reasons": list(render_block_reasons),
                "mode": "hard_no_overlap_authoritative",
            }
            updated["rendered_station_point_count"] = 0
            updated["rendered_redline_segment_count"] = 0
        else:
            updated["render_allowed"] = True
            updated["rendered_station_point_count"] = len(group_station_points)
            updated["rendered_redline_segment_count"] = len(group_redline_segments)
            validation["batch_conflict_resolution_gate"] = {
                "passed": True,
                "reason": "owned_non_overlapping_subsection" if not candidate_conflicts else "salvaged_distinct_subsection_with_geometry",
                "conflicts": [],
                "salvageable_conflicts": salvageable_conflicts,
                "mode": "hard_no_overlap_authoritative",
            }
            validation["render_gate"] = {
                "render_allowed": True,
                "block_reasons": [
                    reason
                    for reason in list((validation.get("render_gate") or {}).get("block_reasons") or [])
                    if str(reason) != "batch_level_conflict_resolution"
                ],
                "mode": "hard_no_overlap_authoritative",
            }
            accepted.append(updated)

        updated["validation"] = validation
        updated_by_group[group_id] = updated

    merged: List[Dict[str, Any]] = []
    for match in final_matches:
        group_id = str(match.get("group_id") or "")
        if group_id in updated_by_group:
            merged.append(updated_by_group[group_id])
        else:
            validation = dict(match.get("validation") or {})
            validation.setdefault(
                "batch_conflict_resolution_gate",
                {
                    "passed": True,
                    "reason": "not_render_eligible_before_batch_pass",
                    "conflicts": [],
                    "mode": "hard_no_overlap_authoritative",
                },
            )
            match["validation"] = validation
            merged.append(match)

    return merged




def _resolve_batch_route_ownership(group_matches: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ordered_matches = [dict(match) for match in group_matches]
    assigned_by_route: Dict[str, List[Dict[str, Any]]] = {}
    resolved_matches: List[Dict[str, Any]] = []

    for match in ordered_matches:
        route_id = str(match.get("route_id") or "")
        selected_hypothesis = dict(match.get("selected_hypothesis") or {})
        matched_route = dict(match.get("_matched_route") or {})
        selected_ranking = dict(match.get("score_breakdown") or {})
        mapping = dict(match.get("mapping") or {})
        normalized_group = dict(match.get("_normalized_group") or {})
        evaluated_hypotheses = list(match.get("_evaluated_hypotheses") or [])
        prior_assigned = list(assigned_by_route.get(route_id, []))

        if route_id and normalized_group and evaluated_hypotheses:
            selected_hypothesis, matched_route, _selected_ranking_unused, mapping, within_gate = _apply_within_route_anchor_separation(
                selected_hypothesis,
                matched_route,
                selected_ranking,
                mapping,
                evaluated_hypotheses,
                prior_assigned,
                normalized_group,
            )
            selected_hypothesis, matched_route, _selected_ranking_unused, mapping, authoritative_route_id = _authoritative_selection_bundle(
                selected_hypothesis,
                matched_route,
                selected_ranking,
                mapping,
                evaluated_hypotheses,
            )

            group_rows = list(normalized_group.get("station_rows") or [])
            candidate_rankings = list(match.get("candidate_rankings") or [])
            filter_meta = dict(match.get("print_filter") or {})
            group_station_points, mapping = _build_station_points_for_group(group_rows, matched_route, candidate_rankings, filter_meta, mapping)
            group_redline_segments = _build_redline_segments_for_group(group_rows, matched_route, candidate_rankings, mapping, filter_meta)

            if authoritative_route_id:
                for point in group_station_points:
                    point["route_id"] = authoritative_route_id
                    point["matched_route_id"] = authoritative_route_id
                    point["matched_route_name"] = matched_route.get("route_name")
                for segment in group_redline_segments:
                    segment["route_id"] = authoritative_route_id
                    segment["matched_route_id"] = authoritative_route_id
                    segment["route_name"] = matched_route.get("route_name")
                    segment["matched_route_name"] = matched_route.get("route_name")

            validation = dict(match.get("validation") or {})
            validation["within_route_anchor_separation_gate"] = dict(selected_hypothesis.get("within_route_anchor_separation_gate") or within_gate)
            validation["batch_conflict_resolution_gate"] = {
                "passed": bool((selected_hypothesis.get("within_route_anchor_separation_gate") or within_gate or {}).get("passed", True)),
                "reason": str((selected_hypothesis.get("within_route_anchor_separation_gate") or within_gate or {}).get("reason") or "owned_non_overlapping_subsection"),
                "conflicts": list((selected_hypothesis.get("within_route_anchor_separation_gate") or within_gate or {}).get("conflicts") or []),
                "mode": "hard_no_overlap_authoritative",
            }
            validation["render_gate"] = {
                "render_allowed": True,
                "block_reasons": [],
                "mode": "hard_no_overlap_authoritative",
            }

            match["selected_hypothesis"] = dict(selected_hypothesis)
            match["mapping"] = dict(mapping)
            match["validation"] = validation
            match["group_station_points"] = list(group_station_points)
            match["group_redline_segments"] = list(group_redline_segments)
            match["render_allowed"] = True
            match["render_block_reasons"] = []
            match["rendered_station_point_count"] = len(group_station_points)
            match["rendered_redline_segment_count"] = len(group_redline_segments)
            match["route_id"] = matched_route.get("route_id")
            match["route_name"] = matched_route.get("route_name")
            match["source_folder"] = matched_route.get("source_folder")

        resolved_matches.append(match)
        if route_id and bool(match.get("render_allowed")):
            assigned_by_route.setdefault(route_id, []).append(match)

    return resolved_matches

def _rebuild_field_data_outputs() -> None:
    rows = STATE.get("committed_rows", []) or []
    groups = _group_rows_for_matching(rows)

    group_matches: List[Dict[str, Any]] = []
    matching_debug: List[Dict[str, Any]] = []

    for group_idx, group in enumerate(groups):
        normalized_group = _normalize_bore_group(group, group_idx)
        rankings, filter_meta, _all_rankings = _candidate_rankings_for_group_v2(group, normalized_group)
        if not rankings:
            continue

        anchored_hypotheses: List[Dict[str, Any]] = []
        for ranking in rankings[:3]:
            matched_route = _find_route_by_id(ranking.get("route_id"))
            if not matched_route:
                continue
            anchored_hypotheses.append(_anchor_route_subsection(matched_route, normalized_group, ranking, filter_meta))

        anchored_hypotheses.sort(key=lambda item: (-float(item.get("combined_score", 0.0) or 0.0), -float(item.get("route_score", 0.0) or 0.0), str(item.get("route_name", ""))))
        if not anchored_hypotheses:
            continue

        selected_hypothesis, matched_route, selected_ranking, mapping, evaluated_hypotheses = _select_best_hypothesis_with_gate(
            group,
            normalized_group,
            rankings,
            filter_meta,
            anchored_hypotheses,
        )

        rendered_matches_so_far = [match for match in group_matches if bool(match.get("render_allowed"))]
        selected_hypothesis, matched_route, selected_ranking, mapping, within_route_anchor_separation_gate = _apply_within_route_anchor_separation(
            selected_hypothesis,
            matched_route,
            selected_ranking,
            mapping,
            evaluated_hypotheses,
            rendered_matches_so_far,
            normalized_group,
        )
        selected_hypothesis, matched_route, selected_ranking, mapping, authoritative_route_id = _authoritative_selection_bundle(
            selected_hypothesis,
            matched_route,
            selected_ranking,
            mapping,
            evaluated_hypotheses,
        )

        group_station_points, mapping = _build_station_points_for_group(group, matched_route, rankings, filter_meta, mapping)
        group_redline_segments = _build_redline_segments_for_group(group, matched_route, rankings, mapping, filter_meta)

        if authoritative_route_id:
            filtered_station_points: List[Dict[str, Any]] = []
            for point in group_station_points:
                point_copy = dict(point)
                point_copy["route_id"] = authoritative_route_id
                point_copy["matched_route_id"] = authoritative_route_id
                point_copy["matched_route_name"] = matched_route.get("route_name")
                verification = dict(point_copy.get("verification") or {})
                verification["authoritative_route_id"] = authoritative_route_id
                point_copy["verification"] = verification
                if str(point_copy.get("route_id") or "").strip() == authoritative_route_id:
                    filtered_station_points.append(point_copy)
            group_station_points = filtered_station_points

            filtered_redline_segments: List[Dict[str, Any]] = []
            for segment in group_redline_segments:
                segment_copy = dict(segment)
                segment_copy["route_id"] = authoritative_route_id
                segment_copy["matched_route_id"] = authoritative_route_id
                segment_copy["route_name"] = matched_route.get("route_name")
                segment_copy["matched_route_name"] = matched_route.get("route_name")
                verification = dict(segment_copy.get("verification") or {})
                verification["authoritative_route_id"] = authoritative_route_id
                segment_copy["verification"] = verification
                if str(segment_copy.get("route_id") or "").strip() == authoritative_route_id:
                    filtered_redline_segments.append(segment_copy)
            group_redline_segments = filtered_redline_segments
        validation = _build_validation_checks(normalized_group, anchored_hypotheses, mapping, group_station_points, matched_route)
        validation["billing_gate"] = {
            "billable_candidate": bool(selected_hypothesis.get("billable_candidate")),
            "gate_reasons": list(selected_hypothesis.get("billable_gate_reasons") or []),
            "mode": "deterministic_pass_fail_gate",
        }
        validation["route_uniqueness_gate"] = dict(selected_hypothesis.get("route_uniqueness_gate") or {
            "passed": True,
            "reason": "no_uniqueness_conflict_detected",
            "competing_billable_candidates": [],
        })
        validation["geometry_lock_gate"] = dict(selected_hypothesis.get("geometry_lock_gate") or {
            "passed": True,
            "reason": "no_parallel_route_conflict_detected",
            "competing_parallel_routes": [],
        })
        validation["physical_feasibility_gate"] = dict(selected_hypothesis.get("physical_feasibility_gate") or {
            "passed": True,
            "reason": "within_physical_span_bounds",
        })
        validation["segment_fit_gate"] = dict(selected_hypothesis.get("segment_fit_gate") or {
            "passed": True,
            "reason": "segment_fit_valid",
            "details": {},
        })
        validation["boundary_exactness_gate"] = dict(selected_hypothesis.get("boundary_exactness_gate") or {
            "passed": True,
            "reason": "boundary_exactness_valid",
            "details": {},
        })
        validation["continuity_gate"] = dict(selected_hypothesis.get("continuity_gate") or {
            "passed": True,
            "reason": "continuity_valid",
            "details": {},
        })
        validation["chain_gate"] = dict(selected_hypothesis.get("chain_gate") or {
            "passed": True,
            "reason": "chain_valid",
            "details": {},
        })
        validation["node_resolution_gate"] = dict(selected_hypothesis.get("node_resolution_gate") or {
            "passed": True,
            "reason": "node_resolution_valid",
            "details": {},
        })
        validation["within_route_anchor_separation_gate"] = dict(selected_hypothesis.get("within_route_anchor_separation_gate") or within_route_anchor_separation_gate or {
            "passed": True,
            "reason": "no_within_route_overlap_conflict",
            "conflicts": [],
            "reselected": False,
        })

        anchored_hypotheses = [dict(item["hypothesis"]) for item in evaluated_hypotheses]
        render_allowed, render_block_reasons = _group_render_is_allowed(validation, selected_hypothesis)

        # Context-stable preview-safe render policy:
        # A valid per-group placement should stay visible regardless of whether the same bore log
        # arrives alone or beside other nearby logs in a batch. Foremen verify before billing, so
        # the backend must prioritize the right corridor / right segment / right direction and keep
        # sane reconstructions visible instead of killing them at the final gate.
        has_route = bool(matched_route and matched_route.get("route_id"))
        has_station_points = len(group_station_points) > 0
        has_redline_segments = len(group_redline_segments) > 0
        has_geometry_output = has_station_points and has_redline_segments
        within_route_gate = dict(validation.get("within_route_anchor_separation_gate") or {})
        physical_gate = dict(validation.get("physical_feasibility_gate") or {})
        continuity_gate = dict(validation.get("continuity_gate") or {})
        segment_fit_gate = dict(validation.get("segment_fit_gate") or {})
        chain_gate = dict(validation.get("chain_gate") or {})
        chain_preview_safe, chain_preview_reasons = _chain_ambiguity_preview_safe(validation, selected_hypothesis)

        hard_fail_reasons = []
        if has_route is False:
            hard_fail_reasons.append("no_matched_route")
        if has_geometry_output is False:
            hard_fail_reasons.append("no_geometry_output")
        if not bool(chain_gate.get("passed", True)) and not chain_preview_safe:
            hard_fail_reasons.append(str(chain_gate.get("reason") or "chain_gate_failed"))

        preview_reasons = []
        if chain_preview_safe:
            preview_reasons.extend(chain_preview_reasons)
        if not bool(physical_gate.get("passed", True)):
            preview_reasons.append(str(physical_gate.get("reason") or "physical_feasibility_warn"))
        if not bool(within_route_gate.get("passed", True)):
            preview_reasons.append(str(within_route_gate.get("reason") or "within_route_anchor_overlap_conflict"))
        if not bool(continuity_gate.get("passed", True)):
            preview_reasons.append(str(continuity_gate.get("reason") or "continuity_gate_warn"))
        if not bool(segment_fit_gate.get("passed", True)):
            preview_reasons.append(str(segment_fit_gate.get("reason") or "segment_fit_gate_warn"))

        if hard_fail_reasons:
            render_allowed = False
            render_block_reasons = list(render_block_reasons) + hard_fail_reasons
            render_mode = "deterministic_hard_block_only"
        else:
            render_allowed = True
            render_block_reasons = [
                reason
                for reason in list(render_block_reasons)
                if str(reason) not in {
                    "within_route_anchor_overlap_conflict",
                    "batch_level_conflict_resolution",
                    "chain_gate:multiple_possible_chain_links",
                    "node_resolution_gate:chain_gate_failed_first",
                    "multiple_possible_chain_links",
                    "chain_gate_failed_first",
                }
            ]
            render_mode = "context_stable_preview_safe"
            if preview_reasons:
                validation["preview_review_gate"] = {
                    "passed": True,
                    "reason": "rendered_for_foreman_verification",
                    "review_reasons": preview_reasons,
                    "mode": "context_stable_preview_safe",
                }
            else:
                validation["preview_review_gate"] = {
                    "passed": True,
                    "reason": "clean_render_candidate",
                    "review_reasons": [],
                    "mode": "context_stable_preview_safe",
                }

        validation["render_gate"] = {
            "render_allowed": bool(render_allowed),
            "block_reasons": list(render_block_reasons),
            "mode": render_mode,
        }

        for point in group_station_points:
            point.setdefault("verification", {})
            point["verification"]["validation"] = validation
        for segment in group_redline_segments:
            segment.setdefault("verification", {})
            segment["verification"]["validation"] = validation

        group_matches.append(
            {
                "group_id": normalized_group.get("group_id"),
                "route_id": matched_route.get("route_id"),
                "route_name": matched_route.get("route_name"),
                "source_folder": matched_route.get("source_folder"),
                "confidence": round(float(selected_hypothesis.get("combined_score", 0.0) or 0.0), 3),
                "confidence_label": validation.get("confidence_label"),
                "final_decision": "; ".join(reason for reason in (selected_hypothesis.get("anchor_reasons") or []) if reason) or selected_ranking.get("reason"),
                "route_role": matched_route.get("route_role"),
                "expected_span_ft": selected_ranking.get("expected_span_ft"),
                "length_gap_ft": selected_ranking.get("length_gap_ft"),
                "print": str(group[0].get("print") or ""),
                "source_file": str(group[0].get("source_file") or ""),
                "print_filter": dict(filter_meta),
                "candidate_rankings": list(rankings),
                "mapping": dict(mapping),
                "validation": dict(validation),
                "selected_hypothesis": dict(selected_hypothesis),
                "score_breakdown": dict(selected_ranking.get("score_breakdown") or {}),
                "render_allowed": bool(render_allowed),
                "render_block_reasons": list(render_block_reasons),
                "rendered_station_point_count": len(group_station_points) if render_allowed else 0,
                "rendered_redline_segment_count": len(group_redline_segments) if render_allowed else 0,
                "group_station_points": list(group_station_points),
                "group_redline_segments": list(group_redline_segments),
                "_normalized_group": dict(normalized_group),
                "_matched_route": dict(matched_route),
                "_evaluated_hypotheses": list(evaluated_hypotheses),
            }
        )

        matching_debug.append(_build_matching_debug_record(normalized_group, filter_meta, rankings, anchored_hypotheses, selected_hypothesis, validation))

    group_matches = _apply_batch_level_conflict_resolution(group_matches)
    all_station_points = []
    all_redline_segments = []
    mapping_modes = []
    for match in group_matches:
        if bool(match.get("render_allowed")):
            all_station_points.extend(list(match.get("group_station_points") or []))
            all_redline_segments.extend(list(match.get("group_redline_segments") or []))
            mapping_modes.append(str((match.get("mapping") or {}).get("mode") or "absolute"))

    # propagate batch gate into matching_debug for consistency
    gate_by_group = {
        str(match.get("group_id") or ""): dict((match.get("validation") or {}).get("batch_conflict_resolution_gate") or {})
        for match in group_matches
    }
    for record in matching_debug:
        group_id = str(record.get("group_id") or "")
        if group_id in gate_by_group:
            record_validation = dict(record.get("validation") or {})
            record_validation["batch_conflict_resolution_gate"] = gate_by_group[group_id]
            record["validation"] = record_validation
            if not bool(gate_by_group[group_id].get("passed", True)):
                selected = dict(record.get("selected_hypothesis") or {})
                selected["batch_conflict_resolution_gate"] = gate_by_group[group_id]
                record["selected_hypothesis"] = selected

    STATE["station_points"] = all_station_points
    STATE["redline_segments"] = list(all_redline_segments)
    STATE["station_mapping_mode"] = ",".join(sorted(set(mapping_modes))) if mapping_modes else None
    STATE["station_mapping_min_ft"] = None
    STATE["station_mapping_max_ft"] = None
    STATE["station_mapping_range_ft"] = None
    STATE["matching_debug"] = matching_debug

    rendered_matches = [match for match in group_matches if bool(match.get("render_allowed"))]

    unique_route_ids = []
    for match in rendered_matches:
        route_id = match.get("route_id")
        if route_id and route_id not in unique_route_ids:
            unique_route_ids.append(route_id)

    selected_rendered_match = None
    if rendered_matches:
        selected_rendered_match = sorted(
            rendered_matches,
            key=lambda match: (
                0 if bool(((match.get("selected_hypothesis") or {}).get("authoritative_route_commit") or {}).get("committed")) else 1,
                -int(match.get("rendered_station_point_count") or 0),
                -float(match.get("confidence") or 0.0),
                str(match.get("group_id") or ""),
            ),
        )[0]

    if len(unique_route_ids) == 1:
        matched_route = _find_route_by_id(unique_route_ids[0])
        if matched_route:
            _set_active_route(matched_route)
        STATE["selected_route_match"] = selected_rendered_match
    else:
        STATE["selected_route_match"] = None

    STATE["route_match_candidates"] = group_matches
    warn_count = sum(1 for record in matching_debug if str(record.get("validation", {}).get("validation_status") or "") == "warn")
    fail_count = sum(1 for record in matching_debug if str(record.get("validation", {}).get("validation_status") or "") == "fail")
    blocked_count = sum(1 for match in group_matches if not bool(match.get("render_allowed")))
    STATE["verification_summary"] = {
        "status": "independent_route_matching_active" if group_matches else "awaiting_bore_logs",
        "version": "v4",
        "route_selection_method": "candidate_pool_plus_anchored_hypothesis_validation_with_final_render_gate",
        "route_selection_reason": "Each bore-log group now flows through normalization, candidate-pool scoring, anchored hypothesis selection, post-match validation, and a final deterministic render gate before stations and redlines are accepted onto the map.",
        "group_count": len(group_matches),
        "unique_matched_routes": len(unique_route_ids),
        "rendered_group_count": len(rendered_matches),
        "blocked_group_count": blocked_count,
        "warn_count": warn_count,
        "fail_count": fail_count,
    }


def _kmz_reference_lite() -> Dict[str, Any]:
    kmz_reference = STATE.get("kmz_reference", {}) or {}
    visual_reference = dict(kmz_reference.get("visual_reference", {}) or {})
    return {
        "folder_summary": kmz_reference.get("folder_summary", []) or [],
        "line_role_summary": kmz_reference.get("line_role_summary", []) or [],
        "point_role_summary": kmz_reference.get("point_role_summary", []) or [],
        "line_layers": kmz_reference.get("line_layers", []) or [],
        "explicit_redline_layers": kmz_reference.get("explicit_redline_layers", []) or [],
        "visual_reference": visual_reference,
        # Keep actual KMZ render geometry in the lightweight payload because the frontend map
        # depends on these arrays to draw the design. The heavy debug objects stay excluded.
        "line_features": kmz_reference.get("line_features", []) or [],
        "polygon_features": kmz_reference.get("polygon_features", []) or [],
        "point_features": kmz_reference.get("point_features", []) or [],
        "line_feature_count": len(kmz_reference.get("line_features", []) or []),
        "polygon_feature_count": len(kmz_reference.get("polygon_features", []) or []),
        "point_feature_count": len(kmz_reference.get("point_features", []) or []),
    }


def _compact_group_payload_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    validation = dict(entry.get("validation") or {})
    route_consensus_gate = dict(validation.get("route_consensus_gate") or {})
    preview_review_gate = dict(validation.get("preview_review_gate") or {})
    render_gate = dict(validation.get("render_gate") or {})
    return {
        "group_id": entry.get("group_id"),
        "source_file": entry.get("source_file"),
        "print": entry.get("print"),
        "row_count": int(entry.get("row_count", 0) or 0),
        "min_station_ft": entry.get("min_station_ft"),
        "max_station_ft": entry.get("max_station_ft"),
        "selected_route_id": entry.get("route_id") or (entry.get("selected_hypothesis") or {}).get("route_id"),
        "selected_route_name": entry.get("route_name") or (entry.get("selected_hypothesis") or {}).get("route_name"),
        "render_allowed": bool(entry.get("render_allowed")),
        "rendered_station_point_count": int(entry.get("rendered_station_point_count", 0) or 0),
        "rendered_redline_segment_count": int(entry.get("rendered_redline_segment_count", 0) or 0),
        "validation_status": validation.get("validation_status"),
        "confidence_label": validation.get("confidence_label"),
        "route_consensus_gate": {
            "passed": route_consensus_gate.get("passed"),
            "reason": route_consensus_gate.get("reason"),
            "consensus_route_id": route_consensus_gate.get("consensus_route_id"),
        },
        "authoritative_route_id": (entry.get("mapping") or {}).get("authoritative_route_id")
            or (entry.get("selected_hypothesis") or {}).get("authoritative_route_id")
            or (entry.get("selected_hypothesis") or {}).get("mapping", {}).get("authoritative_route_id"),
        "preview_review_gate_reason": preview_review_gate.get("reason"),
        "render_gate_block_reasons": list(render_gate.get("block_reasons") or []),
    }


def _grouping_summary_from_rows(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    groups = _group_rows_for_matching(rows)
    summaries: List[Dict[str, Any]] = []
    for idx, group in enumerate(groups):
        station_values = [float(row.get("station_ft") or 0.0) for row in group if row.get("station_ft") is not None]
        summaries.append(
            {
                "group_id": f"group_{idx + 1}",
                "source_file": str(group[0].get("source_file") or "") if group else "",
                "print": ",".join(_collect_group_print_tokens(group)),
                "row_count": len(group),
                "min_station_ft": round(min(station_values), 2) if station_values else None,
                "max_station_ft": round(max(station_values), 2) if station_values else None,
            }
        )
    return summaries


def _selected_route_match_summary(match: Any) -> Dict[str, Any]:
    if not isinstance(match, dict):
        return {}

    candidate_rankings = match.get("candidate_rankings") or []
    preview_rankings: List[Dict[str, Any]] = []
    for item in candidate_rankings[:3]:
        if not isinstance(item, dict):
            continue
        preview_rankings.append(
            {
                "route_id": item.get("route_id"),
                "route_name": item.get("route_name"),
                "route_role": item.get("route_role"),
                "route_length_ft": item.get("route_length_ft"),
                "expected_span_ft": item.get("expected_span_ft"),
                "length_gap_ft": item.get("length_gap_ft"),
                "score": item.get("score"),
            }
        )

    return {
        "route_name": match.get("route_name"),
        "route_role": match.get("route_role"),
        "confidence_label": match.get("confidence_label"),
        "final_decision": match.get("final_decision"),
        "expected_span_ft": match.get("expected_span_ft"),
        "length_gap_ft": match.get("length_gap_ft"),
        "print": match.get("print"),
        "print_filter": match.get("print_filter") if isinstance(match.get("print_filter"), dict) else {},
        "candidate_rankings_preview": preview_rankings,
    }


def _segment_overlap_ft(a: Dict[str, Any], b: Dict[str, Any]) -> float:
    try:
        a_start = float(a.get("start_ft") or 0.0)
        a_end = float(a.get("end_ft") or 0.0)
        b_start = float(b.get("start_ft") or 0.0)
        b_end = float(b.get("end_ft") or 0.0)
    except Exception:
        return 0.0
    if a_end < a_start:
        a_start, a_end = a_end, a_start
    if b_end < b_start:
        b_start, b_end = b_end, b_start
    return max(0.0, min(a_end, b_end) - max(a_start, b_start))


def _segment_length(seg: Dict[str, Any]) -> float:
    try:
        start_ft = float(seg.get("start_ft") or 0.0)
        end_ft = float(seg.get("end_ft") or 0.0)
    except Exception:
        return 0.0
    if end_ft < start_ft:
        start_ft, end_ft = end_ft, start_ft
    return max(0.0, end_ft - start_ft)


def _classify_overlap(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    overlap_ft = _segment_overlap_ft(a, b)
    a_length = _segment_length(a)
    b_length = _segment_length(b)
    denom = max(min(a_length, b_length), 1e-9)
    overlap_ratio = overlap_ft / denom if overlap_ft > 0.0 else 0.0
    same_provenance = (
        str(a.get("source_file") or "").strip() == str(b.get("source_file") or "").strip()
        and str(a.get("crew") or "").strip() == str(b.get("crew") or "").strip()
        and str(a.get("date") or "").strip() == str(b.get("date") or "").strip()
    )
    if same_provenance and overlap_ratio > 0.85:
        overlap_type = "drop_duplicate"
    elif same_provenance and overlap_ratio > 0.5:
        overlap_type = "trim_partial"
    elif overlap_ft > 0.0:
        overlap_type = "minor_overlap_keep"
    else:
        overlap_type = "no_overlap"
    return {
        "overlap_ft": round(overlap_ft, 2),
        "overlap_ratio": round(overlap_ratio, 6),
        "same_provenance": bool(same_provenance),
        "classification": overlap_type,
    }


def _subtract_overlap(seg: Dict[str, Any], existing: Dict[str, Any]) -> List[Dict[str, Any]]:
    overlap_ft = _segment_overlap_ft(seg, existing)
    if overlap_ft <= 0.0:
        return [dict(seg)]

    try:
        seg_start = float(seg.get("start_ft") or 0.0)
        seg_end = float(seg.get("end_ft") or 0.0)
        existing_start = float(existing.get("start_ft") or 0.0)
        existing_end = float(existing.get("end_ft") or 0.0)
    except Exception:
        return [dict(seg)]

    if seg_end < seg_start:
        seg_start, seg_end = seg_end, seg_start
    if existing_end < existing_start:
        existing_start, existing_end = existing_end, existing_start

    remainders: List[Tuple[float, float]] = []
    if seg_start < existing_start:
        remainders.append((seg_start, min(seg_end, existing_start)))
    if seg_end > existing_end:
        remainders.append((max(seg_start, existing_end), seg_end))

    route_id = str(seg.get("route_id") or seg.get("matched_route_id") or "").strip()
    route = _find_route_by_id(route_id)
    route_coords = list((route or {}).get("coords") or seg.get("coords") or [])

    trimmed: List[Dict[str, Any]] = []
    part_index = 1
    for part_start, part_end in remainders:
        if part_end - part_start <= 0.01:
            continue
        part_seg = dict(seg)
        part_seg["start_ft"] = round(part_start, 2)
        part_seg["end_ft"] = round(part_end, 2)
        part_seg["length_ft"] = round(part_end - part_start, 2)
        if route_coords:
            clipped = _clip_route_segment(route_coords, part_start, part_end)
            if len(clipped) >= 2:
                part_seg["coords"] = clipped
        part_seg["segment_id"] = f"{str(seg.get('segment_id') or 'segment')}__trim_{part_index}"
        trimmed.append(part_seg)
        part_index += 1
    return trimmed


def _deduplicate_segments(segments: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    route_rank: Dict[str, int] = {
        str(route.get("route_id") or "").strip(): idx
        for idx, route in enumerate(STATE.get("route_catalog", []) or [])
    }

    ordered = sorted(
        [dict(seg) for seg in (segments or [])],
        key=lambda seg: (
            route_rank.get(str(seg.get("route_id") or seg.get("matched_route_id") or "").strip(), 10**9),
            str(seg.get("route_id") or seg.get("matched_route_id") or "").strip(),
            float(seg.get("start_ft") or 0.0),
            float(seg.get("end_ft") or 0.0),
            str(seg.get("source_file") or ""),
            str(seg.get("crew") or ""),
            str(seg.get("date") or ""),
            int(seg.get("row_index") or 0),
            str(seg.get("segment_id") or ""),
        ),
    )

    accepted: List[Dict[str, Any]] = []
    for segment in ordered:
        current_parts = [dict(segment)]
        for existing in accepted:
            existing_route_id = str(existing.get("route_id") or existing.get("matched_route_id") or "").strip()
            next_parts: List[Dict[str, Any]] = []
            for part in current_parts:
                part_route_id = str(part.get("route_id") or part.get("matched_route_id") or "").strip()
                if not part_route_id or part_route_id != existing_route_id:
                    next_parts.append(part)
                    continue
                overlap_meta = _classify_overlap(part, existing)
                classification = str(overlap_meta.get("classification") or "")
                if classification == "drop_duplicate":
                    continue
                if classification == "trim_partial":
                    next_parts.extend(_subtract_overlap(part, existing))
                    continue
                next_parts.append(part)
            current_parts = next_parts
            if not current_parts:
                break
        accepted.extend(current_parts)
    return accepted


def _merge_route_intervals(intervals: Sequence[Tuple[float, float]], tolerance_ft: float = 0.01) -> List[Tuple[float, float]]:
    cleaned: List[Tuple[float, float]] = []
    for start_ft, end_ft in intervals:
        try:
            start_val = float(start_ft)
            end_val = float(end_ft)
        except Exception:
            continue
        if end_val < start_val:
            start_val, end_val = end_val, start_val
        if end_val - start_val <= 0.0:
            continue
        cleaned.append((start_val, end_val))
    if not cleaned:
        return []
    cleaned.sort(key=lambda item: (item[0], item[1]))
    merged: List[Tuple[float, float]] = [cleaned[0]]
    merge_tolerance = max(0.0, float(tolerance_ft or 0.0))
    for start_val, end_val in cleaned[1:]:
        prev_start, prev_end = merged[-1]
        if start_val <= prev_end + merge_tolerance:
            merged[-1] = (prev_start, max(prev_end, end_val))
        else:
            merged.append((start_val, end_val))
    return merged


def _unique_coverage_summary(redline_segments: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    route_segments: Dict[str, List[Tuple[float, float]]] = {}
    route_names: Dict[str, str] = {}
    raw_length_ft = 0.0
    tolerance_ft = 5.0

    for segment in redline_segments or []:
        route_id = str(segment.get('route_id') or segment.get('matched_route_id') or '').strip()
        if not route_id:
            continue
        try:
            start_val = float(segment.get('start_ft'))
            end_val = float(segment.get('end_ft'))
        except Exception:
            continue
        if end_val < start_val:
            start_val, end_val = end_val, start_val
        segment_length_ft = max(0.0, end_val - start_val)
        if segment_length_ft <= 0.0:
            continue

        interval = (start_val, end_val)
        route_segments.setdefault(route_id, []).append(interval)
        route_names[route_id] = str(segment.get('route_name') or segment.get('matched_route_name') or route_id)
        raw_length_ft += segment_length_ft

    routes: List[Dict[str, Any]] = []
    total_interval_count = 0
    total_merged_interval_count = 0
    total_unique_length_ft = 0.0

    for route_id in sorted(route_segments.keys()):
        intervals = sorted(route_segments.get(route_id, []), key=lambda item: (item[0], item[1]))
        total_interval_count += len(intervals)

        merged_intervals = _merge_route_intervals(intervals, tolerance_ft=tolerance_ft)
        total_merged_interval_count += len(merged_intervals)
        route_unique_length_ft = sum(max(0.0, end_val - start_val) for start_val, end_val in merged_intervals)
        total_unique_length_ft += route_unique_length_ft

        routes.append(
            {
                'route_id': route_id,
                'route_name': route_names.get(route_id, route_id),
                'merged_intervals': [
                    {
                        'start_ft': round(start_val, 2),
                        'end_ft': round(end_val, 2),
                        'length_ft': round(max(0.0, end_val - start_val), 2),
                    }
                    for start_val, end_val in merged_intervals
                ],
                'unique_length_ft': round(route_unique_length_ft, 2),
            }
        )

    unique_length_ft = round(total_unique_length_ft, 2)
    raw_length_ft = round(raw_length_ft, 2)
    deduped_overlap_ft = round(max(0.0, raw_length_ft - unique_length_ft), 2)

    return {
        'raw_length_ft': raw_length_ft,
        'unique_length_ft': unique_length_ft,
        'deduped_overlap_ft': deduped_overlap_ft,
        'route_interval_count': total_interval_count,
        'route_merged_interval_count': total_merged_interval_count,
        'routes': routes,
    }


def _coverage_runtime_verification(redline_segments: Sequence[Dict[str, Any]], coverage_summary: Dict[str, Any]) -> Dict[str, Any]:
    raw_length_ft = round(float(coverage_summary.get('raw_length_ft', 0.0) or 0.0), 2)
    unique_length_ft = round(float(coverage_summary.get('unique_length_ft', 0.0) or 0.0), 2)
    overlap_removed_ft = round(max(0.0, raw_length_ft - unique_length_ft), 2)
    return {
        'module_file': str(Path(__file__).resolve()),
        'coverage_function_mode': 'merged_unique_intervals',
        'coverage_function_marker': 'RUNTIME_VERIFY_MERGED_UNIQUE_V5',
        'coverage_source_segment_count': len(redline_segments or []),
        'coverage_source_interval_count': int(coverage_summary.get('route_interval_count', 0) or 0),
        'coverage_raw_length_ft': raw_length_ft,
        'coverage_unique_length_ft': unique_length_ft,
        'coverage_overlap_removed_ft': overlap_removed_ft,
    }


def _total_design_length_ft(route_catalog: Sequence[Dict[str, Any]]) -> float:
    total_ft = 0.0
    seen_route_ids = set()
    for route in route_catalog or []:
        route_id = str(route.get('route_id') or '').strip()
        if not route_id or route_id in seen_route_ids:
            continue
        seen_route_ids.add(route_id)
        total_ft += max(0.0, float(route.get('length_ft', 0.0) or 0.0))
    return round(total_ft, 2)


def _summary_payload(include_debug: bool = False) -> Dict[str, Any]:
    route_id = STATE.get("route_id")
    route_coords = STATE.get("route_coords", []) or []
    route_length_ft = float(STATE.get("route_length_ft", 0.0) or 0.0)
    redline_segments = STATE.get("redline_segments", []) or []
    station_points = STATE.get("station_points", []) or []
    active_route_id = str(route_id or "").strip()
    active_route_station_points = [
        point
        for point in station_points
        if str(point.get("route_id") or point.get("matched_route_id") or "").strip() == active_route_id
    ] if active_route_id else []
    active_route_redline_segments = [
        segment
        for segment in redline_segments
        if str(segment.get("route_id") or segment.get("matched_route_id") or "").strip() == active_route_id
    ] if active_route_id else []
    route_catalog = STATE.get("route_catalog", []) or []
    matching_debug = STATE.get("matching_debug", []) or []
    route_match_candidates = STATE.get("route_match_candidates", []) or []
    committed_rows = STATE.get("committed_rows", []) or []
    grouped_rows_summary = _grouping_summary_from_rows(committed_rows)
    compact_group_summaries = [_compact_group_payload_entry(entry) for entry in route_match_candidates]
    rendered_group_count = sum(1 for entry in compact_group_summaries if entry.get("render_allowed"))
    blocked_group_count = max(0, len(compact_group_summaries) - rendered_group_count)

    coverage_basis_segments = redline_segments
    coverage_summary = _unique_coverage_summary(redline_segments)
    active_route_coverage_summary = _unique_coverage_summary(active_route_redline_segments)

    coverage_route_ids = {
        str(route_entry.get("route_id") or "").strip()
        for route_entry in (coverage_summary.get("routes") or [])
        if str(route_entry.get("route_id") or "").strip()
    }
    if coverage_route_ids:
        total_design_length_ft = sum(
            float(route_entry.get("length_ft", 0.0) or 0.0)
            for route_entry in route_catalog
            if str(route_entry.get("route_id") or "").strip() in coverage_route_ids
        )
    else:
        total_design_length_ft = route_length_ft if route_length_ft > 0.0 else _total_design_length_ft(route_catalog)

    covered_length_ft = float(coverage_summary.get("unique_length_ft", 0.0) or 0.0)
    completion_pct = round((covered_length_ft / total_design_length_ft) * 100.0, 2) if total_design_length_ft > 0 else 0.0
    active_route_covered_length_ft = float(active_route_coverage_summary.get("unique_length_ft", 0.0) or 0.0)
    active_route_completion_pct = round((active_route_covered_length_ft / route_length_ft) * 100.0, 2) if route_length_ft > 0 else 0.0
    merged_segment_count_for_coverage = int(coverage_summary.get("route_merged_interval_count", 0) or 0)
    raw_segment_count_for_coverage = len(redline_segments)
    runtime_verification = _coverage_runtime_verification(redline_segments, coverage_summary)
    active_route_runtime_verification = _coverage_runtime_verification(active_route_redline_segments, active_route_coverage_summary)

    verification_summary = STATE.get("verification_summary", {}) or {}
    selected_route_match_summary = _selected_route_match_summary(STATE.get("selected_route_match"))

    if include_debug:
        payload = {
            "route_name": STATE.get("route_name"),
            "suggested_route_id": STATE.get("route_id"),
            "selected_route_id": STATE.get("route_id"),
            "selected_route_name": STATE.get("route_name"),
            "loaded_field_data_files": int(STATE.get("loaded_field_data_files", 0) or 0),
            "latest_structured_file": STATE.get("latest_structured_file"),
            "group_count": len(grouped_rows_summary),
            "rendered_group_count": rendered_group_count,
            "blocked_group_count": blocked_group_count,
            "station_points_count": len(station_points),
            "redline_segments_count": len(redline_segments),
            "total_row_count": len(committed_rows),
            "total_length_ft": total_design_length_ft,
            "covered_length_ft": covered_length_ft,
            "completion_pct": completion_pct,
            "station_mapping_mode": STATE.get("station_mapping_mode"),
            "station_mapping_min_ft": STATE.get("station_mapping_min_ft"),
            "station_mapping_max_ft": STATE.get("station_mapping_max_ft"),
            "station_mapping_range_ft": STATE.get("station_mapping_range_ft"),
            "verification_summary": verification_summary,
            "bug_report_count": len(STATE.get("bug_reports", []) or []),
            "recent_bug_reports": (STATE.get("bug_reports", []) or [])[:10],
            "billing": {
                "material_rate_per_ft": 3.5,
                "splicing_rate_per_ft": 1.5,
                "footage_ft": covered_length_ft,
                "material_total": round(covered_length_ft * 3.5, 2),
                "splicing_total": round(covered_length_ft * 1.5, 2),
                "grand_total": round((covered_length_ft * 3.5) + (covered_length_ft * 1.5), 2),
            },
            "counts": {
                "route_catalog": len(route_catalog),
                "route_match_candidates": len(route_match_candidates),
                "matching_debug": len(matching_debug),
                "station_points": len(station_points),
                "redline_segments": len(redline_segments),
            },
            "grouping_summary": grouped_rows_summary,
            "group_summaries": compact_group_summaries,
            "kmz_reference": _kmz_reference_lite(),
            "selected_route_match": STATE.get("selected_route_match"),
            "route_coords": route_coords,
            "map_points": route_coords,
            "committed_rows": committed_rows,
            "station_points": station_points,
            "redline_segments": redline_segments,
            "coverage_summary": coverage_summary,
            "active_route_coverage_summary": active_route_coverage_summary,
            "coverage_debug": {
                "coverage_basis": "all_final_redline_segments",
                "selected_route_length_ft": route_length_ft,
                "summary_total_length_ft": total_design_length_ft,
                "raw_final_redline_segment_count": raw_segment_count_for_coverage,
                "merged_segment_count": merged_segment_count_for_coverage,
            },
            "runtime_verification": runtime_verification,
            "active_route_runtime_verification": active_route_runtime_verification,
            "route_catalog": route_catalog,
            "route_match_candidates": route_match_candidates,
            "group_outputs": route_match_candidates,
            "matching_debug": matching_debug,
            "kmz_reference_full": STATE.get("kmz_reference", {}) or {},
        }
        return payload

    return {
        "route_id": route_id,
        "suggested_route_id": route_id,
        "selected_route_id": route_id,
        "route_name": STATE.get("route_name"),
        "selected_route_name": STATE.get("route_name"),
        "route_length_ft": route_length_ft,
        "route_coords": route_coords,
        "map_points": route_coords,
        "kmz_reference": _kmz_reference_lite(),
        "loaded_field_data_files": int(STATE.get("loaded_field_data_files", 0) or 0),
        "latest_structured_file": STATE.get("latest_structured_file"),
        "group_count": len(grouped_rows_summary),
        "rendered_group_count": rendered_group_count,
        "blocked_group_count": blocked_group_count,
        "station_points_count": len(station_points),
        "redline_segments_count": len(redline_segments),
        "station_points": station_points,
        "redline_segments": redline_segments,
        "active_route_station_points_count": len(active_route_station_points),
        "active_route_redline_segments_count": len(active_route_redline_segments),
        "active_route_station_points": active_route_station_points,
        "active_route_redline_segments": active_route_redline_segments,
        "total_row_count": len(committed_rows),
        "total_length_ft": total_design_length_ft,
        "covered_length_ft": covered_length_ft,
        "completion_pct": completion_pct,
        "active_route_covered_length_ft": active_route_covered_length_ft,
        "active_route_completion_pct": active_route_completion_pct,
        "billing": {
            "material_rate_per_ft": 3.5,
            "splicing_rate_per_ft": 1.5,
            "footage_ft": covered_length_ft,
            "material_total": round(covered_length_ft * 3.5, 2),
            "splicing_total": round(covered_length_ft * 1.5, 2),
            "grand_total": round((covered_length_ft * 3.5) + (covered_length_ft * 1.5), 2),
        },
        "coverage_debug": {
            "coverage_basis": "all_final_redline_segments",
            "selected_route_length_ft": route_length_ft,
            "summary_total_length_ft": total_design_length_ft,
            "raw_final_redline_segment_count": raw_segment_count_for_coverage,
            "merged_segment_count": merged_segment_count_for_coverage,
        },
        "station_mapping_mode": STATE.get("station_mapping_mode"),
        "station_mapping_min_ft": STATE.get("station_mapping_min_ft"),
        "station_mapping_max_ft": STATE.get("station_mapping_max_ft"),
        "station_mapping_range_ft": STATE.get("station_mapping_range_ft"),
        "selected_route_match": selected_route_match_summary,
        "verification_summary": {
            "status": verification_summary.get("status"),
            "version": verification_summary.get("version"),
            "route_selection_method": verification_summary.get("route_selection_method"),
            "route_selection_reason": verification_summary.get("route_selection_reason"),
            "group_count": verification_summary.get("group_count"),
            "unique_matched_routes": verification_summary.get("unique_matched_routes"),
            "rendered_group_count": verification_summary.get("rendered_group_count"),
            "blocked_group_count": verification_summary.get("blocked_group_count"),
            "warn_count": verification_summary.get("warn_count"),
            "fail_count": verification_summary.get("fail_count"),
        },
        "bug_report_count": len(STATE.get("bug_reports", []) or []),
        "matching_debug_count": len(matching_debug),
        "route_match_candidate_count": len(route_match_candidates),
        "runtime_verification": runtime_verification,
        "active_route_runtime_verification": active_route_runtime_verification,
    }


@app.post("/api/upload-design")
async def upload_design(file: UploadFile = File(...)) -> JSONResponse:
    try:
        file_bytes = await file.read()
        route_catalog = _build_route_catalog(file_bytes, file.filename or "design.kmz")
        STATE["route_catalog"] = route_catalog
        STATE["kmz_reference"] = _build_kmz_reference(file_bytes, file.filename or "design.kmz")

        default_route = _choose_default_route(route_catalog)
        _set_active_route(default_route)

        rebuild_warning: Optional[str] = None

        if STATE.get("committed_rows"):
            try:
                _rebuild_field_data_outputs()
            except Exception as rebuild_exc:
                STATE["station_points"] = []
                STATE["redline_segments"] = []
                STATE["selected_route_match"] = None
                STATE["route_match_candidates"] = []
                STATE["matching_debug"] = []
                STATE["verification_summary"] = {
                    "status": "kmz_loaded_rebuild_pending",
                    "version": "v2",
                    "route_selection_method": "independent_candidate_scoring_per_group",
                    "route_selection_reason": "KMZ loaded successfully, but existing bore-log data needs to be re-uploaded after route rebuild failed.",
                    "group_count": 0,
                    "unique_matched_routes": 0,
                }
                rebuild_warning = f"KMZ uploaded, but previous bore-log overlays were cleared because rebuild failed: {rebuild_exc}"
        else:
            STATE["station_points"] = []
            STATE["redline_segments"] = []
            STATE["selected_route_match"] = None
            STATE["route_match_candidates"] = []
            STATE["matching_debug"] = []
            STATE["verification_summary"] = {
                "status": "awaiting_bore_logs",
                "version": "v2",
                "route_selection_method": "independent_candidate_scoring_per_group",
                "route_selection_reason": "KMZ candidate routes loaded. Bore-log matching will happen independently per group after field data upload.",
                "group_count": 0,
                "unique_matched_routes": 0,
            }

        payload = _summary_payload()
        if rebuild_warning:
            payload["warning"] = rebuild_warning
            payload["message"] = "Design uploaded successfully with previous overlays cleared."
            return _ok(**payload)

        return _ok(message="Design uploaded successfully", **payload)
    except Exception as exc:
        return _err(str(exc))


@app.post("/api/select-active-route")
async def select_active_route(route_id: str = Form(...)) -> JSONResponse:
    try:
        matched_route = _find_route_by_id(route_id)
        if not matched_route:
            return _err("Route not found.", status_code=404)

        _set_active_route(matched_route)
        return _ok(message="Active route updated", **_summary_payload())
    except Exception as exc:
        return _err(str(exc))


@app.post("/api/upload-structured-bore-files")
async def upload_structured_bore_files(files: List[UploadFile] = File(...)) -> JSONResponse:
    try:
        existing_rows = list(STATE.get("committed_rows", []) or [])
        existing_by_file: Dict[str, List[Dict[str, Any]]] = {}
        for row in existing_rows:
            source_file = str(row.get("source_file") or "").strip()
            if not source_file:
                continue
            existing_by_file.setdefault(source_file, []).append(row)

        latest_name: Optional[str] = None

        for file in files:
            file_bytes = await file.read()
            latest_name = file.filename or "structured_file"
            existing_by_file[latest_name] = _read_bore_log_rows(file_bytes, latest_name)

        merged_rows: List[Dict[str, Any]] = []
        for source_file in sorted(existing_by_file.keys()):
            merged_rows.extend(existing_by_file[source_file])

        STATE["committed_rows"] = merged_rows
        STATE["loaded_field_data_files"] = len(existing_by_file)
        STATE["latest_structured_file"] = latest_name

        _rebuild_field_data_outputs()
        return _ok(message="Bore logs uploaded successfully", **_summary_payload())
    except Exception as exc:
        return _err(str(exc))



@app.post("/api/reset-state")
def reset_state() -> JSONResponse:
    _reset_workspace_state()
    return _ok(message="Workspace reset successfully", **_summary_payload())


@app.get("/api/current-state")
def current_state() -> JSONResponse:
    return _ok(**_summary_payload(include_debug=False))


@app.get("/api/debug-state")
def debug_state() -> JSONResponse:
    return _ok(**_summary_payload(include_debug=True))


@app.post("/api/report-bug")
def report_bug(payload: Dict[str, Any] = Body(...)) -> JSONResponse:
    bug_reports = list(STATE.get("bug_reports", []) or [])
    entry = {
        "id": str(payload.get("id") or ""),
        "timestamp": str(payload.get("timestamp") or ""),
        "level": str(payload.get("level") or "info"),
        "category": str(payload.get("category") or "ui"),
        "message": str(payload.get("message") or ""),
        "details": payload.get("details") if isinstance(payload.get("details"), dict) else {},
    }
    bug_reports.insert(0, entry)
    STATE["bug_reports"] = bug_reports[:200]
    return _ok(message="Bug report captured", bug_report_count=len(STATE["bug_reports"]))


@app.get("/api/bug-reports")
def get_bug_reports() -> JSONResponse:
    return _ok(bug_reports=STATE.get("bug_reports", []) or [])

STATION_PHOTO_ROOT = BASE_DIR / "uploads" / "station_photos"
STATION_PHOTO_INDEX_PATH = STATION_PHOTO_ROOT / "index.json"
STATION_PHOTO_MAX_FILES_PER_UPLOAD = 10


def _ensure_station_photo_storage() -> None:
    STATION_PHOTO_ROOT.mkdir(parents=True, exist_ok=True)
    if not STATION_PHOTO_INDEX_PATH.exists():
        STATION_PHOTO_INDEX_PATH.write_text(json.dumps({"photos": []}, indent=2), encoding="utf-8")


def _load_station_photo_index() -> Dict[str, Any]:
    _ensure_station_photo_storage()
    try:
        data = json.loads(STATION_PHOTO_INDEX_PATH.read_text(encoding="utf-8"))
    except Exception:
        data = {"photos": []}
    if not isinstance(data, dict):
        data = {"photos": []}
    photos = data.get("photos")
    if not isinstance(photos, list):
        data["photos"] = []
    return data


def _save_station_photo_index(index_data: Dict[str, Any]) -> None:
    _ensure_station_photo_storage()
    temp_path = STATION_PHOTO_INDEX_PATH.with_suffix(".tmp")
    temp_path.write_text(json.dumps(index_data, indent=2), encoding="utf-8")
    temp_path.replace(STATION_PHOTO_INDEX_PATH)


def _safe_photo_name(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "file"
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in raw)
    cleaned = cleaned.strip("._")
    return cleaned or "file"


def _station_photo_identity_raw(
    route_name: Any,
    source_file: Any,
    station_label: Any,
    mapped_station_ft: Any,
    lat: Any,
    lon: Any,
) -> str:
    key_parts = [
        str(route_name or "").strip(),
        str(source_file or "").strip(),
        str(station_label or "").strip(),
        str(mapped_station_ft or "").strip(),
        str(lat or "").strip(),
        str(lon or "").strip(),
    ]
    return "|".join(key_parts)


def _station_photo_identity_hash(raw_identity: Any) -> str:
    raw = str(raw_identity or "").strip()
    if not raw:
        return ""
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _station_photo_folder(station_identity_hash: str) -> Path:
    return STATION_PHOTO_ROOT / station_identity_hash


def _station_photo_public_record(record: Dict[str, Any]) -> Dict[str, Any]:
    photo_id = str(record.get("photo_id") or "").strip()
    return {
        "photo_id": photo_id,
        "station_identity": str(record.get("station_identity") or ""),
        "station_summary": str(record.get("station_summary") or ""),
        "original_filename": str(record.get("original_filename") or ""),
        "stored_filename": str(record.get("stored_filename") or ""),
        "content_type": str(record.get("content_type") or ""),
        "uploaded_at": str(record.get("uploaded_at") or ""),
        "relative_url": f"/api/station-photos/file/{photo_id}",
    }


@app.get("/api/station-photos")
async def get_station_photos(station_identity: str) -> JSONResponse:
    station_identity_raw = str(station_identity or "").strip()
    if not station_identity_raw:
        return _err("station_identity is required.")
    station_identity_hash = _station_photo_identity_hash(station_identity_raw)
    index_data = _load_station_photo_index()
    matches = [
        _station_photo_public_record(record)
        for record in index_data.get("photos", [])
        if str(record.get("station_identity_hash") or "").strip() == station_identity_hash
    ]
    matches.sort(key=lambda item: str(item.get("uploaded_at") or ""), reverse=True)
    return _ok(
        photos=matches,
        station_identity=station_identity_raw,
        station_identity_hash=station_identity_hash,
    )


@app.post("/api/station-photos/upload")
async def upload_station_photos(
    station_identity: str = Form(...),
    station_summary: str = Form(""),
    route_name: str = Form(""),
    source_file: str = Form(""),
    station_label: str = Form(""),
    mapped_station_ft: str = Form(""),
    lat: str = Form(""),
    lon: str = Form(""),
    files: List[UploadFile] = File(...),
) -> JSONResponse:
    station_identity_raw = str(station_identity or "").strip()
    if not station_identity_raw:
        return _err("station_identity is required.")

    expected_identity_raw = _station_photo_identity_raw(
        route_name, source_file, station_label, mapped_station_ft, lat, lon
    )
    if station_identity_raw != expected_identity_raw:
        return _err("Selected station identity did not match the upload payload.")

    station_identity_hash = _station_photo_identity_hash(station_identity_raw)

    upload_files = list(files or [])
    if not upload_files:
        return _err("At least one image file is required.")
    if len(upload_files) > STATION_PHOTO_MAX_FILES_PER_UPLOAD:
        return _err(f"Upload up to {STATION_PHOTO_MAX_FILES_PER_UPLOAD} files at a time.")

    _ensure_station_photo_storage()
    station_folder = _station_photo_folder(station_identity_hash)
    station_folder.mkdir(parents=True, exist_ok=True)

    index_data = _load_station_photo_index()
    photo_records: List[Dict[str, Any]] = index_data.setdefault("photos", [])

    created: List[Dict[str, Any]] = []
    for upload in upload_files:
        original_filename = _safe_photo_name(upload.filename or "image")
        content_type = str(upload.content_type or "").strip().lower()
        if content_type and not content_type.startswith("image/"):
            return _err(f"{original_filename} is not an image upload.")

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        extension = Path(original_filename).suffix or ""
        photo_id = hashlib.sha256(
            f"{station_identity_hash}|{original_filename}|{timestamp}".encode("utf-8")
        ).hexdigest()[:24]
        stored_filename = f"{timestamp}_{photo_id}{extension}"
        stored_path = station_folder / stored_filename

        with open(stored_path, "wb") as handle:
            shutil.copyfileobj(upload.file, handle)

        record = {
            "photo_id": photo_id,
            "station_identity": station_identity_raw,
            "station_identity_hash": station_identity_hash,
            "station_summary": str(station_summary or "").strip(),
            "route_name": str(route_name or "").strip(),
            "source_file": str(source_file or "").strip(),
            "station_label": str(station_label or "").strip(),
            "mapped_station_ft": str(mapped_station_ft or "").strip(),
            "lat": str(lat or "").strip(),
            "lon": str(lon or "").strip(),
            "original_filename": original_filename,
            "stored_filename": stored_filename,
            "stored_path": str(stored_path),
            "content_type": content_type,
            "uploaded_at": datetime.now(timezone.utc).isoformat(),
        }
        photo_records.append(record)
        created.append(_station_photo_public_record(record))

    _save_station_photo_index(index_data)
    return _ok(
        message=f"Uploaded {len(created)} station photo{'s' if len(created) != 1 else ''}.",
        station_identity=station_identity_raw,
        station_identity_hash=station_identity_hash,
        photos=created,
    )


@app.get("/api/station-photos/file/{photo_id}")
async def get_station_photo_file(photo_id: str):
    target = str(photo_id or "").strip()
    if not target:
        return _err("photo_id is required.")
    index_data = _load_station_photo_index()
    for record in index_data.get("photos", []):
        if str(record.get("photo_id") or "").strip() != target:
            continue
        stored_path = str(record.get("stored_path") or "").strip()
        if not stored_path or not os.path.exists(stored_path):
            return _err("Photo file was not found.", status_code=404)
        content_type = str(record.get("content_type") or "").strip() or None
        return FileResponse(
            stored_path,
            media_type=content_type,
            filename=str(record.get("original_filename") or os.path.basename(stored_path)),
        )
    return _err("Photo file was not found.", status_code=404)
