import json
import zipfile
import xml.etree.ElementTree as ET
from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple

import folium
import pandas as pd
import requests
import streamlit as st
from folium import plugins
from streamlit_folium import st_folium

BACKEND = "http://127.0.0.1:8000"

st.set_page_config(page_title="OSP Redlining Software", layout="wide")
st.title("OSP Redlining Software")
st.caption("Upload KMZ → upload all bore logs at once → review per-file route assignments → export to Google Earth")


def _parse_kml_coordinate_text(coord_text: str) -> List[List[float]]:
    coords: List[List[float]] = []
    for token in str(coord_text or "").replace("\n", " ").replace("\t", " ").split():
        parts = token.split(",")
        if len(parts) < 2:
            continue
        try:
            lon = float(parts[0])
            lat = float(parts[1])
        except Exception:
            continue
        coords.append([lat, lon])
    return coords


def extract_polygon_features_from_design_upload(file_bytes: bytes, file_name: str) -> List[Dict[str, Any]]:
    polygons: List[Dict[str, Any]] = []
    lower_name = str(file_name or "").lower()

    def _read_kml_payloads() -> List[bytes]:
        payloads: List[bytes] = []
        if lower_name.endswith(".kmz"):
            try:
                with zipfile.ZipFile(BytesIO(file_bytes)) as zf:
                    for member in zf.namelist():
                        if member.lower().endswith(".kml"):
                            payloads.append(zf.read(member))
            except Exception:
                return []
        elif lower_name.endswith(".kml"):
            payloads.append(file_bytes)
        return payloads

    for payload in _read_kml_payloads():
        try:
            root = ET.fromstring(payload)
        except Exception:
            continue

        for placemark in root.findall(".//{*}Placemark"):
            name_node = placemark.find("{*}name")
            polygon_node = placemark.find(".//{*}Polygon")
            if polygon_node is None:
                continue
            coord_node = polygon_node.find(".//{*}outerBoundaryIs/{*}LinearRing/{*}coordinates")
            if coord_node is None or not str(coord_node.text or "").strip():
                continue

            coords = _parse_kml_coordinate_text(coord_node.text or "")
            if len(coords) < 3:
                continue
            if coords[0] != coords[-1]:
                coords.append(coords[0])

            polygons.append(
                {
                    "name": str(name_node.text or "").strip() if name_node is not None else "KMZ Boundary",
                    "coords": coords,
                }
            )
    return polygons


def merge_polygon_features_into_kmz_reference(kmz_reference: Dict[str, Any], polygon_features: List[Dict[str, Any]]) -> Dict[str, Any]:
    merged = dict(kmz_reference or {})
    merged["polygon_features"] = polygon_features or []
    return merged


def _kmz_outline_polygons(kmz_reference: Dict[str, Any]) -> List[List[List[float]]]:
    polygons: List[List[List[float]]] = []
    for feature in (kmz_reference or {}).get("polygon_features", []) or []:
        coords = feature.get("coords", []) or []
        normalized: List[List[float]] = []
        for coord in coords:
            if not coord or len(coord) < 2:
                continue
            normalized.append([float(coord[0]), float(coord[1])])
        if len(normalized) < 3:
            continue
        if normalized[0] != normalized[-1]:
            normalized.append(normalized[0])
        polygons.append(normalized)
    return polygons



def station_to_feet(value: Any) -> Optional[int]:
    text = str(value or "").strip().replace(" ", "")
    if not text or "+" not in text:
        return None
    left, right = text.split("+", 1)
    if not left.isdigit() or not right.isdigit():
        return None
    return int(left) * 100 + int(right)


def normalize_station(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    text = text.replace("O", "0").replace("Q", "0").replace("I", "1").replace("L", "1")
    text = "".join(ch for ch in text if ch.isdigit() or ch == "+")
    if not text:
        return ""
    if "+" in text:
        left, right = text.split("+", 1)
        if not left or not right or not left.isdigit() or not right.isdigit():
            return ""
        return f"{int(left)}+{int(right):02d}"
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) < 3:
        return ""
    return f"{int(digits[:-2])}+{int(digits[-2:]):02d}"


def safe_float(value: Any) -> Optional[float]:
    text = str(value or "").strip()
    if text == "":
        return None
    try:
        return float(text)
    except Exception:
        text = "".join(ch for ch in text if ch.isdigit() or ch in ".-")
        if not text:
            return None
        try:
            return float(text)
        except Exception:
            return None


def _haversine_feet(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    import math
    r_m = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r_m * c * 3.28084


def _route_chainage_frontend(coords: List[List[float]]) -> List[float]:
    if not coords:
        return [0.0]
    chainage = [0.0]
    for i in range(1, len(coords)):
        chainage.append(chainage[-1] + _haversine_feet(float(coords[i - 1][0]), float(coords[i - 1][1]), float(coords[i][0]), float(coords[i][1])))
    return chainage


def _interpolate_point_frontend(a: List[float], b: List[float], ratio: float) -> List[float]:
    ratio = min(max(float(ratio), 0.0), 1.0)
    return [
        float(a[0]) + (float(b[0]) - float(a[0])) * ratio,
        float(a[1]) + (float(b[1]) - float(a[1])) * ratio,
    ]


def _point_at_distance_frontend(route_coords: List[List[float]], chainage: List[float], distance_ft: float) -> List[float]:
    if not route_coords:
        return [0.0, 0.0]
    if len(route_coords) == 1:
        return [float(route_coords[0][0]), float(route_coords[0][1])]
    d = min(max(float(distance_ft), 0.0), float(chainage[-1]))
    for idx in range(1, len(chainage)):
        seg_start = float(chainage[idx - 1])
        seg_end = float(chainage[idx])
        if d <= seg_end or idx == len(chainage) - 1:
            seg_len = max(seg_end - seg_start, 1e-9)
            ratio = (d - seg_start) / seg_len
            return _interpolate_point_frontend(route_coords[idx - 1], route_coords[idx], ratio)
    last = route_coords[-1]
    return [float(last[0]), float(last[1])]


def _slice_route_between_distances_frontend(route_coords: List[List[float]], start_distance_ft: float, end_distance_ft: float) -> List[List[float]]:
    if not route_coords:
        return []
    if len(route_coords) == 1:
        return [[float(route_coords[0][0]), float(route_coords[0][1])]]
    chainage = _route_chainage_frontend(route_coords)
    route_end_ft = float(chainage[-1]) if chainage else 0.0
    start_ft = min(max(float(start_distance_ft), 0.0), route_end_ft)
    end_ft = min(max(float(end_distance_ft), 0.0), route_end_ft)
    if end_ft < start_ft:
        start_ft, end_ft = end_ft, start_ft

    start_point = _point_at_distance_frontend(route_coords, chainage, start_ft)
    end_point = _point_at_distance_frontend(route_coords, chainage, end_ft)

    if abs(end_ft - start_ft) <= 1e-9:
        return [start_point, end_point]

    clipped: List[List[float]] = [start_point]
    for idx in range(1, len(route_coords) - 1):
        d = float(chainage[idx])
        if start_ft < d < end_ft:
            clipped.append([float(route_coords[idx][0]), float(route_coords[idx][1])])
    clipped.append(end_point)
    return clipped




def _valid_completed_trace_span(local_station_points: List[Dict[str, Any]]) -> bool:
    if len(local_station_points or []) < 2:
        return False
    start_point = local_station_points[0] or {}
    end_point = local_station_points[-1] or {}
    start_route_ft = start_point.get("mapped_route_ft", start_point.get("station_ft"))
    end_route_ft = end_point.get("mapped_route_ft", end_point.get("station_ft"))
    if start_route_ft is None or end_route_ft is None:
        return False
    try:
        start_route_ft = float(start_route_ft)
        end_route_ft = float(end_route_ft)
    except Exception:
        return False
    if abs(end_route_ft - start_route_ft) < 1.0:
        return False
    start_lat = start_point.get("lat")
    start_lon = start_point.get("lon")
    end_lat = end_point.get("lat")
    end_lon = end_point.get("lon")
    if None in (start_lat, start_lon, end_lat, end_lon):
        return False
    same_mapped_location = _haversine_feet(float(start_lat), float(start_lon), float(end_lat), float(end_lon)) < 1.0
    if same_mapped_location:
        return False
    return True

def build_local_station_points(station_rows_df: pd.DataFrame, route_coords: List[List[float]]) -> List[Dict[str, Any]]:
    if station_rows_df is None or station_rows_df.empty or len(route_coords or []) < 2:
        return []

    normalized_rows: List[Dict[str, Any]] = []
    seen = set()
    for _, row in station_rows_df.iterrows():
        station = normalize_station(row.get("station"))
        station_ft = station_to_feet(station)
        if not station or station_ft is None or station in seen:
            continue
        seen.add(station)
        normalized_rows.append(
            {
                "station": station,
                "station_ft": float(station_ft),
                "depth_ft": row.get("depth_ft"),
                "boc_ft": row.get("boc_ft"),
                "notes": str(row.get("notes", "") or "").strip(),
                "date": str(row.get("date", "") or "").strip(),
                "crew": str(row.get("crew", "") or "").strip(),
                "print": str(row.get("print", "") or "").strip(),
            }
        )

    if not normalized_rows:
        return []

    normalized_rows.sort(key=lambda item: item.get("station_ft", 0.0))
    base_station_ft = float(normalized_rows[0].get("station_ft", 0.0) or 0.0)
    chainage = _route_chainage_frontend(route_coords)

    points: List[Dict[str, Any]] = []
    for item in normalized_rows:
        mapped_route_ft = max(float(item.get("station_ft", 0.0) or 0.0) - base_station_ft, 0.0)
        lat, lon = _point_at_distance_frontend(route_coords, chainage, mapped_route_ft)
        points.append(
            {
                "station": item.get("station"),
                "station_ft": float(item.get("station_ft", 0.0) or 0.0),
                "mapped_route_ft": float(mapped_route_ft),
                "base_station_ft": float(base_station_ft),
                "lat": round(float(lat), 8),
                "lon": round(float(lon), 8),
                "depth_ft": item.get("depth_ft"),
                "boc_ft": item.get("boc_ft"),
                "notes": item.get("notes", ""),
                "date": item.get("date", ""),
                "crew": item.get("crew", ""),
                "print": item.get("print", ""),
            }
        )
    return points


def _route_catalog_index(route_catalog: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {str(item.get("route_id") or ""): item for item in (route_catalog or []) if str(item.get("route_id") or "")}


def _group_station_rows_by_source_file(station_rows_df: pd.DataFrame) -> List[Tuple[str, pd.DataFrame]]:
    if station_rows_df is None or station_rows_df.empty:
        return []
    working = station_rows_df.copy()
    if "source_file" not in working.columns:
        working["source_file"] = "field_file"
    working["source_file"] = working["source_file"].fillna("").astype(str).str.strip().replace({"": "field_file"})
    groups: List[Tuple[str, pd.DataFrame]] = []
    for source_file, group_df in working.groupby("source_file", sort=False):
        groups.append((str(source_file or "field_file"), group_df.reset_index(drop=True)))
    return groups


def build_source_file_route_runs(
    station_rows_df: pd.DataFrame,
    route_catalog: List[Dict[str, Any]],
    per_file_route_assignments: List[Dict[str, Any]],
    fallback_route_id: Optional[str],
    fallback_route_name: Optional[str],
    fallback_route_coords: List[List[float]],
) -> List[Dict[str, Any]]:
    groups = _group_station_rows_by_source_file(station_rows_df)
    if not groups:
        return []

    route_index = _route_catalog_index(route_catalog)
    assignment_by_file = {
        str(item.get("source_file") or "field_file"): item
        for item in (per_file_route_assignments or [])
        if str(item.get("source_file") or "field_file")
    }

    multi_file = len(groups) > 1
    runs: List[Dict[str, Any]] = []
    for source_file, group_df in groups:
        assignment = assignment_by_file.get(source_file, {})
        route_id = None
        route_name = None
        route_coords: List[List[float]] = []

        if multi_file:
            route_id = assignment.get("selected_route_id")
            route_name = assignment.get("selected_route_name")
            route_obj = route_index.get(str(route_id or "")) if route_id else None
            route_coords = (route_obj or {}).get("geometry", []) or []
        else:
            route_id = fallback_route_id or assignment.get("selected_route_id")
            route_name = fallback_route_name or assignment.get("selected_route_name")
            if fallback_route_coords:
                route_coords = fallback_route_coords
            else:
                route_obj = route_index.get(str(route_id or "")) if route_id else None
                route_coords = (route_obj or {}).get("geometry", []) or []

        local_station_points = build_local_station_points(group_df, route_coords)
        for point in local_station_points:
            point["source_file"] = source_file
            point["route_id"] = route_id
            point["route_name"] = route_name

        completed_trace_coords: List[List[float]] = []
        has_valid_completed_trace_span = _valid_completed_trace_span(local_station_points)
        if local_station_points and has_valid_completed_trace_span and len(route_coords or []) >= 2:
            start_station_ft = local_station_points[0].get("mapped_route_ft", local_station_points[0].get("station_ft"))
            end_station_ft = local_station_points[-1].get("mapped_route_ft", local_station_points[-1].get("station_ft"))
            completed_trace_coords = _slice_route_between_distances_frontend(route_coords, float(start_station_ft), float(end_station_ft))

        runs.append({
            "source_file": source_file,
            "route_id": route_id,
            "route_name": route_name,
            "route_coords": route_coords,
            "station_rows": group_df,
            "local_station_points": local_station_points,
            "completed_trace_coords": completed_trace_coords,
        })
    return runs


def _normalize_per_file_assignments(assignments: List[Dict[str, Any]], route_catalog: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    route_index = _route_catalog_index(route_catalog)
    normalized: List[Dict[str, Any]] = []
    for item in assignments or []:
        entry = dict(item or {})
        source_file = str(entry.get("source_file") or "field_file").strip() or "field_file"
        selected_route_id = str(entry.get("selected_route_id") or "").strip() or None
        selected_route_name = str(entry.get("selected_route_name") or "").strip() or None
        selected_route_match = entry.get("selected_route_match") or None
        candidates = entry.get("route_match_candidates") or []
        if selected_route_id and not selected_route_name:
            route_obj = route_index.get(selected_route_id) or {}
            selected_route_name = str(route_obj.get("name") or selected_route_id)
        if selected_route_id and not selected_route_match:
            selected_route_match = next((candidate for candidate in candidates if str(candidate.get("route_id") or "") == selected_route_id), None)
            if not selected_route_match:
                route_obj = route_index.get(selected_route_id) or {}
                if route_obj:
                    selected_route_match = {
                        "route_id": selected_route_id,
                        "route_name": selected_route_name,
                        "confidence_label": "Manual assignment",
                        "match_reasons": ["Route chosen manually for this source file."],
                        "coverage_ok": True,
                        "route_length_ft": route_obj.get("total_length_ft"),
                    }
        entry["source_file"] = source_file
        entry["selected_route_id"] = selected_route_id
        entry["selected_route_name"] = selected_route_name
        entry["selected_route_match"] = selected_route_match
        entry["route_match_candidates"] = candidates
        normalized.append(entry)
    return normalized




def _format_file_size(num_bytes: int) -> str:
    try:
        size = float(num_bytes or 0)
    except Exception:
        size = 0.0
    units = ["B", "KB", "MB", "GB"]
    idx = 0
    while size >= 1024.0 and idx < len(units) - 1:
        size /= 1024.0
        idx += 1
    if idx == 0:
        return f"{int(size)} {units[idx]}"
    return f"{size:.1f} {units[idx]}"


def _build_uploaded_field_file_rows(field_files: List[Any], assignments: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    assignment_by_file = {
        str(item.get("source_file") or "field_file").strip() or "field_file": item
        for item in (assignments or [])
    }
    rows: List[Dict[str, Any]] = []
    for uploaded in field_files or []:
        name = str(getattr(uploaded, "name", "field_file") or "field_file")
        extension = name.rsplit(".", 1)[-1].upper() if "." in name else "—"
        size_text = _format_file_size(int(getattr(uploaded, "size", 0) or 0))
        assignment = assignment_by_file.get(name) or {}
        selected_route_id = str(assignment.get("selected_route_id") or "").strip()
        selected_route_name = str(assignment.get("selected_route_name") or "").strip()
        if selected_route_id:
            status = "Assigned"
            route_text = selected_route_name or selected_route_id
        elif assignment:
            status = "Review needed"
            route_text = "Waiting for manual route assignment"
        else:
            status = "Ready to process"
            route_text = "Not processed yet"
        rows.append({
            "file_name": name,
            "type": extension,
            "size": size_text,
            "status": status,
            "route": route_text,
        })
    return rows


def render_bulk_upload_summary(field_files: List[Any], reference_pdfs: List[Any], assignments: List[Dict[str, Any]]) -> None:
    field_files = field_files or []
    reference_pdfs = reference_pdfs or []
    if not field_files and not reference_pdfs:
        return

    assigned_count = sum(1 for item in (assignments or []) if str(item.get("selected_route_id") or "").strip())
    needs_review_count = max(len(assignments or []) - assigned_count, 0)

    metric_cols = st.columns(4)
    metric_cols[0].metric("Bore log files selected", len(field_files))
    metric_cols[1].metric("Reference PDFs", len(reference_pdfs))
    metric_cols[2].metric("Assigned files", assigned_count)
    metric_cols[3].metric("Review needed", needs_review_count)

    file_rows = _build_uploaded_field_file_rows(field_files, assignments)

    if reference_pdfs:
        pdf_rows = [
            {
                "reference_pdf": str(getattr(uploaded, "name", "reference.pdf") or "reference.pdf"),
                "size": _format_file_size(int(getattr(uploaded, "size", 0) or 0)),
            }
            for uploaded in reference_pdfs
        ]
        with st.expander("Reference PDFs selected", expanded=False):
            st.dataframe(pd.DataFrame(pdf_rows), use_container_width=True, hide_index=True, height=min(220, 76 + (len(pdf_rows) * 35)))

def _enable_trace_render_layers() -> None:
    st.session_state.layer_show_main_route = True
    st.session_state.layer_show_redline_segments = True
    st.session_state.layer_show_station_labels = True
    st.session_state.layer_show_bore_stations = True


def _set_manual_route_assignment(source_file: str, route_id: str, route_catalog: List[Dict[str, Any]]) -> None:
    source_file = str(source_file or "field_file").strip() or "field_file"
    route_id = str(route_id or "").strip()
    route_index = _route_catalog_index(route_catalog)
    route_obj = route_index.get(route_id) or {}
    selected_route_name = str(route_obj.get("name") or route_id)
    assignments = _normalize_per_file_assignments(st.session_state.per_file_route_assignments or [], route_catalog)
    updated = False
    for entry in assignments:
        if str(entry.get("source_file") or "field_file") != source_file:
            continue
        candidates = entry.get("route_match_candidates") or []
        selected_match = next((candidate for candidate in candidates if str(candidate.get("route_id") or "") == route_id), None)
        if not selected_match:
            selected_match = {
                "route_id": route_id,
                "route_name": selected_route_name,
                "confidence_label": "Manual assignment",
                "match_reasons": ["Route chosen manually for this source file."],
                "coverage_ok": True,
                "route_length_ft": route_obj.get("total_length_ft"),
            }
        entry["selected_route_id"] = route_id
        entry["selected_route_name"] = selected_route_name
        entry["selected_route_match"] = selected_match
        entry["manual_assignment"] = True
        updated = True
        break
    if not updated:
        assignments.append({
            "source_file": source_file,
            "row_count": 0,
            "selected_route_id": route_id,
            "selected_route_name": selected_route_name,
            "selected_route_match": {
                "route_id": route_id,
                "route_name": selected_route_name,
                "confidence_label": "Manual assignment",
                "match_reasons": ["Route chosen manually for this source file."],
                "coverage_ok": True,
                "route_length_ft": route_obj.get("total_length_ft"),
            },
            "route_match_candidates": [],
            "manual_assignment": True,
        })
    st.session_state.per_file_route_assignments = assignments
    _enable_trace_render_layers()



def render_per_file_assignment_controls() -> None:
    assignments = _normalize_per_file_assignments(st.session_state.per_file_route_assignments or [], st.session_state.route_catalog or [])
    if not assignments:
        return

    assigned_count = sum(1 for item in assignments if str(item.get("selected_route_id") or "").strip())
    needs_review_count = max(len(assignments) - assigned_count, 0)

    expander_title = f"Route Assignment ({len(assignments)} files)"
    with st.expander(expander_title, expanded=False):
        st.caption(f"Assigned: {assigned_count} • Review: {needs_review_count}")

        for idx, entry in enumerate(assignments):
            source_file = str(entry.get("source_file") or "field_file")
            selected_route_id = entry.get("selected_route_id")
            candidates = entry.get("route_match_candidates") or []

            route_options = []
            seen = set()

            for item in candidates:
                rid = str(item.get("route_id") or "").strip()
                if rid and rid not in seen:
                    seen.add(rid)
                    route_options.append((rid, str(item.get("route_name") or rid)))

            for route in st.session_state.route_catalog or []:
                rid = str(route.get("route_id") or "").strip()
                if rid and rid not in seen:
                    seen.add(rid)
                    route_options.append((rid, str(route.get("name") or rid)))

            option_ids = [rid for rid, _ in route_options]
            row_left, row_mid, row_right = st.columns([2.2, 4.2, 1.0], vertical_alignment="center")

            with row_left:
                status = "Assigned" if selected_route_id else "Select route"
                st.markdown(f"**{source_file}**")
                st.caption(status)

            with row_mid:
                if option_ids:
                    current_id = str(selected_route_id or "")
                    default_index = option_ids.index(current_id) if current_id in option_ids else 0
                    chosen = st.selectbox(
                        "Route",
                        options=option_ids,
                        index=default_index,
                        format_func=lambda rid: next((label for r, label in route_options if r == rid), rid),
                        key=f"inline_route_{source_file}",
                        label_visibility="collapsed",
                    )
                else:
                    chosen = None
                    st.caption("No routes available")

            with row_right:
                if chosen:
                    if st.button("Apply", key=f"apply_inline_{source_file}", use_container_width=True):
                        _set_manual_route_assignment(source_file, chosen, st.session_state.route_catalog or [])
                        st.rerun()

            if idx < len(assignments) - 1:
                st.markdown("<div style='height:6px;'></div>", unsafe_allow_html=True)

def route_confidence_color(label: str) -> str:
    label = str(label or "").lower()
    if "strong" in label:
        return "#1B5E20"
    if "possible" in label or "review" in label:
        return "#E65100"
    if False:
        return "#B71C1C"
    return "#37474F"


def route_confidence_badge(label: str) -> str:
    label = str(label or "Review needed")
    color = route_confidence_color(label)
    return f"<span style='display:inline-block;padding:4px 9px;border-radius:999px;background:{color};color:white;font-size:12px;font-weight:600'>{label}</span>"


def recommendation_badge(label: str) -> str:
    label = str(label or "Review option")
    colors = {
        "Recommended": "#1B5E20",
        "Alternative": "#1565C0",
        "Review option": "#E65100",
        "Low-confidence option": "#6D4C41",
    }
    color = colors.get(label, "#455A64")
    return f"<span style='display:inline-block;padding:4px 9px;border-radius:999px;background:{color};color:white;font-size:12px;font-weight:700'>{label}</span>"


def _turn_angle_degrees(a: List[float], b: List[float], c: List[float]) -> float:
    import math
    v1 = (float(a[0]) - float(b[0]), float(a[1]) - float(b[1]))
    v2 = (float(c[0]) - float(b[0]), float(c[1]) - float(b[1]))
    n1 = math.hypot(v1[0], v1[1])
    n2 = math.hypot(v2[0], v2[1])
    if n1 <= 1e-12 or n2 <= 1e-12:
        return 0.0
    dot = max(-1.0, min(1.0, (v1[0] * v2[0] + v1[1] * v2[1]) / (n1 * n2)))
    return math.degrees(math.acos(dot))


def route_geometry_penalty(route: Optional[Dict[str, Any]]) -> float:
    if not route:
        return 0.0
    coords = route.get("geometry", []) or []
    if len(coords) < 3:
        return 0.0
    meaningful_turns = 0
    for idx in range(1, len(coords) - 1):
        angle = _turn_angle_degrees(coords[idx - 1], coords[idx], coords[idx + 1])
        if angle >= 25:
            meaningful_turns += 1
    length_ft = float(route.get("total_length_ft", 0.0) or 0.0)
    if length_ft <= 0:
        return float(meaningful_turns)
    return round((meaningful_turns / max(length_ft / 500.0, 1.0)), 2)


def recommendation_for_route(route: Optional[Dict[str, Any]], match: Optional[Dict[str, Any]], top_confidence: float, top_route_id: Optional[str]) -> Dict[str, Any]:
    role = str((route or {}).get("role") or "other").strip().lower()
    confidence = float((match or {}).get("confidence", 0.0) or 0.0)
    route_id = (route or {}).get("route_id")
    length_delta = (match or {}).get("length_delta_ft")
    coverage_ok = bool((match or {}).get("coverage_ok", False))
    penalty = route_geometry_penalty(route)
    reasons = []
    if coverage_ok:
        reasons.append("route covers the farthest logged station")
    else:
        reasons.append("route is shorter than the farthest logged station")
    if length_delta is not None:
        if float(length_delta) <= 50:
            reasons.append("very tight length fit to the bore span")
        elif float(length_delta) <= 150:
            reasons.append("good length fit to the bore span")
        else:
            reasons.append("length fit is weaker and needs review")
    if role in {"underground_cable", "backbone", "vacant_pipe", "terminal_tail"}:
        reasons.append(f"role looks drill-path relevant ({route_role_label(role)})")
    else:
        reasons.append(f"role is less drill-path oriented ({route_role_label(role)})")
    if penalty <= 0.5:
        reasons.append("geometry is simple with low extra-turn penalty")
    elif penalty <= 1.5:
        reasons.append("geometry is still believable but has a few extra turns")
    else:
        reasons.append("geometry has extra turns, so trust this less")

    if route_id == top_route_id and confidence >= 0.90 and coverage_ok:
        label = "Recommended"
    elif confidence >= max(0.75, top_confidence - 0.08):
        label = "Alternative"
    elif confidence >= 0.55:
        label = "Review option"
    else:
        label = "Low-confidence option"
    return {"label": label, "reasons": reasons, "geometry_penalty": penalty}


def render_route_match_panel(match: Optional[Dict[str, Any]], route: Optional[Dict[str, Any]], suggested_route_id: Optional[str], selected_route_id: Optional[str], recommendation: Optional[Dict[str, Any]] = None) -> None:
    if not match:
        st.caption("Upload bore-log data to get a route confidence hint.")
        return

    route_length = float(match.get("route_length_ft", 0.0) or 0.0)
    dataset_span = match.get("dataset_span_ft")
    length_delta = match.get("length_delta_ft")
    label = match.get("confidence_label") or "Review needed"
    confidence_pct = round(float(match.get("confidence", 0.0) or 0.0) * 100.0)
    fit_ratio = match.get("length_fit")
    fit_pct = round(float(fit_ratio) * 100.0, 1) if fit_ratio is not None else None

    hint_parts = []
    if selected_route_id and selected_route_id == suggested_route_id:
        hint_parts.append("Matches current top suggestion")
    elif suggested_route_id:
        hint_parts.append("Different from current top suggestion")
    if match.get("coverage_ok") is False:
        hint_parts.append("Route is shorter than the farthest station")

    rec_html = recommendation_badge(recommendation.get("label")) if recommendation else ""
    why_html = "".join(f"<li>{reason}</li>" for reason in (recommendation or {}).get("reasons", [])[:4])
    geometry_penalty = (recommendation or {}).get("geometry_penalty")
    html = f"""
    <div style='padding:12px 14px;border:1px solid #2d3748;border-radius:12px;background:#0f172a;margin-top:8px;margin-bottom:8px;'>
      <div style='display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;'>
        <div>
          <div style='font-size:13px;color:#cbd5e1;margin-bottom:6px;'>Route confidence</div>
          <div style='font-size:15px;font-weight:700;color:#f8fafc;'>{match.get('route_name') or 'Selected route'}</div>
        </div>
        <div style='display:flex;gap:8px;align-items:center;flex-wrap:wrap;'>{rec_html}{route_confidence_badge(label)}</div>
      </div>
      <div style='display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;margin-top:10px;font-size:13px;color:#e2e8f0;'>
        <div><b>Confidence:</b> {confidence_pct}%</div>
        <div><b>Route length:</b> {route_length:,.1f} ft</div>
        <div><b>Dataset span:</b> {dataset_span if dataset_span is not None else '—'} ft</div>
        <div><b>Length delta:</b> {length_delta if length_delta is not None else '—'} ft</div>
        <div><b>Span fit:</b> {fit_pct if fit_pct is not None else '—'}%</div>
        <div><b>Role:</b> {route_role_label(match.get('role') or 'other')}</div>
        <div><b>Geometry penalty:</b> {geometry_penalty if geometry_penalty is not None else '—'}</div>
        <div><b>Top suggestion:</b> {'Yes' if route and route.get('route_id') == suggested_route_id else 'No'}</div>
      </div>
      <div style='margin-top:10px;font-size:12px;color:#cbd5e1;'>{' • '.join(hint_parts) if hint_parts else 'Use this as a quick route sanity check, then validate against the map.'}</div>
      {"<div style='margin-top:10px;padding:10px;border-radius:10px;background:#111827;border:1px solid #334155;'><div style='font-size:12px;font-weight:700;color:#e2e8f0;margin-bottom:6px;'>Why this is suggested</div><ul style='margin:0 0 0 18px;padding:0;color:#cbd5e1;font-size:12px;'>" + why_html + "</ul></div>" if why_html else ''}
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)




def is_drill_path_role(role: Any) -> bool:
    return str(role or '').strip().lower() in {"backbone", "underground_cable", "vacant_pipe", "terminal_tail"}


def route_role_label(role: Any) -> str:
    role = str(role or "other").strip().lower()
    return {
        "backbone": "Backbone",
        "underground_cable": "Underground Cable",
        "vacant_pipe": "Vacant Pipe",
        "terminal_tail": "Terminal Tail",
        "house_drop": "House Drop",
        "redline_reference": "Redline Ref",
        "marker": "Marker",
        "other": "Other",
    }.get(role, role.replace("_", " ").title())


def persistent_layer_checkbox_row() -> None:
    st.caption("Persistent map layers")
    row1 = st.columns(4)
    row2 = st.columns(4)
    items = [
        ("layer_show_kmz_design", "KMZ Design"),
        ("layer_show_kmz_shadow", "KMZ Shadow"),
        ("layer_show_house_drops", "House Drops"),
        ("layer_show_kmz_nodes", "KMZ Nodes"),
        ("layer_show_main_route", "Main Route"),
        ("layer_show_redline_segments", "Redline Segments"),
        ("layer_show_station_labels", "BEGIN / END Labels"),
        ("layer_show_bore_stations", "Bore Stations"),
    ]
    for col, (key, label) in zip(row1 + row2, items):
        with col:
            st.checkbox(label, key=key)

def get_backend_json(method, url, **kwargs):
    resp = method(url, **kwargs)
    resp.raise_for_status()
    return resp.json()


def build_local_route_match(route: Optional[Dict[str, Any]], station_df: pd.DataFrame, suggested_route_id: Optional[str]) -> Optional[Dict[str, Any]]:
    if not route:
        return None
    route_length = float(route.get("total_length_ft", 0.0) or 0.0)
    station_feet: List[int] = []
    if station_df is not None and not station_df.empty and "station" in station_df.columns:
        for raw in station_df["station"].tolist():
            ft = station_to_feet(raw)
            if ft is not None:
                station_feet.append(ft)
    if station_feet:
        min_ft = min(station_feet)
        max_ft = max(station_feet)
        dataset_span = max_ft - min_ft
        farthest_station = max_ft
    else:
        dataset_span = None
        farthest_station = None
    if dataset_span is None:
        confidence = 1.0 if route.get("route_id") == suggested_route_id else 0.45
        label = "No station span yet"
        coverage_ok = True
        length_delta = None
        length_fit = None
    else:
        length_delta = abs(route_length - dataset_span)
        safe_span = max(dataset_span, 1.0)
        fit_ratio = route_length / safe_span
        fit_distance = abs(1.0 - fit_ratio)
        coverage_ok = route_length >= farthest_station - 1e-6 if farthest_station is not None else True
        confidence = 1.0 - min(1.0, fit_distance)
        if coverage_ok:
            confidence = min(1.0, confidence + 0.15)
        else:
            confidence = max(0.05, confidence - 0.35)
        if route.get("route_id") == suggested_route_id:
            confidence = min(1.0, confidence + 0.05)
        length_fit = fit_ratio
        if confidence >= 0.8 and coverage_ok and 0.75 <= fit_ratio <= 1.08:
            label = "Strong match"
        elif confidence >= 0.55 and coverage_ok and 0.55 <= fit_ratio <= 1.25:
            label = "Possible match"
        elif not coverage_ok:
            label = "Low confidence"
        else:
            label = "Review needed"
    return {
        "route_id": route.get("route_id"),
        "route_name": route.get("route_name"),
        "route_length_ft": round(route_length, 1),
        "dataset_span_ft": dataset_span,
        "length_delta_ft": round(length_delta, 1) if length_delta is not None else None,
        "length_fit": round(length_fit, 4) if length_fit is not None else None,
        "coverage_ok": coverage_ok,
        "confidence": round(confidence, 4),
        "confidence_label": label,
        "role": route.get("role") or "other",
    }


def upload_design_file(uploaded_file):
    files = {"file": (uploaded_file.name, uploaded_file.getvalue(), "application/octet-stream")}
    return get_backend_json(requests.post, f"{BACKEND}/api/upload-design", files=files, timeout=180)


def process_field_package(uploaded_files, reference_pdfs=None):
    files = []
    for uploaded in uploaded_files:
        files.append(("files", (uploaded.name, uploaded.getvalue(), "application/octet-stream")))
    pdf_parts = []
    for uploaded in reference_pdfs or []:
        pdf_parts.append(("pdfs", (uploaded.name, uploaded.getvalue(), "application/pdf")))
    return get_backend_json(requests.post, f"{BACKEND}/api/process-field-package", files=files + pdf_parts, timeout=360)


def commit_rows(rows, route_id: Optional[str]):
    payload = {"rows_json": json.dumps(rows)}
    if route_id:
        payload["route_id"] = route_id
    return get_backend_json(
        requests.post,
        f"{BACKEND}/api/commit-handwritten-bore-log",
        data=payload,
        timeout=180,
    )


def select_active_route(route_id: str):
    return get_backend_json(
        requests.post,
        f"{BACKEND}/api/select-active-route",
        data={"route_id": route_id},
        timeout=120,
    )


def extract_handwritten_files(uploaded_files):
    files = []
    for uploaded in uploaded_files:
        files.append(("files", (uploaded.name, uploaded.getvalue(), "application/octet-stream")))
    return get_backend_json(
        requests.post,
        f"{BACKEND}/api/extract-handwritten-bore-logs",
        files=files,
        timeout=300,
    )


def extracted_rows_to_station_df(rows):
    normalized = []
    for row in rows or []:
        station = normalize_station(row.get("station") or row.get("start_station"))
        depth_ft = safe_float(row.get("depth_ft"))
        boc_ft = safe_float(row.get("boc_ft"))
        notes = str(row.get("notes", "") or "").strip()
        source_file = str(row.get("source_file", "") or "").strip()
        confidence = row.get("confidence", "")
        if not station:
            continue
        normalized.append({
            "station": station,
            "depth_ft": depth_ft,
            "boc_ft": boc_ft,
            "notes": notes,
            "source_file": source_file,
            "confidence": confidence,
        })

    df = pd.DataFrame(normalized)
    if df.empty:
        return pd.DataFrame(columns=["station", "depth_ft", "boc_ft", "notes", "source_file", "confidence"])
    df["station_ft"] = df["station"].apply(station_to_feet)
    df = df.sort_values(by=["source_file", "station_ft", "station"], kind="stable").reset_index(drop=True)
    return df[["station", "depth_ft", "boc_ft", "notes", "source_file", "confidence"]]


def get_current_state():
    try:
        return get_backend_json(requests.get, f"{BACKEND}/api/current-state", timeout=60)
    except Exception:
        return None


def normalize_header(value: Any) -> str:
    import re
    return re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip("_")


def parse_bore_log_table(uploaded_file) -> pd.DataFrame:
    raw = uploaded_file.getvalue()
    name = uploaded_file.name.lower()
    if name.endswith(".csv"):
        df = pd.read_csv(BytesIO(raw))
    else:
        excel = pd.ExcelFile(BytesIO(raw))
        chosen_df = None
        best_score = -1
        for sheet in excel.sheet_names:
            candidate = pd.read_excel(BytesIO(raw), sheet_name=sheet)
            score = sum(
                1
                for col in candidate.columns
                if normalize_header(col) in {"station", "depth_ft", "depth", "boc_ft", "boc", "notes", "note", "comments", "date", "crew", "print", "page", "print_page", "sheet", "sheet_page"}
            )
            if score > best_score:
                best_score = score
                chosen_df = candidate
        if chosen_df is None:
            chosen_df = pd.read_excel(BytesIO(raw), sheet_name=0)
        df = chosen_df.copy()

    df = df.dropna(axis=0, how="all").dropna(axis=1, how="all")
    normalized_columns = {normalize_header(col): col for col in df.columns}

    def pick(*aliases):
        for alias in aliases:
            if alias in normalized_columns:
                return normalized_columns[alias]
        return None

    station_col = pick("station", "sta", "stationing", "station_no", "station_number")
    depth_col = pick("depth_ft", "depth", "depth_feet")
    boc_col = pick("boc_ft", "boc", "bore_of_cover", "cover_ft")
    notes_col = pick("notes", "note", "comments", "comment", "description", "reason")
    date_col = pick("date", "drill_date", "work_date")
    crew_col = pick("crew", "drill_crew", "operator", "foreman")
    print_col = pick("print", "page", "print_page", "sheet", "sheet_page")
    date_col = pick("date", "work_date", "drill_date")
    crew_col = pick("crew", "drill_crew")
    print_col = pick("print", "sheet", "page", "print_page")

    if not station_col:
        raise ValueError("Excel bore log must include a station column like station or STA.")

    rows: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        station = normalize_station(row.get(station_col))
        depth_ft = safe_float(row.get(depth_col)) if depth_col else None
        boc_ft = safe_float(row.get(boc_col)) if boc_col else None
        notes = str(row.get(notes_col, "") or "").strip() if notes_col else ""
        date = str(row.get(date_col, "") or "").strip() if date_col else ""
        crew = str(row.get(crew_col, "") or "").strip() if crew_col else ""
        print_page = str(row.get(print_col, "") or "").strip() if print_col else ""
        if not station and depth_ft is None and boc_ft is None and not notes and not date and not crew and not print_page:
            continue
        if not station:
            continue
        rows.append({
            "station": station,
            "station_ft": station_to_feet(station),
            "depth_ft": depth_ft,
            "boc_ft": boc_ft,
            "notes": notes,
            "date": str(row.get(date_col, "") or "").strip() if date_col else "",
            "crew": str(row.get(crew_col, "") or "").strip() if crew_col else "",
            "print": str(row.get(print_col, "") or "").strip() if print_col else "",
        })

    preview_df = pd.DataFrame(rows)
    if preview_df.empty:
        return pd.DataFrame(columns=["station", "depth_ft", "boc_ft", "notes", "date", "crew", "print"])
    return preview_df.sort_values(by=["station_ft", "station"], kind="stable").reset_index(drop=True)


def _pair_notes(*parts: Any) -> str:
    seen = []
    for part in parts:
        text = str(part or "").strip()
        if text and text not in seen:
            seen.append(text)
    return " | ".join(seen)


def _segment_reason_labels(reasons: List[str]) -> str:
    reasons_text = " ".join(reasons or []).lower()
    has_depth = "depth shift" in reasons_text
    has_boc = "boc shift" in reasons_text
    has_notes = "notes indicate" in reasons_text

    labels: List[str] = []
    if has_depth and has_boc:
        labels.append("Depth + BOC shift")
    elif has_depth:
        labels.append("Depth deviation")
    elif has_boc:
        labels.append("BOC shift")
    if has_notes:
        labels.append("Field notes")
    if not labels and reasons:
        labels.extend(reasons)
    return "; ".join(labels)


def build_span_rows(
    station_rows_df: pd.DataFrame,
    depth_threshold: float = 0.5,
    boc_threshold: float = 1.0,
    merge_gap_ft: float = 50.0,
    min_segment_length_ft: float = 50.0,
    min_sustained_points: int = 2,
):
    rows = []
    invalid = []
    if station_rows_df.empty:
        return rows, invalid

    cleaned = []
    for idx, row in station_rows_df.iterrows():
        station = normalize_station(row.get("station"))
        depth_ft = safe_float(row.get("depth_ft"))
        boc_ft = safe_float(row.get("boc_ft"))
        station_ft = station_to_feet(station)
        notes = str(row.get("notes", "") or "").strip()

        if station == "" and depth_ft is None and boc_ft is None and not notes:
            continue
        if station_ft is None:
            invalid.append({"row_index": int(idx) + 1, "reason": "Invalid station format", "station": str(row.get("station", ""))})
            continue
        cleaned.append(
            {
                "row_index": int(idx) + 1,
                "station": station,
                "station_ft": station_ft,
                "depth_ft": depth_ft,
                "boc_ft": boc_ft,
                "notes": notes,
                "selected": True,
            }
        )

    cleaned.sort(key=lambda r: r["station_ft"])
    note_keywords = {"rock", "hard rock", "obstruction", "utility", "conflict", "adjust", "offset", "deviation", "avoid", "reroute", "crossing", "issue"}

    if len(cleaned) < 2:
        invalid.append({"row_index": None, "reason": "Need at least two valid bore-log rows to generate redlines.", "station": cleaned[0]["station"] if cleaned else ""})
        return rows, invalid

    def _stable_deviation_flags(index: int):
        row = cleaned[index]
        prev_row = cleaned[index - 1] if index > 0 else None
        next_row = cleaned[index + 1] if index + 1 < len(cleaned) else None

        reasons = []
        numeric_trigger = False

        if row["depth_ft"] is not None:
            prev_depth_diff = abs(float(row["depth_ft"]) - float(prev_row["depth_ft"])) if prev_row and prev_row["depth_ft"] is not None else None
            next_depth_diff = abs(float(next_row["depth_ft"]) - float(row["depth_ft"])) if next_row and next_row["depth_ft"] is not None else None
            next_baseline_depth_diff = abs(float(next_row["depth_ft"]) - float(prev_row["depth_ft"])) if prev_row and next_row and prev_row["depth_ft"] is not None and next_row["depth_ft"] is not None else None
            if prev_depth_diff is not None and prev_depth_diff >= float(depth_threshold):
                if not (next_depth_diff is not None and next_baseline_depth_diff is not None and next_depth_diff >= float(depth_threshold) and next_baseline_depth_diff < float(depth_threshold)):
                    numeric_trigger = True
                    reasons.append(f"Depth shift {prev_depth_diff:.2f} ft")

        if row["boc_ft"] is not None:
            prev_boc_diff = abs(float(row["boc_ft"]) - float(prev_row["boc_ft"])) if prev_row and prev_row["boc_ft"] is not None else None
            next_boc_diff = abs(float(next_row["boc_ft"]) - float(row["boc_ft"])) if next_row and next_row["boc_ft"] is not None else None
            next_baseline_boc_diff = abs(float(next_row["boc_ft"]) - float(prev_row["boc_ft"])) if prev_row and next_row and prev_row["boc_ft"] is not None and next_row["boc_ft"] is not None else None
            if prev_boc_diff is not None and prev_boc_diff >= float(boc_threshold):
                if not (next_boc_diff is not None and next_baseline_boc_diff is not None and next_boc_diff >= float(boc_threshold) and next_baseline_boc_diff < float(boc_threshold)):
                    numeric_trigger = True
                    reasons.append(f"BOC shift {prev_boc_diff:.2f} ft")

        notes_text = str(row.get("notes", "") or "").strip()
        note_trigger = any(keyword in notes_text.lower() for keyword in note_keywords)
        if note_trigger:
            reasons.append("Notes indicate field deviation")

        return numeric_trigger, note_trigger, list(dict.fromkeys(reasons))

    anomaly_points = []
    for i in range(1, len(cleaned)):
        current_row = cleaned[i]
        prev_row = cleaned[i - 1]
        if current_row["station_ft"] <= prev_row["station_ft"]:
            invalid.append({"row_index": current_row["row_index"], "reason": "Non-sequential station rows", "station": f"{prev_row['station']} -> {current_row['station']}"})
            continue
        numeric_trigger, note_trigger, reasons = _stable_deviation_flags(i)
        anomaly_points.append(
            {
                "index": i,
                "row_index": current_row["row_index"],
                "station": current_row["station"],
                "station_ft": current_row["station_ft"],
                "depth_ft": current_row["depth_ft"],
                "boc_ft": current_row["boc_ft"],
                "notes": current_row["notes"],
                "numeric_trigger": numeric_trigger,
                "note_trigger": note_trigger,
                "is_anomaly": bool(numeric_trigger or note_trigger),
                "reasons": reasons,
            }
        )

    runs = []
    current_run = None
    for point in anomaly_points:
        if point["is_anomaly"]:
            if current_run is None:
                previous_row = cleaned[max(0, point["index"] - 1)]
                current_run = {
                    "start_station": previous_row["station"],
                    "start_ft": previous_row["station_ft"],
                    "end_station": point["station"],
                    "end_ft": point["station_ft"],
                    "depth_ft": point["depth_ft"],
                    "boc_ft": point["boc_ft"],
                    "notes": point.get("notes", ""),
                    "reasons": list(point.get("reasons") or []),
                    "source_file": "excel_bore_log",
                    "confidence": 1.0,
                    "selected": True,
                    "numeric_points": 1 if point["numeric_trigger"] else 0,
                    "note_points": 1 if point["note_trigger"] else 0,
                    "last_index": point["index"],
                }
            else:
                current_run["end_station"] = point["station"]
                current_run["end_ft"] = point["station_ft"]
                current_run["notes"] = _pair_notes(current_run.get("notes"), point.get("notes"))
                current_run["reasons"] = list(dict.fromkeys((current_run.get("reasons") or []) + (point.get("reasons") or [])))
                current_run["numeric_points"] += 1 if point["numeric_trigger"] else 0
                current_run["note_points"] += 1 if point["note_trigger"] else 0
                current_run["last_index"] = point["index"]
        elif current_run is not None:
            recovery_row = cleaned[point["index"]]
            current_run["end_station"] = recovery_row["station"]
            current_run["end_ft"] = recovery_row["station_ft"]
            runs.append(current_run)
            current_run = None

    if current_run is not None:
        runs.append(current_run)

    filtered_runs = []
    for run in runs:
        if run.get("note_points", 0) > 0:
            filtered_runs.append(run)
            continue
        if run.get("numeric_points", 0) >= int(min_sustained_points):
            filtered_runs.append(run)

    merged_runs = []
    for run in filtered_runs:
        if not merged_runs:
            merged_runs.append(run)
            continue
        previous = merged_runs[-1]
        gap_ft = float(run["start_ft"]) - float(previous["end_ft"])
        if gap_ft <= float(merge_gap_ft):
            previous["end_station"] = run["end_station"]
            previous["end_ft"] = run["end_ft"]
            previous["notes"] = _pair_notes(previous.get("notes"), run.get("notes"))
            previous["reasons"] = list(dict.fromkeys((previous.get("reasons") or []) + (run.get("reasons") or [])))
            previous["numeric_points"] = int(previous.get("numeric_points", 0)) + int(run.get("numeric_points", 0))
            previous["note_points"] = int(previous.get("note_points", 0)) + int(run.get("note_points", 0))
            continue
        merged_runs.append(run)

    for run in merged_runs:
        segment_length_ft = float(run["end_ft"]) - float(run["start_ft"])
        if segment_length_ft < float(min_segment_length_ft):
            continue
        rows.append(
            {
                "start_station": run["start_station"],
                "end_station": run["end_station"],
                "station": run["start_station"],
                "depth_ft": run["depth_ft"],
                "boc_ft": run["boc_ft"],
                "notes": run["notes"],
                "reason": _segment_reason_labels(run.get("reasons") or []),
                "source_file": run["source_file"],
                "confidence": run["confidence"],
                "selected": True,
            }
        )

    if not rows and cleaned:
        invalid.append(
            {
                "row_index": None,
                "reason": f"No sustained deviation windows met the current thresholds. Try lowering thresholds or reducing required sustained anomalous points below {int(min_sustained_points)}.",
                "station": f"{cleaned[0]['station']} -> {cleaned[-1]['station']}",
            }
        )
    return rows, invalid


def make_div_icon(text, text_color="#111111", bg="#fff8dc", border="#444444", size=16):
    html = (
        f'<div style="font-size:{size}px;font-weight:800;color:{text_color};'
        f'background:transparent;border:none;padding:0;white-space:nowrap;'
        f'text-shadow:0 0 2px rgba(255,255,255,.95), 0 0 4px rgba(255,255,255,.95);">{text}</div>'
    )
    return folium.DivIcon(html=html)


def build_station_info_card(station: str, depth_ft: Any, boc_ft: Any, date: Any, crew: Any, print_page: Any, notes: Any) -> str:
    def _display(value: Any, suffix: str = "") -> str:
        text = str(value or "").strip()
        if text == "":
            return "—"
        if text.endswith(" 00:00:00"):
            text = text[:-9]
        elif text.endswith("T00:00:00"):
            text = text[:-9]
        return f"{text}{suffix}"

    row_style = "padding:14px 18px;border-bottom:1px solid #e5e7eb;color:#111827;font-size:22px;line-height:1.3;vertical-align:top;"
    label_style = row_style + "font-weight:800;color:#374151;width:40%;"
    return f"""
    <div style="min-width:460px;max-width:560px;background:#ffffff;border:1px solid #d1d5db;border-radius:14px;box-shadow:0 10px 24px rgba(15,23,42,.18);overflow:hidden;font-family:Arial,sans-serif;">
      <div style="padding:18px 20px;background:#f9fafb;border-bottom:1px solid #e5e7eb;font-weight:800;font-size:22px;line-height:1.2;color:#111827;">Station {station or '—'}</div>
      <table style="width:100%;border-collapse:collapse;font-size:22px;line-height:1.3;background:#ffffff;">
        <tr><td style="{label_style}">Date</td><td style="{row_style}">{_display(date)}</td></tr>
        <tr><td style="{label_style}">Crew</td><td style="{row_style}">{_display(crew)}</td></tr>
        <tr><td style="{label_style}">Print/Page</td><td style="{row_style}">{_display(print_page)}</td></tr>
        <tr><td style="{label_style}">Depth</td><td style="{row_style}">{_display(depth_ft, ' ft')}</td></tr>
        <tr><td style="{label_style}">BOC</td><td style="{row_style}">{_display(boc_ft, ' ft')}</td></tr>
        <tr><td style="padding:14px 18px;color:#374151;font-size:22px;line-height:1.3;vertical-align:top;font-weight:800;width:40%;">Notes</td><td style="padding:14px 18px;color:#111827;font-size:22px;line-height:1.3;vertical-align:top;">{_display(notes)}</td></tr>
      </table>
    </div>
    """


def xml_escape(text):
    value = str(text or "")
    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;").replace("'", "&apos;")


def coords_to_kml_string(coords):
    return " ".join(f"{float(lon):.8f},{float(lat):.8f},0" for lat, lon in coords)


def point_to_kml_string(lat, lon):
    return f"{float(lon):.8f},{float(lat):.8f},0"


def role_color(role: str, fallback: str = "") -> str:
    palette = {
        "backbone": "#1565C0",
        "underground_cable": "#1565C0",
        "terminal_tail": "#FB8C00",
        "vacant_pipe": "#8E24AA",
        "house_drop": "#00ACC1",
        "marker": "#455A64",
        "redline_reference": "#C62828",
        "other": "#1E88E5",
    }
    return fallback or palette.get(role or "other", "#1E88E5")


def role_weight(role: str, width: float) -> float:
    if width and width > 0:
        return max(2.0, min(7.0, float(width) * 1.6))
    defaults = {
        "backbone": 5.0,
        "underground_cable": 4.0,
        "terminal_tail": 3.0,
        "vacant_pipe": 3.0,
        "house_drop": 2.0,
        "redline_reference": 4.0,
        "other": 3.0,
    }
    return defaults.get(role or "other", 3.0)


def build_production_report_rows(
    station_rows_df: pd.DataFrame,
    route_catalog: List[Dict[str, Any]],
    per_file_route_assignments: List[Dict[str, Any]],
    fallback_route_id: Optional[str],
    fallback_route_name: Optional[str],
    fallback_route_coords: List[List[float]],
) -> List[Dict[str, Any]]:
    runs = build_source_file_route_runs(
        station_rows_df,
        route_catalog,
        per_file_route_assignments,
        fallback_route_id,
        fallback_route_name,
        fallback_route_coords,
    )

    rows: List[Dict[str, Any]] = []
    for run in runs:
        local_points = run.get("local_station_points") or []
        if len(local_points) < 2:
            continue
        if not _valid_completed_trace_span(local_points):
            continue

        start_point = local_points[0]
        end_point = local_points[-1]
        start_ft = start_point.get("mapped_route_ft", start_point.get("station_ft"))
        end_ft = end_point.get("mapped_route_ft", end_point.get("station_ft"))
        try:
            length_ft = abs(float(end_ft or 0.0) - float(start_ft or 0.0))
        except Exception:
            length_ft = 0.0

        notes_parts: List[str] = []
        for point in local_points:
            note_text = str(point.get("notes", "") or "").strip()
            if note_text and note_text not in notes_parts:
                notes_parts.append(note_text)

        rows.append({
            "selected": True,
            "source_file": str(run.get("source_file") or "field_file"),
            "route_name": str(run.get("route_name") or run.get("route_id") or ""),
            "start_station": start_point.get("station", ""),
            "end_station": end_point.get("station", ""),
            "length_ft": round(float(length_ft), 2),
            "depth_ft": start_point.get("depth_ft", ""),
            "boc_ft": start_point.get("boc_ft", ""),
            "date": start_point.get("date", ""),
            "crew": start_point.get("crew", ""),
            "print": start_point.get("print", ""),
            "notes": " | ".join(notes_parts) if notes_parts else start_point.get("notes", ""),
        })
    return rows




def build_crew_summary_rows(production_rows: List[Dict[str, Any]], material_rate: float) -> List[Dict[str, Any]]:
    summary: Dict[str, Dict[str, Any]] = {}
    for row in production_rows or []:
        crew = str(row.get("crew") or "").strip() or "Unknown"
        source_file = str(row.get("source_file") or "").strip() or "field_file"
        route_name = str(row.get("route_name") or "").strip() or "Unassigned"
        length_ft = float(row.get("length_ft") or 0.0)

        entry = summary.setdefault(
            crew,
            {
                "crew": crew,
                "bore_logs_set": set(),
                "routes_set": set(),
                "segments": 0,
                "total_length_ft": 0.0,
                "total_cost": 0.0,
            },
        )
        entry["bore_logs_set"].add(source_file)
        entry["routes_set"].add(route_name)
        entry["segments"] += 1
        entry["total_length_ft"] += length_ft
        entry["total_cost"] += length_ft * float(material_rate or 0.0)

    rows: List[Dict[str, Any]] = []
    for crew_name, entry in summary.items():
        rows.append(
            {
                "crew": crew_name,
                "bore_logs": len(entry["bore_logs_set"]),
                "routes": len(entry["routes_set"]),
                "segments": int(entry["segments"]),
                "total_length_ft": round(float(entry["total_length_ft"]), 2),
                "total_cost": round(float(entry["total_cost"]), 2),
            }
        )
    rows.sort(key=lambda item: (item["crew"] != "Unknown", item["crew"]))
    return rows


def compute_feature_bounds(kmz_reference, route_coords, generated_segments, map_points):
    latitudes: List[float] = []
    longitudes: List[float] = []

    def add_point(lat, lon):
        if lat is None or lon is None:
            return
        latitudes.append(float(lat))
        longitudes.append(float(lon))

    for feature in (kmz_reference or {}).get("line_features", []) or []:
        for lat, lon in feature.get("coords", []) or []:
            add_point(lat, lon)

    for feature in (kmz_reference or {}).get("point_features", []) or []:
        add_point(feature.get("lat"), feature.get("lon"))

    for feature in (kmz_reference or {}).get("polygon_features", []) or []:
        for lat, lon in feature.get("coords", []) or []:
            add_point(lat, lon)

    for lat, lon in route_coords or []:
        add_point(lat, lon)

    for segment in generated_segments or []:
        for lat, lon in segment.get("coords", []) or []:
            add_point(lat, lon)

    for point in map_points or []:
        add_point(point.get("lat"), point.get("lon"))

    if not latitudes or not longitudes:
        return None
    return [[min(latitudes), min(longitudes)], [max(latitudes), max(longitudes)]]



def _kmz_outline_polygon(design_lines: List[Dict[str, Any]]) -> List[List[float]]:
    points: List[Tuple[float, float]] = []
    seen = set()
    for feature in design_lines or []:
        for coord in feature.get("coords", []) or []:
            if not coord or len(coord) < 2:
                continue
            lat = float(coord[0])
            lon = float(coord[1])
            key = (round(lon, 12), round(lat, 12))
            if key in seen:
                continue
            seen.add(key)
            points.append((lon, lat))

    if len(points) < 3:
        return []

    def cross(o: Tuple[float, float], a: Tuple[float, float], b: Tuple[float, float]) -> float:
        return (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])

    pts = sorted(points)
    if len(pts) < 3:
        return []

    lower: List[Tuple[float, float]] = []
    for p in pts:
        while len(lower) >= 2 and cross(lower[-2], lower[-1], p) <= 0:
            lower.pop()
        lower.append(p)

    upper: List[Tuple[float, float]] = []
    for p in reversed(pts):
        while len(upper) >= 2 and cross(upper[-2], upper[-1], p) <= 0:
            upper.pop()
        upper.append(p)

    hull = lower[:-1] + upper[:-1]
    if len(hull) < 3:
        return []

    polygon = [[lat, lon] for lon, lat in hull]
    polygon.append(polygon[0])
    return polygon


def build_export_kml(
    route_name,
    route_coords,
    generated_segments,
    station_rows,
    map_points,
    kmz_reference,
    source_file_route_runs=None,
    local_station_points=None,
):
    station_lookup = {}
    for _, row in station_rows.iterrows():
        normalized = normalize_station(row.get("station"))
        if not normalized:
            continue
        source_file_value = str(row.get("source_file", "") or "").strip() or "field_file"
        details = {
            "depth_ft": row.get("depth_ft", ""),
            "boc_ft": row.get("boc_ft", ""),
            "notes": row.get("notes", ""),
            "date": row.get("date", ""),
            "crew": row.get("crew", ""),
            "print": row.get("print", ""),
            "source_file": source_file_value,
        }
        station_lookup[(source_file_value, normalized)] = details
        station_lookup.setdefault(normalized, details)

    export_station_points = local_station_points or map_points or []
    export_route_runs = source_file_route_runs or []

    pieces = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<kml xmlns="http://www.opengis.net/kml/2.2">',
        "<Document>",
        f"<name>{xml_escape(route_name or 'OSP Redlining Export')}</name>",
        "<open>1</open>",
        '<Style id="designLineStyle"><LineStyle><color>ff327d2e</color><width>4</width></LineStyle></Style>',
        '<Style id="mainRouteStyle"><LineStyle><color>ff327d2e</color><width>4</width></LineStyle></Style>',
        '<Style id="asBuiltMaskStyle"><LineStyle><color>ffffffff</color><width>16</width></LineStyle></Style>',
        '<Style id="asBuiltStyle"><LineStyle><color>ff1f1fff</color><width>10</width></LineStyle><BalloonStyle><text><![CDATA[$[description]]]></text></BalloonStyle></Style>',
        '<Style id="generatedRedlineStyle"><LineStyle><color>ff5714ad</color><width>4</width></LineStyle><BalloonStyle><text><![CDATA[$[description]]]></text></BalloonStyle></Style>',
        '<Style id="stationStyle"><IconStyle><scale>0.9</scale><Icon><href>http://maps.google.com/mapfiles/kml/paddle/wht-blank.png</href></Icon></IconStyle><LabelStyle><scale>0.0</scale></LabelStyle><BalloonStyle><text><![CDATA[$[description]]]></text></BalloonStyle></Style>',
        '<Style id="labelStyle"><IconStyle><scale>0</scale></IconStyle><LabelStyle><scale>1.0</scale></LabelStyle><BalloonStyle><text><![CDATA[$[description]]]></text></BalloonStyle></Style>',
        '<Style id="kmzShadowPolyStyle"><LineStyle><color>fffad481</color><width>3</width></LineStyle><PolyStyle><color>2381d4fa</color><fill>1</fill><outline>1</outline></PolyStyle></Style>',
        '<Style id="kmzShadowGlowLineStyle"><LineStyle><color>6681d4fa</color><width>12</width></LineStyle></Style>',
        '<Style id="kmzShadowLineStyle"><LineStyle><color>ff81d4fa</color><width>4</width></LineStyle></Style>',
        '<Style id="kmzNodeStyle"><IconStyle><scale>0.5</scale><color>ffffffff</color><Icon><href>http://maps.google.com/mapfiles/kml/shapes/placemark_circle.png</href></Icon></IconStyle><LabelStyle><scale>0</scale></LabelStyle></Style>',
        '<Style id="houseDropStyle"><LineStyle><color>ff00acc1</color><width>2</width></LineStyle></Style>',
    ]

    pieces += ["<Folder><name>KMZ Design</name>", "<open>1</open>"]

    polygon_features = (kmz_reference or {}).get("polygon_features", []) or []
    if polygon_features:
        pieces += ["<Folder><name>KMZ Shadow</name><open>0</open>"]
        for idx, feature in enumerate(polygon_features, start=1):
            coords = feature.get("coords", []) or []
            if len(coords) < 3:
                continue
            name = feature.get("name") or f"KMZ Shadow {idx}"
            if coords[0] != coords[-1]:
                coords = list(coords) + [coords[0]]
            pieces += [
                "<Placemark>",
                f"<name>{xml_escape(name)}</name>",
                "<styleUrl>#kmzShadowPolyStyle</styleUrl>",
                "<Polygon><outerBoundaryIs><LinearRing><coordinates>",
                coords_to_kml_string(coords),
                "</coordinates></LinearRing></outerBoundaryIs></Polygon>",
                "</Placemark>",
                "<Placemark>",
                f"<name>{xml_escape(name)} Glow</name>",
                "<styleUrl>#kmzShadowGlowLineStyle</styleUrl>",
                "<LineString><tessellate>1</tessellate><coordinates>",
                coords_to_kml_string(coords),
                "</coordinates></LineString>",
                "</Placemark>",
                "<Placemark>",
                f"<name>{xml_escape(name)} Border</name>",
                "<styleUrl>#kmzShadowLineStyle</styleUrl>",
                "<LineString><tessellate>1</tessellate><coordinates>",
                coords_to_kml_string(coords),
                "</coordinates></LineString>",
                "</Placemark>",
            ]
        pieces += ["</Folder>"]

    design_lines = (kmz_reference or {}).get("line_features", []) or []
    if design_lines:
        pieces += ["<Folder><name>Design Lines</name><open>1</open>"]
        for idx, feature in enumerate(design_lines, start=1):
            coords = feature.get("coords", []) or []
            if len(coords) < 2 or feature.get("role") == "house_drop":
                continue
            name = feature.get("name") or feature.get("folder_path") or f"Design Line {idx}"
            pieces += [
                "<Placemark>",
                f"<name>{xml_escape(name)}</name>",
                "<styleUrl>#designLineStyle</styleUrl>",
                "<LineString><tessellate>1</tessellate><coordinates>",
                coords_to_kml_string(coords),
                "</coordinates></LineString>",
                "</Placemark>",
            ]
        pieces += ["</Folder>"]

        house_drops = [f for f in design_lines if f.get("role") == "house_drop"]
        if house_drops:
            pieces += ["<Folder><name>House Drops</name><open>0</open>"]
            for idx, feature in enumerate(house_drops, start=1):
                coords = feature.get("coords", []) or []
                if len(coords) < 2:
                    continue
                name = feature.get("name") or feature.get("folder_path") or f"House Drop {idx}"
                pieces += [
                    "<Placemark>",
                    f"<name>{xml_escape(name)}</name>",
                    "<styleUrl>#houseDropStyle</styleUrl>",
                    "<LineString><tessellate>1</tessellate><coordinates>",
                    coords_to_kml_string(coords),
                    "</coordinates></LineString>",
                    "</Placemark>",
                ]
            pieces += ["</Folder>"]

    design_points = (kmz_reference or {}).get("point_features", []) or []
    if design_points:
        pieces += ["<Folder><name>KMZ Nodes</name><open>0</open>"]
        for idx, feature in enumerate(design_points, start=1):
            if feature.get("role") == "house_drop":
                continue
            lat = feature.get("lat")
            lon = feature.get("lon")
            if lat is None or lon is None:
                continue
            name = feature.get("name") or feature.get("folder_path") or f"KMZ Node {idx}"
            pieces += [
                "<Placemark>",
                f"<name>{xml_escape(name)}</name>",
                "<styleUrl>#kmzNodeStyle</styleUrl>",
                "<Point><coordinates>",
                point_to_kml_string(lat, lon),
                "</coordinates></Point>",
                "</Placemark>",
            ]
        pieces += ["</Folder>"]

    pieces += ["</Folder>"]

    if route_coords:
        pieces += [
            "<Folder><name>Main Route</name><open>0</open>",
            "<Placemark>",
            "<name>Main Route</name>",
            "<styleUrl>#mainRouteStyle</styleUrl>",
            "<LineString><tessellate>1</tessellate><coordinates>",
            coords_to_kml_string(route_coords),
            "</coordinates></LineString>",
            "</Placemark>",
            "</Folder>",
        ]

    completed_runs = [run for run in export_route_runs if len(run.get("completed_trace_coords") or []) >= 2]
    if completed_runs:
        pieces += ["<Folder><name>As-Built Redline</name><open>1</open>"]
        for run in completed_runs:
            coords = run.get("completed_trace_coords") or []
            source_file = str(run.get("source_file") or "field_file")
            route_name_value = str(run.get("route_name") or run.get("route_id") or "")
            run_points = run.get("local_station_points") or []
            begin_station = run_points[0].get("station", "") if run_points else ""
            end_station = run_points[-1].get("station", "") if run_points else ""
            run_dates = [str(point.get("date", "") or "").strip() for point in run_points if str(point.get("date", "") or "").strip()]
            run_crews = [str(point.get("crew", "") or "").strip() for point in run_points if str(point.get("crew", "") or "").strip()]
            run_prints = [str(point.get("print", "") or "").strip() for point in run_points if str(point.get("print", "") or "").strip()]
            run_notes = []
            for point in run_points:
                note_text = str(point.get("notes", "") or "").strip()
                if note_text and note_text not in run_notes:
                    run_notes.append(note_text)
            run_length_ft = 0.0
            try:
                if run_points:
                    run_length_ft = abs(
                        float(run_points[-1].get("mapped_route_ft", run_points[-1].get("station_ft", 0.0)) or 0.0)
                        - float(run_points[0].get("mapped_route_ft", run_points[0].get("station_ft", 0.0)) or 0.0)
                    )
            except Exception:
                run_length_ft = 0.0

            pieces += [
                "<Placemark>",
                f"<name>{xml_escape(source_file)} Mask</name>",
                "<styleUrl>#asBuiltMaskStyle</styleUrl>",
                "<LineString><tessellate>1</tessellate><coordinates>",
                coords_to_kml_string(coords),
                "</coordinates></LineString>",
                "</Placemark>",
                "<Placemark>",
                f"<name>{xml_escape(source_file)}</name>",
                "<styleUrl>#asBuiltStyle</styleUrl>",
                "<description><![CDATA[",
                "<div style='font-family:Arial,sans-serif;min-width:320px;'>",
                "<div style='font-size:18px;font-weight:700;margin-bottom:10px;'>As-Built Completed Drill Trace</div>",
                f"<div><b>Source File:</b> {xml_escape(source_file)}</div>",
                f"<div><b>Route:</b> {xml_escape(route_name_value)}</div>",
                f"<div><b>Begin:</b> {xml_escape(begin_station)}</div>",
                f"<div><b>End:</b> {xml_escape(end_station)}</div>",
                f"<div><b>Length:</b> {run_length_ft:,.1f} ft</div>",
                f"<div><b>Date(s):</b> {xml_escape(' | '.join(dict.fromkeys([str(d).replace(' 00:00:00', '').replace('T00:00:00', '') for d in run_dates])) if run_dates else '—')}</div>",
                f"<div><b>Crew(s):</b> {xml_escape(' | '.join(dict.fromkeys(run_crews)) if run_crews else '—')}</div>",
                f"<div><b>Print/Page:</b> {xml_escape(' | '.join(dict.fromkeys(run_prints)) if run_prints else '—')}</div>",
                f"<div style='margin-top:8px;'><b>Notes:</b> {xml_escape(' | '.join(run_notes) if run_notes else '—')}</div>",
                "</div>",
                "]]></description>",
                "<LineString><tessellate>1</tessellate><coordinates>",
                coords_to_kml_string(coords),
                "</coordinates></LineString>",
                "</Placemark>",
            ]
        pieces += ["</Folder>"]

    if generated_segments:
        pieces += ["<Folder><name>Generated Redlines</name><open>0</open>"]
        for seg in generated_segments or []:
            coords = seg.get("coords", []) or []
            if len(coords) < 2:
                continue
            pieces += [
                "<Placemark>",
                f"<name>{xml_escape(seg.get('start_station', ''))} → {xml_escape(seg.get('end_station', ''))}</name>",
                "<styleUrl>#generatedRedlineStyle</styleUrl>",
                "<description><![CDATA[",
                f"Length: {seg.get('length_ft', 0)} ft<br>",
                f"Depth: {seg.get('depth_ft', '')}<br>",
                f"BOC: {seg.get('boc_ft', '')}<br>",
                f"Reason: {xml_escape(seg.get('reason', ''))}<br>",
                f"Notes: {xml_escape(seg.get('notes', ''))}",
                "]]></description>",
                "<LineString><tessellate>1</tessellate><coordinates>",
                coords_to_kml_string(coords),
                "</coordinates></LineString>",
                "</Placemark>",
            ]
        pieces += ["</Folder>"]

    if export_station_points:
        pieces += ["<Folder><name>Bore Stations</name><open>0</open>"]
        for point in export_station_points:
            station = normalize_station(point.get("station"))
            if not station:
                continue
            source_file_value = str(point.get("source_file", "") or "").strip() or "field_file"
            details = station_lookup.get((source_file_value, station), {}) or station_lookup.get(station, {})
            pieces += [
                "<Placemark>",
                f"<name>{xml_escape(station)}</name>",
                "<styleUrl>#stationStyle</styleUrl>",
                "<description><![CDATA[",
                "<div style='font-family:Arial,sans-serif;min-width:300px;'>",
                f"<div style='font-size:18px;font-weight:700;margin-bottom:10px;'>Station {xml_escape(station)}</div>",
                f"<div><b>Date:</b> {xml_escape(details.get('date', point.get('date', '')))}</div>",
                f"<div><b>Crew:</b> {xml_escape(details.get('crew', point.get('crew', '')))}</div>",
                f"<div><b>Print/Page:</b> {xml_escape(details.get('print', point.get('print', '')))}</div>",
                f"<div><b>Depth:</b> {xml_escape(details.get('depth_ft', point.get('depth_ft', '')))}</div>",
                f"<div><b>BOC:</b> {xml_escape(details.get('boc_ft', point.get('boc_ft', '')))}</div>",
                f"<div style='margin-top:8px;'><b>Notes:</b> {xml_escape(details.get('notes', point.get('notes', '')))}</div>",
                "</div>",
                "]]></description>",
                "<Point><coordinates>",
                point_to_kml_string(point.get("lat"), point.get("lon")),
                "</coordinates></Point>",
                "</Placemark>",
            ]
        pieces += ["</Folder>"]

    if export_route_runs:
        pieces += ["<Folder><name>BEGIN / END Labels</name><open>0</open>"]
        for run in export_route_runs:
            run_points = run.get("local_station_points") or []
            if not run_points:
                continue
            boundary_points = [run_points[0]]
            if len(run_points) > 1:
                boundary_points.append(run_points[-1])
            for idx, point in enumerate(boundary_points):
                label_text = "BEGIN" if idx == 0 else "END"
                pieces += [
                    "<Placemark>",
                    f"<name>{label_text} - {xml_escape(point.get('station', ''))}</name>",
                    "<styleUrl>#labelStyle</styleUrl>",
                    "<description><![CDATA[",
                    "<div style='font-family:Arial,sans-serif;min-width:300px;'>",
                    f"<div style='font-size:18px;font-weight:700;margin-bottom:10px;'>{label_text} Label</div>",
                    f"<div><b>Station:</b> {xml_escape(point.get('station', ''))}</div>",
                    f"<div><b>Date:</b> {xml_escape(str(point.get('date', '') or '').replace(' 00:00:00', '').replace('T00:00:00', ''))}</div>",
                    f"<div><b>Crew:</b> {xml_escape(point.get('crew', ''))}</div>",
                    f"<div><b>Print/Page:</b> {xml_escape(point.get('print', ''))}</div>",
                    f"<div><b>Depth:</b> {xml_escape(point.get('depth_ft', ''))}</div>",
                    f"<div><b>BOC:</b> {xml_escape(point.get('boc_ft', ''))}</div>",
                    f"<div style='margin-top:8px;'><b>Notes:</b> {xml_escape(point.get('notes', ''))}</div>",
                    "</div>",
                    "]]></description>",
                    "<Point><coordinates>",
                    point_to_kml_string(point.get("lat"), point.get("lon")),
                    "</coordinates></Point>",
                    "</Placemark>",
                ]
        pieces += ["</Folder>"]

    pieces += ["</Document>", "</kml>"]
    return "".join(pieces)



def build_export_kmz_bytes(
    route_name,
    route_coords,
    generated_segments,
    station_rows,
    map_points,
    kmz_reference,
    source_file_route_runs=None,
    local_station_points=None,
):
    kml_text = build_export_kml(
        route_name,
        route_coords,
        generated_segments,
        station_rows,
        map_points,
        kmz_reference,
        source_file_route_runs=source_file_route_runs,
        local_station_points=local_station_points,
    )
    kml_bytes = kml_text.encode("utf-8")
    kmz_buffer = BytesIO()
    with zipfile.ZipFile(kmz_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("doc.kml", kml_bytes)
    kmz_buffer.seek(0)
    return kmz_buffer.getvalue(), kml_bytes


defaults = {
    "route_name": None,
    "route_catalog": [],
    "suggested_route_id": None,
    "selected_route_id": None,
    "selected_route_name": None,
    "selected_route_match": None,
    "route_match_candidates": [],
    "preview_route_id": None,
    "assignment_locked": False,
    "last_generation_route_id": None,
    "last_generation_count": 0,
    "generation_status": None,
    "route_coords": [],
    "generated_segments": [],
    "invalid_redlines": [],
    "map_points": [],
    "station_rows": pd.DataFrame(columns=["station", "depth_ft", "boc_ft", "date", "crew", "print", "notes"]),
    "ocr_station_rows": pd.DataFrame(columns=["station", "depth_ft", "boc_ft", "date", "crew", "print", "notes", "source_file", "confidence"]),
    "ocr_file_summaries": [],
    "per_file_route_assignments": [],
    "last_uploaded_name": None,
    "kmz_reference": {},
    "validation_docs": [],
    "sheet_match_suggestions": [],
    "last_corridor_debug": {},
    "route_filter_drill_paths_only": True,
    "layer_show_kmz_design": True,
    "layer_show_kmz_shadow": True,
    "layer_show_house_drops": False,
    "layer_show_kmz_nodes": False,
    "layer_show_main_route": False,
    "layer_show_redline_segments": False,
    "layer_show_station_labels": False,
    "layer_show_bore_stations": False,
}
for key, value in defaults.items():
    if key not in st.session_state:
        st.session_state[key] = value

left, right = st.columns([1.05, 1.95])

with left:
    st.subheader("1) Upload KMZ Design")
    design_file = st.file_uploader("KMZ / KML Design", type=["kmz", "kml"], key="design_uploader")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("Load KMZ Design", use_container_width=True, type="primary"):
            if design_file is None:
                st.warning("Choose the KMZ design first.")
            else:
                try:
                    data = upload_design_file(design_file)
                    if data.get("success"):
                        refreshed = get_current_state() or {}
                        st.session_state.route_name = refreshed.get("route_name") or data.get("route_name")
                        st.session_state.route_catalog = refreshed.get("route_catalog", []) or data.get("route_catalog", []) or []
                        st.session_state.suggested_route_id = refreshed.get("suggested_route_id", data.get("suggested_route_id"))
                        st.session_state.selected_route_id = refreshed.get("selected_route_id", data.get("selected_route_id"))
                        st.session_state.selected_route_name = refreshed.get("selected_route_name", data.get("selected_route_name"))
                        st.session_state.selected_route_match = refreshed.get("selected_route_match", data.get("selected_route_match"))
                        st.session_state.route_match_candidates = refreshed.get("route_match_candidates", []) or data.get("route_match_candidates", []) or []
                        st.session_state.route_coords = refreshed.get("route_coords", []) or data.get("route_coords", []) or []
                        uploaded_polygon_features = extract_polygon_features_from_design_upload(design_file.getvalue(), design_file.name)
                        base_kmz_reference = refreshed.get("kmz_reference", {}) or data.get("kmz_reference", {}) or {}
                        st.session_state.kmz_reference = merge_polygon_features_into_kmz_reference(base_kmz_reference, uploaded_polygon_features)
                        st.session_state.generated_segments = []
                        st.session_state.invalid_redlines = []
                        st.session_state.map_points = []
                        st.session_state.station_rows = pd.DataFrame(columns=["station", "depth_ft", "boc_ft", "notes", "source_file"])
                        st.session_state.per_file_route_assignments = []
                        st.session_state.ocr_station_rows = pd.DataFrame(columns=["start_station", "end_station", "depth_ft", "boc_ft", "notes", "source_file"])
                        st.session_state.validation_docs = []
                        st.session_state.sheet_match_suggestions = []
                        st.session_state.last_uploaded_name = design_file.name
                        st.success(f"Loaded design: {st.session_state.route_name or design_file.name}")
                    else:
                        st.error(data.get("error", "Design upload failed."))
                except Exception as exc:
                    st.error(f"Design upload failed: {exc}")
    with c2:
        if st.button("Refresh State", use_container_width=True):
            state = get_current_state()
            if state:
                st.session_state.route_name = state.get("route_name")
                st.session_state.route_catalog = state.get("route_catalog", []) or []
                st.session_state.suggested_route_id = state.get("suggested_route_id")
                st.session_state.selected_route_id = state.get("selected_route_id")
                st.session_state.selected_route_name = state.get("selected_route_name")
                st.session_state.selected_route_match = state.get("selected_route_match")
                st.session_state.route_match_candidates = state.get("route_match_candidates", []) or []
                st.session_state.route_coords = state.get("route_coords", []) or []
                st.session_state.generated_segments = state.get("redline_segments", []) or []
                st.session_state.invalid_redlines = state.get("validation_issues", []) or []
                st.session_state.map_points = state.get("map_points", []) or []
                refreshed_kmz_reference = state.get("kmz_reference", {}) or {}
                existing_polygon_features = (st.session_state.kmz_reference or {}).get("polygon_features", []) or []
                st.session_state.kmz_reference = merge_polygon_features_into_kmz_reference(refreshed_kmz_reference, existing_polygon_features)
                st.session_state.per_file_route_assignments = []
                st.session_state.validation_docs = state.get("validation_docs", []) or []
                st.session_state.sheet_match_suggestions = state.get("sheet_match_suggestions", []) or []
                st.success("Current state refreshed.")
            else:
                st.warning("Could not refresh current state.")

    kmz_line_count = len((st.session_state.kmz_reference or {}).get("line_features", []) or [])
    kmz_node_count = len((st.session_state.kmz_reference or {}).get("point_features", []) or [])
    kmz_polygon_count = len((st.session_state.kmz_reference or {}).get("polygon_features", []) or [])
    if st.session_state.route_catalog:
        st.info(f"KMZ candidates loaded: {len(st.session_state.route_catalog)} route paths · {kmz_line_count} design lines · {kmz_node_count} design points · {kmz_polygon_count} design polygons")
    else:
        st.caption("Load the current stable KMZ so the backend can build normalized route candidates.")

    st.divider()
    st.subheader("2) Bulk Upload Field Files")
    st.info("Select all bore logs at once, then process the batch into the current route-assignment workflow.")
    with st.container(border=True):
        field_files = st.file_uploader(
            "Bore log field files (multi-select enabled)",
            type=["xlsx", "xls", "csv", "pdf", "png", "jpg", "jpeg"],
            accept_multiple_files=True,
            key="field_files_uploader",
            help="Choose every bore log for this batch in one upload. Excel, CSV, PDF, PNG, JPG, and JPEG are supported.",
        )
        st.caption("Pick all bore logs in one upload, then process the batch.")
        reference_pdfs = st.file_uploader(
            "Reference PDFs (optional support only)",
            type=["pdf"],
            accept_multiple_files=True,
            key="reference_pdf_uploader",
            help="Optional supporting plan sheets only. These do not replace the main field-file workflow.",
        )
        st.caption("Each file stays grouped by source file for clean review after processing.")

    render_bulk_upload_summary(field_files, reference_pdfs, st.session_state.per_file_route_assignments or [])

    if st.button("Process Field Files + Auto-Match", use_container_width=True, type="primary"):
        if not st.session_state.route_catalog:
            st.warning("Upload the KMZ design first.")
        elif not field_files:
            st.warning("Upload at least one bore log or field file first.")
        else:
            try:
                data = process_field_package(field_files, reference_pdfs)
                if data.get("success"):
                    st.session_state.selected_route_id = data.get("selected_route_id")
                    st.session_state.selected_route_name = data.get("selected_route_name")
                    st.session_state.selected_route_match = data.get("selected_route_match")
                    st.session_state.route_match_candidates = data.get("route_match_candidates", []) or []
                    st.session_state.validation_docs = data.get("validation_docs", []) or []
                    st.session_state.sheet_match_suggestions = data.get("sheet_match_suggestions", []) or []
                    st.session_state.per_file_route_assignments = _normalize_per_file_assignments(data.get("per_file_route_assignments", []) or [], st.session_state.route_catalog or [])
                    st.session_state.last_corridor_debug = {
                        "matched_pdf_sheet": data.get("matched_pdf_sheet"),
                        "matched_chain_id": data.get("matched_chain_id"),
                        "matched_corridor_ids": data.get("matched_corridor_ids", []) or [],
                        "matched_station_range": data.get("matched_station_range"),
                        "matched_street_names": data.get("matched_street_names", []) or [],
                        "station_reset_detected": data.get("station_reset_detected", False),
                        "kmz_candidates_before_filter": data.get("kmz_candidates_before_filter"),
                        "kmz_candidates_after_filter": data.get("kmz_candidates_after_filter"),
                    }

                    extracted_rows = data.get("rows", []) or []
                    if extracted_rows:
                        preview_df = pd.DataFrame(extracted_rows)
                        st.session_state.ocr_station_rows = preview_df.copy()
                        station_preview = []
                        pipeline_debug_rows = []
                        for row in extracted_rows:
                            raw_start_station = row.get("start_station")
                            raw_end_station = row.get("end_station")
                            start_station = normalize_station(raw_start_station)
                            end_station = normalize_station(raw_end_station)
                            pipeline_debug_rows.append({
                                "raw_start_station": raw_start_station,
                                "raw_end_station": raw_end_station,
                                "normalized_start_station": start_station,
                                "normalized_end_station": end_station,
                                "source_file": row.get("source_file", ""),
                            })
                            shared_station_payload = {
                                "depth_ft": row.get("depth_ft"),
                                "boc_ft": row.get("boc_ft"),
                                "notes": row.get("notes", ""),
                                "date": row.get("date", ""),
                                "crew": row.get("crew", ""),
                                "print": row.get("print", ""),
                                "source_file": row.get("source_file", ""),
                            }
                            if start_station:
                                station_preview.append({
                                    "station": start_station,
                                    **shared_station_payload,
                                })
                            if end_station and end_station != start_station:
                                station_preview.append({
                                    "station": end_station,
                                    **shared_station_payload,
                                })
                        st.session_state.station_rows = pd.DataFrame(station_preview) if station_preview else pd.DataFrame(columns=["station", "depth_ft", "boc_ft", "notes", "date", "crew", "print", "source_file"])
                        st.session_state.pipeline_debug = {
                            "extracted_row_station_debug": pipeline_debug_rows,
                            "station_preview": station_preview,
                            "station_rows_station_values": st.session_state.station_rows["station"].tolist() if "station" in st.session_state.station_rows.columns else [],
                        }
                    else:
                        st.session_state.ocr_station_rows = pd.DataFrame(columns=["start_station", "end_station", "depth_ft", "boc_ft", "notes", "date", "crew", "print", "source_file"])
                        st.session_state.station_rows = pd.DataFrame(columns=["station", "depth_ft", "boc_ft", "notes", "date", "crew", "print", "source_file"])
                        st.session_state.pipeline_debug = {
                            "extracted_row_station_debug": [],
                            "station_preview": [],
                            "station_rows_station_values": [],
                        }

                    refreshed = get_current_state() or {}
                    st.session_state.route_coords = refreshed.get("route_coords", []) or st.session_state.route_coords
                    existing_polygon_features = (st.session_state.kmz_reference or {}).get("polygon_features", []) or []
                    refreshed_kmz_reference = refreshed.get("kmz_reference", {}) or st.session_state.kmz_reference
                    st.session_state.kmz_reference = merge_polygon_features_into_kmz_reference(refreshed_kmz_reference, existing_polygon_features)

                    status = data.get("auto_match_status")
                    source_file_count = len({str(r.get("source_file", "") or "").strip() or "field_file" for r in extracted_rows}) if extracted_rows else 0
                    has_batch_assignments = any(str(item.get("selected_route_id") or "").strip() for item in (st.session_state.per_file_route_assignments or []))
                    if status == "auto_assigned":
                        try:
                            if extracted_rows and st.session_state.selected_route_id and source_file_count <= 1:
                                _commit_result = commit_rows(extracted_rows, st.session_state.selected_route_id)
                                refreshed = get_current_state() or {}
                                st.session_state.selected_route_id = refreshed.get("selected_route_id", st.session_state.selected_route_id)
                                st.session_state.selected_route_name = refreshed.get("selected_route_name", st.session_state.selected_route_name)
                                st.session_state.selected_route_match = refreshed.get("selected_route_match", st.session_state.selected_route_match)
                                st.session_state.route_match_candidates = refreshed.get("route_match_candidates", []) or st.session_state.route_match_candidates
                                st.session_state.route_coords = refreshed.get("route_coords", []) or st.session_state.route_coords
                                st.session_state.generated_segments = refreshed.get("redline_segments", []) or []
                                st.session_state.invalid_redlines = refreshed.get("validation_issues", []) or []
                                st.session_state.map_points = refreshed.get("map_points", []) or []
                                if refreshed.get("kmz_reference"):
                                    existing_polygon_features = (st.session_state.kmz_reference or {}).get("polygon_features", []) or []
                                    refreshed_kmz_reference = refreshed.get("kmz_reference", {}) or st.session_state.kmz_reference
                                    st.session_state.kmz_reference = merge_polygon_features_into_kmz_reference(refreshed_kmz_reference, existing_polygon_features)
                            else:
                                st.session_state.generated_segments = []
                                st.session_state.invalid_redlines = []
                                st.session_state.map_points = []
                            _enable_trace_render_layers()
                        except Exception as commit_exc:
                            st.warning(f"Auto-match succeeded, but auto-render commit failed: {commit_exc}")
                        if source_file_count <= 1:
                            st.success(f"Auto-matched to {st.session_state.selected_route_name or st.session_state.selected_route_id}.")
                        else:
                            st.success(f"Processed {source_file_count} bore log files with per-file route assignments.")
                    elif status == "needs_review":
                        if source_file_count > 1 or has_batch_assignments:
                            _enable_trace_render_layers()
                        st.info("Route candidates identified. Select the correct path for each file to continue.")
                    else:
                        if source_file_count > 1 and has_batch_assignments:
                            _enable_trace_render_layers()
                        st.info("Route candidates identified. Review the available paths for each file.")
                else:
                    st.error(data.get("error", "Field file processing failed."))
            except Exception as exc:
                st.error(f"Field file processing failed: {exc}")

    if field_files:
        st.markdown(f"**{len(field_files)} files processed • Ready for review**")

    if st.session_state.selected_route_match:
        match = st.session_state.selected_route_match
        st.markdown(route_confidence_badge(match.get("confidence_label") or "High confidence"), unsafe_allow_html=True)
        st.write(f"**Assigned route:** {match.get('route_name') or st.session_state.selected_route_name}")
        st.caption("Why this matched: " + "; ".join(match.get("match_reasons") or []))
    elif st.session_state.route_match_candidates:
        best = st.session_state.route_match_candidates[0]
        st.markdown(route_confidence_badge(best.get("confidence_label") or "Route candidates identified"), unsafe_allow_html=True)
        st.write(f"**Top route candidate:** {best.get('route_name')}")
        st.caption("Why this route was surfaced: " + "; ".join(best.get("match_reasons") or []))

    if False and st.session_state.route_match_candidates:
        with st.expander("Route match details", expanded=False):
            candidate_df = pd.DataFrame([
            {
                "route_name": item.get("route_name"),
                "confidence": item.get("confidence_label"),
                "route_length_ft": item.get("route_length_ft"),
                "dataset_span_ft": item.get("dataset_span_ft"),
                "length_delta_ft": item.get("length_delta_ft"),
                "coverage_ok": item.get("coverage_ok"),
            }
            for item in st.session_state.route_match_candidates
        ])
        st.dataframe(candidate_df, use_container_width=True, hide_index=True, height=220)

    render_per_file_assignment_controls()


    if False and st.session_state.last_corridor_debug:
        with st.expander("Debug (hidden for beta)", expanded=False):
            debug = st.session_state.last_corridor_debug or {}
            debug_df = pd.DataFrame([
                {
                    "matched_pdf_sheet": debug.get("matched_pdf_sheet"),
                    "matched_chain_id": debug.get("matched_chain_id"),
                    "matched_corridor_ids": ", ".join(debug.get("matched_corridor_ids") or []),
                    "matched_station_range": debug.get("matched_station_range"),
                    "matched_street_names": ", ".join(debug.get("matched_street_names") or []),
                    "station_reset_detected": debug.get("station_reset_detected"),
                    "kmz_candidates_before_filter": debug.get("kmz_candidates_before_filter"),
                    "kmz_candidates_after_filter": debug.get("kmz_candidates_after_filter"),
                }
            ])
            st.dataframe(debug_df, use_container_width=True, hide_index=True)

    if False and not st.session_state.ocr_station_rows.empty:
        with st.expander("Extracted field rows", expanded=False):
            st.dataframe(st.session_state.ocr_station_rows, use_container_width=True, hide_index=True, height=240)

    if False:
        pass
        pipeline_debug = st.session_state.get("pipeline_debug", {}) or {}
        extracted_debug_rows = pipeline_debug.get("extracted_row_station_debug", []) or []
        st.markdown("**Extracted rows → normalized stations**")
        if extracted_debug_rows:
            st.dataframe(pd.DataFrame(extracted_debug_rows), use_container_width=True, hide_index=True, height=240)
        else:
            st.caption("No extracted row station debug available yet.")

        st.markdown("**station_preview before DataFrame creation**")
        station_preview_rows = pipeline_debug.get("station_preview", []) or []
        if station_preview_rows:
            st.dataframe(pd.DataFrame(station_preview_rows), use_container_width=True, hide_index=True, height=240)
        else:
            st.caption("station_preview is empty.")

        st.markdown("**st.session_state.station_rows[\"station\"] after DataFrame creation**")
        station_values = pipeline_debug.get("station_rows_station_values", []) or []
        if station_values:
            st.dataframe(pd.DataFrame({"station": station_values}), use_container_width=True, hide_index=True, height=180)
        else:
            st.caption("station_rows station list is empty.")

        st.markdown("**local_station_points summary**")
        st.write({"len(local_station_points)": pipeline_debug.get("local_station_points_count", 0)})

        st.markdown("**full local_station_points payload**")
        local_point_rows = pipeline_debug.get("local_station_points", []) or []
        if local_point_rows:
            st.dataframe(pd.DataFrame(local_point_rows), use_container_width=True, hide_index=True, height=240)
        else:
            st.caption("local_station_points is empty.")

    if st.session_state.sheet_match_suggestions:
        with st.expander("Reference PDF alignment", expanded=False):
            for suggestion in st.session_state.sheet_match_suggestions[:5]:
                st.write(f"Page {suggestion.get('page_number')} · score {suggestion.get('score')}")
                if suggestion.get("station_ranges"):
                    st.caption("Matched station ranges: " + ", ".join(suggestion.get("station_ranges") or []))
                if suggestion.get("reasons"):
                    st.caption("; ".join(suggestion.get("reasons") or []))
                st.divider()


with right:
    st.subheader("4) Weekly Completed Drill Trace Map")
    st.caption("Green/blue design context stays available, while the completed weekly drill trace is shown in red with exact station-by-station field data. Hover or click stations for exact depth and BOC — no averaging.")
    persistent_layer_checkbox_row()
    route_coords = st.session_state.route_coords or []
    kmz_reference = st.session_state.kmz_reference or {}
    design_lines = kmz_reference.get("line_features", []) or []
    design_points = kmz_reference.get("point_features", []) or []
    design_polygons = kmz_reference.get("polygon_features", []) or []

    source_file_route_runs = build_source_file_route_runs(
        st.session_state.station_rows,
        st.session_state.route_catalog or [],
        st.session_state.per_file_route_assignments or [],
        st.session_state.selected_route_id,
        st.session_state.selected_route_name,
        route_coords,
    )
    local_station_points = [point for run in source_file_route_runs for point in (run.get("local_station_points") or [])]
    render_route_coords = route_coords
    if len(source_file_route_runs) > 1:
        render_route_coords = []
    existing_pipeline_debug = st.session_state.get("pipeline_debug", {}) or {}
    st.session_state.pipeline_debug = {
        **existing_pipeline_debug,
        "local_station_points_count": len(local_station_points or []),
        "local_station_points": local_station_points or [],
    }
    bounds = compute_feature_bounds(kmz_reference, render_route_coords, st.session_state.generated_segments or [], local_station_points or st.session_state.map_points or [])
    if bounds:
        center_lat = (bounds[0][0] + bounds[1][0]) / 2
        center_lon = (bounds[0][1] + bounds[1][1]) / 2
        folium_map = folium.Map(location=[center_lat, center_lon], zoom_start=18, max_zoom=24, tiles=None, control_scale=True, prefer_canvas=True)
        folium.TileLayer("CartoDB positron", name="Light Basemap", overlay=False, control=True, show=True, max_zoom=24, max_native_zoom=22).add_to(folium_map)
        folium_map.fit_bounds(bounds, padding=(26, 26))
        folium_map.options["maxZoom"] = 24
    else:
        folium_map = folium.Map(location=[20, 0], zoom_start=2, max_zoom=24, tiles="CartoDB positron", control_scale=True, prefer_canvas=True)

    pane_kmz = folium.map.CustomPane("pane_kmz_design", z_index=200)
    pane_route = folium.map.CustomPane("pane_main_route", z_index=300)
    pane_trace = folium.map.CustomPane("pane_completed_trace", z_index=400)
    pane_bore = folium.map.CustomPane("pane_bore_stations", z_index=500)
    pane_labels = folium.map.CustomPane("pane_station_labels", z_index=600)
    pane_kmz.add_to(folium_map)
    pane_route.add_to(folium_map)
    pane_trace.add_to(folium_map)
    pane_bore.add_to(folium_map)
    pane_labels.add_to(folium_map)

    major_design_group = folium.FeatureGroup(name="KMZ Design", show=bool(st.session_state.layer_show_kmz_design))
    drop_group = folium.FeatureGroup(name="House Drops", show=bool(st.session_state.layer_show_house_drops))
    design_node_group = folium.FeatureGroup(name="KMZ Nodes", show=bool(st.session_state.layer_show_kmz_nodes))
    route_group = folium.FeatureGroup(name="Design / Selected Route", show=bool(st.session_state.layer_show_main_route))
    redline_group = folium.FeatureGroup(name="Completed Weekly Drill Trace", show=bool(st.session_state.layer_show_redline_segments))
    label_group = folium.FeatureGroup(name="BEGIN / END Labels", show=bool(st.session_state.layer_show_station_labels))
    point_group = folium.FeatureGroup(name="Bore Stations", show=bool(st.session_state.layer_show_bore_stations))

    station_lookup = {}
    for _, row in st.session_state.station_rows.iterrows():
        station = normalize_station(row.get("station"))
        source_file = str(row.get("source_file", "") or "").strip() or "field_file"
        if station:
            station_lookup[(source_file, station)] = {
                "depth_ft": row.get("depth_ft", ""),
                "boc_ft": row.get("boc_ft", ""),
                "notes": row.get("notes", ""),
                "date": row.get("date", ""),
                "crew": row.get("crew", ""),
                "print": row.get("print", ""),
                "source_file": source_file,
            }

    for feature in design_lines:
        coords = feature.get("coords", []) or []
        if len(coords) < 2:
            continue
        target_group = drop_group if feature.get("role") == "house_drop" else major_design_group
        color = "#2E7D32"
        weight = 4 if feature.get("role") != "house_drop" else 3
        opacity = 0.90 if feature.get("role") != "house_drop" else 0.75
        tooltip = feature.get("name") or feature.get("folder_path") or feature.get("role") or "Design line"
        folium.PolyLine(coords, color=color, weight=weight, opacity=opacity, tooltip=tooltip, pane="pane_kmz_design").add_to(target_group)


    kmz_outline_polygons = _kmz_outline_polygons(kmz_reference)
    if not kmz_outline_polygons:
        fallback_outline = _kmz_outline_polygon(design_lines)
        if len(fallback_outline) >= 4:
            kmz_outline_polygons = [fallback_outline]

    if st.session_state.layer_show_kmz_shadow:
        for polygon_coords in kmz_outline_polygons:
            folium.Polygon(
                locations=polygon_coords,
                color="#81D4FA",
                weight=3,
                opacity=0.95,
                fill=True,
                fill_color="#81D4FA",
                fill_opacity=0.14,
                pane="pane_kmz_design",
            ).add_to(major_design_group)
            folium.PolyLine(
                polygon_coords,
                color="#81D4FA",
                weight=10,
                opacity=0.20,
                pane="pane_kmz_design",
            ).add_to(major_design_group)
            folium.PolyLine(
                polygon_coords,
                color="#81D4FA",
                weight=5,
                opacity=0.34,
                pane="pane_kmz_design",
            ).add_to(major_design_group)

    for feature in design_points:
        if feature.get("role") == "house_drop":
            continue
        lat = feature.get("lat")
        lon = feature.get("lon")
        if lat is None or lon is None:
            continue
        tooltip = feature.get("name") or feature.get("folder_path") or "KMZ node"
        folium.CircleMarker(location=[lat, lon], radius=3, color="#263238", fill=True, fill_color="#FFFFFF", fill_opacity=0.95, weight=1, tooltip=tooltip, pane="pane_kmz_design").add_to(design_node_group)

    if source_file_route_runs:
        for run in source_file_route_runs:
            completed_trace_coords = run.get("completed_trace_coords") or []
            if len(completed_trace_coords) >= 2:
                tooltip = f"Completed drilled path — {run.get('source_file', '')}" if run.get('source_file') else "Completed drilled path"
                folium.PolyLine(completed_trace_coords, color="#FFCDD2", weight=14, opacity=0.25, pane="pane_completed_trace").add_to(redline_group)
                folium.PolyLine(completed_trace_coords, color="#C62828", weight=7, opacity=0.96, tooltip=tooltip, pane="pane_completed_trace").add_to(redline_group)

    # Interactive per-station trace segments
    for run in source_file_route_runs:
        run_points = run.get("local_station_points") or []
        if len(run_points) >= 2:
            for idx in range(1, len(run_points)):
                prev_pt = run_points[idx - 1]
                cur_pt = run_points[idx]
                seg_coords = [[prev_pt["lat"], prev_pt["lon"]], [cur_pt["lat"], cur_pt["lon"]]]
                seg_tooltip = (
                    f"{prev_pt.get('station')} → {cur_pt.get('station')} | "
                    f"At {cur_pt.get('station')}: Depth {cur_pt.get('depth_ft', '—')} ft, "
                    f"BOC {cur_pt.get('boc_ft', '—')} ft"
                )
                seg_popup = build_station_info_card(
                    station=cur_pt.get('station', ''),
                    depth_ft=cur_pt.get('depth_ft', '—'),
                    boc_ft=cur_pt.get('boc_ft', '—'),
                    date=cur_pt.get('date', ''),
                    crew=cur_pt.get('crew', ''),
                    print_page=cur_pt.get('print', ''),
                    notes=cur_pt.get('notes', ''),
                )
                folium.PolyLine(seg_coords, color="#EF9A9A", weight=12, opacity=0.01, tooltip=seg_tooltip, popup=seg_popup, pane="pane_completed_trace").add_to(redline_group)

    for point in local_station_points or st.session_state.map_points or []:
        lat = point.get("lat")
        lon = point.get("lon")
        if lat is None or lon is None:
            continue
        station = point.get("station", "")
        source_file = str(point.get("source_file", "") or "").strip() or "field_file"
        details = station_lookup.get((source_file, station), {})
        popup_html = build_station_info_card(
            station=station,
            depth_ft=details.get('depth_ft', point.get('depth_ft', '—')),
            boc_ft=details.get('boc_ft', point.get('boc_ft', '—')),
            date=details.get('date', point.get('date', '')),
            crew=details.get('crew', point.get('crew', '')),
            print_page=details.get('print', point.get('print', '')),
            notes=details.get('notes', point.get('notes', '')),
        )
        hover_tooltip = folium.Tooltip(popup_html, sticky=True, direction="top", opacity=1.0)
        folium.CircleMarker(location=[lat, lon], radius=4, color="#000000", fill=True, fill_color="#000000", fill_opacity=0.95, tooltip=hover_tooltip, pane="pane_bore_stations").add_to(point_group)
    for run in source_file_route_runs:
        run_points = run.get("local_station_points") or []
        if run_points:
            boundary_points = [run_points[0]]
            if len(run_points) > 1:
                boundary_points.append(run_points[-1])
            
            for idx, boundary_point in enumerate(boundary_points):
                label_text = "BEGIN" if idx == 0 else "END"

                node_lat = boundary_point['lat']
                node_lon = boundary_point['lon']

                label_lat = node_lat + (0.00004 if idx == 0 else -0.00004)
                label_lon = node_lon + (-0.00004 if idx == 0 else 0.00004)

                folium.Marker(
                    location=[label_lat, label_lon],
                    icon=make_div_icon(label_text, text_color="#000000", bg="#FFFFFF", border="#000000", size=14),
                    pane="pane_station_labels"
                ).add_to(label_group)

                folium.PolyLine(
                    locations=[[label_lat, label_lon], [node_lat, node_lon]],
                    color="#000000",
                    weight=2,
                    opacity=1.0,
                    pane="pane_station_labels"
                ).add_to(label_group)


    # Keep generated deviation segments available as a later analysis layer
    for segment in st.session_state.generated_segments or []:
        coords = segment.get("coords", []) or []
        if len(coords) < 2:
            continue
        start_station = segment.get("start_station", "")
        end_station = segment.get("end_station", "")
        popup_html = (
            f"<b>Deviation layer</b><br>"
            f"<b>{start_station} → {end_station}</b><br>"
            f"<b>Length:</b> {segment.get('length_ft', 0)} ft<br>"
            f"<b>Reason:</b> {segment.get('reason', '')}<br>"
            f"<b>Notes:</b> {segment.get('notes', '')}"
        )
        folium.PolyLine(coords, color="#F8BBD0", weight=9, opacity=0.10, pane="pane_completed_trace").add_to(redline_group)
        folium.PolyLine(coords, color="#AD1457", weight=4, opacity=0.75, tooltip=f"Deviation overlay {start_station} → {end_station}", popup=popup_html, pane="pane_completed_trace").add_to(redline_group)

    major_design_group.add_to(folium_map)
    drop_group.add_to(folium_map)
    design_node_group.add_to(folium_map)
    route_group.add_to(folium_map)
    redline_group.add_to(folium_map)
    label_group.add_to(folium_map)
    point_group.add_to(folium_map)
    plugins.Fullscreen(position="topright").add_to(folium_map)
    plugins.MeasureControl(position="topright", primary_length_unit="feet").add_to(folium_map)

    legend_html = """
    <div style="position: fixed; bottom: 18px; left: 18px; z-index: 9999; background: rgba(255,255,255,.96); padding: 10px 12px; border: 1px solid #777; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,.18); font-size: 12px; min-width: 290px;">
      <div style="font-weight:700; font-size:13px; margin-bottom:6px;">Overlay Legend</div>
      <div><span style="display:inline-block;width:22px;height:4px;background:#C62828;vertical-align:middle;margin-right:8px;"></span>Completed Weekly Drill Trace</div>
      <div><span style="display:inline-block;width:22px;height:4px;background:#2E7D32;vertical-align:middle;margin-right:8px;"></span>KMZ Design</div>
      <div><span style="display:inline-block;width:22px;height:4px;background:#FB8C00;vertical-align:middle;margin-right:8px;"></span>Preview Route</div>
      <div><span style="display:inline-block;width:22px;height:4px;background:#00ACC1;vertical-align:middle;margin-right:8px;"></span>House Drops</div>
      <div><span style="display:inline-block;width:22px;height:4px;background:#AD1457;vertical-align:middle;margin-right:8px;"></span>Deviation Overlay (later analysis)</div>
      <div><span style="display:inline-block;width:10px;height:10px;border:2px solid #555;background:#FFF8DC;border-radius:2px;vertical-align:middle;margin-right:8px;"></span>BEGIN / END Label</div>
      <div><span style="display:inline-block;width:10px;height:10px;border:2px solid #B71C1C;background:#FFCDD2;border-radius:50%;vertical-align:middle;margin-right:8px;"></span>Exact Bore Station</div>
    </div>
    """
    folium_map.get_root().html.add_child(folium.Element(legend_html))
    st_folium(folium_map, width=None, height=720, returned_objects=[])

st.divider()
st.subheader("5) Export to Google Earth")
export_has_content = bool(
    (st.session_state.route_coords or [])
    or (st.session_state.generated_segments or [])
    or not st.session_state.station_rows.empty
    or ((st.session_state.kmz_reference or {}).get("line_features") or [])
    or ((st.session_state.kmz_reference or {}).get("polygon_features") or [])
    or ((st.session_state.kmz_reference or {}).get("point_features") or [])
    or (source_file_route_runs or [])
    or (local_station_points or [])
)
kmz_bytes, kml_bytes = build_export_kmz_bytes(
    st.session_state.route_name or "OSP Redlining Export",
    st.session_state.route_coords or [],
    st.session_state.generated_segments or [],
    st.session_state.station_rows,
    st.session_state.map_points or [],
    st.session_state.kmz_reference or {},
    source_file_route_runs=source_file_route_runs,
    local_station_points=local_station_points,
)
exp1, exp2 = st.columns(2)
with exp1:
    st.download_button("Export KMZ", data=kmz_bytes, file_name="osp_redlining_export.kmz", mime="application/vnd.google-earth.kmz", use_container_width=True, disabled=not export_has_content)
with exp2:
    st.download_button("Export KML", data=kml_bytes, file_name="osp_redlining_export.kml", mime="application/vnd.google-earth.kml+xml", use_container_width=True, disabled=not export_has_content)

st.divider()
st.subheader("6) Production Report")

rate1, _, _ = st.columns([1, 1, 2])
with rate1:
    material_rate = st.number_input("Price Per Ft.", min_value=0.0, value=3.50, step=0.10, format="%.2f")

production_rows = build_production_report_rows(
    st.session_state.station_rows,
    st.session_state.route_catalog or [],
    st.session_state.per_file_route_assignments or [],
    st.session_state.selected_route_id,
    st.session_state.selected_route_name,
    st.session_state.route_coords or [],
)

production_df = pd.DataFrame(production_rows)
if production_df.empty:
    st.caption("Assigned bore logs shown on the map will populate this table automatically.")
else:
    crew_summary_rows = build_crew_summary_rows(production_rows, material_rate)
    if crew_summary_rows:
        st.caption("Crew Separation")
        crew_summary_df = pd.DataFrame(crew_summary_rows)
        st.dataframe(
            crew_summary_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "crew": st.column_config.TextColumn("Crew"),
                "bore_logs": st.column_config.NumberColumn("Bore Logs", format="%d"),
                "routes": st.column_config.NumberColumn("Routes", format="%d"),
                "segments": st.column_config.NumberColumn("Segments", format="%d"),
                "total_length_ft": st.column_config.NumberColumn("Total Length (ft)", format="%.2f"),
                "total_cost": st.column_config.NumberColumn("Total", format="$ %.2f"),
            },
        )

    production_df["material_cost"] = production_df["length_ft"] * material_rate

    table_df = production_df[[
        "selected",
        "source_file",
        "route_name",
        "start_station",
        "end_station",
        "length_ft",
        "depth_ft",
        "boc_ft",
        "date",
        "crew",
        "print",
        "notes",
        "material_cost",
    ]].copy()

    edited_df = st.data_editor(
        table_df,
        use_container_width=True,
        hide_index=True,
        disabled=[
            "source_file",
            "route_name",
            "start_station",
            "end_station",
            "length_ft",
            "depth_ft",
            "boc_ft",
            "date",
            "crew",
            "print",
            "notes",
            "material_cost",
        ],
        column_config={
            "selected": st.column_config.CheckboxColumn("Bill", default=True),
            "source_file": st.column_config.TextColumn("Bore Log"),
            "route_name": st.column_config.TextColumn("Assigned Route"),
            "start_station": st.column_config.TextColumn("Start"),
            "end_station": st.column_config.TextColumn("End"),
            "length_ft": st.column_config.NumberColumn("Length (ft)", format="%.2f"),
            "depth_ft": st.column_config.TextColumn("Depth"),
            "boc_ft": st.column_config.TextColumn("BOC"),
            "date": st.column_config.TextColumn("Date"),
            "crew": st.column_config.TextColumn("Crew"),
            "print": st.column_config.TextColumn("Print"),
            "notes": st.column_config.TextColumn("Notes"),
            "material_cost": st.column_config.NumberColumn("Total", format="$ %.2f"),
        },
        key="production_report_editor",
    )

    selected_df = edited_df[edited_df["selected"] == True].copy()
    total_length = float(selected_df["length_ft"].sum()) if not selected_df.empty else 0.0
    grand_total = float(selected_df["material_cost"].sum()) if not selected_df.empty else 0.0

    s1, s2 = st.columns(2)
    s1.metric("Total Selected Length", f"{total_length:,.2f} ft")
    s2.metric("Grand Total", f"${grand_total:,.2f}")
