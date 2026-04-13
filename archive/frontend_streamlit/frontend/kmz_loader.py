import re
import zipfile
from pathlib import Path
from typing import Dict, List, Optional
from xml.etree import ElementTree as ET


def _strip_namespace(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _find_first_kml_in_kmz(kmz_path: Path) -> Optional[bytes]:
    with zipfile.ZipFile(kmz_path, "r") as zf:
        kml_names = [name for name in zf.namelist() if name.lower().endswith(".kml")]
        if not kml_names:
            return None

        # Prefer doc.kml if present
        for name in kml_names:
            if name.lower().endswith("doc.kml"):
                return zf.read(name)

        return zf.read(kml_names[0])


def _parse_coordinates_text(coord_text: str) -> List[List[float]]:
    coords = []
    if not coord_text:
        return coords

    chunks = re.split(r"\s+", coord_text.strip())
    for chunk in chunks:
        parts = chunk.split(",")
        if len(parts) >= 2:
            try:
                lon = float(parts[0])
                lat = float(parts[1])
                coords.append([lon, lat])
            except ValueError:
                continue
    return coords


def _element_text(elem) -> str:
    if elem is None or elem.text is None:
        return ""
    return elem.text.strip()


def _iter_children_by_local_name(elem, local_name: str):
    for child in list(elem):
        if _strip_namespace(child.tag) == local_name:
            yield child


def _find_first_child_by_local_name(elem, local_name: str):
    for child in list(elem):
        if _strip_namespace(child.tag) == local_name:
            return child
    return None


def _extract_placemark_name(placemark) -> str:
    name_elem = _find_first_child_by_local_name(placemark, "name")
    return _element_text(name_elem) if name_elem is not None else ""


def _extract_linestrings_from_geometry(geometry_elem) -> List[List[List[float]]]:
    """
    Returns list of line coordinate arrays in [lon, lat] form.
    """
    lines = []
    geom_type = _strip_namespace(geometry_elem.tag)

    if geom_type == "LineString":
        coords_elem = _find_first_child_by_local_name(geometry_elem, "coordinates")
        if coords_elem is not None:
            coords = _parse_coordinates_text(_element_text(coords_elem))
            if len(coords) >= 2:
                lines.append(coords)

    elif geom_type == "MultiGeometry":
        for child in list(geometry_elem):
            lines.extend(_extract_linestrings_from_geometry(child))

    return lines


def kmz_to_geojson(kmz_path: Path) -> Dict:
    """
    Extracts LineString geometry from a KMZ and returns a GeoJSON FeatureCollection.
    """
    kml_bytes = _find_first_kml_in_kmz(kmz_path)
    if not kml_bytes:
        raise ValueError(f"No KML file found inside KMZ: {kmz_path.name}")

    root = ET.fromstring(kml_bytes)

    features = []

    for elem in root.iter():
        if _strip_namespace(elem.tag) != "Placemark":
            continue

        placemark_name = _extract_placemark_name(elem)

        for child in list(elem):
            child_local = _strip_namespace(child.tag)
            if child_local not in {"LineString", "MultiGeometry"}:
                continue

            extracted_lines = _extract_linestrings_from_geometry(child)
            for coords in extracted_lines:
                features.append(
                    {
                        "type": "Feature",
                        "properties": {
                            "name": placemark_name,
                            "construction_type": "PLAN",
                        },
                        "geometry": {
                            "type": "LineString",
                            "coordinates": coords,
                        },
                    }
                )

    return {
        "type": "FeatureCollection",
        "features": features,
    }


def find_first_kmz_file(uploads_dir: Path) -> Optional[Path]:
    kmz_files = sorted(uploads_dir.glob("*.kmz"))
    if kmz_files:
        return kmz_files[0]
    return None