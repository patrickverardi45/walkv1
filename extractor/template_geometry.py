from __future__ import annotations

from typing import List, Tuple

import cv2
import numpy as np

from extractor.schemas import CellGeometry, TableGrid

BBox = Tuple[int, int, int, int]


def _merge_groups(values: np.ndarray, gap: int) -> List[Tuple[int, int]]:
    if values.size == 0:
        return []
    groups: List[Tuple[int, int]] = []
    start = int(values[0])
    end = int(values[0])
    for raw in values[1:]:
        value = int(raw)
        if value - end <= gap:
            end = value
        else:
            groups.append((start, end))
            start = value
            end = value
    groups.append((start, end))
    return groups


def _group_centers(groups: List[Tuple[int, int]]) -> List[int]:
    return [int((start + end) / 2) for start, end in groups]


def _extract_table_contours(binary_inv: np.ndarray) -> List[BBox]:
    horizontal_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (90, 1))
    vertical_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, 90))
    horizontal = cv2.morphologyEx(binary_inv, cv2.MORPH_OPEN, horizontal_kernel)
    vertical = cv2.morphologyEx(binary_inv, cv2.MORPH_OPEN, vertical_kernel)
    grid = cv2.add(horizontal, vertical)

    contours, _ = cv2.findContours(grid, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    boxes: List[BBox] = []
    for contour in contours:
        x, y, w, h = cv2.boundingRect(contour)
        if w < 180 or h < 600:
            continue
        aspect = w / float(h)
        if aspect > 0.5:
            continue
        boxes.append((x, y, x + w, y + h))

    boxes.sort(key=lambda box: box[0])
    if len(boxes) >= 3:
        return boxes[:3]

    h, w = binary_inv.shape[:2]
    fallback_ratios = [
        (0.075, 0.316, 0.316, 0.918),
        (0.360, 0.316, 0.599, 0.918),
        (0.644, 0.316, 0.885, 0.918),
    ]
    out: List[BBox] = []
    for rx1, ry1, rx2, ry2 in fallback_ratios:
        out.append((int(w * rx1), int(h * ry1), int(w * rx2), int(h * ry2)))
    return out


def _detect_vertical_lines(table_binary_inv: np.ndarray) -> List[int]:
    height, width = table_binary_inv.shape[:2]
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, max(35, height // 8)))
    vertical = cv2.morphologyEx(table_binary_inv, cv2.MORPH_OPEN, kernel)
    projection = vertical.sum(axis=0) / 255.0
    threshold = max(10.0, float(projection.max()) * 0.45)
    indices = np.where(projection >= threshold)[0]
    groups = _merge_groups(indices, gap=max(6, width // 30))
    centers = _group_centers(groups)

    if len(centers) > 4:
        compressed: List[int] = [centers[0]]
        min_spacing = max(35, width // 7)
        for value in centers[1:]:
            if value - compressed[-1] >= min_spacing:
                compressed.append(value)
        centers = compressed

    if len(centers) >= 4:
        return [centers[0], centers[1], centers[2], centers[-1]]

    return [0, int(width * 0.255), int(width * 0.615), width - 1]


def _detect_horizontal_lines(table_binary_inv: np.ndarray) -> List[int]:
    height, width = table_binary_inv.shape[:2]
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (max(40, width // 4), 1))
    horizontal = cv2.morphologyEx(table_binary_inv, cv2.MORPH_OPEN, kernel)
    projection = horizontal.sum(axis=1) / 255.0
    threshold = max(10.0, float(projection.max()) * 0.40)
    indices = np.where(projection >= threshold)[0]
    groups = _merge_groups(indices, gap=max(4, height // 150))
    centers = _group_centers(groups)

    if len(centers) >= 6:
        return centers

    header_h = max(18, int(height * 0.035))
    approx_rows = 33
    remaining = max(1, height - header_h)
    row_h = remaining / float(approx_rows)
    lines = [0, header_h]
    for i in range(1, approx_rows + 1):
        lines.append(min(height, header_h + int(round(i * row_h))))
    lines[-1] = height
    return lines


def build_table_grids(binary_inv: np.ndarray) -> List[TableGrid]:
    grids: List[TableGrid] = []
    for idx, (x1, y1, x2, y2) in enumerate(_extract_table_contours(binary_inv), start=1):
        crop = binary_inv[y1:y2, x1:x2]
        grids.append(
            TableGrid(
                table_index=idx,
                bbox=(x1, y1, x2, y2),
                vertical_lines=_detect_vertical_lines(crop),
                horizontal_lines=_detect_horizontal_lines(crop),
            )
        )
    return grids


def iter_row_cells(table_grid: TableGrid, inset_x: int = 6, inset_y: int = 3) -> List[CellGeometry]:
    x1, y1, _, _ = table_grid.bbox
    xs = table_grid.vertical_lines
    ys = table_grid.horizontal_lines
    if len(xs) < 4 or len(ys) < 3:
        return []

    cells: List[CellGeometry] = []
    for row_number in range(1, len(ys) - 1):
        top = ys[row_number]
        bottom = ys[row_number + 1]
        if bottom - top < 12:
            continue
        cells.append(
            CellGeometry(
                row_index=len(cells) + 1,
                station_bbox=(x1 + xs[0] + inset_x, y1 + top + inset_y, x1 + xs[1] - inset_x, y1 + bottom - inset_y),
                depth_bbox=(x1 + xs[1] + inset_x, y1 + top + inset_y, x1 + xs[2] - inset_x, y1 + bottom - inset_y),
                boc_bbox=(x1 + xs[2] + inset_x, y1 + top + inset_y, x1 + xs[3] - inset_x, y1 + bottom - inset_y),
            )
        )
    return cells
