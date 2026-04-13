from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple

import cv2
import numpy as np


@dataclass
class TableBox:
    x: int
    y: int
    w: int
    h: int

    @property
    def as_tuple(self) -> Tuple[int, int, int, int]:
        return self.x, self.y, self.w, self.h


def _cluster_positions(values: np.ndarray, gap: int = 3) -> List[Tuple[int, int]]:
    vals = sorted({int(v) for v in values.tolist()})
    if not vals:
        return []
    groups: List[Tuple[int, int]] = []
    start = prev = vals[0]
    for value in vals[1:]:
        if value - prev <= gap:
            prev = value
        else:
            groups.append((start, prev))
            start = prev = value
    groups.append((start, prev))
    return groups


def detect_tables(gray: np.ndarray) -> List[TableBox]:
    inv = cv2.threshold(gray, 205, 255, cv2.THRESH_BINARY_INV)[1]
    h, w = gray.shape[:2]

    vert_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, max(40, h // 35)))
    hor_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (max(60, w // 30), 1))
    vert = cv2.morphologyEx(inv, cv2.MORPH_OPEN, vert_kernel)
    hor = cv2.morphologyEx(inv, cv2.MORPH_OPEN, hor_kernel)
    lines = cv2.add(vert, hor)

    num_labels, _, stats, _ = cv2.connectedComponentsWithStats(lines, 8)
    boxes: List[TableBox] = []
    for idx in range(1, num_labels):
        x, y, ww, hh, area = stats[idx]
        if ww < 120 or hh < 300 or area < 2000:
            continue
        aspect = ww / max(hh, 1)
        if 0.15 <= aspect <= 0.7:
            boxes.append(TableBox(int(x), int(y), int(ww), int(hh)))

    boxes = sorted(boxes, key=lambda b: (b.x, b.y))

    deduped: List[TableBox] = []
    for box in boxes:
        if not deduped:
            deduped.append(box)
            continue
        prev = deduped[-1]
        if abs(box.x - prev.x) < 40 and abs(box.y - prev.y) < 80:
            if box.w * box.h > prev.w * prev.h:
                deduped[-1] = box
        else:
            deduped.append(box)

    return deduped[:3]


def iter_row_boxes(table: TableBox, gray: np.ndarray, estimated_rows: int = 28):
    """Use actual horizontal lines when possible; fall back to proportional rows."""
    x, y, w, h = table.as_tuple
    roi = gray[y:y + h, x:x + w]
    inv = cv2.threshold(roi, 205, 255, cv2.THRESH_BINARY_INV)[1]
    hor_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (max(30, w // 7), 1))
    hor = cv2.morphologyEx(inv, cv2.MORPH_OPEN, hor_kernel)
    ysum = (hor > 0).sum(axis=1)
    line_rows = np.where(ysum > max(20, w // 10))[0]
    clusters = _cluster_positions(line_rows, gap=3)

    line_midpoints = [int((a + b) / 2) for a, b in clusters if (b - a + 1) <= 30]
    if len(line_midpoints) >= 10:
        row_idx = 0
        # first band is header; start after that
        for i in range(1, len(line_midpoints) - 1):
            y1 = int(line_midpoints[i] + 2)
            y2 = int(line_midpoints[i + 1] - 2)
            if y2 - y1 < 12:
                continue
            yield row_idx, (x, y + y1, w, y2 - y1)
            row_idx += 1
        return

    header_h = max(52, int(h * 0.045))
    usable_h = h - header_h
    row_h = usable_h / float(estimated_rows)
    for row_idx in range(estimated_rows):
        y1 = int(y + header_h + row_idx * row_h)
        y2 = int(y + header_h + (row_idx + 1) * row_h)
        if y2 <= y1:
            continue
        yield row_idx, (x, y1, w, y2 - y1)


def split_row_cells(row_img: np.ndarray) -> List[np.ndarray]:
    h, w = row_img.shape[:2]
    # tuned from the real Brenham forms
    cuts = [0.02, 0.37, 0.69, 0.98]
    cells: List[np.ndarray] = []
    top = max(1, int(h * 0.03))
    bottom = max(top + 1, h - int(h * 0.03))
    for idx in range(3):
        x1 = int(cuts[idx] * w)
        x2 = int(cuts[idx + 1] * w)
        x1 = min(max(0, x1), w - 1)
        x2 = min(max(x1 + 1, x2), w)
        cells.append(row_img[top:bottom, x1:x2])
    return cells
