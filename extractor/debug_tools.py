from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Tuple

import cv2
import numpy as np

from extractor.preprocess import ensure_dir
from extractor.template_geometry import TableBox



def save_debug_page(debug_dir: Path, name: str, image: np.ndarray) -> None:
    ensure_dir(debug_dir)
    cv2.imwrite(str(debug_dir / name), image)



def draw_table_overlays(page_gray: np.ndarray, tables: Iterable[TableBox]) -> np.ndarray:
    overlay = cv2.cvtColor(page_gray, cv2.COLOR_GRAY2BGR)
    for table in tables:
        x1, y1, x2, y2 = table.bbox
        cv2.rectangle(overlay, (x1, y1), (x2, y2), (0, 0, 255), 2)

        for x in table.vertical_lines:
            cv2.line(overlay, (x1 + x, y1), (x1 + x, y2), (255, 0, 0), 1)
        for y in table.horizontal_lines:
            cv2.line(overlay, (x1, y1 + y), (x2, y1 + y), (0, 180, 0), 1)
    return overlay



def save_cell(debug_dir: Path, table_index: int, row_index: int, field_name: str, image: np.ndarray) -> None:
    ensure_dir(debug_dir)
    cell_dir = debug_dir / f"table_{table_index:02d}"
    ensure_dir(cell_dir)
    filename = f"row_{row_index:02d}_{field_name}.png"
    cv2.imwrite(str(cell_dir / filename), image)
