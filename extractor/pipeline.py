from __future__ import annotations

from dataclasses import asdict
import glob
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import cv2
import fitz  # PyMuPDF
import pandas as pd

from .layout import detect_tables, iter_row_boxes, split_row_cells
from .ocr_utils import (
    build_station_candidates,
    int_to_station,
    parse_cell,
    repair_station_sequence,
    smooth_numeric_series,
    station_to_int,
)


class BoreLogOCRPipeline:
    def __init__(self, input_root: str, output_root: str):
        self.input_root = input_root
        self.output_root = output_root
        self.debug_root = os.path.join(output_root, "debug")
        os.makedirs(self.debug_root, exist_ok=True)

    def _iter_files(self) -> List[str]:
        patterns = ["*.jpeg", "*.jpg", "*.png", "*.pdf"]
        files: List[str] = []
        for pattern in patterns:
            files.extend(glob.glob(os.path.join(self.input_root, pattern)))
        return sorted(files)

    def _render_pdf_pages(self, pdf_path: str) -> List[Tuple[str, any]]:
        doc = fitz.open(pdf_path)
        pages = []
        for i, page in enumerate(doc):
            pix = page.get_pixmap(matrix=fitz.Matrix(2, 2), alpha=False)
            img_path = os.path.join(self.debug_root, "rendered_pages", f"{Path(pdf_path).stem}_page{i+1}.png")
            os.makedirs(os.path.dirname(img_path), exist_ok=True)
            pix.save(img_path)
            img = cv2.imread(img_path)
            pages.append((img_path, img))
        return pages

    def process(self) -> Dict[str, str]:
        all_rows: List[Dict] = []
        invalid_rows: List[Dict] = []
        file_summaries: List[Dict] = []

        files = self._iter_files()
        for file_path in files:
            file_name = os.path.basename(file_path)
            page_images: List[Tuple[str, any]]
            if file_path.lower().endswith(".pdf"):
                page_images = self._render_pdf_pages(file_path)
            else:
                page_images = [(file_path, cv2.imread(file_path))]

            file_valid = 0
            file_invalid = 0
            page_counter = 0
            for source_path, page_bgr in page_images:
                page_counter += 1
                if page_bgr is None:
                    invalid_rows.append({
                        "source_file": file_name,
                        "source_page": page_counter,
                        "rejection_reason": "failed to load image",
                    })
                    file_invalid += 1
                    continue

                gray = cv2.cvtColor(page_bgr, cv2.COLOR_BGR2GRAY)
                tables = detect_tables(gray)
                if not tables:
                    invalid_rows.append({
                        "source_file": file_name,
                        "source_page": page_counter,
                        "rejection_reason": "no bore-log table detected",
                    })
                    file_invalid += 1
                    continue

                page_rows: List[Dict] = []
                for table_idx, table in enumerate(tables[:1], start=1):
                    table_dir = os.path.join(self.debug_root, Path(file_name).stem, f"page_{page_counter}", f"table_{table_idx}")
                    os.makedirs(table_dir, exist_ok=True)

                    table_rows: List[Dict] = []
                    blank_streak = 0
                    for row_idx, row_box in iter_row_boxes(table, gray):
                        x, y, w, h = row_box
                        row_img = gray[y:y+h, x:x+w]
                        if row_img.size == 0:
                            continue
                        ink_pixels = int((255 - row_img).sum())
                        if ink_pixels < 22000:
                            blank_streak += 1
                            if blank_streak >= 4 and row_idx >= 4:
                                break
                            continue
                        blank_streak = 0

                        row_debug_dir = os.path.join(table_dir, f"row_{row_idx+1:02d}")
                        os.makedirs(row_debug_dir, exist_ok=True)
                        cv2.imwrite(os.path.join(row_debug_dir, "row.png"), row_img)

                        cells = split_row_cells(row_img)
                        parsed_station = parse_cell(cells[0], "station", row_debug_dir, "station")
                        parsed_depth = parse_cell(cells[1], "depth", row_debug_dir, "depth")
                        parsed_boc = parse_cell(cells[2], "boc", row_debug_dir, "boc")

                        row_record = {
                            "source_file": file_name,
                            "source_page": page_counter,
                            "table_index": table_idx,
                            "row_index": row_idx + 1,
                            "station_raw": parsed_station.raw,
                            "station_cleaned": parsed_station.cleaned,
                            "depth_raw": parsed_depth.raw,
                            "depth_cleaned": parsed_depth.cleaned,
                            "boc_raw": parsed_boc.raw,
                            "boc_cleaned": parsed_boc.cleaned,
                            "notes_raw": "",
                            "notes_cleaned": "",
                            "station_valid": parsed_station.valid,
                            "depth_valid": parsed_depth.valid,
                            "boc_valid": parsed_boc.valid,
                            "station_reason": parsed_station.reason,
                            "depth_reason": parsed_depth.reason,
                            "boc_reason": parsed_boc.reason,
                            "best_station_variant": parsed_station.best_variant,
                            "best_depth_variant": parsed_depth.best_variant,
                            "best_boc_variant": parsed_boc.best_variant,
                            "station_attempts": json.dumps([asdict(a) for a in parsed_station.attempts], ensure_ascii=False),
                            "depth_attempts": json.dumps([asdict(a) for a in parsed_depth.attempts], ensure_ascii=False),
                            "boc_attempts": json.dumps([asdict(a) for a in parsed_boc.attempts], ensure_ascii=False),
                        }
                        table_rows.append(row_record)

                    if not table_rows:
                        continue

                    repaired = repair_station_sequence(table_rows)
                    smoothed_depth = smooth_numeric_series([r.get("depth_cleaned") for r in table_rows], "depth")
                    smoothed_boc = smooth_numeric_series([r.get("boc_cleaned") for r in table_rows], "boc")

                    for row_record, station_text, depth_text, boc_text in zip(table_rows, repaired, smoothed_depth, smoothed_boc):
                        row_record["station_smoothed"] = station_text
                        row_record["station_smoothed_int"] = station_to_int(station_text) if station_text else None
                        row_record["depth_smoothed"] = depth_text
                        row_record["boc_smoothed"] = boc_text
                    page_rows.extend(table_rows)

                grouped: Dict[Tuple[str, int, int], List[Dict]] = {}
                for row_record in page_rows:
                    grouped.setdefault((row_record["source_file"], row_record["source_page"], row_record["table_index"]), []).append(row_record)

                for _key, rows in grouped.items():
                    rows = sorted(rows, key=lambda rr: rr["row_index"])
                    rows = [r for r in rows if r.get("station_smoothed")]
                    for idx in range(len(rows) - 1):
                        current = rows[idx]
                        nxt = rows[idx + 1]
                        start_int = station_to_int(current["station_smoothed"])
                        end_int = station_to_int(nxt["station_smoothed"])
                        if start_int is None or end_int is None:
                            invalid_rows.append({**current, "segment_reason": "missing normalized stations"})
                            file_invalid += 1
                            continue
                        diff = end_int - start_int
                        if not (1 <= diff <= 150):
                            invalid_rows.append({**current, "segment_reason": f"station jump rejected: {diff}"})
                            file_invalid += 1
                            continue

                        current["start_station"] = int_to_station(start_int)
                        current["end_station"] = int_to_station(end_int)
                        current["depth_ft"] = current["depth_smoothed"]
                        current["boc_ft"] = current["boc_smoothed"]
                        current["notes_reason"] = ""
                        current["segment_valid"] = True
                        current["segment_reason"] = "ok"
                        all_rows.append(current)
                        file_valid += 1

            file_summaries.append({
                "source_file": file_name,
                "valid_segments": file_valid,
                "invalid_rows": file_invalid,
            })

        valid_df = pd.DataFrame(all_rows)
        invalid_df = pd.DataFrame(invalid_rows)
        summary_df = pd.DataFrame(file_summaries)

        valid_csv = os.path.join(self.output_root, "ocr_valid_segments.csv")
        invalid_csv = os.path.join(self.output_root, "ocr_invalid_rows.csv")
        summary_csv = os.path.join(self.output_root, "ocr_file_summary.csv")
        valid_df.to_csv(valid_csv, index=False)
        invalid_df.to_csv(invalid_csv, index=False)
        summary_df.to_csv(summary_csv, index=False)

        with open(os.path.join(self.output_root, "iteration_notes.txt"), "w", encoding="utf-8") as f:
            f.write(
                "OCR iteration: switched row extraction to actual table line detection, tightened station sequence repair, and smoothed depth/BOC within each table.\n"
                "Valid segments only come from normalized ##+## to ##+## station pairs with positive bounded jumps.\n"
            )

        return {
            "valid_csv": valid_csv,
            "invalid_csv": invalid_csv,
            "summary_csv": summary_csv,
        }
