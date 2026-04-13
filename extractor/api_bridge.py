from __future__ import annotations

import os
import shutil
import tempfile
from typing import Dict, List, Tuple

import pandas as pd
from pandas.errors import EmptyDataError

from .pipeline import BoreLogOCRPipeline


def _safe_csv_to_records(path: str) -> List[dict]:
    if not path or not os.path.exists(path):
        return []
    try:
        if os.path.getsize(path) == 0:
            return []
        df = pd.read_csv(path)
        if df is None or df.empty:
            return []
        return df.fillna("").to_dict(orient="records")
    except EmptyDataError:
        return []
    except Exception:
        return []


def run_uploaded_files_to_rows(uploaded_files: List[Tuple[str, bytes]]) -> Dict[str, object]:
    work_root = tempfile.mkdtemp(prefix="bore_ocr_")
    input_root = os.path.join(work_root, "input")
    output_root = os.path.join(work_root, "output")
    os.makedirs(input_root, exist_ok=True)
    os.makedirs(output_root, exist_ok=True)

    for name, payload in uploaded_files:
        safe_name = os.path.basename(name) or "upload.bin"
        with open(os.path.join(input_root, safe_name), "wb") as f:
            f.write(payload)

    pipeline = BoreLogOCRPipeline(input_root=input_root, output_root=output_root)
    outputs = pipeline.process()

    candidates_csv = outputs.get("candidates_csv")
    invalid_csv = outputs.get("invalid_csv")
    summary_csv = outputs.get("summary_csv")

    rows: List[dict] = _safe_csv_to_records(candidates_csv)
    invalid_rows: List[dict] = _safe_csv_to_records(invalid_csv)
    file_summary: List[dict] = _safe_csv_to_records(summary_csv)

    if not file_summary:
        for name, _payload in uploaded_files:
            file_summary.append(
                {
                    "source_file": os.path.basename(name) or "upload.bin",
                    "candidate_rows": len(rows),
                    "invalid_rows": len(invalid_rows),
                    "status": "no_summary_csv",
                }
            )

    return {
        "rows": rows,
        "invalid_rows": invalid_rows,
        "file_summary": file_summary,
        "debug_root": os.path.join(output_root, "debug"),
        "work_root": work_root,
    }
