from __future__ import annotations

import argparse
from pathlib import Path

from extractor.pipeline import BoreLogExtractor, write_csv, write_json


def main() -> None:
    parser = argparse.ArgumentParser(description="Run handwritten bore log extraction.")
    parser.add_argument("input_path", help="Path to bore log image or PDF")
    parser.add_argument("--output-dir", default="outputs/bore_extraction", help="Directory for CSV/JSON output")
    parser.add_argument("--debug-dir", default=None, help="Optional directory for debug images")
    parser.add_argument("--poppler-path", default=None, help="Optional Poppler path for PDF support")
    args = parser.parse_args()

    extractor = BoreLogExtractor(poppler_path=args.poppler_path, debug_root=args.debug_dir)
    results = extractor.extract_file(args.input_path)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    stem = Path(args.input_path).stem
    csv_path = output_dir / f"{stem}_extracted.csv"
    json_path = output_dir / f"{stem}_extracted.json"

    write_csv(results, str(csv_path))
    write_json(results, str(json_path))

    total_rows = sum(result.total_rows for result in results)
    valid_rows = sum(result.valid_rows for result in results)

    print(f"[bore_extract] wrote CSV: {csv_path}")
    print(f"[bore_extract] wrote JSON: {json_path}")
    print(f"[bore_extract] total_rows={total_rows} valid_rows={valid_rows}")


if __name__ == "__main__":
    main()
