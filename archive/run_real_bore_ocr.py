from __future__ import annotations

import argparse
import os
import shutil
import zipfile

from extractor import BoreLogOCRPipeline


def ensure_unzipped(zip_path: str, dest_dir: str) -> str:
    extract_root = os.path.join(dest_dir, "input_files")
    if os.path.exists(extract_root):
        shutil.rmtree(extract_root)
    os.makedirs(extract_root, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_root)
    children = [os.path.join(extract_root, x) for x in os.listdir(extract_root)]
    folder = next((c for c in children if os.path.isdir(c)), extract_root)
    return folder


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--zip", required=True, help="Path to the uploaded bore-log zip")
    parser.add_argument("--out", required=True, help="Output directory")
    parser.add_argument("--limit", type=int, default=0, help="Optional file limit for quick sprint iterations")
    args = parser.parse_args()

    os.makedirs(args.out, exist_ok=True)
    input_root = ensure_unzipped(args.zip, args.out)
    pipeline = BoreLogOCRPipeline(input_root=input_root, output_root=args.out)
    if args.limit:
        original_iter = pipeline._iter_files
        pipeline._iter_files = lambda: original_iter()[: args.limit]
    outputs = pipeline.process()
    print("DONE")
    for name, path in outputs.items():
        print(f"{name}: {path}")


if __name__ == "__main__":
    main()
