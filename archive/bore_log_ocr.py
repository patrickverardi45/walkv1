from __future__ import annotations

import io
import json
import os
import re
import tempfile
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import cv2
import numpy as np
import pytesseract
from pdf2image import convert_from_bytes
from PIL import Image


STATION_TRANS = str.maketrans({
    "O": "0", "o": "0", "Q": "0", "D": "0", "U": "0", "u": "0", "C": "0",
    "I": "1", "l": "1", "|": "1", "!": "1", "/": "1", "\\": "1", "T": "1", "t": "1",
    "S": "5", "s": "5", "$": "5",
    "Z": "2", "z": "2",
    "G": "6", "b": "6",
    "q": "9", "g": "9",
    "x": "+", "X": "+", "*": "+", "=": "+", "#": "+",
    " ": "", ",": "", ".": "", ";": "", ":": "", "'": "", '"': "",
    "-": "", "_": "", "(": "", ")": "", "[": "", "]": "",
})

NUMERIC_TRANS = str.maketrans({
    "O": "0", "o": "0", "Q": "0", "D": "0",
    "I": "1", "l": "1", "|": "1", "!": "1", "/": "1", "\\": "1",
    "S": "5", "s": "5", "$": "5",
    "Z": "2", "z": "2",
    ",": ".", ":": ".", ";": ".",
    " ": "",
})


@dataclass
class OCRAttempt:
    variant: str
    raw_text: str
    cleaned_value: str
    valid: bool
    score: float
    rejection_reason: str


@dataclass
class ParsedValue:
    raw_text: str
    cleaned_value: str
    valid: bool
    score: float
    rejection_reason: str
    best_variant: str
    attempts: List[OCRAttempt]


def _ensure_tesseract() -> None:
    try:
        pytesseract.get_tesseract_version()
    except Exception as exc:
        raise RuntimeError("tesseract is not installed or not available in PATH") from exc


def _load_images(file_bytes: bytes, filename: str) -> List[np.ndarray]:
    lower = filename.lower()
    if lower.endswith('.pdf'):
        pages = convert_from_bytes(file_bytes, dpi=260)
        out: List[np.ndarray] = []
        for page in pages:
            rgb = np.array(page.convert('RGB'))
            out.append(cv2.cvtColor(rgb, cv2.COLOR_RGB2BGR))
        return out
    img = Image.open(io.BytesIO(file_bytes)).convert('RGB')
    return [cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)]


def _to_gray(image_bgr: np.ndarray) -> np.ndarray:
    return cv2.cvtColor(image_bgr, cv2.COLOR_BGR2GRAY)


def _find_tables(gray: np.ndarray) -> List[Tuple[int, int, int, int]]:
    inv = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]
    h, w = gray.shape[:2]
    vert = cv2.morphologyEx(inv, cv2.MORPH_OPEN, cv2.getStructuringElement(cv2.MORPH_RECT, (1, max(40, h // 25))))
    horz = cv2.morphologyEx(inv, cv2.MORPH_OPEN, cv2.getStructuringElement(cv2.MORPH_RECT, (max(50, w // 20), 1)))
    grid = cv2.bitwise_or(vert, horz)
    contours, _ = cv2.findContours(grid, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    boxes: List[Tuple[int, int, int, int]] = []
    for c in contours:
        x, y, ww, hh = cv2.boundingRect(c)
        if ww < w * 0.12 or hh < h * 0.22:
            continue
        if ww > w * 0.42 or hh > h * 0.82:
            continue
        aspect = ww / max(hh, 1)
        if aspect > 0.6:
            continue
        boxes.append((x, y, ww, hh))
    boxes.sort(key=lambda b: b[0])
    deduped: List[Tuple[int, int, int, int]] = []
    for box in boxes:
        if not deduped:
            deduped.append(box)
            continue
        px, py, pw, ph = deduped[-1]
        if abs(box[0] - px) < 15 and abs(box[1] - py) < 30:
            if box[2] * box[3] > pw * ph:
                deduped[-1] = box
        else:
            deduped.append(box)
    return deduped[:3]


def _group_line_positions(mask: np.ndarray, axis: int, ratio: float = 0.30) -> List[Tuple[int, int]]:
    proj = mask.sum(axis=axis)
    if proj.size == 0 or proj.max() <= 0:
        return []
    idx = np.where(proj > proj.max() * ratio)[0]
    if len(idx) == 0:
        return []
    groups: List[Tuple[int, int]] = []
    start = int(idx[0])
    prev = int(idx[0])
    for v in idx[1:]:
        v = int(v)
        if v - prev > 2:
            groups.append((start, prev))
            start = v
        prev = v
    groups.append((start, prev))
    return groups


def _tight_trim(gray: np.ndarray) -> np.ndarray:
    if gray.size == 0:
        return gray
    bin_inv = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]
    pts = cv2.findNonZero(bin_inv)
    if pts is None:
        return gray
    x, y, w, h = cv2.boundingRect(pts)
    pad = 4
    x1 = max(0, x - pad)
    y1 = max(0, y - pad)
    x2 = min(gray.shape[1], x + w + pad)
    y2 = min(gray.shape[0], y + h + pad)
    return gray[y1:y2, x1:x2]


def _cell_variants(cell_gray: np.ndarray) -> Dict[str, np.ndarray]:
    variants: Dict[str, np.ndarray] = {}
    base = _tight_trim(cell_gray)
    if base.size == 0:
        base = cell_gray
    base = cv2.copyMakeBorder(base, 12, 12, 12, 12, cv2.BORDER_CONSTANT, value=255)
    up = cv2.resize(base, None, fx=4.0, fy=4.0, interpolation=cv2.INTER_CUBIC)
    variants['gray_x4'] = up

    denoise = cv2.fastNlMeansDenoising(up, None, 12, 7, 21)
    variants['denoise_x4'] = denoise

    clahe = cv2.createCLAHE(clipLimit=2.5, tileGridSize=(8, 8)).apply(denoise)
    variants['clahe_x4'] = clahe

    adaptive = cv2.adaptiveThreshold(clahe, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 31, 9)
    variants['adaptive_x4'] = adaptive

    otsu = cv2.threshold(clahe, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]
    variants['otsu_x4'] = otsu

    morph = cv2.morphologyEx(otsu, cv2.MORPH_CLOSE, cv2.getStructuringElement(cv2.MORPH_RECT, (2, 2)))
    variants['morph_x4'] = morph
    return variants


def _run_tesseract(image: np.ndarray, field: str, psm: int) -> str:
    whitelist = {
        'station': '0123456789+',
        'depth': '0123456789.',
        'boc': '0123456789',
        'notes': '',
    }[field]
    cfg = f'--oem 1 --psm {psm}'
    if whitelist:
        cfg += f' -c tessedit_char_whitelist={whitelist}'
    return pytesseract.image_to_string(image, config=cfg).strip()


def _normalize_station(text: str) -> Tuple[str, bool, str, float]:
    cleaned = text.translate(STATION_TRANS)
    cleaned = re.sub(r'[^0-9+]', '', cleaned)
    if not cleaned:
        return '', False, 'empty OCR', 0.0
    if '+' not in cleaned:
        digits = re.sub(r'\D', '', cleaned)
        if len(digits) < 3:
            return digits, False, 'too few digits for station', 0.15
        cleaned = f"{digits[:-2]}+{digits[-2:]}"
    else:
        left, right = cleaned.split('+', 1)
        left = re.sub(r'\D', '', left)
        right = re.sub(r'\D', '', right)
        if len(right) > 2:
            right = right[-2:]
        cleaned = f'{left}+{right}'
    m = re.fullmatch(r'(\d{1,3})\+(\d{1,2})', cleaned)
    if not m:
        return cleaned, False, 'station regex failed', 0.25
    major = int(m.group(1))
    minor = int(m.group(2))
    if minor > 99:
        return cleaned, False, 'station minor > 99', 0.25
    score = 0.85
    if minor in {0, 13, 25, 45, 50, 69, 75, 94}:
        score += 0.1
    return f'{major}+{minor:02d}', True, 'ok', min(score, 1.0)


def _normalize_depth(text: str) -> Tuple[str, bool, str, float]:
    cleaned = text.translate(NUMERIC_TRANS)
    cleaned = re.sub(r'[^0-9.]', '', cleaned)
    if not cleaned:
        return '', False, 'empty OCR', 0.0
    if cleaned.count('.') > 1:
        first = cleaned.find('.')
        cleaned = cleaned[:first + 1] + cleaned[first + 1:].replace('.', '')
    if '.' not in cleaned and len(cleaned) >= 2:
        cleaned = cleaned[0] + '.' + cleaned[1:]
    m = re.search(r'\d+(?:\.\d+)?', cleaned)
    if not m:
        return cleaned, False, 'depth parse failed', 0.2
    value = float(m.group(0))
    if value > 20:
        value = value / 10.0
    if not (0.5 <= value <= 12.0):
        return f'{value:.1f}', False, 'depth out of range', 0.35
    return f'{value:.1f}', True, 'ok', 0.95


def _normalize_boc(text: str) -> Tuple[str, bool, str, float]:
    cleaned = text.translate(NUMERIC_TRANS)
    cleaned = re.sub(r'\D', '', cleaned)
    if not cleaned:
        return '', False, 'empty OCR', 0.0
    value = int(cleaned[0])
    if not (0 <= value <= 12):
        return str(value), False, 'boc out of range', 0.25
    return str(value), True, 'ok', 0.95


def _parse_cell(cell_gray: np.ndarray, field: str, debug_dir: str, stem: str) -> ParsedValue:
    attempts: List[OCRAttempt] = []
    variants = _cell_variants(cell_gray)
    normalizer = {
        'station': _normalize_station,
        'depth': _normalize_depth,
        'boc': _normalize_boc,
        'notes': lambda t: (t.strip(), bool(t.strip()), 'ok' if t.strip() else 'empty OCR', 0.5 if t.strip() else 0.0),
    }[field]
    psms = {'station': [7, 8, 13], 'depth': [7, 8], 'boc': [10, 8, 7], 'notes': [6, 7]}[field]

    os.makedirs(debug_dir, exist_ok=True)
    cv2.imwrite(os.path.join(debug_dir, f'{stem}_raw.png'), cell_gray)
    for variant_name, variant_img in variants.items():
        cv2.imwrite(os.path.join(debug_dir, f'{stem}_{variant_name}.png'), variant_img)
        for psm in psms:
            raw = _run_tesseract(variant_img, field, psm)
            cleaned, valid, reason, score = normalizer(raw)
            attempts.append(OCRAttempt(f'{variant_name}_psm{psm}', raw, cleaned, valid, score, reason))
    attempts.sort(key=lambda a: (a.valid, a.score, len(a.cleaned_value)), reverse=True)
    best = attempts[0] if attempts else OCRAttempt('none', '', '', False, 0.0, 'no attempts')
    return ParsedValue(best.raw_text, best.cleaned_value, best.valid, best.score, best.rejection_reason, best.variant, attempts)


def _station_to_int(value: str) -> Optional[int]:
    m = re.fullmatch(r'(\d{1,3})\+(\d{2})', value or '')
    if not m:
        return None
    return int(m.group(1)) * 100 + int(m.group(2))


def _smooth_station_sequence(values: Sequence[str]) -> List[str]:
    ints = [_station_to_int(v) for v in values]
    valid = [x for x in ints if x is not None]
    if not valid:
        return list(values)
    direction = -1 if len(valid) >= 2 and valid[0] > valid[-1] else 1
    out = list(values)
    for i, current in enumerate(ints):
        if current is not None:
            continue
        prev_idx = next((j for j in range(i - 1, -1, -1) if ints[j] is not None), None)
        next_idx = next((j for j in range(i + 1, len(ints)) if ints[j] is not None), None)
        guess = None
        if prev_idx is not None and next_idx is not None:
            prev_val = ints[prev_idx]
            next_val = ints[next_idx]
            if prev_val is not None and next_val is not None:
                step = int(round((next_val - prev_val) / max(next_idx - prev_idx, 1)))
                if abs(step) < 10 or abs(step) > 200:
                    step = 50 * direction
                guess = prev_val + step * (i - prev_idx)
        elif prev_idx is not None and ints[prev_idx] is not None:
            guess = ints[prev_idx] + 50 * direction
        elif next_idx is not None and ints[next_idx] is not None:
            guess = ints[next_idx] - 50 * direction
        if guess is not None:
            out[i] = f'{guess // 100}+{guess % 100:02d}'
            ints[i] = guess
    return out


def _extract_rows_from_table(table_gray: np.ndarray, source_file: str, source_page: int, table_index: int, debug_root: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    inv = cv2.threshold(table_gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]
    h, w = table_gray.shape[:2]
    vert = cv2.morphologyEx(inv, cv2.MORPH_OPEN, cv2.getStructuringElement(cv2.MORPH_RECT, (1, max(20, h // 20))))
    horz = cv2.morphologyEx(inv, cv2.MORPH_OPEN, cv2.getStructuringElement(cv2.MORPH_RECT, (max(30, w // 5), 1)))
    row_lines = _group_line_positions(horz, axis=1, ratio=0.30)
    col_lines = _group_line_positions(vert, axis=0, ratio=0.30)
    rows_out: List[Dict[str, Any]] = []
    invalid: List[Dict[str, Any]] = []
    if len(row_lines) < 4 or len(col_lines) < 4:
        invalid.append({
            'source_file': source_file,
            'source_page': source_page,
            'table_index': table_index,
            'row_index': 0,
            'rejection_reason': 'table grid not detected cleanly',
        })
        return rows_out, invalid

    col_bounds = [
        (col_lines[0][1] + 6, col_lines[1][0] - 6),
        (col_lines[1][1] + 6, col_lines[2][0] - 6),
        (col_lines[2][1] + 6, col_lines[3][0] - 6),
    ]
    data_rows = []
    for idx in range(1, len(row_lines) - 1):
        y1 = row_lines[idx][1] + 4
        y2 = row_lines[idx + 1][0] - 4
        if y2 <= y1:
            continue
        if y2 - y1 < max(18, h // 60):
            continue
        data_rows.append((idx, y1, y2))

    blank_streak = 0
    for visual_idx, y1, y2 in data_rows:
        row_img = table_gray[y1:y2, :]
        ink = np.count_nonzero(cv2.threshold(row_img, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1])
        if ink < max(40, row_img.size // 80):
            blank_streak += 1
            invalid.append({
                'source_file': source_file,
                'source_page': source_page,
                'table_index': table_index,
                'row_index': visual_idx,
                'rejection_reason': 'row looked blank',
            })
            continue
        blank_streak = 0
        row_debug = os.path.join(debug_root, Path(source_file).stem, f'page_{source_page}', f'table_{table_index}', f'row_{visual_idx:02d}')
        os.makedirs(row_debug, exist_ok=True)
        cv2.imwrite(os.path.join(row_debug, 'row.png'), row_img)

        station_cell = table_gray[y1:y2, col_bounds[0][0]:col_bounds[0][1]]
        depth_cell = table_gray[y1:y2, col_bounds[1][0]:col_bounds[1][1]]
        boc_cell = table_gray[y1:y2, col_bounds[2][0]:col_bounds[2][1]]
        station = _parse_cell(station_cell, 'station', row_debug, 'station')
        depth = _parse_cell(depth_cell, 'depth', row_debug, 'depth')
        boc = _parse_cell(boc_cell, 'boc', row_debug, 'boc')
        record = {
            'source_file': source_file,
            'source_page': source_page,
            'table_index': table_index,
            'row_index': visual_idx,
            'station_raw': station.raw_text,
            'station_cleaned': station.cleaned_value,
            'station_valid': station.valid,
            'station_reason': station.rejection_reason,
            'best_station_variant': station.best_variant,
            'station_attempts': json.dumps([asdict(a) for a in station.attempts], ensure_ascii=False),
            'depth_raw': depth.raw_text,
            'depth_cleaned': depth.cleaned_value,
            'depth_valid': depth.valid,
            'depth_reason': depth.rejection_reason,
            'best_depth_variant': depth.best_variant,
            'depth_attempts': json.dumps([asdict(a) for a in depth.attempts], ensure_ascii=False),
            'boc_raw': boc.raw_text,
            'boc_cleaned': boc.cleaned_value,
            'boc_valid': boc.valid,
            'boc_reason': boc.rejection_reason,
            'best_boc_variant': boc.best_variant,
            'boc_attempts': json.dumps([asdict(a) for a in boc.attempts], ensure_ascii=False),
            'notes_raw': '',
            'notes_cleaned': '',
        }
        rows_out.append(record)
    return rows_out, invalid


def _dedupe_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    for row in rows:
        key = (row.get('source_file'), row.get('start_station'), row.get('end_station'), row.get('depth_ft'), row.get('boc_ft'))
        existing = deduped.get(key)
        if existing is None or float(row.get('confidence', 0.0)) > float(existing.get('confidence', 0.0)):
            deduped[key] = row
    return list(deduped.values())


def ocr_bore_log_files(file_items: List[Tuple[str, bytes]], debug_root: Optional[str] = None) -> Tuple[List[Dict[str, Any]], str, Dict[str, Any]]:
    _ensure_tesseract()
    if debug_root is None:
        debug_root = tempfile.mkdtemp(prefix='bore_ocr_debug_')
    else:
        os.makedirs(debug_root, exist_ok=True)

    all_valid: List[Dict[str, Any]] = []
    all_invalid: List[Dict[str, Any]] = []
    preview_parts: List[str] = []
    file_summaries: List[Dict[str, Any]] = []

    for filename, file_bytes in file_items:
        pages = _load_images(file_bytes, filename)
        file_valid = 0
        file_invalid = 0
        preview_parts.append(f'FILE: {filename}')
        for page_idx, page_bgr in enumerate(pages, start=1):
            gray = _to_gray(page_bgr)
            tables = _find_tables(gray)
            if not tables:
                all_invalid.append({'source_file': filename, 'source_page': page_idx, 'rejection_reason': 'no bore-log table detected'})
                file_invalid += 1
                continue
            page_rows: List[Dict[str, Any]] = []
            for table_idx, (x, y, w, h) in enumerate(tables, start=1):
                table_gray = gray[y:y+h, x:x+w]
                cv2.imwrite(os.path.join(debug_root, Path(filename).stem + f'_page{page_idx}_table{table_idx}.png'), table_gray)
                rows, invalid = _extract_rows_from_table(table_gray, filename, page_idx, table_idx, debug_root)
                page_rows.extend(rows)
                all_invalid.extend(invalid)
                file_invalid += len(invalid)
            if not page_rows:
                continue
            smoothed = _smooth_station_sequence([r.get('station_cleaned', '') for r in page_rows])
            for row, smooth in zip(page_rows, smoothed):
                row['station_smoothed'] = smooth
            page_rows = [r for r in page_rows if r.get('station_smoothed') and r.get('depth_cleaned') and r.get('boc_cleaned')]
            for idx in range(len(page_rows) - 1):
                cur = page_rows[idx]
                nxt = page_rows[idx + 1]
                s = _station_to_int(cur.get('station_smoothed', ''))
                e = _station_to_int(nxt.get('station_smoothed', ''))
                if s is None or e is None:
                    row = dict(cur)
                    row['rejection_reason'] = 'missing station after smoothing'
                    all_invalid.append(row)
                    file_invalid += 1
                    continue
                start, end = sorted((s, e))
                if start == end:
                    row = dict(cur)
                    row['rejection_reason'] = 'start_station >= end_station'
                    all_invalid.append(row)
                    file_invalid += 1
                    continue
                valid = {
                    'start_station': f'{start // 100}+{start % 100:02d}',
                    'end_station': f'{end // 100}+{end % 100:02d}',
                    'depth_ft': float(cur['depth_cleaned']),
                    'boc_ft': float(cur['boc_cleaned']),
                    'reason': '',
                    'notes': '',
                    'station': cur.get('station_smoothed', ''),
                    'source_file': filename,
                    'confidence': round((0.6 if cur.get('station_valid') else 0.35) + 0.2 + 0.2, 3),
                    'source_page': page_idx,
                    'row_index': cur.get('row_index'),
                    'station_raw': cur.get('station_raw', ''),
                    'station_cleaned': cur.get('station_cleaned', ''),
                    'station_smoothed': cur.get('station_smoothed', ''),
                    'depth_raw': cur.get('depth_raw', ''),
                    'boc_raw': cur.get('boc_raw', ''),
                }
                all_valid.append(valid)
                file_valid += 1
                preview_parts.append(f"{filename} p{page_idx}: {valid['start_station']} -> {valid['end_station']} depth={valid['depth_ft']} boc={valid['boc_ft']}")
        file_summaries.append({'source_file': filename, 'valid_segments': file_valid, 'invalid_rows': file_invalid})

    all_valid = _dedupe_rows(all_valid)
    diagnostics = {
        'debug_root': debug_root,
        'file_summary': file_summaries,
        'invalid_rows': all_invalid,
    }
    return sorted(all_valid, key=lambda r: (_station_to_int(r.get('start_station', '')) or 999999)), '\n'.join(preview_parts)[:4000], diagnostics
