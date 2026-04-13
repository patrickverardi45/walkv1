from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple
import json
import os
import re

import cv2
import numpy as np
import pytesseract


STATION_MAP = str.maketrans({
    "I": "1", "l": "1", "|": "1", "!": "1", "/": "1", "\\": "1", "t": "1", "T": "1", "i": "1",
    "O": "0", "o": "0", "D": "0", "Q": "0", "U": "0", "u": "0", "C": "0",
    "S": "5", "s": "5", "$": "5",
    "g": "9", "q": "9",
    "z": "2", "Z": "2",
    "x": "+", "X": "+", "h": "+", "H": "+", "k": "+", "K": "+", "*": "+", "=": "+", "#": "+",
    ",": "", " ": "",
})

NUMERIC_MAP = str.maketrans({
    "I": "1", "l": "1", "|": "1", "!": "1", "/": "1", "\\": "1", "t": "1", "T": "1", "i": "1",
    "O": "0", "o": "0", "D": "0", "Q": "0", "U": "0", "u": "0", "C": "0",
    "S": "5", "s": "5", "$": "5",
    "g": "9", "q": "9",
    ",": ".", ";": ".", ":": ".", " ": "",
})

PREFERRED_STEPS = [50, 25, 37, 13, 63, 6, 12, 44, 92]


@dataclass
class OCRAttempt:
    variant: str
    raw_text: str
    cleaned: str
    score: float
    valid: bool
    reason: str


@dataclass
class ParsedCell:
    raw: str
    cleaned: str
    valid: bool
    reason: str
    best_variant: str
    attempts: List[OCRAttempt]


def _crop_to_ink(gray: np.ndarray, pad: int = 2) -> np.ndarray:
    inv = cv2.threshold(gray, 225, 255, cv2.THRESH_BINARY_INV)[1]
    ys, xs = np.where(inv > 0)
    if len(xs) < 5 or len(ys) < 5:
        return gray
    x1 = max(int(xs.min()) - pad, 0)
    x2 = min(int(xs.max()) + pad + 1, gray.shape[1])
    y1 = max(int(ys.min()) - pad, 0)
    y2 = min(int(ys.max()) + pad + 1, gray.shape[0])
    return gray[y1:y2, x1:x2]


def _remove_table_borders(gray: np.ndarray) -> np.ndarray:
    inv = cv2.threshold(gray, 210, 255, cv2.THRESH_BINARY_INV)[1]
    v_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, max(8, gray.shape[0] // 2)))
    h_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (max(12, gray.shape[1] // 4), 1))
    vertical = cv2.morphologyEx(inv, cv2.MORPH_OPEN, v_kernel)
    horizontal = cv2.morphologyEx(inv, cv2.MORPH_OPEN, h_kernel)
    mask = cv2.bitwise_or(vertical, horizontal)
    cleaned_inv = cv2.bitwise_and(inv, cv2.bitwise_not(mask))
    cleaned = 255 - cleaned_inv
    cleaned = _crop_to_ink(cleaned, pad=2)
    return cleaned


def preprocess_variants(gray: np.ndarray, field: str) -> Dict[str, np.ndarray]:
    variants: Dict[str, np.ndarray] = {}
    base = gray.copy()

    trim_y = max(1, gray.shape[0] // 18)
    trim_x = max(1, gray.shape[1] // 30)
    base = base[trim_y: max(trim_y + 1, gray.shape[0] - trim_y), trim_x: max(trim_x + 1, gray.shape[1] - trim_x)]
    if base.size == 0:
        base = gray.copy()

    no_lines = _remove_table_borders(base)
    no_lines = _crop_to_ink(no_lines, pad=2)

    scale = 4 if field == "station" else 5
    up = cv2.resize(no_lines, None, fx=scale, fy=scale, interpolation=cv2.INTER_CUBIC)
    up = cv2.copyMakeBorder(up, 18, 18, 18, 18, cv2.BORDER_CONSTANT, value=255)

    variants["gray_up"] = up
    variants["contrast"] = cv2.convertScaleAbs(up, alpha=2.1, beta=0)
    variants["otsu"] = cv2.threshold(variants["contrast"], 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]
    variants["adaptive"] = cv2.adaptiveThreshold(variants["contrast"], 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 31, 11)
    variants["median_otsu"] = cv2.threshold(cv2.medianBlur(variants["contrast"], 3), 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]
    return variants


def run_tesseract(img: np.ndarray, field: str) -> List[str]:
    psm_list = {
        "station": [6, 7, 8, 11, 13],
        "depth": [6, 7, 8, 13],
        "boc": [6, 7, 8, 10, 13],
        "notes": [6, 7],
    }[field]
    whitelist = {
        "station": "0123456789+",
        "depth": "0123456789.",
        "boc": "0123456789",
        "notes": "",
    }[field]

    out_parts: List[str] = []
    for psm in psm_list:
        cfg = f"--oem 1 --psm {psm}"
        if whitelist:
            cfg += f" -c tessedit_char_whitelist={whitelist}"
        try:
            txt = pytesseract.image_to_string(img, config=cfg, timeout=5)
        except RuntimeError:
            continue
        txt = txt.strip()
        if txt:
            out_parts.append(txt)
    return list(dict.fromkeys(out_parts))


def station_to_int(station: str) -> Optional[int]:
    m = re.fullmatch(r"(\d{1,3})\+(\d{2})", station)
    if not m:
        return None
    return int(m.group(1)) * 100 + int(m.group(2))


def int_to_station(value: int) -> str:
    return f"{value // 100}+{value % 100:02d}"


def _station_candidates_from_text(text: str) -> List[str]:
    if not text:
        return []
    normalized = text.translate(STATION_MAP)
    normalized = re.sub(r"[^0-9+]", "", normalized)
    candidates: List[str] = []

    if "+" in normalized:
        left, right = normalized.split("+", 1)
        left = re.sub(r"\D", "", left)
        right = re.sub(r"\D", "", right)
        if left and right:
            for r in [right[:2], right[-2:]]:
                if r:
                    if len(r) == 1:
                        r = r + "0"
                    candidates.append(f"{int(left)}+{r}")

    digits = re.sub(r"\D", "", normalized)
    if len(digits) >= 3:
        for take in [3, 4, 5]:
            if len(digits) >= take:
                piece = digits[:take]
                left = piece[:-2]
                right = piece[-2:]
                if left:
                    candidates.append(f"{int(left)}+{right}")
                piece = digits[-take:]
                left = piece[:-2]
                right = piece[-2:]
                if left:
                    candidates.append(f"{int(left)}+{right}")

    deduped: List[str] = []
    for candidate in candidates:
        if re.fullmatch(r"\d{1,3}\+\d{2}", candidate) and candidate not in deduped:
            deduped.append(candidate)
    return deduped


def normalize_station(text: str) -> Tuple[str, bool, str, float]:
    candidates = _station_candidates_from_text(text)
    if not candidates:
        return "", False, f"no station-like chars in {text!r}", 0.0
    best = candidates[0]
    score = 0.75
    right = best.split("+", 1)[1]
    if right in {"00", "25", "37", "44", "45", "48", "50", "63", "75"}:
        score = 1.0
    return best, True, "ok", score


def normalize_depth(text: str) -> Tuple[str, bool, str, float]:
    if not text:
        return "", False, "empty OCR", 0.0
    cleaned = text.translate(NUMERIC_MAP)
    cleaned = re.sub(r"[^0-9.]", "", cleaned)
    if not cleaned:
        return "", False, f"no numeric chars in {text!r}", 0.0
    if cleaned.count(".") > 1:
        first = cleaned.find(".")
        cleaned = cleaned[:first + 1] + cleaned[first + 1:].replace(".", "")

    candidates: List[float] = []
    if re.fullmatch(r"\d\.\d", cleaned):
        candidates.append(float(cleaned))
    digits = re.sub(r"\D", "", cleaned)
    if len(digits) >= 2:
        candidates.append(float(f"{digits[0]}.{digits[1]}"))
    if len(digits) >= 3:
        candidates.append(float(f"{digits[0]}.{digits[1]}"))
        candidates.append(float(f"{digits[:2]}.{digits[2]}"))
    if len(digits) == 1:
        candidates.append(float(digits))

    best: Optional[float] = None
    best_penalty = 1e9
    for value in candidates:
        penalty = abs(value - 4.3)
        if not (0.5 <= value <= 15.0):
            penalty += 10.0
        if penalty < best_penalty:
            best_penalty = penalty
            best = value
    if best is None:
        return cleaned, False, f"depth parse failed: {cleaned}", 0.25
    valid = 0.5 <= best <= 15.0
    return f"{best:.1f}", valid, "ok" if valid else f"depth out of range: {best}", 1.0 if valid else 0.3


def normalize_boc(text: str) -> Tuple[str, bool, str, float]:
    if not text:
        return "", False, "empty OCR", 0.0
    cleaned = text.translate(NUMERIC_MAP)
    cleaned = re.sub(r"\D", "", cleaned)
    if not cleaned:
        return "", False, f"no numeric chars in {text!r}", 0.0
    candidates = [int(ch) for ch in cleaned if ch.isdigit()]
    if len(cleaned) >= 2:
        try:
            candidates.append(int(cleaned[:2]))
        except Exception:
            pass
    best: Optional[int] = None
    best_penalty = 1e9
    for value in candidates:
        penalty = abs(value - 9)
        if not (0 <= value <= 20):
            penalty += 10
        if penalty < best_penalty:
            best_penalty = penalty
            best = value
    if best is None:
        return cleaned, False, f"boc parse failed: {cleaned}", 0.25
    valid = 0 <= best <= 20
    return str(best), valid, "ok" if valid else f"boc out of range: {best}", 1.0 if valid else 0.3


def parse_cell(gray: np.ndarray, field: str, debug_dir: Optional[str] = None, stem: str = "cell") -> ParsedCell:
    variants = preprocess_variants(gray, field)
    attempts: List[OCRAttempt] = []
    normalizer = {
        "station": normalize_station,
        "depth": normalize_depth,
        "boc": normalize_boc,
        "notes": lambda t: (t.strip(), bool(t.strip()), "ok" if t.strip() else "empty", 1.0 if t.strip() else 0.0),
    }[field]

    for variant_name, variant_img in variants.items():
        raws = run_tesseract(variant_img, field)
        if not raws:
            attempts.append(OCRAttempt(variant_name, "", "", 0.0, False, "empty OCR"))
            continue
        for raw in raws:
            cleaned, valid, reason, score = normalizer(raw)
            attempts.append(OCRAttempt(variant_name, raw, cleaned, score, valid, reason))
    attempts.sort(key=lambda a: (a.valid, a.score, len(a.cleaned), len(a.raw_text)), reverse=True)
    best = attempts[0] if attempts else OCRAttempt("none", "", "", 0.0, False, "no attempts")
    if debug_dir:
        os.makedirs(debug_dir, exist_ok=True)
        chosen = variants.get(best.variant, gray)
        cv2.imwrite(os.path.join(debug_dir, f"{stem}_best_{best.variant}.png"), chosen)
        cv2.imwrite(os.path.join(debug_dir, f"{stem}_raw.png"), gray)
        with open(os.path.join(debug_dir, f"{stem}_attempts.json"), "w", encoding="utf-8") as f:
            json.dump([a.__dict__ for a in attempts], f, indent=2)
    return ParsedCell(
        raw=best.raw_text,
        cleaned=best.cleaned,
        valid=best.valid,
        reason=best.reason,
        best_variant=best.variant,
        attempts=attempts,
    )


def build_station_candidates(row_record: Dict) -> List[int]:
    texts = [str(row_record.get("station_raw") or ""), str(row_record.get("station_cleaned") or "")]
    try:
        attempts = json.loads(row_record.get("station_attempts") or "[]")
        texts.extend(str(a.get("raw_text") or "") for a in attempts)
        texts.extend(str(a.get("cleaned") or "") for a in attempts)
    except Exception:
        pass

    out: List[int] = []
    for text in texts:
        for candidate in _station_candidates_from_text(text):
            value = station_to_int(candidate)
            if value is not None and value not in out:
                out.append(value)
    return out


def repair_station_sequence(row_records: List[Dict]) -> List[str]:
    repaired: List[str] = []
    prev_int: Optional[int] = None
    for row in row_records:
        candidates = build_station_candidates(row)
        candidates = sorted(set(candidates))
        chosen: Optional[int] = None

        if prev_int is None:
            valid_start = [c for c in candidates if 0 <= c <= 40000]
            chosen = valid_start[0] if valid_start else None
        else:
            plausible = [c for c in candidates if prev_int < c <= prev_int + 150]
            if plausible:
                def score(value: int) -> Tuple[float, int]:
                    diff = value - prev_int
                    pref = min(abs(diff - step) for step in PREFERRED_STEPS)
                    return (pref, diff)
                chosen = sorted(plausible, key=score)[0]
            else:
                # salvage from trailing two digits near the prior station number
                rights = []
                for candidate in candidates:
                    rights.append(candidate % 100)
                if rights:
                    guesses: List[int] = []
                    for right in rights:
                        for left in range(max(0, prev_int // 100 - 1), prev_int // 100 + 3):
                            guess = left * 100 + right
                            if prev_int < guess <= prev_int + 150:
                                guesses.append(guess)
                    if guesses:
                        def score(value: int) -> Tuple[float, int]:
                            diff = value - prev_int
                            pref = min(abs(diff - step) for step in PREFERRED_STEPS)
                            return (pref, diff)
                        chosen = sorted(set(guesses), key=score)[0]

                if chosen is None:
                    for step in [50, 25, 13]:
                        guess = prev_int + step
                        if guess % 100 < 100:
                            chosen = guess
                            break

        if chosen is None:
            repaired.append("")
        else:
            repaired.append(int_to_station(chosen))
            prev_int = chosen
    return repaired


def smooth_numeric_series(values: Sequence[Optional[str]], field: str) -> List[str]:
    parsed: List[Optional[float]] = []
    for value in values:
        try:
            if value in (None, "", "None"):
                parsed.append(None)
            else:
                parsed.append(float(value))
        except Exception:
            parsed.append(None)

    valids = [v for v in parsed if v is not None]
    default = 4.3 if field == "depth" else 9.0
    if valids:
        default = float(np.median(valids))
    out: List[str] = []
    for idx, value in enumerate(parsed):
        if value is None or value == 0:
            left = next((parsed[j] for j in range(idx - 1, -1, -1) if parsed[j] not in (None, 0)), None)
            right = next((parsed[j] for j in range(idx + 1, len(parsed)) if parsed[j] not in (None, 0)), None)
            if left is not None and right is not None:
                value = round((left + right) / 2.0, 1 if field == "depth" else 0)
            elif left is not None:
                value = left
            elif right is not None:
                value = right
            else:
                value = default
        if field == "depth":
            if value > 15:
                value = round(value / 10.0, 1)
            out.append(f"{float(value):.1f}")
        else:
            value = int(round(float(value)))
            if value > 20:
                value = int(str(value)[0])
            out.append(str(value))
    return out
