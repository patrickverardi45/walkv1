from __future__ import annotations

import re
from typing import List, Optional, Tuple

from extractor.schemas import OCRCandidate


STATION_PATTERN = re.compile(r"^(\d{1,3})\+(\d{2})$")
DEPTH_PATTERN = re.compile(r"^\d{1,2}(?:\.\d{1,2})?$")
BOC_PATTERN = re.compile(r"^\d{1,2}$")


COMMON_TRANSLATIONS = str.maketrans(
    {
        "O": "0",
        "o": "0",
        "Q": "0",
        "D": "0",
        "I": "1",
        "l": "1",
        "|": "1",
        "S": "5",
        "s": "5",
        "Z": "2",
        "z": "2",
        "B": "8",
        ",": ".",
        " ": "",
        "—": "",
        "-": "",
        "_": "",
        ")": "",
        "(": "",
        "[": "",
        "]": "",
        "{": "",
        "}": "",
        "/": "",
        "\\": "",
        ":": "",
        ";": "",
        "'": "",
        '"': "",
    }
)



def _normalize_base(text: str) -> str:
    return text.strip().translate(COMMON_TRANSLATIONS)



def normalize_station_text(raw_text: str) -> Optional[str]:
    cleaned = _normalize_base(raw_text)
    cleaned = re.sub(r"[^0-9+]", "", cleaned)

    if not cleaned:
        return None

    if "+" not in cleaned:
        digits = re.sub(r"[^0-9]", "", cleaned)
        if len(digits) >= 3:
            # Prefer last two digits as offset.
            cleaned = f"{digits[:-2]}+{digits[-2:]}"
        else:
            return None
    else:
        pieces = [part for part in cleaned.split("+") if part]
        if len(pieces) >= 2:
            left = pieces[0]
            right = "".join(pieces[1:])
            cleaned = f"{left}+{right}"

    match = STATION_PATTERN.match(cleaned)
    if match:
        left, right = match.groups()
        return f"{int(left)}+{right.zfill(2)}"

    digits = re.sub(r"[^0-9]", "", cleaned)
    if len(digits) >= 3:
        left = str(int(digits[:-2]))
        right = digits[-2:].zfill(2)
        candidate = f"{left}+{right}"
        if STATION_PATTERN.match(candidate):
            return candidate

    return None



def normalize_depth_text(raw_text: str) -> Optional[float]:
    cleaned = _normalize_base(raw_text)
    cleaned = re.sub(r"[^0-9.]", "", cleaned)
    if cleaned.count(".") > 1:
        first = cleaned.find(".")
        cleaned = cleaned[: first + 1] + cleaned[first + 1 :].replace(".", "")
    if not cleaned:
        return None
    if cleaned.startswith("."):
        cleaned = f"0{cleaned}"
    if not DEPTH_PATTERN.match(cleaned):
        digits = re.sub(r"[^0-9]", "", cleaned)
        if len(digits) >= 2:
            cleaned = f"{digits[0]}.{digits[1:]}"
        else:
            return None
    try:
        value = float(cleaned)
    except ValueError:
        return None
    if value <= 0 or value > 40:
        return None
    return round(value, 2)



def normalize_boc_text(raw_text: str) -> Optional[int]:
    cleaned = _normalize_base(raw_text)
    cleaned = re.sub(r"[^0-9]", "", cleaned)
    if not cleaned:
        return None
    if not BOC_PATTERN.match(cleaned):
        return None
    value = int(cleaned)
    if value < 0 or value > 50:
        return None
    return value



def pick_best_station(candidates: List[OCRCandidate]) -> Tuple[Optional[str], str, float]:
    if not candidates:
        return None, "", 0.0
    for candidate in candidates:
        normalized = normalize_station_text(candidate.text)
        if normalized is not None:
            return normalized, candidate.text, candidate.confidence
    return None, candidates[0].text, candidates[0].confidence



def pick_best_depth(candidates: List[OCRCandidate]) -> Tuple[Optional[float], str, float]:
    if not candidates:
        return None, "", 0.0
    for candidate in candidates:
        normalized = normalize_depth_text(candidate.text)
        if normalized is not None:
            return normalized, candidate.text, candidate.confidence
    return None, candidates[0].text, candidates[0].confidence



def pick_best_boc(candidates: List[OCRCandidate]) -> Tuple[Optional[int], str, float]:
    if not candidates:
        return None, "", 0.0
    for candidate in candidates:
        normalized = normalize_boc_text(candidate.text)
        if normalized is not None:
            return normalized, candidate.text, candidate.confidence
    return None, candidates[0].text, candidates[0].confidence



def row_confidence(station_conf: float, depth_conf: float, boc_conf: float, station: Optional[str], depth: Optional[float], boc: Optional[int]) -> float:
    confidence = (station_conf + depth_conf + boc_conf) / 3.0
    if station is None:
        confidence *= 0.35
    if depth is None:
        confidence *= 0.65
    if boc is None:
        confidence *= 0.65
    return round(max(0.0, min(1.0, confidence)), 3)
