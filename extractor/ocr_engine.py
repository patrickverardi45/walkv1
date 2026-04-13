from __future__ import annotations

from typing import Dict, List, Tuple

import cv2
import numpy as np
import pytesseract

from extractor.schemas import OCRCandidate


FIELD_CONFIGS: Dict[str, str] = {
    "station": "--oem 3 --psm 7 -c tessedit_char_whitelist=0123456789+OSIlsobzgZ",
    "depth": "--oem 3 --psm 7 -c tessedit_char_whitelist=0123456789.,OSIlsobzgZ",
    "boc": "--oem 3 --psm 10 -c tessedit_char_whitelist=0123456789OSIlsobzgZ",
}



def _build_variants(cell_gray: np.ndarray) -> List[Tuple[str, np.ndarray]]:
    variants: List[Tuple[str, np.ndarray]] = []

    # Original resized.
    resized = cv2.resize(cell_gray, None, fx=4.0, fy=4.0, interpolation=cv2.INTER_CUBIC)
    variants.append(("resized", resized))

    # Binary.
    binary = cv2.threshold(resized, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]
    variants.append(("binary", binary))

    # Light close to connect pen strokes.
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (2, 2))
    closed = cv2.morphologyEx(binary, cv2.MORPH_CLOSE, kernel)
    variants.append(("closed", closed))

    # Slight blur then threshold.
    blurred = cv2.GaussianBlur(resized, (3, 3), 0)
    blurred_binary = cv2.threshold(blurred, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]
    variants.append(("blurred_binary", blurred_binary))

    return variants



def _extract_confidence(tsv_data: Dict[str, List[str]]) -> float:
    confidences: List[float] = []
    for raw_conf in tsv_data.get("conf", []):
        try:
            value = float(raw_conf)
        except Exception:
            continue
        if value >= 0:
            confidences.append(value)
    if not confidences:
        return 0.0
    return max(0.0, min(1.0, sum(confidences) / len(confidences) / 100.0))



def _read_single_variant(image: np.ndarray, field_name: str) -> OCRCandidate:
    config = FIELD_CONFIGS[field_name]
    text = pytesseract.image_to_string(image, config=config).strip()
    tsv_data = pytesseract.image_to_data(
        image,
        config=config,
        output_type=pytesseract.Output.DICT,
    )
    confidence = _extract_confidence(tsv_data)
    return OCRCandidate(text=text, confidence=confidence, source_variant="")



def read_field(cell_gray: np.ndarray, field_name: str) -> List[OCRCandidate]:
    candidates: List[OCRCandidate] = []
    for variant_name, variant_image in _build_variants(cell_gray):
        candidate = _read_single_variant(variant_image, field_name)
        candidate.source_variant = variant_name
        if candidate.text:
            candidates.append(candidate)

    # Keep candidates sorted by confidence, strongest first.
    candidates.sort(key=lambda item: (item.confidence, len(item.text)), reverse=True)
    return candidates
