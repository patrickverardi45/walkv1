from __future__ import annotations

from pathlib import Path
from typing import List, Tuple

import cv2
import numpy as np
from PIL import Image

try:
    from pdf2image import convert_from_path
except Exception:  # pragma: no cover
    convert_from_path = None


SUPPORTED_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".tif", ".tiff", ".bmp", ".webp"}
SUPPORTED_PDF_EXTENSIONS = {".pdf"}


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_pages(input_path: str, poppler_path: str | None = None, dpi: int = 300) -> List[np.ndarray]:
    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    suffix = path.suffix.lower()
    if suffix in SUPPORTED_IMAGE_EXTENSIONS:
        image = cv2.imread(str(path), cv2.IMREAD_COLOR)
        if image is None:
            raise ValueError(f"Failed to load image: {input_path}")
        return [image]

    if suffix in SUPPORTED_PDF_EXTENSIONS:
        if convert_from_path is None:
            raise RuntimeError(
                "pdf2image is not installed. Install it or pass image files directly."
            )
        pages = convert_from_path(str(path), dpi=dpi, poppler_path=poppler_path)
        loaded: List[np.ndarray] = []
        for page in pages:
            rgb = np.array(page.convert("RGB"))
            loaded.append(cv2.cvtColor(rgb, cv2.COLOR_RGB2BGR))
        return loaded

    raise ValueError(f"Unsupported file type: {suffix}")


def normalize_page(image_bgr: np.ndarray) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    gray = cv2.cvtColor(image_bgr, cv2.COLOR_BGR2GRAY)

    # Light denoise without destroying pen strokes.
    denoised = cv2.fastNlMeansDenoising(gray, None, h=10, templateWindowSize=7, searchWindowSize=21)

    # Improve local contrast.
    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
    contrast = clahe.apply(denoised)

    # Slight sharpen.
    blurred = cv2.GaussianBlur(contrast, (0, 0), 1.1)
    sharpened = cv2.addWeighted(contrast, 1.5, blurred, -0.5, 0)

    # Binary image with black ink on white background.
    binary = cv2.adaptiveThreshold(
        sharpened,
        255,
        cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        cv2.THRESH_BINARY,
        31,
        11,
    )

    return gray, sharpened, binary


def crop_with_padding(image: np.ndarray, bbox: Tuple[int, int, int, int], pad: int = 0) -> np.ndarray:
    x1, y1, x2, y2 = bbox
    h, w = image.shape[:2]
    x1 = max(0, x1 - pad)
    y1 = max(0, y1 - pad)
    x2 = min(w, x2 + pad)
    y2 = min(h, y2 + pad)
    return image[y1:y2, x1:x2].copy()


def foreground_ratio(image_gray: np.ndarray, threshold: int = 200) -> float:
    if image_gray.size == 0:
        return 0.0
    ink = np.count_nonzero(image_gray < threshold)
    return float(ink) / float(image_gray.size)


def save_image(path: Path, image: np.ndarray) -> None:
    ensure_dir(path.parent)
    cv2.imwrite(str(path), image)
