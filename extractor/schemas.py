from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

BBox = Tuple[int, int, int, int]


@dataclass
class RawCellValue:
    field: str
    raw_text: str
    confidence: float


@dataclass
class NormalizedRow:
    station: Optional[str]
    depth: Optional[float]
    boc: Optional[int]
    valid: bool
    confidence: float
    notes: Optional[str] = None
    source_file: Optional[str] = None
    page_number: Optional[int] = None
    table_index: Optional[int] = None
    row_index: Optional[int] = None
    raw_station: Optional[str] = None
    raw_depth: Optional[str] = None
    raw_boc: Optional[str] = None

    def to_dict(self) -> Dict[str, object]:
        return {
            "source_file": self.source_file,
            "page_number": self.page_number,
            "table_index": self.table_index,
            "row_index": self.row_index,
            "station": self.station,
            "depth": self.depth,
            "boc": self.boc,
            "valid": self.valid,
            "confidence": self.confidence,
            "notes": self.notes,
            "raw_station": self.raw_station,
            "raw_depth": self.raw_depth,
            "raw_boc": self.raw_boc,
        }


@dataclass
class ExtractionResult:
    rows: List[NormalizedRow] = field(default_factory=list)
    total_rows: int = 0
    valid_rows: int = 0
    low_confidence_rows: int = 0
    debug_path: Optional[str] = None
    source_file: Optional[str] = None
    page_number: Optional[int] = None


@dataclass
class TableGrid:
    table_index: int
    bbox: BBox
    vertical_lines: List[int]
    horizontal_lines: List[int]


@dataclass
class CellGeometry:
    row_index: int
    station_bbox: BBox
    depth_bbox: BBox
    boc_bbox: BBox
