"""
Base column mapper.

Responsibilities
----------------
- Compare incoming CSV headers against the known BigQuery schema.
- Build a header→bq_column mapping for each CSV column.
- Re-emit the CSV with BQ column names as headers and NULL-filled gaps.
- Collect unknown CSV columns (those with no BQ equivalent) for alerting.
- Optionally absorb unknown columns into reserved OVERFLOW_COLUMNS slots
  (e.g. EXTRA_COLUMN1…5 for CAMS) instead of alerting immediately.

Usage
-----
    mapper = CamsColumnMapper()
    mapped_csv, unknowns = mapper.build_output_csv(raw_csv_bytes)
    if unknowns:
        await alert(...)
"""

from __future__ import annotations

import csv
import io
import logging
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class ColumnMapper(ABC):
    """
    Abstract base for provider-specific column mappers.

    Subclass contract
    -----------------
    BQ_COLUMNS       Ordered list of BigQuery target table column names.
    CSV_TO_BQ        {raw_csv_header: bq_column_name}.  Keys must match
                     the CSV header exactly (case-sensitive).
    COMPUTED_COLUMNS BQ columns populated by the pipeline, not from CSV
                     (e.g. row_hash, created_at).  Excluded from output CSV.
    OVERFLOW_COLUMNS Ordered list of BQ column names reserved for unknown
                     CSV columns (e.g. ["EXTRA_COLUMN1", ..., "EXTRA_COLUMN5"]).
                     Set to [] if the provider has no overflow mechanism.
    """

    BQ_COLUMNS: list[str] = []
    CSV_TO_BQ: dict[str, str] = {}
    COMPUTED_COLUMNS: set[str] = set()
    OVERFLOW_COLUMNS: list[str] = []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build_output_csv(self, raw_csv_bytes: bytes) -> tuple[str, list[str]]:
        """
        Transform raw CSV bytes into a new CSV aligned to the BQ schema.

        Returns
        -------
        mapped_csv   : str   — CSV text with BQ column names as header row.
                               Missing BQ columns emit empty string (→ NULL in BQ).
        unknown_cols : list[str] — CSV column names that had no BQ mapping
                                   and no overflow slot.  Fire an alert on these.
        """
        text = _decode(raw_csv_bytes)
        dialect = _sniff(text)

        reader = csv.DictReader(io.StringIO(text), dialect=dialect)
        if reader.fieldnames is None:
            return "", []

        csv_headers: list[str] = [h.strip() for h in reader.fieldnames]
        col_map, overflow_map, unknown_cols = self._build_col_map(csv_headers)

        # Output columns = BQ_COLUMNS minus computed ones
        out_cols = [c for c in self.BQ_COLUMNS if c not in self.COMPUTED_COLUMNS]

        buf = io.StringIO()
        writer = csv.writer(buf, lineterminator="\n")
        writer.writerow(out_cols)

        for raw_row in reader:
            row: list[str] = []
            for bq_col in out_cols:
                if bq_col in self.OVERFLOW_COLUMNS:
                    csv_col = overflow_map.get(bq_col)
                    row.append(_clean(raw_row.get(csv_col, "")) if csv_col else "")
                else:
                    csv_col = col_map.get(bq_col)
                    row.append(_clean(raw_row.get(csv_col, "")) if csv_col else "")
            writer.writerow(row)

        return buf.getvalue(), unknown_cols

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _build_col_map(
        self, csv_headers: list[str]
    ) -> tuple[dict[str, str], dict[str, str], list[str]]:
        """
        Build three mappings:

        col_map      {bq_col: csv_col}  for known columns
        overflow_map {bq_overflow_slot: csv_col}  for unknown columns that fit
        unknown_cols list of csv cols with no mapping and no overflow slot

        Also logs missing BQ columns (those absent from CSV).
        """
        # Reverse index: bq_col → csv_col
        col_map: dict[str, str] = {}
        mapped_bq: set[str] = set()
        truly_unknown: list[str] = []

        for csv_col in csv_headers:
            bq_col = self.CSV_TO_BQ.get(csv_col)
            if bq_col and bq_col not in self.COMPUTED_COLUMNS:
                col_map[bq_col] = csv_col
                mapped_bq.add(bq_col)
            else:
                truly_unknown.append(csv_col)

        # Warn about BQ columns absent from this CSV
        expected = [
            c for c in self.BQ_COLUMNS
            if c not in self.COMPUTED_COLUMNS and c not in self.OVERFLOW_COLUMNS
        ]
        missing = [c for c in expected if c not in mapped_bq]
        if missing:
            logger.warning("CSV missing BQ columns (will be NULL): %s", missing)

        # Distribute unknown CSV cols into overflow slots
        overflow_slots = list(self.OVERFLOW_COLUMNS)
        overflow_map: dict[str, str] = {}  # bq_slot → csv_col
        unknown_cols: list[str] = []

        for csv_col in truly_unknown:
            if overflow_slots:
                slot = overflow_slots.pop(0)
                overflow_map[slot] = csv_col
                logger.info("Unknown CSV col %r → overflow slot %r", csv_col, slot)
            else:
                unknown_cols.append(csv_col)

        return col_map, overflow_map, unknown_cols


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _decode(data: bytes) -> str:
    """Decode bytes, strip NULL bytes and carriage returns."""
    text = data.decode("utf-8", errors="replace")
    text = text.replace("\x00", "").replace("\r\n", "\n").replace("\r", "\n")
    return text


def _sniff(text: str) -> type[csv.Dialect]:
    try:
        return csv.Sniffer().sniff(text[:4096], delimiters=",\t|;")
    except csv.Error:
        return csv.excel


def _clean(value: str) -> str:
    """Strip control characters (keep tab/newline), collapse whitespace."""
    cleaned = "".join(
        ch for ch in value
        if ch >= " " or ch == "\t"
    ).strip()
    return cleaned
