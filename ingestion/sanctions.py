"""
OFAC Sanctions data ingestion.

Downloads and parses the OFAC SDN (Specially Designated Nationals) and
consolidated sanctions lists, storing relevant entries in the
sanctioned_entities table.

CLI usage:
    python -m ingestion.sanctions --refresh
"""

from __future__ import annotations

import asyncio
import csv
import io
import logging
import re
import sys
from typing import TYPE_CHECKING

import httpx
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from database import AsyncSessionLocal
from ontology.models import EntityType, SanctionedEntity, Vessel

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# OFAC download URLs
# ---------------------------------------------------------------------------
SDN_URL  = "https://www.treasury.gov/ofac/downloads/sdn.csv"
CONS_URL = "https://www.treasury.gov/ofac/downloads/consolidated/cons_prim.csv"

# ---------------------------------------------------------------------------
# SDN CSV column layout (file has no header row)
# ---------------------------------------------------------------------------
_SDN_FIELDS = [
    "ent_num", "sdn_name", "sdn_type", "program", "title",
    "call_sign", "vess_type", "tonnage", "grt", "vess_flag",
    "vess_owner", "remarks",
]

# ---------------------------------------------------------------------------
# Flag states with active US/UN/EU sanctions — used for risk flagging
# ---------------------------------------------------------------------------
SANCTIONED_COUNTRIES: dict[str, str] = {
    "IRN": "Iran",
    "PRK": "North Korea",
    "SYR": "Syria",
    "RUS": "Russia",
    "CUB": "Cuba",
    "VEN": "Venezuela",
    "BLR": "Belarus",
    "MMR": "Myanmar",
    "SDN": "Sudan",
    "YEM": "Yemen",
    "LBY": "Libya",
    "SOM": "Somalia",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _clean(s: str | None) -> str | None:
    s = (s or "").strip()
    return s if s and s not in ("-0-", "-", "NULL") else None


def _extract_mmsi(text: str) -> str | None:
    m = re.search(r'\bMMSI[:\s#]+(\d{9})\b', text or "", re.IGNORECASE)
    return m.group(1) if m else None


def _extract_imo(text: str) -> str | None:
    m = re.search(r'\bIMO[:\s#]+(\d{7})\b', text or "", re.IGNORECASE)
    return m.group(1) if m else None


def _edit_distance(a: str, b: str) -> int:
    """Wagner-Fischer (Levenshtein) edit distance — no external dependency."""
    if len(a) > len(b):
        a, b = b, a
    row = list(range(len(a) + 1))
    for ch2 in b:
        new_row = [row[0] + 1]
        for j, ch1 in enumerate(a):
            new_row.append(min(row[j + 1] + 1, new_row[j] + 1, row[j] + (ch1 != ch2)))
        row = new_row
    return row[-1]


# ---------------------------------------------------------------------------
# CSV parsers
# ---------------------------------------------------------------------------

def _parse_sdn_csv(content: str, source: str) -> list[dict]:
    """
    Parse OFAC SDN or cons_prim CSV.

    The SDN CSV has no header row; columns follow _SDN_FIELDS order.
    We include every row — vessel rows get EntityType.vessel, others get
    person/company/other so the sanctions table is complete for name matching.
    """
    reader  = csv.reader(io.StringIO(content))
    entries = []

    for row in reader:
        if len(row) < 4:
            continue
        # Skip header-like rows (ent_num must be a digit string)
        if not row[0].strip().lstrip("-").isdigit():
            continue

        fields = dict(zip(_SDN_FIELDS, [f.strip() for f in row[:12]]))

        sdn_type  = (fields.get("sdn_type") or "").strip().lower()
        vess_flag = _clean(fields.get("vess_flag"))
        vess_type = _clean(fields.get("vess_type"))
        remarks   = _clean(fields.get("remarks")) or ""
        call_sign = _clean(fields.get("call_sign"))

        # Determine entity type
        is_vessel = (
            sdn_type == "vessel"
            or bool(vess_type)
            or bool(vess_flag)
            or "vessel" in remarks.lower()
        )
        if is_vessel:
            entity_type = EntityType.vessel
        elif sdn_type in ("individual", "i"):
            entity_type = EntityType.person
        elif sdn_type in ("entity", "e"):
            entity_type = EntityType.company
        elif sdn_type in ("aircraft", "a"):
            entity_type = EntityType.aircraft
        else:
            entity_type = EntityType.other

        # Extract machine-readable identifiers
        identifiers: dict[str, str] = {}
        mmsi = _extract_mmsi(remarks) or _extract_mmsi(call_sign or "")
        imo  = _extract_imo(remarks)
        if mmsi:      identifiers["mmsi"]      = mmsi
        if imo:       identifiers["imo"]        = imo
        if call_sign: identifiers["call_sign"] = call_sign

        entries.append({
            "source_id":   fields["ent_num"].strip(),
            "name":        _clean(fields.get("sdn_name")) or "Unknown",
            "entity_type": entity_type,
            "identifiers": identifiers if identifiers else None,
            "source":      source,
            "country":     vess_flag,
            "programs":    _clean(fields.get("program")),
            "remarks":     remarks or None,
        })

    return entries


# ---------------------------------------------------------------------------
# Download + store
# ---------------------------------------------------------------------------

async def download_and_store(url: str, source: str) -> int:
    """Download a sanctions CSV, parse it, and upsert into the DB."""
    logger.info("Downloading %s …", source)
    async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
        resp = await client.get(url)
        resp.raise_for_status()

    entries = _parse_sdn_csv(resp.text, source)
    if not entries:
        logger.warning("%s: parsed 0 entries — check CSV format", source)
        return 0

    async with AsyncSessionLocal() as session:
        stmt = (
            insert(SanctionedEntity)
            .values(entries)
            .on_conflict_do_update(
                constraint="uq_sanctioned_source_id",
                set_={
                    "name":        insert(SanctionedEntity).excluded.name,
                    "entity_type": insert(SanctionedEntity).excluded.entity_type,
                    "identifiers": insert(SanctionedEntity).excluded.identifiers,
                    "country":     insert(SanctionedEntity).excluded.country,
                    "programs":    insert(SanctionedEntity).excluded.programs,
                    "remarks":     insert(SanctionedEntity).excluded.remarks,
                },
            )
        )
        await session.execute(stmt)
        await session.commit()

    logger.info("%s: upserted %d entries", source, len(entries))
    return len(entries)


async def refresh_all() -> None:
    """Download and store both OFAC SDN and consolidated lists."""
    sdn_count  = await download_and_store(SDN_URL,  "OFAC_SDN")
    cons_count = await download_and_store(CONS_URL, "OFAC_CONS")
    logger.info(
        "Sanctions refresh complete — SDN: %d entries, CONS: %d entries",
        sdn_count, cons_count,
    )


# ---------------------------------------------------------------------------
# Matching logic  (used by API + investigate tool)
# ---------------------------------------------------------------------------

async def match_vessel_against_sanctions(
    session:      AsyncSession,
    mmsi:         str | None = None,
    imo:          str | None = None,
    name:         str | None = None,
    flag:         str | None = None,
    fuzzy_threshold: int = 3,
) -> dict:
    """
    Check a vessel against the sanctions database.

    Performs four checks in order:
      1. Exact MMSI match on identifiers->>'mmsi'
      2. Exact IMO match on identifiers->>'imo'
      3. Fuzzy name match against vessel-type entries (edit distance < threshold)
      4. Flag state membership in SANCTIONED_COUNTRIES

    Returns a dict with keys: matches, sanctioned_flag, risk_level.
    Each match has a 'match_reason' field.
    """
    matches: list[dict] = []
    seen_ids: set[int]  = set()

    def _row_to_dict(row: SanctionedEntity, reason: str) -> dict:
        return {
            "id":           row.id,
            "name":         row.name,
            "entity_type":  row.entity_type.value,
            "source":       row.source,
            "source_id":    row.source_id,
            "country":      row.country,
            "programs":     row.programs,
            "identifiers":  row.identifiers,
            "remarks":      row.remarks,
            "match_reason": reason,
        }

    # 1. MMSI exact match
    if mmsi:
        rows = (
            await session.execute(
                select(SanctionedEntity)
                .where(SanctionedEntity.identifiers["mmsi"].astext == mmsi)
            )
        ).scalars().all()
        for r in rows:
            if r.id not in seen_ids:
                matches.append(_row_to_dict(r, "mmsi_exact"))
                seen_ids.add(r.id)

    # 2. IMO exact match
    if imo:
        rows = (
            await session.execute(
                select(SanctionedEntity)
                .where(SanctionedEntity.identifiers["imo"].astext == imo)
            )
        ).scalars().all()
        for r in rows:
            if r.id not in seen_ids:
                matches.append(_row_to_dict(r, "imo_exact"))
                seen_ids.add(r.id)

    # 3. Fuzzy name match (vessel entries only; fetch all and filter in Python)
    if name:
        name_upper = name.upper()
        vessel_rows = (
            await session.execute(
                select(SanctionedEntity)
                .where(SanctionedEntity.entity_type == EntityType.vessel)
            )
        ).scalars().all()
        for r in vessel_rows:
            if r.id in seen_ids:
                continue
            dist = _edit_distance(name_upper, r.name.upper())
            if dist < fuzzy_threshold:
                matches.append(_row_to_dict(r, f"name_fuzzy(dist={dist})"))
                seen_ids.add(r.id)

    # 4. Flag state check
    flag_upper      = (flag or "").upper()
    sanctioned_flag = SANCTIONED_COUNTRIES.get(flag_upper)

    # Determine overall risk level
    if matches:
        risk_level = "high"
    elif sanctioned_flag:
        risk_level = "medium"
    else:
        risk_level = "low"

    return {
        "matches":         matches,
        "sanctioned_flag": sanctioned_flag,
        "risk_level":      risk_level,
        "checked_mmsi":    mmsi,
        "checked_imo":     imo,
        "checked_name":    name,
        "checked_flag":    flag,
    }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )

    parser = argparse.ArgumentParser(description="OFAC Sanctions data refresh")
    parser.add_argument("--refresh", action="store_true", help="Download and store latest sanctions lists")
    args = parser.parse_args()

    if args.refresh:
        asyncio.run(refresh_all())
    else:
        parser.print_help()
        sys.exit(1)
