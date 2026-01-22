from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd

COMMON_ALIASES = {
    "full_name": ["full name", "name", "contact name", "customer name"],
    "first_name": ["first name", "firstname", "given name"],
    "last_name": ["last name", "lastname", "surname", "family name"],
    "email": ["email", "e-mail", "email address"],
    "phone": ["phone", "telephone", "mobile"],
    "last_clean_date": ["last clean date", "last_clean_date", "last service date", "service date", "date"],
    "clean_type": ["clean type", "service", "service type", "type"],
    "invoice_amount_brutto": [
        "invoice amount (brutto)",
        "invoice amount brutto",
        "brutto",
        "gross",
        "invoice amount",
        "amount",
    ],
}

def _normalize_header(h: str) -> str:
    return " ".join(str(h).strip().lower().replace("_", " ").split())

def _build_auto_mapping(columns: List[str]) -> Dict[str, str]:
    norm_cols = {c: _normalize_header(c) for c in columns}
    reverse = {v: k for k, v in norm_cols.items()}  # normalized -> original

    mapping: Dict[str, str] = {}
    for canonical, aliases in COMMON_ALIASES.items():
        for a in aliases:
            a_norm = _normalize_header(a)
            if a_norm in reverse:
                mapping[canonical] = reverse[a_norm]
                break
    return mapping

@dataclass
class CleanConfig:
    date_field: str = "last_clean_date"
    dedupe_key: str = "email"  # "email" or "full_name"
    required: Tuple[str, ...] = ("full_name",)
    mapping: Optional[Dict[str, str]] = None  # canonical -> source header

@dataclass
class CleanResult:
    cleaned: pd.DataFrame
    report: pd.DataFrame

def clean_contacts_csv(df: pd.DataFrame, cfg: CleanConfig) -> CleanResult:
    if cfg.mapping is None:
        cfg.mapping = _build_auto_mapping(list(df.columns))

    canonical_cols = set(COMMON_ALIASES.keys())
    out = pd.DataFrame(index=df.index)

    for canonical in canonical_cols:
        src = cfg.mapping.get(canonical)
        if src and src in df.columns:
            out[canonical] = df[src]

    # Build full_name if missing but first/last exist
    if "full_name" not in out.columns and {"first_name", "last_name"}.issubset(out.columns):
        out["full_name"] = (
            out["first_name"].fillna("").astype(str).str.strip()
            + " "
            + out["last_name"].fillna("").astype(str).str.strip()
        ).str.strip()

    # Normalize
    if "full_name" in out.columns:
        out["full_name"] = (
            out["full_name"].fillna("").astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
        )

    if "email" in out.columns:
        out["email"] = out["email"].fillna("").astype(str).str.strip().str.lower()

    # Parse date
    if cfg.date_field in out.columns:
        out[cfg.date_field] = pd.to_datetime(out[cfg.date_field], errors="coerce", utc=False)

    # Validate required
    report_rows = []

    def missing_required(row) -> List[str]:
        missing = []
        for req in cfg.required:
            if req not in out.columns:
                missing.append(req)
                continue
            v = row.get(req)
            if v is None:
                missing.append(req)
            elif isinstance(v, str) and v.strip() == "":
                missing.append(req)
            elif pd.isna(v):
                missing.append(req)
        return missing

    mask_keep = []
    for idx, row in out.iterrows():
        miss = missing_required(row)
        if miss:
            report_rows.append({"row": int(idx), "reason": f"missing_required:{','.join(miss)}"})
            mask_keep.append(False)
        else:
            mask_keep.append(True)

    out_valid = out.loc[pd.Index(out.index)[mask_keep]].copy()

    # Deduplicate
    if cfg.dedupe_key == "email" and "email" in out_valid.columns:
        key_series = out_valid["email"].fillna("")
    else:
        key_series = out_valid.get("full_name", pd.Series([""] * len(out_valid), index=out_valid.index)).fillna("")

    out_valid["_dedupe_key"] = key_series.astype(str).str.strip().str.lower()

    if cfg.date_field in out_valid.columns:
        out_valid = out_valid.sort_values(["_dedupe_key", cfg.date_field], ascending=[True, True])
        deduped = out_valid.drop_duplicates(subset=["_dedupe_key"], keep="last").copy()

        dup_counts = out_valid.groupby("_dedupe_key").size()
        removed_keys = dup_counts[dup_counts > 1].index.tolist()
        for k in removed_keys:
            report_rows.append({"row": "-", "reason": f"deduped_removed_duplicates:key={k}"})
    else:
        deduped = out_valid.drop_duplicates(subset=["_dedupe_key"], keep="last").copy()

    deduped = deduped.drop(columns=["_dedupe_key"], errors="ignore")

    preferred_order = ["full_name", "email", "phone", "last_clean_date", "clean_type", "invoice_amount_brutto"]
    ordered = [c for c in preferred_order if c in deduped.columns] + [c for c in deduped.columns if c not in preferred_order]
    deduped = deduped[ordered]

    report = pd.DataFrame(report_rows)
    return CleanResult(cleaned=deduped, report=report)
