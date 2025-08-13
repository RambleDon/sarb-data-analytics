
#!/usr/bin/env python3
"""
SARB BA900 Puller (robust version)
----------------------------------
- Uses official SARB Web API endpoints:
  * GetPeriods:         /SarbData/IFData/GetPeriods/{ifType}
  * GetInstitutions:    /SarbData/IFData/GetInstitutions/{ifType}/{period}
  * GetInstitutionData: /SarbData/IFData/GetInstitutionData/{ifType}/{period}/{institutionId}
- Pulls the last N valid periods for BA900 (or DI900/BD900), across a chosen set of institutions.
- Parses the embedded XML payload and flattens it to a tidy DataFrame.
- Incrementally updates a Parquet file without duplicating rows.
- Adds proper retries, timeouts, and light concurrency.

Usage (examples):
    python sarb_ba900_pull.py --months 96 --if-type BA900 --out-dir data/ba_900
    python sarb_ba900_pull.py --months 24 --if-type BA900 --all-institutions    # auto-discovers institutions per period
    python sarb_ba900_pull.py --months 12 --if-type DI900 --institutions TOTAL 416061

Notes:
- The API expects period formatted 'YYYY-MM-01' (first of month).
- "Institution IDs" are numeric strings (or 'TOTAL') retrievable via GetInstitutions.
- Parquet requires either 'pyarrow' or 'fastparquet' installed.
"""

import argparse
import concurrent.futures as cf
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import requests
import xml.etree.ElementTree as ET
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = os.environ.get(
    "SARB_API_BASE",
    "https://custom.resbank.co.za/SarbWebApi",
).rstrip("/")

IF_TYPES = {"BA900", "DI900", "BD900"}

# Default banks to pull (you can override with --institutions or --all-institutions)
DEFAULT_INSTITUTIONS = [
    {"name": "Total", "id": "TOTAL"},
    {"name": "Absa", "id": "34118"},
    {"name": "Capitec", "id": "333107"},
    {"name": "Discovery", "id": "165271"},
    {"name": "FirstRand", "id": "416053"},
    {"name": "Investec", "id": "25054"},
    {"name": "Nedbank", "id": "416088"},
    {"name": "Standard Bank", "id": "416061"},
]

# -----------------------------
# Networking helpers
# -----------------------------

def _requests_session(total_retries: int = 5, backoff: float = 0.5, timeout: float = 30.0) -> requests.Session:
    sess = requests.Session()
    retries = Retry(
        total=total_retries,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=16, pool_maxsize=32)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    sess.headers.update({"User-Agent": "SARB-BA900-Puller/1.0 (+github.com/you)"})
    sess.request_timeout = timeout  # custom attribute
    return sess

def _get_json(sess: requests.Session, url: str):
    resp = sess.get(url, timeout=getattr(sess, "request_timeout", 30.0))
    resp.raise_for_status()
    return resp.json()

# -----------------------------
# API wrappers
# -----------------------------

def get_periods(sess: requests.Session, if_type: str) -> List[str]:
    url = f"{BASE_URL}/SarbData/IFData/GetPeriods/{if_type}"
    periods = _get_json(sess, url)
    # The API returns strings like 'YYYY-MM-01' (or ISO)
    # sort descending by date to easily slice newest N
    periods_dt = sorted(
        (pd.to_datetime(p) for p in periods),
        reverse=True,
    )
    return [pd.Timestamp(p).strftime("%Y-%m-01") for p in periods_dt]

def get_institutions(sess: requests.Session, if_type: str, period: str) -> List[Dict[str, str]]:
    url = f"{BASE_URL}/SarbData/IFData/GetInstitutions/{if_type}/{period}"
    data = _get_json(sess, url)
    # Normalize structure
    return [{"Id": d.get("Id"), "Name": d.get("Name"), "LastUpdate": d.get("LastUpdate"), "NameChange": d.get("NameChange")} for d in data]

def get_institution_data(sess: requests.Session, if_type: str, period: str, institution_id: str) -> List[Dict[str, str]]:
    url = f"{BASE_URL}/SarbData/IFData/GetInstitutionData/{if_type}/{period}/{institution_id}"
    data = _get_json(sess, url)
    if not isinstance(data, list):
        return []
    return data

# -----------------------------
# XML parsing / flattening
# -----------------------------

def _to_float_or_none(x: Optional[str]) -> Optional[float]:
    if x is None:
        return None
    s = str(x).strip()
    if s == "" or s.lower() in {"na", "null"}:
        return None
    # Handle (123) style negatives and thousands separators
    s = s.replace("\u00A0", " ")  # NBSP -> space
    s = s.replace(",", "")  # remove commas
    if re.match(r"^\(.*\)$", s):
        s = "-" + s.strip("() ")
    try:
        return float(s)
    except ValueError:
        return None

def parse_sarb_xml(xml_string: str) -> Dict:
    """Parse SARB XML (keeps header + tables/rows/columns)."""
    root = ET.fromstring(xml_string)
    sarb_data = {
        "Type": root.get("Type"),
        "Description": root.get("Description"),
        "Year": root.get("TheYear"),
        "Month": root.get("TheMonth"),
        "Day": root.get("TheDay"),
        "InstitutionCode": root.get("InstitutionCode"),
        "InstitutionDescription": root.get("InstitutionDescription"),
        "LastModified": root.get("LastModified"),
        "Tables": [],
    }
    for table_elem in root.findall('.//Table'):
        table = {
            "TableNumber": table_elem.get("TableNumber"),
            "TableDescription": table_elem.get("TableDescription"),
            "Columns": [],
            "Rows": [],
        }
        for col_header in table_elem.findall('./ColumnHeader'):
            table["Columns"].append({
                "ColumnNumber": col_header.get("ColumnNumber"),
                "ColumnCode": col_header.get("ColumnCode"),
                "ColumnDescription": col_header.get("ColumnDescription"),
            })
        for row_elem in table_elem.findall('./Row'):
            row = {
                "RowNumber": row_elem.get("RowNumber"),
                "ItemNumber": row_elem.get("ItemNumber"),
                "ItemDescription": row_elem.get("ItemDescription"),
                "Values": {},
            }
            for col_elem in row_elem.findall('./Column'):
                col_num = col_elem.get("ColumnNumber")
                row["Values"][col_num] = {
                    "Value": col_elem.get("Value") or "",
                    "Format": col_elem.get("Format"),
                }
            table["Rows"].append(row)
        sarb_data["Tables"].append(table)
    return sarb_data

def records_from_parsed(parsed: Dict, institution_name: str, institution_code: str, period: str) -> List[Dict]:
    records: List[Dict] = []
    for table in parsed.get("Tables", []):
        cols_map = {c.get("ColumnNumber"): c for c in table.get("Columns", [])}
        for row in table.get("Rows", []):
            base = {
                "institution_name": institution_name,
                "institution_code": institution_code,
                "period": period,
                "table_number": table.get("TableNumber"),
                "table_description": table.get("TableDescription"),
                "item_number": row.get("ItemNumber"),
                "item_description": row.get("ItemDescription"),
            }
            for col_num, col_data in row.get("Values", {}).items():
                ch = cols_map.get(col_num, {})
                rec = {
                    **base,
                    "column_number": col_num,
                    "column_code": ch.get("ColumnCode"),
                    "column_description": ch.get("ColumnDescription") or f"Column_{col_num}",
                    "value": col_data.get("Value", ""),
                    "format": col_data.get("Format"),
                }
                rec["value_numeric"] = _to_float_or_none(rec["value"])
                records.append(rec)
    return records

# -----------------------------
# Incremental update logic
# -----------------------------

KEY_COLS = ["institution_code", "period", "table_number", "item_number", "column_number"]

def update_dataframe(existing: Optional[pd.DataFrame], new_df: pd.DataFrame) -> pd.DataFrame:
    if existing is None or existing.empty:
        return new_df.copy()
    # Normalize to string for join key consistency
    for col in KEY_COLS:
        if col in existing.columns:
            existing[col] = existing[col].astype(str)
        if col in new_df.columns:
            new_df[col] = new_df[col].astype(str)
    existing["_key"] = existing[KEY_COLS].agg("_".join, axis=1)
    new_df["_key"] = new_df[KEY_COLS].agg("_".join, axis=1)

    to_update_keys = set(existing["_key"]) & set(new_df["_key"])
    to_insert = new_df[~new_df["_key"].isin(existing["_key"])]
    updated = existing.copy()
    if to_update_keys:
        # Keep the newest value (assume new_df is newer)
        idxer = updated["_key"].isin(to_update_keys)
        # Join on _key to pull the new values
        repl = new_df.set_index("_key")["value"].to_dict()
        repl_num = new_df.set_index("_key")["value_numeric"].to_dict()
        updated.loc[idxer, "value"] = updated.loc[idxer, "_key"].map(repl)
        updated.loc[idxer, "value_numeric"] = updated.loc[idxer, "_key"].map(repl_num)
    if not to_insert.empty:
        updated = pd.concat([updated.drop(columns=["_key"]), to_insert.drop(columns=["_key"])], ignore_index=True)
    else:
        updated = updated.drop(columns=["_key"])
    return updated

# -----------------------------
# Main pull logic
# -----------------------------

@dataclass
class PullTask:
    period: str
    institution_id: str
    institution_name: str

def _pull_one(sess: requests.Session, if_type: str, task: PullTask, pause: float = 0.0) -> List[Dict]:
    """Pull one (period, institution) and return flattened records."""
    try:
        data = get_institution_data(sess, if_type, task.period, task.institution_id)
        if not data:
            return []
        all_records: List[Dict] = []
        for item in data:
            xml = item.get("XMLData")
            inst_name = item.get("InstitutionName") or task.institution_name
            inst_id = item.get("InstitutionId") or task.institution_id
            if not xml:
                continue
            parsed = parse_sarb_xml(xml)
            all_records.extend(records_from_parsed(parsed, inst_name, inst_id, task.period))
        if pause:
            time.sleep(pause)
        return all_records
    except requests.HTTPError as e:
        sys.stderr.write(f"HTTP error for {task.institution_id} {task.period}: {e}\n")
        return []
    except ET.ParseError as e:
        sys.stderr.write(f"XML parse error for {task.institution_id} {task.period}: {e}\n")
        return []
    except Exception as e:
        sys.stderr.write(f"Unexpected error for {task.institution_id} {task.period}: {e}\n")
        return []

def _ensure_out_dir(path: str) -> str:
    os.makedirs(path, exist_ok=True)
    return path

def _read_existing_parquet(path: str) -> Optional[pd.DataFrame]:
    if not os.path.exists(path):
        return None
    try:
        return pd.read_parquet(path)
    except Exception as e:
        sys.stderr.write(f"Warning: could not read existing Parquet ('{path}'): {e}\n")
        return None

def _write_parquet(df: pd.DataFrame, path: str) -> None:
    # Try pyarrow then fastparquet
    try:
        df.to_parquet(path, index=False, engine="pyarrow")
    except Exception:
        df.to_parquet(path, index=False)  # let pandas choose fallback
    print(f"Saved Parquet: {path}")

def pull_ba900(
    months: int = 96,
    if_type: str = "BA900",
    out_dir: str = "data/ba_900",
    use_all_institutions: bool = False,
    institution_ids: Optional[Sequence[str]] = None,
    max_workers: int = 8,
    polite_pause: float = 0.0,
) -> Tuple[pd.DataFrame, str]:
    if if_type not in IF_TYPES:
        raise ValueError(f"if_type must be one of {IF_TYPES}")

    out_dir = _ensure_out_dir(out_dir)
    parquet_path = os.path.join(out_dir, "combined_data.parquet")

    sess = _requests_session()

    # Determine valid periods (newest -> oldest) and take last N
    all_periods_desc = get_periods(sess, if_type)
    periods = all_periods_desc[:months]
    if not periods:
        raise RuntimeError("No valid periods returned by API.")
    print(f"Periods selected: {periods[0]} .. {periods[-1]} ({len(periods)} months)")

    # Institutions: explicit list or auto-detected each period
    tasks: List[PullTask] = []
    if use_all_institutions:
        # Discover institutions per period (IDs can change over time; TOTAL is always present)
        for p in periods:
            try:
                insts = get_institutions(sess, if_type, p)
            except Exception as e:
                sys.stderr.write(f"Warning: could not fetch institutions for {p}: {e}\n")
                insts = []
            for it in insts:
                tasks.append(PullTask(period=p, institution_id=it.get("Id"), institution_name=it.get("Name")))
    else:
        # Use supplied institution ids or defaults
        if institution_ids:
            id_set = set(str(x) for x in institution_ids)
            insts = [{"name": i.get("name"), "id": i.get("id")} for i in DEFAULT_INSTITUTIONS if i["id"] in id_set]
            # If user passed raw IDs not matching defaults, include them anyway with name==id
            for iid in id_set - {i["id"] for i in insts}:
                insts.append({"name": iid, "id": iid})
        else:
            insts = DEFAULT_INSTITUTIONS

        # Optional: filter out institutions that don't exist in a given period by calling GetInstitutions once for newest period
        try:
            newest_period_insts = {d["Id"] for d in get_institutions(sess, if_type, periods[0])}
        except Exception:
            newest_period_insts = None

        for p in periods:
            for i in insts:
                if newest_period_insts is not None and i["id"] not in newest_period_insts and i["id"] != "TOTAL":
                    # likely not present; still try in case of historical presence
                    pass
                tasks.append(PullTask(period=p, institution_id=i["id"], institution_name=i["name"]))

    print(f"Planned pulls: {len(tasks):,} (institutions x months)")

    # Pull concurrently
    all_records: List[Dict] = []
    with cf.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_pull_one, sess, if_type, t, polite_pause) for t in tasks]
        for n, fut in enumerate(cf.as_completed(futures), 1):
            recs = fut.result()
            if recs:
                all_records.extend(recs)
            if n % 50 == 0:
                print(f"Completed {n}/{len(futures)} pulls..." )

    if not all_records:
        raise RuntimeError("No data returned; check connectivity or parameters.")

    new_df = pd.DataFrame(all_records)
    # Ensure consistent dtypes
    if not new_df.empty:
        new_df["period_date"] = pd.to_datetime(new_df["period"], errors="coerce")
        # Helpful sort
        new_df = new_df.sort_values(["institution_code", "period_date", "table_number", "item_number", "column_number"]).reset_index(drop=True)

    existing = _read_existing_parquet(parquet_path)
    final_df = update_dataframe(existing, new_df)
    _write_parquet(final_df, parquet_path)

    # Basic summary to stdout
    periods_covered = (final_df["period"].min(), final_df["period"].max())
    inst_count = final_df["institution_code"].nunique()
    print(f"Rows: {len(final_df):,} | Institutions: {inst_count} | Periods: {periods_covered[0]} to {periods_covered[1]}" )

    return final_df, parquet_path

# -----------------------------
# CLI
# -----------------------------

def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Pull SARB BA900/DI900/BD900 data and save as Parquet.")
    p.add_argument("--months", type=int, default=96, help="How many latest months to pull (default: 96)")
    p.add_argument("--if-type", dest="if_type", type=str, default="BA900", help="BA900 | DI900 | BD900 (default: BA900)")
    p.add_argument("--out-dir", dest="out_dir", type=str, default="data/ba_900", help="Output directory (default: data/ba_900)")
    p.add_argument("--all-institutions", action="store_true", help="Auto-discover all institutions per period via API")
    p.add_argument("--institutions", nargs="*", help="Explicit institution IDs to pull (space-separated). Overrides defaults unless --all-institutions is set.")
    p.add_argument("--max-workers", type=int, default=8, help="Max concurrent pulls (default: 8)")
    p.add_argument("--polite-pause", type=float, default=0.0, help="Optional sleep (seconds) after each request inside worker to ease server load.")
    return p.parse_args(argv)

def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        pull_ba900(
            months=args.months,
            if_type=args.if_type.upper(),
            out_dir=args.out_dir,
            use_all_institutions=args.all_institutions,
            institution_ids=args.institutions,
            max_workers=args.max_workers,
            polite_pause=args.polite_pause,
        )
        return 0
    except KeyboardInterrupt:
        print("Interrupted.")
        return 130
    except Exception as e:
        sys.stderr.write(f"Error: {e}\n")
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
