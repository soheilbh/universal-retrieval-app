"""
Energy data retrieval - matches original FRM_energy_data_hourly_retrieval.py exactly.
"""

import os
import requests
import pandas as pd
from datetime import datetime

TIMEOUT_SECONDS = 600


def run_retrieval(config, progress_callback=None):
    """
    Run energy retrieval - same logic as original FRM_energy_data_hourly_retrieval.py
    config: host, port, database, prefix, start_date, end_date, output_dir, energy_types (optional)
    energy_types: ["gas"] = only gas (original behavior); None/[] = discover all from energy_data
    Returns: path to CSV or None
    """
    host = os.getenv("INFLUXDB_HOST", config.get("host", "localhost"))
    port = os.getenv("INFLUXDB_PORT", config.get("port", "8086"))
    database = config.get("database", "farmsum_db")
    prefix = config.get("prefix", "FRM")
    start_date = config.get("start_date", "2024-01-01")
    end_date = config.get("end_date", "2026-02-03")
    output_dir = config.get("output_dir", "./outputs")
    start_time = config.get("start_time", "00:00:00")
    end_time = config.get("end_time", "23:59:59")
    energy_types = config.get("energy_types")

    os.makedirs(output_dir, exist_ok=True)
    start_str = f"{start_date}T{start_time}Z"
    end_str = f"{end_date}T{end_time}Z"

    if not energy_types:
        energy_types = _discover_types(host, port, database)
    if not energy_types:
        return None, {"name": "energy_data", "successful": 0, "failed": 0, "total_points": 0}
    all_data = {}
    ok, fail = [], []

    for i, et in enumerate(energy_types, 1):
        if progress_callback:
            progress_callback(i, len(energy_types), et, "...")
        data = _query(host, port, database, et, start_str, end_str)
        if data is not None:
            all_data[et] = data
            ok.append(et)
            if progress_callback:
                progress_callback(i, len(energy_types), et, f"OK ({data.notna().sum():,} values)")
        else:
            fail.append(et)
            if progress_callback:
                progress_callback(i, len(energy_types), et, "NO DATA")

    if not all_data:
        return None, {"name": "energy_data", "successful": 0, "failed": len(fail), "total_points": 0}

    df = pd.DataFrame(all_data).sort_index()
    sc = start_date.replace("-", "")
    ec = end_date.replace("-", "")
    filename = f"{prefix}_energy_data_hourly_{sc}_to_{ec}.csv"
    filepath = os.path.join(output_dir, filename)
    df.to_csv(filepath)
    _write_metadata(filepath, host, port, database, start_date, end_date, ok, fail, prefix)
    summary = {"name": "energy_data", "successful": len(ok), "failed": len(fail), "total_points": len(df)}
    return filepath, summary


def _discover_types(host, port, database):
    """Discover all energy types from energy_data measurement."""
    try:
        url = f"http://{host}:{port}/query"
        q = 'SHOW TAG VALUES FROM "energy_data" WITH KEY = "type"'
        r = requests.get(url, params={"db": database, "q": q}, timeout=60)
        if r.status_code != 200:
            return []
        data = r.json()
        if not data.get("results") or not data["results"][0].get("series"):
            return []
        vals = data["results"][0]["series"][0]["values"]
        return [r[-1] for r in vals] if vals else []
    except Exception:
        return []


def _query(host, port, database, energy_type, start_str, end_str):
    """Exact copy of original query_energy_data - SELECT value FROM energy_data"""
    try:
        url = f"http://{host}:{port}/query"
        query = f'''
        SELECT value 
        FROM "energy_data" 
        WHERE type = '{energy_type}' 
        AND time >= '{start_str}' AND time <= '{end_str}'
        '''
        params = {"db": database, "q": query}
        r = requests.get(url, params=params, timeout=TIMEOUT_SECONDS)
        if r.status_code != 200:
            return None
        data = r.json()
        if "results" not in data or not data["results"]:
            return None
        result = data["results"][0]
        if "series" not in result or not result["series"]:
            return None
        series = result["series"][0]
        df = pd.DataFrame(series["values"], columns=series["columns"])
        df["time"] = pd.to_datetime(df["time"])
        df = df.set_index("time")
        return df["value"]
    except Exception:
        return None


def _write_metadata(filepath, host, port, db, start, end, ok, fail, prefix):
    try:
        p = filepath.replace(".csv", "_metadata.txt")
        with open(p, "w") as f:
            f.write(f"{prefix} ENERGY RETRIEVAL\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Database: {host}:{port}/{db}\n")
            f.write(f"Period: {start} to {end}\n")
            f.write(f"OK: {ok}, Failed: {fail}\n")
    except Exception:
        pass
