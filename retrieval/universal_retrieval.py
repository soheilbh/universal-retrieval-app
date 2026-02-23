"""
Universal sensor data retrieval - self-contained, accepts config.
Works for Farmsum, Teesside, or any InfluxDB with same schema.
"""

import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta


def _format_elapsed(seconds):
    """Format seconds as 'Xs' or 'Xm Ys'."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    m = int(seconds // 60)
    s = seconds - m * 60
    return f"{m}m {s:.1f}s"

def _resolution_to_label(resolution):
    """Map InfluxDB interval (e.g. 1m, 1s) to filename-safe label (e.g. 1min, 1sec)."""
    m = {"1s": "1sec", "5s": "5sec", "15s": "15sec", "1m": "1min", "5m": "5min", "15m": "15min", "1h": "1hr"}
    return m.get(resolution, resolution.replace("m", "min").replace("s", "sec").replace("h", "hr"))


def _resolution_to_description(resolution):
    """Map InfluxDB interval to human-readable string for metadata."""
    m = {"1s": "1 second", "5s": "5 seconds", "15s": "15 seconds", "1m": "1 minute",
         "5m": "5 minutes", "15m": "15 minutes", "1h": "1 hour"}
    return m.get(resolution, resolution)


def _chunk_time_ranges(start_str, end_str, interval):
    """
    Split [start_str, end_str] into smaller chunks to avoid huge 1s queries.
    Returns list of (chunk_start_str, chunk_end_str) in Influx format.
    """
    start = pd.Timestamp(start_str.replace("Z", ""))
    end = pd.Timestamp(end_str.replace("Z", ""))
    if start >= end:
        return [(start_str, end_str)]
    # Chunk size by resolution: balance request count vs response size (avoids timeouts, speeds up 1s)
    if interval == "1s":
        days = 7   # ~604k points per chunk
    elif interval in ("5s", "15s"):
        days = 14
    elif interval == "1m":
        days = 14
    else:
        # 5m, 15m, 1h: single query
        return [(start_str, end_str)]
    delta = timedelta(days=days)
    chunks = []
    t = start
    while t < end:
        chunk_end = min(t + delta, end)
        cs = t.strftime("%Y-%m-%dT%H:%M:%SZ")
        ce = chunk_end.strftime("%Y-%m-%dT%H:%M:%SZ")
        chunks.append((cs, ce))
        t = chunk_end
    return chunks if chunks else [(start_str, end_str)]


def run_retrieval(config, progress_callback=None):
    """
    Run sensor retrieval for one unit.
    config: dict with host, port, database, unit_name, prefix, start_date, end_date, output_dir, resolution
    progress_callback: optional fn(current, total, sensor_name, status)
    Returns: path to saved CSV or None
    """
    host = os.getenv("INFLUXDB_HOST", config.get("host", "localhost"))
    port = os.getenv("INFLUXDB_PORT", config.get("port", "8086"))
    database = config.get("database", "farmsum_db")
    unit_name = config.get("unit_name", "BD361-0")
    prefix = config.get("prefix", "FRM")
    start_date = config.get("start_date", "2024-01-01")
    end_date = config.get("end_date", "2026-02-03")
    output_dir = config.get("output_dir", "./outputs")
    start_time = config.get("start_time", "00:00:00")
    end_time = config.get("end_time", "23:59:59")
    resolution = config.get("resolution", "1m")

    os.makedirs(output_dir, exist_ok=True)

    def cb(i, total, name, status):
        if progress_callback:
            progress_callback(i, total, name, status)

    sensors_and_fields = _auto_detect(host, port, database, unit_name)
    if not sensors_and_fields:
        cb(0, 1, unit_name, "No sensors found")
        return None, {"name": unit_name, "successful": 0, "failed": 0, "total_points": 0}

    start_str = f"{start_date}T{start_time}Z"
    end_str = f"{end_date}T{end_time}Z"

    all_data = {}
    successful = []
    failed = []
    sensor_mapping = {}
    failed_mapping = {}
    total = len(sensors_and_fields)

    FALLBACK_FIELDS = ("value_i", "value_b", "value_f", "value")
    for i, (col_name, unit, field) in enumerate(sensors_and_fields, 1):
        t0 = time.perf_counter()
        data = _query_chunked(host, port, database, unit_name, unit, field, start_str, end_str, resolution)
        if data is None:
            for alt in FALLBACK_FIELDS:
                if alt == field:
                    continue
                data = _query_chunked(host, port, database, unit_name, unit, alt, start_str, end_str, resolution)
                if data is not None:
                    field = alt
                    break
        elapsed = time.perf_counter() - t0
        time_str = f" (total time: {_format_elapsed(elapsed)})"
        if data is not None:
            all_data[col_name] = data
            successful.append(col_name)
            sensor_mapping[col_name] = (unit, field)
            cb(i, total, col_name, f"OK ({data.notna().sum():,} values){time_str}")
        else:
            failed.append(col_name)
            failed_mapping[col_name] = (unit, field)
            cb(i, total, col_name, f"NO DATA{time_str}")

    if not all_data:
        return None, {"name": unit_name, "successful": 0, "failed": len(failed), "total_points": 0}

    df = pd.DataFrame(all_data).sort_index()
    start_clean = start_date.replace("-", "")
    end_clean = end_date.replace("-", "")
    res_label = _resolution_to_label(resolution)
    filename = f"{prefix}_{unit_name}_ALL_sensors_{res_label}_{start_clean}_to_{end_clean}.csv"
    filepath = os.path.join(output_dir, filename)
    df.to_csv(filepath)
    _write_metadata(
        filepath, host, port, database, unit_name, start_date, end_date,
        successful, failed, prefix, start_time, end_time,
        df=df, sensor_mapping=sensor_mapping, failed_mapping=failed_mapping,
        resolution=resolution
    )
    summary = {"name": unit_name, "successful": len(successful), "failed": len(failed), "total_points": len(df)}
    return filepath, summary


def _query(host, port, database, measurement, sensor_name, field, start_str, end_str, interval="1m"):
    """Single-range query. Prefer _query_chunked for 1s/5s/15s or long ranges."""
    try:
        url = f"http://{host}:{port}/query"
        query = f'''SELECT LAST({field}) as value FROM "{measurement}" WHERE unit = '{sensor_name}' 
        AND time >= '{start_str}' AND time <= '{end_str}' GROUP BY time({interval}) FILL(previous)'''
        r = requests.get(url, params={"db": database, "q": query}, timeout=600)
        if r.status_code != 200:
            return None
        data = r.json()
        if not data.get("results") or not data["results"][0].get("series"):
            return None
        s = data["results"][0]["series"][0]
        df = pd.DataFrame(s["values"], columns=s["columns"])
        df["time"] = pd.to_datetime(df["time"])
        df = df.set_index("time")
        return df["value"]
    except Exception:
        return None


def _query_chunked(host, port, database, measurement, sensor_name, field, start_str, end_str, interval="1m"):
    """
    Run query in time chunks and concatenate. Much faster for 1s resolution over long ranges.
    """
    chunks = _chunk_time_ranges(start_str, end_str, interval)
    if len(chunks) <= 1:
        return _query(host, port, database, measurement, sensor_name, field, start_str, end_str, interval)
    series = []
    for cs, ce in chunks:
        part = _query(host, port, database, measurement, sensor_name, field, cs, ce, interval)
        if part is None:
            continue
        series.append(part)
    if not series:
        return None
    combined = pd.concat(series).sort_index()
    combined = combined[~combined.index.duplicated(keep="last")]
    return combined


def _get_all_field_keys(host, port, database, measurement):
    """Get ALL field keys for a measurement (float, integer, boolean, any type)."""
    try:
        url = f"http://{host}:{port}/query"
        q = f'SHOW FIELD KEYS FROM "{measurement}"'
        r = requests.get(url, params={"db": database, "q": q}, timeout=60)
        if r.status_code != 200:
            return []
        data = r.json()
        if not data.get("results") or not data["results"][0].get("series"):
            return []
        rows = data["results"][0]["series"][0].get("values", [])
        return [row[0] for row in rows if len(row) >= 1]
    except Exception:
        return []


def _get_fields_with_data_for_unit(host, port, database, measurement, unit, all_fields):
    """For a given unit, discover which fields have data. Uses SELECT * and checks for non-null.
    Returns list of (field_name,) that have at least one non-null value."""
    try:
        url = f"http://{host}:{port}/query"
        q = f'SELECT * FROM "{measurement}" WHERE unit = \'{unit}\' LIMIT 200'
        r = requests.get(url, params={"db": database, "q": q}, timeout=60)
        if r.status_code != 200:
            return _probe_each_field(host, port, database, measurement, unit, all_fields)
        data = r.json()
        if not data.get("results") or not data["results"][0].get("series"):
            return _probe_each_field(host, port, database, measurement, unit, all_fields)
        s = data["results"][0]["series"][0]
        cols = s.get("columns", [])
        vals = s.get("values", [])
        if not vals:
            return _probe_each_field(host, port, database, measurement, unit, all_fields)
        out = []
        for c in cols:
            if c in ("time", "unit") or c not in all_fields:
                continue
            idx = cols.index(c)
            for row in vals:
                if idx < len(row) and row[idx] is not None:
                    out.append(c)
                    break
        return out if out else _probe_each_field(host, port, database, measurement, unit, all_fields)
    except Exception:
        return _probe_each_field(host, port, database, measurement, unit, all_fields)


def _probe_each_field(host, port, database, measurement, unit, fields):
    """Fallback: probe each field with a quick query to see which have data."""
    out = []
    try:
        url = f"http://{host}:{port}/query"
        for f in fields:
            q = f'SELECT "{f}" FROM "{measurement}" WHERE unit = \'{unit}\' LIMIT 1'
            r = requests.get(url, params={"db": database, "q": q}, timeout=30)
            if r.status_code != 200:
                continue
            data = r.json()
            if data.get("results") and data["results"][0].get("series"):
                vals = data["results"][0]["series"][0].get("values", [])
                if vals and len(vals[0]) > 1 and vals[0][1] is not None:
                    out.append(f)
    except Exception:
        pass
    return out


def _auto_detect(host, port, database, unit_name):
    """Returns list of (col_name, unit, field) for each sensor/column to retrieve.
    Universal: for every unit, discover which fields have data, then retrieve each."""
    try:
        url = f"http://{host}:{port}/query"
        q = f'SHOW TAG VALUES FROM "{unit_name}" WITH KEY = "unit"'
        r = requests.get(url, params={"db": database, "q": q}, timeout=60)
        if r.status_code != 200:
            return []
        data = r.json()
        if not data.get("results") or not data["results"][0].get("series"):
            return []
        units = [row[1] for row in data["results"][0]["series"][0]["values"]]
        all_fields = _get_all_field_keys(host, port, database, unit_name)
        if not all_fields:
            all_fields = ["value_f", "value_b", "value_i"]
        out = []
        for unit in units:
            populated = _get_fields_with_data_for_unit(host, port, database, unit_name, unit, all_fields)
            for field in populated:
                col_name = field if field not in ("value_f", "value_b", "value_i") else unit
                out.append((col_name, unit, field))
        return out
    except Exception:
        return []


def _write_metadata(filepath, host, port, db, unit, start, end, ok, fail, prefix, start_time="00:00:00", end_time="23:59:59", df=None, sensor_mapping=None, failed_mapping=None, resolution="1m"):
    try:
        p = filepath.replace(".csv", "_metadata.txt")
        if df is None:
            try:
                df = pd.read_csv(filepath, index_col=0)
            except Exception:
                df = pd.DataFrame()
        total_points = len(df)
        sensor_mapping = sensor_mapping or {}
        failed_mapping = failed_mapping or {}
        res_desc = _resolution_to_description(resolution)
        with open(p, "w") as f:
            f.write(f"{prefix} {unit} Universal Auto-Detection Export\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Unit: {unit}\n")
            f.write(f"Time Period: {start} {start_time} to {end} {end_time}\n")
            f.write(f"Resolution: {res_desc}\n")
            f.write(f"Database: {host}:{port}/{db}\n\n")
            f.write(f"Successful Sensors ({len(ok)}):\n")
            for s in ok:
                f.write(f"  - {s}\n")
            if fail:
                f.write(f"\nFailed Sensors ({len(fail)}) - no data in time range:\n")
                for s in fail:
                    ut, fi = failed_mapping.get(s, ("?", "?"))
                    f.write(f"  - {s} <- unit={ut}, field={fi}\n")
            f.write(f"\nTotal Data Points: {total_points:,}\n")
            if sensor_mapping:
                f.write(f"\nField Mapping (column -> unit tag, InfluxDB field):\n")
                for col in ok:
                    ut, fi = sensor_mapping.get(col, ("?", "?"))
                    f.write(f"  {col} <- unit={ut}, field={fi}\n")
            if df is not None and not df.empty and sensor_mapping:
                f.write(f"\nPer-column stats:\n")
                for col in ok:
                    if col not in df.columns:
                        continue
                    ser = df[col]
                    nn = ser.notna().sum()
                    try:
                        num = pd.to_numeric(ser, errors="coerce")
                        valid = num.dropna()
                        mn = valid.min() if len(valid) else None
                        mx = valid.max() if len(valid) else None
                        stats = f"non-null={nn:,}"
                        if mn is not None and mx is not None:
                            stats += f", min={mn:.4g}, max={mx:.4g}"
                        f.write(f"  {col}: {stats}\n")
                    except Exception:
                        f.write(f"  {col}: non-null={nn:,}\n")
    except Exception:
        pass
