"""
Universal sensor data retrieval - self-contained, accepts config.
Works for Farmsum, Teesside, or any InfluxDB with same schema.
"""

import os
import requests
import pandas as pd
from datetime import datetime

def run_retrieval(config, progress_callback=None):
    """
    Run sensor retrieval for one unit.
    config: dict with host, port, database, unit_name, prefix, start_date, end_date, output_dir
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
    total = len(sensors_and_fields)

    for i, (sensor_name, field) in enumerate(sensors_and_fields.items(), 1):
        data = _query(host, port, database, unit_name, sensor_name, field, start_str, end_str)
        if data is None and field != "value":
            data = _query(host, port, database, unit_name, sensor_name, "value", start_str, end_str)
        if data is not None:
            all_data[sensor_name] = data
            successful.append(sensor_name)
            cb(i, total, sensor_name, f"OK ({data.notna().sum():,} values)")
        else:
            failed.append(sensor_name)
            cb(i, total, sensor_name, "NO DATA")

    if not all_data:
        return None, {"name": unit_name, "successful": 0, "failed": len(failed), "total_points": 0}

    df = pd.DataFrame(all_data).sort_index()
    start_clean = start_date.replace("-", "")
    end_clean = end_date.replace("-", "")
    filename = f"{prefix}_{unit_name}_ALL_sensors_1min_{start_clean}_to_{end_clean}.csv"
    filepath = os.path.join(output_dir, filename)
    df.to_csv(filepath)
    _write_metadata(filepath, host, port, database, unit_name, start_date, end_date, successful, failed, prefix)
    summary = {"name": unit_name, "successful": len(successful), "failed": len(failed), "total_points": len(df)}
    return filepath, summary


def _query(host, port, database, measurement, sensor_name, field, start_str, end_str):
    try:
        url = f"http://{host}:{port}/query"
        query = f'''SELECT LAST({field}) as value FROM "{measurement}" WHERE unit = '{sensor_name}' 
        AND time >= '{start_str}' AND time <= '{end_str}' GROUP BY time(1m) FILL(previous)'''
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


def _auto_detect(host, port, database, unit_name):
    try:
        url = f"http://{host}:{port}/query"
        q = f'SHOW TAG VALUES FROM "{unit_name}" WITH KEY = "unit"'
        r = requests.get(url, params={"db": database, "q": q}, timeout=60)
        if r.status_code != 200:
            return {}
        data = r.json()
        if not data.get("results") or not data["results"][0].get("series"):
            return {}
        sensors = [row[1] for row in data["results"][0]["series"][0]["values"]]
        out = {}
        FRM_F = {"s_code", "s_raw", "s_runtime_sec"}
        TSP_F = {"hours_run", "current_percent"}
        FRM_SI = lambda n: any(x in n.lower() for x in ["pressure", "difference", "low", "input"])
        BOOL_PATTERNS = ("_run", "_manual", "alarm", "special_flags", "type", "running", "fault",
                        "emergency_stop_ok", "protection_switch_ok", "Rotation_detection", "start_")
        for s in sensors:
            if s in FRM_F or s in TSP_F:
                out[s] = "value_f"
            elif s.startswith("si_") and FRM_SI(s):
                out[s] = "value_f"
            elif (s.startswith("s_") or s.startswith("si_") or s.endswith("_run") or s.endswith("_manual") or
                  s in ("alarm", "special_flags", "type", "running", "fault", "emergency_stop_ok",
                        "protection_switch_ok", "Rotation_detection") or s.startswith("start_")):
                out[s] = "value_b"
            else:
                out[s] = "value_f"
        return out
    except Exception:
        return {}


def _write_metadata(filepath, host, port, db, unit, start, end, ok, fail, prefix):
    try:
        p = filepath.replace(".csv", "_metadata.txt")
        with open(p, "w") as f:
            f.write(f"{prefix} {unit} Universal Export\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Database: {host}:{port}/{db}\n")
            f.write(f"Period: {start} to {end}\n")
            f.write(f"OK: {len(ok)}, Failed: {len(fail)}\n")
    except Exception:
        pass
