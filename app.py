"""
Universal Data Retrieval App
Simple UI for Teesside, Farmsum, or any InfluxDB.
"""
APP_VERSION = "1.1.0"  # Bump when deploying - check sidebar to verify build

import os
import tarfile
import streamlit as st
from pathlib import Path
from datetime import datetime, timedelta

# Allow env overrides for Docker (e.g. INFLUXDB_HOST, INFLUXDB_PORT)
DEFAULT_HOST = os.getenv("INFLUXDB_HOST", "localhost")
DEFAULT_PORT = os.getenv("INFLUXDB_PORT", "8086")
from retrieval.universal_retrieval import run_retrieval as run_sensors
from retrieval.energy_retrieval import run_retrieval as run_energy

# Presets (Farmsum default host for Portainer/Docker)
PRESETS = {
    "Farmsum": {
        "host": os.getenv("INFLUXDB_HOST", "172.17.2.3"),
        "port": "8086",
        "database": "farmsum_db",
        "prefix": "FRM",
        "units": "BD361-0, H356-0, BD361-1, energy_data",
        "start_date": "2024-01-01",
    },
    "Teesside": {
        "host": os.getenv("INFLUXDB_HOST", "localhost"),
        "port": "8086",
        "database": "teesside_db",
        "prefix": "TSP",
        "units": "BD01, CB20B, N-F-430214-21-07905",
        "start_date": "2025-04-04",
    },
    "Custom": None,
}
YESTERDAY = datetime.now().date() - timedelta(days=1)

st.set_page_config(page_title="Data Retrieval", layout="centered")
st.title("Universal Data Retrieval")
st.caption(f"v{APP_VERSION}")

# Preset selector at top
preset = st.radio("Preset", list(PRESETS.keys()), horizontal=True)
p = PRESETS.get(preset)

# Config form
with st.form("config"):
    st.subheader("Connection")
    col1, col2 = st.columns(2)
    with col1:
        host = st.text_input("InfluxDB Host", value=p["host"] if p else DEFAULT_HOST, help="e.g. localhost or 172.17.2.3")
        database = st.text_input("Database", value=p["database"] if p else "farmsum_db", help="farmsum_db, teesside_db, farmsum_time_based, etc.")
    with col2:
        port = st.text_input("Port", value=p["port"] if p else DEFAULT_PORT, help="8086 default for Farmsum and Teesside")
        prefix = st.text_input("Prefix", value=p["prefix"] if p else "FRM", help="FRM, TSP, or custom for future DBs")

    st.subheader("Time range")
    col1, col2 = st.columns(2)
    with col1:
        start_default = datetime.strptime(p["start_date"], "%Y-%m-%d").date() if p else datetime.strptime("2024-01-01", "%Y-%m-%d").date()
        start_date = st.date_input("Start date", value=start_default).strftime("%Y-%m-%d")
    with col2:
        end_date = st.date_input("End date", value=YESTERDAY).strftime("%Y-%m-%d")

    resolution = st.selectbox(
        "Resolution",
        options=["1s", "5s", "15s", "1m", "5m", "15m", "1h"],
        index=3,
        format_func=lambda x: {"1s": "1 second", "5s": "5 seconds", "15s": "15 seconds", "1m": "1 minute (default)",
                               "5m": "5 minutes", "15m": "15 minutes", "1h": "1 hour"}[x],
        help="1s = more detail, larger files; 1m = good for trends.",
        key="resolution",
    )

    st.subheader("Units")
    units_input = st.text_input(
        "Unit names (comma-separated)",
        value=p["units"] if p else "BD361-0, H356-0, BD361-1",
        help="e.g. BD361-0, BD01, CB20B, N-F-430214-21-07905, energy_data",
    )
    units = [u.strip() for u in units_input.split(",") if u.strip()]

    gas_only = st.checkbox(
        "Gas only (in energy_data)",
        value=(preset == "Farmsum"),
        help="Checked: retrieve only gas. Unchecked: discover and retrieve all energy types (gas, electric, etc.)",
        key="gas_only",
    )

    run_clicked = st.form_submit_button("Run Retrieval")

if run_clicked:
    output_dir = Path("./outputs")
    output_dir.mkdir(exist_ok=True)
    config = {
        "host": host,
        "port": port,
        "database": database,
        "prefix": prefix,
        "start_date": start_date,
        "end_date": end_date,
        "output_dir": str(output_dir),
        "resolution": resolution,
    }

    files_created = []
    summaries = []
    def is_energy_unit(u):
        return u.lower() in ("energy", "energy_data")
    n_sensor = len([u for u in units if not is_energy_unit(u)])
    energy_types = ["gas"] if gas_only else None  # None = discover all
    n_energy = 1 if any(is_energy_unit(u) for u in units) else 0
    total_units = n_sensor + n_energy
    prog = st.progress(0)
    log_container = st.empty()
    log_lines = []

    def log_cb(current, total, name, msg):
        line = f"[{current}/{total}] {name}: {msg}"
        log_lines.append(line)
        log_container.code("\n".join(log_lines[-15:]), language="text")

    with st.spinner("Retrieving data (this may take several minutes)..."):
        idx = 0
        for unit in units:
            if is_energy_unit(unit):
                log_lines.append(f"--- {unit} ({idx + 1}/{total_units}) ---")
                log_container.code("\n".join(log_lines[-15:]), language="text")
                cfg = {**config, "energy_types": energy_types}  # ["gas"] or None
                result = run_energy(cfg, progress_callback=log_cb)
                path, summary = result if isinstance(result, tuple) else (result, None)
                if path:
                    files_created.append(path)
                if summary:
                    summaries.append(summary)
                idx += 1
                prog.progress(idx / total_units)
            else:
                log_lines.append(f"--- Unit {unit} ({idx + 1}/{total_units}) ---")
                log_container.code("\n".join(log_lines[-15:]), language="text")
                cfg = {**config, "unit_name": unit}
                result = run_sensors(cfg, progress_callback=log_cb)
                path, summary = result if isinstance(result, tuple) else (result, None)
                if path:
                    files_created.append(path)
                if summary:
                    summaries.append(summary)
                idx += 1
                prog.progress(idx / total_units)

    prog.empty()
    log_container.empty()

    if summaries:
        total_ok = sum(s["successful"] for s in summaries)
        total_fail = sum(s["failed"] for s in summaries)
        total_pts = sum(s["total_points"] for s in summaries)
        lines = [
            f"Successful Sensors ({total_ok}):",
            f"Failed Sensors ({total_fail}):",
            f"Total Data Points: {total_pts:,}",
        ]
        st.text("\n".join(lines))

    if files_created:
        # Create tar.gz archive
        archive_name = f"retrieval_{prefix}_{datetime.now().strftime('%Y%m%d_%H%M')}.tar.gz"
        archive_path = output_dir / archive_name
        with tarfile.open(archive_path, "w:gz") as tar:
            for f in output_dir.iterdir():
                if f.is_file() and not f.name.endswith(".tar.gz"):
                    tar.add(f, arcname=f.name)
        # Remove archived files (keep only the tar.gz)
        for f in output_dir.iterdir():
            if f.is_file() and not f.name.endswith(".tar.gz"):
                f.unlink(missing_ok=True)
        st.success(f"Done! {len(files_created)} file(s) → {archive_name}")
        size_mb = archive_path.stat().st_size / (1024 * 1024)
        data = archive_path.read_bytes()
        st.download_button(f"Download {archive_name} ({size_mb:.1f} MB)", data=data, file_name=archive_name, key=f"dl_archive_{archive_name}")
    else:
        st.error("No data retrieved. Check host, port, database, and dates.")

# Download existing files
st.markdown("---")
st.subheader("Download existing exports")
out = Path("./outputs")
if out.exists():
    files = sorted(out.glob("*.tar.gz"), key=lambda p: p.stat().st_mtime, reverse=True)
    if files:
        col_del, col_clear = st.columns(2)
        with col_del:
            to_delete = st.selectbox("Delete file", options=[f.name for f in files], key="del_select")
            if st.button("Delete selected", key="del_btn"):
                (out / to_delete).unlink(missing_ok=True)
                st.rerun()
        with col_clear:
            confirm_clear = st.checkbox("I confirm", key="clear_confirm")
            if st.button("Clear all", key="clear_btn", disabled=not confirm_clear):
                for f in out.iterdir():
                    if f.is_file():
                        f.unlink(missing_ok=True)
                st.rerun()
        st.markdown("---")
        for i, f in enumerate(files[:10]):
            size_mb = f.stat().st_size / (1024 * 1024)
            data = f.read_bytes()
            st.download_button(f"⬇ {f.name} ({size_mb:.1f} MB)", data=data, file_name=f.name, key=f"exist_{i}_{f.name}")
    else:
        st.caption("No tar.gz files yet.")
else:
    st.caption("No outputs yet.")
