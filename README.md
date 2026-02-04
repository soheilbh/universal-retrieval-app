# Universal Data Retrieval App

Simple web UI for downloading sensor and energy data from InfluxDB.  
Works for **Farmsum**, **Teesside**, or any InfluxDB with the same schema.

## Run locally

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Run in Docker

```bash
docker-compose up -d
```

App runs on http://localhost:8502. Farmsum preset defaults to host 172.17.2.3.

## Usage

1. Set **Host** and **Port** (e.g. `localhost:8086` or `172.17.2.3:8086`)
2. Set **Database** (`farmsum_db`, `teesside_db`, etc.)
3. Choose **Prefix** (FRM or TSP)
4. Set **Start** and **End** dates
5. Enter **Units** (comma-separated: `BD361-0, H356-0` or `BD01, CB20B`)
6. Click **Run Retrieval**
7. Download the generated CSV files

Files are saved to `./outputs/` and can be downloaded from the app.
