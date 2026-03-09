from __future__ import annotations

import os
import json
import math
import random
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

# ------------------------------------------------------------
# Optional: load .env automatically
# ------------------------------------------------------------
try:
    from dotenv import load_dotenv  # type: ignore

    load_dotenv()
except Exception:
    pass


@dataclass(frozen=True)
class Config:
    gcp_project: str

    bronze_dataset: str
    bronze_table: str

    meta_dataset: str
    meta_table: str
    pipeline_name: str

    patient_count: int
    cadence_minutes: int
    overlap_minutes: int
    default_start_ts: str

    prefer_generator_py: bool
    source_label: str

    # BigQuery location (EU in your setup)
    bq_location: str

    # Insert strategy
    insert_chunk_size: int          # for streaming inserts
    loadjob_threshold_rows: int     # if rows >= threshold -> use load job (NDJSON)
    request_timeout_s: int          # http timeout for streaming inserts


# ------------------ Time helpers ------------------

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_ts(ts: str) -> datetime:
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts).astimezone(timezone.utc)


def _floor_to_cadence(ts: datetime, cadence_min: int) -> datetime:
    ts = ts.astimezone(timezone.utc).replace(second=0, microsecond=0)
    step = cadence_min * 60
    total_seconds = int((ts - _EPOCH).total_seconds())
    return datetime.fromtimestamp((total_seconds // step) * step, tz=timezone.utc)


def _ceil_to_next_cadence(ts: datetime, cadence_min: int) -> datetime:
    ts = ts.astimezone(timezone.utc).replace(second=0, microsecond=0)
    floored = _floor_to_cadence(ts, cadence_min)
    if floored == ts:
        return floored + timedelta(minutes=cadence_min)
    return floored + timedelta(minutes=cadence_min)


# ------------------ BigQuery helpers ------------------

def ensure_dataset(client: bigquery.Client, project: str, dataset: str, location: str) -> None:
    ds_id = f"{project}.{dataset}"
    try:
        client.get_dataset(ds_id)
    except NotFound:
        ds = bigquery.Dataset(ds_id)
        ds.location = location
        client.create_dataset(ds, exists_ok=True)


def ensure_meta_table(client: bigquery.Client, cfg: Config) -> None:
    table_id = f"{cfg.gcp_project}.{cfg.meta_dataset}.{cfg.meta_table}"
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{table_id}` (
      pipeline   STRING NOT NULL,
      last_ts    TIMESTAMP,
      updated_at TIMESTAMP NOT NULL
    )
    """
    client.query(ddl, location=cfg.bq_location).result()


def read_watermark(client: bigquery.Client, cfg: Config) -> datetime:
    table_id = f"{cfg.gcp_project}.{cfg.meta_dataset}.{cfg.meta_table}"
    q = f"""
    SELECT last_ts
    FROM `{table_id}`
    WHERE pipeline = @pipeline
    ORDER BY updated_at DESC
    LIMIT 1
    """
    job = client.query(
        q,
        location=cfg.bq_location,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("pipeline", "STRING", cfg.pipeline_name)]
        ),
    )
    rows = list(job.result())
    if not rows or rows[0]["last_ts"] is None:
        return _parse_ts(cfg.default_start_ts)

    last_ts = rows[0]["last_ts"]
    if last_ts.tzinfo is None:
        last_ts = last_ts.replace(tzinfo=timezone.utc)
    return last_ts.astimezone(timezone.utc)


def update_watermark(client: bigquery.Client, cfg: Config, new_last_ts: datetime) -> None:
    table_id = f"{cfg.gcp_project}.{cfg.meta_dataset}.{cfg.meta_table}"
    q = f"""
    INSERT INTO `{table_id}` (pipeline, last_ts, updated_at)
    VALUES (@pipeline, @last_ts, CURRENT_TIMESTAMP())
    """
    client.query(
        q,
        location=cfg.bq_location,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("pipeline", "STRING", cfg.pipeline_name),
                bigquery.ScalarQueryParameter("last_ts", "TIMESTAMP", new_last_ts),
            ]
        ),
    ).result()


# ------------------ Row generation (bronze schema compatible) ------------------
# Required keys:
# timestamp, glucose_mmol_l, patient_id, source, ingested_at

def _generate_rows_fallback(
    start_exclusive: datetime,
    end_inclusive: datetime,
    patient_count: int,
    cadence_minutes: int,
    source_label: str,
) -> Tuple[List[dict], Optional[datetime]]:
    if end_inclusive <= start_exclusive:
        return [], None

    end_aligned = _floor_to_cadence(end_inclusive, cadence_minutes)
    start_aligned = _ceil_to_next_cadence(start_exclusive, cadence_minutes)

    if end_aligned < start_aligned:
        return [], None

    timestamps: List[datetime] = []
    t = start_aligned
    while t <= end_aligned:
        timestamps.append(t)
        t += timedelta(minutes=cadence_minutes)

    ingested_at = _utcnow()

    rows: List[dict] = []
    for i in range(1, patient_count + 1):
        patient_id = f"patient_{i:03d}"

        base = 6.5 + (i % 7) * 0.12
        phase = (i % 10) / 10.0 * math.pi

        for ts in timestamps:
            hour = ts.hour + ts.minute / 60.0
            circ = 0.4 * math.sin(2 * math.pi * (hour / 24.0) - 1.2 + phase)
            noise = random.gauss(0, 0.35)
            glucose = base + circ + noise
            glucose = max(2.2, min(22.0, float(glucose)))

            rows.append(
                {
                    "timestamp": ts.isoformat(),
                    "glucose_mmol_l": float(glucose),
                    "patient_id": patient_id,
                    "source": source_label,
                    "ingested_at": ingested_at.isoformat(),
                }
            )

    return rows, end_aligned


def _generate_rows_using_generator_py(
    start_exclusive: datetime,
    end_inclusive: datetime,
    patient_count: int,
    cadence_minutes: int,
    source_label: str,
) -> Tuple[List[dict], Optional[datetime]]:
    try:
        from cgm_pipeline.generator import generate_cgm_dataset  # type: ignore
    except Exception as e:
        raise ImportError(f"Could not import cgm_pipeline.generator.generate_cgm_dataset: {e}")

    end_aligned = _floor_to_cadence(end_inclusive, cadence_minutes)
    start_aligned = _ceil_to_next_cadence(start_exclusive, cadence_minutes)

    if end_aligned < start_aligned:
        return [], None

    # generator uses [start, end)
    start = start_aligned
    end = end_aligned + timedelta(minutes=cadence_minutes)

    df = generate_cgm_dataset(
        n_patients=patient_count,
        start=start,
        end=end,
        freq_min=cadence_minutes,
        seed=42,
    )

    ingested_at = _utcnow()

    rows: List[dict] = []
    for r in df.itertuples(index=False):
        ts: datetime = r.timestamp
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        ts = ts.astimezone(timezone.utc)

        rows.append(
            {
                "timestamp": ts.isoformat(),
                "glucose_mmol_l": float(r.glucose),
                "patient_id": str(r.patient_id),
                "source": source_label,
                "ingested_at": ingested_at.isoformat(),
            }
        )

    return rows, end_aligned


def generate_rows(
    start_exclusive: datetime,
    end_inclusive: datetime,
    cfg: Config,
) -> Tuple[List[dict], Optional[datetime]]:
    if cfg.prefer_generator_py:
        try:
            return _generate_rows_using_generator_py(
                start_exclusive=start_exclusive,
                end_inclusive=end_inclusive,
                patient_count=cfg.patient_count,
                cadence_minutes=cfg.cadence_minutes,
                source_label=cfg.source_label,
            )
        except ImportError:
            return _generate_rows_fallback(
                start_exclusive=start_exclusive,
                end_inclusive=end_inclusive,
                patient_count=cfg.patient_count,
                cadence_minutes=cfg.cadence_minutes,
                source_label=cfg.source_label,
            )
    return _generate_rows_fallback(
        start_exclusive=start_exclusive,
        end_inclusive=end_inclusive,
        patient_count=cfg.patient_count,
        cadence_minutes=cfg.cadence_minutes,
        source_label=cfg.source_label,
    )


# ------------------ Load to BigQuery ------------------

def _stream_insert_chunked(client: bigquery.Client, table_id: str, rows: List[dict], cfg: Config) -> None:
    chunk = max(1, cfg.insert_chunk_size)
    total = len(rows)

    # Best-effort timeout: různá verze klienta má buď .http nebo ._http
    try:
        conn = client._connection  # noqa: SLF001
        if hasattr(conn, "http") and conn.http is not None:
            conn.http.timeout = cfg.request_timeout_s
        elif hasattr(conn, "_http") and conn._http is not None:  # noqa: SLF001
            conn._http.timeout = cfg.request_timeout_s  # noqa: SLF001
    except Exception:
        # pokud to nejde nastavit, nevadí – jen poběží default timeouty requests
        pass

    for i in range(0, total, chunk):
        part = rows[i : i + chunk]
        print(f"[INFO] Streaming insert {i+1}-{i+len(part)} / {total} ...")
        errors = client.insert_rows_json(table_id, part)
        if errors:
            raise RuntimeError(f"Streaming insert errors (first 5): {errors[:5]}")


def _load_job_ndjson(client: bigquery.Client, table_id: str, rows: List[dict], cfg: Config) -> None:
    """
    Write NDJSON to a temp file and run a BigQuery load job.
    This is generally faster and more reliable for large batches.
    """
    print(f"[INFO] Using load job (NDJSON) for {len(rows)} rows ...")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".ndjson", delete=True) as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
        f.flush()

        with open(f.name, "rb") as rf:
            job = client.load_table_from_file(
                rf,
                destination=table_id,
                job_config=job_config,
                location=cfg.bq_location,
            )
            job.result()  # wait

    print("[INFO] Load job completed.")


def load_rows_to_bq(client: bigquery.Client, cfg: Config, rows: List[dict]) -> None:
    if not rows:
        return

    table_id = f"{cfg.gcp_project}.{cfg.bronze_dataset}.{cfg.bronze_table}"

    # Strategy: load job for bigger batches, streaming for small ones
    if len(rows) >= cfg.loadjob_threshold_rows:
        _load_job_ndjson(client, table_id, rows, cfg)
    else:
        _stream_insert_chunked(client, table_id, rows, cfg)


# ------------------ Main ------------------

def run() -> None:
    cfg = Config(
        gcp_project=os.environ.get("GCP_PROJECT", "").strip() or "cgm-de-pipeline-personal",
        bronze_dataset=os.environ.get("BRONZE_DATASET", "bronze"),
        bronze_table=os.environ.get("BRONZE_TABLE", "raw_cgm_readings"),
        meta_dataset=os.environ.get("META_DATASET", "meta"),
        meta_table=os.environ.get("META_TABLE", "ingestion_state"),
        pipeline_name=os.environ.get("PIPELINE_NAME", "cgm_microbatch"),
        patient_count=int(os.environ.get("PATIENT_COUNT", "60")),
        cadence_minutes=int(os.environ.get("CADENCE_MINUTES", "5")),
        overlap_minutes=int(os.environ.get("OVERLAP_MINUTES", "10")),
        default_start_ts=os.environ.get("DEFAULT_START_TS", "2025-01-01T00:00:00Z"),
        prefer_generator_py=os.environ.get("PREFER_GENERATOR_PY", "1") in ("1", "true", "True"),
        source_label=os.environ.get("SOURCE_LABEL", "simulator"),
        bq_location=os.environ.get("BQ_LOCATION", "EU"),
        insert_chunk_size=int(os.environ.get("INSERT_CHUNK_SIZE", "500")),
        loadjob_threshold_rows=int(os.environ.get("LOADJOB_THRESHOLD_ROWS", "10000")),
        request_timeout_s=int(os.environ.get("REQUEST_TIMEOUT_S", "60")),
    )

    print(f"[INFO] Project={cfg.gcp_project} Location={cfg.bq_location}")
    client = bigquery.Client(project=cfg.gcp_project)

    # Ensure datasets exist in EU
    ensure_dataset(client, cfg.gcp_project, cfg.meta_dataset, location=cfg.bq_location)
    ensure_dataset(client, cfg.gcp_project, cfg.bronze_dataset, location=cfg.bq_location)

    ensure_meta_table(client, cfg)

    last_ts = read_watermark(client, cfg)
    now_ts = _utcnow()

    start_exclusive = last_ts - timedelta(minutes=cfg.overlap_minutes)
    end_inclusive = now_ts

    print(f"[INFO] Watermark last_ts={last_ts.isoformat()}  window=({start_exclusive.isoformat()} -> {end_inclusive.isoformat()}]")

    rows, last_aligned_ts = generate_rows(start_exclusive, end_inclusive, cfg)

    print(f"[INFO] Generated rows={len(rows)}  aligned_last_ts={None if last_aligned_ts is None else last_aligned_ts.isoformat()}")

    if not rows:
        if last_aligned_ts is not None and last_aligned_ts > last_ts:
            update_watermark(client, cfg, new_last_ts=last_aligned_ts)
            print(f"[OK] No rows generated; watermark advanced to {last_aligned_ts.isoformat()}.")
        else:
            print("[OK] No rows generated; watermark unchanged.")
        return

    load_rows_to_bq(client, cfg, rows)

    # watermark -> last aligned cadence timestamp
    if last_aligned_ts is None:
        last_aligned_ts = _floor_to_cadence(now_ts, cfg.cadence_minutes)

    update_watermark(client, cfg, new_last_ts=last_aligned_ts)

    print(
        f"[OK] Loaded {len(rows)} rows to {cfg.gcp_project}.{cfg.bronze_dataset}.{cfg.bronze_table} | "
        f"aligned_last_ts={last_aligned_ts.isoformat()} watermark updated."
    )


if __name__ == "__main__":
    run()