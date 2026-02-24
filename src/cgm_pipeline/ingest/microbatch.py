import os
import math
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List

from google.cloud import bigquery


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

    # Default start, pokud watermark neexistuje (ISO8601, např. 2025-01-01T00:00:00Z)
    default_start_ts: str


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_ts(ts: str) -> datetime:
    # očekává "2025-01-01T00:00:00Z" nebo "2025-01-01T00:00:00+00:00"
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts).astimezone(timezone.utc)


def ensure_meta_table(client: bigquery.Client, cfg: Config) -> None:
    table_id = f"{cfg.gcp_project}.{cfg.meta_dataset}.{cfg.meta_table}"
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{table_id}` (
      pipeline   STRING NOT NULL,
      last_ts    TIMESTAMP,
      updated_at TIMESTAMP NOT NULL
    )
    """
    client.query(ddl).result()


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
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("pipeline", "STRING", cfg.pipeline_name)]
        ),
    )
    rows = list(job.result())
    if not rows or rows[0]["last_ts"] is None:
        return _parse_ts(cfg.default_start_ts)
    # BigQuery vrací naive datetime v UTC (většinou). Normalizuj na tz-aware.
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
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("pipeline", "STRING", cfg.pipeline_name),
                bigquery.ScalarQueryParameter("last_ts", "TIMESTAMP", new_last_ts),
            ]
        ),
    ).result()


def generate_cgm_rows(
    from_ts: datetime,
    to_ts: datetime,
    patient_count: int,
    cadence_minutes: int,
) -> List[dict]:
    """
    Generuje syntetická CGM data v 5min cadence pro N pacientů v intervalu (from_ts, to_ts].
    Vrací list dictů (řádky) připravené pro BigQuery load.
    """
    if to_ts <= from_ts:
        return []

    # zaokrouhli "to" dolů na cadence boundary (volitelné, ale dělá hezké timestampy)
    total_seconds = int((to_ts - datetime(1970, 1, 1, tzinfo=timezone.utc)).total_seconds())
    step = cadence_minutes * 60
    to_aligned = datetime.fromtimestamp((total_seconds // step) * step, tz=timezone.utc)

    # posuň from_ts na další boundary
    from_seconds = int((from_ts - datetime(1970, 1, 1, tzinfo=timezone.utc)).total_seconds())
    from_aligned = datetime.fromtimestamp(((from_seconds // step) + 1) * step, tz=timezone.utc)

    if to_aligned < from_aligned:
        return []

    timestamps = []
    t = from_aligned
    while t <= to_aligned:
        timestamps.append(t)
        t += timedelta(minutes=cadence_minutes)

    rows: List[dict] = []
    ingested_at = _utcnow().isoformat()

    # jednoduchý “diurnal” pattern + pacientský offset + náhodný šum
    for p in range(1, patient_count + 1):
        patient_id = f"P{p:03d}"
        base = 6.0 + (p % 7) * 0.15  # pacientský posun
        phase = (p % 10) / 10.0 * math.pi

        for ts in timestamps:
            hour = ts.hour + ts.minute / 60.0
            circadian = 0.8 * math.sin((hour / 24.0) * 2.0 * math.pi + phase)
            noise = random.gauss(0, 0.35)
            glucose = max(2.2, min(18.0, base + circadian + noise))  # clamp

            rows.append(
                {
                    "patient_id": patient_id,
                    "timestamp": ts.isoformat(),          # BigQuery TIMESTAMP parseable
                    "glucose_mmol_l": float(glucose),     # RAW: nezaokrouhluj tady
                    "source": "simulator",
                    "ingested_at": ingested_at,
                }
            )

    return rows


def load_rows_to_bq(client: bigquery.Client, cfg: Config, rows: List[dict]) -> None:
    if not rows:
        return

    table_id = f"{cfg.gcp_project}.{cfg.bronze_dataset}.{cfg.bronze_table}"
    # Předpoklad: tabulka existuje se schema kompatibilním s klíči v rows.
    # Pokud chceš, můžu dodat i CREATE TABLE DDL.
    errors = client.insert_rows_json(table_id, rows)
    if errors:
        # errors je list per-row. Tohle vyhodí exception a zastaví watermark update.
        raise RuntimeError(f"BigQuery insert_rows_json errors (first 5): {errors[:5]}")


def run() -> None:
    cfg = Config(
        gcp_project=os.environ["GCP_PROJECT"],
        bronze_dataset=os.environ.get("BRONZE_DATASET", "bronze"),
        bronze_table=os.environ.get("BRONZE_TABLE", "raw_cgm_readings"),

        meta_dataset=os.environ.get("META_DATASET", "meta"),
        meta_table=os.environ.get("META_TABLE", "ingestion_state"),
        pipeline_name=os.environ.get("PIPELINE_NAME", "cgm_microbatch"),

        patient_count=int(os.environ.get("PATIENT_COUNT", "60")),
        cadence_minutes=int(os.environ.get("CADENCE_MINUTES", "5")),
        overlap_minutes=int(os.environ.get("OVERLAP_MINUTES", "10")),

        default_start_ts=os.environ.get("DEFAULT_START_TS", "2025-01-01T00:00:00Z"),
    )

    client = bigquery.Client(project=cfg.gcp_project)

    ensure_meta_table(client, cfg)

    last_ts = read_watermark(client, cfg)
    now_ts = _utcnow()

    from_ts = last_ts - timedelta(minutes=cfg.overlap_minutes)
    to_ts = now_ts

    rows = generate_cgm_rows(
        from_ts=from_ts,
        to_ts=to_ts,
        patient_count=cfg.patient_count,
        cadence_minutes=cfg.cadence_minutes,
    )

    # 1) load do bronze
    load_rows_to_bq(client, cfg, rows)

    # 2) watermark posuň až po úspěšném loadu
    update_watermark(client, cfg, new_last_ts=to_ts)

    print(
        f"[OK] Loaded {len(rows)} rows to {cfg.bronze_dataset}.{cfg.bronze_table} "
        f"for window ({from_ts.isoformat()} -> {to_ts.isoformat()}], watermark updated."
    )


if __name__ == "__main__":
    run()