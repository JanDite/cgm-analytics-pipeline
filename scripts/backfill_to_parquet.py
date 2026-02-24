from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.cgm_pipeline.ingest.generator import generate_cgm_dataset

N_PATIENTS = 60
FREQ_MIN = 5
DAYS = 180

def main() -> None:
    end = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    end = end.replace(minute=(end.minute // FREQ_MIN) * FREQ_MIN)
    start = end - timedelta(days=DAYS)

    df = generate_cgm_dataset(N_PATIENTS, start, end, freq_min=FREQ_MIN, seed=42)

    df = df.rename(columns={"glucose": "glucose_mmol_l"})
    df["source"] = "backfill"
    df["ingested_at"] = datetime.now(timezone.utc)

    out_dir = REPO_ROOT / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "backfill_cgm_60p_180d.parquet"
    df.to_parquet(out_path, index=False)

    print("Saved:", out_path)
    print("Rows:", len(df))
    print("Min ts:", df["timestamp"].min())
    print("Max ts:", df["timestamp"].max())
    print("Patients:", df["patient_id"].nunique())

if __name__ == "__main__":
    main()