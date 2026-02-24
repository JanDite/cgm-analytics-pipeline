
from __future__ import annotations

import numpy as np
import pandas as pd
from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass
class PatientProfile:
    patient_id: str
    baseline: float          # mmol/L
    variability: float       # std dev of noise
    meal_sensitivity: float  # meal spike amplitude multiplier
    hypo_risk: float         # probability per day
    hyper_risk: float        # probability per day

    # Missing-data / signal-loss simulation
    dropout_point_rate: float  # per-reading missing probability (e.g., 0.003 = 0.3%)
    dropout_event_rate: float  # probability of having a contiguous dropout event per day
    dropout_event_min: int     # min event duration in minutes
    dropout_event_max: int     # max event duration in minutes


def make_patient_profiles(n: int, seed: int = 42) -> list[PatientProfile]:
    """
    Create heterogeneous synthetic patient profiles.
    Values are chosen to look plausible for CGM-like mmol/L dynamics,
    but this is not a physiological model.
    """
    rng = np.random.default_rng(seed)
    profiles: list[PatientProfile] = []

    for i in range(1, n + 1):
        pid = f"patient_{i:03d}"

        baseline = float(rng.normal(6.5, 0.8))  # around 6–8 mmol/L
        variability = float(np.clip(rng.normal(0.35, 0.15), 0.15, 0.9))
        meal_sensitivity = float(np.clip(rng.normal(1.0, 0.25), 0.6, 1.6))

        hypo_risk = float(np.clip(rng.normal(0.05, 0.03), 0.0, 0.15))
        hyper_risk = float(np.clip(rng.normal(0.08, 0.04), 0.0, 0.20))

        # Missing points: usually small percentage
        dropout_point_rate = float(np.clip(rng.normal(0.003, 0.002), 0.0, 0.02))  # 0–2%
        # Contiguous outages: e.g. 0–35% of days include one short outage
        dropout_event_rate = float(np.clip(rng.normal(0.12, 0.06), 0.0, 0.35))
        dropout_event_min = 10
        dropout_event_max = 45

        profiles.append(
            PatientProfile(
                patient_id=pid,
                baseline=baseline,
                variability=variability,
                meal_sensitivity=meal_sensitivity,
                hypo_risk=hypo_risk,
                hyper_risk=hyper_risk,
                dropout_point_rate=dropout_point_rate,
                dropout_event_rate=dropout_event_rate,
                dropout_event_min=dropout_event_min,
                dropout_event_max=dropout_event_max,
            )
        )

    return profiles


def _gaussian_bump(t_minutes: np.ndarray, center_min: int, width_min: int, amp: float) -> np.ndarray:
    """Simple smooth spike/dip shape."""
    return amp * np.exp(-0.5 * ((t_minutes - center_min) / width_min) ** 2)


def generate_cgm_for_patient(
    profile: PatientProfile,
    start: datetime,
    end: datetime,
    freq_min: int = 5,
    seed: int | None = None,
) -> pd.DataFrame:
    """
    Generate CGM readings for a single patient between [start, end),
    including missing data (signal loss).

    Missingness representation:
      - most realistic: missing reading == row does not exist
    """
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("start/end must be timezone-aware datetimes (use UTC).")
    if start >= end:
        raise ValueError("start must be < end")

    rng = np.random.default_rng(seed)

    idx = pd.date_range(start=start, end=end, freq=f"{freq_min}min", inclusive="left").tz_convert("UTC")
    n = len(idx)
    if n == 0:
        return pd.DataFrame(columns=["timestamp", "glucose", "patient_id"])

    # minutes since midnight (for circadian + meals + outage windows)
    t_min = (idx.hour * 60 + idx.minute).to_numpy()

    # circadian rhythm (small)
    circ = 0.4 * np.sin(2 * np.pi * (t_min / 1440.0) - 1.2)

    # meals: breakfast ~08:00, lunch ~13:00, dinner ~19:00
    meal_amp_base = 2.0 * profile.meal_sensitivity
    breakfast = _gaussian_bump(t_min, 8 * 60, 55, meal_amp_base * rng.uniform(0.7, 1.2))
    lunch = _gaussian_bump(t_min, 13 * 60, 65, meal_amp_base * rng.uniform(0.8, 1.3))
    dinner = _gaussian_bump(t_min, 19 * 60, 70, meal_amp_base * rng.uniform(0.8, 1.4))
    meals = breakfast + lunch + dinner

    # daily events (hypo/hyper) decided per day
    day_codes = pd.Series(idx.date).astype("category").cat.codes.to_numpy()
    unique_days = np.unique(day_codes)

    event = np.zeros(n, dtype=float)
    for d in unique_days:
        mask = day_codes == d
        # hypo: night dip
        if rng.random() < profile.hypo_risk:
            center = int(rng.integers(2 * 60, 6 * 60))
            event[mask] += _gaussian_bump(t_min[mask], center, 80, -rng.uniform(1.0, 2.2))
        # hyper: afternoon/evening spike
        if rng.random() < profile.hyper_risk:
            center = int(rng.integers(12 * 60, 22 * 60))
            event[mask] += _gaussian_bump(t_min[mask], center, 120, rng.uniform(1.2, 3.0))

    # noise
    noise = rng.normal(0, profile.variability, size=n)

    glucose = profile.baseline + circ + meals + event + noise
    glucose = np.clip(glucose, 2.2, 22.0)  # plausible clamp (mmol/L)

    # ----------------------------
    # Simulate missing readings
    # ----------------------------
    missing = np.zeros(n, dtype=bool)

    # (A) random point dropouts
    if profile.dropout_point_rate > 0:
        missing |= (rng.random(n) < profile.dropout_point_rate)

    # (B) contiguous dropout events per day
    # One short outage event can happen on a given day with probability dropout_event_rate.
    # The outage is modeled as a window [start_min, end_min) in minutes since midnight.
    for d in unique_days:
        if rng.random() < profile.dropout_event_rate:
            mask = day_codes == d

            # safety: ensure event_max <= minutes_in_day
            max_dur = min(profile.dropout_event_max, 24 * 60)
            min_dur = min(profile.dropout_event_min, max_dur)

            # pick random start so that end stays within the day
            start_min = int(rng.integers(0, 24 * 60 - max_dur + 1))
            dur = int(rng.integers(min_dur, max_dur + 1))
            end_min = start_min + dur

            t = t_min[mask]
            missing[mask] |= (t >= start_min) & (t < end_min)

    df = pd.DataFrame(
        {
            "timestamp": idx,
            "glucose": glucose.astype(float),
            "patient_id": profile.patient_id,
        }
    )

    # Most realistic: remove missing rows entirely
    df = df.loc[~missing].reset_index(drop=True)
    return df


def generate_cgm_dataset(
    n_patients: int,
    start: datetime,
    end: datetime,
    freq_min: int = 5,
    seed: int = 42,
) -> pd.DataFrame:
    """
    Generate CGM readings for multiple patients between [start, end).
    Deterministic per patient given (seed).
    """
    profiles = make_patient_profiles(n_patients, seed=seed)

    frames: list[pd.DataFrame] = []
    for p in profiles:
        # deterministic patient-specific seed
        ps = abs(hash((p.patient_id, seed))) % (2**32)
        frames.append(generate_cgm_for_patient(p, start, end, freq_min=freq_min, seed=ps))

    if not frames:
        return pd.DataFrame(columns=["timestamp", "glucose", "patient_id"])

    out = pd.concat(frames, ignore_index=True)

    # Ensure timestamp is UTC tz-aware (BigQuery TIMESTAMP is UTC anyway)
    if out["timestamp"].dt.tz is None:
        out["timestamp"] = out["timestamp"].dt.tz_localize("UTC")
    else:
        out["timestamp"] = out["timestamp"].dt.tz_convert("UTC")

    return out


# Optional helper for "now" alignment
def utc_now_rounded(freq_min: int = 5) -> datetime:
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    minute = (now.minute // freq_min) * freq_min
    return now.replace(minute=minute)