SELECT
  event_date,
  patient_id,

  COUNT(*) AS measurements_total,
  COUNTIF(is_glucose_valid) AS measurements_valid,
  COUNTIF(is_glucose_null) AS measurements_null,

  SAFE_DIVIDE(COUNTIF(is_glucose_valid), COUNT(*)) AS valid_share,

  AVG(valid_glucose_mmol_l) AS avg_glucose,
  MIN(valid_glucose_mmol_l) AS min_glucose,
  MAX(valid_glucose_mmol_l) AS max_glucose,
  STDDEV_SAMP(valid_glucose_mmol_l) AS stddev_glucose,

  SAFE_DIVIDE(
    STDDEV_SAMP(valid_glucose_mmol_l),
    AVG(valid_glucose_mmol_l)
  ) AS cv_glucose,

  SAFE_DIVIDE(
    COUNTIF(valid_glucose_mmol_l BETWEEN 3.9 AND 10.0),
    COUNTIF(is_glucose_valid)
  ) AS tir_pct,

  SAFE_DIVIDE(
    COUNTIF(valid_glucose_mmol_l < 3.9),
    COUNTIF(is_glucose_valid)
  ) AS tbr_pct,

  SAFE_DIVIDE(
    COUNTIF(valid_glucose_mmol_l > 10.0),
    COUNTIF(is_glucose_valid)
  ) AS tar_pct

FROM {{ ref("cgm_clean") }}
GROUP BY event_date, patient_id