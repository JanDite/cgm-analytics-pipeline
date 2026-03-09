WITH base AS (
  SELECT
    TIMESTAMP(timestamp) AS ts,
    DATE(TIMESTAMP(timestamp)) AS event_date,
    CAST(patient_id AS STRING) AS patient_id,
    CAST(source AS STRING) AS source,
    TIMESTAMP(ingested_at) AS ingested_at,
    SAFE_CAST(glucose_mmol_l AS FLOAT64) AS glucose_num
  FROM {{ source('bronze', 'raw_cgm_readings') }}
),

final AS (
  SELECT
    ts,
    event_date,
    patient_id,
    source,
    ingested_at,

    glucose_num AS glucose_raw,
    ROUND(glucose_num, 1) AS glucose_mmol_l,

    EXTRACT(HOUR FROM ts) AS hour_of_day,

    (glucose_num IS NULL) AS is_glucose_null,
    (glucose_num IS NOT NULL AND glucose_num BETWEEN 2.2 AND 22.0) AS is_glucose_valid,

    CASE
      WHEN glucose_num BETWEEN 2.2 AND 22.0 THEN ROUND(glucose_num, 1)
    END AS valid_glucose_mmol_l,

    CASE
      WHEN glucose_num IS NULL THEN 'NULL_OR_PARSE_ERROR'
      WHEN glucose_num < 2.2 THEN 'BELOW_SENSOR_RANGE'
      WHEN glucose_num > 22.0 THEN 'ABOVE_SENSOR_RANGE'
      ELSE 'OK'
    END AS dq_reason
  FROM base
)

SELECT *
FROM final
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY patient_id, ts
  ORDER BY ingested_at DESC
) = 1