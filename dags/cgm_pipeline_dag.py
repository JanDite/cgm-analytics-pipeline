from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# příkaz na spustění stažení dat pomocí příkazu pro spuštění microbarch
MICROBATCH_CMD = "python /opt/airflow/project/src/cgm_pipeline/ingest/microbatch.py"


# příkaz pro spuštění DBT
# to --select +patient_daily_metrics (gold vrstva) spustí i všehny závislé na ní, tj. prvně to spustí i tu silver vrstvu
DBT_CMD = """
cd $DBT_DIR && \
dbt run \
  --profiles-dir $DBT_PROFILES_DIR \
  --target dev \
  --select +patient_daily_metrics
"""

with DAG(
    dag_id = "cgm_pipeline_15_min",
    start_date = datetime(2026, 3, 1),
    schedule = "*/15 * * * *",
    catchup = False,
    max_active_runs = 1,
    default_args = {
        "retries": 2,
        "retry_delay": timedelta(minutes=2)
    },
    tags = ["cgm", "microbatch", "dbt"]
) as dag:

    ingest_microbatch = BashOperator(
        task_id = "ingest_microbatch",
        bash_command = MICROBATCH_CMD,
        dag = dag
    )

    run_dbt = BashOperator(
        task_id = "run_dbt",
        bash_command = DBT_CMD,
        dag = dag
    )

    ingest_microbatch >> run_dbt
