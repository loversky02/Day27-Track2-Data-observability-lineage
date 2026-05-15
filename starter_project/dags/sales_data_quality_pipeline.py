from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:  # pragma: no cover
    DAG = None

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))


def validate_orders_task() -> dict:
    from src.config import AIRFLOW_INPUT_FILE, DISCORD_WEBHOOK_URL, OUTPUT_DIR
    from src.validation import LabValidationError, build_summary, read_rows, send_discord_message, write_summary

    rows = read_rows(AIRFLOW_INPUT_FILE)
    summary = build_summary(rows)
    output_file = write_summary(summary, OUTPUT_DIR / "validation_summary.json")

    if DISCORD_WEBHOOK_URL:
        send_discord_message(summary, DISCORD_WEBHOOK_URL)

    if summary["validation_status"] == "failed":
        raise LabValidationError(f"Validation failed. Summary saved to {output_file}")

    return summary


if DAG is not None:
    with DAG(
        dag_id="sales_data_quality_pipeline",
        start_date=datetime(2024, 1, 1),
        schedule=None,
        catchup=False,
        tags=["lab", "data-quality", "discord"],
    ) as dag:
        validate_orders = PythonOperator(
            task_id="validate_orders",
            python_callable=validate_orders_task,
        )
else:  # pragma: no cover
    dag = None
