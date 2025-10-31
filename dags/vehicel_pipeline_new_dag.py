# ===========================================
# DAG: Vehicle Streaming Pipeline Orchestration (2 Producers)
# ===========================================
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os, subprocess

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="vehicle_pipeline_new_dag",
    default_args=default_args,
    description="Orchestrate Kafka–Spark–PostgreSQL–Streamlit vehicle counting pipeline (2 Producers)",
    schedule_interval=timedelta(hours=1),
    start_date=days_ago(1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=55),
    tags=["vehicle", "streaming", "kafka", "spark"],
) as dag:

    # === 1️⃣ Start Producer CAM_1 ===
    start_vehicle_producer_cam1 = BashOperator(
        task_id="start_vehicle_producer_cam1",
        bash_command='bash -c "timeout 45m bash /opt/airflow/projects/vehicle_count/scripts/run_producer_cam1.sh"',
        execution_timeout=timedelta(minutes=50),
    )

    # === 2️⃣ Start Producer CAM_2 ===
    start_vehicle_producer_cam2 = BashOperator(
        task_id="start_vehicle_producer_cam2",
        bash_command='bash -c "timeout 45m bash /opt/airflow/projects/vehicle_count/scripts/run_producer_cam2.sh"',
        execution_timeout=timedelta(minutes=50),
    )

    # === 3️⃣ Start Spark Consumer ===
    start_vehicle_consumer = BashOperator(
        task_id="start_vehicle_consumer",
        bash_command='bash -c "timeout 45m bash /opt/airflow/projects/vehicle_count/scripts/run_consumer.sh"',
        execution_timeout=timedelta(minutes=50),
    )

    # === 4️⃣ Monitor job ===
    def monitor_vehicle_job():
        print("[Monitor] Checking vehicle streaming checkpoint...")
        path = "/opt/airflow/checkpoints/vehicle_pipeline_checkpoint"
        if os.path.exists(path):
            size = subprocess.check_output(["du", "-sh", path]).decode().split()[0]
            print(f"[Monitor] Checkpoint exists ({size}) → job running normally.")
        else:
            print("[Monitor] No checkpoint found. Possibly failed or cleaned.")

    monitor_vehicle_stream = PythonOperator(
        task_id="monitor_vehicle_stream",
        python_callable=monitor_vehicle_job,
        trigger_rule="all_done",
    )

    # === 5️⃣ Cleanup checkpoint ===
    cleanup_vehicle_checkpoints = BashOperator(
        task_id="cleanup_vehicle_checkpoints",
        bash_command=(
            "echo '[Cleanup] Removing old vehicle checkpoint...'; "
            "rm -rf /opt/airflow/checkpoints/vehicle_pipeline_checkpoint || true; "
            "echo '[Cleanup] Done.'"
        ),
        trigger_rule="all_done",
    )

    # === DAG Dependencies ===
    [start_vehicle_producer_cam1, start_vehicle_producer_cam2, start_vehicle_consumer] >> monitor_vehicle_stream >> cleanup_vehicle_checkpoints
