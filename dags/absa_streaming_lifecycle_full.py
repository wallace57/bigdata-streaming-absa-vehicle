# ===========================================
# DAG: ABSA Streaming Lifecycle with Retraining
# ===========================================
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os, subprocess, json

# === Default args ===
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# === Helper functions ===
MODEL_DIR = "/opt/airflow/projects/absa_streaming/models"
BEST_MODEL = f"{MODEL_DIR}/best_absa_hardshare.pt"
NEW_MODEL = f"{MODEL_DIR}/candidate_absa.pt"
EVAL_LOG = f"{MODEL_DIR}/eval_result.json"


def monitor_job():
    print("[Monitor] Checking streaming checkpoint...")
    path = "/opt/airflow/checkpoints/absa_streaming_checkpoint"
    if os.path.exists(path):
        size = subprocess.check_output(["du", "-sh", path]).decode().split()[0]
        print(f"[Monitor] Checkpoint exists ({size}) → job running normally.")
    else:
        print("[Monitor] No checkpoint found. Possibly failed or cleaned.")


def evaluate_model():
    """
    Đọc file eval_result.json (output của training script)
    {
        "new_model_acc": 0.83,
        "best_model_acc": 0.80
    }
    Nếu mô hình mới tốt hơn → ghi log cho task update_model.
    """
    if not os.path.exists(EVAL_LOG):
        raise FileNotFoundError(f"Evaluation result not found at {EVAL_LOG}")

    with open(EVAL_LOG) as f:
        result = json.load(f)
    new_acc = result.get("new_model_acc", 0)
    best_acc = result.get("best_model_acc", 0)

    print(f"[Evaluate] Old model acc: {best_acc}, New model acc: {new_acc}")

    # Quyết định update hay không
    if new_acc > best_acc:
        print("[Evaluate] ✅ New model is better. Marking for deployment.")
        return "update_model"
    else:
        print("[Evaluate] ❌ New model is worse. Skip update.")
        return "skip_update"


def update_model():
    """Cập nhật mô hình mới vào vị trí deploy (overwrite best_absa_hardshare.pt)"""
    print("[Update] Deploying new ABSA model...")
    if not os.path.exists(NEW_MODEL):
        raise FileNotFoundError(f"Candidate model not found: {NEW_MODEL}")
    subprocess.run(["cp", "-f", NEW_MODEL, BEST_MODEL], check=True)
    print(f"[Update] ✅ Model updated → {BEST_MODEL}")


# === DAG Definition ===
with DAG(
    dag_id="absa_streaming_lifecycle_full",
    default_args=default_args,
    description="ABSA Streaming + Retraining Orchestration",
    schedule_interval=timedelta(hours=1),
    start_date=days_ago(1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=55),
    tags=["absa", "streaming", "kafka", "spark", "retraining"],
) as dag:

    # === 1️⃣ Start Producer ===
    deploy_producer = BashOperator(
        task_id="deploy_producer",
        bash_command='bash -c "timeout 45m bash /opt/airflow/projects/absa_streaming/scripts/run_producer.sh"',
        execution_timeout=timedelta(minutes=50),
    )

    # === 2️⃣ Start Consumer (Spark Streaming) ===
    deploy_consumer = BashOperator(
        task_id="deploy_consumer",
        bash_command='bash -c "timeout 45m bash /opt/airflow/projects/absa_streaming/scripts/run_consumer.sh"',
        execution_timeout=timedelta(minutes=50),
    )

    # === 3️⃣ Monitor Checkpoint ===
    monitor_stream = PythonOperator(
        task_id="monitor_stream",
        python_callable=monitor_job,
    )

    # === 4️⃣ Cleanup old checkpoints ===
    cleanup_checkpoints = BashOperator(
        task_id="cleanup_checkpoints",
        bash_command="rm -rf /opt/airflow/checkpoints/absa_streaming_checkpoint || true",
    )

    # === 5️⃣ Retrain model ===
    retrain_model = BashOperator(
        task_id="retrain_model",
        bash_command=(
            "echo '[Train] Starting ABSA retraining...'; "
            "python3 /opt/airflow/projects/absa_streaming/scripts/train_retrain.py "
            f"--output {NEW_MODEL} --eval-log {EVAL_LOG}; "
            "echo '[Train] Done.'"
        ),
    )

    # === 6️⃣ Evaluate model performance ===
    evaluate_model_task = PythonOperator(
        task_id="evaluate_model",
        python_callable=evaluate_model,
        do_xcom_push=True,
    )

    # === 7️⃣ Update model if better ===
    update_model_task = PythonOperator(
        task_id="update_model",
        python_callable=update_model,
    )

    # === 8️⃣ Skip update if not better ===
    skip_update = BashOperator(
        task_id="skip_update",
        bash_command="echo '[Skip] Model not improved, keeping current version.'",
    )

    # === Dependencies ===
    # Streaming path
    [deploy_producer, deploy_consumer] >> monitor_stream >> cleanup_checkpoints

    # Retraining path
    retrain_model >> evaluate_model_task
    evaluate_model_task >> [update_model_task, skip_update]
