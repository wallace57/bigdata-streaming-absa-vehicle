#!/bin/bash
# ==========================================
# Script: run_producer.sh
# Chức năng:
#   - Khởi động Kafka Producer để gửi khung hình (frame metadata)
#     từ 2 video offline trong thư mục /opt/airflow/projects/vehicle_pipeline/data/
#     vào topic "vehicle-stream"
#   - Nếu gặp lỗi, trả về exit code != 0 để Airflow tự retry
# ==========================================

set -e  # Dừng script ngay nếu có lỗi

echo "[Producer] 🚀 Starting Vehicle Kafka Producer..."
python /opt/airflow/projects/vehicle_count/scripts/vehicle_producer.py

status=$?
if [ $status -eq 0 ]; then
  echo "[Producer] ✅ Completed successfully."
else
  echo "[Producer] ❌ Failed with exit code $status."
fi

exit $status
