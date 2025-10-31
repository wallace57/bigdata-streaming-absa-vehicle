#!/bin/bash
# ==========================================
# Script: run_producer_cam1.sh
# Chức năng:
#   - Khởi động Kafka Producer cho Camera 1
#   - Đọc video từ: /opt/airflow/projects/vehicle_count/data/video1.mp4
#   - Gửi khung hình (frame metadata) vào topic "vehicle-stream"
#   - Nếu gặp lỗi, trả về exit code != 0 để Airflow tự retry
# ==========================================

set -e  # Dừng script ngay nếu có lỗi

echo "[Producer-CAM1] 🚗 Starting Vehicle Kafka Producer (Camera 1)..."

python /opt/airflow/projects/vehicle_count/scripts/vehicle_producer_single.py CAM_1 /opt/airflow/projects/vehicle_count/data/video1.mp4

status=$?
if [ $status -eq 0 ]; then
  echo "[Producer-CAM1] ✅ Completed successfully."
else
  echo "[Producer-CAM1] ❌ Failed with exit code $status."
fi

exit $status
