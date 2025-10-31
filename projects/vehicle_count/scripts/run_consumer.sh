#!/bin/bash
# ==========================================
# Script: run_consumer.sh
# Chức năng:
#   - Khởi động Spark Structured Streaming Consumer
#   - Đọc dữ liệu từ Kafka (các khung hình xe từ nhiều video)
#   - Thực hiện suy luận đếm xe theo thời gian thực bằng YOLO
#   - Ghi kết quả vào PostgreSQL
# ==========================================

set -e  # Dừng script nếu có lỗi

echo "[Consumer] Starting Spark Structured Streaming job..."

#spark-submit \
#  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0 \
#  --master local[*] \
#  /opt/airflow/projects/vehicle_count/scripts/vehicle_consumer.py

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0 \
  /opt/airflow/projects/vehicle_count/scripts/vehicle_consumer.py

status=$?
if [ $status -eq 0 ]; then
  echo "[Consumer] Completed successfully."
else
  echo "[Consumer] Failed with exit code $status."
fi

exit $status
