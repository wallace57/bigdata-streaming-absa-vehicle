"""
==================================================
🧠 Vehicle Consumer (YOLOv8 + Spark Streaming)
--------------------------------------------------
- Nhận frame từ Kafka topic `vehicle-stream`
- Giải mã base64 → YOLOv8 phát hiện phương tiện
- Tính tổng lượng xe theo camera + thời gian
- Ghi kết quả vào PostgreSQL
==================================================
"""

import base64
import json
import cv2
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, udf, from_unixtime
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)
from ultralytics import YOLO
from pyspark.sql.functions import to_timestamp
# =====================================================
# 1️⃣ Khởi tạo Spark session
# =====================================================
spark = (
    SparkSession.builder
    .appName("VehicleStreamingConsumer")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
        "org.postgresql:postgresql:42.7.1"
    )
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("[Consumer] 🚀 Spark Structured Streaming started...")

# =====================================================
# 2️⃣ Schema của dữ liệu Kafka từ vehicle_producer.py
# =====================================================
schema = StructType([
    StructField("camera_id", StringType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("frame_data", StringType(), True)  # base64 image
])

# =====================================================
# 3️⃣ Đọc dữ liệu từ Kafka
# =====================================================
df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "vehicle-stream")
    .option("startingOffsets", "latest")
    .load()
)

# =====================================================
# 4️⃣ Parse JSON
# =====================================================
df_parsed = (
    df_kafka
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(from_json(col("json_value"), schema).alias("data"))
    .select("data.*")
)

# =====================================================
# 5️⃣ Hàm YOLO detect trong UDF
# =====================================================

yolo_model = YOLO("yolov8n.pt")

def detect_vehicles(frame_b64):
    """Giải mã base64 và đếm số xe + hiển thị log ra console"""
    try:
        img_data = base64.b64decode(frame_b64)
        nparr = np.frombuffer(img_data, np.uint8)
        frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        if frame is None:
            print("[Consumer] ⚠️ Frame decode failed.")
            return 0

        results = yolo_model(frame, verbose=False)
        vehicle_list = []
        count = 0
        for box in results[0].boxes:
            cls_name = yolo_model.names[int(box.cls)]
            if cls_name in ["car", "bus", "truck", "motorbike"]:
                count += 1
                vehicle_list.append(cls_name)

        types_str = ",".join(vehicle_list)
        count = len(vehicle_list)
        print(f"[Kafka → YOLO] 🚗 Detected {count} vehicles in frame.")
        return (count, types_str)
    except Exception as e:
        print(f"[UDF Error] {e}")
        return 0

detect_schema = StructType([
    StructField("count", IntegerType(), True),
    StructField("vehicle_types", StringType(), True)
])

detect_udf = udf(detect_vehicles, detect_schema)

# =====================================================
# 6️⃣ Áp dụng UDF đếm số xe + chuyển đổi timestamp
# =====================================================
df_detected = (
    df_parsed
    .withColumn("detection", detect_udf(col("frame_data")))
    .withColumn("count", col("detection.count"))
    .withColumn("vehicle_type", col("detection.vehicle_types"))
    .withColumn("processed_at", current_timestamp())
    .withColumn("frame_time", from_unixtime(col("timestamp")).cast("timestamp"))
    .select("camera_id", "count", "vehicle_type", "frame_time", "processed_at")
)

# =====================================================
# 7️⃣ Ghi kết quả vào PostgreSQL
# =====================================================
postgres_url = "jdbc:postgresql://postgres:5432/airflow"
postgres_properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

def write_to_postgres(batch_df, batch_id):
    row_count = batch_df.count()
    print(f"\n==============================")
    print(f"[Batch {batch_id}] Writing {row_count} rows to PostgreSQL")
    print("==============================")

    (
        batch_df.write
        .jdbc(
            url=postgres_url,
            table="vehicle_counts",
            mode="append",
            properties=postgres_properties
        )
    )

query = (
    df_detected.writeStream
    .outputMode("update")
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation", "/opt/airflow/checkpoints/vehicle_consumer_cp")
    .start()
)

print("[Consumer] 🧠 Waiting for streaming data...")
query.awaitTermination()
