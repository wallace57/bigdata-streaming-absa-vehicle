"""
==================================================
Vehicle Kafka Producer (Single Camera)
--------------------------------------------------
- Gửi khung hình (frame) từ một video offline
  tương ứng với một camera vào Kafka topic.
- Dùng cho Airflow task riêng biệt:
    * run_producer_cam1.sh → CAM_1
    * run_producer_cam2.sh → CAM_2
==================================================
"""

import cv2
import base64
import json
import time
import sys
import os
from kafka import KafkaProducer

# ==========================
# 1️⃣ Cấu hình
# ==========================
KAFKA_BROKER = "kafka:9092"
TOPIC = "vehicle-stream"
FPS_LIMIT = 5  # số frame/s gửi vào Kafka


# ==========================
# 2️⃣ Hàm hỗ trợ
# ==========================
def encode_frame(frame):
    """Chuyển frame OpenCV sang chuỗi base64."""
    _, buffer = cv2.imencode(".jpg", frame)
    return base64.b64encode(buffer).decode("utf-8")


def stream_video(video_path, producer, camera_id):
    """Đọc video và gửi từng frame vào Kafka."""
    if not os.path.exists(video_path):
        print(f"[Producer-{camera_id}] ❌ Không tìm thấy file: {video_path}")
        sys.exit(1)

    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        print(f"[Producer-{camera_id}] ⚠️ Không thể mở video: {video_path}")
        sys.exit(1)

    fps_interval = 1 / FPS_LIMIT
    print(f"[Producer-{camera_id}] 🎥 Bắt đầu stream từ {os.path.basename(video_path)}")

    frame_count = 0
    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            print(f"[Producer-{camera_id}] ✅ Video {os.path.basename(video_path)} kết thúc ({frame_count} frames).")
            break

        frame_b64 = encode_frame(frame)
        message = {
            "camera_id": camera_id,
            "timestamp": time.time(),
            "frame_data": frame_b64
        }

        producer.send(TOPIC, value=message)
        frame_count += 1
        time.sleep(fps_interval)

    cap.release()
    producer.flush()
    print(f"[Producer-{camera_id}] 📤 Gửi thành công {frame_count} frames tới Kafka.")


# ==========================
# 3️⃣ Main
# ==========================
def main():
    if len(sys.argv) != 3:
        print("❗ Cách dùng: python vehicle_producer_single.py <camera_id> <video_path>")
        sys.exit(1)

    camera_id = sys.argv[1]
    video_path = sys.argv[2]

    print(f"[Producer-{camera_id}] 🔌 Kết nối Kafka broker tại {KAFKA_BROKER} ...")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=3
    )

    stream_video(video_path, producer, camera_id)
    print(f"[Producer-{camera_id}] 🏁 Hoàn tất gửi dữ liệu.")


if __name__ == "__main__":
    main()
