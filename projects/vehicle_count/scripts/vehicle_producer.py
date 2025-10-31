"""
==================================================
Vehicle Kafka Producer
--------------------------------------------------
- Đọc 2 video offline từ thư mục `data/`
- Chuyển từng khung hình (frame) sang base64 JSON
- Gửi dữ liệu liên tục vào Kafka topic "vehicle-stream"
==================================================
"""

import cv2
import base64
import json
import time
from kafka import KafkaProducer
import os

KAFKA_BROKER = "kafka:9092"
TOPIC = "vehicle-stream"
DATA_DIR = "/opt/airflow/projects/vehicle_count/data"
FPS_LIMIT = 5  # số frame/s gửi vào Kafka (giảm tải hệ thống)

def encode_frame(frame):
    """Chuyển frame OpenCV sang chuỗi base64 để gửi qua Kafka."""
    _, buffer = cv2.imencode(".jpg", frame)
    return base64.b64encode(buffer).decode("utf-8")

def stream_video(video_path, producer, camera_id):
    """Đọc video và gửi từng frame vào Kafka."""
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        print(f"[Producer] ⚠️ Không thể mở video: {video_path}")
        return

    fps_interval = 1 / FPS_LIMIT
    print(f"[Producer] 🎥 Streaming from {os.path.basename(video_path)} (camera_id={camera_id})")

    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            print(f"[Producer] ✅ Video {os.path.basename(video_path)} kết thúc.")
            break

        # Mã hóa frame
        frame_b64 = encode_frame(frame)

        # Gửi message
        message = {
            "camera_id": camera_id,
            "timestamp": time.time(),
            "frame_data": frame_b64
        }
        producer.send(TOPIC, value=message)

        # Giới hạn FPS
        time.sleep(fps_interval)

    cap.release()

def main():
    print("[Producer] 🔌 Connecting to Kafka...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    videos = [
        os.path.join(DATA_DIR, "video1.mp4"),
        os.path.join(DATA_DIR, "video2.mp4")
    ]

    for idx, video_path in enumerate(videos, start=1):
        stream_video(video_path, producer, camera_id=f"CAM_{idx}")

    producer.flush()
    print("[Producer] 🏁 All videos processed and sent successfully.")

if __name__ == "__main__":
    main()
