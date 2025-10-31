-- # VAO DB
-- docker exec -it airflow-postgres-1 psql -U airflow -d airflow
-- Bảng lưu từng lần đếm (phù hợp cho biểu đồ bar/line của bạn)
CREATE TABLE IF NOT EXISTS public.vehicle_counts (
  id           BIGSERIAL PRIMARY KEY,
  camera_id    TEXT        NOT NULL,
  vehicle_type TEXT        NOT NULL,
  count        INTEGER     NOT NULL,
  frame_time   TIMESTAMP   NOT NULL,
  processed_at TIMESTAMP   NOT NULL DEFAULT NOW()
);

-- Chỉ mục tối ưu cho truy vấn theo thời gian & camera/loại xe
CREATE INDEX IF NOT EXISTS idx_vehicle_counts_frame_time    ON public.vehicle_counts (frame_time);
CREATE INDEX IF NOT EXISTS idx_vehicle_counts_camera_type   ON public.vehicle_counts (camera_id, vehicle_type);
