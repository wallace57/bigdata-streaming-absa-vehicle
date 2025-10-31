# ============================================================
# SE363 – Phát triển ứng dụng trên nền tảng dữ liệu lớn
# Khoa Công nghệ Phần mềm – Trường Đại học Công nghệ Thông tin, ĐHQG-HCM
# HopDT – Faculty of Software Engineering, University of Information Technology (FSE-UIT)
#
# streamlit_app.py
# ============================================================
# Real-time Dashboard hiển thị số lượng phương tiện (Vehicle Counting)
# từ pipeline Kafka → Spark → PostgreSQL, tự động cập nhật theo thời gian thực.
# ============================================================

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from streamlit_autorefresh import st_autorefresh
import plotly.express as px
import time

# ============================================================
# ⚙️ Cấu hình kết nối PostgreSQL
# ============================================================
DB_CONFIG = {
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",  # Docker service name
    "port": 5432,
    #"database": "vehicle_db",
    "database": "airflow",
}

# ============================================================
# 🔄 Hàm load dữ liệu an toàn từ PostgreSQL
# ============================================================
@st.cache_data(ttl=5)
def load_data():
    engine = create_engine(
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    try:
        conn = engine.raw_connection()
        try:
            df = pd.read_sql(
                """
                SELECT * FROM vehicle_counts
                ORDER BY processed_at DESC
                LIMIT 200
                """,
                conn
            )
        finally:
            conn.close()
        return df
    except Exception as e:
        st.warning(f"⚠️ Không thể kết nối PostgreSQL: {e}")
        return pd.DataFrame()

# ============================================================
# 🧭 Giao diện chính
# ============================================================
st.set_page_config(page_title="UIT Project : Vehicle Streaming", layout="wide")
st.title("🚗 UIT Project : Real-time Vehicle Counting")
st.caption("Minh hoạ pipeline Kafka → Spark → PostgreSQL → Streamlit (CNPM – UIT)")

# ============================================================
# 🔁 Auto-refresh mỗi 5 giây
# ============================================================
st_autorefresh(interval=5 * 1000, limit=None, key="auto_refresh")

# ============================================================
# 📊 Hiển thị dữ liệu
# ============================================================
df = load_data()

if df.empty:
    st.warning("⏳ Chưa có dữ liệu trong bảng `vehicle_counts`. Hãy đảm bảo producer và consumer đang chạy.")
else:
    # Hiển thị dữ liệu thô
    st.subheader("📝 Dữ liệu gần đây")
    st.dataframe(df.head(10), use_container_width=True)

    # Biểu đồ thống kê tổng hợp
    st.subheader("📈 Thống kê theo loại phương tiện và camera")

    col1, col2 = st.columns(2)

    # =============================
    # 1️⃣ Biểu đồ cột - tổng xe theo loại & camera
    # =============================
    with col1:
        df_grouped = (
            df.groupby(["camera_id", "vehicle_type"])["count"]
            .sum()
            .reset_index()
        )
        fig_bar = px.bar(
            df_grouped,
            x="camera_id",
            y="count",
            color="vehicle_type",
            text_auto=True,
            barmode="group",
            title="Số lượng phương tiện theo Camera & Loại xe"
        )
        st.plotly_chart(fig_bar, use_container_width=True)

    # =============================
    # 2️⃣ Biểu đồ đường - xu hướng đếm xe theo thời gian
    # =============================
    with col2:
        df_line = (
            df.groupby(["frame_time", "vehicle_type"])["count"]
            .sum()
            .reset_index()
        )
        fig_line = px.line(
            df_line,
            x="frame_time",
            y="count",
            color="vehicle_type",
            markers=True,
            title="Xu hướng đếm xe theo thời gian (1 phút/lần)"
        )
        st.plotly_chart(fig_line, use_container_width=True)

    # =============================
    # 3️⃣ Tổng hợp nhanh
    # =============================
    st.subheader("📊 Tổng hợp nhanh")
    total_all = int(df["count"].sum())
    total_cam = df["camera_id"].nunique()
    total_types = df["vehicle_type"].nunique()

    col3, col4, col5 = st.columns(3)
    col3.metric("Tổng số xe đã đếm", f"{total_all:,}")
    col4.metric("Số camera đang hoạt động", total_cam)
    col5.metric("Số loại phương tiện", total_types)
