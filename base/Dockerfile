FROM apache/airflow:2.9.0

USER root

# Cài Java (cho Spark)
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Nếu là win thì chạy đoạn này
#ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
#ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Nếu là mac thì chạy đọan này
# Start for Mac chip M
# ==== Cài Java 17 ====
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# ==== Tự dò JAVA_HOME và thiết lập vĩnh viễn ====
# Hoạt động cho cả amd64/arm64 vì lấy theo /usr/bin/java thực tế
RUN set -eux; \
    JAVA_BIN="$(readlink -f /usr/bin/java)"; \
    JAVA_HOME_DIR="$(dirname "$(dirname "$JAVA_BIN")")"; \
    # tạo alias ổn định
    ln -sf "$JAVA_HOME_DIR" /usr/lib/jvm/default-jre; \
    # để non-login shells (Airflow tasks) nhìn thấy
    echo "JAVA_HOME=$JAVA_HOME_DIR" > /etc/environment; \
    # để login shells / interactive cũng thấy
    printf 'export JAVA_HOME=%s\nexport PATH=$JAVA_HOME/bin:$PATH\n' "$JAVA_HOME_DIR" > /etc/profile.d/java_home.sh; \
    chmod +x /etc/profile.d/java_home.sh; \
    # sanity check
    "$JAVA_HOME_DIR/bin/java" -version \

# End for Mac chip M

# Copy file requirements trong chính thư mục base
COPY base/requirements.txt /tmp/base_requirements.txt

USER airflow
RUN pip install --no-cache-dir -r /tmp/base_requirements.txt && \
    # Fix lỗi kafka.vendor.six.moves không tồn tại trong kafka-python 2.0.1
    sed -i "s|from kafka.vendor.six.moves import range|from six.moves import range|" \
    /home/airflow/.local/lib/python3.*/site-packages/kafka/codec.py || true

# ================================
# 🔧 Copy scripts và cấp quyền thực thi (chuẩn reproducible)
# ================================
USER root
COPY projects/absa_streaming/scripts /opt/airflow/projects/absa_streaming/scripts
RUN chmod +x /opt/airflow/projects/absa_streaming/scripts/*.sh
# Copy thư mục streamlit của project vào image
COPY projects/absa_streaming/streamlit /opt/airflow/projects/absa_streaming/streamlit

# Project VEHICLE COUNTING mới
COPY projects/vehicle_count/scripts /opt/airflow/projects/vehicle_count/scripts
COPY projects/vehicle_count/data /opt/airflow/projects/vehicle_count/data
COPY projects/vehicle_count/streamlit /opt/airflow/projects/vehicle_count/streamlit
RUN chmod +x /opt/airflow/projects/vehicle_count/scripts/*.sh

USER airflow
# Cài các thư viện cần thiết cho Streamlit
USER airflow
RUN pip install --no-cache-dir streamlit psycopg2-binary pandas

# [A] Dùng headless (khuyên dùng)
RUN pip uninstall -y opencv-python opencv-contrib-python
RUN pip install --no-cache-dir opencv-python-headless==4.9.0.80
#RUN pip install --no-cache-dir opencv-python-headless==4.11.0.86
