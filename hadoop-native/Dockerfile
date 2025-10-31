# Dockerfile: hadoop-native/Dockerfile
FROM ubuntu:22.04

RUN apt-get update && \
    apt-get install -y wget tar libsnappy-dev zlib1g-dev build-essential && \
    rm -rf /var/lib/apt/lists/*

ENV HADOOP_VERSION=3.3.6
RUN wget https://downloads.apache.org/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz -O /tmp/hadoop.tar.gz && \
    tar -xzf /tmp/hadoop.tar.gz -C /opt/ && \
    ln -s /opt/hadoop-$HADOOP_VERSION /opt/hadoop

ENV HADOOP_HOME=/opt/hadoop
ENV LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH

# Chỉ để giữ file native, không cần entrypoint
CMD ["bash"]
