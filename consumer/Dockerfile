# Base: Official Apache Spark 3.5 (Scala 2.12, Ubuntu 22.04 / temurin-17)
FROM apache/spark:3.5.0

USER root

# Version pins — change here to upgrade the entire Kafka integration
ARG SPARK_VER=3.5.0
ARG KAFKA_VER=3.4.1
ARG SCALA_VER=2.12
ARG POOL2_VER=2.11.1
ARG MAVEN=https://repo1.maven.org/maven2

# Official image is based on eclipse-temurin:17-jammy (Ubuntu 22.04).
# install curl for the JAR downloads + python3-pip for PySpark dependencies.
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        python3-pip \
    && rm -rf /var/lib/apt/lists/*

# ── Kafka Structured Streaming JARs ──────────────────────────────────────────
# Placed directly in Spark's jars/ dir so they are always on the classpath;
# no --packages or --jars flags needed at spark-submit time.
#
#   1. spark-sql-kafka-0-10        Kafka source/sink for Structured Streaming
#   2. spark-token-provider-kafka  token negotiation (required by 1)
#   3. kafka-clients               low-level Kafka producer/consumer
#   4. commons-pool2               connection pooling (required by kafka-clients)
RUN set -eux; \
    JARS=/opt/spark/jars; \
    curl -fSL "${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VER}/${SPARK_VER}/spark-sql-kafka-0-10_${SCALA_VER}-${SPARK_VER}.jar" \
         -o "${JARS}/spark-sql-kafka-0-10_${SCALA_VER}-${SPARK_VER}.jar"; \
    curl -fSL "${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VER}/${SPARK_VER}/spark-token-provider-kafka-0-10_${SCALA_VER}-${SPARK_VER}.jar" \
         -o "${JARS}/spark-token-provider-kafka-0-10_${SCALA_VER}-${SPARK_VER}.jar"; \
    curl -fSL "${MAVEN}/org/apache/kafka/kafka-clients/${KAFKA_VER}/kafka-clients-${KAFKA_VER}.jar" \
         -o "${JARS}/kafka-clients-${KAFKA_VER}.jar"; \
    curl -fSL "${MAVEN}/org/apache/commons/commons-pool2/${POOL2_VER}/commons-pool2-${POOL2_VER}.jar" \
         -o "${JARS}/commons-pool2-${POOL2_VER}.jar"

# Pure-Python packages for PySpark jobs
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# Tell PySpark which Python binary to use on driver and workers
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# apache/spark image user is "spark" (UID 185)
USER spark
