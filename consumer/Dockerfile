# Base: Bitnami Spark 3.5 (Scala 2.12, Python 3.11)
FROM bitnami/spark:3.5

USER root

# Version pins — change here to upgrade the entire Kafka integration
ARG SPARK_VER=3.5.0
ARG KAFKA_VER=3.4.1
ARG SCALA_VER=2.12
ARG POOL2_VER=2.11.1
ARG MAVEN=https://repo1.maven.org/maven2

# curl is not present in the bitnami minideb base
RUN install_packages curl

# ── Kafka Structured Streaming JARs ──────────────────────────────────────────
# Placed directly in Spark's jars/ dir so they are always on the classpath;
# no --packages or --jars flags needed at spark-submit time.
#
#   1. spark-sql-kafka-0-10        Kafka source/sink for Structured Streaming
#   2. spark-token-provider-kafka  token negotiation (required by 1)
#   3. kafka-clients               low-level Kafka producer/consumer
#   4. commons-pool2               connection pooling (required by kafka-clients)
RUN set -eux; \
    JARS=/opt/bitnami/spark/jars; \
    curl -fSL "${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VER}/${SPARK_VER}/spark-sql-kafka-0-10_${SCALA_VER}-${SPARK_VER}.jar" \
         -o "${JARS}/spark-sql-kafka-0-10_${SCALA_VER}-${SPARK_VER}.jar"; \
    curl -fSL "${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VER}/${SPARK_VER}/spark-token-provider-kafka-0-10_${SCALA_VER}-${SPARK_VER}.jar" \
         -o "${JARS}/spark-token-provider-kafka-0-10_${SCALA_VER}-${SPARK_VER}.jar"; \
    curl -fSL "${MAVEN}/org/apache/kafka/kafka-clients/${KAFKA_VER}/kafka-clients-${KAFKA_VER}.jar" \
         -o "${JARS}/kafka-clients-${KAFKA_VER}.jar"; \
    curl -fSL "${MAVEN}/org/apache/commons/commons-pool2/${POOL2_VER}/commons-pool2-${POOL2_VER}.jar" \
         -o "${JARS}/commons-pool2-${POOL2_VER}.jar"

# Pure-Python packages for PySpark jobs
# (pyspark itself is provided by the base image via $SPARK_HOME/python)
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

USER 1001
