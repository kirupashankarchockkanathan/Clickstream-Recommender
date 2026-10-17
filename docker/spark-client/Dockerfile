# Spark 3.5.1 client on Java 8 so it matches the Java 8 Hadoop/YARN containers (bde2020 images).
FROM eclipse-temurin:8-jre-jammy
ARG SPARK_VERSION=3.5.1
RUN apt-get update && apt-get install -y --no-install-recommends curl python3 python3-pip procps ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    | tar -xz -C /opt && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark
ENV SPARK_HOME=/opt/spark HADOOP_CONF_DIR=/etc/hadoop/conf YARN_CONF_DIR=/etc/hadoop/conf PATH=/opt/spark/bin:$PATH
# Kafka + JDBC connector jars fetched once so jobs can run with --jars
RUN mkdir -p /opt/extra-jars && cd /opt/extra-jars && for u in \
 org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar \
 org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar \
 org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
 org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
 org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar ; do curl -fsSLO https://repo1.maven.org/maven2/$u ; done
RUN pip3 install --no-cache-dir kafka-python psycopg2-binary pandas numpy matplotlib streamlit pyarrow
WORKDIR /workspace
CMD ["sleep","infinity"]
