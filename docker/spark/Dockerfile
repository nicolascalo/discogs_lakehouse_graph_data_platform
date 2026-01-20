FROM eclipse-temurin:11-jre-jammy

ENV SPARK_VERSION=3.5.2
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# System dependencies
RUN apt-get update && apt-get install -y curl tar python3 python3-pip && rm -rf /var/lib/apt/lists/*

# Download and extract Spark
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala2.13.tgz \
    | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala2.13 /opt/spark

# Download spark-xml jar compatible with Spark 3.5 + Scala 2.13
RUN curl -fsSL https://repo1.maven.org/maven2/com/databricks/spark-xml_2.13/0.15.0/spark-xml_2.13-0.15.0.jar \
    -o ${SPARK_HOME}/jars/spark-xml_2.13-0.15.0.jar

# Download Delta Spark jar (for Spark 3.5 + Scala 2.13)
RUN curl -fsSL https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/3.2.0/delta-spark_2.13-3.2.0.jar \
    -o ${SPARK_HOME}/jars/delta-spark_2.13-3.2.0.jar


# Delta Spark integration (Scala 2.13, version 3.3.2)
RUN curl -fsSL https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/3.3.2/delta-spark_2.13-3.3.2.jar \
    -o ${SPARK_HOME}/jars/delta-spark_2.13-3.3.2.jar

# Delta storage implementation (no Scala suffix)
RUN curl -fsSL https://repo1.maven.org/maven2/io/delta/delta-storage/3.3.2/delta-storage-3.3.2.jar \
    -o ${SPARK_HOME}/jars/delta-storage-3.3.2.jar

    
# Python dependencies
RUN pip3 install --no-cache-dir pyspark==${SPARK_VERSION} delta-spark pyarrow lxml psycopg2-binary neo4j jupyter ipython pandas
