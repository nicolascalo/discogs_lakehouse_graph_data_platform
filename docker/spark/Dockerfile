FROM eclipse-temurin:17-jre-jammy

ENV SPARK_VERSION=4.0.1
ENV DELTA_SPARK_VERSION=4.0.1
ENV UNITY_CATALOG_VERSION=0.4.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# -----------------------------
# System dependencies
# -----------------------------
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        python3 \
        python3-pip \
        tar \
        git \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

# -----------------------------
# Download and extract Spark
# -----------------------------
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-connect.tgz \
    | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-connect /opt/spark


# -----------------------------
# Download Delta + Unity Catalog JARs
# -----------------------------
RUN mkdir -p $SPARK_HOME/jars && cd $SPARK_HOME/jars && \
    curl -fsSL -O https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/${DELTA_SPARK_VERSION}/delta-spark_2.13-${DELTA_SPARK_VERSION}.jar && \
    curl -fsSL -O https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_SPARK_VERSION}/delta-storage-${DELTA_SPARK_VERSION}.jar && \
#    curl -fsSL -O https://repo1.maven.org/maven2/io/unitycatalog/unitycatalog-spark_2.13/${UNITY_CATALOG_VERSION}/unitycatalog-spark_2.13-${UNITY_CATALOG_VERSION}.jar && \
#    curl -fsSL -O https://repo1.maven.org/maven2/io/unitycatalog/unitycatalog-client/${UNITY_CATALOG_VERSION}/unitycatalog-client-${UNITY_CATALOG_VERSION}.jar && \
    curl -fsSL -O https://repo1.maven.org/maven2/com/fasterxml/jackson/dataformat/jackson-dataformat-xml/2.15.0/jackson-dataformat-xml-2.15.0.jar

# -----------------------------
# Install Python packages
# -----------------------------
RUN pip3 install uv

    
RUN uv pip install --system --no-cache-dir \
    pyspark==${SPARK_VERSION} \
    delta-spark==${DELTA_SPARK_VERSION} \
    zstandard>=0.25.0 \
    pyarrow \
    lxml \
    pandas \
    jupyter \
    requests \
    boto3 \
    dotenv \
    ipython \
    psycopg2-binary \
    grpcio>=1.48.1 \
    grpcio-status>=1.48.1 \
    neo4j

    
# -----------------------------
# Set Spark work directory
# -----------------------------
WORKDIR $SPARK_HOME/work-dir

# Entrypoint (optional)
CMD ["/bin/bash"]
