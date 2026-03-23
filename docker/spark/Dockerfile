FROM apache/spark:4.1.0-scala2.13-java21-python3-ubuntu

ENV SPARK_VERSION=4.1.0
ENV DELTA_SPARK_VERSION=4.1.0
ENV UNITY_CATALOG_VERSION=0.4.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

USER root


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



# Install Hadoop AWS + AWS SDK v2 for S3A
RUN mkdir -p /opt/spark/jars && \
    curl -L -o /opt/spark/jars/hadoop-aws-3.4.1.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar && \
    curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.661.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.661/aws-java-sdk-bundle-1.12.661.jar && \
    curl -L -o /opt/spark/jars/bundle-2.29.38.jar \
        https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.38/bundle-2.29.38.jar        
        
# -----------------------------
# Download Delta + Unity Catalog JARs
# -----------------------------
RUN mkdir -p $SPARK_HOME/jars && cd $SPARK_HOME/jars && \
    curl -fsSL -O https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/${DELTA_SPARK_VERSION}/delta-spark_2.13-${DELTA_SPARK_VERSION}.jar && \
    curl -fsSL -O https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_SPARK_VERSION}/delta-storage-${DELTA_SPARK_VERSION}.jar && \
    curl -fsSL -O https://repo1.maven.org/maven2/io/unitycatalog/unitycatalog-spark_2.13/${UNITY_CATALOG_VERSION}/unitycatalog-spark_2.13-${UNITY_CATALOG_VERSION}.jar && \
    curl -fsSL -O https://repo1.maven.org/maven2/io/unitycatalog/unitycatalog-client/${UNITY_CATALOG_VERSION}/unitycatalog-client-${UNITY_CATALOG_VERSION}.jar && \
    curl -fsSL -O https://repo1.maven.org/maven2/com/fasterxml/jackson/dataformat/jackson-dataformat-xml/2.15.0/jackson-dataformat-xml-2.15.0.jar

# -----------------------------
# Download Spark Pipelines JAR
# -----------------------------
RUN cd $SPARK_HOME/jars && \
    curl -fsSL -O https://repo1.maven.org/maven2/org/apache/spark/spark-pipelines_2.13/${DELTA_SPARK_VERSION}/spark-pipelines_2.13-${DELTA_SPARK_VERSION}.jar

# -----------------------------
# Install Python packages
# -----------------------------
RUN pip3 install uv
RUN uv pip install --system --no-cache-dir \
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
    httpx \
    httpx[http2] \
    neo4j \
    botocore \
    beautifulsoup4 \
    cloudscraper



    
# -----------------------------
# Set Spark work directory
# -----------------------------
WORKDIR $SPARK_HOME/work-dir

# Entrypoint (optional)
CMD ["/bin/bash"]
