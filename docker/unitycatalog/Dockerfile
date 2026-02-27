FROM unitycatalog/unitycatalog:v0.3.1

USER root
RUN apk --no-cache add curl

# Add PostgreSQL JDBC driver
RUN mkdir -p /home/unitycatalog/lib && \
    wget -q https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
    -O /home/unitycatalog/lib/postgresql-42.7.3.jar