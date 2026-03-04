
rm -fv ./logs/discogs_test_*
rm -fv ./metadata/discogs_test_*
mc mb local/discogs

mc rm --recursive --force local/discogs/discogs_test/
mc rm --recursive --force local/discogs/discogs_testmin/

docker exec spark-master python3 /app/uc_reset.py
docker exec spark-master python3 /app/uc_init.py

#docker exec --env ENV=testmin spark-master python3 /app/download_raw.py
#docker exec --env ENV=testmin spark-master python3 /app/download_raw.py


mc cp ./data_test_files/discogs_test/raw/data/discogs_20251101_* local/discogs/discogs_test/raw/data/
mc cp ./data_test_files/discogs_test/raw/data_archive/discogs_20251101_* local/discogs/discogs_test/raw/data/

docker exec spark-master python3 /app/bronze_ingest.py
docker exec spark-master python3 /app/bronze_ingest.py

mc cp ./data_test_files/discogs_test/raw/data/discogs_20251201_* local/discogs/discogs_test/raw/data/
mc cp ./data_test_files/discogs_test/raw/data_archive/discogs_20251201_* local/discogs/discogs_test/raw/data/

docker exec spark-master python3 /app/bronze_ingest.py