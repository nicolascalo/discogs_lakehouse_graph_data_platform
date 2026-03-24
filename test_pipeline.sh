
rm -fv ./logs/discogs_test_*
rm -fv ./metadata/discogs_test_*
rm -rfv ./metadata/discogs_test_*

mc mb local/discogs

mc rm --recursive --force local/discogs/discogs_test/
mc rm --recursive --force local/discogs/discogs_testmin/

docker exec spark-master python3 /app/uc_reset.py

#docker exec --env ENV=testmin spark-master python3 /app/download_raw.py
#docker exec --env ENV=testmin spark-master python3 /app/download_raw.py

mc mb --ignore-existing local/discogs
mc mb --ignore-existing local/discogs/discogs_test
mc mb --ignore-existing local/discogs/discogs_test/raw
mc mb --ignore-existing local/discogs/discogs_test/raw/data

mc cp ./data_test_files/discogs_test/raw/data/discogs_20251201_* local/discogs/discogs_test/raw/data/

docker exec spark-master python3 /app/raw_to_bronze.py
docker exec spark-master python3 /app/raw_to_bronze.py

mc cp ./data_test_files/discogs_test/raw/data/discogs_20260101_* local/discogs/discogs_test/raw/data/

docker exec spark-master python3 /app/raw_to_bronze.py
docker exec spark-master python3 /app/bronze_to_silver_subtables.py
mc cp ./data_test_files/discogs_test/raw/data/discogs_20260201_* local/discogs/discogs_test/raw/data/

docker exec spark-master python3 /app/raw_to_bronze.py
docker exec spark-master python3 /app/bronze_to_silver_subtables.py
#docker exec spark-master python3 /app/bronze_to_silver_subtables.py