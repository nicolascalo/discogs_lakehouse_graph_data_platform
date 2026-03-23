
rm -fv ./logs/discogs_test_silver_*
rm -fv ./metadata/discogs_test_silver_*
rm -rfv ./metadata/discogs_test_silver_*/
mc mb local/discogs

mc rm --recursive --force local/discogs/discogs_test/silver/
mc rm --recursive --force local/discogs/discogs_testmin/

docker exec spark-master python3 /app/uc_reset_silver.py

#docker exec --env ENV=testmin spark-master python3 /app/download_raw.py
#docker exec --env ENV=testmin spark-master python3 /app/download_raw.py

docker exec spark-master python3 /app/bronze_to_silver_subtables.py
docker exec spark-master python3 /app/bronze_to_silver_subtables.py

