#!/usr/bin/env bash
set -euo pipefail
shopt -s nullglob

# Number of lines to extract
N=100000
PROGRESS_INTERVAL=10000  # log every 10k lines

# Directories
RAW_DIR="."
OUT_RAW_DIR="subset_xml_raw"
OUT_GZ_DIR="subset_xml_gz"

mkdir -p "$OUT_RAW_DIR" "$OUT_GZ_DIR"

# Find all .xml.gz files
files=( "$RAW_DIR"/*.xml.gz )

if [ ${#files[@]} -eq 0 ]; then
    echo "No .xml.gz files found in $RAW_DIR"
    exit 1
fi

echo "Found ${#files[@]} files to process."

for f in "${files[@]}"; do
    echo "-------------------------------------------"
    echo "Processing file: $f"

    base="${f%.gz}"         # file.xml.gz -> file.xml
    stem="${base%.xml}"     # file.xml -> file
    decompressed="$OUT_RAW_DIR/$base"
    sample_xml="$OUT_RAW_DIR/${stem}_sample_${N}.xml"
    sample_gz="$OUT_GZ_DIR/${stem}_sample_${N}.xml.gz"

    # Step 1: Decompress full file
    echo "Decompressing $f → $decompressed"
    gzip -dc "$f" > "$decompressed"

    # Step 2: Extract first N lines with progress
    echo "Extracting first $N lines → $sample_xml"
    awk -v N="$N" -v interval="$PROGRESS_INTERVAL" 'NR <= N { print } NR % interval == 0 { print "Processed " NR " lines..." > "/dev/stderr" }' \
        "$decompressed" > "$sample_xml"

    # Step 3: Re-compress sample
    echo "Compressing sample → $sample_gz"
    gzip -c "$sample_xml" > "$sample_gz"

    # Step 4: Remove uncompressed files
    echo "Cleaning up uncompressed files..."
    rm -f "$decompressed" "$sample_xml"

    echo "Done processing $f"
done

echo "All files processed."
