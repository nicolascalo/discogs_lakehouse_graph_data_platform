#!/usr/bin/env bash
set -euo pipefail
shopt -s nullglob  # skip if no matches

# Number of lines to extract
N=100000

# Output directory for uncompressed XML
OUTDIR="subset_xml_raw"
mkdir -p "$OUTDIR"

# List all .xml.gz files
files=( *.xml.gz )

if [ ${#files[@]} -eq 0 ]; then
    echo "No .xml.gz files found in the current directory."
    exit 1
fi

echo "Found ${#files[@]} files to process."

for f in "${files[@]}"; do
    echo "-------------------------------------------"
    echo "Processing file: $f"

    base="${f%.gz}"         # file.xml.gz -> file.xml
    stem="${base%.xml}"     # file.xml -> file
    out_name="${stem}_sample_${N}.xml"

    # Extract first N lines
    echo "Extracting first $N lines..."
    zcat "$f" | head -n "$N" > "$OUTDIR/$out_name"

    echo "Finished $f → $out_name"
done

echo "All files processed."
