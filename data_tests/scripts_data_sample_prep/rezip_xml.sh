#!/usr/bin/env bash
set -euo pipefail

# Input directory with plain XML files
INDIR="subset_xml_raw"

# Output directory for gzipped XML
OUTDIR="subset_xml_gz"

mkdir -p "$OUTDIR"

for f in "$INDIR"/*.xml; do
  [ -e "$f" ] || continue

  echo "Gzipping $(basename "$f")"

  gzip -c "$f" > "$OUTDIR/$(basename "$f").gz"
done
