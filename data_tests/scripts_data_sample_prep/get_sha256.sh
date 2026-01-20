#!/usr/bin/env bash
set -euo pipefail

FOLDER="."  # change to your folder

for f in "$FOLDER"/*; do
    [ -f "$f" ] || continue
    sha256sum "$f"
done
