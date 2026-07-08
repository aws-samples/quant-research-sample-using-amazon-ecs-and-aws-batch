#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# Build the arm64 zip deployment package for AgentCore Runtime (codeConfiguration path).
# Per AWS docs: AgentCore Runtime only supports arm64; deps must be aarch64-manylinux2014
# wheels. Layout: deps at zip root + main.py + config_validation.py at root.
set -euo pipefail
cd "$(dirname "$0")"

PYVER="${PYVER:-3.13}"
OUT=deployment_package
ZIP=deployment_package.zip

echo ">> clean"
rm -rf "$OUT" "$ZIP"
mkdir -p "$OUT"

echo ">> install arm64 deps into $OUT"
uv pip install \
  --python-platform aarch64-manylinux2014 \
  --python-version "$PYVER" \
  --target="$OUT" \
  --only-binary=:all: \
  -r requirements.txt

echo ">> zip deps at root"
( cd "$OUT" && zip -rq "../$ZIP" . )

echo ">> add agent source at root"
zip -q "$ZIP" main.py config_validation.py

echo ">> normalize permissions inside zip is implicit; report size"
ls -lh "$ZIP"
unzip -l "$ZIP" | tail -3
echo ">> done: $ZIP"
