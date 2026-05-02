#!/usr/bin/env bash
# Seeds the local Polaris + MinIO demo environment with Iceberg tables and data.
# Run this once after: docker compose up -d
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
JAR="$(ls "$SCRIPT_DIR"/../build/libs/polaris-sql-engine-*-demo.jar 2>/dev/null | head -1)"

if [[ -z "$JAR" ]]; then
  echo "Demo jar not found. Build it first with:"
  echo "  ./gradlew :extensions:sql-engine:shadowJar"
  exit 1
fi

echo "Waiting for Polaris to be ready..."
for i in $(seq 1 30); do
  curl -sf http://localhost:8182/q/health > /dev/null && break
  echo "  attempt $i/30..."
  sleep 5
done

java -cp "$JAR" org.apache.polaris.sql.cli.DemoDataSeeder "$SCRIPT_DIR/demo.properties"
