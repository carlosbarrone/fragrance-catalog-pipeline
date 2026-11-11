#!/bin/bash
set -euo pipefail

MONGO_USER="${MONGO_USER:-mongoadmin}"
MONGO_PASS="${MONGO_PASS:-change_me_mongo_password}"
MONGO_DB="${MONGO_DB:-parfum}"
OUTPUT_FILE="${OUTPUT_FILE:-mongo_results.json}"

echo "Exporting MongoDB results for database '$MONGO_DB'..."

MONGO_QUERY=$(cat <<EOF
const d = db.getSiblingDB("$MONGO_DB");
const latestRun = d.pipeline_runs.find().sort({ completed_at: -1 }).limit(1).toArray()[0] || null;
print(JSON.stringify({
  exported_at: new Date().toISOString(),
  counts: {
    products: d.products.countDocuments({}),
    quarantine_products: d.quarantine_products.countDocuments({}),
    raw_products: d.raw_products.countDocuments({}),
    pipeline_runs: d.pipeline_runs.countDocuments({})
  },
  latest_run: latestRun,
  sample_products: d.products.find().limit(25).toArray(),
  sample_quarantine: d.quarantine_products.find().limit(25).toArray(),
  quarantine_breakdown: d.quarantine_products.aggregate([
    { \$unwind: "\$hard_fail_reasons" },
    { \$group: { _id: "\$hard_fail_reasons", count: { \$sum: 1 } } },
    { \$sort: { count: -1 } }
  ]).toArray()
}, null, 2));
EOF
)

docker compose exec -T mongodb mongosh \
  --authenticationDatabase admin \
  --username "$MONGO_USER" \
  --password "$MONGO_PASS" \
  --quiet \
  --eval "$MONGO_QUERY" > "$OUTPUT_FILE"

echo "Created $OUTPUT_FILE"
ls -lh "$OUTPUT_FILE"
