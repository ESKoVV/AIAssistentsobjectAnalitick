#!/usr/bin/env bash
set -euo pipefail

required_dirs=(
  "configs/prompts"
  "apps/ingestion/rss" "apps/ingestion/telegram" "apps/ingestion/vk" "apps/ingestion/portals"
  "apps/preprocessing/normalization" "apps/preprocessing/language" "apps/preprocessing/cleaning" "apps/preprocessing/deduplication" "apps/preprocessing/geo_enrichment" "apps/preprocessing/enrichment"
  "apps/ml/embeddings" "apps/ml/clustering" "apps/ml/classification" "apps/ml/summarization" "apps/ml/ranking" "apps/ml/evaluation"
  "apps/orchestration/consumers" "apps/orchestration/pipelines" "apps/orchestration/schedulers"
  "apps/api/public" "apps/api/internal" "apps/api/schemas"
  "apps/ui"
  "domain/models" "domain/entities" "domain/value_objects" "domain/services"
  "storage/postgres" "storage/redis" "storage/kafka"
  "prompts/system" "prompts/task" "prompts/validators"
  "tests/unit" "tests/integration" "tests/contract" "tests/eval"
  "docs/architecture" "docs/data_contracts" "docs/decisions" "docs/runbooks" "docs/metrics"
  "notebooks/exploration" "notebooks/experiments"
)

required_files=(
  "AGENT.md" "README.md" "pyproject.toml" ".env.example" "docker-compose.yml"
  "configs/sources.yaml" "configs/regions.yaml" "configs/taxonomy.yaml" "configs/ranking.yaml"
  "configs/prompts/summarization.md" "configs/prompts/topic_labeling.md" "configs/prompts/relevance_check.md"
)

for d in "${required_dirs[@]}"; do
  [[ -d "$d" ]] || { echo "Missing directory: $d"; exit 1; }
done

for f in "${required_files[@]}"; do
  [[ -f "$f" ]] || { echo "Missing file: $f"; exit 1; }
done

for legacy in src index.html package.json postcss.config.js tailwind.config.js tsconfig.json tsconfig.app.json tsconfig.node.json vite.config.ts .env; do
  if [[ -e "$legacy" ]]; then
    echo "Legacy root file still exists: $legacy"
    exit 1
  fi
done

echo "Structure OK"
