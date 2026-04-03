#!/usr/bin/env bash
set -euo pipefail

mkdir -p apps/ui apps/ingestion/portals

# Move legacy frontend root files into apps/ui if they exist.
for item in \
  .env index.html package.json package-lock.json postcss.config.js tailwind.config.js \
  tsconfig.json tsconfig.app.json tsconfig.node.json vite.config.ts src; do
  if [[ -e "$item" ]]; then
    target="apps/ui/$item"
    if [[ -e "$target" ]]; then
      echo "Skip $item (already exists at $target)"
    else
      echo "Move $item -> $target"
      mv "$item" "$target"
    fi
  fi
done

# Move legacy parser project out of repo root into ingestion area.
if [[ -d "parser_project" ]]; then
  target="apps/ingestion/portals/parser_project"
  if [[ -e "$target" ]]; then
    echo "Target already exists: $target"
    exit 1
  fi
  echo "Move parser_project -> $target"
  mv parser_project "$target"
fi

# Keep single agent instruction file name from target architecture.
if [[ -f "AGENTS.md" && ! -f "AGENT.md" ]]; then
  echo "Rename AGENTS.md -> AGENT.md"
  mv AGENTS.md AGENT.md
fi

echo "Legacy rehome complete"
