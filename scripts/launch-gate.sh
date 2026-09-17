#!/usr/bin/env bash
# Local launch gate: canonical acceptance checks without deployment.
#
# Runs (fail-fast, first failure stops the gate):
#   npm ci
#   npm run typegen
#   npm run typecheck
#   npx tsc --noEmit --noUnusedLocals --noUnusedParameters
#   npm test
#   npm run build            (wrangler deploy --dry-run, does NOT deploy)
#   git diff --check
#
# Guarantees:
# - `npm ci` installs strictly from package-lock.json (reproducible tree).
# - Generated/cache/log outputs stay in git-ignored local paths where the
#   CLI supports it (.wrangler/logs, .cache).
# - Git staging (index) is never modified; the script aborts if the index
#   changed after the checks.
#
# Platform assumptions (validated 2026-09-15):
# - macOS arm64, Node 26.8.2, npm 11.19.1.
# - `npm ci` requires network access to the npm registry and wipes and
#   reinstalls node_modules. It must NOT be replaced by
#   `npm ci --offline --dry-run`, which only proves lockfile/cache
#   reproducibility without installing a clean tree.
set -euo pipefail

cd "$(dirname "$0")/.."

export WRANGLER_SEND_METRICS=false
export WRANGLER_LOG_PATH="${WRANGLER_LOG_PATH:-.wrangler/logs/launch-gate.log}"
export npm_config_cache="${npm_config_cache:-.cache/npm}"

staged_before="$(git diff --cached --name-only)"

npm ci
npm run typegen
npm run typecheck
npx tsc --noEmit --noUnusedLocals --noUnusedParameters
npm test
npm run build
git diff --check

staged_after="$(git diff --cached --name-only)"
if [ "$staged_before" != "$staged_after" ]; then
  echo "launch-gate: git index changed during checks; refusing to continue" >&2
  exit 1
fi

echo "launch-gate: all local checks passed, git index untouched"
