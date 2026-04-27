#!/usr/bin/env bash
set -euo pipefail

# Backward-compatible wrapper from the pre-extraction name. Prefer ./leandex.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/leandex" "$@"
