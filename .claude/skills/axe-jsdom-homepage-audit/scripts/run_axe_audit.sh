#!/usr/bin/env bash
# Run the accessibility test file against a homepage directory.
#
# Usage:
#   run_axe_audit.sh [HOMEPAGE_DIR]
#
# Defaults to the current directory. Expects:
#   - package.json with a "test" script that runs vitest
#   - tests/accessibility/accessibility.test.js (or another file matching the
#     "accessibility" name pattern)
#   - axe-core listed as a devDependency
set -euo pipefail

HOMEPAGE_DIR="${1:-.}"

if [[ ! -d "$HOMEPAGE_DIR" ]]; then
    echo "error: $HOMEPAGE_DIR is not a directory" >&2
    exit 2
fi

if [[ ! -f "$HOMEPAGE_DIR/package.json" ]]; then
    echo "error: no package.json at $HOMEPAGE_DIR — is this a JS project?" >&2
    exit 2
fi

cd "$HOMEPAGE_DIR"

if ! grep -q '"axe-core"' package.json; then
    echo "warning: axe-core not listed in package.json devDependencies." >&2
    echo "         install with: npm install --save-dev axe-core@^4.11.4" >&2
fi

if [[ ! -d node_modules/axe-core ]]; then
    echo "installing dependencies..."
    npm install
fi

# Run only the accessibility test file. Vitest pattern matches by path substring.
exec npm test -- accessibility
