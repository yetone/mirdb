---
name: vitest-jsdom-static-html
description: Pattern for testing the MirDB static homepage (no build step) with vitest + jsdom. Use when adding/modifying integration tests under homepage/tests/integration/ that need to load homepage/index.html, query DOM, or simulate clicks. Triggers on file paths matching homepage/tests/integration/*.test.js or when section markup in homepage/sections/*.html changes.
metadata:
  scope: project
---

See [README.md](references/README.md) for full documentation.
