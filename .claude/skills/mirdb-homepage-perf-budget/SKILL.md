---
name: mirdb-homepage-perf-budget
description: Performance and bundle-size regression suite for the MirDB homepage. Use when adding or updating tests under `frontend/tests/performance/` that need to assert JSDOM render time, sibling re-render isolation, dist/ HTML+CSS payload under the NFR-3 500KB budget, absence of authenticated-API calls on the public homepage, or absence of render-blocking third-party scripts.
scope: project
---

See [README.md](references/README.md) for full documentation.
