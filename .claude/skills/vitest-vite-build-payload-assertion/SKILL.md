---
name: vitest-vite-build-payload-assertion
description: Pattern for asserting Vite production bundle sizes inside a vitest suite by running `npx vite build` lazily in `beforeAll` and walking `dist/`. Use when writing performance or budget tests for any Vite + React project that needs to enforce a per-route HTML+CSS or JS size limit without coupling the test to CI-only build steps.
scope: product
---

See [README.md](references/README.md) for full documentation.
