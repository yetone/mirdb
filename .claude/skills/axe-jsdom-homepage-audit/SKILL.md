---
name: axe-jsdom-homepage-audit
description: Run axe-core WCAG 2 A/AA audits in vitest+jsdom against a component-assembled homepage. Use when the project's test pipeline is vitest+jsdom (not Playwright) and you need to validate WCAG compliance across light and dark themes.
---

Run axe-core WCAG audits in the project's vitest+jsdom pipeline against the fully assembled homepage (inlined components + runtime renderers).

See [README.md](references/README.md) for full documentation, including the assembleHomepage() helper pattern, JSDOM-incompatible rules to disable, and source-CSS contract assertions.

Quick scripted use:

```bash
scripts/run_axe_audit.sh homepage/
```

This runs `npm test -- accessibility` from the given directory and surfaces axe violations with file:line targets.
