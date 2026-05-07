# Playwright Axe A11y Testing

## Overview

This skill captures the pattern used in `homepage/tests/e2e/accessibility.spec.ts` for running an in-test WCAG 2.1 AA audit using `@axe-core/playwright`. Instead of spinning up the Lighthouse CLI (heavy, HTML report, requires headful Chrome), we run the same WCAG rule set inside a Playwright test, return a structured violations array, and reproduce Lighthouse's rule-pass-rate scoring with assertable JSON.

## When to Use This Skill

Use this skill when:

- A scenario or PRD asks for "Lighthouse accessibility score >= 90 with zero critical violations"
- You need a deterministic, sub-second accessibility check that runs in CI without extra browser binaries
- You want to filter axe violations by impact level (`minor` / `moderate` / `serious` / `critical`) to scope a gate
- A page has color-contrast issues owned by other components/teams that you do not want to block on, but still want logged

## Core Capabilities

### 1. Tagged WCAG audit via AxeBuilder

```ts
import AxeBuilder from '@axe-core/playwright';

const results = await new AxeBuilder({ page })
  .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
  .analyze();
```

The four tags select WCAG 2.0 Level A + AA and WCAG 2.1 Level A + AA — the exact surface called out by PRD NFR-3.

### 2. Critical-only gate

```ts
const critical = results.violations.filter((v) => v.impact === 'critical');
expect(critical).toHaveLength(0);
```

`critical` issues are the ones that actually block assistive tech (missing accessible names, broken landmark structure). `serious` issues like color-contrast are real WCAG failures but often live in design-token territory owned by other scenarios — log them but do not gate on them here.

### 3. Lighthouse-style rule pass-rate

```ts
const totalRulesEvaluated =
  results.violations.length + results.passes.length + results.incomplete.length;
const violationCount = results.violations.length;
const passRate =
  totalRulesEvaluated === 0
    ? 100
    : ((totalRulesEvaluated - violationCount) / totalRulesEvaluated) * 100;
expect(passRate).toBeGreaterThanOrEqual(90);
```

Counts rule TYPES (not flagged DOM nodes). One failing rule with 12 nodes is one failed audit, mirroring how Lighthouse computes its 0–100 score. A single offending rule cannot tank the score, but multiple distinct accessibility problems will.

### 4. Surfacing failures in the test log

Always print the violations summary before asserting so a failing CI run includes the offending rule IDs:

```ts
if (critical.length > 0) {
  console.log('Critical violations:', JSON.stringify(
    critical.map((v) => ({ id: v.id, impact: v.impact, help: v.help })),
    null, 2,
  ));
}
```

## Best Practices

- Always `await page.waitForLoadState('networkidle')` in `beforeEach` so axe sees the final DOM.
- Filter on `impact === 'critical'` only when the test_case wording uses that word — if the wording is "AA conformance", consider including `serious` too.
- Prefer the rule-pass-rate formula above over a flagged-node penalty: it matches Lighthouse semantics and is more stable across runs.
- Add `@axe-core/playwright` and `axe-core` as **devDependencies** of the homepage package, not the root.

## Reference Implementation

`homepage/tests/e2e/accessibility.spec.ts` lines 10–46 (Test Case 1).

## Resources

### references/

- `README.md` — This documentation
