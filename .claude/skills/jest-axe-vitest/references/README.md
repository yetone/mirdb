# Jest Axe + Vitest Accessibility Testing

## Overview

How to wire `jest-axe` into the MirDB frontend's Vitest setup so accessibility audits can be run from any test file with `axe(container)`. The `toHaveNoViolations` matcher is registered once globally; individual tests filter violations to "serious" or "critical" and surface a readable failure summary.

## When to Use This Skill

Use this skill when:

- Adding accessibility audits to an existing or new test file in `frontend/tests/`
- Diagnosing why JSDOM-driven axe runs report unexpected color-contrast or missing-style failures
- Wiring `expect.extend(toHaveNoViolations)` into a new test runner or replacement setup file
- You need to filter axe results to only the violations that should block CI

## Core Capabilities

### 1. Register the matcher once in the shared setup

`frontend/tests/setup.ts` imports `toHaveNoViolations` and extends Vitest's `expect`:

```ts
import '@testing-library/jest-dom/vitest';
import { toHaveNoViolations } from 'jest-axe';
import { expect } from 'vitest';

expect.extend(toHaveNoViolations);
```

After this, any test in `frontend/tests/` can:

```ts
import { axe } from 'jest-axe';
const { container } = render(<Component />);
const results = await axe(container);
expect(results).toHaveNoViolations();
```

The setup file is referenced by `vite.config.ts` through `test.setupFiles`.

### 2. Filter to serious/critical impact only

axe-core reports four impact levels (minor / moderate / serious / critical). For CI, MirDB only blocks on serious/critical:

```ts
const SERIOUS_OR_CRITICAL = new Set(['serious', 'critical']);
const blocking = results.violations.filter((v) =>
  SERIOUS_OR_CRITICAL.has(v.impact ?? '')
);
expect(blocking).toHaveLength(0);
```

This keeps lower-impact suggestions visible without blocking the build.

### 3. Handle JSDOM color-contrast limitations

JSDOM does not apply external CSS, so `color-contrast` returns `rgba(0,0,0,0)` for elements whose colour comes from a stylesheet. Two workable strategies:

```ts
// Strategy A: disable the rule in the default audit
const results = await axe(container, {
  rules: { 'color-contrast': { enabled: false } },
});

// Strategy B: keep the rule on, filter out unevaluatable nodes
const realBlocking = blocking.filter(
  (v) => v.id !== 'color-contrast' || (v.nodes && v.nodes.length > 0)
);
```

### 4. Produce readable failure messages

Rather than relying on the matcher's default output, summarise violations so the failure includes the rule, the impact, the offending HTML, and axe's `failureSummary`:

```ts
function summariseViolations(violations) {
  return violations.map((v) => {
    const node = v.nodes?.[0];
    return `  - [${v.impact}] ${v.id}: ${v.help}\n      html: ${node?.html ?? ''}\n      why: ${node?.failureSummary ?? ''}`;
  }).join('\n');
}
```

## Best Practices

- Always register the matcher in the shared setup file, never per test, so the API is uniform across the suite.
- Filter to `serious`/`critical` impact to keep the audit pragmatic; address minor/moderate issues separately.
- Disable `color-contrast` in JSDOM-only contexts; exercise it in a real browser via Playwright or a visual smoke test instead.
- Use a dedicated `tests/accessibility/` folder for integration audits so they can be run/skipped independently.

## Resources

### references/

- `README.md` - This documentation

## Source examples in this repo

- `frontend/tests/setup.ts` — global matcher registration
- `frontend/tests/accessibility/homepage.a11y.test.tsx` — usage with filter + summariser
