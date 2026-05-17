# Helmet-Async Testing

## Overview

react-helmet-async writes head tags asynchronously on a microtask after mount and shares document.head across tests in the same JSDOM. This skill documents the MirDB-canonical recipe for asserting against those tags reliably.

## When to Use This Skill

Use this skill when users request:

- "Write tests for a component that uses react-helmet-async"
- "Tests are flaking because head tags aren't present yet"
- "Tests are leaking title/meta between test cases"
- Any new test file that queries `document.title`, `document.querySelector('meta[...]')`, or similar head selectors after rendering a React tree

## Core Capabilities

### 1. Render under an explicit HelmetProvider

Even though the component-under-test may carry its own fallback HelmetData (see `helmet-async-provider-resilient`), tests should still wrap in a provider to mirror production usage:

```tsx
import { HelmetProvider } from 'react-helmet-async';
import { render } from '@testing-library/react';

const renderSEO = (ui: React.ReactNode = <SEOTags />): void => {
  render(<HelmetProvider>{ui}</HelmetProvider>);
};
```

### 2. Strip document.head before AND after each test

react-helmet-async dispatches into the live document. Without manual cleanup, tags from earlier tests survive and pollute assertions:

```tsx
const stripManagedHead = () => {
  document.head.querySelectorAll('title, meta, link').forEach((node) => node.remove());
  document.title = '';
};

beforeEach(stripManagedHead);
afterEach(stripManagedHead);
```

### 3. Always use waitFor for first assertion

The dispatcher commits on a microtask, so `expect(document.title).toBe(...)` immediately after `render()` will race the commit. Wait for the first tag of interest, then make remaining assertions synchronously:

```tsx
await waitFor(() => {
  expect(document.title).toContain('MirDB');
});
expect(document.title).toBe(HOMEPAGE_SEO_DEFAULTS.title);
```

For meta tag assertions, wait for the selector to appear:

```tsx
await waitFor(() => {
  const meta = document.querySelector('meta[name="description"]');
  expect(meta).not.toBeNull();
});
const meta = document.querySelector('meta[name="description"]')!;
expect(meta.getAttribute('content')).toMatch(/short/i);
```

### 4. Assert against the exported defaults constant

Reference `HOMEPAGE_SEO_DEFAULTS.title` (etc.) in assertions rather than re-stating the marketing copy. If the copy ever changes, only one place needs editing and the test continues to match.

## Best Practices

- Always pair `beforeEach(stripManagedHead)` with `afterEach(stripManagedHead)` - one without the other still leaks between files
- Wrap the very first assertion in `waitFor`, then run the rest synchronously - cheaper than waiting once per assert
- Use specific selectors (`meta[property="og:title"]`, `link[rel="canonical"]`) so wait conditions surface real bugs rather than passing on the wrong tag
- Cover prop overrides in their own tests so regressions in the default-vs-override branch are caught

## Reference Implementation

See `frontend/tests/components/homepage/SEOTags.test.tsx` for the full 8-test reference suite.

## Resources

### references/
- `README.md` - This documentation

### Related skills
- `react-helmet-async-seo` - The SEO tag pattern these tests cover
- `helmet-async-provider-resilient` - Why the production component still works in tests that omit HelmetProvider
