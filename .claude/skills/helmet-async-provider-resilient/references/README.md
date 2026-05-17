# Helmet-Async Provider Resilient

## Overview

react-helmet-async's `<Helmet>` reads its dispatcher context from a parent `<HelmetProvider>`. When `<Helmet>` is rendered without a parent provider, the dispatcher throws `TypeError: Cannot read properties of undefined (reading 'add')` inside `HelmetDispatcher.init`. This skill documents the MirDB-canonical workaround: pass a module-level `HelmetData` instance via Helmet's `helmetData` prop so the component carries its own context.

## When to Use This Skill

Use this skill when users request:

- "Make this Helmet component work without HelmetProvider"
- "Fix sibling-scenario tests breaking because they don't wrap HelmetProvider"
- "Helmet dispatcher add error in tests"
- Any change that introduces a `<Helmet>` element into a tree that is rendered both with AND without `HelmetProvider`

## Core Capabilities

### 1. The pattern

Construct a module-level singleton:

```tsx
import { Helmet, HelmetData, HelmetProvider } from 'react-helmet-async';

const fallbackHelmetData = new HelmetData({}, HelmetProvider.canUseDOM);

export default function SomeComponent() {
  return (
    <Helmet helmetData={fallbackHelmetData}>
      <title>...</title>
    </Helmet>
  );
}
```

### 2. Why it works

Inside react-helmet-async (`lib/index.js` around line 827), when a `helmetData` prop is provided to `<Helmet>` it bypasses the `Context.Consumer` entirely and uses `helmetData.value` as the dispatcher context. The `HelmetData` constructor accepts `(context, canUseDOM)`. With `canUseDOM` set from `HelmetProvider.canUseDOM` (true in browser/JSDOM, false on SSR), the dispatcher's `instances` array lives at the module level, so multiple `HelmetData` objects in client environments all coordinate writes to the same DOM head.

### 3. Coexistence with an outer HelmetProvider

When the component is mounted under main.tsx's `HelmetProvider`, the `helmetData` prop still wins (the provider's context is ignored for this Helmet element). Because `canUseDOM=true` funnels all dispatcher instances into the same module-level array, both paths produce the same merged head state - tags from outer `<Helmet>`s and inner `<Helmet helmetData={...}>` are interleaved correctly.

### 4. When NOT to use

- On SSR: pass an explicit `HelmetData` to render and read collected tags after `renderToString`. Do not rely on a module-level singleton.
- When the component is always rendered under a known provider: the prop is redundant.

## Best Practices

- Construct the `HelmetData` once at module scope, never inside the component body (a new instance per render leaks listeners)
- Build with `HelmetProvider.canUseDOM` so client/JSDOM environments share the global `instances` array
- Annotate the singleton with a comment explaining why - readers will not understand the bypass without it
- Verify by running the FULL test suite (`npx vitest run`) after introducing a `<Helmet>` element, not just the tests for the new component

## Reference Implementation

See `frontend/src/components/homepage/SEOTags.tsx:28` and `:37`.

## Resources

### references/
- `README.md` - This documentation

### Related skills
- `react-helmet-async-seo` - The SEO tag set this pattern was discovered while building
- `helmet-async-testing` - Test-side patterns that motivated this resilience
