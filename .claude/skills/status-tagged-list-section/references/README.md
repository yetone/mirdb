# Status Tagged List Section

## Overview

This skill captures the pattern used by the MirDB homepage `RoadmapSection` (and similar sections) to render a single status-tagged data array as two side-by-side columns with distinct visual styling. The data lives in `src/lib/constants.ts` as one array, the type lives in `src/types/index.ts`, and the section component derives the two groups via `Array.filter`.

## When to Use This Skill

Use this skill when:

- A homepage section needs to compare "completed/shipped" items vs "planned/in-progress" items.
- Items share the same shape but differ by a single status tag.
- You want both color-based and icon-based differentiation (for accessibility / colorblind users).
- The data should be addressable by other sections (status indicator, sitemap, future analytics) without duplication.

## Core Capabilities

### 1. Single-array, status-tagged data shape

Define the type in `src/types/index.ts`:

```ts
export interface RoadmapItem {
  title: string;
  status: 'implemented' | 'planned';
}
```

Place the data in `src/lib/constants.ts` as one tagged array:

```ts
export const ROADMAP_ITEMS = [
  { title: 'Tokio-based async networking with memcached protocol', status: 'implemented' as const },
  { title: 'Memtable with skip list data structure', status: 'implemented' as const },
  { title: 'Minor compaction (memtable to SSTable)', status: 'implemented' as const },
  { title: 'Major compaction (SSTable level compaction)', status: 'implemented' as const },
  { title: 'Raft consensus for distributed operation', status: 'planned' as const },
];
```

### 2. Two-column responsive layout with distinct styling

In the component (`src/components/RoadmapSection.tsx`):

```tsx
const implemented = ROADMAP_ITEMS.filter((item) => item.status === 'implemented');
const planned = ROADMAP_ITEMS.filter((item) => item.status === 'planned');
```

Layout grid: `grid md:grid-cols-2 gap-12` (stacks on mobile, side-by-side at md+).

Color tokens used:

| Status      | Background                                | Icon badge        | Border                                   |
|-------------|-------------------------------------------|-------------------|------------------------------------------|
| implemented | bg-green-50 dark:bg-green-900/20          | bg-green-500      | border-green-200 dark:border-green-800   |
| planned     | bg-amber-50 dark:bg-amber-900/20          | bg-amber-500      | border-amber-200 dark:border-amber-800   |

Inline SVG icons (Heroicons paths) — no runtime icon dependency. `aria-hidden="true"` on the decorative icon span.

### 3. Stable test hooks

Add `data-testid` attributes that the unit tests rely on:

- `data-testid="roadmap-section"` on the `<section>` root
- `data-testid="implemented-list"` / `data-testid="planned-list"` on the `<ul>`s
- `data-testid="implemented-item"` / `data-testid="planned-item"` on each `<li>`

This decouples the assertions from class-name churn.

### 4. Unit tests with React Testing Library + Jest

Tests in `tests/unit/components/<Section>.test.tsx`:

- `getByRole('heading', { level: 2, name: /roadmap/i })` for the section title.
- `getAllByTestId('implemented-item')` length assertion for count.
- `getByText(/raft consensus/i)` for specific item presence.
- `toHaveClass('bg-green-50', 'dark:bg-green-900/20')` for divergent styling assertions.

Run with `npx jest tests/unit/components/<Section>.test.tsx`.

## Best Practices

- Keep the data as ONE array tagged by status — don't split into `IMPLEMENTED_ITEMS` and `PLANNED_ITEMS`. Splitting fragments the type and makes future statuses ('in-progress', 'deprecated') costly to add.
- Differentiate with BOTH color AND icon shape. Color alone fails for colorblind users; icon alone fails when icons are too similar.
- Inline the SVG paths from Heroicons rather than installing an icon package — it keeps the static export small and lets icons inherit color from the parent badge via `text-white`.
- Use Tailwind dark-mode variants (`dark:bg-*-900/20`) so the section renders correctly under both themes without conditional logic.
- Always add `data-testid` hooks before writing tests — selecting on Tailwind classes makes tests brittle.

## Resources

### references/

- `README.md` - This documentation

## Reference implementation

- Component: `homepage/src/components/RoadmapSection.tsx`
- Tests: `homepage/tests/unit/components/RoadmapSection.test.tsx`
- Data: `homepage/src/lib/constants.ts` (`ROADMAP_ITEMS`)
- Type: `homepage/src/types/index.ts` (`RoadmapItem`)
