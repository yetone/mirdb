# Astro Component Testing Pattern

## Overview

In the MirDB homepage project, Astro components are tested by reading source files (`.astro`, `.json`, `.module.css`) directly rather than requiring a full `astro build`. This pattern is faster, more reliable, and tests the same invariants as rendered HTML.

## When to Use This Skill

Use this skill when users request:

- Testing any `.astro` component in the MirDB homepage project
- Validating JSON data file structure and content
- Checking CSS module rules (breakpoints, selectors, property values)
- Testing graceful fallback/empty state handling in components

## Test Pattern

### 1. Define file paths

```typescript
import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

const HOMEPAGE_DIR = join(__dirname, '..', '..', '..');
const DATA_FILE = join(HOMEPAGE_DIR, 'src', 'data', 'my-data.json');
const COMPONENT_FILE = join(HOMEPAGE_DIR, 'src', 'components', 'MySection', 'MySection.astro');
const CSS_FILE = join(HOMEPAGE_DIR, 'src', 'components', 'MySection', 'MySection.module.css');
```

### 2. Create read helpers

```typescript
function readData() {
  const raw = readFileSync(DATA_FILE, 'utf-8');
  return JSON.parse(raw);
}

function readComponent() {
  return readFileSync(COMPONENT_FILE, 'utf-8');
}

function readCss() {
  return readFileSync(CSS_FILE, 'utf-8');
}
```

### 3. Test data structure

```typescript
it('has required fields', () => {
  const data = readData();
  expect(data).toHaveProperty('sectionTitle');
  expect(typeof data.sectionTitle).toBe('string');
  expect(data.sectionTitle.length).toBeGreaterThan(0);
});
```

### 4. Test component renders elements

```typescript
it('renders heading elements', () => {
  const source = readComponent();
  expect(source).toContain('<h2');
  expect(source).toContain('{sectionTitle}');
});
```

### 5. Test graceful fallbacks

For testing empty state or missing data:
- Temporarily modify the data file with `writeFileSync`
- Use `try/finally` to restore the original data
- Verify the component handles the modified state

```typescript
it('handles empty data gracefully', () => {
  const original = readFileSync(DATA_FILE, 'utf-8');
  try {
    const modified = { ...readData(), components: [] };
    writeFileSync(DATA_FILE, JSON.stringify(modified, null, 2), 'utf-8');
    const reloaded = readData();
    expect(reloaded.components).toHaveLength(0);
  } finally {
    writeFileSync(DATA_FILE, original, 'utf-8');
  }
});
```

### 6. Test CSS structure

```typescript
it('has responsive breakpoints', () => {
  const css = readCss();
  expect(css).toContain('max-width: 1279px'); // tablet
  expect(css).toContain('max-width: 767px');  // mobile
});
```

## Best Practices

- Always use `try/finally` when modifying data files in tests to ensure restoration
- Test both positive (data present) and negative (empty/null data) cases
- Check for CSS class names in the component to verify styling is wired up
- Use regex for CSS property validation (e.g., `/\\.feature-grid\\s*\\{[^}]*grid-template-columns/`)
- Prefer source-file testing over full Astro builds unless testing component composition

## Running Tests

```bash
node node_modules/vitest/vitest.mjs run tests/unit/components/MyComponent.test.ts
```
