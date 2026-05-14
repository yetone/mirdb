# Vitest Astro Integration Testing

## Overview

Pattern for integration testing Astro static sites using Vitest. Builds the Astro site, parses the output HTML with linkedom, and runs assertions against the rendered DOM.

## Build-Test Cycle

```typescript
import { execSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { parseHTML } from 'linkedom';

const HOMEPAGE_DIR = resolve(__dirname, '..', '..');
const DIST_DIR = join(HOMEPAGE_DIR, 'dist');
const INDEX_HTML = join(DIST_DIR, 'index.html');

function buildSite() {
  const astroBin = join(HOMEPAGE_DIR, 'node_modules', 'astro', 'astro.js');
  execSync(`node "${astroBin}" build`, {
    cwd: HOMEPAGE_DIR,
    stdio: 'pipe',
    env: { ...process.env, NODE_ENV: 'production' },
  });
}

function parseBuiltHtml() {
  const html = readFileSync(INDEX_HTML, 'utf-8');
  return parseHTML(html);
}
```

## Test Structure

```typescript
describe('Feature Name', () => {
  let document: Document;

  beforeAll(() => {
    buildSite();
    document = parseBuiltHtml().document;
  });

  describe('Test Case: specific scenario', () => {
    it('validates specific behavior', () => {
      const element = document.querySelector('selector');
      expect(element).not.toBeNull();
    });
  });
});
```

## Important Notes

- Use `node_modules/astro/astro.js` directly (not `npx astro`) for reliability
- Build once in `beforeAll` to avoid repeated compilation
- `linkedom` is preferred over `jsdom` for speed in Node.js
- Set `NODE_ENV=production` to avoid dev-only behavior
- Use `stdio: 'pipe'` to suppress build output in tests

## Vitest Configuration

```typescript
// vitest.config.ts
import { defineConfig } from 'vitest/config';
export default defineConfig({
  test: {
    environment: 'node',
    include: ['tests/**/*.test.ts'],
    globals: true,
  },
});
```

## Related Skills

- `wcag-accessibility-testing` - For accessibility-specific test cases
- `css-contrast-ratio-testing` - For color contrast computation in tests
