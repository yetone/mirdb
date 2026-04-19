# Jest HTML String Testing

## Overview

Test static HTML files using Jest with simple string parsing instead of JSDOM. This approach avoids ESM module compatibility issues that occur when JSDOM imports modern ES modules.

## When to Use This Skill

Use this skill when:

- Testing static HTML page structure with Jest
- Encountering ESM compatibility errors with JSDOM
- Verifying HTML contains expected elements, attributes, or content
- Unit testing for static site generators or HTML-first projects

## Problem

When using `jest-environment-jsdom` with modern packages, you may encounter errors like:

```
Must use import to load ES Module: @exodus/bytes/encoding-lite.js
```

This happens because JSDOM's dependencies use ESM modules that conflict with Jest's CommonJS-based environment.

## Solution

Instead of parsing HTML with JSDOM, read the HTML file as a string and use string methods or regex to verify content.

## Core Capabilities

### 1. Jest Configuration

```javascript
// tests/unit/jest.config.js
export default {
  testEnvironment: 'node',  // Use 'node' instead of 'jsdom'
  rootDir: '../../',
  testMatch: ['<rootDir>/tests/unit/**/*.test.js'],
  moduleFileExtensions: ['js', 'json'],
  transform: {},
  verbose: true,
};
```

### 2. Test Pattern

```javascript
import { readFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

describe('HTMLComponent', () => {
  let html;

  beforeAll(() => {
    const htmlPath = join(__dirname, '../../index.html');
    html = readFileSync(htmlPath, 'utf-8');
  });

  test('should contain expected element', () => {
    expect(html).toContain('id="my-section"');
    expect(html).toContain('class="my-class"');
  });

  test('should have elements in correct order', () => {
    const firstIndex = html.indexOf('First Element');
    const secondIndex = html.indexOf('Second Element');
    expect(firstIndex).toBeLessThan(secondIndex);
  });

  test('should match regex pattern', () => {
    expect(html).toMatch(/<code>.*command.*<\/code>/);
  });

  test('should count elements', () => {
    const matches = html.match(/data-step="\d+"/g);
    expect(matches.length).toBeGreaterThanOrEqual(3);
  });
});
```

## Best Practices

- Use `toContain()` for simple presence checks
- Use `indexOf()` comparisons to verify element order
- Use regex `match()` to extract and count elements
- Combine with Playwright for interactive behavior tests

## Limitations

- Cannot test DOM interactions (click events, etc.)
- Cannot query by CSS selectors
- Not suitable for testing JavaScript behavior

For interactive tests, use Playwright E2E tests instead.

## Resources

### references/

- `README.md` - This documentation
