# Vitest HTML Testing

## Overview

Test static HTML files, CSS classes, and DOM structure using Vitest with JSDOM. This approach enables unit testing of landing pages and static sites without needing a full browser environment.

## When to Use This Skill

Use this skill when:

- Testing landing page HTML structure and content
- Verifying DOM elements exist with correct attributes
- Testing CSS class assignments without visual rendering
- Validating accessibility attributes (aria, roles)
- Writing unit tests for static HTML files

## Project Setup

### package.json

```json
{
  "type": "module",
  "scripts": {
    "test": "vitest run",
    "test:watch": "vitest"
  },
  "devDependencies": {
    "@testing-library/jest-dom": "^6.6.3",
    "jsdom": "^25.0.1",
    "vite": "^6.0.1",
    "vitest": "^2.1.8"
  }
}
```

### vite.config.js

```javascript
import { defineConfig } from 'vite';

export default defineConfig({
  test: {
    environment: 'jsdom',
    globals: true,
    setupFiles: ['./tests/setup.js'],
    include: ['tests/**/*.test.js'],
  },
});
```

### tests/setup.js

```javascript
import '@testing-library/jest-dom';
```

## Core Capabilities

### 1. Load and Parse HTML Files

```javascript
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

describe('Landing Page Tests', () => {
  let dom;
  let document;

  beforeEach(() => {
    const htmlPath = path.resolve(__dirname, '../index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');
    dom = new JSDOM(htmlContent, {
      url: 'http://localhost:3000',
      runScripts: 'dangerously',
      resources: 'usable',
    });
    document = dom.window.document;
  });

  afterEach(() => {
    if (dom) {
      dom.window.close();
    }
  });

  // Tests go here...
});
```

### 2. Test Element Presence

```javascript
it('should have a hero section', () => {
  const heroSection = document.querySelector('.hero');
  expect(heroSection).not.toBeNull();
  expect(heroSection).toBeInTheDocument();
});

it('should have an H1 headline', () => {
  const headline = document.querySelector('h1');
  expect(headline).not.toBeNull();
  expect(headline.textContent.trim()).not.toBe('');
});
```

### 3. Test Element Content

```javascript
it('should have headline with 10 words or fewer', () => {
  const headline = document.querySelector('h1');
  const text = headline.textContent.trim();
  const wordCount = text.split(/\s+/).filter(word => word.length > 0).length;
  expect(wordCount).toBeLessThanOrEqual(10);
});

it('should have CTA button with actionable text', () => {
  const ctaButton = document.querySelector('.cta-primary');
  const text = ctaButton.textContent.trim().toLowerCase();
  const actionWords = ['get', 'start', 'try', 'sign', 'join', 'download'];
  const hasActionWord = actionWords.some(word => text.includes(word));
  expect(hasActionWord).toBe(true);
});
```

### 4. Test Element Attributes

```javascript
it('should have button element for accessibility', () => {
  const ctaButton = document.querySelector('.cta-primary');
  expect(ctaButton.tagName.toLowerCase()).toBe('button');
});

it('should have onclick handler', () => {
  const ctaButton = document.querySelector('.cta-primary');
  expect(ctaButton.hasAttribute('onclick')).toBe(true);
});
```

### 5. Test DOM Order

```javascript
it('should have subheadline after headline', () => {
  const headline = document.querySelector('h1');
  const subheadline = document.querySelector('.subheadline');

  const parent = document.querySelector('.hero-content');
  const children = Array.from(parent.children);
  const headlineIndex = children.indexOf(headline);
  const subheadlineIndex = children.indexOf(subheadline);

  expect(subheadlineIndex).toBeGreaterThan(headlineIndex);
});
```

### 6. Test CSS File Content

```javascript
it('should have full-width hero in CSS', () => {
  const cssPath = path.resolve(__dirname, '../styles.css');
  const cssContent = fs.readFileSync(cssPath, 'utf-8');

  expect(cssContent).toMatch(/\.hero\s*\{[^}]*width:\s*100%/);
  expect(cssContent).toMatch(/\.hero\s*\{[^}]*min-height:\s*100vh/);
});
```

## Best Practices

- Use `beforeEach`/`afterEach` to properly create and close JSDOM instances
- Use `path.resolve(__dirname, ...)` for reliable file paths
- Clean up JSDOM windows to prevent memory leaks
- Test both element presence and content
- Use regex patterns to test CSS file content
- Include `toBeInTheDocument()` for better error messages

## Common Assertions

| Assertion | Purpose |
|-----------|---------|
| `expect(el).not.toBeNull()` | Element exists |
| `expect(el).toBeInTheDocument()` | Element in DOM |
| `el.textContent.trim()` | Get text content |
| `el.hasAttribute('attr')` | Check attribute |
| `el.tagName.toLowerCase()` | Check element type |
| `el.classList.contains('class')` | Check CSS class |

## Resources

- [Vitest Documentation](https://vitest.dev/guide/)
- [JSDOM Documentation](https://github.com/jsdom/jsdom)
- [Testing Library Jest-DOM](https://github.com/testing-library/jest-dom)
