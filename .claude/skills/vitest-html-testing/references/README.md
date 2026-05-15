# Vitest + jsdom HTML Testing

## Overview

This skill documents the testing approach for the MirDB homepage project. All homepage scenarios use Vitest with jsdom to test static HTML pages. Tests load the HTML file from disk, query the DOM, and validate structure, content, and CSS.

## When to Use This Skill

Use this skill when users request:

- Writing tests for static HTML pages (unit tests with jsdom)
- Validating HTML structure, heading hierarchy, or semantic elements
- Testing CSS classes, hover states, or focus states via CSS file inspection
- Checking link attributes (href, target, rel) for security and correctness
- Validating image elements (src, alt, dimensions)

## Core Capabilities

### 1. Loading HTML in jsdom

The MirDB homepage is a static site with no build step. Tests load `index.html` directly:

```js
import { JSDOM } from 'jsdom';
import fs from 'node:fs';
import path from 'node:path';

function loadDOM() {
  const html = fs.readFileSync(
    path.resolve(__dirname, '../index.html'),
    'utf-8'
  );
  return new JSDOM(html, { url: 'http://localhost:8080' });
}
```

### 2. DOM Query Patterns

```js
// Element existence
const hero = document.getElementById('hero');
expect(hero).not.toBeNull();

// Scoped queries
const headline = document.querySelector('#hero h1');
expect(headline.tagName).toBe('H1');

// Word count validation
const text = element.textContent.trim();
const wordCount = text.split(/\s+/).length;
expect(wordCount).toBeGreaterThanOrEqual(5);

// Link attributes
expect(link.getAttribute('target')).toBe('_blank');
expect(link.getAttribute('rel')).toMatch(/noopener/);

// CSS file content check
const css = fs.readFileSync('css/hero.css', 'utf-8');
expect(css).toMatch(/\.hero-cta-primary:hover/);
```

### 3. Asset Validation

```js
const img = document.querySelector('header img');
const src = img.getAttribute('src');
const assetPath = path.resolve(__dirname, '..', src);
expect(fs.existsSync(assetPath)).toBe(true);
```

## Best Practices

- Load DOM fresh in `beforeEach` to avoid test pollution
- Use scoped selectors (`#hero h1` not just `h1`) for precise targeting
- Validate both DOM presence AND CSS file content for style tests
- Use `toMatch()` with regex for flexible string matching (URLs, class names)
- Check file existence on disk for asset integrity tests
- Organize tests by test case ID matching the scenario definition

## Test File Structure

Each test file follows this convention:

```
describe('Section Name', () => {
  describe('Test Case N: Description', () => {
    it('should verify specific assertion', () => { ... });
  });
});
```
