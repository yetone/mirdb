---
name: vitest-html-testing
description: Test static HTML pages and DOM structure using Vitest with JSDOM environment
scope: project
---

# Vitest HTML Testing

Test static HTML files, CSS styling, and DOM structure using Vitest with JSDOM. Ideal for testing landing pages, static sites, and HTML templates without a full browser.

## When to Use

- Testing landing page HTML structure
- Verifying DOM element presence and content
- Testing CSS class assignments
- Validating HTML accessibility attributes
- Testing static HTML files in unit tests

## Quick Reference

```javascript
import { JSDOM } from 'jsdom';
import fs from 'fs';

const html = fs.readFileSync('./index.html', 'utf-8');
const dom = new JSDOM(html);
const document = dom.window.document;

const headline = document.querySelector('h1');
expect(headline.textContent).toBe('Expected Text');
```

See [references/README.md](references/README.md) for full documentation.
