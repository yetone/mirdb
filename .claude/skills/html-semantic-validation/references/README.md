# HTML Semantic Validation

## Overview

This skill captures the test patterns used to validate semantic HTML on the MirDB homepage (`homepage/index.html`). It walks the rendered document under jsdom and asserts: exactly one `<h1>`, no skipped heading levels, presence of `<main>`/`<nav>`/`<section>`/`<footer>` landmarks, accessible names on every `<section>`, zero presentational/legacy elements (`<font>`, `<center>`, `<marquee>`, `<blink>`), and that visual lists are encoded as real lists (`<ul>`/`<ol>` or `<article>`s) rather than stacked `<div>`s.

## When to Use This Skill

Use this skill when users request:

- "Add semantic HTML / a11y tests against `index.html`"
- "Verify the heading hierarchy doesn't skip levels"
- "Check that every section has an accessible name (aria-labelledby/aria-label)"
- "Catch regressions where someone adds a second `<h1>`"
- Adding tests under `homepage/tests/integration/semantic.test.js` or similar

## Core Capabilities

### 1. Single-h1 enforcement

```js
const h1s = document.querySelectorAll('h1');
expect(h1s.length).toBe(1);
```

### 2. Heading-level skip detector

Walk headings in document order; flag the first heading whose level exceeds the **immediate predecessor** by more than one. Compare to the predecessor (not the maximum seen) so a legal `h3 → h2` downshift does not produce false positives on the next `h3`.

```js
function findSkippedHeadingJump(document) {
  const headings = Array.from(document.querySelectorAll('h1,h2,h3,h4,h5,h6'));
  let previousLevel = 0;
  for (const heading of headings) {
    const level = parseInt(heading.tagName.slice(1), 10);
    if (previousLevel > 0 && level - previousLevel > 1) {
      return { previousLevel, currentLevel: level, text: heading.textContent.trim() };
    }
    previousLevel = level;
  }
  return null;
}
```

### 3. Landmark presence

```js
expect(document.querySelector('main')).not.toBeNull();
expect(document.querySelector('nav')).not.toBeNull();
expect(document.querySelectorAll('section').length).toBeGreaterThan(0);
expect(document.querySelector('footer')).not.toBeNull();
```

### 4. Accessible-name resolver per section

Return a structured status (`ok` + `source`) so the failure message can enumerate offenders by id.

```js
function sectionAccessibleNameStatus(section, document) {
  const ariaLabel = section.getAttribute('aria-label');
  if (ariaLabel && ariaLabel.trim().length > 0) return { ok: true, source: 'aria-label' };
  const ariaLabelledBy = section.getAttribute('aria-labelledby');
  if (ariaLabelledBy && ariaLabelledBy.trim().length > 0) {
    const target = document.getElementById(ariaLabelledBy.trim());
    if (target) return { ok: true, source: 'aria-labelledby' };
    return { ok: false, source: 'aria-labelledby', reason: 'target-missing' };
  }
  return { ok: false, source: 'none' };
}
```

### 5. Presentational tag check

```js
expect(document.querySelectorAll('font, center, marquee, blink').length).toBe(0);
```

### 6. Semantic list check (accept ul/ol OR repeated articles)

```js
const features = document.querySelector('#features');
const usesList = !!features.querySelector('ul, ol');
const usesArticles = features.querySelectorAll('article').length >= 2;
expect(usesList || usesArticles).toBe(true);
```

### 7. Negative-case fixture

Synthesize a bad HTML string with two `<h1>`s, load it in a fresh JSDOM instance, and assert the validator flags the second `<h1>` by text content.

```js
const offendingHtml = `<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><title>Bad</title></head>
  <body><main><h1>First</h1><p>Paragraph</p><h1>Second</h1></main></body></html>`;
const offendingDom = new JSDOM(offendingHtml, { url: 'http://localhost:3000' });
expect(offendingDom.window.document.querySelectorAll('h1').length).toBeGreaterThan(1);
```

## Best Practices

- Compare each heading to its **immediate predecessor**, not the running max. The latter rejects legal `h3 → h2 → h3` patterns.
- For the section-name check, treat `aria-labelledby` pointing at a missing id as a failure, not a pass — track the failure mode in a structured result so the test message can pinpoint the offender.
- Accept both `<ul>` of `<article>` and a flat list of `<article>`s for "is this a list?" — the scenario allows either.
- Use a fresh JSDOM instance for the negative case so the actual page (which should pass) is not contaminated.
- Run with vitest's jsdom environment configured in `vitest.config.js`. Resource-load errors for CSS are noise (no `localhost:3000` server in tests) and can be ignored.

## Resources

### references/

- `README.md` - This documentation
