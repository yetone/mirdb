# Astro Performance Testing

## Overview

This skill enables writing performance validation tests for Astro static sites that run in CI without a headless browser. It uses proxy assertions (checking factors that correlate with real metrics) and production build output inspection to validate performance requirements.

## When to Use This Skill

Use this skill when users request:

- Performance testing for an Astro/static site
- Lighthouse performance score validation
- FCP/TTI measurement in CI
- Interaction responsiveness verification
- Asset optimization and page weight budget checks
- NFR-1 (Performance) compliance testing

## Core Capabilities

### 1. Production Build Inspection Pattern

Build the site in production mode, then inspect the dist/ output:

```typescript
import { execSync } from 'node:child_process';

function buildSite() {
  const astroBin = join(HOMEPAGE_DIR, 'node_modules', 'astro', 'astro.js');
  execSync(`node "${astroBin}" build`, {
    cwd: HOMEPAGE_DIR,
    stdio: 'pipe',
    env: { ...process.env, NODE_ENV: 'production' },
  });
}
```

Parse the output HTML with linkedom (same pattern as accessibility tests):

```typescript
import { parseHTML } from 'linkedom';

function parseBuiltHtml(): Document {
  const html = readFileSync(INDEX_HTML, 'utf-8');
  return parseHTML(html).document;
}
```

### 2. Lighthouse Proxy Assertions

When Chrome is unavailable, validate individual Lighthouse audit factors:

- **Viewport meta** → Mobile-friendly score
- **Semantic landmarks** (`<main>`, `<nav>`, `<footer>`) → Accessibility sub-score
- **Image width/height attributes** → CLS prevention (Cumulative Layout Shift)
- **Font-display strategy** → Text visibility during load
- **No parser-blocking scripts in `<head>`** → FCP/TTI
- **lang attribute on `<html>`** → SEO best practice
- **Favicon + robots.txt existence** → Best practices audit

### 3. Page Weight Budget Enforcement

Walk the dist directory and sum file sizes:

```typescript
function getDistFiles(): { path: string; size: number }[] {
  const result: { path: string; size: number }[] = [];
  function walk(dir: string) {
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const full = join(dir, entry.name);
      if (entry.isDirectory()) walk(full);
      else if (entry.isFile()) result.push({ path: full, size: statSync(full).size });
    }
  }
  walk(DIST_DIR);
  return result;
}

// Enforce 500KB budget (excluding large binary assets if needed)
const weightExcludingLogo = distFiles
  .filter((f) => !f.path.endsWith('logo.gif'))
  .reduce((sum, f) => sum + f.size, 0);
expect(weightExcludingLogo).toBeLessThanOrEqual(512000);
```

### 4. Interaction Responsiveness Validation

Instead of measuring actual elapsed time (unreliable in Node), verify that event handlers are synchronous:

```typescript
// Verify navigation click handler is synchronous
it('nav link click handler is synchronous (no artificial delays)', () => {
  const { document: pageDoc } = parseHTML(buildFullPageHTML());
  const link = pageDoc.querySelector('[data-nav-link="features"]')!;
  const target = pageDoc.getElementById('features')!;

  let calledWith: any = null;
  target.scrollIntoView = (opts?: any) => { calledWith = opts; };

  link.addEventListener('click', (e: Event) => {
    e.preventDefault();
    pageDoc.getElementById('features')?.scrollIntoView({ behavior: 'smooth' });
  });

  link.dispatchEvent(new Event('click', { bubbles: true, cancelable: true }));

  // Handler calls scrollIntoView immediately — no setTimeout before it
  expect(calledWith).toEqual({ behavior: 'smooth' });
});
```

Key checks for each interactive element:
- **Navigation clicks**: `scrollIntoView` called synchronously, no debounce/throttle
- **Copy buttons**: `classList.add('copied')` before any `setTimeout`
- **Mobile menu**: `classList.add('open')` before focus trap `setTimeout`
- **CSS transitions**: Verify transition durations are ≤ 250ms

### 5. Resource Blocking Analysis

Check for render-blocking and parser-blocking resources:

```typescript
// No external stylesheets that block rendering
const externalStylesheets = document.querySelectorAll('link[rel="stylesheet"][href]');
expect(externalStylesheets.length).toBeLessThanOrEqual(2);

// No synchronous scripts in head
const blockingScripts = Array.from(headScripts).filter((s) => {
  return !s.getAttribute('type') && !s.hasAttribute('defer') && !s.hasAttribute('async');
});
expect(blockingScripts.length).toBe(0);

// No third-party scripts
const thirdPartyScripts = Array.from(externalScripts).filter((s) => {
  const src = s.getAttribute('src') || '';
  return src.startsWith('http') || src.startsWith('//');
});
expect(thirdPartyScripts.length).toBe(0);
```

### 6. Build Output Minification Verification

```typescript
// HTML should not have large comment blocks
const largeComments = rawHtml.match(/<!--[\s\S]{50,}-->/g);
expect(largeComments).toBeNull();

// HTML should not have runs of 4+ consecutive spaces
expect(/[ ]{4,}/.test(rawHtml)).toBe(false);

// No source maps should leak to production
const sourceMaps = distFiles.filter((p) => p.endsWith('.map'));
expect(sourceMaps.length).toBe(0);
```

## Best Practices

- Always build with `NODE_ENV=production` to get production-optimized output
- Parse HTML once in `beforeAll` and reuse across test cases
- Keep tests deterministic — avoid `performance.now()` or `setTimeout`-based measurements
- Use source code inspection for configuration checks (e.g., verify IntersectionObserver usage in NavBar.astro)
- Map each proxy assertion to a specific Lighthouse audit category
- Keep the page weight budget separate for known-large assets (like animated GIFs) that can't be optimized without format conversion

## Resources

### scripts/

*(No helper scripts needed — tests are self-contained vitest files)*

### references/

- `README.md` — This documentation
