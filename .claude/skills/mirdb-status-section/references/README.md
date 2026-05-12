# MirDB Status Section

## Overview

The `#status` section on the MirDB homepage renders project health signals: a CI build status badge image (linked to CircleCI) and a release version label. Values come from `src/data/content.json#status`, and the section is rendered as static HTML so the page works on a CDN with zero client-side JS.

## When to Use This Skill

Use this skill when users request changes that touch:

- `homepage/src/components/status/status.html` or `status.css`
- The `<section id="status">` block in `homepage/public/index.html`
- The `status.version`, `status.ciBadgeUrl`, or `status.ciLinkUrl` fields in `homepage/src/data/content.json`
- `homepage/tests/unit/status.test.js`
- Any external-badge-style link that must open in a new tab with `rel="noopener noreferrer"`

Trigger phrases: "bump the version", "swap CI provider", "add a new status badge", "the badge link is broken", "make the badge open in a new tab", "test the version label".

## Core Capabilities

### 1. The contract: content.json is the source of truth

`src/data/content.json#status` holds three required strings — `version` (bare SemVer), `ciBadgeUrl` (badge image), `ciLinkUrl` (dashboard the badge links to). The HTML inlines concrete values that match these strings; the test compares rendered values back against `content.json`. If you change one, change the other in the same commit or the test fails.

```json
"status": {
  "version": "0.1.0",
  "ciBadgeUrl": "https://circleci.com/gh/yetone/mirdb.svg?style=shield",
  "ciLinkUrl": "https://circleci.com/gh/yetone/mirdb"
}
```

### 2. Required markup shape

```html
<section id="status">
  ...
  <a href="{ciLinkUrl}" target="_blank" rel="noopener noreferrer">
    <img src="{ciBadgeUrl}" alt="...build... or ...CI..." />
  </a>
  ...
  <span class="version">v{version}</span>
</section>
```

Rules:
- The badge `<img>` MUST be wrapped in an anchor (use `closest('a')` in tests, not `parentElement`).
- The anchor MUST set `target="_blank"` AND `rel="noopener noreferrer"` (noreferrer is optional but recommended). Without `noopener`, target=_blank links leak `window.opener` on legacy browsers.
- The `alt` text must include the substring `build` or `CI` (case-insensitive). Tests look for either.
- The version element must have class `.version` and its text must match `/^v?\d+\.\d+\.\d+/`. The visible text uses a `v` prefix (e.g. `v0.1.0`) while content.json holds the bare SemVer.

### 3. Test structure (jest + jsdom)

Tests in `homepage/tests/unit/status.test.js` load `public/index.html` via `tests/helpers/dom.js#loadFullPage()` and assert against the rendered DOM. They also read the live `content.json` so assertions stay data-driven.

```js
const { loadFullPage, loadPartial } = require('../helpers/dom');

beforeAll(() => {
  doc = loadFullPage();
  content = readContent();
});

test('badge anchor opens in new tab with noopener', () => {
  const badge = doc.getElementById('status').querySelector('img');
  const anchor = badge.closest('a');                       // structural, not positional
  expect(anchor.getAttribute('href')).toBe(content.status.ciLinkUrl);
  expect(anchor.getAttribute('target')).toBe('_blank');
  expect(anchor.getAttribute('rel')).toContain('noopener');
});

test('version text matches content.json (normalize v prefix)', () => {
  const text = doc.querySelector('#status .version').textContent.trim();
  expect(text).toMatch(/^v?\d+\.\d+\.\d+/);
  const stripV = s => s.startsWith('v') ? s.slice(1) : s;
  expect(stripV(text)).toBe(stripV(content.status.version));
});
```

### 4. Wiring into the page shell

The section is inlined into `public/index.html` between `#quick-start` and `#roadmap`. Its stylesheet is added to `<head>`:

```html
<link rel="stylesheet" href="../src/components/status/status.css" />
```

Don't fetch content.json from the browser — the build step is the substitution boundary.

## Best Practices

- **Always update `content.json` and `index.html` together.** Drift will fail the test on the next CI run.
- **Use `closest('a')` rather than direct parent traversal in tests** — keeps the contract structural so it survives intermediate wrapper elements (e.g. `<picture>` for retina).
- **Include both `noopener` and `noreferrer` in `rel`** for new-tab external links. The test only checks `noopener`, but `noreferrer` also prevents Referer disclosure.
- **`alt` text must describe the badge's meaning**, not its visual appearance — e.g. "MirDB CI build status badge", not "green shield".
- **Keep version display prefix consistent** (`v0.1.0`) but store bare SemVer (`0.1.0`) in `content.json` for non-display consumers.
- **Run the full suite** (`npx jest`) before pushing — the status section is intentionally coupled to `content.json`, so an unrelated content edit can flip this test.

## Resources

### references/

- `README.md` - This documentation
