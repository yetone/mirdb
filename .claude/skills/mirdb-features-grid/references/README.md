# MirDB Features Grid

## Overview

Implements and tests the Features section of the MirDB homepage — a 6-8 card responsive grid where each card represents one MirDB capability (Memcached protocol, Rust, persistent storage, LSM tree, skip-list, WAL, compaction). Cards are identified by `data-feature` attributes mirroring `src/data/content.json#features[].id`, and structure is enforced by a jest+jsdom test suite.

## When to Use This Skill

Use this skill when users request:

- Adding a new feature card to the homepage Features section
- Reordering or rewording existing feature cards
- Updating a card's icon, title, or description
- Changing the responsive grid behavior (column count, spacing, hover effects)
- Writing or updating jsdom tests for the Features section
- Updating the `features` array in `src/data/content.json`

## Core Capabilities

### 1. Cards live in `homepage/src/components/features/features.html`

Each card is an `<li class="feature-card" data-feature="<id>">` containing exactly three children, in this order:

```html
<li class="feature-card" data-feature="wal">
  <svg class="feature-card__icon" viewBox="0 0 24 24" aria-hidden="true" focusable="false" xmlns="http://www.w3.org/2000/svg">
    <!-- decorative inline SVG -->
  </svg>
  <h3 class="feature-card__title">WAL Durability</h3>
  <p class="feature-card__desc">A Write-Ahead Log records every mutation before acknowledgement, guaranteeing crash recovery.</p>
</li>
```

Rules:
- `data-feature` must match an `id` in `src/data/content.json#features`
- SVG is decorative — always set `aria-hidden="true"` and `focusable="false"`
- One `<svg>`, one `<h3>`, one `<p>` per card — tests assert exact counts

### 2. The list of required `data-feature` ids is canonical

These seven ids must each appear exactly once (REQ-3):

```
memcached, rust, persistence, lsm-tree, skip-list, wal, compaction
```

They are exported from `homepage/tests/helpers/fixtures.js` as `REQUIRED_FEATURE_IDS` and consumed by `tests/unit/features.test.js`.

### 3. Grid layout is in `features.css` (auto-fit, no breakpoint math)

```css
.features-grid {
  display: grid;
  gap: var(--space-5);
  grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
}

@media (max-width: 640px) {
  .features-grid { grid-template-columns: 1fr; }
}
```

Hover and `:focus-within` both elevate the card so keyboard and mouse users get the same cue.

### 4. Tests load the full page via `loadFullPage()`

```javascript
const { loadFullPage } = require('../helpers/dom');
const { REQUIRED_FEATURE_IDS } = require('../helpers/fixtures');

doc = loadFullPage();
expect(doc.getElementById('features')).not.toBeNull();
expect(doc.querySelector('#features .features-grid').children.length).toBeGreaterThanOrEqual(6);
```

`loadFullPage()` reads `public/index.html` and parses the `<body>` into the jsdom document — so tests exercise the *rendered* features section, not just the partial. A regression test additionally loads the standalone partial as defence in depth.

### 5. Content lives in `src/data/content.json`

```json
"features": [
  { "id": "memcached", "title": "Memcached Protocol", "description": "...", "icon": "memcached" },
  ...
]
```

`test_case_8` validates that every entry has non-empty string fields for `id`, `title`, `description`, `icon`, and that every required id is present.

## Best Practices

- Add new feature ids to `tests/helpers/fixtures.js#REQUIRED_FEATURE_IDS` first, then to `content.json`, then to the rendered card — running tests between each step gives you a clear red-green cycle.
- Keep `data-feature` values stable. They are the identity tests/responsive/accessibility rely on. Renaming requires updating fixtures + content.json + features.html together.
- Prefer inline SVG over `<img src>`: zero extra HTTP requests on first paint (NFR-1), works without a build step (NFR-5), and inherits `currentColor` for theming.
- Hover effects must be paired with `:focus-within` (or `:focus`) so keyboard users get the same affordance. NFR-3 / WCAG 2.1 AA.
- Don't hard-code copy in `features.html` for *new* sections — surface it through `content.json` so Scenario 10's build pipeline can inject it.
- Run the test suite from `homepage/` with `npx jest tests/unit/features.test.js` after every change.

## Resources

### references/

- `README.md` - This documentation
