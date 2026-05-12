# mirdb-comparison-card-section

## Overview

Implementation pattern for a competitor-comparison section on the MirDB homepage — the "Why MirDB?" style block that names alternatives (Memcached, Redis, RocksDB...) in one card each and pulls cross-cutting differentiators (Rust / LSM / compaction / planned Raft) into the section's lede paragraph.

This skill captures the pattern as it lives in Scenario 7. Reuse it whenever a future scenario adds another comparison or differentiation section.

## When to Use

- Adding or editing the differentiation section under `homepage/src/components/why-mirdb/`.
- Adding a similar comparison section (e.g., "MirDB vs other LSM stores") with one card per alternative.
- Keeping `homepage/src/data/content.json#whyMirDB` in sync with rendered cards.
- Writing jest + jsdom tests that assert per-card copy by a stable attribute selector rather than by class/position.

## Key Pattern

### 1. Structure: lede + cards

A single `<section id="why-mirdb">` with three nested pieces:

```html
<section id="why-mirdb" class="section section--why-mirdb" aria-labelledby="why-mirdb-heading">
  <div class="container">
    <h2 id="why-mirdb-heading">Why MirDB?</h2>
    <p class="why-mirdb__lede">
      <!-- cross-cutting differentiators that apply to ALL comparisons -->
    </p>
    <ul class="comparison" role="list" aria-label="...">
      <li class="comparison-item" data-competitor="memcached">
        <header class="comparison-item__header">...</header>
        <p class="comparison-item__text">...one focused contrast...</p>
      </li>
      <!-- one li per competitor -->
    </ul>
  </div>
</section>
```

Cross-cutting differentiators (here: Rust, LSM tree, built-in compaction, planned Raft) live in the **lede** — not duplicated per card. The aggregated-text test reads the whole `#why-mirdb` textContent, so this placement is supported AND each card stays focused on one concrete contrast.

### 2. Stable test contract: `data-competitor` attributes

Tests select cards by `li[data-competitor='<name>']`, never by class or position:

```js
const items = doc.querySelectorAll("#why-mirdb li[data-competitor='memcached']");
expect(items.length).toBe(1);
expect(/(persistence|persistent)/i.test(items[0].textContent.toLowerCase())).toBe(true);
```

Future redesigns can drop the badge, restyle the card, or reorder competitors without breaking the test contract — only the `data-competitor` attribute must survive.

### 3. Data layer mirrors view copy

`homepage/src/data/content.json#whyMirDB` is the source-of-truth array. Each entry has the shape:

```json
{ "competitor": "Memcached", "difference": "MirDB writes every value to durable disk storage..." }
```

The `difference` strings should nearly mirror the rendered card copy (verb tense, key terms like "persistent" vs "persists") so a future edit in one place does not silently diverge from the other. A test_case_6-style validation enforces the array shape and required competitors.

### 4. Inlined into public/index.html (no build step)

Until Scenario 10 introduces a build step, the partial under `homepage/src/components/why-mirdb/why-mirdb.html` lives standalone for ownership/reuse but is **also inlined** into `homepage/public/index.html`. The stylesheet is linked from `public/index.html` so the static page renders without compilation.

## File Layout (existing artifacts)

```
homepage/
  public/
    index.html               # inlined section (rendered to users)
  src/
    components/
      why-mirdb/
        why-mirdb.html       # standalone partial (ownership + regression source)
        why-mirdb.css        # responsive grid styles (auto-fit minmax + 640px override)
    data/
      content.json           # whyMirDB array (data source-of-truth)
  tests/
    unit/
      why-mirdb.test.js      # jest+jsdom suite, 7 tests
```

## CSS responsive grid pattern

```css
.comparison {
  list-style: none;
  padding: 0;
  margin: 0;
  display: grid;
  gap: var(--space-5);
  grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
}

@media (max-width: 640px) {
  .comparison { grid-template-columns: 1fr; }
}
```

This is the MDN `auto-fit + minmax` recipe — reflows 3 → 2 → 1 column as the viewport shrinks, then pins one column at the mobile breakpoint owned by Scenario 8.

## Test Pattern (Jest + jsdom)

Tests live at `homepage/tests/unit/why-mirdb.test.js` and:
1. Use the shared `loadFullPage()` helper that reads `public/index.html` into jsdom.
2. Use a `loadContent()` helper that reads `src/data/content.json`.
3. Name each `test('test_case_N: ...', ...)` to map 1:1 onto the scenario.json test cases.
4. Always include a regression test that loads the standalone partial via `loadPartial(...)` to catch drift between partial and inlined copy.

## Accessibility

- `aria-labelledby` on the section points at the `<h2>` id.
- `aria-label` on the `<ul>` describes the comparison set.
- `<header>` per card groups the badge + `<h3>` title.
- Badge is `aria-hidden="true"` because "vs" is decorative.

## Related Knowledge UUIDs

- `b3a7fcb8-a652-48c7-a55a-6638cd2d1b5c` — MirDB project overview vocabulary.
- `5ea4aab2-d7eb-4f2c-b501-66d1cc8fa545` — Memcached protocol context.
- `fd598601-3282-45dd-b150-e3a532139d3c` — LSM-tree storage engine context.

## Cross-References

- Sibling skill `mirdb-features-grid` documents the same source-of-truth + jsdom-test pattern for the Features section.
- Sibling skill `jest-jsdom-static-html-section` documents the shared jsdom helper API.
