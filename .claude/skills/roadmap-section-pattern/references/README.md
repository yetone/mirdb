# Roadmap Section Pattern (MirDB)

## Overview

Implements and tests the **Roadmap** section of the MirDB homepage — a two-column list that distinguishes **implemented** capabilities (Tokio Memcached text protocol server, LSM tree, skip-list memtable, WAL, minor compaction, major compaction) from **planned** items (Raft consensus, binary protocol, pluggable compression). Each item carries a `data-status="done"|"planned"` attribute, an SVG status icon, and an `aria-label` prefixed `Done:` / `Planned:` so the visual distinction is conveyed by shape + text — not color alone — satisfying WCAG 1.4.1.

## When to Use This Skill

- Adding a shipped capability to the done column (e.g., when Raft consensus actually lands)
- Adding a new planned item to the planned column
- Reordering done vs planned entries
- Renaming an item (must update content.json roadmap and the inlined markup in lockstep)
- Splitting a coarse entry into finer-grained ones (the original `Background compaction` became `Minor compaction (memtable flush)` + `Major compaction (level merge)` for this exact reason)
- Writing or updating jsdom tests that assert `data-status` counts and required substrings
- Migrating the visual treatment (icon style, column layout, badge wording) — keep all three non-color signals intact

## Core Capabilities

### 1. Two-column layout in `homepage/src/components/roadmap/roadmap.html`

```html
<section id="roadmap" class="section section--roadmap" aria-labelledby="roadmap-heading">
  <div class="container">
    <h2 id="roadmap-heading">Roadmap</h2>
    <p class="roadmap-lede">...</p>

    <div class="roadmap-columns">
      <section class="roadmap-column roadmap-column--done" aria-labelledby="roadmap-done-heading">
        <h3 id="roadmap-done-heading" class="roadmap-column__title">
          <span class="roadmap-column__badge" aria-hidden="true">Shipped</span>
          Implemented
        </h3>
        <ul class="roadmap-done" role="list">
          <li class="roadmap-item roadmap-item--done" data-status="done"
              aria-label="Done: Tokio Memcached text protocol server">
            <svg class="roadmap-item__icon roadmap-item__icon--done" aria-hidden="true" focusable="false">
              <circle cx="12" cy="12" r="10" fill="currentColor"/>
              <path d="M7 12.5l3.2 3.2L17 9" stroke="#fff" stroke-width="2"/>
            </svg>
            <span class="roadmap-item__text">Tokio Memcached text protocol server</span>
          </li>
          ...
        </ul>
      </section>

      <section class="roadmap-column roadmap-column--planned" aria-labelledby="roadmap-planned-heading">
        <h3 id="roadmap-planned-heading" class="roadmap-column__title">
          <span class="roadmap-column__badge roadmap-column__badge--planned" aria-hidden="true">Next</span>
          Planned
        </h3>
        <ul class="roadmap-planned" role="list">
          <li class="roadmap-item roadmap-item--planned" data-status="planned"
              aria-label="Planned: Raft consensus for distributed deployments">
            <svg class="roadmap-item__icon roadmap-item__icon--planned" aria-hidden="true" focusable="false">
              <circle cx="12" cy="12" r="10" stroke="currentColor" stroke-dasharray="3 3" fill="none"/>
              <path d="M12 7v5l3 3" stroke="currentColor"/>
            </svg>
            <span class="roadmap-item__text">Raft consensus for distributed deployments</span>
          </li>
          ...
        </ul>
      </section>
    </div>
  </div>
</section>
```

The same markup is **inlined** into `homepage/public/index.html` so the page renders without a build step. When you add an item, update **both** files; eventually Scenario 10's build script will read content.json and re-inline, but until then keep them in sync.

### 2. Required non-color signals (WCAG 1.4.1)

Every list item MUST carry all three:

| Signal | Where | Example (done) | Example (planned) |
|---|---|---|---|
| SVG icon shape | `<svg class="roadmap-item__icon">` | filled circle + check | dashed-stroke circle + clock hand |
| Column badge text | `.roadmap-column__badge` | `Shipped` | `Next` |
| aria-label prefix | `aria-label="..."` | `Done: <title>` | `Planned: <title>` |

Color (orange accent for done, muted for planned) is layered on top — never alone.

### 3. Content lives in `homepage/src/data/content.json#roadmap`

```json
"roadmap": {
  "done": [
    { "title": "Tokio Memcached text protocol server" },
    { "title": "LSM tree storage engine" },
    { "title": "Skip-list memtable" },
    { "title": "Write-ahead log durability" },
    { "title": "Minor compaction (memtable flush)" },
    { "title": "Major compaction (level merge)" }
  ],
  "planned": [
    { "title": "Raft consensus for distributed deployments" },
    { "title": "Binary memcached protocol" },
    { "title": "Pluggable compression" }
  ]
}
```

The corresponding scenario test enforces:
- `done.length >= 4`
- `planned.length >= 1`
- Joined `done` titles contain (case-insensitive): `memcached`, `skip`, `minor`, `major`
- Joined `planned` titles contain `raft`

If you split or rename a done item, double-check the substring assertion still holds — that is the trap that the original `Background compaction` entry fell into.

### 4. Test pattern: `homepage/tests/unit/roadmap.test.js`

```js
const { loadFullPage, loadPartial } = require('../helpers/dom');

describe('Roadmap section', () => {
  let doc;
  beforeEach(() => { doc = loadFullPage(); });

  test('done items include memcached, skip, minor, major', () => {
    const doneItems = doc.querySelectorAll('#roadmap [data-status="done"]');
    expect(doneItems.length).toBeGreaterThanOrEqual(4);
    const combined = Array.from(doneItems).map(el => el.textContent).join(' ').toLowerCase();
    expect(combined).toContain('memcached');
    expect(combined).toContain('skip');
    expect(combined).toContain('minor');
    expect(combined).toContain('major');
  });

  test('planned items include at least one Raft entry', () => {
    const planned = doc.querySelectorAll('#roadmap [data-status="planned"]');
    expect(planned.length).toBeGreaterThanOrEqual(1);
    expect(Array.from(planned).some(el => /raft/i.test(el.textContent))).toBe(true);
  });

  test('non-color status distinction', () => {
    const items = doc.querySelectorAll('#roadmap [data-status]');
    items.forEach(item => {
      const hasIcon = item.querySelector('svg') !== null;
      const status = item.getAttribute('data-status');
      const aria = item.getAttribute('aria-label') || '';
      const labelEncodesStatus =
        (status === 'done' && /done|implemented|shipped|complete/i.test(aria)) ||
        (status === 'planned' && /planned|upcoming|next/i.test(aria));
      expect(hasIcon || labelEncodesStatus).toBe(true);
    });
  });
});
```

Always include a partial-loader regression test that asserts the roadmap markup still works as a standalone partial (`loadPartial('src/components/roadmap/roadmap.html')`) — that guards against the static build pipeline silently dropping the partial.

### 5. Section is wired into `public/index.html`

Two edits required when bootstrapping the section:

1. Add stylesheet link in `<head>`:
   ```html
   <link rel="stylesheet" href="../src/components/roadmap/roadmap.css" />
   ```
2. Replace the `<!-- #roadmap — Scenario 6 -->` stub with the full markup from `roadmap.html`.

## Anti-patterns (do NOT do)

- Color-only distinction (e.g., green `done` vs red `planned` without icons or labels). Fails WCAG 1.4.1.
- Single coarse entry `Background compaction` — splits-into-multiple test assertions will fail because no item contains the substring `minor` or `major`.
- Adding to roadmap.html but not content.json (or vice versa) — the schema test will catch it, but the section becomes a lying source of truth.
- Stripping `aria-hidden="true"` from the badge — duplicates content into the accessibility tree because the surrounding h3 already names the column.
- Single `<ul>` with both done and planned mixed — the scenario allows this shape but it tanks scimmability for sighted users. Stick with two columns.

## Related Skills

- `jest-jsdom-static-html-section` — general pattern for the test harness used here.
- `mirdb-features-grid` — sister section using the same content.json source-of-truth pattern.

## Resources

### references/

- `README.md` - This documentation
