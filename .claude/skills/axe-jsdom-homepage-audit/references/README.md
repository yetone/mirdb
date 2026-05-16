# axe-jsdom-homepage-audit

## What this skill does

Runs axe-core WCAG 2 A/AA audits in **vitest + jsdom** (not Playwright) against a fully assembled, component-based homepage. Tests cover both `light` and `dark` themes.

Use this skill when:

- The project tests with vitest + jsdom (already in `devDependencies`)
- The homepage is built from per-component HTML/CSS/JS files with `data-component="..."` placeholders
- You need WCAG 2.1 AA validation but cannot afford to introduce Playwright just for accessibility

## Project-specific anchors

- Production CSS: `homepage/css/accessibility.css` — skip-link, `:focus-visible`, `prefers-reduced-motion`
- Test suite: `homepage/tests/accessibility/accessibility.test.js`
- Page entry: `homepage/index.html` (skip-link at line 22, `<main id="main" tabindex="-1">` at line 26)

## The assembleHomepage() helper pattern

Every accessibility test assertion runs against the same DOM as a real user. Build it once per test by:

1. Setting `<html data-theme="light|dark" lang="en">`
2. Inserting `<title>` and `<meta charset>` into `<head>` (required for axe's `document-title` rule)
3. Inlining every project stylesheet via a list of paths (use `readFileIfExists` so missing scenarios don't break the audit)
4. Reading `index.html`, extracting `<body>`, and inlining `[data-component]` placeholders with each component's HTML file
5. Running the runtime renderers (`renderFeatures`, `renderSocialProof`, `initFooter`)

Skip components whose HTML hasn't been authored yet — remove the placeholder rather than leaving an "empty section" axe will flag.

## Disabling JSDOM-incompatible axe rules

JSDOM doesn't compute layout, so these axe rules emit false positives and must be disabled:

```js
await axe.run(document, {
    runOnly: { type: 'tag', values: ['wcag2a', 'wcag2aa'] },
    rules: {
        'color-contrast': { enabled: false },  // needs real layout
        'target-size':    { enabled: false },  // needs real pixel sizes
    },
});
```

These rules should still be enforced — by the Playwright visual suite, not here.

## Translating "Playwright" scenarios to JSDOM

When the scenario writeup says "Playwright" but the project ships only vitest:

| Scenario step | JSDOM translation |
|---|---|
| "Tab through the page" | `el.focus()` on each focusable in DOM order |
| "Press Enter on the skip-link" | `dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter' }))` + explicit `main.focus()` (hash-anchor focus isn't auto in JSDOM) |
| "emulateMedia({ reducedMotion: 'reduce' })" | Override `globalThis.matchMedia` to return `matches: true` for the query |
| "getComputedStyle outline" | JSDOM doesn't match `:focus-visible` against computed style — assert the CSS *source* contains the rule via regex instead |

## CSS source-contract assertions

Some WCAG behaviours can't be observed via the JSDOM runtime (CSS variables, `:focus-visible`, `@media`). Assert against the source file:

```js
const css = fs.readFileSync('homepage/css/accessibility.css', 'utf8');
expect(css).toMatch(/:focus-visible\s*\{[^}]*outline\s*:/);
expect(css).toMatch(/@media\s*\(prefers-reduced-motion:\s*reduce\)[\s\S]*animation-duration\s*:\s*0/i);
```

This treats the shipped CSS as the source of truth — a regression in the source will fail the test even though JSDOM can't observe the runtime effect.

## Test cases the suite covers

1. axe-core WCAG 2 A/AA scan (light + dark, color-contrast/target-size disabled)
2. Exactly one `<h1>` on the page
3. Heading hierarchy with no skipped levels
4. Every focusable element reaches focus in DOM order; CSS ships the `:focus-visible` safety net
5. Skip-link is the first focusable; Enter moves focus to `#main`
6. `prefers-reduced-motion: reduce` CSS contract + `matchMedia` listener
7. Every `<img>` has alt / role / aria-hidden / `alt=""`

## Running

```bash
cd homepage
npm test -- accessibility
```

Or use the helper script in `scripts/run_axe_audit.sh`.

## Required dev dependencies

```
axe-core ^4.11.4
vitest    (already in project)
jsdom     (already in project)
```
