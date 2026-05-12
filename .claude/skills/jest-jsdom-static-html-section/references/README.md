# jest-jsdom-static-html-section

Validate one section of the **MirDB homepage** (`homepage/`) with a Jest + jsdom unit test that loads `public/index.html` and asserts on the resulting DOM. This is the testing convention used by every section scenario on this project (hero, features, code-example, navigation, status, roadmap, why-mirdb).

## When to use this skill

You are implementing or modifying a section partial under `homepage/src/components/<section>/<section>.html` and need a unit test that:

- Confirms the section element exists with the expected `id` and is the right tag (`<section>`).
- Confirms semantic children are present: `<h1>` / `<h2>`, `<img>` with descriptive `alt`, `<a>` CTAs, `<ul>` lists, etc.
- Confirms text content matches the product copy or the data declared in `homepage/src/data/content.json`.
- Confirms accessibility-relevant attributes are wired (`aria-labelledby`, `role`, `target="_blank"` + `rel="noopener"`).

## Project context (load-bearing details)

- **No build step yet.** Section partials live in `homepage/src/components/<name>/<name>.html` but are also inlined into `homepage/public/index.html` so the site renders straight from `public/`. Tests must read `public/index.html` (the file users actually load), not the partial.
- **Content is data-driven.** Links, badge URLs, code snippets, feature names, and CTA labels live in `homepage/src/data/content.json`. Tests should read this file and assert the rendered markup matches — this protects against the URL/label drifting in one place and not the other.
- **Helpers are shared.** `homepage/tests/helpers/dom.js` exports `loadFullPage()` (reads `public/index.html` and writes its `<body>` into the jsdom document) and `loadPartial(relativePath)` (loads a single component partial). Reuse them; do not re-implement.
- **Jest config.** `homepage/jest.config.js` sets `testEnvironment: 'jsdom'` and matches `**/tests/**/*.test.js`. Run with `cd homepage && npx jest tests/unit/<name>.test.js`.
- **Folder ownership.** Each section scenario only owns its own component folder and its own test file (`homepage/tests/unit/<name>.test.js`). Do not edit other sections' files.

## Standard test skeleton

```js
const fs = require('fs');
const path = require('path');
const { loadFullPage, loadPartial } = require('../helpers/dom');

const REPO_ROOT = path.resolve(__dirname, '..', '..');

function readContent() {
  const raw = fs.readFileSync(
    path.join(REPO_ROOT, 'src', 'data', 'content.json'),
    'utf8'
  );
  return JSON.parse(raw);
}

describe('<Section name>', () => {
  let doc;
  let content;

  beforeAll(() => {
    doc = loadFullPage();
    content = readContent();
  });

  test('section element is present with the expected id', () => {
    const section = doc.getElementById('<section-id>');
    expect(section).not.toBeNull();
    expect(section.tagName.toLowerCase()).toBe('section');
  });

  test('section contains the expected heading', () => {
    const section = doc.getElementById('<section-id>');
    const h = section.querySelector('h1, h2');
    expect(h).not.toBeNull();
    expect(h.textContent.trim()).toBe('<expected text>');
  });

  test('CTAs use content.json as their source of truth', () => {
    const section = doc.getElementById('<section-id>');
    const cta = section.querySelector('.cta-primary, [data-cta="primary"]');
    expect(cta).not.toBeNull();
    expect(cta.getAttribute('href')).toBe(content.links.<key>);
  });

  test('partial-loader sanity: the partial parses standalone', () => {
    const partial = loadPartial('src/components/<section>/<section>.html');
    expect(partial.getElementById('<section-id>')).not.toBeNull();
  });
});
```

## Conventions to follow

1. **One `describe` per section.** Name it after the section ("Hero section", "Features grid", etc.).
2. **Match test_case ids.** When the scenario in `.something/scenario.json` enumerates test cases, name each `test` block like `test_case N: <human description>` so reviewers can map scenario → test 1:1.
3. **Prefer attribute / class selectors over text selectors.** `section.querySelector('.cta-primary')` is more resilient than `section.querySelector('a:nth-child(3)')` when copy moves.
4. **Always include a "partial standalone" sanity check.** It catches the case where the partial file got out of sync with what is inlined in `index.html`.
5. **For external links**, assert all three of `href` (contains expected domain), `target="_blank"`, and `rel` (contains `noopener`).
6. **For images**, assert non-empty `src` (regex on the suffix is fine) AND non-empty descriptive `alt` (NFR-3 WCAG 2.1 AA).
7. **For content.json-driven values**, read the JSON in `beforeAll`; never duplicate URLs / labels in the test file itself.

## Running the tests

```bash
cd homepage
npm install            # first run only
npx jest tests/unit/<name>.test.js
# or all unit tests
npm run test:unit
```

## See also

- `homepage/tests/unit/hero.test.js` — canonical reference implementation that covers all six hero scenario test cases.
- `homepage/tests/helpers/dom.js` — the shared loader used by every section test.
- `homepage/src/data/content.json` — the data source you assert against.
- `.something/scaffold.md` — folder ownership rules per scenario.
