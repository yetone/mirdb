# vitest + jsdom integration tests for the MirDB static homepage

The MirDB homepage is a no-build static site (plain HTML/CSS/JS) under `homepage/`. Integration tests load `homepage/index.html` directly into a fresh JSDOM instance per test — there is no bundler, no dev server, and no transpilation. This skill captures the exact pattern used so new section scenarios (features, install, usage, architecture, resources, footer, …) can be tested the same way without rediscovering the setup.

## When to use

- Adding a new file under `homepage/tests/integration/<scenario>.test.js`
- Modifying any `homepage/sections/*.html` partial that is also inlined into `homepage/index.html`
- Debugging an integration test that fails because the section markup is missing or selectors changed

## When NOT to use

- Unit tests for plain JS modules (`homepage/js/*.js`) — those go in `tests/unit/` and import the module directly
- E2E / cross-browser tests — those use real Playwright (`tests/e2e/`)
- Anything outside `homepage/` (the Rust workspace at the repo root)

## Project conventions

| Concern | Convention |
|---|---|
| Test runner | `vitest run` (configured in `homepage/vitest.config.js`) |
| Test environment | `jsdom` (set globally in vitest config) |
| Test file location | `homepage/tests/integration/<scenario>.test.js` |
| HTML loaded | `homepage/index.html` (sections inlined into it; partials in `sections/` are not imported at test time) |
| DOM API | `JSDOM` constructor with `runScripts: 'dangerously'` and `resources: 'usable'` |
| Cross-scenario selectors | `#hero`, `#features`, `#install`, `#usage`, `#architecture`, `#resources`, `#footer`, `#theme-toggle`, `#hamburger`, `.copy-btn`, `a.cta` — DO NOT rename |
| Test isolation | Fresh JSDOM per test via `beforeEach` / `afterEach` |

## Canonical test template

```js
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

const __dirname = path.dirname(new URL(import.meta.url).pathname);
const INDEX_PATH = path.resolve(__dirname, '../../index.html');

function loadHomepage() {
  const html = fs.readFileSync(INDEX_PATH, 'utf-8');
  return new JSDOM(html, {
    url: 'http://localhost:3000',
    runScripts: 'dangerously',
    resources: 'usable',
  });
}

describe('<Section Name>', () => {
  let dom;
  let document;

  beforeEach(() => {
    dom = loadHomepage();
    document = dom.window.document;
  });

  afterEach(() => {
    dom = null;
    document = null;
  });

  it('positive: renders the required selector', () => {
    const el = document.querySelector('#<section-id>');
    expect(el).not.toBeNull();
  });

  it('negative: regression guard for the structural contract', () => {
    const el = document.querySelector('#<section-id> h2');
    expect(el).not.toBeNull();
  });
});
```

## Simulating clicks (test-case 5 pattern)

```js
const cta = document.querySelector('#hero a.cta');
const clickEvent = new dom.window.MouseEvent('click', {
  bubbles: true,
  cancelable: true,
});
cta.dispatchEvent(clickEvent);
// Assert against the href contract — actual smooth scroll lives in navigation.js
expect(cta.getAttribute('href')).toBe('#install');
```

JSDOM does not perform smooth-scroll or hash navigation reliably for anchor activation; assert against the href contract instead. Real hash-update is verified by Playwright e2e tests in `tests/e2e/`.

## Why inline the section into index.html?

The test pipeline reads `index.html` as a static string; HTML comment include markers (`<!-- include: sections/foo.html -->`) are not resolved without a build step. So each scenario:

1. Writes its canonical partial to `homepage/sections/<scenario>.html` (owned exclusively by that scenario)
2. Inlines the same block into `homepage/index.html` between the include markers seeded by the first builder

This duplication is intentional — it keeps the test pipeline trivial and is what makes parallel scenarios possible.

## Running the tests

```bash
cd homepage
npm install              # if node_modules is missing
npx vitest run tests/integration/<scenario>.test.js
# or run everything:
npm test
```

`npx vitest run` (not `vitest watch`) — the scenario harness derives pass/fail from the exit code and must not block.

## Common pitfalls

1. **Trying to assert window.location.hash after a click** — JSDOM doesn't update it from anchor activation. Assert href instead.
2. **Forgetting to inline the section into index.html** — the partial alone won't be picked up; tests will see the placeholder div.
3. **Renaming a public selector** — breaks other scenarios. Add new selectors instead; treat the table above as a contract.
4. **Sharing DOM across tests** — always reload via `beforeEach`. A theme toggle from one test will pollute the next.
5. **Importing CommonJS helpers from ESM tests** — `tests/helpers/dom.js` is currently CommonJS; the integration test files redeclare `loadHomepage()` inline as ESM. Either is fine; just don't `import` the CJS helper from an ESM test file.
