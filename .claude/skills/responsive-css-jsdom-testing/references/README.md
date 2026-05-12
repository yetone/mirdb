# Responsive CSS Testing in the Homepage Workspace

A pattern for asserting `@media` breakpoint behavior from `jest-environment-jsdom`-backed tests without adding a CSS AST dependency (postcss/csstree). Lives in `homepage/tests/integration/responsive.test.js` and is the canonical reference for any future test that needs to inspect specific declarations inside specific `@media` blocks.

## When to use

- You need to verify that a specific `@media` query exists in a stylesheet.
- You need to read declarations (e.g. `grid-template-columns`, `display`) out of a rule that lives inside an `@media` block.
- You need to drive layout-conditional JS (like `initNavigation()`) under a simulated viewport via `window.matchMedia`.
- You want to keep `homepage/package.json` devDependencies thin (jest + jest-environment-jsdom only — no postcss / csstree).

## The three core helpers

### 1. `extractMediaBlocks(css)` — pull top-level `@media` blocks out of CSS text

This is a brace-depth-tracking parser. It walks the CSS string, finds each `@media` keyword, captures the query text up to the opening brace, and then captures the body by counting `{`/`}` until it returns to depth zero. It correctly handles nested rules (e.g. selectors with their own braces) without needing a real CSS parser.

```js
function extractMediaBlocks(css) {
  const blocks = [];
  for (let i = 0; i < css.length; i++) {
    if (css.startsWith('@media', i)) {
      const braceStart = css.indexOf('{', i);
      if (braceStart === -1) break;
      const query = css.slice(i + 6, braceStart).trim();
      let depth = 1;
      let j = braceStart + 1;
      while (j < css.length && depth > 0) {
        if (css[j] === '{') depth++;
        else if (css[j] === '}') depth--;
        if (depth === 0) break;
        j++;
      }
      const body = css.slice(braceStart + 1, j);
      blocks.push({ query, body });
      i = j;
    }
  }
  return blocks;
}
```

Returns an array of `{ query, body }` — `query` is the text after `@media` and before `{`; `body` is the raw inner text.

### 2. `extractRuleBody(cssBody, selector)` — pull a single rule out of a CSS fragment

Once you have the body of an `@media` block, you typically want one rule. This regex-based helper finds the first rule for a given selector and returns its declaration body.

```js
function extractRuleBody(cssBody, selector) {
  const escaped = selector.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const re = new RegExp(
    '(?:^|[}\\s,])' + escaped + '\\s*(?:,[^{]*)?\\{([^{}]*)\\}',
    'm'
  );
  const m = re.exec(cssBody);
  return m ? m[1].trim() : null;
}
```

It escapes the selector for regex use, accepts an optional comma-separated selector list, and tolerates leading whitespace or a preceding `}`. Returns the declaration body (without braces) or `null`.

### 3. `getProp(ruleBody, prop)` — read a single declaration value

```js
function getProp(ruleBody, prop) {
  if (!ruleBody) return null;
  const re = new RegExp('(?:^|;|\\s)' + prop + '\\s*:\\s*([^;]+)', 'i');
  const m = re.exec(ruleBody);
  return m ? m[1].trim() : null;
}
```

Case-insensitive, semicolon-terminated. Returns the trimmed value or `null`.

## Driving viewport-conditional JS with `makeMatchMediaStub`

The DOM helpers above are static — they only look at the stylesheet. To drive code that branches on `window.matchMedia(...)` (e.g. progressive enhancement scripts), stub it:

```js
function makeMatchMediaStub(matchedQueries) {
  const normalized = matchedQueries.map((q) => q.replace(/\s+/g, ''));
  return function matchMedia(query) {
    const key = String(query || '').replace(/\s+/g, '');
    const matches = normalized.includes(key);
    return {
      matches,
      media: query,
      onchange: null,
      addEventListener() {},
      removeEventListener() {},
      addListener() {},
      removeListener() {},
      dispatchEvent() { return false; },
    };
  };
}
```

Two important properties:
- **Whitespace-normalized comparison.** `(max-width:640px)`, `(max-width: 640px)`, and `(max-width:  640px)` all hit the same key. Real browser `matchMedia` is whitespace-insensitive too, so tests should not be coupled to author formatting.
- **Duck-typed `MediaQueryList`.** Only the fields jsdom navigation code touches (`matches`, `media`, `addEventListener`, etc.) are populated — no need to construct a real `MediaQueryList`.

Install it on the jsdom window before calling code-under-test:

```js
const doc = loadFullPage();
doc.defaultView.matchMedia = makeMatchMediaStub(['(max-width: 640px)']);
```

## Project-specific breakpoint conventions

This workspace uses **non-overlapping integer breakpoints**:

| Tier         | Query                                            |
|--------------|--------------------------------------------------|
| mobile       | `(max-width: 640px)`                             |
| tablet       | `(min-width: 641px) and (max-width: 1024px)`     |
| desktop      | `(min-width: 1025px)`                            |
| wide-desktop | `(min-width: 1440px)` (optional, opportunistic)  |

The boundaries are deliberately **641** (not 640) and **1025** (not 1024) at the upper-band starts. Off-by-one overlap is a classic CSS responsive bug — two media blocks both match at the boundary viewport and the cascade order silently picks the winner. Choosing inclusive integer endpoints avoids that ambiguity entirely.

When writing assertions that locate one of these blocks, use the *combination* of patterns to disambiguate:

```js
// mobile-only: has max-width 640 AND no min-width
const mobile = blocks.find(
  (b) => /max-width:\s*640px/.test(b.query) && !/min-width/.test(b.query)
);

// desktop-only: has min-width 1025 AND no max-width (so wide-desktop is skipped)
const desktop = blocks.find(
  (b) => /min-width:\s*1025px/.test(b.query) && !/max-width/.test(b.query)
);
```

## Reusing existing navigation.js as a black box

When a test needs both a breakpoint stub *and* real DOM behavior driven by an existing component script, follow the test_case 7 pattern in `responsive.test.js`:

```js
const doc = loadPartial('src/components/navigation/navigation.html');
doc.defaultView.matchMedia = makeMatchMediaStub(['(max-width: 640px)']);

// Prevent the script's auto-init IIFE so the test drives initialization deterministically.
doc.MirdbNavigationSkipAutoInit = true;
delete require.cache[require.resolve('../../src/scripts/navigation.js')];
require('../../src/scripts/navigation.js');
window.MirdbNavigation.initNavigation(doc);

const toggle = doc.querySelector('.nav-toggle');
toggle.click();
expect(toggle.getAttribute('aria-expanded')).toBe('true');
```

Three details that matter:
1. **Set the skip flag on the document**, not just the window. The navigation IIFE reads from the doc the script is loaded into.
2. **`delete require.cache[...]`** before `require()`. Otherwise repeated tests in the same Jest worker share module state.
3. **Call `initNavigation(doc)` explicitly** — the auto-init has been suppressed, so nothing wires up until you ask.

## End-to-end example

```js
describe('Responsive layout', () => {
  let css;
  let blocks;

  beforeAll(() => {
    css = fs.readFileSync(
      path.join(REPO_ROOT, 'src/styles/responsive.css'),
      'utf8'
    );
    blocks = extractMediaBlocks(css);
  });

  test('mobile reflows .features-grid to a single column', () => {
    const mobile = blocks.find(
      (b) => /max-width:\s*640px/.test(b.query) && !/min-width/.test(b.query)
    );
    expect(mobile).toBeDefined();

    const ruleBody = extractRuleBody(mobile.body, '.features-grid');
    const cols = getProp(ruleBody, 'grid-template-columns');
    expect(/^1fr$|repeat\(\s*1\s*,/.test(cols)).toBe(true);
  });
});
```

## What this skill deliberately does NOT do

- **No CSSOM via `document.styleSheets`.** jsdom's CSSOM implementation does not fully expose `@media` rule children in a stable way, so reaching for `styleSheets[i].cssRules` is fragile. Reading the raw `.css` file from disk is more reliable.
- **No postcss / csstree.** A 22-line brace-depth parser is enough for the asserts this workspace needs. Skip the transitive dependency surface.
- **No `getComputedStyle()` assertion of media-query effects.** jsdom does not actually evaluate media queries against the viewport. The pattern here asserts the *declarations* exist in the right `@media` block, not that they would compute at a particular viewport size. That separation is intentional and is what makes the tests fast and deterministic.

## File map

| File | What it shows |
|------|---------------|
| `homepage/tests/integration/responsive.test.js` | Reference implementation of all helpers and 7 example assertions |
| `homepage/src/styles/responsive.css` | The four-tier stylesheet under test |
| `homepage/src/scripts/navigation.js` | The script that test_case 7 drives as a black box |
| `homepage/tests/helpers/dom.js` | `loadFullPage()` and `loadPartial()` jsdom builders |
