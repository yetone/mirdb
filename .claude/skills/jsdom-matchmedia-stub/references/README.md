# jsdom window.matchMedia Stub

## Overview

jsdom does not implement layout, so `window.matchMedia('(max-width: 640px)')` always returns `{ matches: false }` regardless of viewport size. This skill documents the project's approach: a tiny stub factory that returns `matches: true` only for whitelisted query strings, while otherwise satisfying the MediaQueryList interface (no thrown errors when navigation code attaches listeners).

## When to Use This Skill

Use this skill when users request:

- "Write a Jest test that exercises code branching on viewport width."
- "Make the responsive nav script testable under jsdom."
- "Force matchMedia to report mobile / tablet / desktop in a test."

## Core Pattern

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

doc.defaultView.matchMedia = makeMatchMediaStub(['(max-width: 640px)']);
```

## Why every MediaQueryList field?

Production code commonly attaches listeners (`mql.addEventListener('change', ...)`) or reads `mql.media`. Returning a bare `{ matches }` object will crash the test the moment that code runs. The stub returns a complete shape so callers continue without modification.

## Why whitespace normalization?

The CSS spec considers `(max-width: 640px)` and `(max-width:640px)` equivalent — both are spec-compliant queries. Tests should match against the meaning, not the formatting, so the stub strips whitespace before comparing.

## Best Practices

- **Always whitelist the exact queries your test cares about** — don't pass all queries as matching. Tests that report `matches: true` for everything will pass even when production code reads the wrong breakpoint.
- **Stub on `doc.defaultView`, not the global `window`** — when using `loadPartial` / `loadFullPage` helpers from `tests/helpers/dom.js`, each test gets its own jsdom Window. Install the stub on that document's defaultView so the test does not leak state into sibling tests.
- **Pair with the matching CSS-text assertion** — `matchMedia` only proves the helper exists; it does not prove the CSS rule fires. Combine with `extractMediaBlocks` (see `css-media-block-parser` skill) to assert the declaration is actually present in `responsive.css`.
- **Do not call `Object.defineProperty` to install** — direct assignment to `doc.defaultView.matchMedia` is enough and avoids "Cannot redefine property" errors when the test runs twice in watch mode.

## Example: forcing mobile then clicking the hamburger

```js
const doc = loadPartial('src/components/navigation/navigation.html');
doc.defaultView.matchMedia = makeMatchMediaStub(['(max-width: 640px)']);

doc.MirdbNavigationSkipAutoInit = true;
delete require.cache[require.resolve('../../src/scripts/navigation.js')];
require('../../src/scripts/navigation.js');
window.MirdbNavigation.initNavigation(doc);

const toggle = doc.querySelector('.nav-toggle');
toggle.click();

expect(toggle.getAttribute('aria-expanded')).toBe('true');
```

## Resources

### references/

- `README.md` — This documentation
