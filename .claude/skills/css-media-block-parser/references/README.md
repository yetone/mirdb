# CSS @media Block Parser

## Overview

A brace-balanced parser that extracts top-level `@media` blocks (and the rules inside them) from raw CSS text. Designed for jsdom-based test suites where the layout engine cannot evaluate media queries — instead of asking the browser to apply rules, the test reads the source file and asserts on the declarations directly.

## When to Use This Skill

Use this skill when users request:

- "Test that the mobile @media rule sets a specific column count."
- "Assert that a CSS file declares the expected breakpoints."
- "Run integration tests on responsive CSS under jsdom."
- "Check that a CSS rule has the right declaration inside a specific media query."

## Core Capabilities

### 1. extractMediaBlocks(css)

Walks a CSS string character-by-character. When it encounters `@media`, it records the query, finds the opening `{`, then tracks brace nesting until the matching `}`. Returns `[{ query, body }]` where `body` is the raw text between the outer braces (still containing nested rules).

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
      blocks.push({ query, body: css.slice(braceStart + 1, j) });
      i = j;
    }
  }
  return blocks;
}
```

### 2. extractRuleBody(cssBody, selector)

Pulls the declaration block for a selector inside a media-block body. The selector is regex-escaped (because CSS uses `.`, `#`, `[]` which are regex metacharacters). The leading `(?:^|[}\\s,])` ensures the match does not start mid-identifier (e.g., `.nav-menu` should not accidentally match `.nav-menu-item`).

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

### 3. getProp(ruleBody, prop)

Reads the value of a single property out of a rule body. Tolerates leading semicolons and whitespace, and stops at the next `;`.

```js
function getProp(ruleBody, prop) {
  if (!ruleBody) return null;
  const re = new RegExp('(?:^|;|\\s)' + prop + '\\s*:\\s*([^;]+)', 'i');
  const m = re.exec(ruleBody);
  return m ? m[1].trim() : null;
}
```

## Best Practices

- **Use this pattern for static structure checks** — verifying breakpoints exist, columns counts are correct, display properties are present. Cheap and deterministic.
- **Do not use this pattern to assert layout** — for "the grid actually rendered with 3 columns" you need a real browser (Playwright/Cypress).
- **Normalize whitespace in regex** — `\\s*` between identifier and `:` accommodates both `display: none` and `display:none`.
- **Accept either `1fr` or `repeat(1, 1fr)`** — both are valid one-column grid declarations. Tests should accept either form.
- **Treat the parser as testing infrastructure** — keep it inside the test file (or a tiny `tests/helpers/css.js`) rather than the production codebase.

## Example: Asserting a tablet breakpoint exists

```js
const css = fs.readFileSync('src/styles/responsive.css', 'utf8');
const blocks = extractMediaBlocks(css);

const tablet = blocks.find(
  (b) => /min-width:\s*641px/.test(b.query) && /max-width:\s*1024px/.test(b.query)
);
expect(tablet).toBeDefined();

const ruleBody = extractRuleBody(tablet.body, '.features-grid');
expect(getProp(ruleBody, 'grid-template-columns')).toMatch(/repeat\(\s*2\s*,/);
```

## Resources

### scripts/

- `extract_media.js` — Standalone Node.js script that prints all @media blocks of a CSS file, useful for ad-hoc inspection.

### references/

- `README.md` — This documentation
