# Cross-Browser Static Testing

## Overview

Use Vitest + jsdom to analyze HTML, CSS, and JS for cross-browser compatibility without requiring actual browser automation. Tests verify feature support by inspecting code rather than runtime behavior.

## When to Use This Skill

Use this skill when users request:

- Testing cross-browser compatibility of static websites
- Validating CSS feature support across target browsers
- Checking JS API compatibility without runtime execution
- Projects already using Vitest + jsdom for unit tests

## Core Capabilities

### 1. Load HTML into jsdom

```javascript
import { JSDOM } from 'jsdom';

function loadDOM() {
  const html = readFileSync(resolve(__dirname, '../index.html'), 'utf-8');
  return new JSDOM(html, { url: 'http://localhost:8080' });
}
```

### 2. Aggregate all CSS for analysis

```javascript
function getAllCSS() {
  const paths = [
    resolve(__dirname, '../css/base.css'),
    resolve(__dirname, '../css/hero.css'),
    // ... other CSS files
  ];
  return paths
    .filter(p => existsSync(p))
    .map(p => readFileSync(p, 'utf-8'))
    .join('\n');
}
```

### 3. Test browser-specific features

```javascript
describe('Chrome Compatibility', () => {
  it('renders hero section correctly', () => {
    const dom = loadDOM();
    const hero = dom.window.document.getElementById('hero');
    expect(hero).not.toBeNull();
  });
});
```

### 4. Verify CSS feature support

```javascript
it('uses CSS Grid (supported in all latest browsers)', () => {
  const css = getAllCSS();
  expect(css).toContain('display: grid');
});
```

### 5. Check JS compatibility

```javascript
it('uses ES5-compatible syntax for broad support', () => {
  const js = readFileSync(jsPath, 'utf-8');
  expect(js).toContain('var ');
  expect(js).not.toContain('=>');
});
```

## Key Assertions

### CSS Verification
- `display: grid` / `display: flex` for layout support
- `position: sticky` for header behavior
- `scroll-behavior: smooth` for smooth scrolling
- `var(--*)` for CSS custom properties
- Vendor prefixes: `-webkit-font-smoothing`, `-moz-osx-font-smoothing`

### JS Verification
- `var` instead of `let`/`const` for maximum compatibility
- `function` declarations instead of arrow functions
- `addEventListener` for event handling
- `querySelector`/`querySelectorAll` for DOM selection
- `classList` API for class manipulation
- `passive: true` for scroll event listeners

### HTML Verification
- All major sections present in DOM
- Navigation links functional
- External links have `rel="noopener noreferrer"`
- Scripts loaded with `defer`
- Viewport meta tag present

## Best Practices

- Use mobile-first CSS to provide fallbacks for older browsers
- Verify vendor prefixes are present where historically needed
- Test that JS uses ES5-compatible syntax for maximum browser support
- Check that all script and stylesheet references resolve to existing files
- Validate semantic HTML landmarks for accessibility consistency

## Resources

### references/

- `README.md` - This documentation
