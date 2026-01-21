# Jest jsdom CSS Testing

## Overview

Test HTML/CSS layouts and styling when using Jest with jsdom environment. Since jsdom doesn't load external CSS stylesheets, this pattern verifies CSS rules by reading the stylesheet file directly.

## When to Use This Skill

Use this skill when:

- Testing CSS Grid or Flexbox layouts in jsdom
- Verifying CSS rules exist for specific classes
- Testing visual consistency of elements via CSS class assignment
- Writing integration tests for landing pages or UI components

## The Problem

jsdom (used by Jest) doesn't load external stylesheets. This means:

```javascript
// This DOES NOT WORK in jsdom:
const style = window.getComputedStyle(element);
expect(style.display).toBe('grid'); // Always returns default value!
```

## The Solution

Read and parse the CSS file directly:

```javascript
test('features section uses CSS grid layout', () => {
  const fs = require('fs');
  const path = require('path');

  // 1. Verify DOM element exists
  const featuresGrid = document.querySelector('.features-grid');
  expect(featuresGrid).toBeInTheDocument();

  // 2. Read the CSS file
  const cssPath = path.resolve(__dirname, '../src/styles.css');
  const cssContent = fs.readFileSync(cssPath, 'utf8');

  // 3. Verify CSS rules
  expect(cssContent).toContain('.features-grid');
  expect(cssContent).toContain('display: grid');
});
```

## Testing Visual Consistency

For visual consistency tests, verify all elements share the same CSS class:

```javascript
test('all feature cards have consistent styling', () => {
  const fs = require('fs');
  const path = require('path');

  const featureCards = document.querySelectorAll('.feature-card');

  // All cards share same class
  const allHaveSameClass = Array.from(featureCards).every(card => {
    return card.classList.contains('feature-card');
  });
  expect(allHaveSameClass).toBe(true);

  // CSS class defines consistent properties
  const cssPath = path.resolve(__dirname, '../src/styles.css');
  const cssContent = fs.readFileSync(cssPath, 'utf8');

  const featureCardRule = cssContent.match(/\.feature-card\s*\{[^}]+\}/);
  expect(featureCardRule).not.toBeNull();

  const cardCSS = featureCardRule[0];
  expect(cardCSS).toContain('padding');
  expect(cardCSS).toContain('border-radius');
  expect(cardCSS).toContain('background-color');
});
```

## Best Practices

1. **Always verify DOM element exists first** before checking CSS
2. **Use regex to extract specific CSS rules** when checking complex properties
3. **Combine with data-testid attributes** for reliable DOM queries
4. **Keep CSS verification focused** - test that rules exist, not exact values

## Project Setup

### jest.config.js

```javascript
module.exports = {
  testEnvironment: 'jsdom',
  setupFilesAfterEnv: ['<rootDir>/tests/setup.js'],
  testMatch: ['**/tests/**/*.test.js'],
};
```

### tests/setup.js

```javascript
const fs = require('fs');
const path = require('path');
require('@testing-library/jest-dom');

beforeEach(() => {
  const htmlPath = path.resolve(__dirname, '../index.html');
  const html = fs.readFileSync(htmlPath, 'utf8');
  document.documentElement.innerHTML = html;
});
```

## Dependencies

```json
{
  "devDependencies": {
    "jest": "^29.7.0",
    "jest-environment-jsdom": "^29.7.0",
    "@testing-library/dom": "^9.3.0",
    "@testing-library/jest-dom": "^6.1.0"
  }
}
```
