---
name: jest-jsdom-css-testing
description: Test HTML/CSS layouts by reading CSS files directly when jsdom doesn't load external stylesheets. Use when testing CSS grid, flexbox, or other CSS properties.
scope: project
---

# Jest jsdom CSS Testing Pattern

When testing HTML/CSS with Jest + jsdom, external stylesheets are not loaded. This skill shows how to verify CSS rules by reading the stylesheet file directly.

## Quick Start

```javascript
test('element uses CSS grid layout', () => {
  const fs = require('fs');
  const path = require('path');

  // Read CSS file directly
  const cssPath = path.resolve(__dirname, '../src/styles.css');
  const cssContent = fs.readFileSync(cssPath, 'utf8');

  // Verify CSS rule exists
  expect(cssContent).toContain('.features-grid');
  expect(cssContent).toContain('display: grid');
});
```

See [README.md](references/README.md) for full documentation.
