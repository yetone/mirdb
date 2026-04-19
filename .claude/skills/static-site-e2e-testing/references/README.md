# Static Site E2E Testing with Playwright

## Overview

This skill documents the pattern for setting up E2E tests for static HTML/CSS/JS sites using Playwright and the `serve` package for local development server.

## When to Use This Skill

Use this skill when:

- Setting up E2E tests for static HTML pages
- Testing UI components without a build system
- Creating Playwright tests for pure HTML/CSS/JS sites
- Testing page navigation and links in static sites

## Project Structure

```
project/
├── index.html
├── css/
│   └── styles.css
├── js/
│   └── main.js
├── tests/
│   ├── e2e/
│   │   ├── playwright.config.js
│   │   └── *.spec.js
│   └── unit/
│       ├── jest.config.cjs
│       └── *.test.js
└── package.json
```

## Core Configuration

### Playwright Config (tests/e2e/playwright.config.js)

Key insight: webServer command path is relative to config file location.

```javascript
import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: './',
  testMatch: '**/*.spec.js',
  webServer: {
    command: 'npx serve ../.. -l 3000',  // Path relative to config file
    url: 'http://localhost:3000',
    reuseExistingServer: !process.env.CI,
    timeout: 120000,
  },
  use: {
    baseURL: 'http://localhost:3000',
  },
});
```

### Jest Config for HTML Testing (tests/unit/jest.config.cjs)

Use `.cjs` extension when package.json has `"type": "module"`.

```javascript
module.exports = {
  testEnvironment: 'jsdom',
  rootDir: '../../',
  testMatch: ['<rootDir>/tests/unit/**/*.test.js'],
  transform: { '^.+\\.js$': 'babel-jest' },
};
```

## Best Practices

- Use `npx serve` for local static file serving in tests
- Install Playwright browsers: `npx playwright install chromium`
- For system deps: `npx playwright install-deps chromium`
- Use `.cjs` for config files with ES module projects

## Common Issues

1. **"Cannot use import statement outside a module"** - Use `.cjs` extension for Jest config
2. **WebServer not finding files** - Check path is relative to config file location
3. **Browser launch fails** - Run `npx playwright install-deps chromium`
