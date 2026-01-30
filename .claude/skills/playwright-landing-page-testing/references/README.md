# Playwright Landing Page Testing

## Overview

This skill provides patterns for E2E testing static landing pages using Playwright, including test organization, shared utilities, and common assertions for verifying page content and accessibility.

## When to Use This Skill

Use this skill when:

- Writing E2E tests for landing page sections
- Testing page content (text, images, links)
- Verifying accessibility attributes
- Setting up Playwright test infrastructure

## Core Capabilities

### 1. Test File Structure

Each page section has its own test file:

```javascript
const { test, expect } = require('@playwright/test');
const { setupPage } = require('../helpers/test-utils');

test.describe('Section Name', () => {
  test.beforeEach(async ({ page }) => {
    await setupPage(page);
  });

  test('descriptive test name', async ({ page }) => {
    // Test implementation
  });
});
```

### 2. Shared Test Utilities

Use the test-utils helper for common operations:

```javascript
// tests/helpers/test-utils.js
const VIEWPORTS = {
  mobile: { width: 375, height: 667 },
  tablet: { width: 768, height: 1024 },
  desktop: { width: 1280, height: 800 },
};

async function setupPage(page) {
  await page.goto('/');
  await page.waitForLoadState('domcontentloaded');
}

async function setViewport(page, device) {
  const viewport = VIEWPORTS[device];
  if (viewport) {
    await page.setViewportSize(viewport);
  }
}
```

### 3. Common Assertions

```javascript
// Verify element visibility
await expect(page.locator('#hero')).toBeVisible();

// Verify text content
const text = await element.textContent();
expect(text).toContain('expected text');

// Verify attributes
await expect(element).toHaveAttribute('href', 'https://example.com');
await expect(element).toHaveAttribute('target', '_blank');
await expect(element).toHaveClass(/btn/);

// Verify image source
const src = await image.getAttribute('src');
expect(src).toContain('logo.gif');
```

### 4. Playwright Configuration

```javascript
// playwright.config.js
module.exports = defineConfig({
  testDir: './tests',
  use: {
    baseURL: 'http://localhost:3000',
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
  webServer: {
    command: 'npx serve . -l 3000',
    url: 'http://localhost:3000',
    reuseExistingServer: !process.env.CI,
  },
});
```

## Best Practices

- Use `setupPage()` in `beforeEach` for consistent page state
- Test accessibility attributes (aria-label, aria-labelledby, alt text)
- Verify external links have `target="_blank"` and `rel="noopener"`
- Use descriptive test names that match scenario requirements
- Group related tests with `test.describe()`

## Installation

```bash
# Install dependencies
npm install --save-dev @playwright/test serve

# Install browsers
npx playwright install chromium
npx playwright install-deps chromium

# Run tests
npx playwright test
```
