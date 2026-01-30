---
name: playwright-landing-page-testing
description: E2E testing pattern for static landing pages using Playwright with test structure, shared utilities, and common assertions.
scope: project
---

# Playwright Landing Page Testing

Use this skill when writing E2E tests for the MirDB landing page using Playwright.

## Quick Start

```bash
# Install dependencies
cd landing-page && npm install

# Install Playwright browsers
npx playwright install chromium
npx playwright install-deps chromium

# Run tests
npx playwright test tests/e2e/
```

## Test Structure

Tests are organized by page section:
- `tests/e2e/hero-section.test.js` - Hero section tests
- `tests/e2e/features-section.test.js` - Features grid tests
- `tests/helpers/test-utils.js` - Shared utilities

See [README.md](references/README.md) for full documentation.
