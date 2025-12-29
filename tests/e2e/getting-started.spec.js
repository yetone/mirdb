// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '../../index.html');

/**
 * Getting Started and Installation Section E2E Tests
 * Tests REQ-8 from PRD - Installation instructions and quick-start guide
 */

test.describe('Getting Started and Installation Section', () => {

  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  // Test Case 1: Section exists with heading containing 'Getting Started' or 'Installation'
  test('TC1: Getting Started section exists with proper heading', async ({ page }) => {
    // Find the getting-started section
    const gettingStartedSection = page.locator('#getting-started, .getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Check for heading containing 'Getting Started' or 'Installation'
    const heading = gettingStartedSection.locator('h2, h1');
    await expect(heading).toBeVisible();

    const headingText = await heading.textContent();
    const hasGettingStarted = headingText.toLowerCase().includes('getting started');
    const hasInstallation = headingText.toLowerCase().includes('installation');

    expect(hasGettingStarted || hasInstallation).toBeTruthy();
  });

  // Test Case 2: Code block contains 'git clone' command for MirDB repository
  test('TC2: Clone command exists with git clone for MirDB repository', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started, .getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Find code block with git clone command
    const codeBlock = gettingStartedSection.locator('code');
    await expect(codeBlock).toBeVisible();

    const codeText = await codeBlock.textContent();
    expect(codeText).toContain('git clone');
    // Should contain mirdb in the clone URL
    expect(codeText.toLowerCase()).toContain('mirdb');
  });

  // Test Case 3: Code block contains 'cargo build --release' command
  test('TC3: Build command exists with cargo build --release', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started, .getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Find code block with cargo build command
    const codeBlock = gettingStartedSection.locator('code');
    await expect(codeBlock).toBeVisible();

    const codeText = await codeBlock.textContent();
    expect(codeText).toContain('cargo build --release');
  });

  // Test Case 4: Code block shows how to run MirDB with config file (mirdb.toml)
  test('TC4: Run command shows how to run MirDB with config file', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started, .getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Find code block with run command
    const codeBlock = gettingStartedSection.locator('code');
    await expect(codeBlock).toBeVisible();

    const codeText = await codeBlock.textContent();
    // Should show how to run with config file
    expect(codeText).toContain('mirdb');
    expect(codeText.toLowerCase()).toContain('.toml');
  });

  // Test Case 5: Link to documentation exists and is functional
  test('TC5: Documentation link exists and points to valid resource', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started, .getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Find documentation link - could be labeled as 'documentation', 'docs', 'full documentation', 'README', etc.
    const docLink = gettingStartedSection.locator('a').filter({
      hasText: /documentation|docs|readme|full doc/i
    }).first();

    await expect(docLink).toBeVisible();

    // Verify link has a valid href
    const href = await docLink.getAttribute('href');
    expect(href).toBeTruthy();
    // Should be a valid URL (github, external docs, or relative path)
    const isValidLink = href.startsWith('http') || href.startsWith('/') || href.startsWith('#') || href.startsWith('./');
    expect(isValidLink).toBeTruthy();
  });

});
