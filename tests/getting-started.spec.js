// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

// Test suite for Getting Started section (REQ-3, US-3)
test.describe('Getting Started Section', () => {
  test.beforeEach(async ({ page }) => {
    // Load the index.html file directly
    const filePath = path.resolve(__dirname, '../index.html');
    await page.goto(`file://${filePath}`);
  });

  // Test Case 1: Check for installation command
  test('TC1: Installation command is displayed (cargo install or similar)', async ({ page }) => {
    // Navigate to Getting Started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Check for installation command - should contain 'cargo install' or similar
    const installationCommand = gettingStartedSection.locator('code', {
      hasText: /cargo install|cargo build|git clone/
    });
    await expect(installationCommand.first()).toBeVisible();

    // Verify the installation instruction contains 'cargo install mirdb-server'
    const cargoInstallCommand = gettingStartedSection.locator('pre code', {
      hasText: 'cargo install mirdb-server'
    });
    await expect(cargoInstallCommand).toBeVisible();
  });

  // Test Case 2: Check for SET operation example
  test('TC2: Code example showing SET command usage is present', async ({ page }) => {
    // Navigate to Getting Started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Check for SET operation example
    // Looking for text that contains 'set' command usage
    const setExample = gettingStartedSection.locator('pre code', {
      hasText: /set\s+\w+/i
    });
    await expect(setExample.first()).toBeVisible();

    // Verify there's a heading or text about SET operation
    const setHeading = gettingStartedSection.locator('h4', { hasText: /SET/i });
    await expect(setHeading).toBeVisible();
  });

  // Test Case 3: Check for GET operation example
  test('TC3: Code example showing GET command usage is present', async ({ page }) => {
    // Navigate to Getting Started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Check for GET operation example
    // Looking for text that contains 'get' command usage
    const getExample = gettingStartedSection.locator('pre code', {
      hasText: /get\s+\w+/i
    });
    await expect(getExample.first()).toBeVisible();

    // Verify there's a heading or text about GET operation
    const getHeading = gettingStartedSection.locator('h4', { hasText: /GET/i });
    await expect(getHeading).toBeVisible();
  });

  // Test Case 4: Verify code syntax highlighting is applied
  test('TC4: Code blocks have CSS classes or styles for syntax highlighting (Prism.js classes)', async ({ page }) => {
    // Navigate to Getting Started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Check for code blocks with language classes (Prism.js uses language-* classes)
    const codeBlocksWithLanguageClass = gettingStartedSection.locator('code[class*="language-"]');
    const count = await codeBlocksWithLanguageClass.count();
    expect(count).toBeGreaterThan(0);

    // Verify that at least one code block has Prism-tokenized content
    // Prism adds classes like 'token', 'keyword', 'string', 'function', etc.
    // After Prism processes code, tokens get wrapped in spans
    const firstCodeBlock = codeBlocksWithLanguageClass.first();
    await expect(firstCodeBlock).toHaveClass(/language-(bash|python|rust|sh|shell)/);

    // Check that code blocks have pre element with language class
    const preWithLanguageClass = gettingStartedSection.locator('pre[class*="language-"], pre:has(code[class*="language-"])');
    const preCount = await preWithLanguageClass.count();
    expect(preCount).toBeGreaterThan(0);

    // Verify the CSS file is linked (checking for prism.css)
    const prismCss = page.locator('link[href*="prism.css"]');
    await expect(prismCss).toHaveAttribute('href', /prism\.css/);

    // Verify prism.js script is included
    const prismJs = page.locator('script[src*="prism.js"]');
    await expect(prismJs).toHaveAttribute('src', /prism\.js/);
  });

  // Test Case 5: Check for link to full documentation
  test('TC5: Link to detailed documentation is present in Getting Started section', async ({ page }) => {
    // Navigate to Getting Started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Check for documentation link - should contain link to GitHub README or docs
    const docsLink = gettingStartedSection.locator('a[href*="github.com/yetone/mirdb"]');
    await expect(docsLink.first()).toBeVisible();

    // Verify there's a link with text about documentation
    const fullDocsLink = gettingStartedSection.locator('a', {
      hasText: /documentation|docs|learn more/i
    });
    await expect(fullDocsLink.first()).toBeVisible();

    // Check that the link has appropriate href
    const docsLinkHref = await fullDocsLink.first().getAttribute('href');
    expect(docsLinkHref).toMatch(/github\.com\/yetone\/mirdb/);
  });

  // Additional test: Navigation to Getting Started section works
  test('Navigation to Getting Started section works', async ({ page }) => {
    // Click on Getting Started nav link
    const navLink = page.locator('nav a[href="#getting-started"]');
    await expect(navLink).toBeVisible();
    await navLink.click();

    // Verify the Getting Started section is now in view
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeInViewport();
  });

  // Additional test: Getting Started section has proper structure
  test('Getting Started section has proper structure with installation and usage steps', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Check for section title
    const sectionTitle = gettingStartedSection.locator('h2');
    await expect(sectionTitle).toContainText('Getting Started');

    // Check for step headings (Installation, Start Server, Basic Operations, etc.)
    const stepHeadings = gettingStartedSection.locator('h3');
    const stepCount = await stepHeadings.count();
    expect(stepCount).toBeGreaterThanOrEqual(3);

    // Verify there's an Installation step
    const installationStep = gettingStartedSection.locator('h3', { hasText: /installation/i });
    await expect(installationStep).toBeVisible();
  });
});
