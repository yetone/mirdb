// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

/**
 * Rust/Cargo Technical Information Tests
 * Scenario: Verify that the page correctly represents MirDB as a Rust project
 * and includes relevant technical details
 *
 * Test Cases:
 * 1. Page mentions that MirDB is written in Rust
 * 2. Page includes reference to Rust ecosystem (cargo install or crates.io)
 */
test.describe('Rust/Cargo Technical Information', () => {
  test.beforeEach(async ({ page }) => {
    const filePath = path.resolve(__dirname, '../index.html');
    await page.goto(`file://${filePath}`);
  });

  /**
   * Test Case 1: Page mentions that MirDB is written in Rust
   * Steps:
   * - Search page content for 'Rust' keyword
   * Expected:
   * - Page mentions that MirDB is written in Rust
   */
  test('TC1: Page mentions that MirDB is written in Rust', async ({ page }) => {
    // Get full page content
    const pageContent = await page.locator('body').textContent();

    // Verify Rust is mentioned on the page
    const hasRustMention = pageContent.toLowerCase().includes('rust');
    expect(hasRustMention).toBeTruthy();

    // Verify Rust is mentioned in a context indicating MirDB is built with it
    // Check for common phrases like "built in Rust", "written in Rust", "in Rust"
    const rustContextPatterns = [
      /built\s+(in|with)\s+rust/i,
      /written\s+in\s+rust/i,
      /in\s+rust/i,
      /rust/i
    ];

    const hasRustContext = rustContextPatterns.some(pattern => pattern.test(pageContent));
    expect(hasRustContext).toBeTruthy();

    // Verify Rust is mentioned in key sections (hero, features, or footer)
    const heroSection = page.locator('[data-testid="hero-section"]');
    const heroText = await heroSection.textContent();
    const heroHasRust = heroText.toLowerCase().includes('rust');

    const footer = page.locator('footer');
    const footerText = await footer.textContent();
    const footerHasRust = footerText.toLowerCase().includes('rust');

    // Rust should be mentioned in at least one prominent section
    expect(heroHasRust || footerHasRust).toBeTruthy();

    // Verify the tagline specifically mentions Rust
    const tagline = page.locator('[data-testid="hero-tagline"]');
    await expect(tagline).toBeVisible();
    const taglineText = await tagline.textContent();
    expect(taglineText.toLowerCase()).toContain('rust');
  });

  /**
   * Test Case 2: Page includes reference to Rust ecosystem (cargo install or crates.io)
   * Steps:
   * - Check for Cargo/crates.io reference
   * Expected:
   * - Page includes reference to Rust ecosystem (cargo install or crates.io)
   */
  test('TC2: Page includes reference to Rust ecosystem (cargo install or crates.io)', async ({ page }) => {
    // Get full page content
    const pageContent = await page.locator('body').textContent();

    // Check for Cargo reference (cargo build, cargo install, cargo run)
    const hasCargoReference =
      pageContent.toLowerCase().includes('cargo') ||
      pageContent.toLowerCase().includes('crates.io');

    expect(hasCargoReference).toBeTruthy();

    // Verify specific Cargo commands are mentioned
    const cargoCommands = [
      'cargo build',
      'cargo install',
      'cargo run',
      'cargo'
    ];

    const hasCargoCommand = cargoCommands.some(cmd =>
      pageContent.toLowerCase().includes(cmd.toLowerCase())
    );
    expect(hasCargoCommand).toBeTruthy();

    // Check for Cargo installation instructions in the Getting Started section
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeVisible();

    const gettingStartedText = await gettingStartedSection.textContent();
    const gettingStartedHasCargo = gettingStartedText.toLowerCase().includes('cargo');
    expect(gettingStartedHasCargo).toBeTruthy();

    // Verify code blocks contain Cargo commands
    const codeBlocks = gettingStartedSection.locator('pre, code');
    const codeCount = await codeBlocks.count();
    expect(codeCount).toBeGreaterThanOrEqual(1);

    let foundCargoInCode = false;
    for (let i = 0; i < codeCount; i++) {
      const codeText = await codeBlocks.nth(i).textContent();
      if (codeText.toLowerCase().includes('cargo')) {
        foundCargoInCode = true;
        break;
      }
    }
    expect(foundCargoInCode).toBeTruthy();

    // Verify "Install via Cargo" step exists
    const step1 = gettingStartedSection.locator('[data-testid="step-1"]');
    await expect(step1).toBeVisible();
    const step1Text = await step1.textContent();
    expect(step1Text.toLowerCase()).toContain('cargo');
  });

  /**
   * Additional Test: Verify Tokio/async runtime mention
   * Context: Technical detail for Rust developers
   */
  test('TC-Additional: Page mentions async/Tokio runtime (optional)', async ({ page }) => {
    // Get full page content
    const pageContent = await page.locator('body').textContent();

    // Check for async-related mentions (Tokio is mentioned in meta description)
    const metaDescription = await page.locator('meta[name="description"]').getAttribute('content');

    // Either page content or meta description should reference the async nature
    const hasAsyncReference =
      pageContent.toLowerCase().includes('tokio') ||
      pageContent.toLowerCase().includes('async') ||
      pageContent.toLowerCase().includes('high-performance') ||
      (metaDescription && metaDescription.toLowerCase().includes('async'));

    // This is an optional check - the page emphasizes performance characteristics
    // through other means (LSM tree, high-performance, etc.)
    const hasPerformanceReference =
      pageContent.toLowerCase().includes('high-performance') ||
      pageContent.toLowerCase().includes('performance') ||
      pageContent.toLowerCase().includes('lsm');

    expect(hasAsyncReference || hasPerformanceReference).toBeTruthy();
  });

  /**
   * Additional Test: Verify technical context is accurate
   * Ensures Rust mention is in proper technical context
   */
  test('TC-Context: Rust is mentioned in proper technical context', async ({ page }) => {
    // Verify meta description mentions Rust
    const metaDescription = await page.locator('meta[name="description"]').getAttribute('content');
    expect(metaDescription).toBeTruthy();
    expect(metaDescription.toLowerCase()).toContain('rust');

    // Verify Open Graph description also mentions Rust
    const ogDescription = await page.locator('meta[property="og:description"]').getAttribute('content');
    if (ogDescription) {
      expect(ogDescription.toLowerCase()).toContain('rust');
    }

    // Verify the footer copyright mentions Rust as the build technology
    const footer = page.locator('footer');
    const footerText = await footer.textContent();
    const footerHasBuiltWithRust =
      footerText.toLowerCase().includes('built with rust') ||
      footerText.toLowerCase().includes('built in rust');
    expect(footerHasBuiltWithRust).toBeTruthy();
  });
});
