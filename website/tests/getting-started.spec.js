// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for MirDB Homepage - Getting Started Section
 * Scenario: Getting Started Section (REQ-3 and REQ-8)
 *
 * These tests verify that the Getting Started section includes:
 * 1. Installation instructions
 * 2. Connection information (default port 12333)
 * 3. Configuration example (TOML format)
 */

test.describe('Getting Started Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should have a clearly labeled Getting Started section', async ({ page }) => {
    // Step 1: Navigate to Getting Started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Verify section heading
    const heading = gettingStartedSection.locator('h2');
    await expect(heading).toHaveText('Getting Started');
  });

  test('TC1: should display installation instructions', async ({ page }) => {
    // Test Case 1: Check for installation instructions
    // Expected: Installation command or download link is present

    const gettingStartedSection = page.locator('#getting-started');

    // Check for Installation heading
    const installationHeading = gettingStartedSection.locator('h3', { hasText: 'Installation' });
    await expect(installationHeading).toBeVisible();

    // Check for git clone command
    const codeBlocks = gettingStartedSection.locator('pre code');
    const allCode = await codeBlocks.allTextContents();
    const combinedCode = allCode.join('\n');

    // Verify installation commands are present
    expect(combinedCode).toContain('git clone');
    expect(combinedCode).toContain('cargo build');
  });

  test('TC2: should display connection information with default port 12333', async ({ page }) => {
    // Test Case 2: Check for connection information
    // Expected: Default port '12333' or address '0.0.0.0:12333' is displayed

    const gettingStartedSection = page.locator('#getting-started');

    // Check for "Connecting to MirDB" heading
    const connectingHeading = gettingStartedSection.locator('h3', { hasText: 'Connecting' });
    await expect(connectingHeading).toBeVisible();

    // Get all text content from Getting Started section
    const sectionText = await gettingStartedSection.textContent();

    // Verify port 12333 is mentioned
    expect(sectionText).toContain('12333');

    // Verify the full address is shown
    expect(sectionText).toContain('0.0.0.0:12333');

    // Verify telnet example is shown
    expect(sectionText).toContain('telnet localhost 12333');
  });

  test('TC3: should display TOML configuration example with key parameters', async ({ page }) => {
    // Test Case 3: Check for configuration example
    // Expected: TOML configuration snippet is shown with key parameters

    const gettingStartedSection = page.locator('#getting-started');

    // Check for Configuration heading
    const configHeading = gettingStartedSection.locator('h3', { hasText: 'Configuration' });
    await expect(configHeading).toBeVisible();

    // Get code blocks content
    const codeBlocks = gettingStartedSection.locator('pre code');
    const allCode = await codeBlocks.allTextContents();
    const combinedCode = allCode.join('\n');

    // Verify TOML configuration parameters are present
    expect(combinedCode).toContain('addr = "0.0.0.0:12333"');
    expect(combinedCode).toContain('work_dir');
    expect(combinedCode).toContain('mem_table_max_size');
    expect(combinedCode).toContain('sst_max_size');
    expect(combinedCode).toContain('block_size');
    expect(combinedCode).toContain('l0_compaction_trigger');
  });

  test('should have installation step before connection information', async ({ page }) => {
    // Verify proper ordering of Getting Started steps
    const gettingStartedSection = page.locator('#getting-started');
    const headings = await gettingStartedSection.locator('h3').allTextContents();

    const installationIndex = headings.findIndex(h => h.includes('Installation'));
    const connectionIndex = headings.findIndex(h => h.includes('Connecting'));
    const configIndex = headings.findIndex(h => h.includes('Configuration'));

    // Installation should come before Connection
    expect(installationIndex).toBeLessThan(connectionIndex);
    // Connection should come before Configuration (at end)
    expect(connectionIndex).toBeLessThan(configIndex);
  });

  test('should have navigation link to Getting Started section', async ({ page }) => {
    // Verify navigation works
    const navLink = page.locator('nav a[href="#getting-started"]');
    await expect(navLink).toBeVisible();

    // Click navigation link
    await navLink.click();

    // Verify section is in view
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeInViewport();
  });

  test('should display basic memcached commands examples', async ({ page }) => {
    // Verify command examples are shown (REQ-5)
    const gettingStartedSection = page.locator('#getting-started');
    const sectionText = await gettingStartedSection.textContent();

    // Check for SET, GET, DELETE command examples
    expect(sectionText).toContain('set mykey');
    expect(sectionText).toContain('get mykey');
    expect(sectionText).toContain('delete mykey');
    expect(sectionText).toContain('STORED');
    expect(sectionText).toContain('DELETED');
  });

  test('should have hero CTA button linking to Getting Started', async ({ page }) => {
    // Verify hero "Get Started" button
    const heroSection = page.locator('#hero');
    const ctaButton = heroSection.locator('a.btn-primary', { hasText: 'Get Started' });

    await expect(ctaButton).toBeVisible();
    await expect(ctaButton).toHaveAttribute('href', '#getting-started');

    // Click and verify navigation
    await ctaButton.click();
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeInViewport();
  });
});
