/**
 * E2E tests for Getting Started Section
 * Owner: Scenario 5
 *
 * Test cases:
 * - Section heading presence
 * - Installation code block
 * - Usage demonstration (usage.gif)
 * - Set/get command examples
 * - Copy-to-clipboard functionality
 * - Default configuration display
 * - Syntax highlighting
 */

const { test, expect } = require('@playwright/test');

test.describe('Getting Started Section', () => {
  test.beforeEach(async ({ page, baseURL }) => {
    await page.goto(baseURL);
  });

  test('should display Getting Started section with h2 heading', async ({ page }) => {
    // Test case 1: Check Getting Started section exists
    const section = page.locator('#getting-started');
    await expect(section).toBeVisible();

    const heading = section.locator('h2');
    await expect(heading).toBeVisible();

    const headingText = await heading.textContent();
    expect(headingText.toLowerCase()).toMatch(/getting started|quick start/);
  });

  test('should display installation code block with cargo build command', async ({ page }) => {
    // Test case 2: Verify installation code block
    const section = page.locator('#getting-started');
    const codeBlocks = section.locator('.code-block');

    // Should have at least one code block
    await expect(codeBlocks.first()).toBeVisible();

    // Check for cargo build or installation commands
    const sectionText = await section.textContent();
    expect(sectionText.toLowerCase()).toMatch(/cargo build|git clone/);
  });

  test('should display usage.gif demonstration', async ({ page }) => {
    // Test case 3: Verify usage demonstration reference
    const section = page.locator('#getting-started');
    const usageGif = section.locator('img[src*="usage.gif"]');

    await expect(usageGif).toBeVisible();

    // Verify alt text for accessibility
    const altText = await usageGif.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText.length).toBeGreaterThan(5);
  });

  test('should display set/get operation examples', async ({ page }) => {
    // Test case 4: Check set/get examples
    const section = page.locator('#getting-started');
    const sectionText = await section.textContent();

    // Verify set and get commands are present
    expect(sectionText).toContain('set');
    expect(sectionText).toContain('get');

    // Check for memcached-style command examples
    expect(sectionText.toLowerCase()).toMatch(/set.*mykey|set.*key/);
    expect(sectionText.toLowerCase()).toMatch(/get.*mykey|get.*key/);
  });

  test('should have copy button that copies code to clipboard', async ({ page, context }) => {
    // Test case 5: Click copy button on code block
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const section = page.locator('#getting-started');
    const firstCodeBlock = section.locator('.code-block').first();
    const copyButton = firstCodeBlock.locator('.copy-btn');

    await expect(copyButton).toBeVisible();

    // Get the code content before clicking
    const codeElement = firstCodeBlock.locator('code');
    const codeContent = await codeElement.textContent();

    // Click copy button
    await copyButton.click();

    // Wait for the copied state
    await expect(copyButton).toHaveClass(/copied/);

    // Verify visual feedback - check icon changes
    const checkIcon = copyButton.locator('.check-icon');
    await expect(checkIcon).not.toHaveClass(/hidden/);

    // Verify clipboard content
    const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardContent.trim()).toBe(codeContent.trim());
  });

  test('should display default configuration values', async ({ page }) => {
    // Test case 6: Verify default configuration display
    const section = page.locator('#getting-started');
    const sectionText = await section.textContent();

    // Check for default port 12333
    expect(sectionText).toContain('12333');

    // Check for work directory /tmp/mirdb
    expect(sectionText).toContain('/tmp/mirdb');
  });

  test('should have code blocks with syntax highlighting classes', async ({ page }) => {
    // Test case 7: Check code block syntax highlighting
    const section = page.locator('#getting-started');
    const codeBlocks = section.locator('.code-block');

    // Should have multiple code blocks
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThan(0);

    // Check first code block has language attribute
    const firstBlock = codeBlocks.first();
    const dataLang = await firstBlock.getAttribute('data-lang');
    expect(dataLang).toBeTruthy();
    expect(['shell', 'rust', 'text']).toContain(dataLang);

    // Verify code element has language class
    const codeElement = firstBlock.locator('code');
    const codeClass = await codeElement.getAttribute('class');
    expect(codeClass).toMatch(/language-/);
  });

  test('should have accessible copy buttons with aria-label', async ({ page }) => {
    // Additional accessibility test
    const section = page.locator('#getting-started');
    const copyButtons = section.locator('.copy-btn');

    const count = await copyButtons.count();
    expect(count).toBeGreaterThan(0);

    // Check each copy button has aria-label
    for (let i = 0; i < count; i++) {
      const button = copyButtons.nth(i);
      const ariaLabel = await button.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel.toLowerCase()).toMatch(/copy|clipboard/);
    }
  });

  test('should restore copy button state after timeout', async ({ page, context }) => {
    // Test copy button resets after showing copied state
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const section = page.locator('#getting-started');
    const firstCodeBlock = section.locator('.code-block').first();
    const copyButton = firstCodeBlock.locator('.copy-btn');

    // Click copy button
    await copyButton.click();

    // Verify copied state
    await expect(copyButton).toHaveClass(/copied/);

    // Wait for state to reset (2 seconds based on main.js)
    await page.waitForTimeout(2500);

    // Verify button returns to normal state
    await expect(copyButton).not.toHaveClass(/copied/);

    // Check aria-label is restored
    const ariaLabel = await copyButton.getAttribute('aria-label');
    expect(ariaLabel.toLowerCase()).toContain('copy');
  });

  test('should display configuration cards in responsive grid', async ({ page }) => {
    // Verify configuration display is in grid layout
    const section = page.locator('#getting-started');
    const configCards = section.locator('.config-card');

    const count = await configCards.count();
    expect(count).toBeGreaterThanOrEqual(4); // Should have at least 4 config values

    // Verify each card is visible
    for (let i = 0; i < Math.min(count, 4); i++) {
      await expect(configCards.nth(i)).toBeVisible();
    }
  });

  test('should navigate to Getting Started section from hero CTA', async ({ page }) => {
    // Verify scrolling to this section works
    const heroCtaButton = page.locator('#hero a[href="#getting-started"]');

    if (await heroCtaButton.isVisible()) {
      await heroCtaButton.click();
      await page.waitForTimeout(1000); // Wait for smooth scroll

      const section = page.locator('#getting-started');
      await expect(section).toBeInViewport();
    }
  });

  test('should have step numbers for installation guide', async ({ page }) => {
    // Verify numbered steps in getting started section
    const section = page.locator('#getting-started');
    const stepHeaders = section.locator('h3');

    // Should have at least 2 steps (Installation, Run, Usage)
    const count = await stepHeaders.count();
    expect(count).toBeGreaterThanOrEqual(2);
  });
});
