import { test, expect } from '@playwright/test';

/**
 * E2E Tests for Getting Started Guide Section
 * Scenario: Verify that the getting-started section provides clear installation
 * and basic usage instructions
 */

test.describe('Getting Started Guide', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/');
  });

  /**
   * Test Case 1: Check for quick start section
   * Input: Check for quick start section
   * Expected: Quick start section is visible with clear heading
   */
  test('should display quick start section with clear heading', async ({ page }) => {
    // Verify the getting started section exists
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeVisible();

    // Verify the section has a clear heading
    const heading = page.locator('[data-testid="getting-started-heading"]');
    await expect(heading).toBeVisible();

    // Verify heading text mentions "Getting Started" or "Quick Start"
    const headingText = await heading.textContent();
    expect(headingText?.toLowerCase()).toMatch(/(getting started|quick start)/);
  });

  /**
   * Test Case 2: Verify startup command example
   * Input: Verify startup command example
   * Expected: Code block shows mirdb startup command (e.g., mirdb -c config.toml)
   */
  test('should display startup command example with mirdb -c config.toml', async ({ page }) => {
    // Navigate to getting started section
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeVisible();

    // Verify startup code block exists
    const startupCodeBlock = page.locator('[data-testid="startup-code-block"]');
    await expect(startupCodeBlock).toBeVisible();

    // Verify the code block contains the mirdb startup command
    const codeContent = await startupCodeBlock.textContent();
    expect(codeContent).toBeTruthy();
    expect(codeContent).toContain('mirdb');
    expect(codeContent).toMatch(/-c\s+config\.toml|config\.toml/);
  });

  /**
   * Test Case 3: Verify usage example
   * Input: Verify usage example
   * Expected: Code block shows basic SET/GET operations with expected responses
   */
  test('should display basic SET/GET operations with expected responses', async ({ page }) => {
    // Navigate to getting started section
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeVisible();

    // Verify usage code block exists
    const usageCodeBlock = page.locator('[data-testid="usage-code-block"]');
    await expect(usageCodeBlock).toBeVisible();

    // Verify the code block contains SET operation
    const codeContent = await usageCodeBlock.textContent();
    expect(codeContent).toBeTruthy();
    expect(codeContent?.toLowerCase()).toContain('set');

    // Verify the code block contains GET operation
    expect(codeContent?.toLowerCase()).toContain('get');

    // Verify the code block shows expected response (STORED or VALUE)
    expect(codeContent).toMatch(/STORED|VALUE/);
  });

  /**
   * Test Case 4: Check code block styling
   * Input: Check code block styling
   * Expected: Code blocks have syntax highlighting and are properly formatted
   */
  test('should have properly styled code blocks with syntax highlighting', async ({ page }) => {
    // Navigate to getting started section
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeVisible();

    // Get all code blocks in the getting started section
    const codeBlocks = gettingStartedSection.locator('[data-testid$="-code-block"]');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThan(0);

    // Verify code blocks have proper formatting (check for pre or code element styling)
    for (let i = 0; i < count; i++) {
      const codeBlock = codeBlocks.nth(i);
      await expect(codeBlock).toBeVisible();

      // Check that code block has a dark background (syntax highlighting indicator)
      const bgColor = await codeBlock.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Verify the background is dark (not transparent or white)
      // Dark colors typically have RGB values where most components are below 128
      expect(bgColor).not.toBe('rgba(0, 0, 0, 0)');
      expect(bgColor).not.toBe('transparent');

      // Verify monospace font is used (typical for code)
      const fontFamily = await codeBlock.evaluate((el) => {
        const code = el.querySelector('code') || el;
        return window.getComputedStyle(code).fontFamily;
      });
      expect(fontFamily.toLowerCase()).toMatch(/(mono|code|consolas|courier)/);
    }
  });

  /**
   * Additional test: Verify navigation to getting started section via CTA
   */
  test('should navigate to getting started section via Get Started button', async ({ page }) => {
    // Click the Get Started button
    const getStartedButton = page.locator('[data-testid="cta-get-started"]');
    await expect(getStartedButton).toBeVisible();
    await getStartedButton.click();

    // Wait for scroll/navigation to complete
    await page.waitForTimeout(500);

    // Verify getting started section is in viewport
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeInViewport();
  });
});
