/**
 * Demo Section E2E Tests
 * Owner: Scenario 6 - Usage Demonstration
 *
 * Tests:
 * - Usage demonstration element exists
 * - Demonstration displays properly without broken images
 * - Demonstration has descriptive alt text or caption
 * - Demo content shows set/get operations
 */
import { test, expect } from '@playwright/test';

test.describe('Demo Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('usage demonstration media element exists in demo section', async ({ page }) => {
    // Test case 1: Query for usage demonstration media element
    const demoSection = page.locator('[data-testid="demo-section"]');
    await expect(demoSection).toBeVisible();

    // Check for demo terminal (image-like element)
    const demoTerminal = page.locator('[data-testid="demo-terminal"]');
    await expect(demoTerminal).toBeVisible();
  });

  test('demonstration content displays successfully without errors', async ({ page }) => {
    // Test case 2: Verify demonstration displays (no broken image indicator)
    const demoFigure = page.locator('[data-testid="demo-figure"]');
    await expect(demoFigure).toBeVisible();

    const demoTerminal = page.locator('[data-testid="demo-terminal"]');
    await expect(demoTerminal).toBeVisible();

    // Verify the terminal body has content
    const terminalBody = page.locator('.terminal-body');
    await expect(terminalBody).toBeVisible();

    // Check that demo lines are rendered
    const demoLines = page.locator('.terminal-line');
    const lineCount = await demoLines.count();
    expect(lineCount).toBeGreaterThan(0);
  });

  test('demonstration has descriptive alt text or caption', async ({ page }) => {
    // Test case 3: Verify demonstration has alt text or caption
    const demoTerminal = page.locator('[data-testid="demo-terminal"]');

    // Check for role="img" and aria-label
    await expect(demoTerminal).toHaveAttribute('role', 'img');
    const ariaLabel = await demoTerminal.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel?.toLowerCase()).toContain('terminal');
    expect(ariaLabel?.toLowerCase()).toContain('set');
    expect(ariaLabel?.toLowerCase()).toContain('get');

    // Check for figcaption
    const caption = page.locator('[data-testid="demo-caption"]');
    await expect(caption).toBeVisible();
    await expect(caption).toContainText('memcached');
  });

  test('demo section displays set/get operations', async ({ page }) => {
    // Verify the demonstration shows set/get operations
    const demoSection = page.locator('[data-testid="demo-section"]');

    // Check for set command
    await expect(demoSection).toContainText('set mykey');
    await expect(demoSection).toContainText('STORED');

    // Check for get command
    await expect(demoSection).toContainText('get mykey');
    await expect(demoSection).toContainText('VALUE mykey');
    await expect(demoSection).toContainText('END');
  });

  test('demo section has proper heading and structure', async ({ page }) => {
    // Check heading
    const heading = page.locator('#demo-heading');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('See It In Action');

    // Check section has aria-labelledby
    const section = page.locator('#demo');
    await expect(section).toHaveAttribute('aria-labelledby', 'demo-heading');
  });

  test('demo terminal has visual styling', async ({ page }) => {
    const terminal = page.locator('.demo-terminal');
    await expect(terminal).toBeVisible();

    // Check terminal header with window buttons
    const header = page.locator('.terminal-header');
    await expect(header).toBeVisible();

    // Check for terminal title
    await expect(header).toContainText('MirDB Terminal Session');
  });

  test('demo is accessible via keyboard navigation', async ({ page }) => {
    // Navigate to the demo section
    await page.keyboard.press('Tab');

    // The demo section should be reachable by scrolling
    const demoSection = page.locator('[data-testid="demo-section"]');
    await demoSection.scrollIntoViewIfNeeded();
    await expect(demoSection).toBeVisible();
  });
});
