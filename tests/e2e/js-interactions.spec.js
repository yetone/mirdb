/**
 * E2E tests for JavaScript Interactions.
 * Owner: Scenario 14 - JavaScript Interactions
 *
 * Tests:
 * - Active section highlighting on scroll
 * - Keyboard accessibility (Enter/Space on focused elements)
 * - No JavaScript errors on page load
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

const homepagePath = 'file://' + path.resolve(__dirname, '../../index.html');

test.describe('JavaScript Interactions', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(homepagePath);
    // Wait for JS to initialize
    await page.waitForTimeout(200);
  });

  // ============================================================
  // Test Case 7: Active section highlighting on scroll
  // ============================================================
  test('active nav link is highlighted when scrolling to each section', async ({ page }) => {
    const sections = ['#features', '#quick-start', '#status'];

    for (const sectionId of sections) {
      // Scroll to section
      await page.evaluate((id) => {
        const el = document.querySelector(id);
        if (el) el.scrollIntoView({ behavior: 'instant' });
      }, sectionId);

      // Wait for IntersectionObserver to fire
      await page.waitForTimeout(300);

      // Find the corresponding nav link
      const navLink = page.locator(`.nav-link[href="${sectionId}"]`).first();

      // The nav link should have the active class
      await expect(navLink).toHaveClass(/active/);
    }
  });

  // ============================================================
  // Test Case 8: Keyboard accessibility - Enter on theme toggle
  // ============================================================
  test('theme toggles when Enter is pressed on focused toggle button', async ({ page }) => {
    const toggleButton = page.locator('[data-testid="theme-toggle"]');
    const html = page.locator('html');

    // Get initial theme
    const initialTheme = await html.getAttribute('data-theme');

    // Focus the toggle button
    await toggleButton.focus();

    // Verify it's focused
    const isFocused = await toggleButton.evaluate(el => el === document.activeElement);
    expect(isFocused).toBe(true);

    // Press Enter
    await page.keyboard.press('Enter');
    await page.waitForTimeout(100);

    // Theme should have toggled
    const newTheme = await html.getAttribute('data-theme');
    expect(newTheme).not.toBe(initialTheme);

    // Press Enter again to toggle back
    await page.keyboard.press('Enter');
    await page.waitForTimeout(100);

    const restoredTheme = await html.getAttribute('data-theme');
    expect(restoredTheme).toBe(initialTheme);
  });

  test('theme toggles when Space is pressed on focused toggle button', async ({ page }) => {
    const toggleButton = page.locator('[data-testid="theme-toggle"]');
    const html = page.locator('html');

    const initialTheme = await html.getAttribute('data-theme');

    await toggleButton.focus();
    await page.keyboard.press('Space');
    await page.waitForTimeout(100);

    const newTheme = await html.getAttribute('data-theme');
    expect(newTheme).not.toBe(initialTheme);
  });

  test('copy button works with keyboard Enter', async ({ page, context }) => {
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const firstWrapper = page.locator('#quick-start .code-block-wrapper').first();
    const copyButton = firstWrapper.locator('button.copy-button');

    // Focus the copy button
    await copyButton.focus();

    // Press Enter
    await page.keyboard.press('Enter');
    await page.waitForTimeout(100);

    // Verify clipboard has content
    const clipboardText = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardText.length).toBeGreaterThan(0);

    // Verify visual feedback
    const label = copyButton.locator('.copy-label');
    await expect(label).toHaveText('Copied!');
  });

  test('mobile menu toggle works with keyboard Enter', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await page.waitForTimeout(200);

    const toggle = page.locator('.mobile-menu-toggle');
    const menu = page.locator('#nav-menu');

    // Menu should be hidden initially
    await expect(menu).not.toBeVisible();

    // Focus the toggle
    await toggle.focus();

    // Press Enter to open
    await page.keyboard.press('Enter');
    await page.waitForTimeout(100);

    // Menu should be visible
    await expect(menu).toBeVisible();
    await expect(toggle).toHaveAttribute('aria-expanded', 'true');

    // Press Enter again to close
    await page.keyboard.press('Enter');
    await page.waitForTimeout(100);

    await expect(menu).not.toBeVisible();
    await expect(toggle).toHaveAttribute('aria-expanded', 'false');
  });

  // ============================================================
  // Test Case 9: No JavaScript errors on page load
  // ============================================================
  test('no JavaScript errors on page load', async ({ page }) => {
    const errors = [];
    const warnings = [];

    page.on('pageerror', (error) => {
      errors.push(error.message);
    });

    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        errors.push(msg.text());
      }
      if (msg.type() === 'warning') {
        warnings.push(msg.text());
      }
    });

    // Reload the page to catch any errors during load
    await page.reload();
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(300);

    expect(errors).toHaveLength(0);
  });

  test('no JavaScript exceptions when interacting with all features', async ({ page, context }) => {
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const errors = [];
    page.on('pageerror', (error) => {
      errors.push(error.message);
    });

    // Click a nav link (smooth scroll)
    const featuresLink = page.locator('.nav-link[href="#features"]').first();
    await featuresLink.click();
    await page.waitForTimeout(500);

    // Click theme toggle
    const toggleButton = page.locator('[data-testid="theme-toggle"]');
    await toggleButton.click();
    await page.waitForTimeout(100);
    await toggleButton.click();
    await page.waitForTimeout(100);

    // Click copy button
    const copyButton = page.locator('#quick-start .copy-button').first();
    await copyButton.click();
    await page.waitForTimeout(100);

    // Set mobile viewport and test hamburger menu
    await page.setViewportSize({ width: 375, height: 667 });
    await page.waitForTimeout(200);

    const hamburger = page.locator('.mobile-menu-toggle');
    await hamburger.click();
    await page.waitForTimeout(100);
    await hamburger.click();
    await page.waitForTimeout(100);

    expect(errors).toHaveLength(0);
  });

  // ============================================================
  // Additional: Smooth scroll behavior
  // ============================================================
  test('clicking nav link scrolls smoothly and updates URL hash', async ({ page }) => {
    const featuresLink = page.locator('.nav-link[href="#features"]').first();
    await featuresLink.click();
    await page.waitForTimeout(500);

    // URL should contain the hash
    const url = page.url();
    expect(url).toContain('#features');

    // Features section should be in viewport
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  // ============================================================
  // Additional: Copy button feedback resets after timeout
  // ============================================================
  test('copy button label resets to "Copy" after feedback timeout', async ({ page, context }) => {
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const copyButton = page.locator('#quick-start .copy-button').first();
    const label = copyButton.locator('.copy-label');

    await copyButton.click();
    await expect(label).toHaveText('Copied!');

    // Wait for the 2-second timeout
    await page.waitForTimeout(2200);
    await expect(label).toHaveText('Copy');
    await expect(copyButton).not.toHaveClass(/copied/);
  });
});
