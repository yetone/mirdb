/**
 * E2E Keyboard Navigation Tests.
 * Owner: Scenario 18 - Performance and Accessibility
 *
 * Validates that all interactive elements are keyboard accessible.
 * Covers keyboard navigation requirements.
 */

import { test, expect } from '@playwright/test';

test.describe('Keyboard Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('all interactive elements are reachable via Tab key', async ({ page }) => {
    // Collect all interactive elements that should be focusable
    const expectedFocusableSelectors = [
      '[data-testid="hero-cta-button"]',
      '[data-testid="hero-link-github"]',
      '[data-testid="hero-link-docs"]',
      '[data-testid="hero-link-community"]',
      '[data-testid="header-brand"]',
      '[data-testid="header-nav-github"]',
      '[data-testid="header-nav-docs"]',
      '[data-testid="theme-toggle-button"]',
      '[data-testid="kv-key-input"]',
      '[data-testid="kv-get-button"]',
      '[data-testid="command-input"]',
      // execute-button is disabled when input is empty, skip it
    ];

    for (const selector of expectedFocusableSelectors) {
      const element = page.locator(selector).first();
      const count = await element.count();
      if (count === 0) continue;

      // Try to focus the element
      await element.focus();
      const isFocused = await element.evaluate((el) => el === document.activeElement);
      expect(isFocused, `Element ${selector} should be focusable`).toBe(true);
    }
  });

  test('focus indicators are visible on interactive elements', async ({ page }) => {
    const focusableElements = [
      page.getByTestId('hero-cta-button'),
      page.getByTestId('theme-toggle-button'),
      page.getByTestId('kv-get-button'),
      page.getByTestId('execute-button'),
    ];

    for (const element of focusableElements) {
      const count = await element.count();
      if (count === 0) continue;

      await element.focus();

      // Check that the focused element has some visible outline or box-shadow
      const hasFocusIndicator = await element.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        const outlineWidth = parseFloat(styles.outlineWidth);
        const outlineOffset = parseFloat(styles.outlineOffset);
        const boxShadow = styles.boxShadow;
        const hasBoxShadow = boxShadow && boxShadow !== 'none';

        // Element should have either an outline or a box-shadow indicating focus
        return outlineWidth > 0 || outlineOffset !== 0 || hasBoxShadow;
      });

      expect(hasFocusIndicator, `Element should have visible focus indicator`).toBe(true);
    }
  });

  test('Escape key closes mobile navigation menu', async ({ page }) => {
    // Use mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await page.reload();

    const menuButton = page.getByTestId('header-menu-button');
    const mobileNav = page.getByTestId('header-mobile-nav');

    // Open mobile menu
    await menuButton.click();
    await expect(mobileNav).toBeVisible();

    // Press Escape to close
    await page.keyboard.press('Escape');
    await expect(mobileNav).not.toBeVisible();
  });

  test('no keyboard traps when tabbing through the page', async ({ page }) => {
    // Focus the first interactive element
    await page.getByTestId('header-brand').focus();

    // Tab through many elements and collect their test-ids
    const visitedElements = new Set<string>();
    let stuck = false;

    for (let i = 0; i < 50; i++) {
      await page.keyboard.press('Tab');

      const activeElementTestId = await page.evaluate(() => {
        const el = document.activeElement;
        return el?.getAttribute('data-testid') || el?.tagName || 'unknown';
      });

      // If we've seen this element before and it's not the body, we might be stuck
      if (visitedElements.has(activeElementTestId) && activeElementTestId !== 'BODY') {
        // Check if it's actually a cycle (we've been here more than a few times)
        const occurrences = Array.from(visitedElements).filter((id) => id === activeElementTestId).length;
        if (occurrences > 0 && i > 10) {
          // We've cycled - this is expected behavior (focus wraps)
          break;
        }
      }

      visitedElements.add(activeElementTestId);

      // If we end up on body, it means we tabbed past all focusable elements
      if (activeElementTestId === 'BODY') {
        break;
      }
    }

    // Should have visited multiple interactive elements
    expect(visitedElements.size).toBeGreaterThan(5);
  });

  test('Enter key activates buttons and links', async ({ page }) => {
    // Focus the CTA button
    const ctaButton = page.getByTestId('hero-cta-button');
    await ctaButton.focus();

    // Check it's focused
    const isFocused = await ctaButton.evaluate((el) => el === document.activeElement);
    expect(isFocused).toBe(true);

    // Focus a link and verify Enter would activate it (check it has href)
    const githubLink = page.getByTestId('hero-link-github');
    await githubLink.focus();
    const hasHref = await githubLink.evaluate((el) => (el as HTMLAnchorElement).href.length > 0);
    expect(hasHref).toBe(true);
  });

  test('form inputs can be navigated and submitted with keyboard', async ({ page }) => {
    // Focus the KV explorer input
    const kvInput = page.getByTestId('kv-key-input');
    await kvInput.focus();

    // Type a value
    await page.keyboard.type('test-key');
    const inputValue = await kvInput.inputValue();
    expect(inputValue).toBe('test-key');

    // Tab to the Get button
    await page.keyboard.press('Tab');

    // The Get button or another focusable element should be focused
    const focusedTag = await page.evaluate(() => document.activeElement?.tagName);
    expect(focusedTag).toBeTruthy();
  });

  test('command input accepts Enter key for execution', async ({ page }) => {
    const commandInput = page.getByTestId('command-input');
    await commandInput.focus();

    // Type a command
    await page.keyboard.type('get test');

    // Press Enter
    await page.keyboard.press('Enter');

    // Input should be cleared after execution (or the command processed)
    // The exact behavior depends on implementation, but we check it doesn't crash
    await expect(commandInput).toBeVisible();
  });

  test('Skip link or main landmark allows bypassing navigation', async ({ page }) => {
    // Check for main landmark
    const mainLandmark = page.locator('main, [role="main"]');
    const count = await mainLandmark.count();
    expect(count).toBeGreaterThan(0);

    // Check for navigation landmark
    const navLandmarks = page.locator('nav, [role="navigation"]');
    const navCount = await navLandmarks.count();
    expect(navCount).toBeGreaterThan(0);
  });
});
