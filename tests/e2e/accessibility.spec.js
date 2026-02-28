/**
 * Accessibility E2E Tests
 * Owner: Scenarios 8, 9, 10, 18
 *
 * This file contains keyboard navigation tests for Scenario 8.
 * Other scenarios (9, 10, 18) will add their tests to this file.
 *
 * Test cases for Scenario 8 - Keyboard Navigation:
 * - Skip navigation link appears on first Tab
 * - All interactive elements receive visible focus
 * - Keyboard activation works on buttons and links
 * - Focus order matches visual order
 */

const { test, expect } = require('@playwright/test');
const { waitForPageLoad, VIEWPORTS } = require('./test-utils');

test.describe('Accessibility - Keyboard Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);
  });

  test('skip navigation link becomes visible on first Tab press', async ({ page }) => {
    // Skip link should be hidden initially (positioned off-screen with negative top)
    const skipLink = page.locator('.skip-link');

    // Check initial state - should be positioned off-screen (negative top value)
    const initialTop = await skipLink.evaluate((el) => {
      return parseInt(window.getComputedStyle(el).top, 10);
    });
    expect(initialTop).toBeLessThan(0);

    // Press Tab to focus the skip link
    await page.keyboard.press('Tab');

    // Wait for the skip link to become visible
    await expect(skipLink).toBeFocused();

    // Wait for CSS transition to complete (150ms defined in styles.css)
    await page.waitForTimeout(200);

    // Check that skip link is now visible (top should be positive or close to 0)
    const visibleTop = await skipLink.evaluate((el) => {
      return parseInt(window.getComputedStyle(el).top, 10);
    });
    expect(visibleTop).toBeGreaterThanOrEqual(0);

    // Verify the skip link text
    await expect(skipLink).toHaveText('Skip to main content');
  });

  test('each navigation link receives visible focus outline', async ({ page }) => {
    // Tab to skip link first
    await page.keyboard.press('Tab');

    // Tab to nav brand link
    await page.keyboard.press('Tab');
    const navBrandLink = page.locator('.nav-brand a');
    await expect(navBrandLink).toBeFocused();

    // Verify focus outline exists
    const brandOutline = await navBrandLink.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        outline: styles.outline,
        outlineColor: styles.outlineColor,
        outlineStyle: styles.outlineStyle,
        outlineWidth: styles.outlineWidth,
      };
    });
    expect(brandOutline.outlineStyle).not.toBe('none');

    // Tab through navigation menu links
    const navLinks = ['Features', 'Quick Start', 'Status', 'GitHub', 'Docs'];

    for (const linkText of navLinks) {
      await page.keyboard.press('Tab');
      const currentFocused = page.locator(':focus');
      const focusedText = await currentFocused.textContent();

      // Verify we're on a nav link (may include external icon)
      expect(focusedText).toContain(linkText.split(' ')[0]);

      // Verify focus outline is visible
      const focusOutline = await currentFocused.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return styles.outlineStyle;
      });
      expect(focusOutline).not.toBe('none');
    }
  });

  test('CTA button action is triggered via Enter key (scroll to section)', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Focus the CTA button directly
    const ctaButton = page.locator('.hero__cta');
    await ctaButton.focus();

    // Verify CTA button is focused
    await expect(ctaButton).toBeFocused();
    await expect(ctaButton).toHaveText('Get Started');

    // Press Enter to activate
    await page.keyboard.press('Enter');

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500);

    // Verify scroll occurred (page should scroll to quickstart section)
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    // Verify we scrolled to the quickstart section
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();
  });

  test('copy button action is triggered via keyboard', async ({ page }) => {
    // First, scroll to the quickstart section to ensure copy buttons are accessible
    await page.locator('#quickstart').scrollIntoViewIfNeeded();

    // Focus on the first copy button directly
    const firstCopyButton = page.locator('.copy-button').first();
    await firstCopyButton.focus();

    // Verify it's focused
    await expect(firstCopyButton).toBeFocused();

    // Get initial button text
    const initialText = await firstCopyButton.locator('.copy-text').textContent();
    expect(initialText).toBe('Copy');

    // Press Enter to trigger copy
    await page.keyboard.press('Enter');

    // Wait for feedback
    await page.waitForTimeout(100);

    // Verify copy feedback is shown
    const feedbackText = await firstCopyButton.locator('.copy-text').textContent();
    expect(feedbackText).toBe('Copied!');

    // Verify the button has the copied class
    await expect(firstCopyButton).toHaveClass(/copied/);
  });

  test('focus order matches visual order (logical tab sequence)', async ({ page }) => {
    const expectedFocusOrder = [
      { selector: '.skip-link', description: 'Skip link' },
      { selector: '.nav-brand a', description: 'Nav brand' },
      { selector: '.nav-menu a[href="#features"]', description: 'Features link' },
      { selector: '.nav-menu a[href="#quickstart"]', description: 'Quick Start link' },
      { selector: '.nav-menu a[href="#status"]', description: 'Status link' },
      { selector: '.nav-menu a[href="https://github.com/yetone/mirdb"]', description: 'GitHub link' },
      { selector: '.nav-menu a[href="https://github.com/memcached/memcached/wiki/Commands"]', description: 'Docs link' },
      { selector: '.hero__cta', description: 'CTA button' },
    ];

    for (let i = 0; i < expectedFocusOrder.length; i++) {
      await page.keyboard.press('Tab');
      const expected = expectedFocusOrder[i];
      const focusedElement = page.locator(expected.selector);

      // Verify the expected element is focused
      await expect(focusedElement, `${expected.description} should be focused at position ${i + 1}`).toBeFocused();
    }
  });

  test('all interactive elements can be activated via keyboard', async ({ page }) => {
    // Test navigation links with Enter key
    await page.keyboard.press('Tab'); // Skip link
    await page.keyboard.press('Tab'); // Nav brand
    await page.keyboard.press('Tab'); // Features link

    // Verify Features link is focused
    const featuresLink = page.locator('.nav-menu a[href="#features"]');
    await expect(featuresLink).toBeFocused();

    // Press Enter to navigate
    await page.keyboard.press('Enter');

    // Wait for smooth scroll
    await page.waitForTimeout(500);

    // Verify scroll to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  test('mobile hamburger menu is keyboard accessible', async ({ page }) => {
    // Set viewport to mobile size
    await page.setViewportSize(VIEWPORTS.mobile);

    // Reload page with new viewport
    await page.reload();
    await waitForPageLoad(page);

    // Tab to skip link, then nav brand, then hamburger toggle
    await page.keyboard.press('Tab'); // Skip link
    await page.keyboard.press('Tab'); // Nav brand
    await page.keyboard.press('Tab'); // Hamburger toggle

    const navToggle = page.locator('.nav-toggle');
    await expect(navToggle).toBeFocused();

    // Verify menu is closed
    await expect(navToggle).toHaveAttribute('aria-expanded', 'false');

    // Press Enter to open menu
    await page.keyboard.press('Enter');

    // Verify menu is open
    await expect(navToggle).toHaveAttribute('aria-expanded', 'true');

    // Press Escape to close menu
    await page.keyboard.press('Escape');

    // Verify menu is closed again
    await expect(navToggle).toHaveAttribute('aria-expanded', 'false');
  });

  test('focus is visible on all interactive states', async ({ page }) => {
    // Check focus visibility on multiple elements
    const interactiveElements = [
      '.skip-link',
      '.nav-brand a',
      '.nav-menu a',
      '.hero__cta',
      '.copy-button',
    ];

    for (const selector of interactiveElements) {
      const element = page.locator(selector).first();
      await element.focus();

      // Verify focus indicator is visible (outline or ring)
      const focusStyles = await element.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          outline: styles.outline,
          outlineStyle: styles.outlineStyle,
          outlineWidth: styles.outlineWidth,
          outlineColor: styles.outlineColor,
        };
      });

      // Focus should have some visible indicator (outline)
      const hasOutline = focusStyles.outlineStyle !== 'none' &&
                         focusStyles.outlineWidth !== '0px';

      expect(hasOutline, `${selector} should have visible focus outline`).toBe(true);
    }
  });

  test('skip link activates and moves focus to main content', async ({ page }) => {
    // Press Tab to focus skip link
    await page.keyboard.press('Tab');

    const skipLink = page.locator('.skip-link');
    await expect(skipLink).toBeFocused();

    // Press Enter to activate skip link
    await page.keyboard.press('Enter');

    // Wait for navigation
    await page.waitForTimeout(300);

    // Verify URL hash changed or main content is in view
    const url = page.url();
    expect(url).toContain('#main-content');

    // Main content should be in viewport
    const mainContent = page.locator('#main-content');
    await expect(mainContent).toBeInViewport();
  });
});
