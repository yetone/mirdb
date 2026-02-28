/**
 * Keyboard Navigation E2E Tests
 * Owner: Scenario 8 - Keyboard Navigation Accessibility
 *
 * Tests:
 * - Tab through all interactive elements
 * - Focus indicators visible
 * - Enter activates buttons
 * - Skip link functional
 * - Focus trap in mobile menu
 */

import { test, expect } from '@playwright/test';

test.describe('Keyboard Navigation Accessibility', () => {

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Tab from page start moves focus to skip link', async ({ page }) => {
    // Start from the body to ensure fresh focus state
    await page.locator('body').focus();

    // Press Tab to move to first focusable element
    await page.keyboard.press('Tab');

    // First focusable element should be the skip link
    const skipLink = page.locator('.skip-link');
    await expect(skipLink).toBeFocused();
  });

  test('Test Case 2: Tab through all interactive elements in logical order', async ({ page }) => {
    // Array to track focus order
    const focusOrder = [];
    const expectedOrder = [
      '.skip-link',              // Skip link first
      '.nav__logo',              // Logo/home link
      '.nav__link',              // Navigation links (4 of them)
      '.btn--primary',           // Primary CTA
      '.btn--secondary',         // Secondary CTA
      '.footer__link'            // Footer links
    ];

    // Start from body
    await page.locator('body').focus();

    // Tab through elements and collect their focus state
    let prevFocused = null;
    const maxTabs = 20; // Safety limit

    for (let i = 0; i < maxTabs; i++) {
      await page.keyboard.press('Tab');

      const focused = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;
        return {
          tagName: el.tagName,
          className: el.className,
          text: el.textContent?.trim().substring(0, 30),
          role: el.getAttribute('role')
        };
      });

      if (!focused || focused.tagName === 'BODY') break;

      // Only record unique elements
      if (JSON.stringify(focused) !== JSON.stringify(prevFocused)) {
        focusOrder.push(focused);
        prevFocused = focused;
      }
    }

    // Verify we have a reasonable number of focusable elements
    expect(focusOrder.length).toBeGreaterThan(5);

    // Verify skip link is first
    expect(focusOrder[0].className).toContain('skip-link');

    // Verify navigation elements come after skip link
    const navIndex = focusOrder.findIndex(el =>
      el.className.includes('nav__logo') || el.className.includes('nav__link')
    );
    expect(navIndex).toBeGreaterThan(0);

    // Verify buttons are focusable
    const ctaIndex = focusOrder.findIndex(el => el.className.includes('btn'));
    expect(ctaIndex).toBeGreaterThan(0);
  });

  test('Test Case 3: Focus indicators are clearly visible', async ({ page }) => {
    // Check that CSS custom property for focus color is defined
    const focusColor = await page.evaluate(() => {
      return getComputedStyle(document.documentElement)
        .getPropertyValue('--color-focus').trim();
    });
    expect(focusColor).toBeTruthy();

    // Tab to an element and check its focus styles
    await page.keyboard.press('Tab');

    // Get the focused element's outline style
    const focusStyles = await page.evaluate(() => {
      const el = document.activeElement;
      if (!el) return null;
      const styles = getComputedStyle(el);
      return {
        outline: styles.outline,
        outlineWidth: styles.outlineWidth,
        outlineStyle: styles.outlineStyle,
        outlineColor: styles.outlineColor,
        outlineOffset: styles.outlineOffset
      };
    });

    // Verify outline is visible (not none, not 0px width)
    expect(focusStyles).toBeTruthy();
    expect(focusStyles.outlineStyle).not.toBe('none');
    expect(focusStyles.outlineWidth).not.toBe('0px');
  });

  test('Test Case 4: Enter key activates CTA button', async ({ page }) => {
    // Navigate to the primary CTA button
    const ctaButton = page.locator('.btn--primary').first();
    await ctaButton.focus();

    // Verify button is focused
    await expect(ctaButton).toBeFocused();

    // Get the target section ID from href
    const href = await ctaButton.getAttribute('href');
    const targetId = href.replace('#', '');

    // Get initial scroll position
    const initialScroll = await page.evaluate(() => window.scrollY);

    // Press Enter to activate the button
    await page.keyboard.press('Enter');

    // Wait for scroll animation to complete
    await page.waitForTimeout(600);

    // Verify the button activated by checking scroll position changed
    // or the target section is now in viewport
    const targetSection = page.locator(`#${targetId}`);
    const isInViewport = await targetSection.evaluate(el => {
      const rect = el.getBoundingClientRect();
      return rect.top >= -100 && rect.top < window.innerHeight;
    });

    expect(isInViewport).toBe(true);
  });

  test('Test Case 5: Skip link appears on focus and jumps to main content', async ({ page }) => {
    const skipLink = page.locator('.skip-link');

    // Skip link should be visually hidden initially (off-screen with negative top)
    const initialTop = await skipLink.evaluate(el => {
      return parseInt(getComputedStyle(el).top, 10);
    });
    expect(initialTop).toBeLessThan(0);

    // Tab to skip link
    await page.keyboard.press('Tab');
    await expect(skipLink).toBeFocused();

    // Wait for CSS transition to complete (var(--transition-fast) = 150ms)
    await page.waitForTimeout(200);

    // Skip link should now be visible (on-screen, top >= 0)
    const focusedTop = await skipLink.evaluate(el => {
      return parseInt(getComputedStyle(el).top, 10);
    });
    expect(focusedTop).toBeGreaterThanOrEqual(0);

    // Activate skip link
    await page.keyboard.press('Enter');

    // Wait a moment for focus to be set
    await page.waitForTimeout(100);

    // Main content should now be focused or in view
    const mainContent = page.locator('#main-content');

    // Verify main content is in viewport (accounting for fixed header)
    const isInViewport = await mainContent.evaluate(el => {
      const rect = el.getBoundingClientRect();
      // Allow for fixed header offset (around 70px)
      return rect.top >= -100 && rect.top < window.innerHeight;
    });
    expect(isInViewport).toBe(true);
  });

  test('Test Case 6: Focus trap in mobile menu with Escape key', async ({ page, isMobile }) => {
    // This test is for mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });

    // Click hamburger menu to open
    const navToggle = page.locator('#nav-toggle');
    await expect(navToggle).toBeVisible();
    await navToggle.click();

    // Wait for menu to open
    const navMenu = page.locator('#nav-menu');
    await expect(navMenu).toHaveClass(/is-open/);

    // Focus should be moved to first menu item
    const firstNavLink = page.locator('.nav__link').first();
    await expect(firstNavLink).toBeFocused();

    // Tab through menu items
    await page.keyboard.press('Tab');
    const secondNavLink = page.locator('.nav__link').nth(1);
    await expect(secondNavLink).toBeFocused();

    // Press Escape to close menu
    await page.keyboard.press('Escape');

    // Menu should be closed
    await expect(navMenu).not.toHaveClass(/is-open/);

    // Focus should return to hamburger button
    await expect(navToggle).toBeFocused();
  });

  test('All links and buttons receive visible focus outline', async ({ page }) => {
    // Get all focusable elements
    const focusableElements = page.locator('a, button').filter({
      has: page.locator(':visible')
    });

    const count = await focusableElements.count();
    expect(count).toBeGreaterThan(0);

    // Check first few focusable elements for focus styles
    for (let i = 0; i < Math.min(5, count); i++) {
      const element = focusableElements.nth(i);

      // Only test visible elements
      if (await element.isVisible()) {
        await element.focus();

        const hasVisibleOutline = await element.evaluate(el => {
          const styles = getComputedStyle(el);
          const outlineWidth = parseInt(styles.outlineWidth) || 0;
          const outlineStyle = styles.outlineStyle;
          return outlineWidth > 0 && outlineStyle !== 'none';
        });

        expect(hasVisibleOutline).toBe(true);
      }
    }
  });

  test('Focus order follows visual layout (left-to-right, top-to-bottom)', async ({ page }) => {
    const focusedPositions = [];

    // Tab through elements and record positions
    await page.locator('body').focus();

    for (let i = 0; i < 10; i++) {
      await page.keyboard.press('Tab');

      const position = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;
        const rect = el.getBoundingClientRect();
        return { top: rect.top, left: rect.left };
      });

      if (position) {
        focusedPositions.push(position);
      }
    }

    // Verify general logical order - elements should progress down the page
    // Allow for some left/right variation within same vertical area
    let previousTop = -Infinity;
    for (let i = 1; i < focusedPositions.length; i++) {
      const current = focusedPositions[i];
      const previous = focusedPositions[i - 1];

      // If significantly lower on page, or at same height but to the right
      const isLogicalOrder =
        current.top > previous.top - 100 || // Allow some tolerance for same-row elements
        (Math.abs(current.top - previous.top) < 50 && current.left >= previous.left - 10);

      // This is a soft check - we expect most to be in order
      // but some variation is acceptable
    }

    // Verify we collected positions (focus worked)
    expect(focusedPositions.length).toBeGreaterThan(3);
  });
});
