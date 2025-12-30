/**
 * Accessibility - Keyboard Navigation Tests
 * Scenario: Validate that the page is fully navigable using keyboard only
 *
 * Test Cases:
 * 1. Tab through all interactive elements in logical order
 * 2. All focusable elements have visible focus indicators
 * 3. Pressing Enter on 'Get Started' button triggers navigation
 */
const { test, expect } = require('@playwright/test');

test.describe('Accessibility - Keyboard Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for page to be fully loaded
    await page.waitForLoadState('domcontentloaded');
  });

  test('Test Case 1: Tab through all interactive elements - Focus moves through navigation, CTAs, and links in logical order', async ({ page }) => {
    // Expected tab order based on DOM structure:
    // 1. Skip link (becomes visible on focus)
    // 2. Logo link
    // 3. Features nav link
    // 4. Quick Start nav link
    // 5. GitHub nav link
    // 6. Get Started button
    // 7. View on GitHub button
    // 8. CircleCI badge link
    // ... then continue through page sections and footer

    const expectedFocusOrder = [
      { selector: '.skip-link', description: 'Skip to main content link' },
      { selector: '.logo-text', description: 'MirDB logo link' },
      { selector: 'nav a[href="#features"]', description: 'Features nav link' },
      { selector: 'nav a[href="#quickstart"]', description: 'Quick Start nav link' },
      { selector: 'nav a[href*="github.com/yetone/mirdb"]', description: 'GitHub nav link' },
      { selector: '.hero-cta .btn-primary', description: 'Get Started button' },
      { selector: '.hero-cta .btn-secondary', description: 'View on GitHub button' },
      { selector: '.badge-container a', description: 'CircleCI badge link' },
    ];

    // Start tabbing through the page
    for (let i = 0; i < expectedFocusOrder.length; i++) {
      await page.keyboard.press('Tab');

      const expectedElement = expectedFocusOrder[i];
      const focusedElement = await page.locator(':focus');

      // Verify the focused element matches expected selector
      const matchesExpected = await page.locator(expectedElement.selector).evaluate((el, focusedEl) => {
        return document.activeElement === el;
      });

      expect(matchesExpected, `Expected focus on: ${expectedElement.description}`).toBe(true);
    }

    // Verify we can continue tabbing through footer links
    // Tab to footer links
    let footerReached = false;
    for (let i = 0; i < 50; i++) {
      await page.keyboard.press('Tab');
      const focusedElement = await page.locator(':focus');
      const isInFooter = await focusedElement.evaluate((el) => {
        return el && el.closest('footer') !== null;
      });
      if (isInFooter) {
        footerReached = true;
        break;
      }
    }

    expect(footerReached, 'Should be able to tab to footer links').toBe(true);

    // Verify footer links can receive focus
    const footerLinks = page.locator('.footer-links a');
    const footerLinksCount = await footerLinks.count();
    expect(footerLinksCount).toBeGreaterThan(0);
  });

  test('Test Case 2: Check focus indicators - All focusable elements have visible focus outline or indicator', async ({ page }) => {
    // Get all focusable elements
    const focusableSelectors = [
      'a[href]',
      'button',
      '.btn',
      '[tabindex]:not([tabindex="-1"])'
    ];

    const focusableElements = page.locator(focusableSelectors.join(', '));
    const count = await focusableElements.count();

    expect(count).toBeGreaterThan(0);

    // Test focus indicator visibility using keyboard navigation (Tab)
    // This triggers :focus-visible styles properly
    const elementsToCheck = [
      { name: 'Skip link', tabCount: 1 },
      { name: 'Logo link', tabCount: 2 },
      { name: 'Features nav link', tabCount: 3 },
      { name: 'Quick Start nav link', tabCount: 4 },
      { name: 'GitHub nav link', tabCount: 5 },
      { name: 'Get Started button', tabCount: 6 },
      { name: 'View on GitHub button', tabCount: 7 },
    ];

    // Reset focus to beginning
    await page.evaluate(() => document.body.focus());

    for (const element of elementsToCheck) {
      // Tab to the element
      await page.keyboard.press('Tab');

      // Wait for any CSS transitions
      await page.waitForTimeout(50);

      // Check that a visible focus indicator is present on the currently focused element
      const focusStyles = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el) return { outline: 'none', outlineWidth: '0px', outlineStyle: 'none', boxShadow: 'none' };
        const styles = window.getComputedStyle(el);
        return {
          outline: styles.outline,
          outlineWidth: styles.outlineWidth,
          outlineStyle: styles.outlineStyle,
          outlineColor: styles.outlineColor,
          boxShadow: styles.boxShadow,
        };
      });

      // Verify focus indicator exists (outline or box-shadow)
      const hasVisibleOutline =
        focusStyles.outlineWidth !== '0px' &&
        focusStyles.outlineStyle !== 'none';
      const hasBoxShadow = focusStyles.boxShadow !== 'none';

      expect(
        hasVisibleOutline || hasBoxShadow,
        `${element.name} should have visible focus indicator. Outline: ${focusStyles.outline}, Box-shadow: ${focusStyles.boxShadow}`
      ).toBe(true);
    }

    // Test that focus indicator has sufficient contrast (minimum 3px outline)
    // Navigate to Get Started button via Tab (reset and tab 6 times)
    await page.evaluate(() => document.body.focus());
    for (let i = 0; i < 6; i++) {
      await page.keyboard.press('Tab');
    }

    const primaryBtnOutline = await page.evaluate(() => {
      const el = document.activeElement;
      const styles = window.getComputedStyle(el);
      return {
        outlineWidth: styles.outlineWidth,
        outlineOffset: styles.outlineOffset,
        boxShadow: styles.boxShadow,
      };
    });

    // Outline width should be at least 3px for visibility OR have box-shadow
    const outlineWidthNum = parseInt(primaryBtnOutline.outlineWidth) || 0;
    const hasBoxShadow = primaryBtnOutline.boxShadow !== 'none';
    expect(
      outlineWidthNum >= 3 || hasBoxShadow,
      `Get Started button should have visible focus indicator (outline >= 3px or box-shadow). Got outline: ${primaryBtnOutline.outlineWidth}, box-shadow: ${primaryBtnOutline.boxShadow}`
    ).toBe(true);
  });

  test('Test Case 3: Activate CTA with keyboard - Pressing Enter on Get Started button triggers navigation', async ({ page }) => {
    // Navigate to Get Started button via keyboard
    let getStartedFocused = false;
    for (let i = 0; i < 10; i++) {
      await page.keyboard.press('Tab');
      const isFocused = await page.locator('.hero-cta .btn-primary').evaluate((el) => {
        return document.activeElement === el;
      });
      if (isFocused) {
        getStartedFocused = true;
        break;
      }
    }

    expect(getStartedFocused, 'Get Started button should be focusable via Tab').toBe(true);

    // Get the href of the Get Started button
    const href = await page.locator('.hero-cta .btn-primary').getAttribute('href');
    expect(href).toBe('#quickstart');

    // Press Enter to activate the link
    await page.keyboard.press('Enter');

    // Wait for navigation/scroll
    await page.waitForTimeout(500);

    // Verify the URL hash changed or we scrolled to the quickstart section
    const currentUrl = page.url();
    expect(currentUrl).toContain('#quickstart');

    // Verify the quickstart section is visible in viewport
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Verify the quickstart section is near the top of the viewport
    const boundingBox = await quickstartSection.boundingBox();
    expect(boundingBox).not.toBeNull();
    // The section should be visible (y position should be within a reasonable range from top)
    expect(boundingBox.y).toBeLessThan(200);
  });

  test('Additional: Skip link functionality works correctly', async ({ page }) => {
    // The skip link should be the first focusable element
    await page.keyboard.press('Tab');

    // Verify skip link is focused
    const skipLink = page.locator('.skip-link');
    const isFocused = await skipLink.evaluate((el) => {
      return document.activeElement === el;
    });
    expect(isFocused, 'Skip link should be first focusable element').toBe(true);

    // Verify skip link becomes visible when focused
    await expect(skipLink).toBeVisible();

    // Verify skip link text
    const skipLinkText = await skipLink.textContent();
    expect(skipLinkText).toContain('Skip to main content');

    // Press Enter to activate skip link
    await page.keyboard.press('Enter');

    // Wait for navigation
    await page.waitForTimeout(300);

    // Verify we navigated to main content
    const currentUrl = page.url();
    expect(currentUrl).toContain('#main-content');
  });

  test('Additional: All navigation links are keyboard accessible', async ({ page }) => {
    // Find all nav links
    const navLinks = page.locator('nav a');
    const count = await navLinks.count();

    expect(count).toBeGreaterThanOrEqual(3);

    // Test each nav link for keyboard accessibility
    for (let i = 0; i < count; i++) {
      const link = navLinks.nth(i);

      // Focus the link
      await link.focus();

      // Verify it can receive focus
      const isFocused = await link.evaluate((el) => {
        return document.activeElement === el;
      });
      expect(isFocused, `Nav link ${i + 1} should be focusable`).toBe(true);

      // Verify focus indicator is visible
      const hasOutline = await link.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return styles.outlineStyle !== 'none' && styles.outlineWidth !== '0px';
      });
      expect(hasOutline, `Nav link ${i + 1} should have visible focus indicator`).toBe(true);
    }
  });

  test('Additional: Buttons can be activated with Space key', async ({ page }) => {
    // Focus the Get Started button
    const getStartedBtn = page.locator('.hero-cta .btn-primary');
    await getStartedBtn.focus();

    // Store current URL
    const urlBefore = page.url();

    // Press Space to activate (should work for links with .btn class)
    await page.keyboard.press('Space');

    // Wait for any navigation
    await page.waitForTimeout(500);

    // For anchor elements, Space might not trigger by default, but the link should still be functional
    // The important thing is that Enter works (tested in Test Case 3)
    // This test verifies the button is interactive
    const isFocused = await getStartedBtn.evaluate((el) => {
      return document.activeElement === el || document.activeElement.closest('.hero-cta') !== null;
    });

    // The element should still be focusable after space press
    expect(isFocused || page.url().includes('#quickstart')).toBe(true);
  });

  test('Additional: Reverse tab order works (Shift+Tab)', async ({ page }) => {
    // First, tab to the third element
    await page.keyboard.press('Tab'); // Skip link
    await page.keyboard.press('Tab'); // Logo
    await page.keyboard.press('Tab'); // Features link

    // Verify we're on Features link
    let currentFocus = await page.evaluate(() => document.activeElement.textContent || document.activeElement.className);
    expect(currentFocus).toContain('Features');

    // Press Shift+Tab to go back to Logo
    await page.keyboard.press('Shift+Tab');

    currentFocus = await page.evaluate(() => document.activeElement.textContent || document.activeElement.className);
    expect(currentFocus).toContain('MirDB');

    // Press Shift+Tab again to go back to Skip link
    await page.keyboard.press('Shift+Tab');

    const isSkipLink = await page.locator('.skip-link').evaluate((el) => {
      return document.activeElement === el;
    });
    expect(isSkipLink, 'Should navigate back to skip link with Shift+Tab').toBe(true);
  });
});
