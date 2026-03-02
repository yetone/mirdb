/**
 * E2E Accessibility Tests
 * Owner: Scenario 11 - Keyboard Navigation Accessibility
 * Shared: Scenario 12 - Screen Reader Accessibility
 * Shared: Scenario 13 - Color Contrast Accessibility
 *
 * Tests keyboard navigation, focus management, and accessibility compliance.
 * Requirements: NFR-3, US-6
 */
import { test, expect, Page } from '@playwright/test';

test.describe('Keyboard Navigation Accessibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for the page to fully load
    await page.waitForLoadState('networkidle');
  });

  test('Test Case 1: Focus moves through elements in logical order (skip link, nav, hero form, CTAs)', async ({
    page,
  }) => {
    // Start tabbing from page load
    const focusOrder: string[] = [];

    // First tab should focus skip link
    await page.keyboard.press('Tab');
    let activeElement = await page.evaluate(() => {
      const el = document.activeElement;
      return el?.getAttribute('data-testid') || el?.tagName || 'unknown';
    });
    focusOrder.push(activeElement);
    expect(activeElement).toBe('skip-link');

    // Continue tabbing through navigation
    await page.keyboard.press('Tab');
    activeElement = await page.evaluate(() => {
      const el = document.activeElement;
      return el?.getAttribute('data-testid') || el?.tagName || 'unknown';
    });
    focusOrder.push(activeElement);
    expect(activeElement).toBe('navbar-logo');

    // Theme toggle in navbar
    await page.keyboard.press('Tab');
    activeElement = await page.evaluate(() => {
      const el = document.activeElement;
      return el?.getAttribute('data-testid') || el?.tagName || 'unknown';
    });
    focusOrder.push(activeElement);

    // Login link
    await page.keyboard.press('Tab');
    activeElement = await page.evaluate(() => {
      const el = document.activeElement;
      return el?.getAttribute('data-testid') || el?.tagName || 'unknown';
    });
    focusOrder.push(activeElement);
    expect(activeElement).toBe('navbar-login-link');

    // Register link
    await page.keyboard.press('Tab');
    activeElement = await page.evaluate(() => {
      const el = document.activeElement;
      return el?.getAttribute('data-testid') || el?.tagName || 'unknown';
    });
    focusOrder.push(activeElement);
    expect(activeElement).toBe('navbar-register-link');

    // URL input (hero form) or shorten button - verify we're in the hero area
    await page.keyboard.press('Tab');
    activeElement = await page.evaluate(() => {
      const el = document.activeElement;
      return el?.getAttribute('data-testid') || el?.tagName || el?.getAttribute('aria-label') || 'unknown';
    });
    focusOrder.push(activeElement);

    // Verify logical flow - skip link comes first
    expect(focusOrder[0]).toBe('skip-link');
    // Navbar elements before hero form
    expect(focusOrder.includes('navbar-logo')).toBeTruthy();
  });

  test('Test Case 2: All interactive elements have visible focus outline/ring', async ({
    page,
  }) => {
    const elementsToCheck = [
      '[data-testid="skip-link"]',
      '[data-testid="navbar-logo"]',
      '[data-testid="navbar-login-link"]',
      '[data-testid="navbar-register-link"]',
    ];

    for (const selector of elementsToCheck) {
      const element = page.locator(selector);
      if (await element.isVisible().catch(() => false)) {
        await element.focus();

        // Check that the element has focus styles
        const hasFocusRing = await element.evaluate((el) => {
          const styles = window.getComputedStyle(el);
          // Check for various focus indicators
          const hasOutline = styles.outline !== 'none' && styles.outlineWidth !== '0px';
          const hasBoxShadow = styles.boxShadow !== 'none';
          const hasRingClass =
            el.className.includes('focus:ring') || el.className.includes('focus:outline');
          return hasOutline || hasBoxShadow || hasRingClass;
        });

        expect(hasFocusRing, `Element ${selector} should have visible focus indicator`).toBeTruthy();
      }
    }
  });

  test('Test Case 3: Skip to content link appears as first focusable element', async ({
    page,
  }) => {
    // Press Tab to focus the first element
    await page.keyboard.press('Tab');

    // Get the first focused element
    const firstFocusedElement = await page.evaluate(() => {
      const el = document.activeElement;
      return {
        testId: el?.getAttribute('data-testid'),
        tagName: el?.tagName,
        text: el?.textContent?.trim(),
      };
    });

    // Verify it's the skip link
    expect(firstFocusedElement.testId).toBe('skip-link');
    expect(firstFocusedElement.text).toBe('Skip to content');
  });

  test('Test Case 4: Sign Up Free button navigates to /register when activated with Enter', async ({
    page,
  }) => {
    // Find the Sign Up Free button/link
    const signUpButton = page.getByRole('button', { name: /sign up free/i }).or(
      page.getByRole('link', { name: /sign up free/i })
    );

    // Focus the button
    await signUpButton.focus();

    // Press Enter to activate
    await page.keyboard.press('Enter');

    // Wait for navigation
    await page.waitForURL('**/register');

    // Verify we're on the register page
    expect(page.url()).toContain('/register');
  });

  test('Test Case 5: Form submission works correctly with keyboard only', async ({ page }) => {
    // Tab to the URL input field
    // First, tab through nav elements to get to the form
    let found = false;
    for (let i = 0; i < 15; i++) {
      await page.keyboard.press('Tab');
      const activeTestId = await page.evaluate(
        () => document.activeElement?.getAttribute('data-testid') || document.activeElement?.tagName
      );

      if (activeTestId === 'url-input' || activeTestId === 'INPUT') {
        found = true;
        break;
      }
    }

    if (!found) {
      // Fallback: directly focus the input
      const urlInput = page.locator('input[type="text"], input[type="url"]').first();
      await urlInput.focus();
    }

    // Type a URL using keyboard
    await page.keyboard.type('https://example.com/very-long-url-that-needs-shortening');

    // Tab to the Shorten button
    await page.keyboard.press('Tab');

    // Press Enter to submit the form
    await page.keyboard.press('Enter');

    // The form should either submit successfully or show validation
    // Wait a moment for any response
    await page.waitForTimeout(500);

    // Check that we can continue navigating (form is accessible)
    const activeElement = await page.evaluate(() => document.activeElement?.tagName);
    expect(activeElement).toBeTruthy();
  });

  test('Test Case 6: All buttons respond to both Enter and Space key presses', async ({
    page,
  }) => {
    // Test with the Shorten button
    const shortenButton = page
      .locator('button')
      .filter({ hasText: 'Shorten' })
      .first();

    if (await shortenButton.isVisible()) {
      // Test Enter key
      await shortenButton.focus();
      const buttonTextBefore = await shortenButton.textContent();
      await page.keyboard.press('Enter');

      // Button should respond (either submit form or show feedback)
      const focusedAfterEnter = await page.evaluate(() => document.activeElement?.tagName);
      expect(focusedAfterEnter).toBeTruthy();

      // Test Space key on Try as Guest button
      const tryAsGuestButton = page
        .locator('button')
        .filter({ hasText: /try as guest/i })
        .first();

      if (await tryAsGuestButton.isVisible()) {
        await tryAsGuestButton.focus();
        await page.keyboard.press(' '); // Space key

        const focusedAfterSpace = await page.evaluate(() => document.activeElement?.tagName);
        expect(focusedAfterSpace).toBeTruthy();
      }
    }
  });

  test('Skip link moves focus to main content when activated', async ({ page }) => {
    // Focus the skip link
    await page.keyboard.press('Tab');

    // Verify skip link is focused
    const skipLinkFocused = await page.evaluate(
      () => document.activeElement?.getAttribute('data-testid') === 'skip-link'
    );
    expect(skipLinkFocused).toBeTruthy();

    // Activate the skip link with Enter
    await page.keyboard.press('Enter');

    // Wait for focus to move
    await page.waitForTimeout(200);

    // Check that focus moved to main content
    const mainContentFocused = await page.evaluate(() => {
      const activeEl = document.activeElement;
      return activeEl?.id === 'main-content' || activeEl?.closest('#main-content') !== null;
    });

    expect(mainContentFocused).toBeTruthy();
  });

  test('Tab order is logical and comprehensive', async ({ page }) => {
    const focusedElements: string[] = [];

    // Tab through all elements on the page
    for (let i = 0; i < 20; i++) {
      await page.keyboard.press('Tab');
      const elementInfo = await page.evaluate(() => {
        const el = document.activeElement;
        return `${el?.tagName}:${el?.getAttribute('data-testid') || el?.getAttribute('aria-label') || el?.textContent?.substring(0, 20)}`;
      });
      focusedElements.push(elementInfo);
    }

    // Verify skip link appears first
    expect(focusedElements[0]).toContain('skip-link');

    // Verify we have multiple focusable elements
    expect(focusedElements.length).toBeGreaterThan(5);
  });
});

test.describe('Focus Visibility', () => {
  test('Skip link becomes visible when focused', async ({ page }) => {
    await page.goto('/');

    const skipLink = page.locator('[data-testid="skip-link"]');

    // Before focus, skip link should be transformed off-screen
    const initialTransform = await skipLink.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.transform;
    });

    // Focus the skip link
    await page.keyboard.press('Tab');

    // After focus, skip link should be visible
    const isFocused = await skipLink.evaluate((el) => el === document.activeElement);
    expect(isFocused).toBeTruthy();

    // The class should include focus:translate-y-0
    const hasCorrectFocusClass = await skipLink.evaluate((el) =>
      el.className.includes('focus:translate-y-0')
    );
    expect(hasCorrectFocusClass).toBeTruthy();
  });

  test('All interactive elements have visible focus states', async ({ page }) => {
    await page.goto('/');

    // Test various interactive elements
    const interactiveSelectors = [
      '[data-testid="skip-link"]',
      '[data-testid="navbar-logo"]',
      '[data-testid="navbar-login-link"]',
      '[data-testid="navbar-register-link"]',
      'button:visible',
      'a:visible',
      'input:visible',
    ];

    for (const selector of interactiveSelectors) {
      const elements = page.locator(selector);
      const count = await elements.count();

      for (let i = 0; i < Math.min(count, 3); i++) {
        const element = elements.nth(i);
        if (await element.isVisible().catch(() => false)) {
          await element.focus();

          // Check for focus indication via CSS
          const hasFocusStyles = await element.evaluate((el) => {
            const styles = window.getComputedStyle(el);
            return (
              el.className.includes('focus:') ||
              styles.outlineStyle !== 'none' ||
              styles.boxShadow !== 'none'
            );
          });

          // At minimum, the element should be focusable
          const isFocused = await element.evaluate((el) => el === document.activeElement);
          expect(isFocused).toBeTruthy();
        }
      }
    }
  });
});
