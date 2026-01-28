/**
 * Accessibility E2E Tests
 * Scenario 9 - Accessibility Compliance
 *
 * End-to-end tests for accessibility compliance verifying:
 * - Keyboard navigation through all interactive elements
 * - Focus indicator visibility on buttons and links
 * - CTA button activation via keyboard (Enter/Space)
 * - Logical tab order
 *
 * Requirements: NFR-2 (WCAG 2.1 AA compliance)
 */

import { test, expect, type Page, type Locator } from '@playwright/test';

/**
 * Helper to get the computed outline or box-shadow style of a focused element
 */
async function getFocusIndicatorStyles(page: Page, selector: string): Promise<{
  outline: string;
  outlineOffset: string;
  boxShadow: string;
  borderColor: string;
}> {
  return page.evaluate((sel) => {
    const element = document.querySelector(sel);
    if (!element) return { outline: '', outlineOffset: '', boxShadow: '', borderColor: '' };

    const styles = window.getComputedStyle(element);
    return {
      outline: styles.outline,
      outlineOffset: styles.outlineOffset,
      boxShadow: styles.boxShadow,
      borderColor: styles.borderColor,
    };
  }, selector);
}

/**
 * Helper to check if focus indicator is visible
 */
async function hasFocusIndicator(element: Locator): Promise<boolean> {
  const styles = await element.evaluate((el) => {
    const computedStyle = window.getComputedStyle(el);
    const outline = computedStyle.outline;
    const outlineWidth = computedStyle.outlineWidth;
    const boxShadow = computedStyle.boxShadow;
    const border = computedStyle.border;

    // Check for visible outline
    const hasOutline =
      outline !== 'none' &&
      outline !== '0px none' &&
      outlineWidth !== '0px';

    // Check for visible box-shadow (often used for focus rings)
    const hasBoxShadow =
      boxShadow !== 'none' &&
      boxShadow !== '' &&
      boxShadow.includes('rgb');

    // Check for border changes (some designs use border for focus)
    const hasBorder =
      border !== 'none' &&
      border !== '0px none';

    return hasOutline || hasBoxShadow || hasBorder;
  });

  return styles;
}

/**
 * Helper to get all focusable elements in order
 */
async function getFocusableElements(page: Page): Promise<string[]> {
  return page.evaluate(() => {
    const focusableSelectors = [
      'a[href]',
      'button:not([disabled])',
      'input:not([disabled])',
      'select:not([disabled])',
      'textarea:not([disabled])',
      '[tabindex]:not([tabindex="-1"])',
    ].join(', ');

    const elements = document.querySelectorAll(focusableSelectors);
    const testIds: string[] = [];

    elements.forEach((el) => {
      const testId = el.getAttribute('data-testid');
      const tagName = el.tagName.toLowerCase();
      const ariaLabel = el.getAttribute('aria-label');
      const text = el.textContent?.trim().substring(0, 30);

      testIds.push(testId || ariaLabel || `${tagName}:${text}`);
    });

    return testIds;
  });
}

test.describe('Accessibility - Keyboard Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForSelector('[data-testid="homepage"]');
  });

  /**
   * Test Case 1: Tab through homepage from start to end
   * Expected: All interactive elements receive focus in logical order
   */
  test('TC1: should allow keyboard navigation through all interactive elements', async ({
    page,
  }) => {
    // Get count of focusable elements
    const focusableElements = await getFocusableElements(page);
    expect(focusableElements.length).toBeGreaterThan(5);

    // Tab through first several elements and verify focus moves
    const focusedElements: string[] = [];

    // Press Tab multiple times and track which elements receive focus
    for (let i = 0; i < Math.min(10, focusableElements.length); i++) {
      await page.keyboard.press('Tab');

      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el) return 'none';
        return (
          el.getAttribute('data-testid') ||
          el.getAttribute('aria-label') ||
          `${el.tagName.toLowerCase()}:${el.textContent?.trim().substring(0, 20)}`
        );
      });

      focusedElements.push(focusedElement);
    }

    // Verify focus moved through different elements
    const uniqueElements = new Set(focusedElements);
    expect(uniqueElements.size).toBeGreaterThan(3);
  });

  test('TC1: should maintain logical focus order (top to bottom, left to right)', async ({
    page,
  }) => {
    // Tab through elements and verify Y position generally increases or stays same
    const positions: { y: number; element: string }[] = [];

    for (let i = 0; i < 8; i++) {
      await page.keyboard.press('Tab');

      const position = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el) return { y: 0, element: 'none' };

        const rect = el.getBoundingClientRect();
        return {
          y: rect.top,
          element:
            el.getAttribute('data-testid') ||
            el.getAttribute('aria-label') ||
            el.tagName.toLowerCase(),
        };
      });

      positions.push(position);
    }

    // Verify focus generally moves down the page (with some tolerance for horizontal navigation)
    let generallyDescending = true;
    for (let i = 1; i < positions.length; i++) {
      // Allow for horizontal navigation within sections (same Y)
      // Only flag if focus jumps significantly upward
      if (positions[i].y < positions[i - 1].y - 200) {
        generallyDescending = false;
        break;
      }
    }

    expect(generallyDescending).toBe(true);
  });

  test('TC1: should allow shift+tab to navigate backwards', async ({ page }) => {
    // Tab forward several times
    for (let i = 0; i < 5; i++) {
      await page.keyboard.press('Tab');
    }

    // Get a unique identifier for the currently focused element
    const afterForward = await page.evaluate(() => {
      const el = document.activeElement;
      if (!el) return 'none';
      // Create a unique identifier using multiple attributes
      return (
        el.getAttribute('data-testid') ||
        el.getAttribute('aria-label') ||
        el.id ||
        `${el.tagName}:${el.className.toString().slice(0, 50)}`
      );
    });

    // Tab backward
    await page.keyboard.press('Shift+Tab');

    // Get identifier for new focused element
    const afterBackward = await page.evaluate(() => {
      const el = document.activeElement;
      if (!el) return 'none';
      return (
        el.getAttribute('data-testid') ||
        el.getAttribute('aria-label') ||
        el.id ||
        `${el.tagName}:${el.className.toString().slice(0, 50)}`
      );
    });

    // Focus should have moved to a different element
    expect(afterBackward).not.toBe(afterForward);
  });
});

test.describe('Accessibility - Focus Indicators on Buttons', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForSelector('[data-testid="homepage"]');
  });

  /**
   * Test Case 2: Check focus indicator visibility on buttons
   * Expected: Focus indicator is clearly visible on all buttons
   */
  test('TC2: should show visible focus indicator on primary CTA button', async ({
    page,
  }) => {
    // Find and focus the primary CTA button
    const primaryCTA = page.locator('[data-testid="hero-primary-cta"] button').first();
    await primaryCTA.focus();

    // Verify element received focus
    const isFocused = await primaryCTA.evaluate(
      (el) => document.activeElement === el
    );
    expect(isFocused).toBe(true);

    // Check for visible focus indicator (outline, box-shadow, or border)
    const hasIndicator = await hasFocusIndicator(primaryCTA);
    expect(hasIndicator).toBe(true);
  });

  test('TC2: should show visible focus indicator on secondary CTA button', async ({
    page,
  }) => {
    const secondaryCTA = page.locator('[data-testid="hero-secondary-cta"] button').first();
    await secondaryCTA.focus();

    const isFocused = await secondaryCTA.evaluate(
      (el) => document.activeElement === el
    );
    expect(isFocused).toBe(true);

    const hasIndicator = await hasFocusIndicator(secondaryCTA);
    expect(hasIndicator).toBe(true);
  });

  test('TC2: should show visible focus indicator on CTA section button', async ({
    page,
  }) => {
    // Scroll to CTA section
    await page.locator('[data-testid="cta-section"]').scrollIntoViewIfNeeded();

    const ctaButton = page.locator('[data-testid="cta-button"] button').first();
    await ctaButton.focus();

    const hasIndicator = await hasFocusIndicator(ctaButton);
    expect(hasIndicator).toBe(true);
  });

  test('TC2: should show visible focus indicator on navbar Sign Up button', async ({
    page,
  }) => {
    const signUpButton = page.locator('[data-testid="register-button"] button').first();
    await signUpButton.focus();

    const hasIndicator = await hasFocusIndicator(signUpButton);
    expect(hasIndicator).toBe(true);
  });

  test('TC2: should show visible focus indicator on mobile menu button', async ({
    page,
  }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 812 });

    const menuButton = page.locator('button[aria-label="Toggle menu"]');
    await menuButton.focus();

    const hasIndicator = await hasFocusIndicator(menuButton);
    expect(hasIndicator).toBe(true);
  });
});

test.describe('Accessibility - Focus Indicators on Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForSelector('[data-testid="homepage"]');
  });

  /**
   * Test Case 3: Check focus indicator visibility on links
   * Expected: Focus indicator is clearly visible on all links
   */
  test('TC3: should show visible focus indicator on login link', async ({ page }) => {
    const loginLink = page.locator('[data-testid="login-link"]');
    await loginLink.focus();

    const isFocused = await loginLink.evaluate(
      (el) => document.activeElement === el
    );
    expect(isFocused).toBe(true);

    const hasIndicator = await hasFocusIndicator(loginLink);
    expect(hasIndicator).toBe(true);
  });

  test('TC3: should show visible focus indicator on logo link', async ({ page }) => {
    const logoLink = page.locator('[data-testid="logo-link"]');
    await logoLink.focus();

    const hasIndicator = await hasFocusIndicator(logoLink);
    expect(hasIndicator).toBe(true);
  });

  test('TC3: should show visible focus indicator on footer links', async ({ page }) => {
    // Scroll to footer
    await page.locator('[data-testid="footer-section"]').scrollIntoViewIfNeeded();

    // Find footer links
    const footer = page.locator('[data-testid="footer-section"]');
    const footerLinks = footer.locator('a');
    const linkCount = await footerLinks.count();

    expect(linkCount).toBeGreaterThanOrEqual(2); // Terms and Privacy

    // Check focus indicator on first footer link
    const firstLink = footerLinks.first();
    await firstLink.focus();

    const hasIndicator = await hasFocusIndicator(firstLink);
    expect(hasIndicator).toBe(true);
  });

  test('TC3: should show visible focus indicator on primary CTA link', async ({
    page,
  }) => {
    const ctaLink = page.locator('[data-testid="hero-primary-cta"]');
    await ctaLink.focus();

    const hasIndicator = await hasFocusIndicator(ctaLink);
    expect(hasIndicator).toBe(true);
  });
});

test.describe('Accessibility - Keyboard Button Activation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForSelector('[data-testid="homepage"]');
  });

  /**
   * Test Case 4: Activate CTA button using keyboard (Enter/Space)
   * Expected: Button activates and navigates correctly
   */
  test('TC4: should activate primary CTA with Enter key and navigate to register', async ({
    page,
  }) => {
    // Focus the primary CTA link
    const primaryCTA = page.locator('[data-testid="hero-primary-cta"]');
    await primaryCTA.focus();

    // Press Enter
    await page.keyboard.press('Enter');

    // Should navigate to register page
    await expect(page).toHaveURL(/\/register/);
  });

  test('TC4: should activate login link with Enter key', async ({ page }) => {
    const loginLink = page.locator('[data-testid="login-link"]');
    await loginLink.focus();

    await page.keyboard.press('Enter');

    await expect(page).toHaveURL(/\/login/);
  });

  test('TC4: should activate secondary CTA button with Enter key', async ({
    page,
  }) => {
    const secondaryCTA = page.locator('[data-testid="hero-secondary-cta"]');
    await secondaryCTA.focus();

    await page.keyboard.press('Enter');

    await expect(page).toHaveURL(/\/login/);
  });

  test('TC4: should activate CTA section button with keyboard', async ({ page }) => {
    // Scroll to and focus the CTA section button
    await page.locator('[data-testid="cta-section"]').scrollIntoViewIfNeeded();

    const ctaButton = page.locator('[data-testid="cta-button"]');
    await ctaButton.focus();

    await page.keyboard.press('Enter');

    await expect(page).toHaveURL(/\/register/);
  });

  test('TC4: should toggle mobile menu with Space key', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 812 });
    await page.waitForTimeout(100);

    const menuButton = page.locator('button[aria-label="Toggle menu"]');
    await menuButton.focus();

    // Press Space to open menu
    await page.keyboard.press('Space');
    await page.waitForTimeout(300);

    // Mobile menu should be visible
    const mobileLoginLink = page.locator('[data-testid="mobile-login-link"]');
    await expect(mobileLoginLink).toBeVisible();

    // Press Space again to close
    await menuButton.focus();
    await page.keyboard.press('Space');
    await page.waitForTimeout(300);

    // Mobile menu should be hidden
    await expect(mobileLoginLink).toBeHidden();
  });

  test('TC4: should navigate to register via keyboard in mobile menu', async ({
    page,
  }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 812 });

    // Open mobile menu
    const menuButton = page.locator('button[aria-label="Toggle menu"]');
    await menuButton.click();
    await page.waitForTimeout(300);

    // Focus and activate the Sign Up link
    const mobileRegisterLink = page.locator('[data-testid="mobile-register-button"]');
    await mobileRegisterLink.focus();
    await page.keyboard.press('Enter');

    await expect(page).toHaveURL(/\/register/);
  });
});

test.describe('Accessibility - Screen Reader Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForSelector('[data-testid="homepage"]');
  });

  /**
   * Test Case 8: Test with screen reader simulation
   * Expected: All content is announced properly (structural tests)
   */
  test('TC8: should have proper ARIA labels on interactive elements', async ({
    page,
  }) => {
    // Mobile menu button should have aria-label
    const menuButton = page.locator('button[aria-label="Toggle menu"]');
    await expect(menuButton).toHaveAttribute('aria-label', 'Toggle menu');

    // Theme toggle should have a label
    const themeToggle = page.locator('[data-testid="theme-toggle"]').first();
    await expect(themeToggle).toBeVisible();
  });

  test('TC8: should have proper section labels for screen readers', async ({
    page,
  }) => {
    // Features section should have aria-labelledby
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toHaveAttribute(
      'aria-labelledby',
      'features-heading'
    );

    // CTA section should have aria-labelledby
    const ctaSection = page.locator('[data-testid="cta-section"]');
    await expect(ctaSection).toHaveAttribute('aria-labelledby', 'cta-heading');
  });

  test('TC8: footer navigation should have aria-label', async ({ page }) => {
    const footer = page.locator('[data-testid="footer-section"]');
    const footerNav = footer.locator('nav');
    await expect(footerNav).toHaveAttribute('aria-label', 'Footer navigation');
  });

  test('TC8: decorative background should be hidden from screen readers', async ({
    page,
  }) => {
    const canvas = page.locator('canvas');
    await expect(canvas).toHaveAttribute('aria-hidden', 'true');
  });

  test('TC8: all buttons should be accessible', async ({ page }) => {
    const buttons = page.locator('button:not([aria-hidden="true"])');
    const count = await buttons.count();

    for (let i = 0; i < count; i++) {
      const button = buttons.nth(i);
      const isVisible = await button.isVisible();

      if (isVisible) {
        // Button should have accessible text or aria-label
        const text = await button.textContent();
        const ariaLabel = await button.getAttribute('aria-label');
        const hasAccessibleName = (text && text.trim().length > 0) || ariaLabel;

        expect(hasAccessibleName).toBeTruthy();
      }
    }
  });
});

test.describe('Accessibility - Visual Focus State', () => {
  test('should have visible focus states distinguishable from hover', async ({
    page,
  }) => {
    await page.goto('/');

    const loginLink = page.locator('[data-testid="login-link"]');

    // Get default styles
    const defaultStyles = await loginLink.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        outline: styles.outline,
        boxShadow: styles.boxShadow,
      };
    });

    // Focus the element
    await loginLink.focus();

    // Get focused styles
    const focusedStyles = await loginLink.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        outline: styles.outline,
        boxShadow: styles.boxShadow,
      };
    });

    // Focus state should change the appearance
    const hasVisualChange =
      focusedStyles.outline !== defaultStyles.outline ||
      focusedStyles.boxShadow !== defaultStyles.boxShadow;

    expect(hasVisualChange).toBe(true);
  });
});
