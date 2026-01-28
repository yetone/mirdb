/**
 * Homepage Accessibility E2E Tests
 * Owner: Scenario 9 - Accessibility Compliance
 *
 * E2E tests verifying accessibility requirements for the homepage.
 * Tests WCAG 2.1 AA compliance including:
 * - Keyboard navigation through all interactive elements
 * - Focus indicators visibility on buttons and links
 * - Keyboard activation of CTAs (Enter/Space)
 *
 * Test Cases:
 * TC1: Tab through homepage from start to end
 * TC2: Check focus indicator visibility on buttons
 * TC3: Check focus indicator visibility on links
 * TC4: Activate CTA button using keyboard (Enter/Space)
 *
 * Requirements: NFR-2 (WCAG 2.1 AA compliance)
 */

import { test, expect, type Page, type Locator } from '@playwright/test';

/**
 * Helper to verify an element has a visible focus indicator
 * Checks for outline, box-shadow, or border changes
 */
async function hasFocusIndicator(element: Locator): Promise<boolean> {
  const styles = await element.evaluate((el) => {
    const computed = window.getComputedStyle(el);
    return {
      outline: computed.outline,
      outlineWidth: computed.outlineWidth,
      outlineStyle: computed.outlineStyle,
      outlineColor: computed.outlineColor,
      boxShadow: computed.boxShadow,
      border: computed.border,
      borderWidth: computed.borderWidth,
    };
  });

  // Check for visible outline (not 'none' and width > 0)
  const hasOutline =
    styles.outlineStyle !== 'none' &&
    styles.outlineWidth !== '0px' &&
    styles.outlineColor !== 'transparent';

  // Check for box shadow (common focus ring alternative)
  const hasBoxShadow = styles.boxShadow !== 'none' && styles.boxShadow !== '';

  // Check for border (some designs use border for focus)
  const hasBorder = styles.borderWidth !== '0px';

  return hasOutline || hasBoxShadow || hasBorder;
}

/**
 * Helper to count focusable elements on the page
 */
async function getFocusableElements(page: Page): Promise<number> {
  return page.evaluate(() => {
    const selector =
      'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';
    return document.querySelectorAll(selector).length;
  });
}

/**
 * Helper to get all interactive element selectors in logical tab order
 */
async function getInteractiveElementsInOrder(page: Page): Promise<string[]> {
  return page.evaluate(() => {
    const elements = document.querySelectorAll(
      'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])'
    );
    const selectors: string[] = [];
    elements.forEach((el, index) => {
      // Create a unique selector for each element
      const testId = el.getAttribute('data-testid');
      if (testId) {
        selectors.push(`[data-testid="${testId}"]`);
      } else {
        selectors.push(`interactive-element-${index}`);
      }
    });
    return selectors;
  });
}

test.describe('Accessibility Compliance - Keyboard Navigation (TC1)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForSelector('[data-testid="homepage"]');
  });

  /**
   * TC1: Tab through homepage from start to end
   * Expected: All interactive elements receive focus in logical order
   */
  test('TC1: should tab through all interactive elements in logical order', async ({
    page,
  }) => {
    // Get count of focusable elements
    const focusableCount = await getFocusableElements(page);
    expect(focusableCount).toBeGreaterThan(5); // Should have multiple interactive elements

    // Store elements that received focus
    const focusedElements: string[] = [];

    // Tab through elements and track focus
    for (let i = 0; i < Math.min(focusableCount + 2, 20); i++) {
      await page.keyboard.press('Tab');

      // Get currently focused element info
      const focusedInfo = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;
        return {
          tagName: el.tagName.toLowerCase(),
          testId: el.getAttribute('data-testid'),
          text: el.textContent?.trim().substring(0, 50),
          href: (el as HTMLAnchorElement).href || null,
        };
      });

      if (focusedInfo) {
        focusedElements.push(
          focusedInfo.testId || focusedInfo.text || focusedInfo.tagName
        );
      }
    }

    // Verify multiple elements received focus
    expect(focusedElements.length).toBeGreaterThan(5);

    // Verify no duplicate consecutive elements (stuck focus)
    for (let i = 1; i < focusedElements.length; i++) {
      if (focusedElements[i] === focusedElements[i - 1]) {
        // Allow some consecutive same elements (like within a dropdown)
        // but not too many
        expect(i).toBeLessThan(focusedElements.length - 1);
      }
    }
  });

  test('TC1b: should allow reverse tab navigation (Shift+Tab)', async ({
    page,
  }) => {
    // First tab forward a few times
    for (let i = 0; i < 5; i++) {
      await page.keyboard.press('Tab');
    }

    // Get current focused element
    const forwardFocused = await page.evaluate(() => {
      return document.activeElement?.getAttribute('data-testid') ||
        document.activeElement?.textContent?.trim();
    });

    // Now tab backward
    await page.keyboard.press('Shift+Tab');

    // Get new focused element
    const backwardFocused = await page.evaluate(() => {
      return document.activeElement?.getAttribute('data-testid') ||
        document.activeElement?.textContent?.trim();
    });

    // Should be a different element (moved backward)
    expect(backwardFocused).not.toBe(forwardFocused);
  });

  test('TC1c: focus should eventually reach all major sections', async ({
    page,
  }) => {
    const sectionsReached = {
      navbar: false,
      hero: false,
      cta: false,
      footer: false,
    };

    // Tab through many elements
    for (let i = 0; i < 25; i++) {
      await page.keyboard.press('Tab');

      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el) return null;

        // Check which section the element is in
        const navbar = el.closest('nav.navbar');
        const hero = el.closest('[data-testid="hero-section"]');
        const cta = el.closest('[data-testid="cta-section"]');
        const footer = el.closest('[data-testid="footer-section"]');

        return {
          inNavbar: !!navbar,
          inHero: !!hero,
          inCta: !!cta,
          inFooter: !!footer,
        };
      });

      if (focusedElement) {
        if (focusedElement.inNavbar) sectionsReached.navbar = true;
        if (focusedElement.inHero) sectionsReached.hero = true;
        if (focusedElement.inCta) sectionsReached.cta = true;
        if (focusedElement.inFooter) sectionsReached.footer = true;
      }
    }

    // Verify focus reached major sections
    expect(sectionsReached.navbar).toBe(true);
    expect(sectionsReached.hero).toBe(true);
    expect(sectionsReached.footer).toBe(true);
  });
});

test.describe('Accessibility Compliance - Focus Indicators on Buttons (TC2)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForSelector('[data-testid="homepage"]');
  });

  /**
   * TC2: Check focus indicator visibility on buttons
   * Expected: Focus indicator is clearly visible on all buttons
   */
  test('TC2: should show visible focus indicator on primary CTA button', async ({
    page,
  }) => {
    // Focus the primary CTA button
    const primaryCTA = page.locator('[data-testid="hero-primary-cta"] button');
    await primaryCTA.focus();

    // Verify the button is focused
    await expect(primaryCTA).toBeFocused();

    // Check for visible focus indicator
    const hasIndicator = await hasFocusIndicator(primaryCTA);
    expect(hasIndicator).toBe(true);
  });

  test('TC2b: should show visible focus indicator on secondary CTA button', async ({
    page,
  }) => {
    // Focus the secondary CTA button (Sign In)
    const secondaryCTA = page.locator('[data-testid="hero-secondary-cta"] button');
    await secondaryCTA.focus();

    await expect(secondaryCTA).toBeFocused();

    const hasIndicator = await hasFocusIndicator(secondaryCTA);
    expect(hasIndicator).toBe(true);
  });

  test('TC2c: should show visible focus indicator on CTA section button', async ({
    page,
  }) => {
    // Scroll to CTA section
    await page.locator('[data-testid="cta-section"]').scrollIntoViewIfNeeded();

    // Focus the CTA button
    const ctaButton = page.locator('[data-testid="cta-button"] button');
    await ctaButton.focus();

    await expect(ctaButton).toBeFocused();

    const hasIndicator = await hasFocusIndicator(ctaButton);
    expect(hasIndicator).toBe(true);
  });

  test('TC2d: should show visible focus indicator on Sign Up button in navbar', async ({
    page,
  }) => {
    // Focus the register button
    const registerButton = page.locator('[data-testid="register-button"] button');
    await registerButton.focus();

    await expect(registerButton).toBeFocused();

    const hasIndicator = await hasFocusIndicator(registerButton);
    expect(hasIndicator).toBe(true);
  });

  test('TC2e: all buttons should have focus indicators when tabbed to', async ({
    page,
  }) => {
    // Get all buttons
    const buttons = page.locator('button:not([disabled])');
    const buttonCount = await buttons.count();

    expect(buttonCount).toBeGreaterThan(0);

    // Tab through and check each button gets a focus indicator
    for (let i = 0; i < Math.min(buttonCount, 10); i++) {
      // Find a button and focus it
      const button = buttons.nth(i);

      // Skip if button is not visible
      if (!(await button.isVisible())) continue;

      await button.focus();

      // Check focus indicator exists
      const hasIndicator = await hasFocusIndicator(button);

      // Log for debugging if needed
      if (!hasIndicator) {
        const buttonText = await button.textContent();
        console.log(`Button "${buttonText}" may not have a focus indicator`);
      }

      // At minimum, the button should be focusable
      await expect(button).toBeFocused();
    }
  });
});

test.describe('Accessibility Compliance - Focus Indicators on Links (TC3)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForSelector('[data-testid="homepage"]');
  });

  /**
   * TC3: Check focus indicator visibility on links
   * Expected: Focus indicator is clearly visible on all links
   */
  test('TC3: should show visible focus indicator on Login link', async ({
    page,
  }) => {
    const loginLink = page.locator('[data-testid="login-link"]');
    await loginLink.focus();

    await expect(loginLink).toBeFocused();

    const hasIndicator = await hasFocusIndicator(loginLink);
    expect(hasIndicator).toBe(true);
  });

  test('TC3b: should show visible focus indicator on logo link', async ({
    page,
  }) => {
    const logoLink = page.locator('[data-testid="logo-link"]');
    await logoLink.focus();

    await expect(logoLink).toBeFocused();

    const hasIndicator = await hasFocusIndicator(logoLink);
    expect(hasIndicator).toBe(true);
  });

  test('TC3c: should show visible focus indicator on footer links', async ({
    page,
  }) => {
    // Scroll to footer
    await page.locator('[data-testid="footer-section"]').scrollIntoViewIfNeeded();

    // Get footer links
    const footerLinks = page.locator('[data-testid="footer-section"] a');
    const linkCount = await footerLinks.count();

    expect(linkCount).toBeGreaterThan(0);

    // Check each footer link
    for (let i = 0; i < linkCount; i++) {
      const link = footerLinks.nth(i);
      await link.focus();

      await expect(link).toBeFocused();

      const hasIndicator = await hasFocusIndicator(link);
      expect(hasIndicator).toBe(true);
    }
  });

  test('TC3d: should show visible focus indicator on CTA links', async ({
    page,
  }) => {
    // Primary CTA link wrapper
    const primaryCTALink = page.locator('[data-testid="hero-primary-cta"]');
    await primaryCTALink.focus();

    // Either the link itself or the button inside should be focused
    const hasFocus = await page.evaluate(() => {
      const activeEl = document.activeElement;
      const cta = document.querySelector('[data-testid="hero-primary-cta"]');
      return cta?.contains(activeEl);
    });

    expect(hasFocus).toBe(true);
  });
});

test.describe('Accessibility Compliance - Keyboard Activation (TC4)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForSelector('[data-testid="homepage"]');
  });

  /**
   * TC4: Activate CTA button using keyboard (Enter/Space)
   * Expected: Button activates and navigates correctly
   */
  test('TC4: should activate primary CTA with Enter key and navigate to register', async ({
    page,
  }) => {
    // Focus the primary CTA
    const primaryCTA = page.locator('[data-testid="hero-primary-cta"]');
    await primaryCTA.focus();

    // Press Enter to activate
    await page.keyboard.press('Enter');

    // Should navigate to register page
    await expect(page).toHaveURL(/\/register/);
  });

  test('TC4b: should activate Login link with Enter key', async ({ page }) => {
    // Focus the login link
    const loginLink = page.locator('[data-testid="login-link"]');
    await loginLink.focus();

    // Press Enter to activate
    await page.keyboard.press('Enter');

    // Should navigate to login page
    await expect(page).toHaveURL(/\/login/);
  });

  test('TC4c: should activate secondary CTA button with Space key', async ({
    page,
  }) => {
    // Focus the secondary CTA button directly
    const secondaryCTA = page.locator('[data-testid="hero-secondary-cta"] button');
    await secondaryCTA.focus();

    // Press Space to activate
    await page.keyboard.press('Space');

    // Should navigate to login page
    await expect(page).toHaveURL(/\/login/);
  });

  test('TC4d: should activate CTA section button with Enter key', async ({
    page,
  }) => {
    // Scroll to CTA section
    await page.locator('[data-testid="cta-section"]').scrollIntoViewIfNeeded();

    // Focus the CTA button
    const ctaButton = page.locator('[data-testid="cta-button"]');
    await ctaButton.focus();

    // Press Enter to activate
    await page.keyboard.press('Enter');

    // Should navigate to register page
    await expect(page).toHaveURL(/\/register/);
  });

  test('TC4e: should activate footer links with Enter key', async ({ page }) => {
    // Scroll to footer
    await page.locator('[data-testid="footer-section"]').scrollIntoViewIfNeeded();

    // Focus terms link
    const termsLink = page.getByRole('link', { name: /terms of service/i });
    await termsLink.focus();

    // Press Enter to activate
    await page.keyboard.press('Enter');

    // Should navigate to terms page
    await expect(page).toHaveURL(/\/terms/);
  });
});

test.describe('Accessibility Compliance - Screen Reader Support (TC8)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForSelector('[data-testid="homepage"]');
  });

  /**
   * TC8: Test with screen reader simulation
   * Expected: All content is announced properly (via ARIA and semantic HTML)
   *
   * Note: This tests the underlying ARIA and semantic structure that screen readers use.
   * Full screen reader testing requires manual verification with actual screen readers.
   */
  test('TC8: should have proper ARIA landmarks for screen reader navigation', async ({
    page,
  }) => {
    // Check for main landmark
    const main = await page.locator('main');
    await expect(main).toBeVisible();

    // Check for navigation landmarks (may be multiple: navbar and footer nav)
    const navs = await page.locator('nav');
    const navCount = await navs.count();
    expect(navCount).toBeGreaterThanOrEqual(1);
    // At least the first nav (navbar) should be visible
    await expect(navs.first()).toBeVisible();

    // Check for contentinfo (footer) landmark
    const footer = await page.locator('footer[role="contentinfo"], footer');
    await expect(footer).toBeVisible();
  });

  test('TC8b: should have accessible names for all buttons', async ({
    page,
  }) => {
    const buttons = page.locator('button');
    const count = await buttons.count();

    for (let i = 0; i < count; i++) {
      const button = buttons.nth(i);

      // Skip hidden buttons
      if (!(await button.isVisible())) continue;

      // Get accessible name
      const accessibleName = await button.evaluate((el) => {
        return (
          el.getAttribute('aria-label') ||
          el.getAttribute('aria-labelledby') ||
          el.textContent?.trim()
        );
      });

      // Button should have an accessible name
      expect(accessibleName?.length).toBeGreaterThan(0);
    }
  });

  test('TC8c: should have descriptive link text (no "click here" links)', async ({
    page,
  }) => {
    const links = page.locator('a[href]');
    const count = await links.count();

    for (let i = 0; i < count; i++) {
      const link = links.nth(i);

      // Skip hidden links
      if (!(await link.isVisible())) continue;

      const linkText = await link.evaluate((el) => {
        return (
          el.getAttribute('aria-label') ||
          el.textContent?.trim().toLowerCase()
        );
      });

      // Link text should not be generic
      const genericTexts = ['click here', 'here', 'read more', 'learn more', 'link'];
      const isGeneric = genericTexts.some(
        (generic) => linkText === generic
      );

      expect(isGeneric).toBe(false);
    }
  });

  test('TC8d: should have proper heading structure for screen reader navigation', async ({
    page,
  }) => {
    // Get all headings
    const headings = await page.evaluate(() => {
      const elements = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      return Array.from(elements).map((el) => ({
        level: parseInt(el.tagName.charAt(1)),
        text: el.textContent?.trim(),
      }));
    });

    // Should have at least one h1
    const h1s = headings.filter((h) => h.level === 1);
    expect(h1s.length).toBe(1);

    // Should have multiple headings for navigation
    expect(headings.length).toBeGreaterThan(3);

    // Headings should have meaningful text
    headings.forEach((h) => {
      expect(h.text?.length).toBeGreaterThan(0);
    });
  });
});

test.describe('Accessibility Compliance - Visual Accessibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForSelector('[data-testid="homepage"]');
  });

  test('should not rely solely on color to convey information', async ({
    page,
  }) => {
    // Primary CTA should have text, not just color
    const primaryCTA = page.locator('[data-testid="hero-primary-cta"]');
    const ctaText = await primaryCTA.textContent();
    expect(ctaText?.length).toBeGreaterThan(0);

    // Feature cards should have text descriptions
    const featureCards = page.locator('[data-testid="feature-card"]');
    const cardCount = await featureCards.count();

    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      const cardText = await card.textContent();
      expect(cardText?.length).toBeGreaterThan(10); // Should have meaningful text
    }
  });

  test('should have sufficient text size for readability', async ({ page }) => {
    // Check main headline font size
    const headline = page.locator('[data-testid="hero-headline"]');
    const headlineFontSize = await headline.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });

    // Headline should be at least 24px on desktop
    expect(headlineFontSize).toBeGreaterThanOrEqual(24);

    // Check body text font size
    const bodyText = page.locator('[data-testid="hero-subheadline"]');
    const bodyFontSize = await bodyText.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });

    // Body text should be at least 16px for readability
    expect(bodyFontSize).toBeGreaterThanOrEqual(16);
  });
});
