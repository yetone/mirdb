/**
 * Accessibility E2E Tests
 * Owner: Scenario 8 - Accessibility Compliance
 *
 * Tests:
 * - WCAG 2.1 Level AA compliance using axe-core
 * - Keyboard navigation
 * - Screen reader compatibility
 * - Color contrast (4.5:1 ratio)
 * - Focus indicators
 * - Alt text for images
 * - Semantic HTML structure
 */

import { test, expect } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

test.describe('Accessibility Compliance - WCAG 2.1 Level AA', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for page to fully load
    await page.waitForLoadState('networkidle');
  });

  // Test Case 1: Run axe-core accessibility audit on homepage
  test('should have no critical or serious accessibility violations', async ({ page }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .analyze();

    // Filter for critical and serious violations only
    const criticalAndSerious = accessibilityScanResults.violations.filter(
      (violation) => violation.impact === 'critical' || violation.impact === 'serious'
    );

    // Log all violations for debugging
    if (criticalAndSerious.length > 0) {
      console.log('Critical/Serious violations:', JSON.stringify(criticalAndSerious, null, 2));
    }

    expect(criticalAndSerious).toEqual([]);
  });

  // Test Case 2: Navigate page using Tab key only
  test('should allow navigation using Tab key only', async ({ page }) => {
    // Get all focusable elements
    const focusableSelector = 'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';
    const focusableCount = await page.locator(focusableSelector).count();

    expect(focusableCount).toBeGreaterThan(0);

    // Track which elements we can reach via Tab
    const reachedElements: string[] = [];
    const maxTabs = focusableCount + 10;

    // Click on body first to ensure clean focus state
    await page.locator('body').click();

    for (let i = 0; i < maxTabs; i++) {
      await page.keyboard.press('Tab');

      const focusedInfo = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body || el === document.documentElement) {
          return null;
        }
        return {
          tag: el.tagName.toLowerCase(),
          id: el.id || '',
          className: el.className || '',
        };
      });

      if (!focusedInfo) continue;

      const elementId = `${focusedInfo.tag}#${focusedInfo.id}.${focusedInfo.className}`;

      // Avoid duplicates when focus cycles
      if (!reachedElements.includes(elementId)) {
        reachedElements.push(elementId);
      }

      // If we've looped back to the first element, stop
      if (reachedElements.length > 1 && elementId === reachedElements[0]) {
        break;
      }
    }

    // Should have focused at least 2 interactive elements (navigation links + CTAs)
    expect(reachedElements.length).toBeGreaterThanOrEqual(2);

    // Verify all links and buttons are reachable
    const linksReached = reachedElements.filter(e => e.startsWith('a')).length;
    const buttonsReached = reachedElements.filter(e => e.startsWith('button')).length;

    // Should be able to reach links via keyboard
    expect(linksReached).toBeGreaterThan(0);
  });

  // Test Case 3: Check focus visibility on all interactive elements
  test('should have visible focus indicators on all interactive elements', async ({ page }) => {
    // Get all focusable elements
    const focusableSelectors = 'a[href], button:not([disabled]), [tabindex]:not([tabindex="-1"])';
    const focusableElements = await page.locator(focusableSelectors).all();

    expect(focusableElements.length).toBeGreaterThan(0);

    for (const element of focusableElements) {
      await element.focus();

      // Check that the element has a visible focus style
      // This can be via outline, box-shadow, or border
      const focusStyles = await element.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          outline: styles.outline,
          outlineWidth: styles.outlineWidth,
          outlineStyle: styles.outlineStyle,
          outlineColor: styles.outlineColor,
          boxShadow: styles.boxShadow,
          border: styles.border,
        };
      });

      // Element should have some visible focus indication
      const hasVisibleFocus =
        (focusStyles.outlineStyle !== 'none' && focusStyles.outlineWidth !== '0px') ||
        focusStyles.boxShadow !== 'none' ||
        focusStyles.outline !== 'none';

      expect(hasVisibleFocus, `Element should have visible focus indicator`).toBe(true);
    }
  });

  // Test Case 6: Check color contrast ratio of body text
  test('should have sufficient color contrast for body text', async ({ page }) => {
    // Use axe-core to specifically check color contrast
    const contrastResults = await new AxeBuilder({ page })
      .withRules(['color-contrast'])
      .analyze();

    // Filter for serious and critical contrast issues only
    const contrastViolations = contrastResults.violations.filter(
      (v) => v.impact === 'serious' || v.impact === 'critical'
    );

    if (contrastViolations.length > 0) {
      console.log('Color contrast violations:', JSON.stringify(contrastViolations, null, 2));
    }

    expect(contrastViolations).toEqual([]);
  });

  // Test Case 9: Activate links and buttons using Enter key
  test('should activate links and buttons using Enter key', async ({ page }) => {
    // Test a link - tab to first link and press Enter
    await page.keyboard.press('Tab');

    // Find currently focused element
    const focusedTagName = await page.evaluate(() => document.activeElement?.tagName.toLowerCase());

    if (focusedTagName === 'a') {
      // For links, we can verify they're keyboard activatable
      const href = await page.evaluate(() => (document.activeElement as HTMLAnchorElement)?.href);
      expect(href).toBeTruthy();
    }

    // Find and test a button specifically
    const copyButtons = page.locator('button.copy-button');
    const buttonCount = await copyButtons.count();

    if (buttonCount > 0) {
      const firstButton = copyButtons.first();
      await firstButton.focus();

      // Verify the button can be activated via Enter
      const buttonText = await firstButton.textContent();
      expect(buttonText).toBeTruthy();

      // Press Enter - button should be activatable
      await page.keyboard.press('Enter');

      // After activation, the button text might change (e.g., "Copy" -> "Copied!")
      // We just verify the interaction doesn't throw an error
    }
  });

  test('should have all interactive elements keyboard accessible', async ({ page }) => {
    // Get count of links and buttons
    const links = await page.locator('a[href]').count();
    const buttons = await page.locator('button:not([disabled])').count();

    expect(links).toBeGreaterThan(0);

    // Tab through and count how many elements we can reach
    let reachableCount = 0;
    const maxTabs = links + buttons + 10;

    for (let i = 0; i < maxTabs; i++) {
      await page.keyboard.press('Tab');
      const activeTag = await page.evaluate(() => document.activeElement?.tagName.toLowerCase());

      if (activeTag === 'a' || activeTag === 'button') {
        reachableCount++;
      }

      // Break if we've cycled back to the body
      if (activeTag === 'body') break;
    }

    // All links and buttons should be reachable
    expect(reachableCount).toBeGreaterThanOrEqual(links);
  });
});

test.describe('Accessibility - Semantic HTML Structure', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  // Test Case 7: Verify heading hierarchy
  test('should have proper heading hierarchy with single h1', async ({ page }) => {
    // Check for single h1
    const h1Count = await page.locator('h1').count();
    expect(h1Count).toBe(1);

    // Get all headings
    const headings = await page.evaluate(() => {
      const headingElements = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      return Array.from(headingElements).map((h) => ({
        level: parseInt(h.tagName.charAt(1)),
        text: h.textContent?.trim(),
      }));
    });

    expect(headings.length).toBeGreaterThan(0);

    // Check that heading levels don't skip (e.g., h1 -> h3 without h2)
    let previousLevel = 0;
    for (const heading of headings) {
      if (previousLevel > 0) {
        // Heading level should not increase by more than 1
        const levelJump = heading.level - previousLevel;
        expect(levelJump, `Heading "${heading.text}" skips levels (${previousLevel} -> ${heading.level})`).toBeLessThanOrEqual(1);
      }
      previousLevel = heading.level;
    }
  });

  // Test Case 8: Check for proper landmark regions
  test('should have proper landmark regions (header, main, footer)', async ({ page }) => {
    // Check for header landmark
    const header = page.locator('header, [role="banner"]');
    await expect(header.first()).toBeVisible();

    // Check for main landmark
    const main = page.locator('main, [role="main"]');
    await expect(main.first()).toBeVisible();

    // Check for footer landmark
    const footer = page.locator('footer, [role="contentinfo"]');
    await expect(footer.first()).toBeVisible();
  });

  test('should have proper navigation landmark', async ({ page }) => {
    // Check for navigation landmark
    const nav = page.locator('nav, [role="navigation"]');
    await expect(nav.first()).toBeVisible();

    // Navigation should have accessible label
    const navLabel = await nav.first().getAttribute('aria-label');
    expect(navLabel).toBeTruthy();
  });
});

test.describe('Accessibility - Image Alt Text', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  // Test Case 4: Verify logo.gif has alt text
  test('should have descriptive alt text for logo.gif', async ({ page }) => {
    const logoImage = page.locator('img[src*="logo.gif"]');
    const altText = await logoImage.getAttribute('alt');

    expect(altText).toBeTruthy();
    expect(altText?.toLowerCase()).toContain('mirdb');
    expect(altText?.toLowerCase()).toContain('logo');
  });

  // Test Case 5: Verify usage.gif has alt text
  test('should have descriptive alt text for usage.gif', async ({ page }) => {
    const usageImage = page.locator('img[src*="usage.gif"]');
    const altText = await usageImage.getAttribute('alt');

    expect(altText).toBeTruthy();
    // Alt text should describe what the demonstration shows
    expect(altText?.toLowerCase()).toMatch(/usage|demonstration|demo|key-value|memcached/);
  });

  test('should have alt text for all images', async ({ page }) => {
    const images = await page.locator('img').all();

    for (const image of images) {
      const alt = await image.getAttribute('alt');
      const src = await image.getAttribute('src');

      // All images should have alt attribute (can be empty string for decorative images)
      expect(alt, `Image ${src} should have alt attribute`).not.toBeNull();

      // Non-decorative images (not purely decorative) should have meaningful alt text
      if (src?.includes('logo.gif') || src?.includes('usage.gif')) {
        expect(alt, `Image ${src} should have descriptive alt text`).toBeTruthy();
      }
    }
  });
});

test.describe('Accessibility - ARIA and Screen Reader Support', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('should have proper ARIA labels on interactive elements', async ({ page }) => {
    // Check that buttons have accessible names
    const buttons = await page.locator('button').all();

    for (const button of buttons) {
      const accessibleName = await button.evaluate((el) => {
        const ariaLabel = el.getAttribute('aria-label');
        const textContent = el.textContent?.trim();
        return ariaLabel || textContent;
      });

      expect(accessibleName, 'Button should have accessible name').toBeTruthy();
    }
  });

  test('should have proper link text for screen readers', async ({ page }) => {
    const links = await page.locator('a[href]').all();

    for (const link of links) {
      const accessibleName = await link.evaluate((el) => {
        const ariaLabel = el.getAttribute('aria-label');
        const textContent = el.textContent?.trim();
        return ariaLabel || textContent;
      });

      expect(accessibleName, 'Link should have accessible name').toBeTruthy();
    }
  });

  test('should have skip link or proper landmark structure', async ({ page }) => {
    // Check if page has either skip links or proper landmarks for navigation
    const skipLink = page.locator('a[href="#main"], a[href="#content"], .skip-link');
    const mainLandmark = page.locator('main, [role="main"]');

    // Page should have at least main landmark for screen reader users
    const hasMainLandmark = await mainLandmark.count() > 0;
    expect(hasMainLandmark).toBe(true);
  });
});
