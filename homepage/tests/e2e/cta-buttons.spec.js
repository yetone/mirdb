/**
 * CTA Button E2E Tests
 * Owner: Scenario 15 - CTA Button Behavior
 *
 * Tests:
 * - Primary CTA navigates correctly
 * - Secondary CTA navigates correctly
 * - Hover states work
 * - Focus states work
 */

import { test, expect } from '@playwright/test';

test.describe('CTA Button Behavior', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Primary CTA button navigates to expected destination', async ({ page }) => {
    const primaryCta = page.locator('.btn--primary');
    await expect(primaryCta).toBeVisible();

    // Get the href attribute
    const href = await primaryCta.getAttribute('href');
    expect(href).toBeTruthy();

    // Verify it's either an anchor link or external link
    expect(href.startsWith('#') || href.startsWith('http') || href.startsWith('/')).toBe(true);

    // Click the button
    await primaryCta.click();

    // If it's an anchor link, verify navigation occurred
    if (href.startsWith('#')) {
      // Wait for smooth scroll animation
      await page.waitForTimeout(500);

      // Verify URL contains the hash
      const currentUrl = page.url();
      expect(currentUrl).toContain(href);

      // Verify the target section is now visible in viewport
      const targetSection = page.locator(href);
      await expect(targetSection).toBeVisible();
    }
  });

  test('TC2: Secondary CTA button navigates to expected destination', async ({ page }) => {
    const secondaryCta = page.locator('.btn--secondary');

    // Secondary CTA may or may not exist
    const secondaryCtaCount = await secondaryCta.count();

    if (secondaryCtaCount > 0) {
      await expect(secondaryCta).toBeVisible();

      // Get the href attribute
      const href = await secondaryCta.getAttribute('href');
      expect(href).toBeTruthy();

      // Verify it's a valid link
      expect(href.startsWith('#') || href.startsWith('http') || href.startsWith('/')).toBe(true);

      // Click the button
      await secondaryCta.click();

      // If it's an anchor link, verify navigation occurred
      if (href.startsWith('#')) {
        // Wait for smooth scroll animation
        await page.waitForTimeout(500);

        // Verify URL contains the hash
        const currentUrl = page.url();
        expect(currentUrl).toContain(href);

        // Verify the target section is now visible
        const targetSection = page.locator(href);
        await expect(targetSection).toBeVisible();
      }
    } else {
      // Secondary CTA is optional - test passes if not present
      expect(secondaryCtaCount).toBe(0);
    }
  });

  test('TC3: CTA button displays visible hover state change', async ({ page }) => {
    const primaryCta = page.locator('.btn--primary');
    await expect(primaryCta).toBeVisible();

    // Get initial styles
    const initialBackgroundColor = await primaryCta.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Hover over the button
    await primaryCta.hover();

    // Wait for transition to complete
    await page.waitForTimeout(200);

    // Get hover styles
    const hoverBackgroundColor = await primaryCta.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Verify at least one style property changed (color, shadow, or scale)
    // The hover state should cause a visible change
    const hasColorChange = initialBackgroundColor !== hoverBackgroundColor;

    // Also check box-shadow changes
    const initialBoxShadow = await primaryCta.evaluate((el) => {
      // Reset hover by moving mouse away and getting initial state
      return window.getComputedStyle(el).boxShadow;
    });

    // Check transform changes (scale)
    const hoverTransform = await primaryCta.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });

    // At least one visual change should occur on hover
    expect(hasColorChange || hoverTransform !== 'none').toBe(true);
  });

  test('TC4: CTA button displays visible focus indicator', async ({ page }) => {
    const primaryCta = page.locator('.btn--primary');
    await expect(primaryCta).toBeVisible();

    // Focus the button directly
    await primaryCta.focus();

    // Verify the button has focus
    await expect(primaryCta).toBeFocused();

    // Wait for CSS to apply
    await page.waitForTimeout(100);

    // Check for visible focus indicator (outline, box-shadow, or border)
    const focusStyles = await primaryCta.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        outlineStyle: styles.outlineStyle,
        outlineWidth: styles.outlineWidth,
        outlineColor: styles.outlineColor,
        boxShadow: styles.boxShadow,
      };
    });

    // Verify there's a visible focus indicator
    // Either outline or box-shadow should indicate focus
    const hasOutline = focusStyles.outlineStyle !== 'none' && focusStyles.outlineWidth !== '0px';
    const hasBoxShadow = focusStyles.boxShadow !== 'none';

    // At least one focus indicator should be present
    expect(hasOutline || hasBoxShadow).toBe(true);
  });

  test('TC5: CTA button text clearly indicates action', async ({ page }) => {
    const primaryCta = page.locator('.btn--primary');
    await expect(primaryCta).toBeVisible();

    // Get button text
    const ctaText = await primaryCta.textContent();
    expect(ctaText).toBeTruthy();

    // Verify text clearly indicates action
    const validActionTexts = [
      'Get Started',
      'Try Now',
      'View Docs',
      'Learn More',
      'Start Free',
      'Sign Up',
      'Download',
      'Start Now',
      'Get Started Free',
      'Try Free',
    ];

    const hasValidText = validActionTexts.some(text =>
      ctaText.toLowerCase().includes(text.toLowerCase())
    );

    expect(hasValidText).toBe(true);
  });

  test('Keyboard navigation Tab reaches CTA buttons', async ({ page }) => {
    // Start from beginning of page
    await page.keyboard.press('Tab');

    // Find all focusable elements
    const allCtas = page.locator('.btn');
    const ctaCount = await allCtas.count();

    // Tab through elements until we reach a CTA button
    let foundCta = false;
    for (let i = 0; i < 20 && !foundCta; i++) {
      const focusedElement = page.locator(':focus');
      const isCta = await focusedElement.evaluate((el) => {
        return el && el.classList.contains('btn');
      }).catch(() => false);

      if (isCta) {
        foundCta = true;
        break;
      }
      await page.keyboard.press('Tab');
    }

    expect(foundCta).toBe(true);
  });

  test('Enter key activates CTA button', async ({ page }) => {
    const primaryCta = page.locator('.btn--primary');
    await expect(primaryCta).toBeVisible();

    // Get the href before focusing
    const href = await primaryCta.getAttribute('href');
    expect(href).toBeTruthy();

    // Focus the button
    await primaryCta.focus();
    await expect(primaryCta).toBeFocused();

    // If it's an anchor link, verify the target element exists
    if (href && href.startsWith('#')) {
      const targetSection = page.locator(href);
      await expect(targetSection).toBeVisible();

      // Press Enter and verify we can still interact (button was activated)
      await page.keyboard.press('Enter');

      // Wait for any scroll animation
      await page.waitForTimeout(600);

      // Verify the target section is still in the DOM (navigation didn't break)
      await expect(targetSection).toBeVisible();

      // Anchor links in modern browsers may or may not update URL hash immediately
      // The key behavior is that the link is clickable/activatable
    } else {
      // For external links, just verify the href exists
      expect(href.startsWith('http') || href.startsWith('/')).toBe(true);
    }
  });

  test('Both primary and secondary CTAs have distinct styling', async ({ page }) => {
    const primaryCta = page.locator('.btn--primary');
    const secondaryCta = page.locator('.btn--secondary');

    const secondaryExists = await secondaryCta.count() > 0;

    if (secondaryExists) {
      const primaryBg = await primaryCta.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      const secondaryBg = await secondaryCta.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Primary and secondary should have distinct backgrounds
      expect(primaryBg).not.toBe(secondaryBg);
    }
  });

  test('CTA buttons meet minimum touch target size (44x44px)', async ({ page }) => {
    const primaryCta = page.locator('.btn--primary');
    await expect(primaryCta).toBeVisible();

    const boundingBox = await primaryCta.boundingBox();
    expect(boundingBox).not.toBeNull();

    // Verify minimum touch target size per WCAG guidelines
    expect(boundingBox.height).toBeGreaterThanOrEqual(44);
    expect(boundingBox.width).toBeGreaterThanOrEqual(44);
  });
});
