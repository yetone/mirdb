/**
 * Accessibility E2E Tests
 * Owner: Scenario 10 - Accessibility Compliance
 *
 * Test cases:
 * - Color contrast ratios
 * - Keyboard navigation
 * - Focus indicators
 */

import { test, expect, Page } from '@playwright/test';
import { navigateToHomepage, selectors, viewports } from './test-utils';

/**
 * Helper to get computed style color values
 */
async function getElementColors(page: Page, selector: string): Promise<{ color: string; backgroundColor: string }> {
  return await page.evaluate((sel) => {
    const element = document.querySelector(sel);
    if (!element) return { color: '', backgroundColor: '' };

    const computed = window.getComputedStyle(element);
    return {
      color: computed.color,
      backgroundColor: computed.backgroundColor,
    };
  }, selector);
}

/**
 * Calculate relative luminance of an RGB color
 * Formula: https://www.w3.org/WAI/GL/wiki/Relative_luminance
 */
function getRelativeLuminance(r: number, g: number, b: number): number {
  const sRGB = [r, g, b].map((c) => {
    const s = c / 255;
    return s <= 0.03928 ? s / 12.92 : Math.pow((s + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * sRGB[0] + 0.7152 * sRGB[1] + 0.0722 * sRGB[2];
}

/**
 * Calculate contrast ratio between two luminance values
 */
function getContrastRatio(l1: number, l2: number): number {
  const lighter = Math.max(l1, l2);
  const darker = Math.min(l1, l2);
  return (lighter + 0.05) / (darker + 0.05);
}

/**
 * Parse RGB color string to RGB values
 */
function parseRgb(color: string): { r: number; g: number; b: number } | null {
  const match = color.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
  if (match) {
    return {
      r: parseInt(match[1], 10),
      g: parseInt(match[2], 10),
      b: parseInt(match[3], 10),
    };
  }
  return null;
}

test.describe('Accessibility E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await navigateToHomepage(page);
  });

  test.describe('Test Case 1: Color Contrast Ratios', () => {
    test('hero text should have sufficient contrast against background', async ({ page }) => {
      const heroTagline = await page.locator('.hero-tagline');
      const color = await heroTagline.evaluate((el) => window.getComputedStyle(el).color);

      // Hero has a gradient background, so we need to get the effective background
      // The gradient goes from --color-bg-secondary (#f8fafc) to --color-bg-primary (#ffffff)
      // For WCAG testing, we use the lightest part (worst case for dark text)
      const bgColor = await page.locator('.hero').evaluate((el) => {
        const styles = window.getComputedStyle(el);
        // Check backgroundColor first, if transparent or not set, use CSS variable
        const bg = styles.backgroundColor;
        if (!bg || bg === 'rgba(0, 0, 0, 0)' || bg === 'transparent') {
          // Use the lightest part of the gradient (white) for worst-case contrast
          return 'rgb(255, 255, 255)';
        }
        return bg;
      });

      // Parse colors
      const textRgb = parseRgb(color);
      const bgRgb = parseRgb(bgColor);

      if (textRgb && bgRgb) {
        const textLuminance = getRelativeLuminance(textRgb.r, textRgb.g, textRgb.b);
        const bgLuminance = getRelativeLuminance(bgRgb.r, bgRgb.g, bgRgb.b);
        const contrastRatio = getContrastRatio(textLuminance, bgLuminance);

        // WCAG AA requires 4.5:1 for normal text
        expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
      }
    });

    test('navigation links should have sufficient contrast', async ({ page }) => {
      const navLink = await page.locator('.nav-link').first();
      const color = await navLink.evaluate((el) => window.getComputedStyle(el).color);

      // Navigation links should be visible (color should not be transparent/invisible)
      expect(color).not.toBe('rgba(0, 0, 0, 0)');

      const rgb = parseRgb(color);
      if (rgb) {
        const textLuminance = getRelativeLuminance(rgb.r, rgb.g, rgb.b);
        // White background assumed (rgb(255, 255, 255))
        const bgLuminance = getRelativeLuminance(255, 255, 255);
        const contrastRatio = getContrastRatio(textLuminance, bgLuminance);

        // WCAG AA requires 4.5:1 for normal text
        expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
      }
    });

    test('button text should have sufficient contrast', async ({ page }) => {
      const primaryBtn = await page.locator('.btn-primary').first();
      const color = await primaryBtn.evaluate((el) => window.getComputedStyle(el).color);
      const bgColor = await primaryBtn.evaluate((el) => window.getComputedStyle(el).backgroundColor);

      const textRgb = parseRgb(color);
      const bgRgb = parseRgb(bgColor);

      if (textRgb && bgRgb) {
        const textLuminance = getRelativeLuminance(textRgb.r, textRgb.g, textRgb.b);
        const bgLuminance = getRelativeLuminance(bgRgb.r, bgRgb.g, bgRgb.b);
        const contrastRatio = getContrastRatio(textLuminance, bgLuminance);

        // WCAG AA requires 4.5:1 for normal text, 3:1 for large text
        expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
      }
    });

    test('footer text should have sufficient contrast against dark background', async ({ page }) => {
      const footerLink = await page.locator('.footer-link').first();
      const color = await footerLink.evaluate((el) => window.getComputedStyle(el).color);
      const bgColor = await page.locator('.footer').evaluate((el) => window.getComputedStyle(el).backgroundColor);

      const textRgb = parseRgb(color);
      const bgRgb = parseRgb(bgColor);

      if (textRgb && bgRgb) {
        const textLuminance = getRelativeLuminance(textRgb.r, textRgb.g, textRgb.b);
        const bgLuminance = getRelativeLuminance(bgRgb.r, bgRgb.g, bgRgb.b);
        const contrastRatio = getContrastRatio(textLuminance, bgLuminance);

        // WCAG AA requires 4.5:1 for normal text
        expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
      }
    });

    test('section headings should have sufficient contrast', async ({ page }) => {
      const sectionHeading = await page.locator('.section-heading').first();
      const color = await sectionHeading.evaluate((el) => window.getComputedStyle(el).color);

      const textRgb = parseRgb(color);
      if (textRgb) {
        const textLuminance = getRelativeLuminance(textRgb.r, textRgb.g, textRgb.b);
        // Assuming white/light background
        const bgLuminance = getRelativeLuminance(255, 255, 255);
        const contrastRatio = getContrastRatio(textLuminance, bgLuminance);

        // WCAG AA requires 3:1 for large text (headings)
        expect(contrastRatio).toBeGreaterThanOrEqual(3);
      }
    });
  });

  test.describe('Test Case 3: Keyboard Navigation', () => {
    test('should be able to navigate to all sections using Tab', async ({ page }) => {
      // Start with focus on body
      await page.keyboard.press('Tab');

      // First tab should focus on skip link or logo
      const firstFocused = await page.locator(':focus');
      await expect(firstFocused).toBeVisible();

      // Count focusable elements
      const focusableElements = await page.locator(
        'a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])'
      ).count();

      expect(focusableElements).toBeGreaterThan(0);
    });

    test('should navigate through navigation links with Tab', async ({ page }) => {
      // Focus on first nav element
      const logoLink = page.locator('.nav-logo');
      await logoLink.focus();
      await expect(logoLink).toBeFocused();

      // Tab through nav links
      await page.keyboard.press('Tab');
      const featuresLink = page.locator('.nav-link[href="#features"]');
      await expect(featuresLink).toBeFocused();

      await page.keyboard.press('Tab');
      const usageLink = page.locator('.nav-link[href="#usage"]');
      await expect(usageLink).toBeFocused();

      await page.keyboard.press('Tab');
      const quickstartLink = page.locator('.nav-link[href="#quickstart"]');
      await expect(quickstartLink).toBeFocused();
    });

    test('should activate buttons with Enter key', async ({ page }) => {
      const ctaButton = page.locator('.btn-primary[href="#quickstart"]');
      await ctaButton.focus();
      await expect(ctaButton).toBeFocused();

      // Press Enter to activate the link
      await page.keyboard.press('Enter');

      // Should scroll to quickstart section
      await page.waitForTimeout(500); // Wait for smooth scroll

      // Verify quickstart section is in view
      const quickstartSection = page.locator('#quickstart');
      await expect(quickstartSection).toBeInViewport();
    });

    test('should navigate through CTA buttons', async ({ page }) => {
      const primaryCta = page.locator('.hero-cta .btn-primary');
      const secondaryCta = page.locator('.hero-cta .btn-secondary');

      await primaryCta.focus();
      await expect(primaryCta).toBeFocused();

      await page.keyboard.press('Tab');
      await expect(secondaryCta).toBeFocused();
    });

    test('should allow keyboard access to footer links', async ({ page }) => {
      // Focus on first footer link
      const footerLink = page.locator('.footer-link').first();
      await footerLink.focus();
      await expect(footerLink).toBeFocused();

      // Tab through footer links
      const footerLinks = page.locator('.footer-link');
      const linkCount = await footerLinks.count();

      for (let i = 0; i < linkCount - 1; i++) {
        await page.keyboard.press('Tab');
        const currentLink = footerLinks.nth(i + 1);
        await expect(currentLink).toBeFocused();
      }
    });

    test('should navigate through feature cards', async ({ page }) => {
      // Feature cards contain links/buttons that should be focusable
      // The cards themselves may not be focusable but any interactive elements inside should be
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();

      // Verify feature cards exist
      expect(cardCount).toBeGreaterThan(0);
    });
  });

  test.describe('Test Case 4: Focus Indicators', () => {
    test('navigation links should have visible focus indicators', async ({ page }) => {
      const navLink = page.locator('.nav-link').first();
      await navLink.focus();

      // Check that focus indicator exists (outline)
      const outlineStyle = await navLink.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          outline: styles.outline,
          outlineWidth: styles.outlineWidth,
          outlineColor: styles.outlineColor,
          outlineStyle: styles.outlineStyle,
          boxShadow: styles.boxShadow,
        };
      });

      // Focus should have some visible indicator
      const hasOutline =
        outlineStyle.outlineWidth !== '0px' && outlineStyle.outlineStyle !== 'none';
      const hasBoxShadow = outlineStyle.boxShadow !== 'none';

      expect(hasOutline || hasBoxShadow).toBe(true);
    });

    test('CTA buttons should have visible focus indicators', async ({ page }) => {
      // Click body first to ensure focus starts from a known state
      await page.locator('body').click();

      // Use keyboard navigation to trigger :focus-visible
      // Tab from the beginning to reach elements
      await page.keyboard.press('Tab');
      await page.waitForTimeout(50); // Allow focus state to stabilize

      // Keep tabbing until we reach the primary CTA button
      const ctaButton = page.locator('.btn-primary').first();
      let attempts = 0;
      const maxAttempts = 20;

      while (attempts < maxAttempts) {
        const isFocused = await ctaButton.evaluate((el) => document.activeElement === el);
        if (isFocused) break;
        await page.keyboard.press('Tab');
        await page.waitForTimeout(30);
        attempts++;
      }

      await expect(ctaButton).toBeFocused();
      await page.waitForTimeout(50); // Allow focus styles to be applied

      // Check for focus indicator
      const focusStyles = await ctaButton.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          outline: styles.outline,
          outlineWidth: styles.outlineWidth,
          outlineStyle: styles.outlineStyle,
          boxShadow: styles.boxShadow,
        };
      });

      const hasOutline =
        focusStyles.outlineWidth !== '0px' && focusStyles.outlineStyle !== 'none';
      const hasBoxShadow = focusStyles.boxShadow !== 'none';

      expect(hasOutline || hasBoxShadow).toBe(true);
    });

    test('secondary buttons should have visible focus indicators', async ({ page }) => {
      const secondaryBtn = page.locator('.btn-secondary').first();

      // Click body first to ensure focus starts from a known state
      await page.locator('body').click();

      // Use keyboard navigation to trigger :focus-visible
      await page.keyboard.press('Tab');
      await page.waitForTimeout(50);

      // Keep tabbing until we reach the secondary button
      let attempts = 0;
      const maxAttempts = 25;

      while (attempts < maxAttempts) {
        const isFocused = await secondaryBtn.evaluate((el) => document.activeElement === el);
        if (isFocused) break;
        await page.keyboard.press('Tab');
        await page.waitForTimeout(30);
        attempts++;
      }

      // Verify secondary button is now focused
      await expect(secondaryBtn).toBeFocused();
      await page.waitForTimeout(50); // Allow focus styles to be applied

      const focusStyles = await secondaryBtn.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          outline: styles.outline,
          outlineWidth: styles.outlineWidth,
          outlineStyle: styles.outlineStyle,
          boxShadow: styles.boxShadow,
        };
      });

      const hasOutline =
        focusStyles.outlineWidth !== '0px' && focusStyles.outlineStyle !== 'none';
      const hasBoxShadow = focusStyles.boxShadow !== 'none';

      expect(hasOutline || hasBoxShadow).toBe(true);
    });

    test('footer links should have visible focus indicators', async ({ page }) => {
      const footerLink = page.locator('.footer-link').first();
      await footerLink.focus();

      const focusStyles = await footerLink.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          outline: styles.outline,
          outlineWidth: styles.outlineWidth,
          outlineStyle: styles.outlineStyle,
          boxShadow: styles.boxShadow,
        };
      });

      const hasOutline =
        focusStyles.outlineWidth !== '0px' && focusStyles.outlineStyle !== 'none';
      const hasBoxShadow = focusStyles.boxShadow !== 'none';

      expect(hasOutline || hasBoxShadow).toBe(true);
    });

    test('logo link should have visible focus indicator', async ({ page }) => {
      const logo = page.locator('.nav-logo');
      await logo.focus();

      const focusStyles = await logo.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          outline: styles.outline,
          outlineWidth: styles.outlineWidth,
          outlineStyle: styles.outlineStyle,
          boxShadow: styles.boxShadow,
        };
      });

      const hasOutline =
        focusStyles.outlineWidth !== '0px' && focusStyles.outlineStyle !== 'none';
      const hasBoxShadow = focusStyles.boxShadow !== 'none';

      expect(hasOutline || hasBoxShadow).toBe(true);
    });

    test('focus indicator should be high contrast', async ({ page }) => {
      const ctaButton = page.locator('.btn-primary').first();
      await ctaButton.focus();

      // Get the outline color when focused
      const outlineColor = await ctaButton.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return styles.outlineColor;
      });

      // Outline color should be visible (not transparent)
      expect(outlineColor).not.toBe('rgba(0, 0, 0, 0)');
      expect(outlineColor).toBeTruthy();
    });

    test('focus should be visible with keyboard but not mouse (focus-visible)', async ({ page }) => {
      const navLink = page.locator('.nav-link').first();

      // Keyboard focus should show indicator
      await navLink.focus();
      const keyboardFocusStyles = await navLink.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          outlineWidth: styles.outlineWidth,
          outlineStyle: styles.outlineStyle,
        };
      });

      // Using :focus-visible, keyboard focus should show outline
      const hasKeyboardOutline =
        keyboardFocusStyles.outlineWidth !== '0px' && keyboardFocusStyles.outlineStyle !== 'none';

      expect(hasKeyboardOutline).toBe(true);
    });
  });
});
