/**
 * Accessibility E2E Tests
 * Owner: Scenario 11 - Accessibility Compliance
 *
 * Test coverage:
 * - Semantic HTML (nav, main, section, footer)
 * - Image alt attributes
 * - Focus indicators
 * - ARIA landmarks
 * - Lang attribute
 * - Skip-to-content link
 */

import { test, expect } from '@playwright/test';

test.describe('Accessibility Compliance - WCAG 2.1 AA', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Navigation is wrapped in a nav element', async ({ page }) => {
    // Check for nav element in header
    const header = page.locator('header');
    await expect(header).toBeVisible();

    const nav = header.locator('nav');
    await expect(nav).toBeVisible();

    // Verify nav has proper ARIA label for screen readers
    const ariaLabel = await nav.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel?.toLowerCase()).toContain('navigation');
  });

  test('TC2: Page has a main element containing primary content', async ({ page }) => {
    // Check for main element
    const main = page.locator('main');
    await expect(main).toBeVisible();

    // Verify main has role="main" for backwards compatibility
    const role = await main.getAttribute('role');
    expect(role).toBe('main');

    // Verify main contains the primary content sections
    const heroSection = main.locator('#hero');
    const featuresSection = main.locator('#features');
    const quickStartSection = main.locator('#quick-start');

    await expect(heroSection).toBeVisible();
    await expect(featuresSection).toBeVisible();
    await expect(quickStartSection).toBeVisible();
  });

  test('TC3: Each section has an appropriate heading (h2, h3)', async ({ page }) => {
    // Check hero section has h1
    const h1 = page.locator('h1');
    await expect(h1).toHaveCount(1);
    await expect(h1).toBeVisible();

    // Check features section has h2
    const featuresHeading = page.locator('#features h2');
    await expect(featuresHeading).toBeVisible();
    await expect(featuresHeading).toContainText('Features');

    // Check features have h3 subheadings
    const featureH3s = page.locator('#features h3');
    const h3Count = await featureH3s.count();
    expect(h3Count).toBeGreaterThanOrEqual(3);

    // Check quick-start section has h2
    const quickStartHeading = page.locator('#quick-start h2');
    await expect(quickStartHeading).toBeVisible();

    // Check demo section has h2
    const demoHeading = page.locator('#demo h2');
    await expect(demoHeading).toBeVisible();

    // Check status section has h2
    const statusHeading = page.locator('#status h2');
    await expect(statusHeading).toBeVisible();
  });

  test('TC4: Every img element has an alt attribute', async ({ page }) => {
    // Get all images on the page
    const images = page.locator('img');
    const imageCount = await images.count();

    expect(imageCount).toBeGreaterThan(0);

    // Check each image has an alt attribute
    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i);
      const altText = await img.getAttribute('alt');
      expect(altText, `Image ${i + 1} should have alt attribute`).not.toBeNull();
      expect(altText!.length, `Image ${i + 1} alt text should not be empty`).toBeGreaterThan(0);
    }
  });

  test('TC5: Buttons and links show visible focus state when focused via keyboard', async ({ page }) => {
    // Test focus on navigation links
    const navLinks = page.locator('.nav-link');
    const linkCount = await navLinks.count();

    for (let i = 0; i < linkCount; i++) {
      const link = navLinks.nth(i);
      await link.focus();

      // Check that the element has focus styles (outline)
      const outline = await link.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return styles.outline || styles.outlineStyle;
      });

      // Verify outline is visible (not 'none')
      expect(outline).not.toBe('none');
    }

    // Test focus on CTA button
    const ctaButton = page.locator('.hero-cta');
    await ctaButton.focus();

    const ctaOutline = await ctaButton.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return styles.outlineStyle;
    });
    expect(ctaOutline).not.toBe('none');

    // Test focus on copy button
    const copyButton = page.locator('.code-copy-btn');
    await copyButton.focus();

    const copyOutline = await copyButton.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return styles.outlineStyle;
    });
    expect(copyOutline).not.toBe('none');
  });

  test('TC6: Page has skip-to-content link or proper ARIA landmarks', async ({ page }) => {
    // Check for skip-to-content link
    const skipLink = page.locator('.skip-link, a[href="#main-content"]');
    await expect(skipLink).toHaveCount(1);

    // Verify skip link text
    await expect(skipLink).toContainText(/skip/i);

    // Verify skip link points to main content
    const href = await skipLink.getAttribute('href');
    expect(href).toBe('#main-content');

    // Verify main content has the target id
    const mainContent = page.locator('#main-content');
    await expect(mainContent).toBeVisible();

    // Also check for proper ARIA landmarks
    const header = page.locator('header[role="banner"]');
    await expect(header).toHaveCount(1);

    const nav = page.locator('nav[role="navigation"]');
    await expect(nav).toHaveCount(1);

    const main = page.locator('main[role="main"]');
    await expect(main).toHaveCount(1);

    const footer = page.locator('footer[role="contentinfo"]');
    await expect(footer).toHaveCount(1);
  });

  test('TC7: HTML element has lang="en" or appropriate language code', async ({ page }) => {
    // Check html element has lang attribute
    const html = page.locator('html');
    const langAttr = await html.getAttribute('lang');

    expect(langAttr).not.toBeNull();
    expect(langAttr).toBe('en');
  });

  test('Skip link becomes visible on focus', async ({ page }) => {
    // The skip link should be hidden by default (off-screen)
    const skipLink = page.locator('.skip-link');

    // Focus the skip link using keyboard
    await page.keyboard.press('Tab');

    // Now it should be focused
    await expect(skipLink).toBeFocused();

    // Wait for transition to complete and check visibility
    // The skip link should be within viewport when focused
    await expect(skipLink).toBeInViewport({ timeout: 1000 });
  });

  test('Skip link navigates to main content when activated', async ({ page }) => {
    // Focus the skip link
    await page.keyboard.press('Tab');

    const skipLink = page.locator('.skip-link');
    await expect(skipLink).toBeFocused();

    // Activate the skip link
    await page.keyboard.press('Enter');

    // Verify the URL hash changed to #main-content
    const url = page.url();
    expect(url).toContain('#main-content');
  });

  test('Semantic structure: footer element exists', async ({ page }) => {
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();
    await expect(footer).toHaveAttribute('role', 'contentinfo');
  });

  test('All interactive elements are keyboard accessible', async ({ page }) => {
    // Tab through the page and count focusable elements
    let focusableCount = 0;
    let maxTabs = 50; // Prevent infinite loops

    await page.keyboard.press('Tab'); // Start tabbing

    while (maxTabs > 0) {
      const activeElement = await page.evaluate(() => {
        const el = document.activeElement;
        return el ? el.tagName : null;
      });

      if (activeElement === 'BODY' || !activeElement) {
        break;
      }

      focusableCount++;
      await page.keyboard.press('Tab');
      maxTabs--;
    }

    // Should have multiple focusable elements
    expect(focusableCount).toBeGreaterThan(5);
  });

  test('Screen reader only content exists for external links', async ({ page }) => {
    // Check for sr-only spans on external links
    const srOnlyElements = page.locator('.sr-only');
    const count = await srOnlyElements.count();

    expect(count).toBeGreaterThan(0);

    // Verify sr-only class properly hides content visually
    const firstSrOnly = srOnlyElements.first();
    const isVisuallyHidden = await firstSrOnly.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return (
        styles.position === 'absolute' &&
        styles.width === '1px' &&
        styles.height === '1px' &&
        styles.overflow === 'hidden'
      );
    });

    expect(isVisuallyHidden).toBe(true);
  });
});
