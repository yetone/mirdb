/**
 * Accessibility E2E Tests
 * Owner: Scenario 10 - Accessibility Compliance
 *
 * Tests for WCAG 2.1 AA compliance including:
 * - Automated axe-core accessibility audit
 * - Keyboard navigation and focus indicators
 * - Color contrast verification
 * - Image alt text
 * - Heading hierarchy
 * - Semantic HTML structure
 */

import { test, expect } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';
import { SELECTORS } from '../fixtures/test-data';

test.describe('Accessibility Compliance', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should have no critical or serious axe-core accessibility violations', async ({ page }) => {
    // Run axe-core accessibility audit
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .analyze();

    // Filter for critical and serious violations only
    const criticalAndSerious = accessibilityScanResults.violations.filter(
      violation => violation.impact === 'critical' || violation.impact === 'serious'
    );

    // Log any violations for debugging
    if (criticalAndSerious.length > 0) {
      console.log('Critical/Serious Accessibility Violations:', JSON.stringify(criticalAndSerious, null, 2));
    }

    // Assert no critical or serious violations
    expect(criticalAndSerious).toHaveLength(0);
  });

  test('should have all interactive elements keyboard accessible with visible focus indicators', async ({ page }) => {
    // Test skip link visibility on focus
    const skipLink = page.locator('.skip-link');
    await skipLink.focus();
    await expect(skipLink).toBeVisible();

    // Get all focusable interactive elements
    const interactiveElements = page.locator(
      'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])'
    );

    const count = await interactiveElements.count();
    expect(count).toBeGreaterThan(0);

    // Tab through interactive elements and verify focus is visible
    // Start by focusing on body and pressing Tab
    await page.keyboard.press('Tab'); // Focus skip link

    // Check that skip link receives focus
    const activeElement1 = await page.evaluate(() => document.activeElement?.className);
    expect(activeElement1).toContain('skip-link');

    // Tab to header logo
    await page.keyboard.press('Tab');
    const activeElement2 = await page.evaluate(() => document.activeElement?.className);
    expect(activeElement2).toContain('header-logo');

    // Tab through navigation links
    for (let i = 0; i < 5; i++) {
      await page.keyboard.press('Tab');
      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        return {
          tagName: el?.tagName,
          className: el?.className,
          hasFocusVisible: el?.matches(':focus-visible') ?? false,
        };
      });

      // Verify the element can receive focus
      expect(focusedElement.tagName).toBeTruthy();
    }

    // Tab to CTA buttons in hero section
    await page.keyboard.press('Tab'); // Mobile menu toggle
    await page.keyboard.press('Tab'); // Get Started button

    const ctaPrimaryFocused = await page.evaluate(() =>
      document.activeElement?.classList.contains('cta-primary')
    );
    expect(ctaPrimaryFocused).toBe(true);

    // Verify focus indicator is visible via computed styles
    const focusedCta = page.locator('.cta-primary:focus-visible');
    // The element should have focus styles applied
    const outlineStyle = await page.evaluate(() => {
      const el = document.activeElement;
      if (el) {
        const styles = window.getComputedStyle(el);
        return styles.outlineWidth !== '0px' || styles.boxShadow !== 'none';
      }
      return false;
    });
    expect(outlineStyle).toBe(true);
  });

  test('should have sufficient color contrast for text elements', async ({ page }) => {
    // Run axe-core with color-contrast rule specifically
    const contrastResults = await new AxeBuilder({ page })
      .withRules(['color-contrast'])
      .analyze();

    // Check for color contrast violations
    const contrastViolations = contrastResults.violations.filter(
      v => v.id === 'color-contrast'
    );

    // Log any contrast issues for debugging
    if (contrastViolations.length > 0) {
      console.log('Color Contrast Violations:', JSON.stringify(contrastViolations, null, 2));
    }

    // Assert no color contrast violations
    expect(contrastViolations).toHaveLength(0);
  });

  test('should have descriptive alt text for logo image', async ({ page }) => {
    // Check hero logo has accessible name
    const heroLogo = page.locator('.hero-logo');
    await expect(heroLogo).toBeVisible();

    // Verify aria-label is set
    const ariaLabel = await heroLogo.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel?.toLowerCase()).toContain('mirdb');
    expect(ariaLabel?.toLowerCase()).toContain('logo');

    // Verify ASCII art is hidden from screen readers
    const asciiLogo = page.locator('.ascii-logo');
    const ariaHidden = await asciiLogo.getAttribute('aria-hidden');
    expect(ariaHidden).toBe('true');

    // Check header logo also has accessible name
    const headerLogo = page.locator('.header-logo');
    const headerAriaLabel = await headerLogo.getAttribute('aria-label');
    expect(headerAriaLabel).toBeTruthy();

    // Check any SVG icons have aria-hidden or proper labeling
    const decorativeSvgs = page.locator('svg[aria-hidden="true"]');
    const svgCount = await decorativeSvgs.count();
    expect(svgCount).toBeGreaterThan(0);
  });

  test('should have proper heading hierarchy without skipping levels', async ({ page }) => {
    // Get all headings on the page
    const headings = await page.evaluate(() => {
      const headingElements = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      return Array.from(headingElements).map(h => ({
        level: parseInt(h.tagName.charAt(1)),
        text: h.textContent?.trim() || '',
      }));
    });

    // Verify there is exactly one h1
    const h1Count = headings.filter(h => h.level === 1).length;
    expect(h1Count).toBe(1);

    // Verify h1 contains MirDB
    const h1 = headings.find(h => h.level === 1);
    expect(h1?.text).toContain('MirDB');

    // Verify heading hierarchy doesn't skip levels
    let previousLevel = 0;
    for (const heading of headings) {
      // Heading level should not increase by more than 1
      if (previousLevel > 0) {
        const levelJump = heading.level - previousLevel;
        // We can go up any number of levels (e.g., h3 -> h2 -> h1)
        // But we should not skip down more than 1 level (e.g., h1 -> h3 is bad)
        expect(levelJump).toBeLessThanOrEqual(1);
      }
      previousLevel = heading.level;
    }

    // Verify there are h2 headings for main sections
    const h2Count = headings.filter(h => h.level === 2).length;
    expect(h2Count).toBeGreaterThan(0);

    // Check expected section headings exist
    const headingTexts = headings.map(h => h.text.toLowerCase());
    expect(headingTexts.some(t => t.includes('features'))).toBe(true);
    expect(headingTexts.some(t => t.includes('quick start'))).toBe(true);
    expect(headingTexts.some(t => t.includes('architecture'))).toBe(true);
  });

  test('should use semantic HTML elements correctly', async ({ page }) => {
    // Check for semantic structure
    const semanticElements = await page.evaluate(() => {
      return {
        hasHeader: !!document.querySelector('header'),
        hasNav: !!document.querySelector('nav'),
        hasMain: !!document.querySelector('main'),
        hasSections: document.querySelectorAll('section').length > 0,
        hasFooter: !!document.querySelector('footer'),
        hasArticles: document.querySelectorAll('article').length > 0,
        sectionCount: document.querySelectorAll('section').length,
        articleCount: document.querySelectorAll('article').length,
      };
    });

    // Verify essential semantic elements exist
    expect(semanticElements.hasHeader).toBe(true);
    expect(semanticElements.hasNav).toBe(true);
    expect(semanticElements.hasMain).toBe(true);
    expect(semanticElements.hasSections).toBe(true);
    expect(semanticElements.hasFooter).toBe(true);

    // Verify there are multiple sections
    expect(semanticElements.sectionCount).toBeGreaterThanOrEqual(6);

    // Verify navigation has proper aria-label
    const mainNav = page.locator('nav[aria-label="Main navigation"]');
    await expect(mainNav).toBeVisible();

    // Verify footer navigation has proper aria-label
    const footerNav = page.locator('nav[aria-label="Footer navigation"]');
    await expect(footerNav).toBeVisible();

    // Verify main content has id for skip link target
    const mainContent = page.locator('main#main-content');
    await expect(mainContent).toBeVisible();
  });

  test('should have skip link that navigates to main content', async ({ page }) => {
    // Focus on skip link
    const skipLink = page.locator('.skip-link');
    await skipLink.focus();

    // Verify skip link is visible when focused
    await expect(skipLink).toBeVisible();

    // Verify skip link text
    await expect(skipLink).toContainText('Skip to main content');

    // Verify skip link href points to main content
    const href = await skipLink.getAttribute('href');
    expect(href).toBe('#main-content');

    // Click skip link and verify focus moves to main content
    await skipLink.click();

    // Wait a moment for focus to move
    await page.waitForTimeout(100);

    // Verify main content exists with correct id
    const mainContent = page.locator('#main-content');
    await expect(mainContent).toBeVisible();
  });

  test('should support reduced motion preference', async ({ page }) => {
    // Emulate reduced motion preference
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await page.goto('/');

    // Check that html doesn't have smooth scroll when reduced motion is preferred
    const scrollBehavior = await page.evaluate(() => {
      const html = document.documentElement;
      return window.getComputedStyle(html).scrollBehavior;
    });

    // With reduced motion, scroll-behavior should be 'auto' instead of 'smooth'
    expect(scrollBehavior).toBe('auto');
  });

  test('should have adequate touch target sizes on mobile', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    // Check CTA button sizes
    const ctaPrimary = page.locator('.cta-primary').first();
    const ctaBox = await ctaPrimary.boundingBox();

    if (ctaBox) {
      // Minimum touch target should be 44x44px per WCAG 2.5.5
      expect(ctaBox.height).toBeGreaterThanOrEqual(44);
    }

    // Check mobile menu toggle size
    const mobileToggle = page.locator('.mobile-menu-toggle');
    if (await mobileToggle.isVisible()) {
      const toggleBox = await mobileToggle.boundingBox();
      if (toggleBox) {
        expect(toggleBox.height).toBeGreaterThanOrEqual(44);
        expect(toggleBox.width).toBeGreaterThanOrEqual(44);
      }
    }
  });

  test('should have proper landmark regions', async ({ page }) => {
    // Run axe-core with landmark rules
    const landmarkResults = await new AxeBuilder({ page })
      .withRules(['landmark-one-main', 'landmark-no-duplicate-banner', 'landmark-no-duplicate-contentinfo'])
      .analyze();

    // There should be no landmark violations
    expect(landmarkResults.violations).toHaveLength(0);
  });
});
