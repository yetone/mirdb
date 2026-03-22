/**
 * Accessibility E2E Tests
 * Owners: Scenarios 11, 12, 13 (Accessibility), Scenario 22 (No JS)
 *
 * Test groups:
 * - axe-core accessibility audit
 * - Color contrast validation
 * - Heading hierarchy
 * - ARIA landmarks
 * - Keyboard navigation (Scenario 12)
 * - Focus indicators (Scenario 12)
 * - Image alt text (Scenario 13)
 * - No-JavaScript fallback (Scenario 22)
 */

import { test, expect } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';
import { waitForLoad } from './utils';

test.describe('Accessibility - Basic Requirements (Scenario 11)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForLoad(page);
  });

  test('axe-core accessibility scan finds no critical or serious violations', async ({ page }) => {
    // Run axe-core accessibility audit
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21aa'])
      .analyze();

    // Filter for critical and serious violations only
    const criticalAndSerious = accessibilityScanResults.violations.filter(
      (violation) => violation.impact === 'critical' || violation.impact === 'serious'
    );

    // Log any violations for debugging
    if (criticalAndSerious.length > 0) {
      console.log('Critical/Serious violations found:', JSON.stringify(criticalAndSerious, null, 2));
    }

    // Expect no critical or serious violations
    expect(criticalAndSerious).toHaveLength(0);
  });

  test('body text has sufficient contrast ratio (at least 4.5:1)', async ({ page }) => {
    // Helper function to calculate contrast ratio
    const contrastRatio = await page.evaluate(() => {
      // Function to parse CSS color to RGB
      function parseColor(color: string): { r: number; g: number; b: number } | null {
        const canvas = document.createElement('canvas');
        canvas.width = 1;
        canvas.height = 1;
        const ctx = canvas.getContext('2d');
        if (!ctx) return null;
        ctx.fillStyle = color;
        ctx.fillRect(0, 0, 1, 1);
        const [r, g, b] = ctx.getImageData(0, 0, 1, 1).data;
        return { r, g, b };
      }

      // Function to calculate relative luminance
      function getLuminance(r: number, g: number, b: number): number {
        const [rs, gs, bs] = [r, g, b].map((c) => {
          c = c / 255;
          return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
        });
        return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
      }

      // Function to calculate contrast ratio
      function getContrastRatio(fg: { r: number; g: number; b: number }, bg: { r: number; g: number; b: number }): number {
        const l1 = getLuminance(fg.r, fg.g, fg.b);
        const l2 = getLuminance(bg.r, bg.g, bg.b);
        const lighter = Math.max(l1, l2);
        const darker = Math.min(l1, l2);
        return (lighter + 0.05) / (darker + 0.05);
      }

      // Get body element and computed styles
      const body = document.body;
      const bodyStyles = window.getComputedStyle(body);

      // Get text color and background color
      const textColor = bodyStyles.color;
      const bgColor = bodyStyles.backgroundColor;

      const fgParsed = parseColor(textColor);
      const bgParsed = parseColor(bgColor);

      if (!fgParsed || !bgParsed) return 0;

      return getContrastRatio(fgParsed, bgParsed);
    });

    // WCAG 2.1 AA requires at least 4.5:1 contrast ratio for normal text
    expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
  });

  test('heading hierarchy has exactly one h1 and headings in sequential order', async ({ page }) => {
    // Get all headings on the page
    const headings = await page.evaluate(() => {
      const headingElements = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      return Array.from(headingElements).map((el) => ({
        tag: el.tagName.toLowerCase(),
        level: parseInt(el.tagName.charAt(1)),
        text: el.textContent?.trim() || '',
      }));
    });

    // Check exactly one h1
    const h1Count = headings.filter((h) => h.tag === 'h1').length;
    expect(h1Count).toBe(1);

    // Check sequential order (no skipping levels)
    let previousLevel = 0;
    for (const heading of headings) {
      // Heading level should not skip more than 1 level
      // (e.g., h1 -> h3 is invalid, but h2 -> h2 or h2 -> h3 is valid)
      if (previousLevel > 0) {
        const levelDiff = heading.level - previousLevel;
        // Can go down any amount (h3 -> h2 is OK)
        // Can go up at most 1 level (h2 -> h3 is OK, h2 -> h4 is not)
        expect(levelDiff).toBeLessThanOrEqual(1);
      }
      previousLevel = heading.level;
    }
  });

  test('page has required ARIA landmark regions (main, contentinfo/footer)', async ({ page }) => {
    // Check for main landmark
    const mainLandmark = await page.locator('main, [role="main"]').count();
    expect(mainLandmark).toBeGreaterThanOrEqual(1);

    // Check for contentinfo landmark (footer)
    const contentinfoLandmark = await page.locator('footer, [role="contentinfo"]').count();
    expect(contentinfoLandmark).toBeGreaterThanOrEqual(1);

    // Check for banner landmark (header)
    const bannerLandmark = await page.locator('header, [role="banner"]').count();
    expect(bannerLandmark).toBeGreaterThanOrEqual(1);
  });

  test('page has navigation landmark if navigation exists', async ({ page }) => {
    // Check if there are navigation elements
    const navElements = await page.locator('nav, [role="navigation"]').count();

    // If navigation exists, it should have proper landmark role
    if (navElements > 0) {
      // Navigation should have aria-label for accessibility
      const navWithLabel = await page.locator('nav[aria-label], [role="navigation"][aria-label]').count();
      expect(navWithLabel).toBeGreaterThanOrEqual(1);
    }

    // The page should have at least one navigation in the footer
    const footerNav = await page.locator('footer nav, footer [role="navigation"]').count();
    expect(footerNav).toBeGreaterThanOrEqual(1);
  });

  test('all sections have proper ARIA labeling', async ({ page }) => {
    // Get all sections with aria-labelledby or aria-label
    const sections = await page.evaluate(() => {
      const sectionElements = document.querySelectorAll('section');
      return Array.from(sectionElements).map((section) => ({
        id: section.id,
        hasAriaLabel: section.hasAttribute('aria-label'),
        hasAriaLabelledBy: section.hasAttribute('aria-labelledby'),
        ariaLabelledBy: section.getAttribute('aria-labelledby'),
      }));
    });

    // Each section should have either aria-label or aria-labelledby
    for (const section of sections) {
      const hasLabel = section.hasAriaLabel || section.hasAriaLabelledBy;
      expect(hasLabel).toBeTruthy();

      // If aria-labelledby is used, the referenced element should exist
      if (section.hasAriaLabelledBy && section.ariaLabelledBy) {
        const labelElement = await page.locator(`#${section.ariaLabelledBy}`).count();
        expect(labelElement).toBeGreaterThanOrEqual(1);
      }
    }
  });
});
