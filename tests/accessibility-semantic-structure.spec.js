// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Accessibility - Semantic Structure Tests
 *
 * These tests verify proper heading hierarchy and semantic HTML structure
 * as specified in NFR-3 (WCAG 2.1 AA compliance).
 */

test.describe('Accessibility - Semantic Structure', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should have exactly one H1 element containing the product name', async ({ page }) => {
    // Test Case 1: Check for single H1 element
    // Expected: Page has exactly one H1 element (product name)

    const h1Elements = page.locator('h1');
    const h1Count = await h1Elements.count();

    // Verify exactly one H1 exists
    expect(h1Count).toBe(1);

    // Verify H1 contains the product name "MirDB"
    const h1Text = await h1Elements.first().textContent();
    expect(h1Text).toContain('MirDB');
  });

  test('should have proper heading hierarchy without skipping levels', async ({ page }) => {
    // Test Case 2: Verify heading hierarchy
    // Expected: Headings follow logical order (H1 > H2 > H3) without skipping

    // Get all headings in document order
    const headings = await page.evaluate(() => {
      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      return Array.from(allHeadings).map(h => ({
        level: parseInt(h.tagName.charAt(1)),
        text: h.textContent?.trim() || ''
      }));
    });

    // Verify we have headings
    expect(headings.length).toBeGreaterThan(0);

    // Verify first heading is H1
    expect(headings[0].level).toBe(1);

    // Verify no heading level is skipped (e.g., no H1 directly to H3)
    for (let i = 1; i < headings.length; i++) {
      const currentLevel = headings[i].level;
      const previousLevel = headings[i - 1].level;

      // Current level should not be more than 1 level deeper than any previous heading
      // This allows H1 > H2 > H3, but not H1 > H3 (skipping H2)
      const maxAllowedLevel = previousLevel + 1;

      expect(currentLevel).toBeLessThanOrEqual(
        Math.max(maxAllowedLevel, previousLevel),
        `Heading "${headings[i].text}" (H${currentLevel}) skips levels after "${headings[i-1].text}" (H${previousLevel})`
      );
    }

    // Verify we have H2 elements following H1
    const h2Elements = headings.filter(h => h.level === 2);
    expect(h2Elements.length).toBeGreaterThan(0);
  });

  test('should use semantic HTML elements appropriately', async ({ page }) => {
    // Test Case 3: Check for semantic HTML elements
    // Expected: Page uses nav, main, section, and footer elements appropriately

    // Check for nav element
    const navElement = page.locator('nav');
    await expect(navElement.first()).toBeVisible();

    // Check for main element
    const mainElement = page.locator('main');
    await expect(mainElement).toBeVisible();

    // Check that main contains the primary content sections
    const mainContent = await mainElement.locator('section').count();
    expect(mainContent).toBeGreaterThan(0);

    // Check for section elements with proper structure
    const sectionElements = page.locator('section');
    const sectionCount = await sectionElements.count();
    expect(sectionCount).toBeGreaterThan(0);

    // Check for footer element
    const footerElement = page.locator('footer');
    await expect(footerElement).toBeVisible();
  });

  test('should have proper navigation landmark', async ({ page }) => {
    // Test Case 4: Verify navigation landmark
    // Expected: Navigation is wrapped in nav element or has role='navigation'

    // Check for nav element OR element with role="navigation"
    const navByElement = page.locator('nav');
    const navByRole = page.locator('[role="navigation"]');

    const navElementCount = await navByElement.count();
    const navRoleCount = await navByRole.count();

    // At least one navigation landmark should exist
    const totalNavLandmarks = navElementCount + navRoleCount;
    expect(totalNavLandmarks).toBeGreaterThanOrEqual(1);

    // If nav element exists, verify it contains links
    if (navElementCount > 0) {
      const navLinks = navByElement.first().locator('a');
      const linkCount = await navLinks.count();
      expect(linkCount).toBeGreaterThan(0);
    }

    // If role="navigation" exists, verify it also contains links
    if (navRoleCount > 0) {
      const roleNavLinks = navByRole.first().locator('a');
      const linkCount = await roleNavLinks.count();
      expect(linkCount).toBeGreaterThan(0);
    }
  });

  test('should have proper ARIA landmarks for main content areas', async ({ page }) => {
    // Additional accessibility check: verify landmark regions
    // Helps assistive technology navigation

    const landmarks = await page.evaluate(() => {
      const results = {
        hasMain: false,
        hasNav: false,
        hasFooter: false,
        hasHeader: false,
        hasBanner: false,
        hasContentinfo: false
      };

      // Check for semantic elements
      results.hasMain = document.querySelector('main') !== null ||
                        document.querySelector('[role="main"]') !== null;
      results.hasNav = document.querySelector('nav') !== null ||
                       document.querySelector('[role="navigation"]') !== null;
      results.hasFooter = document.querySelector('footer') !== null ||
                          document.querySelector('[role="contentinfo"]') !== null;
      results.hasHeader = document.querySelector('header') !== null ||
                          document.querySelector('[role="banner"]') !== null;

      return results;
    });

    // Verify essential landmarks exist
    expect(landmarks.hasMain).toBe(true);
    expect(landmarks.hasNav).toBe(true);
    expect(landmarks.hasFooter).toBe(true);
    expect(landmarks.hasHeader).toBe(true);
  });
});
