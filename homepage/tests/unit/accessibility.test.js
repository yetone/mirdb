/**
 * Unit Tests for Accessibility Compliance (WCAG 2.1 AA)
 * Owner: Scenario 9 - Accessibility Compliance
 *
 * Tests:
 * - Heading hierarchy (single h1, no skipped levels)
 * - Image alt attributes
 * - ARIA labels on icon buttons
 * - Code block accessibility
 */

const { test, expect } = require('@playwright/test');

test.describe('Accessibility Unit Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 4: Page has exactly one h1 element (MirDB title)', async ({ page }) => {
    // Count all h1 elements on the page
    const h1Elements = await page.locator('h1').all();

    expect(h1Elements.length).toBe(1);

    // Verify the h1 contains the expected content
    const h1Text = await page.locator('h1').textContent();
    expect(h1Text).toContain('MirDB');
  });

  test('Test Case 5: Heading hierarchy sequence - no levels skipped', async ({ page }) => {
    // Get all heading elements and their levels
    const headings = await page.evaluate(() => {
      const headingElements = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      return Array.from(headingElements).map(h => ({
        level: parseInt(h.tagName.charAt(1)),
        text: h.textContent.trim().substring(0, 50)
      }));
    });

    // Verify no heading levels are skipped
    // Rule: You can go down by any amount, but when going up, you can only go up by 1
    let previousLevel = 0;
    const violations = [];

    for (const heading of headings) {
      const currentLevel = heading.level;

      // Check if we're skipping levels going down (e.g., h1 to h3 without h2)
      if (previousLevel > 0 && currentLevel > previousLevel + 1) {
        violations.push({
          issue: `Skipped from h${previousLevel} to h${currentLevel}`,
          heading: heading.text
        });
      }

      previousLevel = currentLevel;
    }

    expect(violations).toHaveLength(0);
  });

  test('Test Case 6: All images have alt text', async ({ page }) => {
    // Get all img elements and check for alt attributes
    const imagesWithoutAlt = await page.evaluate(() => {
      const images = document.querySelectorAll('img');
      const missingAlt = [];

      images.forEach(img => {
        // Check if alt attribute exists (empty string is valid for decorative images with aria-hidden)
        const hasAlt = img.hasAttribute('alt');
        const ariaHidden = img.getAttribute('aria-hidden') === 'true';

        if (!hasAlt && !ariaHidden) {
          missingAlt.push({
            src: img.src,
            parent: img.parentElement?.className || 'unknown'
          });
        }
      });

      return missingAlt;
    });

    expect(imagesWithoutAlt).toHaveLength(0);
  });

  test('Test Case 8: Icon-only buttons have aria-label or accessible name', async ({ page }) => {
    // Get all buttons and check for accessible names
    const buttonsWithoutLabels = await page.evaluate(() => {
      const buttons = document.querySelectorAll('button');
      const violations = [];

      buttons.forEach(button => {
        // Check for accessible name
        const ariaLabel = button.getAttribute('aria-label');
        const ariaLabelledBy = button.getAttribute('aria-labelledby');
        const title = button.getAttribute('title');
        const textContent = button.textContent?.trim();

        // Check if button has visible text (excluding icon-only buttons)
        const hasVisibleText = textContent && !button.querySelector('.visually-hidden') &&
          !textContent.includes('Copy'); // Copy buttons have text

        // Button needs at least one accessible name
        const hasAccessibleName = ariaLabel || ariaLabelledBy || title || hasVisibleText;

        if (!hasAccessibleName) {
          violations.push({
            outerHTML: button.outerHTML.substring(0, 100),
            classes: button.className
          });
        }
      });

      return violations;
    });

    expect(buttonsWithoutLabels).toHaveLength(0);
  });

  test('Test Case 9: Code blocks have proper accessibility labeling', async ({ page }) => {
    // Check code blocks for accessibility attributes
    const codeBlockAccessibility = await page.evaluate(() => {
      const codeBlocks = document.querySelectorAll('.code-block__content, pre');
      const results = [];

      codeBlocks.forEach(block => {
        const hasRole = block.getAttribute('role') === 'code';
        const hasAriaLabel = block.hasAttribute('aria-label');
        const hasAriaLabelledBy = block.hasAttribute('aria-labelledby');
        const isTabindexed = block.hasAttribute('tabindex');
        const isInRegion = block.closest('[role="region"]') !== null;

        // Code block should have at least one accessibility feature
        const isAccessible = hasRole || hasAriaLabel || hasAriaLabelledBy || isInRegion;

        results.push({
          isAccessible,
          hasRole,
          hasAriaLabel,
          hasAriaLabelledBy,
          isTabindexed,
          isInRegion,
          id: block.id || 'unnamed'
        });
      });

      return results;
    });

    // All code blocks should be accessible
    codeBlockAccessibility.forEach(block => {
      expect(block.isAccessible).toBe(true);
    });
  });

  test('Skip link exists and points to main content', async ({ page }) => {
    // Check for skip link
    const skipLink = await page.locator('.skip-link, a[href="#main-content"]').first();

    await expect(skipLink).toBeAttached();

    const href = await skipLink.getAttribute('href');
    expect(href).toBe('#main-content');

    // Verify target exists
    const mainContent = await page.locator('#main-content');
    await expect(mainContent).toBeAttached();
  });

  test('Landmark roles are properly defined', async ({ page }) => {
    const landmarks = await page.evaluate(() => {
      return {
        hasMain: document.querySelector('main, [role="main"]') !== null,
        hasHeader: document.querySelector('header, [role="banner"]') !== null,
        hasFooter: document.querySelector('footer, [role="contentinfo"]') !== null,
        hasNav: document.querySelector('nav, [role="navigation"]') !== null
      };
    });

    expect(landmarks.hasMain).toBe(true);
    expect(landmarks.hasHeader).toBe(true);
    expect(landmarks.hasFooter).toBe(true);
    expect(landmarks.hasNav).toBe(true);
  });
});
