/**
 * MirDB Landing Page - Accessibility Unit Tests
 * Owner: Scenario 12 - Accessibility Compliance
 *
 * Tests WCAG 2.1 AA accessibility requirements:
 * - Skip navigation link
 * - Image alt text
 * - ARIA labels
 * - Color contrast
 * - Heading hierarchy
 */

const { test, expect } = require('@playwright/test');
const { setupPage } = require('../helpers/test-utils');

test.describe('Accessibility Compliance - Unit Tests', () => {
  test.beforeEach(async ({ page }) => {
    await setupPage(page);
  });

  test('Test Case 1: Skip navigation link is present for screen readers', async ({ page }) => {
    // Check for skip link
    const skipLink = page.locator('.skip-link, [href="#main-content"]');
    await expect(skipLink).toHaveCount(1);

    // Verify skip link text
    const skipLinkText = await skipLink.textContent();
    expect(skipLinkText?.toLowerCase()).toContain('skip');
    expect(skipLinkText?.toLowerCase()).toContain('main');

    // Verify skip link points to main content
    const href = await skipLink.getAttribute('href');
    expect(href).toBe('#main-content');

    // Verify main-content element exists
    const mainContent = page.locator('#main-content');
    await expect(mainContent).toHaveCount(1);

    // Verify skip link is initially visually hidden but becomes visible on focus
    const skipLinkStyles = await skipLink.evaluate(el => {
      const styles = window.getComputedStyle(el);
      return {
        position: styles.position,
        left: styles.left,
        top: styles.top
      };
    });

    // Skip link should be positioned off-screen initially
    expect(
      skipLinkStyles.position === 'absolute' ||
      parseInt(skipLinkStyles.left) < 0 ||
      parseInt(skipLinkStyles.top) < 0
    ).toBe(true);
  });

  test('Test Case 2: Logo image has appropriate alt text', async ({ page }) => {
    // Find the logo image
    const logoImage = page.locator('img.hero-logo, .hero img[src*="logo"]');

    if (await logoImage.count() > 0) {
      const altText = await logoImage.getAttribute('alt');

      // Alt text should exist
      expect(altText).toBeTruthy();

      // Alt text should be descriptive (contain "MirDB" or "logo")
      const altLower = altText?.toLowerCase() || '';
      expect(
        altLower.includes('mirdb') || altLower.includes('logo')
      ).toBe(true);

      // Alt text should not be generic like "image" or just "logo"
      expect(altText).not.toBe('image');
      expect(altText).not.toBe('logo');
    }
  });

  test('Test Case 3: Usage GIF has descriptive alt text', async ({ page }) => {
    // Find usage demo image/gif
    const usageImage = page.locator('img[src*="usage"], .usage-demo img, .usage-gif');

    if (await usageImage.count() > 0) {
      const altText = await usageImage.getAttribute('alt');

      // Alt text should exist
      expect(altText).toBeTruthy();

      // Alt text should be descriptive (more than just "usage" or "demo")
      expect(altText?.length).toBeGreaterThan(10);

      // Alt text should describe the demo content
      const altLower = altText?.toLowerCase() || '';
      const isDescriptive =
        altLower.includes('demo') ||
        altLower.includes('example') ||
        altLower.includes('showing') ||
        altLower.includes('terminal') ||
        altLower.includes('command') ||
        altLower.includes('mirdb');

      expect(isDescriptive).toBe(true);
    }
  });

  test('Test Case 6: Copy buttons have aria-label describing their function', async ({ page }) => {
    // Find all copy buttons
    const copyButtons = page.locator('.copy-btn, button[class*="copy"]');
    const count = await copyButtons.count();

    if (count > 0) {
      for (let i = 0; i < count; i++) {
        const button = copyButtons.nth(i);
        const ariaLabel = await button.getAttribute('aria-label');

        // Each copy button should have an aria-label
        expect(ariaLabel).toBeTruthy();

        // aria-label should describe the copy function
        const labelLower = ariaLabel?.toLowerCase() || '';
        expect(
          labelLower.includes('copy') ||
          labelLower.includes('clipboard')
        ).toBe(true);
      }
    }
  });

  test('Test Case 7: Text meets 4.5:1 contrast ratio minimum', async ({ page }) => {
    // Get CSS custom properties for color scheme
    const colors = await page.evaluate(() => {
      const root = document.documentElement;
      const styles = getComputedStyle(root);

      return {
        bg: styles.getPropertyValue('--color-bg').trim(),
        text: styles.getPropertyValue('--color-text').trim(),
        textMuted: styles.getPropertyValue('--color-text-muted').trim(),
        accent: styles.getPropertyValue('--color-accent').trim()
      };
    });

    // Helper function to parse color to RGB
    function parseColor(color) {
      if (color.startsWith('#')) {
        const hex = color.slice(1);
        return {
          r: parseInt(hex.slice(0, 2), 16),
          g: parseInt(hex.slice(2, 4), 16),
          b: parseInt(hex.slice(4, 6), 16)
        };
      }
      return null;
    }

    // Helper function to calculate relative luminance
    function getLuminance(rgb) {
      const [r, g, b] = [rgb.r, rgb.g, rgb.b].map(c => {
        c = c / 255;
        return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
      });
      return 0.2126 * r + 0.7152 * g + 0.0722 * b;
    }

    // Helper function to calculate contrast ratio
    function getContrastRatio(color1, color2) {
      const l1 = getLuminance(color1);
      const l2 = getLuminance(color2);
      const lighter = Math.max(l1, l2);
      const darker = Math.min(l1, l2);
      return (lighter + 0.05) / (darker + 0.05);
    }

    const bgColor = parseColor(colors.bg);
    const textColor = parseColor(colors.text);
    const accentColor = parseColor(colors.accent);

    if (bgColor && textColor) {
      const mainTextContrast = getContrastRatio(bgColor, textColor);
      // Main text should meet 4.5:1 ratio
      expect(mainTextContrast).toBeGreaterThanOrEqual(4.5);
    }

    if (bgColor && accentColor) {
      const accentContrast = getContrastRatio(bgColor, accentColor);
      // Accent text (links) should meet 4.5:1 ratio
      expect(accentContrast).toBeGreaterThanOrEqual(4.5);
    }
  });

  test('Test Case 8: Headings follow proper h1 > h2 > h3 hierarchy', async ({ page }) => {
    // Get all headings in document order
    const headings = await page.evaluate(() => {
      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      return Array.from(allHeadings).map(h => ({
        level: parseInt(h.tagName.substring(1)),
        text: h.textContent?.trim().substring(0, 50),
        id: h.id
      }));
    });

    expect(headings.length).toBeGreaterThan(0);

    // Should have exactly one h1
    const h1Count = headings.filter(h => h.level === 1).length;
    expect(h1Count).toBe(1);

    // First heading should be h1
    expect(headings[0].level).toBe(1);

    // Check for proper hierarchy - no skipping levels
    for (let i = 1; i < headings.length; i++) {
      const prevLevel = headings[i - 1].level;
      const currLevel = headings[i].level;

      // Current heading should not skip more than one level from previous
      // e.g., h1 -> h3 is invalid, h2 -> h4 is invalid
      // But h2 -> h2 or h2 -> h3 is valid
      expect(currLevel).toBeLessThanOrEqual(prevLevel + 1);
    }
  });

  test('All images have alt attributes', async ({ page }) => {
    const images = page.locator('img');
    const count = await images.count();

    for (let i = 0; i < count; i++) {
      const img = images.nth(i);
      const alt = await img.getAttribute('alt');

      // Every image must have an alt attribute (can be empty for decorative images)
      expect(alt !== null, `Image ${i} missing alt attribute`).toBe(true);
    }
  });

  test('ARIA attributes are properly used', async ({ page }) => {
    // Check for proper aria-expanded on toggle buttons
    const toggleButtons = page.locator('[aria-expanded]');
    const toggleCount = await toggleButtons.count();

    for (let i = 0; i < toggleCount; i++) {
      const button = toggleButtons.nth(i);
      const expanded = await button.getAttribute('aria-expanded');

      // aria-expanded should be "true" or "false"
      expect(['true', 'false']).toContain(expanded);

      // If aria-controls is present, the referenced element should exist
      const controls = await button.getAttribute('aria-controls');
      if (controls) {
        const controlledElement = page.locator(`#${controls}`);
        await expect(controlledElement).toHaveCount(1);
      }
    }

    // Check for aria-labelledby references
    const labelledElements = page.locator('[aria-labelledby]');
    const labelledCount = await labelledElements.count();

    for (let i = 0; i < labelledCount; i++) {
      const element = labelledElements.nth(i);
      const labelledBy = await element.getAttribute('aria-labelledby');

      if (labelledBy) {
        // The referenced element should exist
        const labelElement = page.locator(`#${labelledBy}`);
        await expect(labelElement).toHaveCount(1);
      }
    }
  });

  test('Form elements have associated labels', async ({ page }) => {
    const inputs = page.locator('input, select, textarea');
    const count = await inputs.count();

    for (let i = 0; i < count; i++) {
      const input = inputs.nth(i);
      const type = await input.getAttribute('type');

      // Skip hidden inputs
      if (type === 'hidden') continue;

      const id = await input.getAttribute('id');
      const ariaLabel = await input.getAttribute('aria-label');
      const ariaLabelledBy = await input.getAttribute('aria-labelledby');

      // Input should have either a label, aria-label, or aria-labelledby
      const hasLabel = id ? await page.locator(`label[for="${id}"]`).count() > 0 : false;

      expect(
        hasLabel || ariaLabel || ariaLabelledBy,
        `Form element ${i} missing accessible label`
      ).toBeTruthy();
    }
  });

  test('Landmark regions are properly defined', async ({ page }) => {
    // Page should have main landmark
    const main = page.locator('main, [role="main"]');
    await expect(main).toHaveCount(1);

    // Page should have navigation
    const nav = page.locator('nav, [role="navigation"]');
    expect(await nav.count()).toBeGreaterThanOrEqual(1);

    // Navigation should have aria-label for screen readers
    const navLabel = await nav.first().getAttribute('aria-label');
    expect(navLabel).toBeTruthy();

    // Page should have footer/contentinfo
    const footer = page.locator('footer, [role="contentinfo"]');
    await expect(footer).toHaveCount(1);
  });

  test('Interactive elements have accessible names', async ({ page }) => {
    // Check buttons
    const buttons = page.locator('button');
    const buttonCount = await buttons.count();

    for (let i = 0; i < buttonCount; i++) {
      const button = buttons.nth(i);
      const text = await button.textContent();
      const ariaLabel = await button.getAttribute('aria-label');
      const ariaLabelledBy = await button.getAttribute('aria-labelledby');

      // Button should have accessible name
      expect(
        text?.trim() || ariaLabel || ariaLabelledBy,
        `Button ${i} missing accessible name`
      ).toBeTruthy();
    }

    // Check links
    const links = page.locator('a');
    const linkCount = await links.count();

    for (let i = 0; i < linkCount; i++) {
      const link = links.nth(i);
      const text = await link.textContent();
      const ariaLabel = await link.getAttribute('aria-label');
      const title = await link.getAttribute('title');

      // Link should have accessible name
      expect(
        text?.trim() || ariaLabel || title,
        `Link ${i} missing accessible name`
      ).toBeTruthy();
    }
  });

  test('No duplicate IDs exist', async ({ page }) => {
    const duplicateIds = await page.evaluate(() => {
      const allIds = document.querySelectorAll('[id]');
      const idCounts = {};

      allIds.forEach(el => {
        const id = el.id;
        idCounts[id] = (idCounts[id] || 0) + 1;
      });

      return Object.entries(idCounts)
        .filter(([_, count]) => count > 1)
        .map(([id, count]) => ({ id, count }));
    });

    expect(duplicateIds, `Duplicate IDs found: ${JSON.stringify(duplicateIds)}`).toHaveLength(0);
  });

  test('Language is specified on html element', async ({ page }) => {
    const lang = await page.locator('html').getAttribute('lang');
    expect(lang).toBeTruthy();
    expect(lang).toBe('en');
  });
});
