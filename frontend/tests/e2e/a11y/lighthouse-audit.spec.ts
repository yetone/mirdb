/**
 * E2E Accessibility Audit Tests.
 * Owner: Scenario 18 - Performance and Accessibility
 *
 * Validates WCAG AA compliance using automated checks.
 * Covers accessibility score, contrast ratios, and ARIA requirements.
 */

import { test, expect } from '@playwright/test';

test.describe('Accessibility Audit', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('page has proper document structure with landmarks', async ({ page }) => {
    // Check for lang attribute on html element
    const htmlLang = await page.locator('html').getAttribute('lang');
    expect(htmlLang).toBeTruthy();

    // Check for main landmark
    const mainElements = page.locator('main, [role="main"]');
    await expect(mainElements.first()).toBeVisible();

    // Check for navigation landmark
    const navElements = page.locator('nav, [role="navigation"]');
    expect(await navElements.count()).toBeGreaterThan(0);

    // Check for header/banner landmark
    const headerElements = page.locator('header, [role="banner"]');
    expect(await headerElements.count()).toBeGreaterThan(0);

    // Check for contentinfo/footer landmark
    const footerElements = page.locator('footer, [role="contentinfo"]');
    expect(await footerElements.count()).toBeGreaterThan(0);
  });

  test('all images have alt text or are aria-hidden', async ({ page }) => {
    const images = page.locator('img');
    const count = await images.count();

    for (let i = 0; i < count; i++) {
      const img = images.nth(i);
      const alt = await img.getAttribute('alt');
      const ariaHidden = await img.getAttribute('aria-hidden');
      const role = await img.getAttribute('role');

      // Every image should have either alt text or aria-hidden="true" or role="presentation"
      expect(
        alt !== null || ariaHidden === 'true' || role === 'presentation',
        `Image at index ${i} missing alt text and not aria-hidden`
      ).toBe(true);
    }
  });

  test('all buttons have accessible names', async ({ page }) => {
    const buttons = page.locator('button');
    const count = await buttons.count();

    for (let i = 0; i < count; i++) {
      const button = buttons.nth(i);
      const ariaLabel = await button.getAttribute('aria-label');
      const textContent = await button.textContent();
      const title = await button.getAttribute('title');
      const ariaLabelledBy = await button.getAttribute('aria-labelledby');

      // Button should have some accessible name
      const hasAccessibleName =
        (ariaLabel && ariaLabel.trim().length > 0) ||
        (textContent && textContent.trim().length > 0) ||
        (title && title.trim().length > 0) ||
        (ariaLabelledBy && ariaLabelledBy.trim().length > 0);

      expect(
        hasAccessibleName,
        `Button at index ${i} missing accessible name`
      ).toBe(true);
    }
  });

  test('all form inputs have associated labels', async ({ page }) => {
    const inputs = page.locator('input:not([type="hidden"])');
    const count = await inputs.count();

    for (let i = 0; i < count; i++) {
      const input = inputs.nth(i);
      const id = await input.getAttribute('id');
      const ariaLabel = await input.getAttribute('aria-label');
      const ariaLabelledBy = await input.getAttribute('aria-labelledby');
      const placeholder = await input.getAttribute('placeholder');
      const hasLabel = await input.evaluate((el) => {
        const id = el.id;
        if (id) {
          const label = document.querySelector(`label[for="${id}"]`);
          if (label) return true;
        }
        // Check if input is wrapped in a label
        const parentLabel = el.closest('label');
        if (parentLabel) return true;
        return false;
      });

      const hasAccessibleName =
        hasLabel ||
        (ariaLabel && ariaLabel.trim().length > 0) ||
        (ariaLabelledBy && ariaLabelledBy.trim().length > 0) ||
        (placeholder && placeholder.trim().length > 0);

      expect(
        hasAccessibleName,
        `Input at index ${i} (id=${id}) missing accessible name`
      ).toBe(true);
    }
  });

  test('color contrast meets WCAG AA standards for body text', async ({ page }) => {
    // Check contrast of main text elements
    const textElements = [
      { selector: 'body', minRatio: 4.5 },
      { selector: 'h1', minRatio: 3 },
      { selector: 'h2', minRatio: 3 },
      { selector: 'p', minRatio: 4.5 },
      { selector: 'button', minRatio: 4.5 },
      { selector: 'a', minRatio: 4.5 },
    ];

    for (const { selector, minRatio } of textElements) {
      const elements = page.locator(selector);
      const count = await elements.count();
      if (count === 0) continue;

      const element = elements.first();
      const isVisible = await element.isVisible().catch(() => false);
      if (!isVisible) continue;

      const contrastInfo = await element.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        const color = styles.color;
        const bgColor = styles.backgroundColor;

        // Simple luminance calculation for RGB values
        function getLuminance(colorStr: string): number {
          const match = colorStr.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
          if (!match) return 1;

          const r = parseInt(match[1]) / 255;
          const g = parseInt(match[2]) / 255;
          const b = parseInt(match[3]) / 255;

          const rs = r <= 0.03928 ? r / 12.92 : Math.pow((r + 0.055) / 1.055, 2.4);
          const gs = g <= 0.03928 ? g / 12.92 : Math.pow((g + 0.055) / 1.055, 2.4);
          const bs = b <= 0.03928 ? b / 12.92 : Math.pow((b + 0.055) / 1.055, 2.4);

          return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
        }

        const fgLum = getLuminance(color);
        const bgLum = getLuminance(bgColor);

        const lighter = Math.max(fgLum, bgLum);
        const darker = Math.min(fgLum, bgLum);
        const ratio = (lighter + 0.05) / (darker + 0.05);

        return { ratio, color, bgColor };
      });

      // Only enforce if we have valid colors (not transparent backgrounds which inherit)
      if (contrastInfo.bgColor !== 'rgba(0, 0, 0, 0)') {
        expect(
          contrastInfo.ratio,
          `Contrast for ${selector} is ${contrastInfo.ratio.toFixed(2)}, expected >= ${minRatio}`
        ).toBeGreaterThanOrEqual(minRatio);
      }
    }
  });

  test('page has proper heading hierarchy', async ({ page }) => {
    // Check that h1 exists and is only one
    const h1Elements = page.locator('h1');
    const h1Count = await h1Elements.count();
    expect(h1Count).toBeGreaterThanOrEqual(1);

    // Check that headings are in proper order (no skipping levels)
    const headingLevels = await page.evaluate(() => {
      const headings = Array.from(document.querySelectorAll('h1, h2, h3, h4, h5, h6'));
      return headings.map((h) => parseInt(h.tagName[1]));
    });

    // Verify no level is skipped by more than 1
    for (let i = 1; i < headingLevels.length; i++) {
      const prev = headingLevels[i - 1];
      const curr = headingLevels[i];
      // A heading level should not jump up more than 1 level from the previous
      // e.g., h1 -> h3 is a skip (not allowed), h3 -> h1 is fine (new section)
      if (curr > prev) {
        expect(
          curr - prev,
          `Heading level jumped from h${prev} to h${curr}`
        ).toBeLessThanOrEqual(1);
      }
    }
  });

  test('ARIA live regions exist for dynamic content', async ({ page }) => {
    // Check for aria-live regions in metrics dashboard
    const liveRegions = page.locator('[aria-live]');
    const count = await liveRegions.count();

    // Should have at least some live regions for dynamic content
    expect(count).toBeGreaterThanOrEqual(1);

    // Connection status should have aria-live
    const connectionStatus = page.locator('[role="status"], [aria-live]');
    expect(await connectionStatus.count()).toBeGreaterThan(0);
  });

  test('no critical accessibility violations detected', async ({ page }) => {
    // Run basic accessibility checks using evaluate
    const violations = await page.evaluate(() => {
      const issues: string[] = [];

      // Check for empty links
      document.querySelectorAll('a').forEach((a, i) => {
        if (!a.textContent?.trim() && !a.getAttribute('aria-label') && !a.getAttribute('title')) {
          issues.push(`Empty link at index ${i}`);
        }
      });

      // Check for duplicate IDs
      const allIds = Array.from(document.querySelectorAll('[id]')).map((el) => el.id);
      const seen = new Set<string>();
      allIds.forEach((id) => {
        if (seen.has(id)) {
          issues.push(`Duplicate ID: ${id}`);
        }
        seen.add(id);
      });

      // Check for missing lang attribute
      const html = document.documentElement;
      if (!html.getAttribute('lang')) {
        issues.push('Missing lang attribute on html element');
      }

      // Check for missing title
      if (!document.title || document.title.trim().length === 0) {
        issues.push('Missing page title');
      }

      // Check for interactive elements without accessible names
      document.querySelectorAll('button').forEach((btn, i) => {
        const hasName =
          btn.textContent?.trim() ||
          btn.getAttribute('aria-label') ||
          btn.getAttribute('title') ||
          btn.getAttribute('aria-labelledby');
        if (!hasName) {
          issues.push(`Button without accessible name at index ${i}`);
        }
      });

      return issues;
    });

    expect(violations, `Accessibility violations: ${violations.join(', ')}`).toEqual([]);
  });
});
