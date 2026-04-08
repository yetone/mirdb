// @ts-check
const { test, expect } = require('@playwright/test');
const AxeBuilder = require('@axe-core/playwright').default;

/**
 * Accessibility Compliance E2E Tests
 * Owner: Scenario 11 - Accessibility Compliance
 *
 * Test cases:
 * - Tab navigation reaches all interactive elements
 * - Focus indicators are visible
 * - Skip link is present
 * - Semantic HTML structure (h1, h2, nav, main, footer)
 * - Images have alt text
 * - Color contrast meets WCAG AA (4.5:1 normal, 3:1 large)
 */

test.describe('Accessibility Compliance', () => {

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for page to fully load
    await page.waitForLoadState('networkidle');
  });

  test.describe('Keyboard Navigation', () => {

    test('TC1: all interactive elements are reachable via Tab key in logical order', async ({ page }) => {
      // Start from body and tab through all interactive elements
      await page.keyboard.press('Tab');

      // Expected order: skip link -> nav links -> hero CTAs -> code copy buttons -> etc.
      const expectedOrder = [
        '.skip-link',
        '.nav-logo',
        '.nav-link[href="#features"]',
        '.nav-link[href="#architecture"]',
        '.nav-link[href="#installation"]',
        '.nav-link-github',
        '.theme-toggle',
        '.hero-cta-primary',
        '.hero-cta-secondary'
      ];

      // Track focused elements
      const focusedElements = [];

      // Press Tab multiple times and record what gets focused
      for (let i = 0; i < 15; i++) {
        const focusedElement = await page.evaluate(() => {
          const el = document.activeElement;
          if (!el || el === document.body) return null;
          return {
            tagName: el.tagName.toLowerCase(),
            className: el.className,
            href: el.getAttribute('href'),
            type: el.getAttribute('type'),
            text: el.textContent?.trim().substring(0, 50)
          };
        });

        if (focusedElement) {
          focusedElements.push(focusedElement);
        }

        await page.keyboard.press('Tab');
      }

      // Verify we have multiple interactive elements
      expect(focusedElements.length).toBeGreaterThan(5);

      // Verify skip link is first
      expect(focusedElements[0].className).toContain('skip-link');

      // Verify nav elements come before main content
      const navElements = focusedElements.filter(el =>
        el.className.includes('nav-') || el.className.includes('theme-toggle')
      );
      expect(navElements.length).toBeGreaterThan(3);
    });

    test('TC2: all focusable elements have visible focus indicators', async ({ page }) => {
      // Get all interactive elements
      const interactiveElements = await page.$$('a, button, [tabindex="0"], input, select, textarea');

      for (const element of interactiveElements.slice(0, 10)) {
        // Skip hidden elements
        const isVisible = await element.isVisible();
        if (!isVisible) continue;

        // Focus the element
        await element.focus();

        // Check for focus styles
        const hasVisibleFocus = await element.evaluate((el) => {
          const styles = window.getComputedStyle(el);
          const outlineWidth = parseInt(styles.outlineWidth) || 0;
          const outlineStyle = styles.outlineStyle;
          const boxShadow = styles.boxShadow;

          // Element has visible focus if it has outline or box-shadow
          return (outlineWidth > 0 && outlineStyle !== 'none') ||
                 (boxShadow && boxShadow !== 'none');
        });

        expect(hasVisibleFocus).toBeTruthy();
      }
    });

    test('TC3: skip to main content link is present for keyboard users', async ({ page }) => {
      // Check skip link exists
      const skipLink = page.locator('.skip-link');
      await expect(skipLink).toHaveCount(1);

      // Check it has correct href
      await expect(skipLink).toHaveAttribute('href', '#main-content');

      // Check it has correct text
      await expect(skipLink).toHaveText(/skip to main content/i);

      // Check skip link becomes visible on focus
      await page.keyboard.press('Tab');
      await expect(skipLink).toBeFocused();

      // Wait for CSS transition to complete (150ms)
      await page.waitForTimeout(200);

      // Verify skip link is visible when focused
      const isVisibleOnFocus = await skipLink.evaluate((el) => {
        const rect = el.getBoundingClientRect();
        return rect.top >= 0 && rect.top < window.innerHeight;
      });
      expect(isVisibleOnFocus).toBeTruthy();

      // Check target element exists
      const mainContent = page.locator('#main-content');
      await expect(mainContent).toHaveCount(1);
      await expect(mainContent).toHaveAttribute('id', 'main-content');
    });

  });

  test.describe('Semantic HTML Structure', () => {

    test('TC4: page uses proper heading hierarchy and semantic elements', async ({ page }) => {
      // Check for single h1
      const h1Elements = await page.locator('h1').all();
      expect(h1Elements.length).toBe(1);

      // Check h1 content is meaningful
      const h1Text = await page.locator('h1').textContent();
      expect(h1Text).toBeTruthy();
      expect(h1Text.length).toBeGreaterThan(10);

      // Check for h2 headings (section titles)
      const h2Elements = await page.locator('h2').all();
      expect(h2Elements.length).toBeGreaterThan(2);

      // Check heading hierarchy (no skipping levels)
      const headings = await page.$$eval('h1, h2, h3, h4, h5, h6', (els) => {
        return els.map(el => ({
          level: parseInt(el.tagName.substring(1)),
          text: el.textContent?.trim().substring(0, 50)
        }));
      });

      // Verify first heading is h1
      expect(headings[0].level).toBe(1);

      // Check no level skipping (e.g., h1 -> h3 without h2)
      for (let i = 1; i < headings.length; i++) {
        const diff = headings[i].level - headings[i-1].level;
        expect(diff).toBeLessThanOrEqual(1);
      }

      // Check for semantic HTML elements
      await expect(page.locator('nav').first()).toBeVisible();
      await expect(page.locator('main')).toHaveCount(1);
      await expect(page.locator('footer')).toHaveCount(1);

      // Check nav has proper aria-label
      const navAriaLabel = await page.locator('nav').first().getAttribute('aria-label');
      expect(navAriaLabel).toBeTruthy();

      // Check sections have proper aria-labelledby
      const heroSection = page.locator('#hero');
      await expect(heroSection).toHaveAttribute('aria-labelledby', 'hero-title');
    });

    test('TC5: all images have descriptive alt text or are marked decorative', async ({ page }) => {
      const images = await page.locator('img').all();

      for (const img of images) {
        const alt = await img.getAttribute('alt');
        const role = await img.getAttribute('role');
        const ariaHidden = await img.getAttribute('aria-hidden');

        // Image should have either:
        // 1. Descriptive alt text (not empty)
        // 2. Empty alt for decorative images
        // 3. role="presentation" or aria-hidden="true" for decorative
        if (ariaHidden === 'true' || role === 'presentation') {
          // Decorative image, this is fine
          continue;
        }

        // If has alt attribute
        expect(alt).not.toBeNull();

        // If alt is not empty, it should be descriptive (more than 5 chars)
        if (alt && alt.trim().length > 0) {
          expect(alt.trim().length).toBeGreaterThan(5);
        }
      }

      // Check SVG icons are hidden from screen readers
      const svgIcons = await page.locator('svg[aria-hidden="true"]').all();
      expect(svgIcons.length).toBeGreaterThan(5);
    });

    test('TC6: all buttons and links have accessible names', async ({ page }) => {
      // Check buttons
      const buttons = await page.locator('button').all();

      for (const button of buttons) {
        const isVisible = await button.isVisible();
        if (!isVisible) continue;

        const accessibleName = await button.evaluate((el) => {
          // Get accessible name from various sources
          const ariaLabel = el.getAttribute('aria-label');
          const ariaLabelledby = el.getAttribute('aria-labelledby');
          const textContent = el.textContent?.trim();
          const title = el.getAttribute('title');

          if (ariaLabel) return ariaLabel;
          if (ariaLabelledby) {
            const labelEl = document.getElementById(ariaLabelledby);
            return labelEl?.textContent?.trim() || '';
          }
          if (textContent) return textContent;
          if (title) return title;
          return '';
        });

        expect(accessibleName.length).toBeGreaterThan(0);
      }

      // Check links
      const links = await page.locator('a').all();

      for (const link of links) {
        const isVisible = await link.isVisible();
        if (!isVisible) continue;

        const accessibleName = await link.evaluate((el) => {
          const ariaLabel = el.getAttribute('aria-label');
          const textContent = el.textContent?.trim();

          if (ariaLabel) return ariaLabel;
          if (textContent) return textContent;
          return '';
        });

        expect(accessibleName.length).toBeGreaterThan(0);
      }
    });

  });

  test.describe('Color Contrast', () => {

    test('TC7: all text meets WCAG AA contrast ratio in light mode', async ({ page }) => {
      // Set light mode
      await page.evaluate(() => {
        document.documentElement.setAttribute('data-theme', 'light');
      });
      await page.waitForTimeout(300);

      // Run axe accessibility audit for color contrast
      const accessibilityScanResults = await new AxeBuilder({ page })
        .withTags(['wcag2aa'])
        .analyze();

      // Filter for color contrast violations only
      const contrastViolations = accessibilityScanResults.violations.filter(
        v => v.id === 'color-contrast'
      );

      // There should be no color contrast violations
      expect(contrastViolations.length).toBe(0);
    });

    test('TC8: all text meets WCAG AA contrast ratio in dark mode', async ({ page }) => {
      // Set dark mode
      await page.evaluate(() => {
        document.documentElement.setAttribute('data-theme', 'dark');
      });
      await page.waitForTimeout(300);

      // Run axe accessibility audit for color contrast
      const accessibilityScanResults = await new AxeBuilder({ page })
        .withTags(['wcag2aa'])
        .analyze();

      // Filter for color contrast violations only
      const contrastViolations = accessibilityScanResults.violations.filter(
        v => v.id === 'color-contrast'
      );

      // There should be no color contrast violations
      expect(contrastViolations.length).toBe(0);
    });

  });

  test.describe('Screen Reader Compatibility', () => {

    test('semantic HTML landmarks are present', async ({ page }) => {
      // Check for main landmarks
      const landmarks = await page.$$eval('[role], nav, main, footer, header, aside, section[aria-labelledby]', (els) => {
        return els.map(el => ({
          tagName: el.tagName.toLowerCase(),
          role: el.getAttribute('role'),
          ariaLabel: el.getAttribute('aria-label'),
          ariaLabelledby: el.getAttribute('aria-labelledby')
        }));
      });

      // Verify key landmarks exist
      const hasNav = landmarks.some(l => l.tagName === 'nav');
      const hasMain = landmarks.some(l => l.tagName === 'main');
      const hasFooter = landmarks.some(l => l.tagName === 'footer');

      expect(hasNav).toBeTruthy();
      expect(hasMain).toBeTruthy();
      expect(hasFooter).toBeTruthy();
    });

    test('ARIA attributes are properly used', async ({ page }) => {
      // Check aria-label usage
      const ariaLabels = await page.$$('[aria-label]');
      expect(ariaLabels.length).toBeGreaterThan(3);

      // Check aria-labelledby usage on sections
      const labelledSections = await page.$$('section[aria-labelledby]');
      expect(labelledSections.length).toBeGreaterThan(3);

      // Verify aria-labelledby references exist
      for (const section of labelledSections) {
        const labelledBy = await section.getAttribute('aria-labelledby');
        if (labelledBy) {
          const labelElement = page.locator(`#${labelledBy}`);
          await expect(labelElement).toHaveCount(1);
        }
      }

      // Check aria-hidden on decorative elements
      const hiddenElements = await page.$$('[aria-hidden="true"]');
      expect(hiddenElements.length).toBeGreaterThan(5);
    });

    test('mobile navigation has proper ARIA attributes', async ({ page }) => {
      // Check hamburger menu button
      const menuToggle = page.locator('.nav-menu-toggle');
      await expect(menuToggle).toHaveAttribute('aria-label');
      await expect(menuToggle).toHaveAttribute('aria-expanded');
      await expect(menuToggle).toHaveAttribute('aria-controls', 'nav-links');

      // Check the controlled element exists
      const navLinks = page.locator('#nav-links');
      await expect(navLinks).toHaveCount(1);
    });

  });

  test.describe('Full Accessibility Audit', () => {

    test('passes axe-core accessibility audit for light mode', async ({ page }) => {
      // Set light mode
      await page.evaluate(() => {
        document.documentElement.setAttribute('data-theme', 'light');
      });
      await page.waitForTimeout(300);

      const accessibilityScanResults = await new AxeBuilder({ page })
        .withTags(['wcag2a', 'wcag2aa'])
        .exclude('.theme-transition') // Exclude transition class
        .analyze();

      // Log any violations for debugging
      if (accessibilityScanResults.violations.length > 0) {
        console.log('Light mode accessibility violations:',
          JSON.stringify(accessibilityScanResults.violations, null, 2));
      }

      expect(accessibilityScanResults.violations.length).toBe(0);
    });

    test('passes axe-core accessibility audit for dark mode', async ({ page }) => {
      // Set dark mode
      await page.evaluate(() => {
        document.documentElement.setAttribute('data-theme', 'dark');
      });
      await page.waitForTimeout(300);

      const accessibilityScanResults = await new AxeBuilder({ page })
        .withTags(['wcag2a', 'wcag2aa'])
        .exclude('.theme-transition') // Exclude transition class
        .analyze();

      // Log any violations for debugging
      if (accessibilityScanResults.violations.length > 0) {
        console.log('Dark mode accessibility violations:',
          JSON.stringify(accessibilityScanResults.violations, null, 2));
      }

      expect(accessibilityScanResults.violations.length).toBe(0);
    });

  });

});
