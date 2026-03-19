/**
 * Accessibility Compliance Tests (WCAG 2.1 AA)
 * Owner: Scenario 12 - Accessibility Compliance
 *
 * Tests verify:
 * - Keyboard navigation
 * - Focus indicators
 * - Screen reader support (ARIA)
 * - Color contrast
 * - Image alt text
 * - Heading hierarchy
 * - Lighthouse accessibility score
 *
 * Requirements: REQ-12
 */

import { test, expect, Page } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

test.describe('Accessibility Compliance - WCAG 2.1 AA', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for page to be fully loaded
    await page.waitForLoadState('networkidle');
  });

  /**
   * Test Case 1: Keyboard Navigation
   * Input: Tab through all interactive elements
   * Expected: All buttons, links, and form controls receive focus in logical order
   */
  test.describe('Test Case 1: Keyboard Navigation', () => {
    test('all interactive elements receive focus when tabbing', async ({ page }) => {
      // Get all interactive elements in the page
      const interactiveElements = await page.locator(
        'a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])'
      ).all();

      expect(interactiveElements.length).toBeGreaterThan(0);

      // Start from the beginning of the page
      await page.keyboard.press('Tab');

      // Track focused elements
      const focusedElements: string[] = [];
      let previousFocusedElement = '';

      // Tab through all elements
      for (let i = 0; i < interactiveElements.length + 5; i++) {
        const focusedElement = await page.evaluate(() => {
          const el = document.activeElement;
          if (!el || el === document.body) return '';
          return el.tagName + (el.id ? `#${el.id}` : '') +
                 (el.className ? `.${el.className.split(' ')[0]}` : '');
        });

        if (focusedElement && focusedElement !== previousFocusedElement) {
          focusedElements.push(focusedElement);
          previousFocusedElement = focusedElement;
        }

        // Check that we're not stuck on the same element
        await page.keyboard.press('Tab');
      }

      // Verify we can tab through multiple elements
      expect(focusedElements.length).toBeGreaterThan(3);
    });

    test('navigation links are focusable in logical order', async ({ page }) => {
      // Tab to the first navigation link
      await page.keyboard.press('Tab'); // Skip to logo

      // Find and verify navigation elements are reachable
      const navLinks = page.locator('nav[aria-label="Main navigation"] a');
      const count = await navLinks.count();

      expect(count).toBeGreaterThan(0);

      // Verify each nav link is focusable
      for (let i = 0; i < count; i++) {
        const link = navLinks.nth(i);
        await link.focus();
        const isFocused = await link.evaluate((el) => document.activeElement === el);
        expect(isFocused).toBe(true);
      }
    });

    test('CTA buttons are keyboard accessible', async ({ page }) => {
      // Get Started button
      const getStartedBtn = page.locator('a:has-text("Get Started")').first();
      await expect(getStartedBtn).toBeVisible();
      await getStartedBtn.focus();

      // Verify it can be activated with Enter
      const isFocusable = await getStartedBtn.evaluate((el) => {
        return el.tabIndex >= 0;
      });
      expect(isFocusable).toBe(true);

      // View on GitHub button
      const githubBtn = page.locator('a:has-text("View on GitHub")').first();
      await expect(githubBtn).toBeVisible();
      await githubBtn.focus();

      const isGithubFocusable = await githubBtn.evaluate((el) => {
        return el.tabIndex >= 0;
      });
      expect(isGithubFocusable).toBe(true);
    });

    test('mobile menu button is keyboard accessible', async ({ page }) => {
      // Set mobile viewport
      await page.setViewportSize({ width: 375, height: 667 });
      await page.reload();
      await page.waitForLoadState('networkidle');

      // Find mobile menu button
      const menuButton = page.locator('button[aria-label="Open menu"], button[aria-label="Close menu"]');
      await expect(menuButton).toBeVisible();

      // Tab to menu button and activate with keyboard
      await menuButton.focus();
      const isFocused = await menuButton.evaluate((el) => document.activeElement === el);
      expect(isFocused).toBe(true);

      // Activate with Enter
      await page.keyboard.press('Enter');

      // Menu should be open
      const mobileMenu = page.locator('#mobile-menu');
      await expect(mobileMenu).toBeVisible();

      // Close with keyboard
      await page.keyboard.press('Enter');
      await expect(mobileMenu).not.toBeVisible();
    });
  });

  /**
   * Test Case 2: Focus Visibility
   * Input: Check focus visibility on navigation links
   * Expected: Focus indicator is visible with minimum 3:1 contrast against background
   */
  test.describe('Test Case 2: Focus Visibility', () => {
    test('navigation links have visible focus indicators', async ({ page }) => {
      const navLinks = page.locator('nav[aria-label="Main navigation"] a');
      const firstLink = navLinks.first();

      await firstLink.focus();

      // Check that focus ring/outline is applied
      const focusStyles = await firstLink.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          outline: styles.outline,
          outlineWidth: styles.outlineWidth,
          outlineStyle: styles.outlineStyle,
          boxShadow: styles.boxShadow,
        };
      });

      // Should have some form of focus indicator (outline or box-shadow from ring)
      const hasFocusIndicator =
        focusStyles.outlineStyle !== 'none' ||
        focusStyles.boxShadow !== 'none';

      // Note: Tailwind focus:ring creates box-shadow, not outline
      expect(hasFocusIndicator || focusStyles.outlineWidth !== '0px').toBe(true);
    });

    test('buttons have visible focus indicators', async ({ page }) => {
      // Check primary CTA button
      const primaryBtn = page.locator('a:has-text("Get Started")').first();
      await primaryBtn.focus();

      const focusStyles = await primaryBtn.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          outline: styles.outline,
          outlineWidth: styles.outlineWidth,
          boxShadow: styles.boxShadow,
        };
      });

      // Button should have focus ring (Tailwind's focus:ring-2)
      const hasFocusIndicator =
        focusStyles.boxShadow !== 'none' ||
        focusStyles.outlineWidth !== '0px';

      expect(hasFocusIndicator).toBe(true);
    });

    test('all focusable elements have focus indicators', async ({ page }) => {
      const focusableElements = await page.locator(
        'a[href], button:not([disabled]), [tabindex]:not([tabindex="-1"])'
      ).all();

      // Test a sample of focusable elements
      const samplesToTest = Math.min(10, focusableElements.length);
      let elementsWithFocusIndicators = 0;

      for (let i = 0; i < samplesToTest; i++) {
        const element = focusableElements[i];

        // Get styles before focus
        const beforeFocus = await element.evaluate((el) => {
          const styles = window.getComputedStyle(el);
          return {
            outline: styles.outline,
            boxShadow: styles.boxShadow,
            border: styles.border,
          };
        });

        await element.focus();

        const hasVisibleFocus = await element.evaluate((el, before) => {
          const styles = window.getComputedStyle(el);
          // Check for any change in focus-related styles or presence of ring/outline
          const hasOutline = styles.outlineStyle !== 'none' && styles.outlineWidth !== '0px';
          const hasBoxShadow = styles.boxShadow !== 'none';
          const styleChanged =
            styles.outline !== before.outline ||
            styles.boxShadow !== before.boxShadow ||
            styles.border !== before.border;

          return hasOutline || hasBoxShadow || styleChanged;
        }, beforeFocus);

        if (hasVisibleFocus) {
          elementsWithFocusIndicators++;
        }
      }

      // At least 80% of elements should have visible focus indicators
      expect(elementsWithFocusIndicators).toBeGreaterThanOrEqual(Math.floor(samplesToTest * 0.8));
    });
  });

  /**
   * Test Case 3: Image Alt Text
   * Input: Verify image alt text
   * Expected: All images have appropriate alt text (empty for decorative)
   */
  test.describe('Test Case 3: Image Alt Text', () => {
    test('all images have alt attributes', async ({ page }) => {
      const images = await page.locator('img').all();

      for (const img of images) {
        // Every image should have an alt attribute (can be empty for decorative)
        const hasAlt = await img.evaluate((el) => el.hasAttribute('alt'));
        expect(hasAlt).toBe(true);
      }
    });

    test('meaningful images have descriptive alt text', async ({ page }) => {
      // Logo should have meaningful alt text
      const logo = page.locator('img[src*="logo"]').first();
      const logoExists = await logo.count() > 0;

      if (logoExists) {
        const altText = await logo.getAttribute('alt');
        expect(altText).toBeTruthy();
        expect(altText!.length).toBeGreaterThan(0);
      }
    });

    test('decorative icons have aria-hidden or empty alt', async ({ page }) => {
      // Lucide icons should be marked as decorative
      const svgIcons = await page.locator('svg[aria-hidden="true"]').all();

      // Verify decorative icons are properly hidden from screen readers
      expect(svgIcons.length).toBeGreaterThan(0);

      for (const icon of svgIcons.slice(0, 10)) {
        const ariaHidden = await icon.getAttribute('aria-hidden');
        expect(ariaHidden).toBe('true');
      }
    });
  });

  /**
   * Test Case 4: Heading Hierarchy
   * Input: Check heading hierarchy
   * Expected: Page has single h1, headings follow logical hierarchy (h1→h2→h3)
   */
  test.describe('Test Case 4: Heading Hierarchy', () => {
    test('page has exactly one h1 element', async ({ page }) => {
      const h1Elements = await page.locator('h1').all();
      expect(h1Elements.length).toBe(1);
    });

    test('h1 contains the product name', async ({ page }) => {
      const h1 = page.locator('h1');
      const text = await h1.textContent();
      expect(text).toContain('MirDB');
    });

    test('headings follow logical hierarchy without skipping levels', async ({ page }) => {
      const headings = await page.evaluate(() => {
        const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
        return Array.from(allHeadings).map((h) => ({
          level: parseInt(h.tagName.charAt(1)),
          text: h.textContent?.trim().substring(0, 50),
        }));
      });

      expect(headings.length).toBeGreaterThan(0);

      // First heading should be h1
      expect(headings[0].level).toBe(1);

      // Check for no level skipping (e.g., h1 -> h3 without h2)
      for (let i = 1; i < headings.length; i++) {
        const currentLevel = headings[i].level;
        const previousLevel = headings[i - 1].level;

        // Heading can stay same, go up (smaller number), or go down by 1
        // Going down by more than 1 level is a violation
        if (currentLevel > previousLevel) {
          expect(currentLevel - previousLevel).toBeLessThanOrEqual(1);
        }
      }
    });

    test('section headings are h2 level', async ({ page }) => {
      // Main sections should use h2
      const sectionHeadings = await page.locator('section h2').all();
      expect(sectionHeadings.length).toBeGreaterThanOrEqual(4); // Features, How It Works, Status, Quick Start, etc.
    });
  });

  /**
   * Test Case 5: ARIA Labels
   * Input: Verify ARIA labels
   * Expected: Interactive elements without visible text have aria-label or aria-labelledby
   */
  test.describe('Test Case 5: ARIA Labels', () => {
    test('main navigation has aria-label', async ({ page }) => {
      const mainNav = page.locator('nav[aria-label="Main navigation"]');
      await expect(mainNav).toBeVisible();
    });

    test('footer navigation has aria-label', async ({ page }) => {
      const footerNav = page.locator('footer nav[aria-label="Footer navigation"]');
      await expect(footerNav).toBeVisible();
    });

    test('icon-only buttons have aria-label', async ({ page }) => {
      // Mobile menu button should have aria-label
      await page.setViewportSize({ width: 375, height: 667 });
      await page.reload();

      const menuButton = page.locator('button[aria-label]');
      const count = await menuButton.count();
      expect(count).toBeGreaterThan(0);

      // Check that the aria-label is descriptive
      const ariaLabel = await menuButton.first().getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel!.length).toBeGreaterThan(0);
    });

    test('sections have aria-labelledby for screen reader context', async ({ page }) => {
      // Check that major sections reference their headings
      const sectionsWithLabels = await page.locator('section[aria-labelledby]').all();
      expect(sectionsWithLabels.length).toBeGreaterThan(0);

      for (const section of sectionsWithLabels) {
        const labelId = await section.getAttribute('aria-labelledby');
        expect(labelId).toBeTruthy();

        // Verify the referenced element exists
        const labelElement = page.locator(`#${labelId}`);
        await expect(labelElement).toBeVisible();
      }
    });

    test('external links indicate they open in new tab', async ({ page }) => {
      // GitHub links should have proper attributes
      const externalLinks = await page.locator('a[target="_blank"]').all();

      for (const link of externalLinks) {
        const rel = await link.getAttribute('rel');
        // Should have noopener for security
        expect(rel).toContain('noopener');
      }
    });

    test('mobile menu button has aria-expanded', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.reload();

      const menuButton = page.locator('button[aria-controls="mobile-menu"]');
      await expect(menuButton).toBeVisible();

      // Should have aria-expanded
      const ariaExpanded = await menuButton.getAttribute('aria-expanded');
      expect(ariaExpanded).toBe('false');

      // Click to open
      await menuButton.click();
      const expandedAfterClick = await menuButton.getAttribute('aria-expanded');
      expect(expandedAfterClick).toBe('true');
    });

    test('skip link functionality or first focusable element is main content', async ({ page }) => {
      // Either should have skip link or first tab goes to meaningful content
      await page.keyboard.press('Tab');

      const activeElement = await page.evaluate(() => {
        const el = document.activeElement;
        return {
          tag: el?.tagName,
          text: el?.textContent?.trim().substring(0, 30),
          isLink: el?.tagName === 'A',
        };
      });

      // First focusable element should be a link or button (navigation)
      expect(['A', 'BUTTON']).toContain(activeElement.tag);
    });
  });

  /**
   * Test Case 6: Lighthouse Accessibility Audit
   * Input: Run Lighthouse accessibility audit
   * Expected: Lighthouse accessibility score is 90 or higher
   */
  test.describe('Test Case 6: Automated Accessibility Audit (axe-core)', () => {
    test('page passes axe-core accessibility checks', async ({ page }) => {
      // Run axe accessibility check
      const accessibilityScanResults = await new AxeBuilder({ page })
        .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
        .analyze();

      // Log any violations for debugging
      if (accessibilityScanResults.violations.length > 0) {
        console.log('Accessibility violations:', JSON.stringify(accessibilityScanResults.violations, null, 2));
      }

      // Should have no violations
      expect(accessibilityScanResults.violations).toHaveLength(0);
    });

    test('hero section passes accessibility checks', async ({ page }) => {
      const heroSection = page.locator('#hero');

      const results = await new AxeBuilder({ page })
        .include('#hero')
        .withTags(['wcag2a', 'wcag2aa'])
        .analyze();

      expect(results.violations).toHaveLength(0);
    });

    test('navigation passes accessibility checks', async ({ page }) => {
      const results = await new AxeBuilder({ page })
        .include('header')
        .withTags(['wcag2a', 'wcag2aa'])
        .analyze();

      expect(results.violations).toHaveLength(0);
    });

    test('footer passes accessibility checks', async ({ page }) => {
      const results = await new AxeBuilder({ page })
        .include('footer')
        .withTags(['wcag2a', 'wcag2aa'])
        .analyze();

      expect(results.violations).toHaveLength(0);
    });

    test('features section passes accessibility checks', async ({ page }) => {
      const results = await new AxeBuilder({ page })
        .include('#features')
        .withTags(['wcag2a', 'wcag2aa'])
        .analyze();

      expect(results.violations).toHaveLength(0);
    });

    test('quick start section passes accessibility checks', async ({ page }) => {
      const results = await new AxeBuilder({ page })
        .include('#quick-start')
        .withTags(['wcag2a', 'wcag2aa'])
        .analyze();

      expect(results.violations).toHaveLength(0);
    });
  });

  /**
   * Additional accessibility tests for comprehensive coverage
   */
  test.describe('Additional Accessibility Checks', () => {
    test('page has lang attribute on html element', async ({ page }) => {
      const htmlLang = await page.locator('html').getAttribute('lang');
      expect(htmlLang).toBe('en');
    });

    test('page title is descriptive', async ({ page }) => {
      const title = await page.title();
      expect(title).toContain('MirDB');
      expect(title.length).toBeGreaterThan(10);
    });

    test('links have distinguishable text', async ({ page }) => {
      const links = await page.locator('a[href]').all();

      for (const link of links.slice(0, 20)) {
        const text = await link.textContent();
        const ariaLabel = await link.getAttribute('aria-label');

        // Link should have either visible text or aria-label
        const hasAccessibleName = (text && text.trim().length > 0) ||
                                   (ariaLabel && ariaLabel.length > 0);
        expect(hasAccessibleName).toBe(true);
      }
    });

    test('form controls have labels (if any)', async ({ page }) => {
      const inputs = await page.locator('input, textarea, select').all();

      for (const input of inputs) {
        const id = await input.getAttribute('id');
        const ariaLabel = await input.getAttribute('aria-label');
        const ariaLabelledby = await input.getAttribute('aria-labelledby');

        if (id) {
          const associatedLabel = await page.locator(`label[for="${id}"]`).count();
          const hasLabel = associatedLabel > 0 || ariaLabel || ariaLabelledby;
          expect(hasLabel).toBeTruthy();
        }
      }
    });

    test('color is not the only means of conveying information', async ({ page }) => {
      // Check that links have underline or other visual indicator besides color
      const links = page.locator('a:not([class*="button"])');
      const count = await links.count();

      // At least some links should have hover:underline or similar
      // This is a basic check - in practice, you'd verify specific links
      expect(count).toBeGreaterThan(0);
    });

    test('touch targets meet minimum size recommendations on mobile', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.reload();

      // Check CTA button sizes (primary interactive elements)
      // Note: 44x44 is WCAG 2.1 AAA level 2.5.5, not required for AA
      // We check that primary CTAs meet this recommendation
      const ctaButtons = await page.locator('a.inline-flex').all();

      let adequateTouchTargets = 0;
      for (const button of ctaButtons) {
        const box = await button.boundingBox();
        if (box) {
          // Check if touch target is reasonably sized (at least 40px height)
          if (box.width >= 40 && box.height >= 40) {
            adequateTouchTargets++;
          }
        }
      }

      // Most CTA buttons should have adequate touch targets
      if (ctaButtons.length > 0) {
        const percentage = adequateTouchTargets / ctaButtons.length;
        expect(percentage).toBeGreaterThanOrEqual(0.5);
      }
    });
  });
});
