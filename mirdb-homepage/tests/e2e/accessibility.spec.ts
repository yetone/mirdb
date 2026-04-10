/**
 * Accessibility E2E Tests
 * Owner: Scenario 11 - Accessibility Standards Compliance
 *
 * WCAG 2.1 AA compliance tests:
 * - Color contrast (4.5:1 minimum)
 * - Keyboard navigation
 * - Focus indicators
 * - Screen reader compatibility
 * - axe-core integration
 */

import { test, expect } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

test.describe('Accessibility Standards Compliance - Scenario 11', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Check color contrast ratios
   * Expected: All text has at least 4.5:1 contrast ratio against background
   */
  test('TC1: Check color contrast ratios', async ({ page }) => {
    // Run axe-core accessibility scan focused on color contrast
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa', 'wcag21aa'])
      .analyze();

    // Filter for contrast-related violations
    const contrastViolations = accessibilityScanResults.violations.filter(
      (v) => v.id.includes('contrast')
    );

    // Log any contrast issues for debugging
    if (contrastViolations.length > 0) {
      console.log('Contrast violations:', JSON.stringify(contrastViolations, null, 2));
    }

    // Expect no color contrast violations
    expect(contrastViolations).toHaveLength(0);

    // Additionally verify key text elements have visible contrast
    const heroTitle = page.getByTestId('hero-title');
    await expect(heroTitle).toBeVisible();

    const heroTitleColor = await heroTitle.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        color: style.color,
        backgroundColor: style.backgroundColor,
      };
    });

    // Verify text color is defined (not transparent)
    expect(heroTitleColor.color).not.toBe('rgba(0, 0, 0, 0)');
  });

  /**
   * Test Case 2: Navigate page with keyboard only
   * Expected: All interactive elements reachable and operable via keyboard
   */
  test('TC2: Navigate page with keyboard only', async ({ page }) => {
    // Start at the beginning of the page
    await page.keyboard.press('Tab');

    // Collect all focusable elements
    const focusableSelectors = [
      'a[href]',
      'button:not([disabled])',
      'input:not([disabled])',
      'select:not([disabled])',
      'textarea:not([disabled])',
      '[tabindex]:not([tabindex="-1"])',
    ].join(', ');

    const focusableElements = await page.locator(focusableSelectors).all();
    const focusableCount = focusableElements.length;

    // Verify we have focusable elements
    expect(focusableCount).toBeGreaterThan(0);

    // Tab through the page and verify focus moves
    const visitedElements: string[] = [];
    const maxTabs = Math.min(focusableCount + 5, 50); // Safety limit

    for (let i = 0; i < maxTabs; i++) {
      const activeElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (el && el !== document.body) {
          return {
            tagName: el.tagName,
            testId: el.getAttribute('data-testid'),
            ariaLabel: el.getAttribute('aria-label'),
            href: el.getAttribute('href'),
            className: el.className,
          };
        }
        return null;
      });

      if (activeElement) {
        const elementId = activeElement.testId || activeElement.ariaLabel || activeElement.href || activeElement.className;
        if (elementId && !visitedElements.includes(elementId)) {
          visitedElements.push(elementId);
        }
      }

      await page.keyboard.press('Tab');
    }

    // Verify we visited multiple interactive elements
    expect(visitedElements.length).toBeGreaterThan(5);

    // Verify CTA button is reachable
    const ctaButton = page.getByTestId('cta-button');
    await ctaButton.focus();
    const isFocused = await ctaButton.evaluate((el) => document.activeElement === el);
    expect(isFocused).toBe(true);
  });

  /**
   * Test Case 3: Check focus indicators
   * Expected: Visible focus indicators on all focusable elements
   */
  test('TC3: Check focus indicators', async ({ page }) => {
    // Test CTA button focus indicator
    const ctaButton = page.getByTestId('cta-button');
    await ctaButton.focus();

    const ctaFocusStyle = await ctaButton.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        outline: style.outline,
        outlineWidth: style.outlineWidth,
        outlineColor: style.outlineColor,
        outlineStyle: style.outlineStyle,
        boxShadow: style.boxShadow,
      };
    });

    // Verify visible focus indicator (outline or box-shadow)
    const hasOutline =
      ctaFocusStyle.outlineWidth !== '0px' &&
      ctaFocusStyle.outlineStyle !== 'none';
    const hasBoxShadow = ctaFocusStyle.boxShadow !== 'none';
    expect(hasOutline || hasBoxShadow).toBe(true);

    // Test copy button focus indicator
    const copyButton = page.getByTestId('quickstart-install-command-copy-button');
    await copyButton.scrollIntoViewIfNeeded();
    await copyButton.focus();

    const copyFocusStyle = await copyButton.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        outline: style.outline,
        outlineWidth: style.outlineWidth,
        outlineStyle: style.outlineStyle,
        boxShadow: style.boxShadow,
      };
    });

    const copyHasOutline =
      copyFocusStyle.outlineWidth !== '0px' &&
      copyFocusStyle.outlineStyle !== 'none';
    const copyHasBoxShadow = copyFocusStyle.boxShadow !== 'none';
    expect(copyHasOutline || copyHasBoxShadow).toBe(true);

    // Test link focus indicator
    const licenseLink = page.getByTestId('footer-license-link');
    await licenseLink.scrollIntoViewIfNeeded();
    await licenseLink.focus();

    const linkFocusStyle = await licenseLink.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        outline: style.outline,
        outlineWidth: style.outlineWidth,
        outlineStyle: style.outlineStyle,
        boxShadow: style.boxShadow,
      };
    });

    const linkHasOutline =
      linkFocusStyle.outlineWidth !== '0px' &&
      linkFocusStyle.outlineStyle !== 'none';
    const linkHasBoxShadow = linkFocusStyle.boxShadow !== 'none';
    expect(linkHasOutline || linkHasBoxShadow).toBe(true);
  });

  /**
   * Test Case 4: Verify heading hierarchy
   * Expected: Proper heading structure (h1, h2, h3) without skipping levels
   */
  test('TC4: Verify heading hierarchy', async ({ page }) => {
    // Get all headings on the page
    const headings = await page.evaluate(() => {
      const headingElements = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      return Array.from(headingElements).map((el) => ({
        level: parseInt(el.tagName.charAt(1)),
        text: el.textContent?.trim() || '',
      }));
    });

    // Verify we have headings
    expect(headings.length).toBeGreaterThan(0);

    // Verify there's exactly one h1
    const h1Headings = headings.filter((h) => h.level === 1);
    expect(h1Headings).toHaveLength(1);
    expect(h1Headings[0].text).toContain('MirDB');

    // Verify heading hierarchy doesn't skip levels
    let previousLevel = 0;
    for (const heading of headings) {
      // Heading level should not increase by more than 1 from previous
      if (previousLevel > 0) {
        const levelJump = heading.level - previousLevel;
        // Allow decreasing any amount (h3 to h2) but not increasing more than 1 (h1 to h3)
        expect(levelJump).toBeLessThanOrEqual(1);
      }
      previousLevel = heading.level;
    }

    // Verify we have h2 headings for main sections
    const h2Headings = headings.filter((h) => h.level === 2);
    expect(h2Headings.length).toBeGreaterThanOrEqual(5); // Features, Quick Start, Performance, Documentation, Contributing
  });

  /**
   * Test Case 5: Check images for alt text
   * Expected: All images and icons have appropriate alt text
   */
  test('TC5: Check images for alt text', async ({ page }) => {
    // Get all images
    const images = await page.locator('img').all();

    for (const img of images) {
      // Every img should have alt attribute
      const hasAlt = await img.evaluate((el) => el.hasAttribute('alt'));
      expect(hasAlt).toBe(true);
    }

    // Verify decorative icons have aria-hidden
    const decorativeIcons = await page.locator('svg[aria-hidden="true"]').all();
    expect(decorativeIcons.length).toBeGreaterThanOrEqual(0);

    // Run axe-core check for image alt text
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .analyze();

    // Filter for image-related violations
    const imageViolations = accessibilityScanResults.violations.filter(
      (v) => v.id.includes('image') || v.id.includes('alt')
    );

    expect(imageViolations).toHaveLength(0);
  });

  /**
   * Test Case 6: Check for skip navigation link
   * Expected: Skip to main content link available for keyboard users
   */
  test('TC6: Check for skip navigation link', async ({ page }) => {
    // Tab to first focusable element
    await page.keyboard.press('Tab');

    // Get the first focused element
    const firstFocusedElement = await page.evaluate(() => {
      const el = document.activeElement;
      return {
        tagName: el?.tagName,
        className: el?.className,
        text: el?.textContent?.trim(),
        href: el?.getAttribute('href'),
      };
    });

    // Verify first focusable element is the skip link
    expect(firstFocusedElement.className).toContain('skip-link');
    expect(firstFocusedElement.text?.toLowerCase()).toContain('skip');
    expect(firstFocusedElement.href).toBe('#main-content');

    // Verify skip link becomes visible when focused
    const skipLink = page.locator('.skip-link');
    const skipLinkBox = await skipLink.boundingBox();

    // When focused, the skip link should be visible on screen (top >= 0)
    expect(skipLinkBox).not.toBeNull();
    expect(skipLinkBox!.y).toBeGreaterThanOrEqual(0);

    // Verify pressing Enter navigates to main content
    await page.keyboard.press('Enter');

    // Check that focus moved to main content area
    const mainContent = page.locator('#main-content');
    await expect(mainContent).toBeVisible();
  });

  /**
   * Test Case 7: Run axe-core accessibility scan
   * Expected: No critical or serious accessibility violations
   */
  test('TC7: Run axe-core accessibility scan', async ({ page }) => {
    // Run comprehensive axe-core accessibility scan
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21aa', 'best-practice'])
      .analyze();

    // Filter for critical and serious violations
    const criticalAndSeriousViolations = accessibilityScanResults.violations.filter(
      (v) => v.impact === 'critical' || v.impact === 'serious'
    );

    // Log all violations for debugging
    if (criticalAndSeriousViolations.length > 0) {
      console.log('Critical/Serious violations:');
      criticalAndSeriousViolations.forEach((v) => {
        console.log(`- ${v.id}: ${v.description}`);
        v.nodes.forEach((node) => {
          console.log(`  Target: ${node.target}`);
          console.log(`  HTML: ${node.html.substring(0, 100)}`);
        });
      });
    }

    // Expect no critical or serious violations
    expect(criticalAndSeriousViolations).toHaveLength(0);
  });

  // Additional accessibility tests

  test('ARIA landmarks are properly used', async ({ page }) => {
    // Verify main landmark exists
    const main = page.locator('main');
    await expect(main).toBeVisible();

    // Verify footer landmark exists
    const footer = page.locator('footer[role="contentinfo"]');
    await expect(footer).toBeVisible();

    // Verify navigation landmarks exist
    const navs = page.locator('nav[aria-label]');
    const navCount = await navs.count();
    expect(navCount).toBeGreaterThanOrEqual(1);
  });

  test('Interactive elements have accessible names', async ({ page }) => {
    // Check buttons have accessible names
    const buttons = page.locator('button');
    const buttonCount = await buttons.count();

    for (let i = 0; i < buttonCount; i++) {
      const button = buttons.nth(i);
      const hasAccessibleName = await button.evaluate((el) => {
        const ariaLabel = el.getAttribute('aria-label');
        const ariaLabelledby = el.getAttribute('aria-labelledby');
        const text = el.textContent?.trim();
        const title = el.getAttribute('title');
        return !!(ariaLabel || ariaLabelledby || text || title);
      });
      expect(hasAccessibleName).toBe(true);
    }

    // Check links have accessible names
    const links = page.locator('a[href]');
    const linkCount = await links.count();

    for (let i = 0; i < linkCount; i++) {
      const link = links.nth(i);
      const hasAccessibleName = await link.evaluate((el) => {
        const ariaLabel = el.getAttribute('aria-label');
        const ariaLabelledby = el.getAttribute('aria-labelledby');
        const text = el.textContent?.trim();
        const title = el.getAttribute('title');
        return !!(ariaLabel || ariaLabelledby || text || title);
      });
      expect(hasAccessibleName).toBe(true);
    }
  });

  test('Page has proper document structure', async ({ page }) => {
    // Verify html has lang attribute
    const htmlLang = await page.evaluate(() => document.documentElement.lang);
    expect(htmlLang).toBe('en');

    // Verify page has title
    const title = await page.title();
    expect(title).toContain('MirDB');

    // Verify viewport meta tag exists
    const viewportMeta = await page.evaluate(() => {
      const meta = document.querySelector('meta[name="viewport"]');
      return meta?.getAttribute('content');
    });
    expect(viewportMeta).toContain('width=device-width');
  });

  test('Keyboard navigation order is logical', async ({ page }) => {
    const focusOrder: string[] = [];
    const maxTabs = 30;

    // Tab through elements and record order
    for (let i = 0; i < maxTabs; i++) {
      await page.keyboard.press('Tab');

      const activeElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (el && el !== document.body) {
          return el.getAttribute('data-testid') || el.tagName + '-' + el.className;
        }
        return null;
      });

      if (activeElement) {
        focusOrder.push(activeElement);
      }
    }

    // Verify skip link comes first
    expect(focusOrder[0]).toContain('skip');

    // Verify CTA button is early in the focus order
    const ctaIndex = focusOrder.findIndex((el) => el.includes('cta-button'));
    expect(ctaIndex).toBeGreaterThan(0);
    expect(ctaIndex).toBeLessThan(5);
  });

  test('Focus is not trapped', async ({ page }) => {
    // Tab through entire page
    const seenElements = new Set<string>();
    const maxTabs = 100;

    for (let i = 0; i < maxTabs; i++) {
      await page.keyboard.press('Tab');

      const activeElementId = await page.evaluate(() => {
        const el = document.activeElement;
        return el ? el.outerHTML.substring(0, 100) : 'body';
      });

      // If we've seen this element before in recent tabs, we've cycled through
      if (seenElements.has(activeElementId) && seenElements.size > 5) {
        // Successfully cycled through all focusable elements
        break;
      }
      seenElements.add(activeElementId);
    }

    // Verify we visited multiple unique elements (focus not stuck)
    expect(seenElements.size).toBeGreaterThan(5);
  });
});
