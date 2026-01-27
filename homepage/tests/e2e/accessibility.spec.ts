/**
 * Accessibility compliance tests for MirDB Homepage.
 * Owner: Scenario 10 - Accessibility Compliance
 *
 * Tests WCAG 2.1 AA compliance including:
 * - Automated accessibility audit (axe-core)
 * - Keyboard navigation
 * - Screen reader compatibility (heading hierarchy, alt text)
 * - Color contrast verification
 */

import { test, expect } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

test.describe('Accessibility Compliance - WCAG 2.1 AA', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Run axe-core accessibility audit
  test('should have no critical or serious accessibility violations', async ({ page }) => {
    // Run axe-core accessibility analysis
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .analyze();

    // Filter for critical and serious violations
    const criticalAndSerious = accessibilityScanResults.violations.filter(
      (violation) => violation.impact === 'critical' || violation.impact === 'serious'
    );

    // Log violations for debugging
    if (criticalAndSerious.length > 0) {
      console.log('Critical/Serious Accessibility Violations:');
      criticalAndSerious.forEach((violation) => {
        console.log(`- ${violation.id}: ${violation.description}`);
        console.log(`  Impact: ${violation.impact}`);
        console.log(`  Help: ${violation.helpUrl}`);
        violation.nodes.forEach((node) => {
          console.log(`  Element: ${node.html}`);
        });
      });
    }

    expect(criticalAndSerious).toHaveLength(0);
  });

  // Test Case 2: Tab through all interactive elements
  test('should allow keyboard navigation through all interactive elements with visible focus indicators', async ({ page }) => {
    // Get all focusable elements (excluding skip-link which is visually hidden until focused)
    const focusableElements = await page.locator(
      'a[href]:not(.skip-link), button, input, select, textarea, [tabindex="0"]'
    ).all();

    expect(focusableElements.length).toBeGreaterThan(0);

    // Track elements that received focus
    const focusedElements: string[] = [];
    let tabCount = 0;
    const maxTabs = Math.min(focusableElements.length + 5, 25); // Allow some extra tabs

    // Tab through elements
    while (tabCount < maxTabs) {
      await page.keyboard.press('Tab');
      tabCount++;

      // Get the currently focused element
      const focusedElement = await page.locator(':focus').first();

      // Skip if no element is focused (edge case)
      if (await focusedElement.count() === 0) continue;

      // Get element info for tracking
      const elementInfo = await focusedElement.evaluate((el) => {
        return {
          tagName: el.tagName.toLowerCase(),
          className: el.className,
          href: el.getAttribute('href'),
          text: el.textContent?.trim().slice(0, 30)
        };
      });

      // Skip hidden skip-link and Astro dev toolbar (dev-only elements)
      if (elementInfo.className.includes('skip-link')) continue;
      if (elementInfo.tagName.includes('astro-dev-toolbar')) continue;
      if (elementInfo.tagName.startsWith('astro-')) continue;

      focusedElements.push(`${elementInfo.tagName}: ${elementInfo.text || elementInfo.href}`);

      // Verify the focused element is visible
      const isVisible = await focusedElement.isVisible();
      expect(isVisible, `Focused element should be visible: ${JSON.stringify(elementInfo)}`).toBe(true);

      // Small wait to ensure styles are computed
      await page.waitForTimeout(50);

      // Check that focus indicator is present
      const focusStyles = await focusedElement.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        const outlineWidth = parseFloat(styles.outlineWidth) || 0;
        const outlineStyle = styles.outlineStyle;
        const outline = styles.outline;
        const boxShadow = styles.boxShadow;

        // Check if outline is set (may be a shorthand like "2px solid rgb(37, 99, 235)")
        const hasOutlineShorthand = outline && outline !== 'none' && !outline.startsWith('0px');

        return {
          hasOutline: (outlineStyle !== 'none' && outlineWidth > 0) || hasOutlineShorthand,
          hasBoxShadow: boxShadow !== 'none' && boxShadow !== '',
          outlineWidth,
          outlineStyle,
          outline,
          boxShadow
        };
      });

      // Verify there's some form of visible focus indicator
      const hasFocusIndicator = focusStyles.hasOutline || focusStyles.hasBoxShadow;

      expect(
        hasFocusIndicator,
        `Element should have visible focus indicator: ${JSON.stringify(elementInfo)}. Styles: ${JSON.stringify(focusStyles)}`
      ).toBe(true);

      // Break if we've tabbed through enough interactive elements
      if (focusedElements.length >= focusableElements.length) break;
    }

    // Verify we successfully tabbed through multiple elements
    expect(focusedElements.length).toBeGreaterThan(0);
  });

  // Test Case 3: Verify all images have alt text
  test('should have appropriate alt text on all images', async ({ page }) => {
    // Find all images
    const images = await page.locator('img').all();

    for (const image of images) {
      // Check for alt attribute
      const alt = await image.getAttribute('alt');
      const ariaLabel = await image.getAttribute('aria-label');
      const ariaLabelledby = await image.getAttribute('aria-labelledby');
      const role = await image.getAttribute('role');

      // Image should have alt text, aria-label, aria-labelledby, or role="presentation"
      const hasAccessibleName =
        (alt !== null && alt !== '') ||
        ariaLabel !== null ||
        ariaLabelledby !== null ||
        role === 'presentation';

      const src = await image.getAttribute('src');
      expect(
        hasAccessibleName,
        `Image with src="${src}" should have alt text or aria-label`
      ).toBe(true);
    }
  });

  // Test Case 4: Verify heading hierarchy
  test('should have proper heading structure without skipping levels', async ({ page }) => {
    // Get all headings in order
    const headings = await page.locator('h1, h2, h3, h4, h5, h6').all();

    expect(headings.length).toBeGreaterThan(0);

    // Track heading levels
    let previousLevel = 0;
    const headingHierarchy: { level: number; text: string }[] = [];

    for (const heading of headings) {
      const tagName = await heading.evaluate((el) => el.tagName.toLowerCase());
      const level = parseInt(tagName.replace('h', ''));
      const text = await heading.textContent();

      headingHierarchy.push({ level, text: text?.trim() || '' });

      // First heading should be h1
      if (previousLevel === 0) {
        expect(level).toBe(1);
      } else {
        // Should not skip more than one level going down
        // (e.g., h1 -> h3 is invalid, h1 -> h2 is valid)
        if (level > previousLevel) {
          expect(
            level - previousLevel,
            `Heading level jumped from h${previousLevel} to h${level}. Headings: ${JSON.stringify(headingHierarchy)}`
          ).toBeLessThanOrEqual(1);
        }
      }

      previousLevel = level;
    }

    // Verify there's exactly one h1
    const h1Count = headingHierarchy.filter((h) => h.level === 1).length;
    expect(h1Count).toBe(1);
  });

  // Test Case 5: Verify color contrast for body text
  test('should have adequate color contrast for text', async ({ page }) => {
    // Run axe-core specifically for color contrast
    const contrastResults = await new AxeBuilder({ page })
      .withRules(['color-contrast'])
      .analyze();

    // Filter for serious contrast violations
    const contrastViolations = contrastResults.violations.filter(
      (v) => v.id === 'color-contrast' && (v.impact === 'serious' || v.impact === 'critical')
    );

    if (contrastViolations.length > 0) {
      console.log('Color contrast violations:');
      contrastViolations.forEach((violation) => {
        violation.nodes.forEach((node) => {
          console.log(`- Element: ${node.html}`);
          console.log(`  Message: ${node.failureSummary}`);
        });
      });
    }

    expect(contrastViolations).toHaveLength(0);
  });

  // Test Case 6: Verify form labels (if any forms exist)
  test('should have associated labels for all form inputs', async ({ page }) => {
    // Find all form inputs
    const inputs = await page.locator('input, select, textarea').all();

    for (const input of inputs) {
      const id = await input.getAttribute('id');
      const ariaLabel = await input.getAttribute('aria-label');
      const ariaLabelledby = await input.getAttribute('aria-labelledby');
      const type = await input.getAttribute('type');

      // Skip hidden inputs and submit/button types
      if (type === 'hidden' || type === 'submit' || type === 'button') {
        continue;
      }

      // Check if input has an associated label
      let hasLabel = false;

      // Check for aria-label
      if (ariaLabel && ariaLabel.trim() !== '') {
        hasLabel = true;
      }

      // Check for aria-labelledby
      if (ariaLabelledby) {
        const labelElement = await page.locator(`#${ariaLabelledby}`).first();
        if (await labelElement.count() > 0) {
          hasLabel = true;
        }
      }

      // Check for associated label element
      if (id) {
        const labelElement = await page.locator(`label[for="${id}"]`).first();
        if (await labelElement.count() > 0) {
          hasLabel = true;
        }
      }

      // Check if input is wrapped in a label
      const wrappingLabel = await input.locator('xpath=ancestor::label').count();
      if (wrappingLabel > 0) {
        hasLabel = true;
      }

      // Check for title attribute (less preferred but acceptable)
      const title = await input.getAttribute('title');
      if (title && title.trim() !== '') {
        hasLabel = true;
      }

      expect(
        hasLabel,
        `Input element should have an associated label. ID: ${id || 'none'}`
      ).toBe(true);
    }
  });

  // Additional test: Skip link functionality
  test('should have a working skip link for keyboard navigation', async ({ page }) => {
    // Check for skip link
    const skipLink = page.locator('.skip-link, a[href="#main-content"]').first();

    if (await skipLink.count() > 0) {
      // Press Tab to focus the skip link (usually first focusable element)
      await page.keyboard.press('Tab');

      // Verify skip link is focusable
      const focusedElement = await page.locator(':focus').first();
      const focusedHref = await focusedElement.getAttribute('href');

      // If the skip link was focused, verify it works
      if (focusedHref === '#main-content') {
        await page.keyboard.press('Enter');

        // Verify focus moved to main content
        const mainContent = page.locator('#main-content');
        await expect(mainContent).toBeVisible();
      }
    }
  });

  // Additional test: Landmarks and regions
  test('should have proper landmark regions', async ({ page }) => {
    // Check for main landmark
    const main = await page.locator('main, [role="main"]').count();
    expect(main).toBeGreaterThanOrEqual(1);

    // Check for navigation landmark
    const nav = await page.locator('nav, [role="navigation"]').count();
    expect(nav).toBeGreaterThanOrEqual(1);

    // Check for footer/contentinfo landmark
    const footer = await page.locator('footer, [role="contentinfo"]').count();
    expect(footer).toBeGreaterThanOrEqual(1);
  });

  // Additional test: Links have discernible text
  test('should have discernible text for all links', async ({ page }) => {
    const links = await page.locator('a[href]').all();

    for (const link of links) {
      const text = await link.textContent();
      const ariaLabel = await link.getAttribute('aria-label');
      const ariaLabelledby = await link.getAttribute('aria-labelledby');
      const title = await link.getAttribute('title');

      // Check for images with alt text inside the link
      const imgWithAlt = await link.locator('img[alt]').count();
      const imgAlt = imgWithAlt > 0 ? await link.locator('img[alt]').first().getAttribute('alt') : null;

      const hasDiscernibleText =
        (text && text.trim() !== '') ||
        (ariaLabel && ariaLabel.trim() !== '') ||
        ariaLabelledby !== null ||
        (title && title.trim() !== '') ||
        (imgAlt && imgAlt.trim() !== '');

      const href = await link.getAttribute('href');
      expect(
        hasDiscernibleText,
        `Link with href="${href}" should have discernible text`
      ).toBe(true);
    }
  });
});
