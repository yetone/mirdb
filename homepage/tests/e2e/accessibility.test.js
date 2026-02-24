/**
 * Accessibility E2E Tests
 * Owner: Scenario 11 - Accessibility Compliance
 *
 * Test cases:
 * - Page has header, main, footer elements
 * - Navigation has aria-label
 * - Tab order is logical
 * - Focus indicators visible
 * - axe-core audit passes
 * - Heading hierarchy correct
 */

import { test, expect } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

test.describe('Accessibility Compliance', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('page contains semantic <header>, <main>, and <footer> elements', async ({ page }) => {
    // Test Case 1: Analyze page structure
    const header = page.locator('header');
    const main = page.locator('main');
    const footer = page.locator('footer');

    await expect(header).toBeVisible();
    await expect(main).toBeVisible();
    await expect(footer).toBeVisible();

    // Verify there is exactly one of each landmark
    await expect(header).toHaveCount(1);
    await expect(main).toHaveCount(1);
    await expect(footer).toHaveCount(1);
  });

  test('navigation is wrapped in <nav> element with aria-label', async ({ page }) => {
    // Test Case 2: Analyze navigation
    const navElements = page.locator('nav');

    // Should have at least one nav element
    const navCount = await navElements.count();
    expect(navCount).toBeGreaterThan(0);

    // Main navigation should have aria-label
    const mainNav = page.locator('nav[aria-label="Main navigation"]');
    await expect(mainNav).toBeVisible();

    // Footer navigation should have aria-label
    const footerNav = page.locator('nav[aria-label="Footer navigation"]');
    await expect(footerNav).toBeVisible();
  });

  test('all interactive elements receive focus in logical order', async ({ page }) => {
    // Test Case 3: Tab through page
    // Get all focusable elements
    const focusableSelector = 'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';
    const focusableElements = page.locator(focusableSelector);

    const count = await focusableElements.count();
    expect(count).toBeGreaterThan(0);

    // Tab through first several elements and verify focus moves
    for (let i = 0; i < Math.min(5, count); i++) {
      await page.keyboard.press('Tab');

      // Get currently focused element
      const focused = page.locator(':focus');
      await expect(focused).toBeVisible();
    }
  });

  test('visible focus ring on all focusable elements', async ({ page }) => {
    // Test Case 4: Check focus indicators
    // Test skip link focus
    const skipLink = page.locator('.skip-link');
    await skipLink.focus();

    // Get the outline style
    const skipLinkOutline = await skipLink.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.outline;
    });

    // Focus indicator should not be 'none' or empty
    expect(skipLinkOutline).not.toBe('none');
    expect(skipLinkOutline).not.toBe('');

    // Test button focus
    const themeToggle = page.locator('#theme-toggle');
    await themeToggle.focus();

    const buttonOutline = await themeToggle.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        outline: style.outline,
        outlineWidth: style.outlineWidth,
        outlineStyle: style.outlineStyle
      };
    });

    // Should have visible outline
    expect(buttonOutline.outlineStyle).not.toBe('none');

    // Test link focus
    const getStartedBtn = page.locator('#cta-get-started');
    await getStartedBtn.focus();

    const linkOutline = await getStartedBtn.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.outlineStyle;
    });

    expect(linkOutline).not.toBe('none');
  });

  test('no critical or serious accessibility violations from axe-core', async ({ page }) => {
    // Test Case 5: Run axe-core accessibility audit
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .analyze();

    // Filter for critical and serious violations only
    const criticalOrSeriousViolations = accessibilityScanResults.violations.filter(
      (violation) => violation.impact === 'critical' || violation.impact === 'serious'
    );

    // Log violations for debugging if any exist
    if (criticalOrSeriousViolations.length > 0) {
      console.log('Critical/Serious Accessibility Violations:');
      criticalOrSeriousViolations.forEach((violation) => {
        console.log(`- ${violation.id}: ${violation.description}`);
        console.log(`  Impact: ${violation.impact}`);
        violation.nodes.forEach((node) => {
          console.log(`  Element: ${node.html}`);
        });
      });
    }

    expect(criticalOrSeriousViolations).toHaveLength(0);
  });

  test('headings follow proper h1-h6 hierarchy without skipping levels', async ({ page }) => {
    // Test Case 6: Check heading hierarchy
    const headings = await page.evaluate(() => {
      const headingElements = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      return Array.from(headingElements).map((h) => ({
        level: parseInt(h.tagName.substring(1)),
        text: h.textContent?.trim().substring(0, 50)
      }));
    });

    // Should have exactly one h1
    const h1Count = headings.filter((h) => h.level === 1).length;
    expect(h1Count).toBe(1);

    // Check for skipped levels
    let previousLevel = 0;
    const skippedLevels = [];

    for (const heading of headings) {
      if (previousLevel > 0 && heading.level > previousLevel + 1) {
        skippedLevels.push({
          from: previousLevel,
          to: heading.level,
          heading: heading.text
        });
      }
      previousLevel = heading.level;
    }

    // Log any skipped levels for debugging
    if (skippedLevels.length > 0) {
      console.log('Skipped heading levels:');
      skippedLevels.forEach((skip) => {
        console.log(`- Jumped from h${skip.from} to h${skip.to} at "${skip.heading}"`);
      });
    }

    expect(skippedLevels).toHaveLength(0);
  });

  test('skip link is functional and appears on focus', async ({ page }) => {
    // Additional test: Skip link functionality
    const skipLink = page.locator('.skip-link');

    // Skip link should exist
    await expect(skipLink).toHaveCount(1);

    // Press Tab to focus on skip link (it should be the first focusable element)
    await page.keyboard.press('Tab');

    // Skip link should be visible when focused
    await expect(skipLink).toBeFocused();
    await expect(skipLink).toBeVisible();

    // Skip link should have correct href
    await expect(skipLink).toHaveAttribute('href', '#main-content');

    // Verify the main content target exists
    const mainContent = page.locator('#main-content');
    await expect(mainContent).toBeVisible();

    // Click skip link - verify navigation by checking scroll position or target visibility
    await skipLink.click();

    // After clicking, the hero section within main should be at or near top of viewport
    // We verify the skip link works by checking if main content is scrolled into view
    const mainBoundingBox = await mainContent.boundingBox();
    expect(mainBoundingBox).not.toBeNull();
    // Main content should be visible and near the top of the viewport
    expect(mainBoundingBox.y).toBeLessThanOrEqual(100);
  });

  test('images have appropriate alt text or aria-hidden', async ({ page }) => {
    // Additional test: Image accessibility
    const images = page.locator('img');
    const count = await images.count();

    for (let i = 0; i < count; i++) {
      const img = images.nth(i);

      // Each image should either have alt text or be aria-hidden
      const hasAlt = await img.getAttribute('alt') !== null;
      const isAriaHidden = await img.getAttribute('aria-hidden') === 'true';

      expect(hasAlt || isAriaHidden).toBeTruthy();
    }
  });

  test('buttons have accessible names', async ({ page }) => {
    // Additional test: Button accessibility
    const buttons = page.locator('button');
    const count = await buttons.count();

    for (let i = 0; i < count; i++) {
      const button = buttons.nth(i);

      // Each button should have either text content or aria-label
      const textContent = await button.textContent();
      const ariaLabel = await button.getAttribute('aria-label');

      const hasAccessibleName = (textContent && textContent.trim().length > 0) || ariaLabel;
      expect(hasAccessibleName).toBeTruthy();
    }
  });

  test('form controls have associated labels', async ({ page }) => {
    // Additional test: Form control accessibility
    const inputs = page.locator('input:not([type="hidden"]), select, textarea');
    const count = await inputs.count();

    // If there are form controls, verify they have labels
    for (let i = 0; i < count; i++) {
      const input = inputs.nth(i);

      // Check for associated label via id/for, aria-label, or aria-labelledby
      const id = await input.getAttribute('id');
      const ariaLabel = await input.getAttribute('aria-label');
      const ariaLabelledBy = await input.getAttribute('aria-labelledby');

      if (id) {
        const associatedLabel = page.locator(`label[for="${id}"]`);
        const hasAssociatedLabel = await associatedLabel.count() > 0;
        const hasAriaLabel = ariaLabel !== null || ariaLabelledBy !== null;

        expect(hasAssociatedLabel || hasAriaLabel).toBeTruthy();
      }
    }
  });

  test('ARIA attributes are used correctly', async ({ page }) => {
    // Additional test: ARIA attribute validity
    // Check that aria-expanded is boolean
    const elementsWithAriaExpanded = page.locator('[aria-expanded]');
    const expandedCount = await elementsWithAriaExpanded.count();

    for (let i = 0; i < expandedCount; i++) {
      const element = elementsWithAriaExpanded.nth(i);
      const value = await element.getAttribute('aria-expanded');
      expect(['true', 'false']).toContain(value);
    }

    // Check that aria-hidden is boolean
    const elementsWithAriaHidden = page.locator('[aria-hidden]');
    const hiddenCount = await elementsWithAriaHidden.count();

    for (let i = 0; i < hiddenCount; i++) {
      const element = elementsWithAriaHidden.nth(i);
      const value = await element.getAttribute('aria-hidden');
      expect(['true', 'false']).toContain(value);
    }
  });

  test('color contrast meets WCAG AA standards via axe-core', async ({ page }) => {
    // Test color contrast specifically
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withRules(['color-contrast'])
      .analyze();

    const contrastViolations = accessibilityScanResults.violations.filter(
      (v) => v.id === 'color-contrast' && (v.impact === 'critical' || v.impact === 'serious')
    );

    if (contrastViolations.length > 0) {
      console.log('Color Contrast Violations:');
      contrastViolations.forEach((violation) => {
        violation.nodes.forEach((node) => {
          console.log(`- Element: ${node.html}`);
          console.log(`  Message: ${node.failureSummary}`);
        });
      });
    }

    expect(contrastViolations).toHaveLength(0);
  });
});
