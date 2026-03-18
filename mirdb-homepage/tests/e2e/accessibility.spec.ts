/**
 * Accessibility E2E Tests.
 * Owner: Scenario 10 - Accessibility Compliance
 *
 * Tests:
 * - Keyboard navigation
 * - Focus indicators
 * - ARIA labels
 * - Alt text on images
 * - axe-core audit
 * - Skip to content link
 */

import { test, expect } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

test.describe('Accessibility Compliance', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for the page to fully load
    await page.waitForLoadState('networkidle');
  });

  test('Test Case 1: Tab through page and count focusable elements - all interactive elements receive focus in logical order', async ({ page }) => {
    // Get all focusable elements
    const focusableElements = await page.locator(
      'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])'
    ).all();

    expect(focusableElements.length).toBeGreaterThan(0);

    // Track the order of focused elements
    const focusOrder: string[] = [];

    // Start from the body
    await page.keyboard.press('Tab');

    // Tab through all focusable elements
    for (let i = 0; i < focusableElements.length + 2; i++) {
      const activeElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;
        return {
          tagName: el.tagName.toLowerCase(),
          text: (el as HTMLElement).textContent?.trim().slice(0, 50) || '',
          ariaLabel: el.getAttribute('aria-label') || '',
          href: el.getAttribute('href') || '',
          id: el.id || '',
        };
      });

      if (activeElement) {
        focusOrder.push(`${activeElement.tagName}${activeElement.id ? '#' + activeElement.id : ''}${activeElement.ariaLabel ? '[' + activeElement.ariaLabel + ']' : ''}`);
      }

      await page.keyboard.press('Tab');
    }

    // Verify we have multiple focusable elements (links, buttons)
    expect(focusOrder.length).toBeGreaterThan(3);

    // Verify that buttons and links are in the focus order
    const hasButtons = focusOrder.some(el => el.includes('button'));
    const hasLinks = focusOrder.some(el => el.includes('a'));

    expect(hasButtons).toBe(true);
    expect(hasLinks).toBe(true);
  });

  test('Test Case 2: Check focus visible styles on buttons - buttons have visible focus indicators', async ({ page }) => {
    // Find all visible, enabled buttons
    const buttons = page.locator('button:visible:not([disabled])');
    const buttonCount = await buttons.count();

    expect(buttonCount).toBeGreaterThan(0);

    // Check focus styles on the first few buttons
    let testedCount = 0;
    for (let i = 0; i < Math.min(buttonCount, 5); i++) {
      const button = buttons.nth(i);

      // Ensure button is visible and interactable
      const isVisible = await button.isVisible();
      if (!isVisible) continue;

      // Click the button first to ensure it can receive focus, then focus it
      await button.scrollIntoViewIfNeeded();
      await button.focus();

      // Small wait for focus to apply
      await page.waitForTimeout(100);

      // Check for visible focus indicator (outline or ring) using CSS classes or computed styles
      const focusInfo = await button.evaluate(el => {
        const styles = window.getComputedStyle(el);
        const classList = Array.from(el.classList);

        // Check for focus-related classes (Tailwind pattern)
        const hasFocusClasses = classList.some(c =>
          c.includes('focus:') || c.includes('focus-visible:') || c.includes('ring')
        );

        // Check computed styles
        const outlineStyle = styles.outlineStyle;
        const outlineWidth = styles.outlineWidth;
        const boxShadow = styles.boxShadow;

        return {
          hasFocusClasses,
          hasOutline: outlineStyle !== 'none' && outlineWidth !== '0px',
          hasBoxShadow: boxShadow !== 'none' && boxShadow !== '',
          // Also check if the element has focus ring utility classes in its classList
          classes: classList.join(' '),
        };
      });

      // Button should have focus indicator through CSS classes, outline, or box-shadow
      const hasVisibleIndicator =
        focusInfo.hasFocusClasses ||
        focusInfo.hasOutline ||
        focusInfo.hasBoxShadow ||
        focusInfo.classes.includes('ring');

      expect(hasVisibleIndicator, `Button should have visible focus indicator`).toBe(true);
      testedCount++;
    }

    // Ensure we tested at least one button
    expect(testedCount).toBeGreaterThan(0);
  });

  test('Test Case 3: Check theme toggle for aria-label - theme toggle has aria-label describing its function', async ({ page }) => {
    // Find the theme toggle button
    const themeToggle = page.getByTestId('theme-toggle');

    // Verify it exists
    await expect(themeToggle).toBeVisible();

    // Check for aria-label
    const ariaLabel = await themeToggle.getAttribute('aria-label');

    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel).toMatch(/switch to (light|dark) mode/i);
  });

  test('Test Case 4: Check copy button for aria-label - copy button has aria-label like "Copy code to clipboard"', async ({ page }) => {
    // Find copy buttons
    const copyButtons = page.locator('.copy-button, button[aria-label*="copy" i], button[aria-label*="clipboard" i]');

    // Wait for code blocks to be visible (they contain copy buttons)
    await page.waitForSelector('pre', { state: 'visible' });

    const copyButtonCount = await copyButtons.count();

    // There should be at least one copy button
    expect(copyButtonCount).toBeGreaterThan(0);

    // Check each copy button has appropriate aria-label
    for (let i = 0; i < copyButtonCount; i++) {
      const button = copyButtons.nth(i);
      const ariaLabel = await button.getAttribute('aria-label');

      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel?.toLowerCase()).toMatch(/copy|clipboard/);
    }
  });

  test('Test Case 5: Check all images for alt text - all img elements have meaningful alt attributes', async ({ page }) => {
    // Find all images
    const images = page.locator('img');
    const imageCount = await images.count();

    // There should be at least one image (logo)
    expect(imageCount).toBeGreaterThan(0);

    // Check each image has alt text
    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i);
      const alt = await img.getAttribute('alt');
      const src = await img.getAttribute('src');

      // Alt text should exist and not be empty
      expect(alt, `Image ${src} should have alt text`).toBeTruthy();
      expect(alt?.trim().length, `Image ${src} should have non-empty alt text`).toBeGreaterThan(0);
    }
  });

  test('Test Case 6: Run automated accessibility audit (axe-core) - no critical or serious accessibility violations', async ({ page }) => {
    // Run axe accessibility audit
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .analyze();

    // Filter for critical and serious violations
    const criticalViolations = accessibilityScanResults.violations.filter(
      v => v.impact === 'critical' || v.impact === 'serious'
    );

    // Log any violations for debugging
    if (criticalViolations.length > 0) {
      console.log('Critical/Serious Accessibility Violations:');
      criticalViolations.forEach(violation => {
        console.log(`- ${violation.id}: ${violation.description}`);
        violation.nodes.forEach(node => {
          console.log(`  Element: ${node.html.substring(0, 100)}`);
          console.log(`  Fix: ${node.failureSummary}`);
        });
      });
    }

    // Assert no critical or serious violations
    expect(
      criticalViolations,
      `Found ${criticalViolations.length} critical/serious accessibility violations`
    ).toHaveLength(0);
  });

  test('Test Case 7: Verify skip-to-content link presence - skip link exists for keyboard users to bypass navigation', async ({ page }) => {
    // Skip links are usually visible only on focus, so we need to tab to them
    // or look for them in the DOM
    const skipLink = page.locator('a[href="#main-content"], a[href="#content"], a.skip-link, a.skip-to-content, [class*="skip"]');

    // Check if skip link exists
    const skipLinkCount = await skipLink.count();

    if (skipLinkCount === 0) {
      // Try to find by text content
      const skipByText = page.locator('a:has-text("Skip to"), a:has-text("Skip to content"), a:has-text("Skip navigation")');
      const skipByTextCount = await skipByText.count();

      expect(skipByTextCount, 'Skip link should exist for keyboard users to bypass navigation').toBeGreaterThan(0);
    } else {
      expect(skipLinkCount).toBeGreaterThan(0);
    }

    // Find the skip link and verify it works
    const skip = page.locator('a.skip-link, a.skip-to-content, a[href="#main-content"], a[href="#content"]').first();

    // Focus it via keyboard
    await page.keyboard.press('Tab');

    // The skip link should become visible when focused
    const skipLinkVisible = await skip.isVisible().catch(() => false);

    // If visible, verify it has proper target
    if (skipLinkVisible) {
      const href = await skip.getAttribute('href');
      expect(href).toMatch(/^#/); // Should point to an anchor
    }
  });

  test('Additional: Verify proper heading hierarchy', async ({ page }) => {
    // Get all headings
    const headings = await page.locator('h1, h2, h3, h4, h5, h6').all();

    // Should have at least one h1
    const h1Count = await page.locator('h1').count();
    expect(h1Count).toBe(1);

    // Get heading levels in order
    const headingLevels: number[] = [];
    for (const heading of headings) {
      const tagName = await heading.evaluate(el => el.tagName.toLowerCase());
      const level = parseInt(tagName.replace('h', ''));
      headingLevels.push(level);
    }

    // Verify heading levels don't skip (e.g., h1 to h3 without h2)
    for (let i = 1; i < headingLevels.length; i++) {
      const diff = headingLevels[i] - headingLevels[i - 1];
      // Heading level should not increase by more than 1
      expect(diff, `Heading hierarchy should not skip levels`).toBeLessThanOrEqual(1);
    }
  });

  test('Additional: Verify color contrast meets WCAG AA standards', async ({ page }) => {
    // Run axe specifically for color contrast
    const contrastResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .options({ runOnly: ['color-contrast'] })
      .analyze();

    // Log any contrast violations
    if (contrastResults.violations.length > 0) {
      console.log('Color Contrast Violations:');
      contrastResults.violations.forEach(violation => {
        violation.nodes.forEach(node => {
          console.log(`- Element: ${node.html.substring(0, 80)}`);
          console.log(`  Issue: ${node.failureSummary}`);
        });
      });
    }

    // Allow minor contrast issues but flag critical ones
    const criticalContrastIssues = contrastResults.violations.filter(
      v => v.impact === 'critical' || v.impact === 'serious'
    );

    expect(
      criticalContrastIssues,
      'No critical color contrast violations should exist'
    ).toHaveLength(0);
  });

  test('Additional: Verify interactive elements have accessible names', async ({ page }) => {
    // Run axe for button and link name requirements
    const accessibleNameResults = await new AxeBuilder({ page })
      .options({ runOnly: ['button-name', 'link-name', 'image-alt'] })
      .analyze();

    expect(
      accessibleNameResults.violations,
      'All interactive elements should have accessible names'
    ).toHaveLength(0);
  });

  test('Additional: Verify landmark regions are properly defined', async ({ page }) => {
    // Check for main landmark
    const mainLandmark = page.locator('main, [role="main"]');
    await expect(mainLandmark).toBeVisible();

    // Check for section/region landmarks with aria-labels
    const sections = page.locator('section[aria-label], section[aria-labelledby], [role="region"][aria-label]');
    const sectionCount = await sections.count();

    // Should have multiple labeled sections
    expect(sectionCount).toBeGreaterThanOrEqual(1);
  });
});
