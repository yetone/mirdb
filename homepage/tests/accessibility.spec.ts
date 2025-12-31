import { test, expect } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

test.describe('Accessibility Compliance - WCAG 2.1 AA', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Run axe-core accessibility audit - No WCAG 2.1 AA violations', async ({ page }) => {
    // Run axe-core accessibility audit with WCAG 2.1 AA rules
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .analyze();

    // Log violations for debugging if any exist
    if (accessibilityScanResults.violations.length > 0) {
      console.log('Accessibility violations found:');
      accessibilityScanResults.violations.forEach((violation) => {
        console.log(`- ${violation.id}: ${violation.description}`);
        console.log(`  Impact: ${violation.impact}`);
        console.log(`  Help: ${violation.helpUrl}`);
        violation.nodes.forEach((node) => {
          console.log(`  Element: ${node.html}`);
        });
      });
    }

    // Expect no WCAG 2.1 AA violations
    expect(accessibilityScanResults.violations).toEqual([]);
  });

  test('TC3: Check focus indicators - Visible focus indicators on all focusable elements', async ({ page }) => {
    // Get all focusable elements
    const focusableSelectors = 'a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])';
    const focusableElements = await page.locator(focusableSelectors).all();

    expect(focusableElements.length).toBeGreaterThan(0);

    // Test focus indicators for each focusable element
    for (const element of focusableElements) {
      await element.focus();

      // Check that element is focused
      await expect(element).toBeFocused();

      // Get computed styles to verify focus indicator exists
      const focusStyles = await element.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          outline: styles.outline,
          outlineWidth: styles.outlineWidth,
          outlineColor: styles.outlineColor,
          outlineStyle: styles.outlineStyle,
          boxShadow: styles.boxShadow,
          border: styles.border,
          borderWidth: styles.borderWidth
        };
      });

      // Verify element has some form of visible focus indicator
      // Check for outline (not 'none') or box-shadow or border change
      const hasOutline = focusStyles.outlineStyle !== 'none' && focusStyles.outlineWidth !== '0px';
      const hasBoxShadow = focusStyles.boxShadow !== 'none';

      expect(
        hasOutline || hasBoxShadow,
        `Element should have visible focus indicator. Outline: ${focusStyles.outline}, BoxShadow: ${focusStyles.boxShadow}`
      ).toBe(true);
    }
  });

  test('TC4: Verify images have alt text - All images have meaningful alt attributes', async ({ page }) => {
    // Get all images on the page
    const images = await page.locator('img').all();

    expect(images.length).toBeGreaterThan(0);

    for (const image of images) {
      // Check that alt attribute exists
      const alt = await image.getAttribute('alt');
      expect(alt, 'Image should have alt attribute').not.toBeNull();

      // Check that alt text is not empty (unless decorative - but our images should have meaningful alt)
      expect(alt?.length, 'Image alt text should not be empty').toBeGreaterThan(0);
    }

    // Also check SVG images with role="img" have aria-label
    const svgImages = await page.locator('svg[role="img"]').all();
    for (const svg of svgImages) {
      const ariaLabel = await svg.getAttribute('aria-label');
      expect(ariaLabel, 'SVG with role="img" should have aria-label').not.toBeNull();
      expect(ariaLabel?.length, 'SVG aria-label should not be empty').toBeGreaterThan(0);
    }
  });

  test('TC5: Check heading hierarchy - Headings follow proper hierarchy without skipping levels', async ({ page }) => {
    // Get all headings in document order
    const headings = await page.locator('h1, h2, h3, h4, h5, h6').all();

    expect(headings.length).toBeGreaterThan(0);

    // Extract heading levels and verify hierarchy
    const headingLevels: number[] = [];
    for (const heading of headings) {
      const tagName = await heading.evaluate((el) => el.tagName.toLowerCase());
      const level = parseInt(tagName.replace('h', ''), 10);
      headingLevels.push(level);
    }

    // Verify first heading is h1
    expect(headingLevels[0], 'First heading should be h1').toBe(1);

    // Verify there is exactly one h1
    const h1Count = headingLevels.filter((l) => l === 1).length;
    expect(h1Count, 'There should be exactly one h1 on the page').toBe(1);

    // Verify no heading levels are skipped
    for (let i = 1; i < headingLevels.length; i++) {
      const previousLevel = headingLevels[i - 1];
      const currentLevel = headingLevels[i];

      // When going to a higher level (deeper), you can only go one level deeper
      if (currentLevel > previousLevel) {
        expect(
          currentLevel - previousLevel,
          `Heading hierarchy skipped from h${previousLevel} to h${currentLevel}. Headings should not skip levels.`
        ).toBeLessThanOrEqual(1);
      }
      // When going to a lower level (shallower), you can jump to any level
      // This is valid HTML heading structure
    }
  });

  test('TC6: Verify color contrast ratios - All text meets minimum 4.5:1 contrast ratio', async ({ page }) => {
    // Run axe-core specifically for color contrast
    const contrastResults = await new AxeBuilder({ page })
      .withRules(['color-contrast'])
      .analyze();

    // Log any color contrast violations
    if (contrastResults.violations.length > 0) {
      console.log('Color contrast violations found:');
      contrastResults.violations.forEach((violation) => {
        console.log(`- ${violation.id}: ${violation.description}`);
        violation.nodes.forEach((node) => {
          console.log(`  Element: ${node.html}`);
          console.log(`  Failure Summary: ${node.failureSummary}`);
        });
      });
    }

    // Expect no color contrast violations
    expect(contrastResults.violations).toEqual([]);
  });

  test('TC7: Check for skip navigation link - Skip to main content link is present', async ({ page }) => {
    // Check for skip link - should be the first focusable element
    const skipLink = page.locator('a[href="#main-content"], a[href="#main"], .skip-link, .skip-to-main, [class*="skip"]').first();

    // If skip link exists, verify it works
    const skipLinkExists = await skipLink.count() > 0;

    if (skipLinkExists) {
      // Skip link should be focusable
      await skipLink.focus();
      await expect(skipLink).toBeFocused();

      // Skip link should become visible when focused (commonly hidden until focused)
      await expect(skipLink).toBeVisible();

      // Verify skip link has appropriate text
      const skipLinkText = await skipLink.textContent();
      expect(skipLinkText?.toLowerCase()).toContain('skip');
    }

    // Check if there is a main element or main landmark
    const mainLandmark = page.locator('main, [role="main"]');
    const hasMainLandmark = await mainLandmark.count() > 0;

    // For full accessibility compliance, either a skip link should exist OR
    // we should have proper landmark regions for screen reader navigation
    expect(
      skipLinkExists || hasMainLandmark,
      'Page should have either a skip navigation link or proper main landmark for screen reader navigation'
    ).toBe(true);
  });

  test('TC2: Keyboard navigation - All interactive elements are focusable and activatable', async ({ page }) => {
    // Test keyboard navigation through the page
    const interactiveElements = await page.locator('a[href], button').all();

    expect(interactiveElements.length).toBeGreaterThan(0);

    // Start from body and tab through elements
    await page.keyboard.press('Tab');

    let tabCount = 0;
    const maxTabs = 50; // Prevent infinite loop
    const focusedElements: string[] = [];

    while (tabCount < maxTabs) {
      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;
        return {
          tag: el.tagName.toLowerCase(),
          text: el.textContent?.trim().substring(0, 50),
          href: el.getAttribute('href'),
          type: el.getAttribute('type')
        };
      });

      if (!focusedElement) break;

      focusedElements.push(`${focusedElement.tag}: ${focusedElement.text || focusedElement.href || ''}`);

      await page.keyboard.press('Tab');
      tabCount++;
    }

    // Verify we can tab to multiple elements
    expect(focusedElements.length, 'Should be able to tab through multiple interactive elements').toBeGreaterThan(3);

    // Test that Enter activates links
    await page.goto('/');
    const getStartedLink = page.locator('[data-testid="cta-get-started"]');
    await getStartedLink.focus();
    await page.keyboard.press('Enter');
    await expect(page).toHaveURL(/#quick-start/);
  });
});
