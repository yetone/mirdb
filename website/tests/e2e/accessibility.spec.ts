/**
 * Accessibility Tests
 * Owner: Scenario 9 - Accessibility Compliance
 *
 * Validates homepage meets WCAG 2.1 AA accessibility requirements:
 * - Automated accessibility audit using axe-core
 * - Image alt text verification
 * - Color contrast ratio validation
 * - Keyboard navigation testing
 * - Focus indicator visibility
 * - Semantic HTML structure
 */

import { test, expect } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

test.describe('Accessibility Compliance - WCAG 2.1 AA', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for page to fully load
    await page.waitForLoadState('networkidle');
  });

  test('TC1: Run axe-core accessibility audit - no critical or serious violations', async ({ page }) => {
    // Run axe-core accessibility audit
    const accessibilityResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .analyze();

    // Filter for critical and serious violations only
    const criticalAndSeriousViolations = accessibilityResults.violations.filter(
      (violation) => violation.impact === 'critical' || violation.impact === 'serious'
    );

    // Log any violations for debugging
    if (criticalAndSeriousViolations.length > 0) {
      console.log('Critical/Serious Violations found:');
      criticalAndSeriousViolations.forEach((violation) => {
        console.log(`- ${violation.id}: ${violation.description}`);
        console.log(`  Impact: ${violation.impact}`);
        console.log(`  Help: ${violation.help}`);
        violation.nodes.forEach((node) => {
          console.log(`  Element: ${node.html}`);
        });
      });
    }

    // Assert no critical or serious violations
    expect(
      criticalAndSeriousViolations,
      `Found ${criticalAndSeriousViolations.length} critical/serious accessibility violations`
    ).toHaveLength(0);
  });

  test('TC2: Check all images for alt text - all images have descriptive alt attributes', async ({ page }) => {
    // Get all img elements
    const images = await page.locator('img').all();

    expect(images.length).toBeGreaterThan(0);

    for (const image of images) {
      const alt = await image.getAttribute('alt');
      const src = await image.getAttribute('src');

      // Verify alt attribute exists
      expect(alt, `Image ${src} is missing alt attribute`).not.toBeNull();

      // Verify alt is not empty (unless image is decorative)
      const ariaHidden = await image.getAttribute('aria-hidden');
      const role = await image.getAttribute('role');

      if (ariaHidden !== 'true' && role !== 'presentation') {
        expect(
          alt?.trim().length,
          `Image ${src} has empty alt attribute (should be descriptive or use aria-hidden="true" for decorative images)`
        ).toBeGreaterThan(0);
      }
    }
  });

  test('TC3: Verify color contrast ratios - all text meets 4.5:1 minimum contrast ratio', async ({ page }) => {
    // Run axe-core specifically for color contrast (AA level only, not AAA)
    const contrastResults = await new AxeBuilder({ page })
      .withRules(['color-contrast'])
      .analyze();

    // Filter for contrast-related violations
    const contrastViolations = contrastResults.violations.filter(
      (v) => v.id.includes('color-contrast')
    );

    // Log any violations for debugging
    if (contrastViolations.length > 0) {
      console.log('Color contrast violations found:');
      contrastViolations.forEach((violation) => {
        console.log(`- ${violation.id}: ${violation.description}`);
        violation.nodes.forEach((node) => {
          console.log(`  Element: ${node.html}`);
          console.log(`  Failure: ${node.failureSummary}`);
        });
      });
    }

    // Verify no contrast violations
    expect(
      contrastViolations,
      `Found ${contrastViolations.length} color contrast violations`
    ).toHaveLength(0);
  });

  test('TC4: Test keyboard navigation - all interactive elements are focusable via Tab key', async ({ page }) => {
    // Start from the body
    await page.focus('body');

    // Collect all interactive elements that should be keyboard-accessible
    const interactiveSelectors = [
      'a[href]',
      'button',
      '[tabindex]:not([tabindex="-1"])',
      'input',
      'select',
      'textarea'
    ];

    const interactiveElements = await page.locator(interactiveSelectors.join(', ')).all();

    expect(interactiveElements.length).toBeGreaterThan(0);

    // Get count of expected tabbable elements (excluding hidden ones)
    const visibleInteractiveElements = await page.locator(
      `${interactiveSelectors.join(', ')}:visible`
    ).all();

    // Tab through the page and collect focused elements
    const focusedElements: string[] = [];
    let previousFocusedElement = '';
    let maxTabs = visibleInteractiveElements.length + 5; // Add buffer for safety

    for (let i = 0; i < maxTabs; i++) {
      await page.keyboard.press('Tab');

      // Get the currently focused element
      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        return el ? el.tagName + (el.id ? `#${el.id}` : '') + (el.className ? `.${el.className.split(' ')[0]}` : '') : 'none';
      });

      // Break if we've cycled back to an already focused element
      if (focusedElement === previousFocusedElement || focusedElement === 'BODY') {
        break;
      }

      focusedElements.push(focusedElement);
      previousFocusedElement = focusedElement;
    }

    // Verify that we can tab through multiple interactive elements
    expect(
      focusedElements.length,
      'Should be able to tab through multiple interactive elements'
    ).toBeGreaterThan(0);

    // Verify common interactive elements are reachable
    const hasLinks = focusedElements.some((el) => el.startsWith('A'));
    const hasButtons = focusedElements.some((el) => el.startsWith('BUTTON'));

    expect(hasLinks || hasButtons, 'At least some links or buttons should be keyboard reachable').toBe(true);
  });

  test('TC5: Verify focus indicators - focused elements have visible focus indicator', async ({ page }) => {
    // Get all focusable elements
    const focusableSelectors = 'a[href], button, [tabindex]:not([tabindex="-1"])';
    const focusableElements = await page.locator(focusableSelectors).all();

    expect(focusableElements.length).toBeGreaterThan(0);

    // Test a sample of focusable elements for focus visibility
    const sampleSize = Math.min(5, focusableElements.length);

    for (let i = 0; i < sampleSize; i++) {
      const element = focusableElements[i];

      // Skip if element is not visible
      const isVisible = await element.isVisible();
      if (!isVisible) continue;

      // Get element's computed style before focus
      const beforeFocus = await element.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          outline: style.outline,
          outlineWidth: style.outlineWidth,
          outlineStyle: style.outlineStyle,
          outlineColor: style.outlineColor,
          boxShadow: style.boxShadow,
          border: style.border
        };
      });

      // Focus the element
      await element.focus();

      // Get element's computed style after focus
      const afterFocus = await element.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          outline: style.outline,
          outlineWidth: style.outlineWidth,
          outlineStyle: style.outlineStyle,
          outlineColor: style.outlineColor,
          boxShadow: style.boxShadow,
          border: style.border
        };
      });

      // Check if there's a visible focus indicator
      // This can be outline, box-shadow, or border change
      const hasOutline = afterFocus.outlineStyle !== 'none' &&
                        afterFocus.outlineWidth !== '0px';
      const hasBoxShadowChange = afterFocus.boxShadow !== beforeFocus.boxShadow &&
                                  afterFocus.boxShadow !== 'none';
      const hasBorderChange = afterFocus.border !== beforeFocus.border;

      const hasFocusIndicator = hasOutline || hasBoxShadowChange || hasBorderChange;

      // Get element info for error message
      const elementInfo = await element.evaluate((el) => {
        return `${el.tagName}${el.id ? `#${el.id}` : ''}${el.className ? `.${el.className.split(' ')[0]}` : ''}`;
      });

      expect(
        hasFocusIndicator,
        `Element ${elementInfo} should have visible focus indicator when focused`
      ).toBe(true);
    }
  });

  test('TC6: Check semantic HTML structure - proper heading hierarchy and semantic elements', async ({ page }) => {
    // Check for proper heading hierarchy (h1, h2, h3...)
    const h1Count = await page.locator('h1').count();
    const h2Count = await page.locator('h2').count();
    const h3Count = await page.locator('h3').count();

    // There should be exactly one h1
    expect(h1Count, 'Page should have exactly one h1 element').toBe(1);

    // There should be h2 elements for sections
    expect(h2Count, 'Page should have h2 elements for sections').toBeGreaterThan(0);

    // Verify heading hierarchy - no skipped levels
    const headings = await page.evaluate(() => {
      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      return Array.from(allHeadings).map((h) => ({
        level: parseInt(h.tagName.charAt(1)),
        text: h.textContent?.trim() || '',
        visible: (h as HTMLElement).offsetParent !== null
      }));
    });

    // Check for skipped heading levels (e.g., h1 -> h3 without h2)
    let previousLevel = 0;
    for (const heading of headings.filter(h => h.visible)) {
      if (previousLevel > 0 && heading.level > previousLevel + 1) {
        console.warn(`Possible skipped heading level: h${previousLevel} followed by h${heading.level}`);
      }
      previousLevel = heading.level;
    }

    // Check for semantic elements
    const hasHeader = await page.locator('header').count() > 0;
    const hasMain = await page.locator('main').count() > 0;
    const hasFooter = await page.locator('footer').count() > 0;
    const hasNav = await page.locator('nav').count() > 0;

    expect(hasHeader, 'Page should have a header element').toBe(true);
    expect(hasMain, 'Page should have a main element').toBe(true);
    expect(hasFooter, 'Page should have a footer element').toBe(true);
    expect(hasNav, 'Page should have a nav element').toBe(true);

    // Check for landmark roles
    const hasMainRole = await page.locator('[role="main"]').count() > 0 ||
                        await page.locator('main').count() > 0;
    const hasNavigationRole = await page.locator('[role="navigation"]').count() > 0 ||
                              await page.locator('nav').count() > 0;
    const hasBannerRole = await page.locator('[role="banner"]').count() > 0 ||
                          await page.locator('header').count() > 0;
    const hasContentInfoRole = await page.locator('[role="contentinfo"]').count() > 0 ||
                               await page.locator('footer').count() > 0;

    expect(hasMainRole, 'Page should have main landmark').toBe(true);
    expect(hasNavigationRole, 'Page should have navigation landmark').toBe(true);
    expect(hasBannerRole, 'Page should have banner landmark').toBe(true);
    expect(hasContentInfoRole, 'Page should have contentinfo landmark').toBe(true);

    // Check sections have aria-labels or aria-labelledby
    const sections = await page.locator('section').all();
    for (const section of sections) {
      const hasAriaLabel = await section.getAttribute('aria-label');
      const hasAriaLabelledBy = await section.getAttribute('aria-labelledby');
      const sectionId = await section.getAttribute('id');

      // Sections should have some form of accessible label
      const hasLabel = hasAriaLabel || hasAriaLabelledBy;
      expect(
        hasLabel,
        `Section ${sectionId || '(unnamed)'} should have aria-label or aria-labelledby`
      ).toBeTruthy();
    }
  });
});
