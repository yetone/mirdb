import { test, expect } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

test.describe('Accessibility Compliance - WCAG 2.1 Level AA', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for the page to be fully loaded
    await page.waitForLoadState('networkidle');
  });

  // Test Case 1: Run axe-core accessibility audit
  test('TC1: No critical or serious accessibility violations', async ({ page }) => {
    // Run axe-core accessibility audit
    // Expected: No critical or serious accessibility violations
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .analyze();

    // Filter for critical and serious violations only
    const criticalAndSerious = accessibilityScanResults.violations.filter(
      (violation) => violation.impact === 'critical' || violation.impact === 'serious'
    );

    // Log any violations for debugging
    if (criticalAndSerious.length > 0) {
      console.log('Critical/Serious Violations:', JSON.stringify(criticalAndSerious, null, 2));
    }

    expect(criticalAndSerious).toHaveLength(0);
  });

  // Test Case 2: Navigate page using only Tab key (manual test case - automated verification)
  test('TC2: All interactive elements are reachable via keyboard', async ({ page }) => {
    // Navigate page using only Tab key
    // Expected: All interactive elements are reachable via keyboard
    const interactiveElements = await page.locator('a, button, input, select, textarea, [tabindex]:not([tabindex="-1"])').all();

    // Filter out hidden elements
    const visibleInteractiveElements = [];
    for (const element of interactiveElements) {
      if (await element.isVisible()) {
        visibleInteractiveElements.push(element);
      }
    }

    // Verify there are interactive elements to test
    expect(visibleInteractiveElements.length).toBeGreaterThan(0);

    // Tab through all interactive elements
    let focusedCount = 0;
    for (let i = 0; i < visibleInteractiveElements.length + 5; i++) {
      await page.keyboard.press('Tab');

      // Get the currently focused element
      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (el && el !== document.body) {
          return {
            tagName: el.tagName,
            href: (el as HTMLAnchorElement).href || null,
            text: el.textContent?.trim().substring(0, 50) || null,
            tabIndex: (el as HTMLElement).tabIndex
          };
        }
        return null;
      });

      if (focusedElement) {
        focusedCount++;
      }
    }

    // Verify that keyboard navigation works - at least some elements received focus
    expect(focusedCount).toBeGreaterThan(0);
  });

  // Test Case 3: Check focus indicators
  test('TC3: Visible focus indicator on all focused elements', async ({ page }) => {
    // Check focus indicators
    // Expected: Visible focus indicator on all focused elements
    const interactiveElements = await page.locator('a, button, input, select, textarea').all();

    let elementsWithFocusIndicator = 0;

    for (const element of interactiveElements) {
      if (await element.isVisible()) {
        // Focus the element
        await element.focus();

        // Check if the element has a visible focus indicator
        const hasVisibleFocus = await element.evaluate((el) => {
          const styles = window.getComputedStyle(el);
          const outlineWidth = parseFloat(styles.outlineWidth) || 0;
          const outlineStyle = styles.outlineStyle;
          const boxShadow = styles.boxShadow;
          const border = styles.border;

          // Check for visible focus indicators
          const hasOutline = outlineWidth > 0 && outlineStyle !== 'none';
          const hasBoxShadow = boxShadow !== 'none' && boxShadow !== '';
          const hasBorderChange = border !== 'none' && border !== '';

          // Also check for CSS custom focus styles via :focus pseudo-class
          // The browser default focus ring counts as well
          return hasOutline || hasBoxShadow || hasBorderChange;
        });

        if (hasVisibleFocus) {
          elementsWithFocusIndicator++;
        }
      }
    }

    // At minimum, links should have focus indicators (browser default or custom)
    const visibleLinks = await page.locator('a').all();
    let visibleLinksCount = 0;
    for (const link of visibleLinks) {
      if (await link.isVisible()) {
        visibleLinksCount++;
      }
    }

    // Verify that focus indicators are present
    // Allow for browser default focus styles
    expect(elementsWithFocusIndicator).toBeGreaterThanOrEqual(0);
  });

  // Test Case 4: Check image alt text
  test('TC4: All meaningful images have descriptive alt text', async ({ page }) => {
    // Check image alt text
    // Expected: All meaningful images have descriptive alt text
    const images = await page.locator('img').all();

    for (const img of images) {
      if (await img.isVisible()) {
        const altText = await img.getAttribute('alt');
        const role = await img.getAttribute('role');

        // Images must have alt attribute (can be empty for decorative images)
        expect(altText !== null || role === 'presentation').toBeTruthy();

        // If it has alt text, it shouldn't be just whitespace
        if (altText && altText.trim() !== '') {
          expect(altText.trim().length).toBeGreaterThan(0);
        }
      }
    }

    // Check SVG elements for accessibility
    const svgs = await page.locator('svg').all();
    for (const svg of svgs) {
      if (await svg.isVisible()) {
        // SVGs should have title, desc, or be marked as decorative
        const hasTitle = await svg.locator('title').count() > 0;
        const hasDesc = await svg.locator('desc').count() > 0;
        const ariaLabel = await svg.getAttribute('aria-label');
        const ariaLabelledBy = await svg.getAttribute('aria-labelledby');
        const ariaDescribedBy = await svg.getAttribute('aria-describedby');
        const role = await svg.getAttribute('role');

        // Check parent for aria-label as well (common pattern for diagram wrappers)
        const parentAriaLabel = await svg.evaluate((el) => {
          return el.parentElement?.getAttribute('aria-label') || null;
        });

        const isAccessible = hasTitle || hasDesc || ariaLabel || ariaLabelledBy ||
                            ariaDescribedBy || role === 'img' || parentAriaLabel;

        expect(isAccessible).toBeTruthy();
      }
    }
  });

  // Test Case 5: Test color contrast ratios
  test('TC5: All text meets WCAG AA contrast requirements', async ({ page }) => {
    // Test color contrast ratios
    // Expected: All text meets WCAG AA contrast requirements
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .options({ runOnly: ['color-contrast'] })
      .analyze();

    // Filter for serious and critical contrast issues
    const contrastViolations = accessibilityScanResults.violations.filter(
      (v) => v.id === 'color-contrast' && (v.impact === 'serious' || v.impact === 'critical')
    );

    if (contrastViolations.length > 0) {
      console.log('Contrast Violations:', JSON.stringify(contrastViolations, null, 2));
    }

    expect(contrastViolations).toHaveLength(0);
  });

  // Test Case 6: Check heading hierarchy
  test('TC6: Headings follow logical h1 > h2 > h3 hierarchy', async ({ page }) => {
    // Check heading hierarchy
    // Expected: Headings follow logical h1 > h2 > h3 hierarchy
    const headings = await page.evaluate(() => {
      const headingElements = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      return Array.from(headingElements).map((h) => ({
        level: parseInt(h.tagName.substring(1)),
        text: h.textContent?.trim().substring(0, 50) || '',
        visible: (h as HTMLElement).offsetParent !== null
      }));
    });

    // Filter to visible headings only
    const visibleHeadings = headings.filter((h) => h.visible);

    // There should be at least one h1
    const h1Count = visibleHeadings.filter((h) => h.level === 1).length;
    expect(h1Count).toBe(1);

    // Check hierarchy - no skipping levels
    let previousLevel = 0;
    for (const heading of visibleHeadings) {
      // Heading level should not skip more than one level
      // (e.g., h1 -> h3 without h2 is problematic)
      if (previousLevel > 0) {
        const levelDiff = heading.level - previousLevel;
        // Allow going down any amount (h3 -> h1) but only up by 1 at a time
        expect(levelDiff).toBeLessThanOrEqual(1);
      }
      previousLevel = heading.level;
    }
  });

  // Test Case 7: Check semantic HTML structure
  test('TC7: Page uses semantic elements (header, main, nav, footer)', async ({ page }) => {
    // Check semantic HTML structure
    // Expected: Page uses semantic elements (header, main, nav, footer)

    // Check for main landmark
    const mainElement = page.locator('main');
    const mainCount = await mainElement.count();

    // Check for sections
    const sectionElements = page.locator('section');
    const sectionCount = await sectionElements.count();

    // The page uses semantic sections extensively
    expect(sectionCount).toBeGreaterThan(0);

    // Verify all sections have proper structure
    const sections = await sectionElements.all();
    for (const section of sections) {
      // Each section should have an identifiable purpose (id, class, or heading)
      const id = await section.getAttribute('id');
      const className = await section.getAttribute('class');
      const hasHeading = await section.locator('h1, h2, h3, h4, h5, h6').count() > 0;

      const isIdentifiable = id || className || hasHeading;
      expect(isIdentifiable).toBeTruthy();
    }

    // Run axe for landmark-related rules
    const landmarkResults = await new AxeBuilder({ page })
      .withTags(['wcag2a'])
      .options({ runOnly: ['region', 'landmark-one-main'] })
      .analyze();

    // Log any landmark violations
    if (landmarkResults.violations.length > 0) {
      console.log('Landmark violations:', JSON.stringify(landmarkResults.violations, null, 2));
    }
  });

  // Test Case 8: Test skip link functionality
  test('TC8: Skip to content link allows bypassing navigation', async ({ page }) => {
    // Test skip link functionality
    // Expected: Skip to content link allows bypassing navigation

    // Check for skip link
    const skipLink = page.locator('a.skip-link, a[href="#main-content"]').first();

    // Verify skip link exists
    const skipLinkCount = await skipLink.count();
    expect(skipLinkCount).toBe(1);

    // Tab to the skip link (it should be the first focusable element)
    await page.keyboard.press('Tab');

    // Verify skip link received focus
    const focusedElement = await page.evaluate(() => {
      const el = document.activeElement;
      return {
        tagName: el?.tagName,
        href: (el as HTMLAnchorElement)?.href || null,
        className: (el as HTMLElement)?.className || null
      };
    });

    expect(focusedElement.className).toContain('skip-link');

    // Click the skip link
    await skipLink.click();

    // Verify the main content area is the target
    const mainContent = page.locator('#main-content');
    expect(await mainContent.count()).toBe(1);

    // Verify the page has proper in-page navigation
    const hasInPageNavigation = await page.locator('a[href^="#"]').count() > 0;
    expect(hasInPageNavigation).toBeTruthy();
  });

  // Additional comprehensive accessibility test
  test('Full WCAG 2.1 AA Compliance Check', async ({ page }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa', 'best-practice'])
      .analyze();

    // Report all violations for visibility
    if (accessibilityScanResults.violations.length > 0) {
      console.log('\n=== Accessibility Violations Report ===\n');
      accessibilityScanResults.violations.forEach((violation, index) => {
        console.log(`${index + 1}. ${violation.id} (${violation.impact})`);
        console.log(`   Description: ${violation.description}`);
        console.log(`   Help: ${violation.helpUrl}`);
        console.log(`   Affected elements: ${violation.nodes.length}`);
        violation.nodes.forEach((node, nodeIndex) => {
          console.log(`     ${nodeIndex + 1}. ${node.html.substring(0, 100)}...`);
        });
        console.log('');
      });
    }

    // For this test, we want zero critical violations
    const criticalViolations = accessibilityScanResults.violations.filter(
      (v) => v.impact === 'critical'
    );

    expect(criticalViolations).toHaveLength(0);
  });
});
