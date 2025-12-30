// @ts-check
const { test, expect } = require('@playwright/test');
const AxeBuilder = require('@axe-core/playwright').default;

test.describe('Accessibility Compliance - WCAG 2.1 Level AA', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Tab through all interactive elements - keyboard navigation with visible focus indicator', async ({ page }) => {
    // Test Case 1: Tab through all interactive elements on the page
    // Expected: All buttons, links, and inputs are focusable with visible focus indicator

    // Get all interactive elements (links, buttons, inputs)
    const interactiveElements = await page.locator('a[href], button, input, select, textarea').all();

    expect(interactiveElements.length).toBeGreaterThan(0);

    // Start from body and tab through elements
    await page.locator('body').click();

    let focusedCount = 0;
    const maxTabs = interactiveElements.length + 5; // Safety limit

    for (let i = 0; i < maxTabs; i++) {
      await page.keyboard.press('Tab');

      // Wait a bit for focus to settle
      await page.waitForTimeout(50);

      const focusedElement = page.locator(':focus');
      const count = await focusedElement.count();

      if (count === 0) continue;

      const tagName = await focusedElement.first().evaluate(el => el.tagName.toLowerCase()).catch(() => null);

      if (!tagName) continue;

      // Only check interactive elements (not scrollable containers, etc.)
      if (!['a', 'button', 'input', 'select', 'textarea'].includes(tagName)) continue;

      // Verify focus is visible by checking computed outline or box-shadow
      const focusStyles = await focusedElement.first().evaluate(el => {
        const styles = window.getComputedStyle(el);
        return {
          outline: styles.outline,
          outlineWidth: styles.outlineWidth,
          outlineStyle: styles.outlineStyle,
          outlineColor: styles.outlineColor,
          boxShadow: styles.boxShadow
        };
      });

      // Check that focus indicator is visible (outline is not 'none' or has visible outline/box-shadow)
      const hasOutline = focusStyles.outlineStyle !== 'none' &&
                         focusStyles.outlineWidth !== '0px' &&
                         focusStyles.outlineColor !== 'transparent';
      const hasBoxShadow = focusStyles.boxShadow &&
                           focusStyles.boxShadow !== 'none' &&
                           focusStyles.boxShadow !== '';

      const hasVisibleFocus = hasOutline || hasBoxShadow;

      expect(hasVisibleFocus).toBeTruthy();
      focusedCount++;
    }

    // Verify that we tabbed through multiple interactive elements
    expect(focusedCount).toBeGreaterThan(0);
  });

  test('TC2: Run automated accessibility audit (axe-core) - no critical or serious violations', async ({ page }) => {
    // Test Case 2: Run automated accessibility audit (axe-core)
    // Expected: No critical or serious accessibility violations

    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .analyze();

    // Filter for critical and serious violations only
    const criticalAndSerious = accessibilityScanResults.violations.filter(
      v => v.impact === 'critical' || v.impact === 'serious'
    );

    // Log any violations for debugging
    if (criticalAndSerious.length > 0) {
      console.log('Critical/Serious violations found:');
      criticalAndSerious.forEach(v => {
        console.log(`- ${v.id}: ${v.description} (${v.impact})`);
        v.nodes.forEach(n => console.log(`  Element: ${n.html}`));
      });
    }

    expect(criticalAndSerious).toHaveLength(0);
  });

  test('TC3: Check color contrast ratios - meets WCAG AA requirements (4.5:1)', async ({ page }) => {
    // Test Case 3: Check color contrast ratios using contrast checker
    // Expected: All text meets WCAG AA contrast requirements (4.5:1)

    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .options({ runOnly: ['color-contrast'] })
      .analyze();

    const contrastViolations = accessibilityScanResults.violations.filter(
      v => v.id === 'color-contrast'
    );

    // Log any contrast violations for debugging
    if (contrastViolations.length > 0) {
      console.log('Color contrast violations found:');
      contrastViolations.forEach(v => {
        v.nodes.forEach(n => {
          console.log(`- Element: ${n.html}`);
          console.log(`  Message: ${n.failureSummary}`);
        });
      });
    }

    expect(contrastViolations).toHaveLength(0);
  });

  test('TC4: Verify HTML document has lang attribute', async ({ page }) => {
    // Test Case 4: Verify HTML document has lang attribute
    // Expected: HTML element has lang='en' or appropriate language attribute

    const html = page.locator('html');
    const langAttr = await html.getAttribute('lang');

    // Verify lang attribute exists and is not empty
    expect(langAttr).toBeTruthy();

    // Verify it's a valid language code (e.g., 'en', 'en-US', etc.)
    expect(langAttr).toMatch(/^[a-z]{2}(-[A-Z]{2})?$/);

    // Verify it's set to English (as expected for this project)
    expect(langAttr?.toLowerCase()).toContain('en');
  });

  test('TC5: Verify all images have alt attributes', async ({ page }) => {
    // Test Case 5: Verify all images have alt attributes
    // Expected: Every img element has an alt attribute (empty for decorative)

    const images = await page.locator('img').all();

    for (const img of images) {
      const altAttr = await img.getAttribute('alt');
      const src = await img.getAttribute('src');

      // Alt attribute must exist (can be empty string for decorative images)
      expect(altAttr !== null).toBeTruthy();
    }

    // Also check SVGs with role="img" have aria-label or aria-labelledby
    const svgImages = await page.locator('svg[role="img"]').all();

    for (const svg of svgImages) {
      const ariaLabel = await svg.getAttribute('aria-label');
      const ariaLabelledBy = await svg.getAttribute('aria-labelledby');

      // SVG with role="img" must have aria-label or aria-labelledby
      expect(ariaLabel || ariaLabelledBy).toBeTruthy();
    }

    // Use axe-core to verify image-alt rule
    const accessibilityScanResults = await new AxeBuilder({ page })
      .options({ runOnly: ['image-alt', 'svg-img-alt'] })
      .analyze();

    const imageAltViolations = accessibilityScanResults.violations.filter(
      v => v.id === 'image-alt' || v.id === 'svg-img-alt'
    );

    expect(imageAltViolations).toHaveLength(0);
  });

  test('TC6: Check heading hierarchy (h1 through h6) - single h1, no skipped levels', async ({ page }) => {
    // Test Case 6: Check heading hierarchy (h1 through h6)
    // Expected: Single h1, headings don't skip levels

    // Verify there is exactly one h1
    const h1Count = await page.locator('h1').count();
    expect(h1Count).toBe(1);

    // Get all headings in order
    const headings = await page.locator('h1, h2, h3, h4, h5, h6').all();

    let previousLevel = 0;

    for (const heading of headings) {
      const tagName = await heading.evaluate(el => el.tagName.toLowerCase());
      const currentLevel = parseInt(tagName.charAt(1));

      // First heading should be h1
      if (previousLevel === 0) {
        expect(currentLevel).toBe(1);
      } else {
        // Heading level should not skip more than 1 level
        // (e.g., h1 -> h3 is invalid, but h2 -> h2 or h2 -> h3 is fine)
        const levelJump = currentLevel - previousLevel;
        expect(levelJump).toBeLessThanOrEqual(1);
      }

      previousLevel = currentLevel;
    }

    // Also use axe-core to verify heading-order rule
    const accessibilityScanResults = await new AxeBuilder({ page })
      .options({ runOnly: ['heading-order'] })
      .analyze();

    const headingViolations = accessibilityScanResults.violations.filter(
      v => v.id === 'heading-order'
    );

    // Log any heading violations for debugging
    if (headingViolations.length > 0) {
      console.log('Heading order violations found:');
      headingViolations.forEach(v => {
        v.nodes.forEach(n => {
          console.log(`- Element: ${n.html}`);
          console.log(`  Message: ${n.failureSummary}`);
        });
      });
    }

    expect(headingViolations).toHaveLength(0);
  });
});
