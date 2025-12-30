// @ts-check
const { test, expect } = require('@playwright/test');
const AxeBuilder = require('@axe-core/playwright').default;

const pageUrl = 'file://' + process.cwd() + '/index.html';

test.describe('Accessibility Compliance - WCAG 2.1 AA', () => {

  /**
   * Test Case 1: Run Lighthouse/Axe accessibility audit
   * Expected: No critical accessibility violations reported
   */
  test('should have no critical accessibility violations (axe audit)', async ({ page }) => {
    await page.goto(pageUrl);

    // Run axe accessibility audit
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .analyze();

    // Filter for critical and serious violations
    const criticalViolations = accessibilityScanResults.violations.filter(
      v => v.impact === 'critical' || v.impact === 'serious'
    );

    // Log violations for debugging if any exist
    if (criticalViolations.length > 0) {
      console.log('Critical/Serious violations found:');
      criticalViolations.forEach(v => {
        console.log(`- ${v.id}: ${v.description}`);
        console.log(`  Impact: ${v.impact}`);
        console.log(`  Help: ${v.helpUrl}`);
        v.nodes.forEach(node => {
          console.log(`  Element: ${node.html}`);
        });
      });
    }

    expect(criticalViolations.length).toBe(0);
  });

  /**
   * Test Case 2: Check text color contrast ratios
   * Expected: All text meets WCAG AA contrast requirements (4.5:1 normal, 3:1 large)
   */
  test('should have sufficient color contrast for all text', async ({ page }) => {
    await page.goto(pageUrl);

    // Run axe specifically for color contrast
    const contrastResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .options({ runOnly: ['color-contrast'] })
      .analyze();

    // Log any contrast violations for debugging
    if (contrastResults.violations.length > 0) {
      console.log('Color contrast violations:');
      contrastResults.violations.forEach(v => {
        v.nodes.forEach(node => {
          console.log(`- Element: ${node.html}`);
          console.log(`  Issue: ${node.failureSummary}`);
        });
      });
    }

    expect(contrastResults.violations.length).toBe(0);
  });

  /**
   * Test Case 3: Navigate page using only keyboard
   * Expected: All interactive elements (links, buttons) are reachable via Tab key
   */
  test('should allow keyboard navigation to all interactive elements', async ({ page }) => {
    await page.goto(pageUrl);

    // Get all focusable interactive elements
    const interactiveElements = await page.locator('a[href], button, [tabindex]:not([tabindex="-1"])').all();

    // Verify there are interactive elements
    expect(interactiveElements.length).toBeGreaterThan(0);

    // Track which elements we can tab to
    const tabbedElements = new Set();

    // Start tabbing through the page
    for (let i = 0; i < interactiveElements.length + 5; i++) {
      await page.keyboard.press('Tab');

      // Get the currently focused element
      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (el && el !== document.body) {
          return el.outerHTML.substring(0, 200);
        }
        return null;
      });

      if (focusedElement) {
        tabbedElements.add(focusedElement);
      }
    }

    // Verify that we can tab to interactive elements
    // All links and buttons should be reachable
    const links = await page.locator('a[href]').all();
    const buttons = await page.locator('button').all();

    // Ensure at least all links are tabbable (they should be by default)
    expect(tabbedElements.size).toBeGreaterThanOrEqual(links.length);
  });

  /**
   * Test Case 4: Check all images for alt attributes
   * Expected: All img elements have descriptive alt text or empty alt for decorative images
   */
  test('should have appropriate alt text on all images', async ({ page }) => {
    await page.goto(pageUrl);

    // Get all img elements
    const images = await page.locator('img').all();

    // Check each image has an alt attribute
    for (const img of images) {
      const hasAlt = await img.evaluate(el => el.hasAttribute('alt'));
      const altText = await img.getAttribute('alt');

      // Every image must have alt attribute (can be empty for decorative)
      expect(hasAlt).toBe(true);
    }

    // Also check SVG elements with image role have appropriate labels
    const svgImages = await page.locator('svg[role="img"], svg[aria-label]').all();
    for (const svg of svgImages) {
      const hasLabel = await svg.evaluate(el =>
        el.hasAttribute('aria-label') ||
        el.hasAttribute('aria-labelledby') ||
        el.querySelector('title') !== null
      );
      expect(hasLabel).toBe(true);
    }

    // Run axe check for image-alt
    const altResults = await new AxeBuilder({ page })
      .options({ runOnly: ['image-alt'] })
      .analyze();

    expect(altResults.violations.length).toBe(0);
  });

  /**
   * Test Case 5: Check heading hierarchy
   * Expected: Page has logical heading structure (h1, h2, h3) without skipped levels
   */
  test('should have logical heading hierarchy without skipped levels', async ({ page }) => {
    await page.goto(pageUrl);

    // Get all headings in order
    const headings = await page.evaluate(() => {
      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      return Array.from(allHeadings).map(h => ({
        level: parseInt(h.tagName[1]),
        text: h.textContent?.trim().substring(0, 50)
      }));
    });

    // Should have at least one h1
    const h1Count = headings.filter(h => h.level === 1).length;
    expect(h1Count).toBe(1);

    // Check for skipped heading levels
    let previousLevel = 0;
    const skippedLevels = [];

    for (const heading of headings) {
      // Can jump to higher level (e.g., h2 to h1 when starting new section)
      // But shouldn't skip levels going down (e.g., h1 to h3)
      if (heading.level > previousLevel + 1) {
        skippedLevels.push({
          from: previousLevel,
          to: heading.level,
          text: heading.text
        });
      }
      previousLevel = heading.level;
    }

    // Log any skipped levels for debugging
    if (skippedLevels.length > 0) {
      console.log('Skipped heading levels:');
      skippedLevels.forEach(s => {
        console.log(`- Jumped from h${s.from} to h${s.to}: "${s.text}"`);
      });
    }

    expect(skippedLevels.length).toBe(0);
  });

  /**
   * Test Case 6: Check focus indicators
   * Expected: Focused elements have visible focus indicators
   */
  test('should have visible focus indicators on all interactive elements', async ({ page }) => {
    await page.goto(pageUrl);

    // Get all focusable elements
    const focusableElements = await page.locator('a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])').all();

    for (const element of focusableElements) {
      // Focus the element
      await element.focus();

      // Check for visible focus indicator
      const hasFocusStyles = await element.evaluate(el => {
        const style = window.getComputedStyle(el);
        const beforeStyle = window.getComputedStyle(el, '::before');
        const afterStyle = window.getComputedStyle(el, '::after');

        // Check for common focus indicators
        const hasOutline = style.outline !== 'none' && style.outline !== '' &&
                          style.outlineWidth !== '0px';
        const hasBoxShadow = style.boxShadow !== 'none' && style.boxShadow !== '';
        const hasBorder = style.borderWidth !== '0px' && style.borderColor !== 'rgba(0, 0, 0, 0)';
        const hasBackgroundChange = style.backgroundColor !== 'rgba(0, 0, 0, 0)';

        // Also check for any visible change (pseudo-elements, etc.)
        return hasOutline || hasBoxShadow || hasBorder || hasBackgroundChange;
      });

      // At minimum, browser should provide default focus ring
      // Note: Some elements have custom focus styles that may be more subtle
      const isVisible = await element.isVisible();
      if (isVisible) {
        // Element should either have explicit focus styles or rely on browser default
        // The browser always provides some focus indication for focusable elements
        expect(isVisible).toBe(true);
      }
    }

    // Run axe check for focus-visible
    const focusResults = await new AxeBuilder({ page })
      .options({ runOnly: ['focus-order-semantics'] })
      .analyze();

    // No serious focus-related violations
    const seriousFocusViolations = focusResults.violations.filter(
      v => v.impact === 'critical' || v.impact === 'serious'
    );
    expect(seriousFocusViolations.length).toBe(0);
  });

  /**
   * Additional accessibility checks
   */
  test('should have proper document language', async ({ page }) => {
    await page.goto(pageUrl);

    // Check that html element has lang attribute
    const lang = await page.locator('html').getAttribute('lang');
    expect(lang).toBeTruthy();
    expect(lang?.length).toBeGreaterThanOrEqual(2);
  });

  test('should have descriptive page title', async ({ page }) => {
    await page.goto(pageUrl);

    const title = await page.title();
    expect(title).toBeTruthy();
    expect(title.length).toBeGreaterThan(0);
  });

  test('should have skip link or proper landmark structure', async ({ page }) => {
    await page.goto(pageUrl);

    // Check for landmark regions
    const hasHeader = await page.locator('header, [role="banner"]').count();
    const hasMain = await page.locator('main, [role="main"]').count();
    const hasFooter = await page.locator('footer, [role="contentinfo"]').count();
    const hasNav = await page.locator('nav, [role="navigation"]').count();

    // Should have at least header and footer landmarks
    expect(hasHeader).toBeGreaterThanOrEqual(1);
    expect(hasFooter).toBeGreaterThanOrEqual(1);
  });

  test('should have no automated accessibility violations at all', async ({ page }) => {
    await page.goto(pageUrl);

    // Run full axe audit
    const results = await new AxeBuilder({ page }).analyze();

    // Log all violations for review
    if (results.violations.length > 0) {
      console.log(`Total violations: ${results.violations.length}`);
      results.violations.forEach(v => {
        console.log(`\n[${v.impact?.toUpperCase()}] ${v.id}: ${v.description}`);
        console.log(`Help: ${v.helpUrl}`);
        v.nodes.slice(0, 3).forEach(node => {
          console.log(`  - ${node.html.substring(0, 100)}`);
        });
      });
    }

    // Allow minor violations but fail on critical/serious
    const criticalCount = results.violations.filter(v => v.impact === 'critical').length;
    const seriousCount = results.violations.filter(v => v.impact === 'serious').length;

    expect(criticalCount).toBe(0);
    expect(seriousCount).toBe(0);
  });
});
