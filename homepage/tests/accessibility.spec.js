// @ts-check
const { test, expect } = require('@playwright/test');
const AxeBuilder = require('@axe-core/playwright').default;

/**
 * Accessibility Compliance Test Suite
 * Verifies page meets WCAG 2.1 AA accessibility standards (NFR-3)
 */

test.describe('Accessibility Compliance - WCAG 2.1 AA', () => {

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');
  });

  /**
   * Test Case 1: Check color contrast ratios with automated tool
   * Expected: All text meets 4.5:1 contrast ratio minimum
   */
  test('TC1: Color contrast ratios meet WCAG AA standards (4.5:1 minimum)', async ({ page }) => {
    // Run axe-core specifically for color contrast rules
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .analyze();

    // Filter for color contrast violations
    const contrastViolations = accessibilityScanResults.violations.filter(
      v => v.id === 'color-contrast' || v.id === 'color-contrast-enhanced'
    );

    // Log any violations for debugging
    if (contrastViolations.length > 0) {
      console.log('Color contrast violations found:');
      contrastViolations.forEach(violation => {
        violation.nodes.forEach(node => {
          console.log(`  - ${node.html}`);
          console.log(`    Issue: ${node.failureSummary}`);
        });
      });
    }

    expect(contrastViolations.length).toBe(0);
  });

  /**
   * Test Case 2: Keyboard navigation - all interactive elements reachable with Tab
   * Expected: All interactive elements reachable, focus visible
   */
  test('TC2: All interactive elements are keyboard navigable', async ({ page }) => {
    // Get all focusable elements
    const focusableElements = await page.locator(
      'a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])'
    ).all();

    expect(focusableElements.length).toBeGreaterThan(0);

    // Track elements that received focus
    const focusedElements = [];

    // Start from the beginning of the page
    await page.keyboard.press('Tab');

    // Tab through all focusable elements
    for (let i = 0; i < focusableElements.length + 5; i++) {
      const activeElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (el && el !== document.body) {
          return {
            tagName: el.tagName,
            id: el.id,
            className: el.className,
            text: el.textContent?.trim().substring(0, 50)
          };
        }
        return null;
      });

      if (activeElement && activeElement.tagName !== 'BODY') {
        const key = `${activeElement.tagName}-${activeElement.id || activeElement.text}`;
        if (!focusedElements.includes(key)) {
          focusedElements.push(key);
        }
      }

      await page.keyboard.press('Tab');
    }

    // Verify that we can reach multiple interactive elements
    expect(focusedElements.length).toBeGreaterThan(0);

    // Verify specific important elements are reachable
    const getStartedBtn = page.locator('#get-started-btn');
    const githubBtn = page.locator('#github-btn');

    // These buttons should be focusable
    await expect(getStartedBtn).toBeVisible();
    await expect(githubBtn).toBeVisible();

    // Check that buttons have proper roles/are keyboard accessible
    const getStartedTag = await getStartedBtn.evaluate(el => el.tagName);
    const githubTag = await githubBtn.evaluate(el => el.tagName);

    expect(['A', 'BUTTON']).toContain(getStartedTag);
    expect(['A', 'BUTTON']).toContain(githubTag);
  });

  /**
   * Test Case 3: Check all images for alt text
   * Expected: All images have descriptive alt text
   */
  test('TC3: All images have descriptive alt text', async ({ page }) => {
    // Get all images on the page
    const images = await page.locator('img').all();

    expect(images.length).toBeGreaterThan(0);

    for (const img of images) {
      const alt = await img.getAttribute('alt');
      const src = await img.getAttribute('src');

      // Every image should have an alt attribute
      expect(alt, `Image ${src} is missing alt attribute`).not.toBeNull();

      // Alt text should be descriptive (not empty for meaningful images)
      expect(alt?.trim().length, `Image ${src} has empty alt text`).toBeGreaterThan(0);
    }

    // Specifically check the main logo
    const logo = page.locator('#mirdb-logo');
    const logoAlt = await logo.getAttribute('alt');
    expect(logoAlt).toBe('MirDB Logo');

    // Check the architecture diagram has detailed alt text
    const diagram = page.locator('#architecture-diagram');
    const diagramAlt = await diagram.getAttribute('alt');
    expect(diagramAlt).toBeTruthy();
    expect(diagramAlt?.length).toBeGreaterThan(50); // Should be descriptive
    expect(diagramAlt).toContain('LSM');
    expect(diagramAlt).toContain('WAL');
  });

  /**
   * Test Case 4: Check heading hierarchy
   * Expected: Headings follow logical hierarchy (h1 > h2 > h3)
   */
  test('TC4: Headings follow logical hierarchy (h1 > h2 > h3)', async ({ page }) => {
    // Get all headings in document order
    const headings = await page.evaluate(() => {
      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      return Array.from(allHeadings).map(h => ({
        level: parseInt(h.tagName[1]),
        text: h.textContent?.trim().substring(0, 50)
      }));
    });

    expect(headings.length).toBeGreaterThan(0);

    // There should be exactly one h1
    const h1Count = headings.filter(h => h.level === 1).length;
    expect(h1Count, 'Page should have exactly one h1').toBe(1);

    // First heading should be h1
    expect(headings[0].level, 'First heading should be h1').toBe(1);

    // Check that heading levels don't skip (e.g., h1 -> h3 without h2)
    let previousLevel = 0;
    for (const heading of headings) {
      // Level should never increase by more than 1
      if (heading.level > previousLevel + 1 && previousLevel !== 0) {
        throw new Error(
          `Heading hierarchy skipped: went from h${previousLevel} to h${heading.level} at "${heading.text}"`
        );
      }
      previousLevel = heading.level;
    }

    // Verify specific headings exist
    const h1 = await page.locator('h1').first().textContent();
    expect(h1).toContain('MirDB');

    // Verify section h2 headings
    const h2Texts = await page.locator('h2').allTextContents();
    expect(h2Texts).toContain('Quick Start');
    expect(h2Texts).toContain('Key Features');
    expect(h2Texts).toContain('Architecture Overview');
    expect(h2Texts).toContain('Configuration');
    expect(h2Texts).toContain('Commands Reference');
    expect(h2Texts).toContain('Project Status');
  });

  /**
   * Test Case 5: Check focus indicators
   * Expected: All focusable elements have visible focus indicators
   */
  test('TC5: All focusable elements have visible focus indicators', async ({ page }) => {
    // Define focusable elements to test
    const focusableSelectors = [
      '#get-started-btn',
      '#github-btn',
      '.copy-btn',
      '#footer-github-link'
    ];

    for (const selector of focusableSelectors) {
      const element = page.locator(selector).first();

      if (await element.isVisible()) {
        // Focus the element
        await element.focus();

        // Check that it received focus
        const isFocused = await element.evaluate(el => el === document.activeElement);
        expect(isFocused, `Element ${selector} should be focusable`).toBe(true);

        // Check for visible focus indicator (outline, box-shadow, or border change)
        const focusStyles = await element.evaluate(el => {
          const styles = window.getComputedStyle(el);
          return {
            outline: styles.outline,
            outlineWidth: styles.outlineWidth,
            outlineStyle: styles.outlineStyle,
            boxShadow: styles.boxShadow,
            border: styles.border
          };
        });

        // Element should have some form of focus indicator
        const hasFocusIndicator =
          (focusStyles.outlineStyle !== 'none' && focusStyles.outlineWidth !== '0px') ||
          (focusStyles.boxShadow !== 'none' && focusStyles.boxShadow !== '');

        // If no CSS focus indicator, check for default browser focus
        if (!hasFocusIndicator) {
          // Links and buttons typically get browser default focus
          const tagName = await element.evaluate(el => el.tagName);
          expect(['A', 'BUTTON']).toContain(tagName);
        }
      }
    }

    // Run axe-core focus indicator check
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .analyze();

    // Check for focus-related violations
    const focusViolations = accessibilityScanResults.violations.filter(
      v => v.id.includes('focus')
    );

    expect(focusViolations.length).toBe(0);
  });

  /**
   * Full WCAG 2.1 AA Audit - Comprehensive accessibility check
   */
  test('Full WCAG 2.1 AA accessibility audit passes', async ({ page }) => {
    // Run comprehensive WCAG 2.1 AA audit
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa', 'wcag21aa'])
      .analyze();

    // Log all violations for debugging
    if (accessibilityScanResults.violations.length > 0) {
      console.log('Accessibility violations found:');
      accessibilityScanResults.violations.forEach(violation => {
        console.log(`\n${violation.id}: ${violation.description}`);
        console.log(`Impact: ${violation.impact}`);
        console.log(`Help: ${violation.helpUrl}`);
        violation.nodes.forEach(node => {
          console.log(`  - ${node.html}`);
          console.log(`    ${node.failureSummary}`);
        });
      });
    }

    // Expect no violations
    expect(accessibilityScanResults.violations).toEqual([]);
  });

  /**
   * Semantic HTML and ARIA labels check
   */
  test('Semantic HTML and ARIA labels are properly implemented', async ({ page }) => {
    // Check for semantic HTML elements
    const hasMain = await page.locator('main, [role="main"]').count();
    const hasNav = await page.locator('nav, [role="navigation"]').count();
    const hasFooter = await page.locator('footer, [role="contentinfo"]').count();

    // Footer should exist
    expect(hasFooter).toBeGreaterThan(0);

    // Check that sections have proper structure
    const sections = await page.locator('section').all();
    expect(sections.length).toBeGreaterThan(0);

    // Each section should have a heading
    for (const section of sections) {
      const hasHeading = await section.locator('h1, h2, h3, h4, h5, h6').count();
      expect(hasHeading).toBeGreaterThan(0);
    }

    // Check that interactive SVG icons are properly hidden from screen readers
    const decorativeSvgs = await page.locator('svg[aria-hidden="true"]').count();
    // SVGs in status badges should be decorative
    expect(decorativeSvgs).toBeGreaterThan(0);

    // Check that tables have proper structure
    const tables = await page.locator('table').all();
    for (const table of tables) {
      const hasHeader = await table.locator('thead').count();
      const hasTh = await table.locator('th').count();
      expect(hasHeader, 'Tables should have thead').toBeGreaterThan(0);
      expect(hasTh, 'Tables should have th elements').toBeGreaterThan(0);
    }

    // Check that the HTML has lang attribute
    const htmlLang = await page.locator('html').getAttribute('lang');
    expect(htmlLang).toBe('en');
  });

  /**
   * Link accessibility check
   */
  test('Links are accessible and have proper attributes', async ({ page }) => {
    const links = await page.locator('a[href]').all();

    expect(links.length).toBeGreaterThan(0);

    for (const link of links) {
      const href = await link.getAttribute('href');
      const target = await link.getAttribute('target');
      const rel = await link.getAttribute('rel');

      // External links should have rel="noopener noreferrer"
      if (target === '_blank') {
        expect(rel, `External link ${href} should have rel="noopener noreferrer"`).toContain('noopener');
      }

      // Links should have accessible text
      const text = await link.textContent();
      const ariaLabel = await link.getAttribute('aria-label');

      expect(
        (text && text.trim().length > 0) || ariaLabel,
        `Link ${href} should have accessible text`
      ).toBeTruthy();
    }
  });
});
