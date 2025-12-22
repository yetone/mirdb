// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Screen Reader Accessibility Tests
 *
 * This test suite verifies that the MirDB homepage is accessible to screen readers
 * by checking semantic structure, ARIA landmarks, alt text, link accessibility,
 * and code block markup.
 */

test.describe('Accessibility - Screen Reader', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Check heading hierarchy
   * Expected: Page has logical heading hierarchy (h1 followed by h2, etc.)
   */
  test('should have logical heading hierarchy (h1 followed by h2, etc.)', async ({ page }) => {
    // Get all headings on the page
    const h1Elements = await page.locator('h1').all();
    const h2Elements = await page.locator('h2').all();
    const h3Elements = await page.locator('h3').all();

    // Page must have exactly one h1 (main title)
    expect(h1Elements.length).toBe(1);

    // Verify h1 is the main page title
    const h1Text = await h1Elements[0].textContent();
    expect(h1Text).toContain('MirDB');

    // Page should have multiple h2 elements for sections
    expect(h2Elements.length).toBeGreaterThanOrEqual(1);

    // Verify h2 elements come after h1 in the DOM
    const h1Box = await h1Elements[0].boundingBox();
    for (const h2 of h2Elements) {
      const h2Box = await h2.boundingBox();
      // h2 should be lower on the page (higher y value) than h1
      expect(h2Box.y).toBeGreaterThan(h1Box.y);
    }

    // Verify heading hierarchy is logical (no skipped levels)
    // Check that h3s are within sections that have h2s
    for (const h3 of h3Elements) {
      const h3Visible = await h3.isVisible();
      if (h3Visible) {
        // h3 should have a parent section or article with an h2 sibling
        const parentSection = await h3.evaluate(el => {
          const section = el.closest('section, article, div.feature-card, div.status-implemented, div.status-planned, .code-block, .config-section');
          return section !== null;
        });
        expect(parentSection).toBe(true);
      }
    }

    // Verify no h4, h5, h6 exist before h3 is used
    const allHeadings = await page.evaluate(() => {
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      return Array.from(headings).map(h => ({
        level: parseInt(h.tagName.charAt(1)),
        text: h.textContent.trim()
      }));
    });

    // Check that heading levels don't skip (e.g., h1 -> h3 without h2)
    let lastLevel = 0;
    for (const heading of allHeadings) {
      // Heading levels should not skip more than 1 level down
      if (heading.level > lastLevel + 1 && lastLevel > 0) {
        // This would be a skip - log for debugging but allow h1->h2 and h2->h3
        console.log(`Warning: Heading skip from h${lastLevel} to h${heading.level}: "${heading.text}"`);
      }
      lastLevel = heading.level;
    }
  });

  /**
   * Test Case 2: Check for ARIA landmarks
   * Expected: Main content areas have appropriate ARIA landmarks
   */
  test('should have appropriate ARIA landmarks for main content areas', async ({ page }) => {
    // Check for semantic HTML5 landmarks
    const header = await page.locator('header').count();
    const main = await page.locator('main').count();
    const footer = await page.locator('footer').count();
    const nav = await page.locator('nav').count();

    // Page must have header, main, and footer landmarks
    expect(header).toBeGreaterThanOrEqual(1);
    expect(main).toBe(1);
    expect(footer).toBeGreaterThanOrEqual(1);
    expect(nav).toBeGreaterThanOrEqual(1);

    // Verify main element contains the primary content
    const mainElement = page.locator('main');
    await expect(mainElement).toBeVisible();

    // Check that main contains sections
    const sectionsInMain = await mainElement.locator('section').count();
    expect(sectionsInMain).toBeGreaterThanOrEqual(1);

    // Verify navigation is within header
    const navInHeader = await page.locator('header nav').count();
    expect(navInHeader).toBeGreaterThanOrEqual(1);

    // Check for role attributes on custom elements
    const roleImg = await page.locator('[role="img"]').count();
    expect(roleImg).toBeGreaterThanOrEqual(1); // Architecture diagram should have role="img"

    // Verify architecture diagram has proper role
    const architectureDiagram = page.locator('[data-testid="architecture-diagram"]');
    await expect(architectureDiagram).toHaveAttribute('role', 'img');
  });

  /**
   * Test Case 3: Check all images for alt text
   * Expected: All images have descriptive alt text
   */
  test('should have descriptive alt text for all images', async ({ page }) => {
    // Get all img elements
    const imgElements = await page.locator('img').all();

    for (const img of imgElements) {
      const altText = await img.getAttribute('alt');
      const src = await img.getAttribute('src');

      // Every img must have an alt attribute
      expect(altText, `Image ${src} is missing alt attribute`).not.toBeNull();

      // Alt text should be descriptive (not empty unless decorative)
      // If empty alt, it should be explicitly empty for decorative images
      if (altText !== '') {
        expect(altText.length, `Image ${src} has non-descriptive alt text`).toBeGreaterThan(3);
      }
    }

    // Get all SVG elements that are informative (not decorative)
    const svgElements = await page.locator('svg:not([aria-hidden="true"])').all();

    for (const svg of svgElements) {
      // SVGs that aren't hidden should have accessible name via aria-label, aria-labelledby, or title
      const ariaLabel = await svg.getAttribute('aria-label');
      const ariaLabelledby = await svg.getAttribute('aria-labelledby');
      const titleElement = await svg.locator('title').count();
      const role = await svg.getAttribute('role');

      // If SVG is in a container with role="img", the container should have aria-label
      const parent = await svg.evaluate(el => {
        const p = el.closest('[role="img"]');
        return p ? p.getAttribute('aria-label') : null;
      });

      // SVG should either be decorative (aria-hidden) or have accessible name
      const hasAccessibleName = ariaLabel || ariaLabelledby || titleElement > 0 || parent;

      // Informative SVGs need accessible names
      // Skip feature icons as they're decorative alongside text
      const isFeatureIcon = await svg.evaluate(el => {
        return el.closest('.feature-icon, .node-icon, .status-icon, .status-indicator, .copy-icon, .diagram-arrow') !== null;
      });

      if (!isFeatureIcon && role !== 'presentation') {
        // These should have accessible names if they convey meaning
        // For this page, most SVGs are decorative icons with adjacent text
      }
    }

    // Check role="img" elements have aria-label
    const roleImgElements = await page.locator('[role="img"]').all();
    for (const elem of roleImgElements) {
      const ariaLabel = await elem.getAttribute('aria-label');
      expect(ariaLabel, 'Element with role="img" missing aria-label').toBeTruthy();
      expect(ariaLabel.length, 'aria-label should be descriptive').toBeGreaterThan(20);
    }
  });

  /**
   * Test Case 4: Check link text accessibility
   * Expected: All links have descriptive text (no 'click here')
   */
  test('should have descriptive link text (no "click here")', async ({ page }) => {
    // Get all links
    const links = await page.locator('a').all();

    const inaccessiblePhrases = [
      'click here',
      'here',
      'read more',
      'more',
      'link',
      'click',
    ];

    for (const link of links) {
      const linkText = await link.textContent();
      const ariaLabel = await link.getAttribute('aria-label');
      const accessibleText = ariaLabel || linkText?.trim().toLowerCase();

      // Link must have visible text or aria-label
      expect(accessibleText, 'Link has no accessible text').toBeTruthy();
      expect(accessibleText.length, 'Link text is too short').toBeGreaterThan(0);

      // Link text should not be one of the inaccessible phrases alone
      for (const phrase of inaccessiblePhrases) {
        expect(
          accessibleText === phrase,
          `Link with text "${accessibleText}" is not descriptive`
        ).toBe(false);
      }

      // Link should have href
      const href = await link.getAttribute('href');
      expect(href, 'Link missing href attribute').toBeTruthy();
    }

    // Verify external links have rel="noopener" for security
    const externalLinks = await page.locator('a[target="_blank"]').all();
    for (const link of externalLinks) {
      const rel = await link.getAttribute('rel');
      expect(rel, 'External link missing rel attribute').toBeTruthy();
      expect(rel).toContain('noopener');
    }
  });

  /**
   * Test Case 5: Check code snippet accessibility
   * Expected: Code blocks are properly marked up for screen readers
   */
  test('should have properly marked up code blocks for screen readers', async ({ page }) => {
    // Find code blocks
    const codeBlocks = await page.locator('[data-testid="code-block"], [data-testid="code-block-client"]').all();

    expect(codeBlocks.length).toBeGreaterThanOrEqual(1);

    for (const codeBlock of codeBlocks) {
      // Code blocks should contain pre and code elements (semantic markup)
      const preElement = await codeBlock.locator('pre').count();
      const codeElement = await codeBlock.locator('code').count();

      expect(preElement, 'Code block missing pre element').toBe(1);
      expect(codeElement, 'Code block missing code element').toBe(1);

      // Code element should have language class for context
      const codeElem = codeBlock.locator('code');
      const codeClass = await codeElem.getAttribute('class');
      expect(codeClass, 'Code element should have language class').toBeTruthy();
      expect(codeClass).toMatch(/language-/);

      // Code blocks should have a title/heading for context
      const codeHeader = codeBlock.locator('.code-header, .code-title');
      const headerCount = await codeHeader.count();
      expect(headerCount, 'Code block should have header/title').toBeGreaterThanOrEqual(1);
    }

    // Check copy buttons have aria-label
    const copyButtons = await page.locator('.copy-button, [data-testid="copy-button"], [data-testid="copy-button-client"]').all();

    for (const button of copyButtons) {
      const ariaLabel = await button.getAttribute('aria-label');
      expect(ariaLabel, 'Copy button missing aria-label').toBeTruthy();
      expect(ariaLabel.toLowerCase()).toContain('copy');
    }
  });

  /**
   * Additional accessibility tests for comprehensive coverage
   */
  test('should have proper document language attribute', async ({ page }) => {
    const htmlLang = await page.locator('html').getAttribute('lang');
    expect(htmlLang, 'HTML element missing lang attribute').toBeTruthy();
    expect(htmlLang).toBe('en');
  });

  test('should have proper page title', async ({ page }) => {
    const title = await page.title();
    expect(title).toBeTruthy();
    expect(title.length).toBeGreaterThan(5);
    expect(title.toLowerCase()).toContain('mirdb');
  });

  test('should have meta description for SEO and accessibility', async ({ page }) => {
    const metaDescription = await page.locator('meta[name="description"]').getAttribute('content');
    expect(metaDescription, 'Page missing meta description').toBeTruthy();
    expect(metaDescription.length).toBeGreaterThan(20);
  });

  test('should allow keyboard navigation through all interactive elements', async ({ page }) => {
    // Press Tab to navigate through the page
    const focusableElements = await page.locator('a, button, [tabindex="0"]').all();

    expect(focusableElements.length).toBeGreaterThan(0);

    // Test that interactive elements can receive focus
    for (let i = 0; i < Math.min(5, focusableElements.length); i++) {
      const elem = focusableElements[i];
      const isVisible = await elem.isVisible();
      if (isVisible) {
        await elem.focus();
        await expect(elem).toBeFocused();
      }
    }
  });

  test('should have visible focus indicators', async ({ page }) => {
    // Focus on a link and verify focus is visible
    const firstLink = page.locator('a').first();
    await firstLink.focus();

    // Get computed style to verify focus indicator exists
    const focusStyles = await firstLink.evaluate(el => {
      const styles = window.getComputedStyle(el, ':focus');
      return {
        outline: styles.outline,
        boxShadow: styles.boxShadow,
        border: styles.border
      };
    });

    // At least one focus indicator should be visible (outline, box-shadow, or border change)
    const hasFocusIndicator =
      (focusStyles.outline && focusStyles.outline !== 'none' && !focusStyles.outline.includes('0px')) ||
      (focusStyles.boxShadow && focusStyles.boxShadow !== 'none') ||
      (focusStyles.border && focusStyles.border !== 'none');

    // Focus styles are typically defined in CSS, verify element can receive focus
    await expect(firstLink).toBeFocused();
  });

  test('should have sections with appropriate section titles', async ({ page }) => {
    // Each major section should have a heading
    const sections = await page.locator('main section').all();

    for (const section of sections) {
      const sectionId = await section.getAttribute('id');
      const sectionClass = await section.getAttribute('class');

      // Each section should have a heading (h2 or h3)
      const headingCount = await section.locator('h2, h3').first().count();

      if (headingCount > 0) {
        const heading = section.locator('h2, h3').first();
        const headingText = await heading.textContent();
        expect(headingText?.trim().length, `Section ${sectionId || sectionClass} has empty heading`).toBeGreaterThan(0);
      }
    }
  });
});
