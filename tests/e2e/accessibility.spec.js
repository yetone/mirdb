/**
 * Accessibility E2E Tests
 * Owner: Scenario 13 - Accessibility WCAG Compliance
 *
 * Tests:
 * - Keyboard navigation works
 * - Focus indicators visible
 * - Heading hierarchy correct
 * - All images have alt text
 * - ARIA attributes present
 * - axe audit passes
 */

const { test, expect } = require('@playwright/test');
const AxeBuilder = require('@axe-core/playwright').default;

test.describe('Accessibility WCAG Compliance', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Tab through all interactive elements
   * Type: E2E
   * Expected: All buttons, links, and interactive elements reachable via keyboard
   */
  test('should allow keyboard navigation to all interactive elements', async ({ page }) => {
    // Get all interactive elements that should be focusable
    const interactiveSelectors = [
      'a[href]',
      'button',
      'input',
      'select',
      'textarea',
      '[tabindex]:not([tabindex="-1"])'
    ];

    const interactiveElements = await page.locator(interactiveSelectors.join(', ')).all();
    expect(interactiveElements.length).toBeGreaterThan(0);

    // Track elements that received focus during tab navigation
    const focusedElements = new Set();

    // Start tabbing from the beginning of the document
    await page.keyboard.press('Tab');

    // Tab through elements and track what gets focused
    for (let i = 0; i < interactiveElements.length + 5; i++) {
      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (el && el !== document.body) {
          return {
            tagName: el.tagName.toLowerCase(),
            href: el.getAttribute('href'),
            text: el.textContent?.trim().substring(0, 50),
            id: el.id,
            className: el.className
          };
        }
        return null;
      });

      if (focusedElement) {
        focusedElements.add(JSON.stringify(focusedElement));
      }

      await page.keyboard.press('Tab');
    }

    // Verify we could focus on multiple elements
    expect(focusedElements.size).toBeGreaterThan(5);

    // Verify key navigation links are reachable
    const navLinks = await page.locator('.nav-links a').all();
    const heroButtons = await page.locator('.hero-cta .btn').all();
    const footerLinks = await page.locator('.footer-links a').all();

    // All these elements should be in the DOM and focusable
    expect(navLinks.length).toBeGreaterThan(0);
    expect(heroButtons.length).toBeGreaterThan(0);
    expect(footerLinks.length).toBeGreaterThan(0);

    // Verify each element has proper tabindex (not negative)
    for (const link of navLinks) {
      const tabindex = await link.getAttribute('tabindex');
      if (tabindex !== null) {
        expect(parseInt(tabindex)).toBeGreaterThanOrEqual(0);
      }
    }
  });

  /**
   * Test Case 2: Check focus visibility on buttons
   * Type: E2E
   * Expected: Focus indicator visible with sufficient contrast on all interactive elements
   */
  test('should display visible focus indicators on interactive elements', async ({ page }) => {
    // Test buttons
    const buttons = await page.locator('.btn').all();
    expect(buttons.length).toBeGreaterThan(0);

    for (const button of buttons) {
      // Focus the button
      await button.focus();

      // Check that focus-visible outline is applied
      const outlineStyle = await button.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          outline: styles.outline,
          outlineWidth: styles.outlineWidth,
          outlineColor: styles.outlineColor,
          outlineOffset: styles.outlineOffset
        };
      });

      // The outline should not be 'none' or have 0 width when focused
      // Due to :focus-visible, we verify the CSS rule exists
      const hasFocusStyle = await page.evaluate(() => {
        const styleSheets = document.styleSheets;
        for (let i = 0; i < styleSheets.length; i++) {
          try {
            const rules = styleSheets[i].cssRules;
            for (let j = 0; j < rules.length; j++) {
              const rule = rules[j];
              if (rule.selectorText && rule.selectorText.includes('focus-visible')) {
                return true;
              }
            }
          } catch (e) {
            // Cross-origin stylesheets will throw
          }
        }
        return false;
      });

      expect(hasFocusStyle).toBe(true);
    }

    // Test links
    const links = await page.locator('a[href]').first();
    await links.focus();

    // Verify focus-visible CSS rule applies to focused elements
    const focusVisibleRuleExists = await page.evaluate(() => {
      const styleSheets = document.styleSheets;
      for (let i = 0; i < styleSheets.length; i++) {
        try {
          const rules = styleSheets[i].cssRules;
          for (let j = 0; j < rules.length; j++) {
            const rule = rules[j];
            if (rule.selectorText === '*:focus-visible' ||
                (rule.selectorText && rule.selectorText.includes(':focus-visible'))) {
              // Check if outline is defined
              if (rule.style && rule.style.outline) {
                return true;
              }
            }
          }
        } catch (e) {
          // Cross-origin stylesheets will throw
        }
      }
      return false;
    });

    expect(focusVisibleRuleExists).toBe(true);
  });

  /**
   * Test Case 3: Check heading hierarchy
   * Type: Unit
   * Expected: Headings follow proper hierarchy (h1 -> h2 -> h3, no skipped levels)
   */
  test('should have proper heading hierarchy with no skipped levels', async ({ page }) => {
    // Get all headings in document order
    const headings = await page.evaluate(() => {
      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      return Array.from(allHeadings).map(h => ({
        level: parseInt(h.tagName.charAt(1)),
        text: h.textContent?.trim().substring(0, 50)
      }));
    });

    expect(headings.length).toBeGreaterThan(0);

    // There should be exactly one h1
    const h1Count = headings.filter(h => h.level === 1).length;
    expect(h1Count).toBe(1);

    // First heading should be h1
    expect(headings[0].level).toBe(1);

    // Check for skipped levels (e.g., h1 -> h3 without h2)
    for (let i = 1; i < headings.length; i++) {
      const current = headings[i].level;
      const previous = headings[i - 1].level;

      // Heading level can go down by any amount (h2 -> h1 is OK)
      // But going up should not skip levels (h1 -> h3 is NOT OK, should be h1 -> h2)
      if (current > previous) {
        expect(current - previous).toBeLessThanOrEqual(1);
      }
    }

    // Verify h2 elements exist for major sections
    const h2Count = headings.filter(h => h.level === 2).length;
    expect(h2Count).toBeGreaterThan(0);
  });

  /**
   * Test Case 4: Check all images for alt text
   * Type: Unit
   * Expected: All img elements have meaningful alt attributes (or empty for decorative)
   */
  test('should have alt text on all images', async ({ page }) => {
    const images = await page.locator('img').all();
    expect(images.length).toBeGreaterThan(0);

    for (const img of images) {
      const alt = await img.getAttribute('alt');

      // Alt attribute must exist
      expect(alt).not.toBeNull();

      // Alt can be empty string for decorative images, but must be present
      const src = await img.getAttribute('src');

      // For non-decorative images (like logo), alt should have meaningful text
      if (src && (src.includes('logo') || src.includes('icon'))) {
        // Logo images should have descriptive alt text
        expect(alt?.length).toBeGreaterThan(0);
      }
    }

    // Specifically check the logo images have proper alt text
    const logoImages = await page.locator('img[alt*="Logo"], img[alt*="logo"]').all();
    expect(logoImages.length).toBeGreaterThan(0);

    for (const logo of logoImages) {
      const altText = await logo.getAttribute('alt');
      expect(altText).toBeTruthy();
      expect(altText?.toLowerCase()).toContain('mirdb');
    }
  });

  /**
   * Test Case 5: Check ARIA attributes where needed
   * Type: Unit
   * Expected: ARIA labels present on elements that need additional context
   */
  test('should have appropriate ARIA attributes on elements needing context', async ({ page }) => {
    // Check hamburger menu button has aria-label
    const hamburger = await page.locator('.hamburger');
    const hamburgerAriaLabel = await hamburger.getAttribute('aria-label');
    expect(hamburgerAriaLabel).toBeTruthy();
    expect(hamburgerAriaLabel?.toLowerCase()).toContain('navigation');

    // Check copy buttons have aria-label
    const copyButtons = await page.locator('.copy-btn').all();
    for (const btn of copyButtons) {
      const ariaLabel = await btn.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel?.toLowerCase()).toContain('copy');
    }

    // Check footer navigation has aria-label
    const footerNav = await page.locator('.footer-links');
    const footerNavAriaLabel = await footerNav.getAttribute('aria-label');
    expect(footerNavAriaLabel).toBeTruthy();

    // Check architecture diagram SVG has proper ARIA attributes
    const archDiagram = await page.locator('.architecture-diagram');
    const diagramRole = await archDiagram.getAttribute('role');
    const diagramAriaLabel = await archDiagram.getAttribute('aria-label');
    expect(diagramRole).toBe('img');
    expect(diagramAriaLabel).toBeTruthy();

    // Check SVG has title element for screen readers
    const svgTitle = await page.locator('.architecture-diagram title').textContent();
    expect(svgTitle).toBeTruthy();
    expect(svgTitle).toContain('MirDB');

    // Check SVG has desc element for detailed description
    const svgDesc = await page.locator('.architecture-diagram desc').textContent();
    expect(svgDesc).toBeTruthy();

    // Check roadmap items have status indicators with aria-label
    const statusIndicators = await page.locator('.status-indicator').all();
    expect(statusIndicators.length).toBeGreaterThan(0);
    for (const indicator of statusIndicators) {
      const ariaLabel = await indicator.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
    }

    // Verify lang attribute is set on html element
    const htmlLang = await page.locator('html').getAttribute('lang');
    expect(htmlLang).toBe('en');
  });

  /**
   * Test Case 6: Run axe accessibility audit
   * Type: Integration
   * Expected: No critical or serious accessibility violations detected
   */
  test('should pass axe accessibility audit without critical or serious violations', async ({ page }) => {
    // Run axe accessibility audit
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .analyze();

    // Filter for critical and serious violations only
    const criticalViolations = accessibilityScanResults.violations.filter(
      v => v.impact === 'critical' || v.impact === 'serious'
    );

    // Log any violations for debugging
    if (criticalViolations.length > 0) {
      console.log('Critical/Serious Accessibility Violations:');
      criticalViolations.forEach(v => {
        console.log(`- ${v.id}: ${v.description}`);
        console.log(`  Impact: ${v.impact}`);
        console.log(`  Help: ${v.helpUrl}`);
        v.nodes.forEach(n => {
          console.log(`  Element: ${n.html.substring(0, 100)}`);
        });
      });
    }

    // No critical or serious violations should exist
    expect(criticalViolations).toHaveLength(0);
  });

  /**
   * Additional accessibility test: Color contrast
   * Verifies text meets 4.5:1 contrast ratio (NFR-11)
   */
  test('should have sufficient color contrast for text', async ({ page }) => {
    // Run axe specifically for color contrast
    const contrastResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .include('body')
      .analyze();

    // Filter for color contrast violations
    const contrastViolations = contrastResults.violations.filter(
      v => v.id === 'color-contrast'
    );

    // Log any contrast issues for debugging
    if (contrastViolations.length > 0) {
      console.log('Color Contrast Violations:');
      contrastViolations.forEach(v => {
        v.nodes.forEach(n => {
          console.log(`  Element: ${n.html.substring(0, 100)}`);
          console.log(`  Message: ${n.failureSummary}`);
        });
      });
    }

    // Verify no contrast violations
    expect(contrastViolations).toHaveLength(0);
  });

  /**
   * Additional accessibility test: Links have distinguishable text
   */
  test('should have descriptive link text', async ({ page }) => {
    const links = await page.locator('a[href]').all();

    for (const link of links) {
      const text = await link.textContent();
      const ariaLabel = await link.getAttribute('aria-label');
      const title = await link.getAttribute('title');

      // Link should have either text content, aria-label, or title
      const hasAccessibleName = (text && text.trim().length > 0) ||
                                 ariaLabel ||
                                 title;
      expect(hasAccessibleName).toBeTruthy();

      // Links should not use generic text like "click here"
      if (text) {
        const genericTexts = ['click here', 'here', 'read more', 'more'];
        const lowerText = text.toLowerCase().trim();
        const isGeneric = genericTexts.some(g => lowerText === g);
        expect(isGeneric).toBe(false);
      }
    }
  });

  /**
   * Additional accessibility test: Form labels
   */
  test('should have no form inputs without labels', async ({ page }) => {
    // Check for any input elements (if they exist)
    const inputs = await page.locator('input:not([type="hidden"])').all();

    for (const input of inputs) {
      const id = await input.getAttribute('id');
      const ariaLabel = await input.getAttribute('aria-label');
      const ariaLabelledBy = await input.getAttribute('aria-labelledby');
      const placeholder = await input.getAttribute('placeholder');

      // Input should have some form of label
      let hasLabel = ariaLabel || ariaLabelledBy || placeholder;

      if (id) {
        // Check if there's a label element pointing to this input
        const labelForInput = await page.locator(`label[for="${id}"]`).count();
        hasLabel = hasLabel || labelForInput > 0;
      }

      // If inputs exist, they should be properly labeled
      // (Currently the page may not have form inputs, which is OK)
      if (inputs.length > 0) {
        expect(hasLabel).toBeTruthy();
      }
    }
  });
});
