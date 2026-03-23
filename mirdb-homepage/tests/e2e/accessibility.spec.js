/**
 * Accessibility Compliance Tests
 * Owner: Scenario 9 - Accessibility Compliance
 *
 * Test cases:
 * - Semantic heading structure (single h1, logical hierarchy)
 * - Image alt attributes
 * - Descriptive link text
 * - WCAG AA color contrast
 * - Keyboard tab order
 * - Visible focus indicators
 * - axe-core audit passes
 */
const { test, expect } = require('@playwright/test');
const AxeBuilder = require('@axe-core/playwright').default;

test.describe('Accessibility Compliance', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('http://localhost:3000');
  });

  // Test Case 1: Verify semantic heading structure
  test('should have single h1 and logical heading hierarchy', async ({ page }) => {
    // Check for exactly one h1
    const h1Elements = await page.locator('h1').all();
    expect(h1Elements.length).toBe(1);

    // Get all headings and verify hierarchy
    const headings = await page.evaluate(() => {
      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      return Array.from(allHeadings).map(h => ({
        tag: h.tagName.toLowerCase(),
        text: h.textContent.trim().substring(0, 50),
        level: parseInt(h.tagName.charAt(1))
      }));
    });

    // Verify heading hierarchy - no skipped levels
    let previousLevel = 0;
    for (const heading of headings) {
      const currentLevel = heading.level;
      // First heading should be h1
      if (previousLevel === 0) {
        expect(currentLevel).toBe(1);
      }
      // Heading level should not skip more than one level down
      // (e.g., h1 -> h3 is invalid, h1 -> h2 -> h3 is valid)
      // Going from higher to lower is always allowed (h3 -> h2 is valid)
      if (currentLevel > previousLevel) {
        expect(currentLevel - previousLevel).toBeLessThanOrEqual(1);
      }
      previousLevel = currentLevel;
    }

    // Verify h1 contains expected content
    const h1Text = await page.locator('h1').textContent();
    expect(h1Text).toContain('MirDB');
  });

  // Test Case 2: Verify image alt attributes
  test('should have alt text or aria-labels for all images and SVGs', async ({ page }) => {
    // Check all img elements have alt attributes
    const images = await page.locator('img').all();
    for (const img of images) {
      const alt = await img.getAttribute('alt');
      expect(alt).not.toBeNull();
      // alt can be empty for decorative images, but should be present
    }

    // Check SVGs - they should either have aria-hidden="true" (decorative)
    // or have appropriate accessible labels
    // Note: aria-hidden can be on the SVG or on a parent container
    const svgs = await page.locator('svg').all();
    for (const svg of svgs) {
      const ariaHidden = await svg.getAttribute('aria-hidden');
      const ariaLabel = await svg.getAttribute('aria-label');
      const role = await svg.getAttribute('role');
      const title = await svg.locator('title').count();

      // Check if any ancestor has aria-hidden="true"
      const parentHasAriaHidden = await svg.evaluate(el => {
        let parent = el.parentElement;
        while (parent) {
          if (parent.getAttribute('aria-hidden') === 'true') {
            return true;
          }
          parent = parent.parentElement;
        }
        return false;
      });

      // SVG should either be decorative (aria-hidden on self or parent) or have accessible label
      const isDecorative = ariaHidden === 'true' || parentHasAriaHidden;
      const hasAccessibleName = ariaLabel !== null || title > 0 || role === 'img';

      expect(isDecorative || hasAccessibleName).toBeTruthy();
    }

    // Check figure elements with role="img" have aria-label
    const figureImgs = await page.locator('figure[role="img"]').all();
    for (const fig of figureImgs) {
      const ariaLabel = await fig.getAttribute('aria-label');
      expect(ariaLabel).not.toBeNull();
      expect(ariaLabel.length).toBeGreaterThan(0);
    }
  });

  // Test Case 3: Verify link text is descriptive
  test('should have descriptive link text without generic phrases', async ({ page }) => {
    const links = await page.locator('a').all();
    const genericPhrases = [
      'click here',
      'read more',
      'more',
      'here',
      'learn more',
      'continue reading'
    ];

    for (const link of links) {
      const linkText = (await link.textContent()).toLowerCase().trim();
      const ariaLabel = await link.getAttribute('aria-label');
      const effectiveText = ariaLabel || linkText;

      // Skip empty links (like logo links that might use images)
      if (effectiveText.length === 0) {
        // If no text, should have aria-label
        expect(ariaLabel).not.toBeNull();
        continue;
      }

      // Check link text is not a generic phrase
      for (const phrase of genericPhrases) {
        expect(effectiveText).not.toBe(phrase);
      }

      // Link text should be descriptive (at least 2 characters or have aria-label)
      expect(effectiveText.length).toBeGreaterThan(1);
    }
  });

  // Test Case 4: Verify color contrast ratios (WCAG AA)
  test('should meet WCAG AA color contrast requirements', async ({ page }) => {
    // Use axe-core specifically for color contrast checks
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .include('body')
      .analyze();

    // Filter for color-contrast related violations
    const contrastViolations = accessibilityScanResults.violations.filter(
      v => v.id === 'color-contrast'
    );

    // Should have no color contrast violations
    expect(contrastViolations).toHaveLength(0);
  });

  // Test Case 5: Verify keyboard tab order
  test('should have logical tab order with no focus traps', async ({ page }) => {
    // Get all focusable elements
    const focusableElements = await page.evaluate(() => {
      const focusable = document.querySelectorAll(
        'a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])'
      );
      return Array.from(focusable).map((el, index) => ({
        tag: el.tagName.toLowerCase(),
        tabindex: el.getAttribute('tabindex'),
        rect: el.getBoundingClientRect(),
        index
      }));
    });

    // Verify elements exist
    expect(focusableElements.length).toBeGreaterThan(0);

    // Test tab navigation through the page
    let tabCount = 0;
    const maxTabs = focusableElements.length + 5; // Extra buffer for safety
    const visitedElements = new Set();

    // Start by focusing the body
    await page.keyboard.press('Tab');
    tabCount++;

    while (tabCount < maxTabs) {
      const activeElement = await page.evaluate(() => {
        const el = document.activeElement;
        return {
          tag: el.tagName.toLowerCase(),
          id: el.id,
          className: el.className,
          href: el.getAttribute('href')
        };
      });

      // Create a unique identifier for this element
      const elementId = `${activeElement.tag}-${activeElement.id}-${activeElement.href}`;

      // Check for focus trap (visiting same element repeatedly)
      if (visitedElements.has(elementId) && activeElement.tag !== 'body') {
        // If we've cycled back to a visited element, we might be at the end
        // This is only a problem if we haven't visited enough elements
        break;
      }

      visitedElements.add(elementId);
      await page.keyboard.press('Tab');
      tabCount++;

      // If we've returned to body, we've completed the cycle
      if (activeElement.tag === 'body' && tabCount > 1) {
        break;
      }
    }

    // We should have been able to tab through multiple elements
    expect(visitedElements.size).toBeGreaterThan(1);

    // Should not have hit the max tabs limit (indicating potential focus trap)
    expect(tabCount).toBeLessThan(maxTabs);
  });

  // Test Case 6: Verify focus indicators
  test('should have visible focus indicators on focusable elements', async ({ page }) => {
    // Get all focusable elements
    const focusableSelectors = [
      'a[href]',
      'button',
      'input',
      'select',
      'textarea',
      '[tabindex]:not([tabindex="-1"])'
    ];

    for (const selector of focusableSelectors) {
      const elements = await page.locator(selector).all();

      for (const element of elements) {
        // Skip hidden elements
        const isVisible = await element.isVisible();
        if (!isVisible) continue;

        // Get styles before focus
        const beforeStyles = await element.evaluate(el => {
          const computed = window.getComputedStyle(el);
          return {
            outline: computed.outline,
            outlineWidth: computed.outlineWidth,
            outlineStyle: computed.outlineStyle,
            boxShadow: computed.boxShadow,
            border: computed.border,
            borderColor: computed.borderColor
          };
        });

        // Focus the element
        await element.focus();

        // Get styles after focus
        const afterStyles = await element.evaluate(el => {
          const computed = window.getComputedStyle(el);
          return {
            outline: computed.outline,
            outlineWidth: computed.outlineWidth,
            outlineStyle: computed.outlineStyle,
            boxShadow: computed.boxShadow,
            border: computed.border,
            borderColor: computed.borderColor
          };
        });

        // Check if there's a visible focus indicator
        // Either outline, box-shadow, or border should change
        const hasOutlineChange = afterStyles.outline !== beforeStyles.outline ||
          (afterStyles.outlineStyle !== 'none' && afterStyles.outlineWidth !== '0px');
        const hasBoxShadowChange = afterStyles.boxShadow !== beforeStyles.boxShadow &&
          afterStyles.boxShadow !== 'none';
        const hasBorderChange = afterStyles.border !== beforeStyles.border ||
          afterStyles.borderColor !== beforeStyles.borderColor;

        // At minimum, elements should have some focus indication from browser defaults
        // or custom styles
        const hasFocusIndicator = hasOutlineChange || hasBoxShadowChange || hasBorderChange;

        // Note: Browser default focus styles might not be detected as "change"
        // because they're applied as part of :focus pseudo-class
        // We check that outline is not explicitly removed
        const outlineNotRemoved = afterStyles.outlineStyle !== 'none' ||
          afterStyles.outlineWidth !== '0px' ||
          afterStyles.boxShadow !== 'none';

        expect(hasFocusIndicator || outlineNotRemoved).toBeTruthy();
      }
    }
  });

  // Test Case 7: Run automated accessibility audit (axe-core)
  test('should pass axe-core accessibility audit with no critical issues', async ({ page }) => {
    // Run axe-core audit
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa', 'best-practice'])
      .analyze();

    // Filter violations by impact level
    const criticalViolations = accessibilityScanResults.violations.filter(
      v => v.impact === 'critical'
    );
    const seriousViolations = accessibilityScanResults.violations.filter(
      v => v.impact === 'serious'
    );

    // Log all violations for debugging
    if (accessibilityScanResults.violations.length > 0) {
      console.log('Accessibility violations found:');
      accessibilityScanResults.violations.forEach(violation => {
        console.log(`- [${violation.impact}] ${violation.id}: ${violation.description}`);
        violation.nodes.forEach(node => {
          console.log(`  Element: ${node.html.substring(0, 100)}`);
        });
      });
    }

    // No critical violations allowed
    expect(criticalViolations).toHaveLength(0);

    // No serious violations allowed
    expect(seriousViolations).toHaveLength(0);

    // Log passes count for verification
    console.log(`Accessibility audit passed: ${accessibilityScanResults.passes.length} checks passed`);
  });
});
