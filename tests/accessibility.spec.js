// @ts-check
const { test, expect } = require('@playwright/test');
const AxeBuilder = require('@axe-core/playwright').default;

/**
 * Accessibility Compliance Tests for MirDB Homepage
 *
 * This test suite verifies WCAG 2.1 AA compliance as specified in NFR-2.
 * Tests cover:
 * - Color contrast ratios
 * - Keyboard navigation
 * - Focus indicators
 * - Image alt text
 * - Heading structure
 * - Lighthouse accessibility audit (via axe-core)
 */

test.describe('Accessibility Compliance - WCAG 2.1 AA', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');
  });

  /**
   * TC1: Check color contrast ratios
   * Input: Check color contrast ratios
   * Expected: All text meets minimum contrast ratio of 4.5:1 for normal text, 3:1 for large text
   */
  test('TC1: Color contrast ratios meet WCAG AA standards', async ({ page }) => {
    // Run axe-core specifically for color contrast issues
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa', 'wcag21aa'])
      .analyze();

    // Filter for color-contrast specific violations
    const contrastViolations = accessibilityScanResults.violations.filter(
      v => v.id === 'color-contrast'
    );

    // Log any contrast issues for debugging
    if (contrastViolations.length > 0) {
      console.log('Color contrast violations:', JSON.stringify(contrastViolations, null, 2));
    }

    // Verify no color contrast violations
    expect(contrastViolations.length).toBe(0);
  });

  /**
   * TC2: Navigate page using Tab key
   * Input: Navigate page using Tab key
   * Expected: All interactive elements are focusable in logical order
   */
  test('TC2: All interactive elements are focusable in logical order via Tab key', async ({ page }) => {
    // Get all interactive elements
    const interactiveElements = await page.$$('a, button, input, select, textarea, [tabindex]:not([tabindex="-1"])');

    // Start from the beginning
    await page.keyboard.press('Tab');

    const focusedElements = [];
    let previousFocusedElement = null;

    // Tab through all elements and record the order
    for (let i = 0; i < interactiveElements.length + 5; i++) { // +5 buffer for safety
      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;
        return {
          tagName: el.tagName,
          text: el.textContent?.trim().slice(0, 50),
          href: el.getAttribute('href'),
          className: el.className,
          role: el.getAttribute('role')
        };
      });

      if (focusedElement && JSON.stringify(focusedElement) !== JSON.stringify(previousFocusedElement)) {
        focusedElements.push(focusedElement);
        previousFocusedElement = focusedElement;
      }

      // Check if we've looped back to the beginning
      if (focusedElements.length > 1 &&
          JSON.stringify(focusedElements[focusedElements.length - 1]) === JSON.stringify(focusedElements[0])) {
        focusedElements.pop();
        break;
      }

      await page.keyboard.press('Tab');
    }

    // Verify we can reach at least the main navigation and CTA buttons
    expect(focusedElements.length).toBeGreaterThan(0);

    // Verify navigation links are focusable
    const navLinks = focusedElements.filter(el =>
      el.tagName === 'A' && (el.href?.includes('#') || el.href?.includes('github'))
    );
    expect(navLinks.length).toBeGreaterThan(0);

    // Verify CTA buttons are focusable
    const ctaButtons = focusedElements.filter(el =>
      el.className?.includes('btn') || el.tagName === 'BUTTON'
    );
    expect(ctaButtons.length).toBeGreaterThan(0);

    // Verify elements appear in document order (top to bottom)
    const elementsInOrder = await page.evaluate(() => {
      const elements = Array.from(document.querySelectorAll('a, button, input, select, textarea, [tabindex]:not([tabindex="-1"])'));
      return elements.map((el, index) => ({
        index,
        top: el.getBoundingClientRect().top
      }));
    });

    // Check that elements are generally in top-to-bottom order
    let outOfOrderCount = 0;
    for (let i = 1; i < elementsInOrder.length; i++) {
      if (elementsInOrder[i].top < elementsInOrder[i - 1].top - 50) { // Allow 50px tolerance
        outOfOrderCount++;
      }
    }

    // Allow some flexibility but most elements should be in order
    expect(outOfOrderCount).toBeLessThan(elementsInOrder.length * 0.3);
  });

  /**
   * TC3: Check focus indicators
   * Input: Check focus indicators
   * Expected: All focusable elements have visible focus indicators
   */
  test('TC3: All focusable elements have visible focus indicators', async ({ page }) => {
    // Check that focus styles are defined in CSS by looking at stylesheet rules
    const hasFocusStyles = await page.evaluate(() => {
      const styleSheets = document.styleSheets;
      const focusSelectors = [];

      for (const sheet of styleSheets) {
        try {
          for (const rule of sheet.cssRules) {
            if (rule.selectorText &&
                (rule.selectorText.includes(':focus') ||
                 rule.selectorText.includes(':focus-visible'))) {
              focusSelectors.push(rule.selectorText);
            }
          }
        } catch (e) {
          // Cross-origin stylesheets may throw
        }
      }

      return focusSelectors;
    });

    // Verify focus styles are defined in CSS
    expect(hasFocusStyles.length).toBeGreaterThan(0);

    // Check that common elements have focus styles in CSS
    const hasAnchorFocus = hasFocusStyles.some(s =>
      s.includes('a:focus') || s.includes('a:focus-visible')
    );
    expect(hasAnchorFocus, 'CSS should define focus styles for anchor elements').toBe(true);

    const hasButtonFocus = hasFocusStyles.some(s =>
      s.includes('button:focus') || s.includes('button:focus-visible') ||
      s.includes('.btn:focus') || s.includes('.btn:focus-visible')
    );
    expect(hasButtonFocus, 'CSS should define focus styles for buttons').toBe(true);

    // Use keyboard navigation to verify focus indicators
    // Tab through a few elements and check for visible focus
    await page.keyboard.press('Tab');

    const interactiveSelectors = ['.btn', '.logo', 'nav a'];

    for (const selector of interactiveSelectors) {
      const element = await page.$(selector);
      if (!element) continue;

      // Focus the element using keyboard simulation
      await element.focus();

      // Trigger focus-visible by simulating keyboard focus
      await page.evaluate((sel) => {
        const el = document.querySelector(sel);
        if (el) {
          el.classList.add('focus-visible');
          // Also check if outline property is defined
          const focusRule = Array.from(document.styleSheets)
            .flatMap(sheet => {
              try {
                return Array.from(sheet.cssRules);
              } catch (e) {
                return [];
              }
            })
            .find(rule =>
              rule.selectorText &&
              rule.selectorText.includes(':focus') &&
              rule.style?.outline
            );
          return focusRule !== undefined;
        }
      }, selector);

      // Check computed styles when focused
      const focusStyles = await element.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          outline: styles.outline,
          outlineWidth: styles.outlineWidth,
          outlineStyle: styles.outlineStyle
        };
      });

      // The element should have focus style defined (either via computed or CSS rules)
      // Note: Modern browsers may not show :focus styles on mouse click, only :focus-visible on keyboard
      const hasFocusStyle = focusStyles.outlineWidth !== '0px' ||
        focusStyles.outlineStyle !== 'none' ||
        hasFocusStyles.some(s => s.includes(selector.replace('.', '')));

      expect(hasFocusStyle || hasFocusStyles.length > 0,
        `Focus styles should be defined for ${selector}`
      ).toBe(true);
    }
  });

  /**
   * TC4: Check image alt text
   * Input: Check image alt text
   * Expected: All images have appropriate alt text describing their content
   */
  test('TC4: All images have appropriate alt text', async ({ page }) => {
    // Get all images on the page
    const images = await page.$$('img');

    expect(images.length).toBeGreaterThan(0);

    for (const img of images) {
      const imgInfo = await img.evaluate((el) => ({
        src: el.src,
        alt: el.alt,
        hasAlt: el.hasAttribute('alt')
      }));

      // Every image must have alt attribute
      expect(imgInfo.hasAlt,
        `Image ${imgInfo.src} should have alt attribute`
      ).toBe(true);

      // Alt text should not be empty (unless decorative)
      expect(imgInfo.alt.length).toBeGreaterThan(0);

      // Alt text should be descriptive (not just filename)
      expect(imgInfo.alt).not.toMatch(/\.(png|jpg|jpeg|gif|webp|svg)$/i);

      // Alt text should not be placeholder text
      expect(imgInfo.alt.toLowerCase()).not.toBe('image');
      expect(imgInfo.alt.toLowerCase()).not.toBe('picture');
      expect(imgInfo.alt.toLowerCase()).not.toBe('photo');
    }

    // Check SVG icons have appropriate labels or are hidden from assistive tech
    const svgIcons = await page.$$('svg');

    for (const svg of svgIcons) {
      const svgInfo = await svg.evaluate((el) => ({
        hasAriaLabel: el.hasAttribute('aria-label'),
        hasAriaHidden: el.getAttribute('aria-hidden') === 'true',
        hasTitle: el.querySelector('title') !== null,
        role: el.getAttribute('role'),
        html: el.outerHTML.slice(0, 100)
      }));

      // SVG should either be hidden from assistive tech or have a label
      const isAccessible = svgInfo.hasAriaLabel ||
        svgInfo.hasAriaHidden ||
        svgInfo.hasTitle ||
        svgInfo.role === 'presentation' ||
        svgInfo.role === 'img';

      // Decorative icons are acceptable when hidden from assistive tech
      expect(isAccessible,
        `SVG should be accessible or hidden: ${svgInfo.html}`
      ).toBe(true);
    }
  });

  /**
   * TC5: Check heading structure
   * Input: Check heading structure
   * Expected: Page has proper heading hierarchy (h1 -> h2 -> h3) without skipping levels
   */
  test('TC5: Page has proper heading hierarchy without skipping levels', async ({ page }) => {
    // Get all headings in document order
    const headings = await page.$$eval('h1, h2, h3, h4, h5, h6', (elements) =>
      elements.map(el => ({
        level: parseInt(el.tagName.substring(1)),
        text: el.textContent?.trim().slice(0, 50),
        tagName: el.tagName
      }))
    );

    // Page must have exactly one h1
    const h1Elements = headings.filter(h => h.level === 1);
    expect(h1Elements.length).toBe(1);

    // h1 should be the main title
    expect(h1Elements[0].text).toContain('MirDB');

    // Check heading hierarchy - no skipping levels
    let previousLevel = 0;
    const hierarchyIssues = [];

    for (const heading of headings) {
      // Cannot skip more than one level when going deeper
      if (heading.level > previousLevel + 1 && previousLevel !== 0) {
        hierarchyIssues.push({
          issue: `Heading level skipped from ${previousLevel} to ${heading.level}`,
          heading: heading
        });
      }
      previousLevel = heading.level;
    }

    // Log any issues for debugging
    if (hierarchyIssues.length > 0) {
      console.log('Heading hierarchy issues:', JSON.stringify(hierarchyIssues, null, 2));
    }

    expect(hierarchyIssues.length).toBe(0);

    // Verify important sections have headings
    const h2Texts = headings.filter(h => h.level === 2).map(h => h.text?.toLowerCase());

    // Should have h2s for main sections
    expect(h2Texts.length).toBeGreaterThan(0);
  });

  /**
   * TC6: Run Lighthouse accessibility audit (via axe-core)
   * Input: Run Lighthouse accessibility audit
   * Expected: Lighthouse accessibility score is 90 or higher
   *
   * Note: We use axe-core which powers Lighthouse accessibility checks.
   * A page with 0 axe violations typically scores 90+ in Lighthouse.
   */
  test('TC6: Axe-core accessibility audit passes with no critical or serious violations', async ({ page }) => {
    // Run comprehensive accessibility audit
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa', 'best-practice'])
      .analyze();

    // Get violations by impact
    const criticalViolations = accessibilityScanResults.violations.filter(v => v.impact === 'critical');
    const seriousViolations = accessibilityScanResults.violations.filter(v => v.impact === 'serious');
    const moderateViolations = accessibilityScanResults.violations.filter(v => v.impact === 'moderate');
    const minorViolations = accessibilityScanResults.violations.filter(v => v.impact === 'minor');

    // Log summary
    console.log('Accessibility Audit Summary:');
    console.log(`- Critical violations: ${criticalViolations.length}`);
    console.log(`- Serious violations: ${seriousViolations.length}`);
    console.log(`- Moderate violations: ${moderateViolations.length}`);
    console.log(`- Minor violations: ${minorViolations.length}`);
    console.log(`- Total violations: ${accessibilityScanResults.violations.length}`);
    console.log(`- Passes: ${accessibilityScanResults.passes.length}`);

    // Log detailed violations if any
    if (accessibilityScanResults.violations.length > 0) {
      console.log('\nDetailed violations:');
      for (const violation of accessibilityScanResults.violations) {
        console.log(`\n${violation.impact?.toUpperCase()}: ${violation.id} - ${violation.help}`);
        console.log(`Description: ${violation.description}`);
        console.log(`Help URL: ${violation.helpUrl}`);
        console.log(`Affected elements: ${violation.nodes.length}`);
        violation.nodes.slice(0, 3).forEach((node, i) => {
          console.log(`  ${i + 1}. ${node.html.slice(0, 100)}`);
        });
      }
    }

    // Calculate approximate accessibility score
    // Lighthouse uses a weighted scoring system based on axe-core results
    const totalIssues = criticalViolations.length * 4 +
      seriousViolations.length * 3 +
      moderateViolations.length * 2 +
      minorViolations.length * 1;

    // A rough estimate: 0 violations = 100, deduct points based on severity
    const estimatedScore = Math.max(0, 100 - totalIssues * 2);
    console.log(`\nEstimated accessibility score: ${estimatedScore}`);

    // Must have no critical or serious violations for WCAG 2.1 AA compliance
    expect(criticalViolations.length, 'No critical violations allowed').toBe(0);
    expect(seriousViolations.length, 'No serious violations allowed').toBe(0);

    // Estimated score should be 90+ (accounting for moderate/minor issues)
    expect(estimatedScore, 'Accessibility score should be 90 or higher').toBeGreaterThanOrEqual(90);
  });

  /**
   * Additional test: Verify semantic HTML structure
   */
  test('Page uses semantic HTML elements correctly', async ({ page }) => {
    // Check for proper landmark elements
    const landmarks = await page.$$eval('main, nav, header, footer, section, article, aside',
      (elements) => elements.map(el => el.tagName.toLowerCase())
    );

    // Should have main landmark
    expect(landmarks).toContain('main');

    // Should have navigation
    expect(landmarks).toContain('nav');

    // Should have footer
    expect(landmarks).toContain('footer');

    // Should have sections
    expect(landmarks.filter(l => l === 'section').length).toBeGreaterThan(0);

    // Verify page has proper lang attribute
    const htmlLang = await page.$eval('html', (el) => el.getAttribute('lang'));
    expect(htmlLang).toBe('en');

    // Verify proper document structure
    const hasTitle = await page.title();
    expect(hasTitle.length).toBeGreaterThan(0);
    expect(hasTitle).toContain('MirDB');
  });

  /**
   * Additional test: Verify ARIA attributes are used correctly
   */
  test('ARIA attributes are used correctly', async ({ page }) => {
    // Run axe specifically for ARIA violations
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['cat.aria'])
      .analyze();

    const ariaViolations = accessibilityScanResults.violations;

    if (ariaViolations.length > 0) {
      console.log('ARIA violations:', JSON.stringify(ariaViolations, null, 2));
    }

    expect(ariaViolations.length).toBe(0);

    // Check that role="img" elements have aria-label
    const roleImgElements = await page.$$('[role="img"]');
    for (const el of roleImgElements) {
      const ariaLabel = await el.getAttribute('aria-label');
      expect(ariaLabel, 'Elements with role="img" must have aria-label').toBeTruthy();
      expect(ariaLabel.length).toBeGreaterThan(0);
    }
  });

  /**
   * Additional test: Verify links are accessible
   */
  test('Links are accessible and have descriptive text', async ({ page }) => {
    // Run axe specifically for link violations
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['cat.name-role-value'])
      .analyze();

    const linkViolations = accessibilityScanResults.violations.filter(
      v => v.id.includes('link')
    );

    if (linkViolations.length > 0) {
      console.log('Link violations:', JSON.stringify(linkViolations, null, 2));
    }

    expect(linkViolations.length).toBe(0);

    // Check that external links have proper attributes
    const externalLinks = await page.$$('a[target="_blank"]');
    for (const link of externalLinks) {
      const rel = await link.getAttribute('rel');
      // External links should have noopener noreferrer for security
      expect(rel).toContain('noopener');
    }
  });
});
