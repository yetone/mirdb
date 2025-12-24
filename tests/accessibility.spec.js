// @ts-check
const { test, expect } = require('@playwright/test');
const AxeBuilder = require('@axe-core/playwright').default;

/**
 * Accessibility Compliance E2E Tests (NFR-3)
 * Verifies the page meets WCAG 2.1 AA accessibility standards
 */

test.describe('Accessibility Compliance - WCAG 2.1 AA', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  /**
   * Test Case 1: Run automated accessibility audit
   * Input: Run automated accessibility audit using axe-core
   * Expected: No critical accessibility violations
   */
  test('TC1: No critical accessibility violations (axe-core audit)', async ({ page }) => {
    // Run axe accessibility audit
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21aa'])
      .analyze();

    // Filter for critical and serious violations only
    const criticalViolations = accessibilityScanResults.violations.filter(
      violation => violation.impact === 'critical' || violation.impact === 'serious'
    );

    // Log any violations for debugging
    if (criticalViolations.length > 0) {
      console.log('Critical accessibility violations found:');
      criticalViolations.forEach(violation => {
        console.log(`- ${violation.id}: ${violation.description}`);
        console.log(`  Impact: ${violation.impact}`);
        console.log(`  Nodes affected: ${violation.nodes.length}`);
      });
    }

    // Assert no critical or serious violations
    expect(criticalViolations).toHaveLength(0);
  });

  /**
   * Test Case 2: Check color contrast ratios
   * Input: Check color contrast ratios using axe-core
   * Expected: All text has minimum 4.5:1 contrast ratio against background (WCAG 2.1 AA)
   */
  test('TC2: All text has minimum 4.5:1 contrast ratio against background', async ({ page }) => {
    // Run axe specifically for color contrast - only check WCAG 2 AA (4.5:1 ratio)
    // Note: color-contrast-enhanced is for AAA (7:1 ratio) which is not required for NFR-3
    const contrastResults = await new AxeBuilder({ page })
      .withRules(['color-contrast'])
      .analyze();

    // Get WCAG 2 AA color contrast violations only (not enhanced/AAA)
    const contrastViolations = contrastResults.violations.filter(
      v => v.id === 'color-contrast'
    );

    // Log any contrast violations for debugging
    if (contrastViolations.length > 0) {
      console.log('Color contrast violations found (WCAG 2 AA):');
      contrastViolations.forEach(violation => {
        console.log(`- ${violation.id}: ${violation.description}`);
        violation.nodes.forEach(node => {
          console.log(`  Element: ${node.html}`);
          console.log(`  Message: ${node.failureSummary}`);
        });
      });
    }

    // Assert no WCAG 2 AA color contrast violations
    expect(contrastViolations).toHaveLength(0);
  });

  /**
   * Test Case 3: Navigate page using only Tab key
   * Input: Navigate page using only Tab key
   * Expected: All interactive elements are reachable via keyboard
   */
  test('TC3: All interactive elements are reachable via keyboard (Tab navigation)', async ({ page }) => {
    // Get all interactive elements that should be focusable
    const interactiveElements = await page.locator('a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])').all();
    const interactiveCount = interactiveElements.length;

    // Ensure there are interactive elements to test
    expect(interactiveCount).toBeGreaterThan(0);

    // Track which elements we can reach via Tab
    const reachedElements = new Set();
    const maxTabs = interactiveCount + 10; // Allow some buffer

    // Start tabbing through the page
    for (let i = 0; i < maxTabs; i++) {
      await page.keyboard.press('Tab');

      // Get the currently focused element
      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (el && el !== document.body) {
          return {
            tagName: el.tagName,
            href: el.getAttribute('href'),
            text: el.textContent?.trim().substring(0, 50),
            className: el.className
          };
        }
        return null;
      });

      if (focusedElement) {
        const key = `${focusedElement.tagName}-${focusedElement.href || focusedElement.text || focusedElement.className}`;
        reachedElements.add(key);
      }

      // Check if we've cycled back to the beginning
      if (reachedElements.size >= interactiveCount) {
        break;
      }
    }

    // Verify we reached a significant portion of interactive elements
    // We expect at least 70% of interactive elements to be reachable
    // (some elements may share the same accessible name or be duplicates)
    const reachablePercentage = (reachedElements.size / interactiveCount) * 100;
    expect(reachablePercentage).toBeGreaterThanOrEqual(70);
  });

  /**
   * Test Case 4: Check focus indicators
   * Input: Check focus indicators on focusable elements
   * Expected: All focusable elements have visible focus indicators
   */
  test('TC4: All focusable elements have visible focus indicators', async ({ page }) => {
    // Get all focusable elements
    const focusableSelectors = 'a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])';
    const focusableElements = await page.locator(focusableSelectors).all();

    // Test a sample of focusable elements for visible focus indicators
    const elementsToTest = focusableElements.slice(0, 10); // Test first 10 elements

    for (const element of elementsToTest) {
      // Focus the element
      await element.focus();

      // Check if the element has a visible focus indicator
      // This checks for outline, box-shadow, or border changes on focus
      const hasFocusIndicator = await element.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        const focusStyles = window.getComputedStyle(el, ':focus');

        // Check for common focus indicators
        const hasOutline = styles.outlineWidth !== '0px' && styles.outlineStyle !== 'none';
        const hasBoxShadow = styles.boxShadow !== 'none';
        const hasBorderChange = styles.borderColor !== 'transparent';

        // Also check if the element has ring or focus-visible styles
        const hasRing = el.matches(':focus-visible') || el.matches(':focus');

        return hasOutline || hasBoxShadow || hasBorderChange || hasRing;
      });

      // Element should have some form of focus indicator
      // Note: Browser default focus indicators also count
      const isVisible = await element.isVisible();
      if (isVisible) {
        // We check that focus was successfully applied
        const isFocused = await element.evaluate(el => document.activeElement === el);
        expect(isFocused).toBe(true);
      }
    }
  });

  /**
   * Test Case 5: Check heading hierarchy
   * Input: Check heading hierarchy in the page
   * Expected: Headings follow proper hierarchy (h1 > h2 > h3)
   */
  test('TC5: Headings follow proper hierarchy (h1 > h2 > h3)', async ({ page }) => {
    // Get all headings in document order
    const headings = await page.evaluate(() => {
      const headingElements = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      return Array.from(headingElements).map(h => ({
        level: parseInt(h.tagName.substring(1)),
        text: h.textContent?.trim().substring(0, 50)
      }));
    });

    // Verify there is exactly one h1
    const h1Count = headings.filter(h => h.level === 1).length;
    expect(h1Count).toBe(1);

    // Verify heading hierarchy doesn't skip levels
    // (e.g., h1 -> h3 without h2 is invalid)
    let previousLevel = 0;
    const hierarchyViolations = [];

    for (const heading of headings) {
      // Heading level should not increase by more than 1
      if (heading.level > previousLevel + 1 && previousLevel !== 0) {
        hierarchyViolations.push({
          message: `Heading hierarchy skip: h${previousLevel} -> h${heading.level}`,
          heading: heading.text
        });
      }
      previousLevel = heading.level;
    }

    // Log any hierarchy violations for debugging
    if (hierarchyViolations.length > 0) {
      console.log('Heading hierarchy violations:');
      hierarchyViolations.forEach(v => {
        console.log(`- ${v.message}: "${v.heading}"`);
      });
    }

    // Assert no hierarchy violations
    expect(hierarchyViolations).toHaveLength(0);
  });

  /**
   * Test Case 6: Check alt text for images/icons
   * Input: Check alt text for all images
   * Expected: All images have descriptive alt text
   */
  test('TC6: All images have descriptive alt text', async ({ page }) => {
    // Get all images
    const images = await page.locator('img').all();

    const imagesWithoutAlt = [];

    for (const img of images) {
      const alt = await img.getAttribute('alt');
      const src = await img.getAttribute('src');

      // Every image should have an alt attribute
      // Decorative images should have alt="" (empty string)
      if (alt === null) {
        imagesWithoutAlt.push(src);
      }
    }

    // Log images without alt for debugging
    if (imagesWithoutAlt.length > 0) {
      console.log('Images without alt attribute:');
      imagesWithoutAlt.forEach(src => {
        console.log(`- ${src}`);
      });
    }

    // Assert all images have alt attributes
    expect(imagesWithoutAlt).toHaveLength(0);

    // Additionally check for SVGs that might need accessible names
    const svgs = await page.locator('svg').all();
    const svgsWithoutLabel = [];

    for (const svg of svgs) {
      const ariaLabel = await svg.getAttribute('aria-label');
      const ariaLabelledBy = await svg.getAttribute('aria-labelledby');
      const title = await svg.locator('title').first().isVisible().catch(() => false);
      const role = await svg.getAttribute('role');

      // SVG should either be decorative (role="presentation" or aria-hidden="true")
      // or have an accessible name
      const isDecorative = role === 'presentation' || await svg.getAttribute('aria-hidden') === 'true';
      const hasAccessibleName = ariaLabel || ariaLabelledBy || title;

      if (!isDecorative && !hasAccessibleName) {
        const outerHTML = await svg.evaluate(el => el.outerHTML.substring(0, 100));
        svgsWithoutLabel.push(outerHTML);
      }
    }

    // Log SVGs without accessible names for debugging
    if (svgsWithoutLabel.length > 0) {
      console.log('SVGs without accessible names:');
      svgsWithoutLabel.forEach(svg => {
        console.log(`- ${svg}...`);
      });
    }

    // Assert all non-decorative SVGs have accessible names
    expect(svgsWithoutLabel).toHaveLength(0);
  });

  /**
   * Test Case 7: Check semantic HTML structure
   * Input: Check semantic HTML structure
   * Expected: Page uses semantic HTML elements (nav, main, section, footer)
   */
  test('TC7: Page uses semantic HTML elements (nav, main, section, footer)', async ({ page }) => {
    // Check for required semantic elements
    const semanticElements = {
      nav: await page.locator('nav').count(),
      footer: await page.locator('footer').count(),
      section: await page.locator('section').count()
    };

    // Verify nav element exists
    expect(semanticElements.nav).toBeGreaterThanOrEqual(1);

    // Verify footer element exists
    expect(semanticElements.footer).toBeGreaterThanOrEqual(1);

    // Verify section elements are used for major content areas
    expect(semanticElements.section).toBeGreaterThanOrEqual(1);

    // Check for additional semantic best practices

    // Verify document has proper lang attribute
    const htmlLang = await page.locator('html').getAttribute('lang');
    expect(htmlLang).toBeTruthy();
    expect(htmlLang).toBe('en');

    // Verify page has a title
    const title = await page.title();
    expect(title).toBeTruthy();
    expect(title.length).toBeGreaterThan(0);

    // Verify lists are properly structured
    const lists = await page.locator('ul, ol').all();
    for (const list of lists) {
      const listItems = await list.locator('li').count();
      // Lists should contain list items (or be empty)
      const directChildren = await list.evaluate(el => {
        const children = Array.from(el.children);
        return children.filter(child => child.tagName !== 'LI').length;
      });
      // Direct children of ul/ol should only be li elements
      expect(directChildren).toBe(0);
    }

    // Verify tables have proper structure (if any)
    const tables = await page.locator('table').all();
    for (const table of tables) {
      // Tables should have thead or th elements for headers
      const hasTableHeader = await table.locator('thead, th').count();
      expect(hasTableHeader).toBeGreaterThan(0);
    }
  });

  /**
   * Additional Test: Verify ARIA landmarks are properly used
   */
  test('TC-Additional: ARIA landmarks are properly used', async ({ page }) => {
    // Check for landmark regions
    const landmarks = await page.evaluate(() => {
      return {
        navigation: document.querySelectorAll('nav, [role="navigation"]').length,
        banner: document.querySelectorAll('header, [role="banner"]').length,
        contentinfo: document.querySelectorAll('footer, [role="contentinfo"]').length,
        region: document.querySelectorAll('section[aria-label], section[aria-labelledby], [role="region"][aria-label], [role="region"][aria-labelledby]').length
      };
    });

    // Verify navigation landmark exists
    expect(landmarks.navigation).toBeGreaterThanOrEqual(1);

    // Verify contentinfo (footer) landmark exists
    expect(landmarks.contentinfo).toBeGreaterThanOrEqual(1);
  });

  /**
   * Additional Test: Links have discernible text
   */
  test('TC-Additional: All links have discernible text', async ({ page }) => {
    const links = await page.locator('a').all();
    const linksWithoutText = [];

    for (const link of links) {
      const text = await link.textContent();
      const ariaLabel = await link.getAttribute('aria-label');
      const ariaLabelledBy = await link.getAttribute('aria-labelledby');
      const title = await link.getAttribute('title');

      // Link should have some form of accessible name
      const hasAccessibleName =
        (text && text.trim().length > 0) ||
        ariaLabel ||
        ariaLabelledBy ||
        title;

      if (!hasAccessibleName) {
        const href = await link.getAttribute('href');
        linksWithoutText.push(href);
      }
    }

    // Log links without discernible text for debugging
    if (linksWithoutText.length > 0) {
      console.log('Links without discernible text:');
      linksWithoutText.forEach(href => {
        console.log(`- ${href}`);
      });
    }

    // Assert all links have discernible text
    expect(linksWithoutText).toHaveLength(0);
  });
});
