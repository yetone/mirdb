// @ts-check
const { test, expect } = require('@playwright/test');
const AxeBuilder = require('@axe-core/playwright').default;

/**
 * Test Suite: Accessibility Compliance
 * Scenario: Verify page meets WCAG 2.1 AA accessibility requirements (NFR-2)
 */

test.describe('Accessibility Compliance', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for all content to be fully loaded
    await page.waitForLoadState('networkidle');
  });

  /**
   * Test Case 1: Run Lighthouse/Axe accessibility audit
   * Input: Run Lighthouse accessibility audit
   * Expected: No critical accessibility issues reported
   */
  test('TC1: No critical accessibility issues reported', async ({ page }) => {
    // Run axe accessibility scan
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .analyze();

    // Filter for critical and serious violations (equivalent to critical in Lighthouse)
    const criticalViolations = accessibilityScanResults.violations.filter(
      v => v.impact === 'critical' || v.impact === 'serious'
    );

    // Log any violations for debugging purposes
    if (criticalViolations.length > 0) {
      console.log('Critical/Serious accessibility violations found:');
      criticalViolations.forEach(violation => {
        console.log(`- ${violation.id}: ${violation.description}`);
        console.log(`  Impact: ${violation.impact}`);
        console.log(`  Help: ${violation.helpUrl}`);
        violation.nodes.forEach(node => {
          console.log(`  Element: ${node.html}`);
        });
      });
    }

    // Assert no critical or serious violations
    expect(criticalViolations.length).toBe(0);
  });

  /**
   * Test Case 2: Check color contrast ratios
   * Input: Check color contrast ratios
   * Expected: All text meets 4.5:1 contrast ratio minimum (WCAG 2.1 AA level)
   */
  test('TC2: All text meets 4.5:1 contrast ratio minimum', async ({ page }) => {
    // Run axe specifically for WCAG AA color contrast rule (4.5:1 for normal text, 3:1 for large text)
    // Note: color-contrast-enhanced is WCAG AAA (7:1) which is not required by NFR-2
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withRules(['color-contrast'])
      .analyze();

    // Get all color contrast violations
    const contrastViolations = accessibilityScanResults.violations;

    // Log any contrast violations
    if (contrastViolations.length > 0) {
      console.log('Color contrast violations found:');
      contrastViolations.forEach(violation => {
        console.log(`- ${violation.id}: ${violation.description}`);
        violation.nodes.forEach(node => {
          console.log(`  Element: ${node.html}`);
          console.log(`  Message: ${node.failureSummary}`);
        });
      });
    }

    // Assert no contrast violations for WCAG AA level
    expect(contrastViolations.length).toBe(0);
  });

  /**
   * Test Case 3: Test keyboard navigation
   * Input: Test keyboard navigation
   * Expected: All interactive elements are focusable and operable via keyboard
   * Type: Manual (verified through automated keyboard focus testing)
   */
  test('TC3: All interactive elements are keyboard accessible', async ({ page }) => {
    // Get all interactive elements: links and buttons
    const interactiveElements = await page.locator('a[href], button').all();

    // Track elements that should be focusable
    const focusableElements = [];

    for (const element of interactiveElements) {
      const isVisible = await element.isVisible();
      if (isVisible) {
        focusableElements.push(element);
      }
    }

    // Start at the beginning of the page
    await page.keyboard.press('Tab');

    // Track how many unique elements we can focus
    let focusedCount = 0;
    const focusedElements = new Set();
    const maxTabs = focusableElements.length + 10; // Add buffer for potential hidden focusable elements

    for (let i = 0; i < maxTabs && focusedCount < focusableElements.length; i++) {
      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        return el ? el.outerHTML.slice(0, 200) : null;
      });

      if (focusedElement && !focusedElements.has(focusedElement)) {
        focusedElements.add(focusedElement);

        // Check if focused element is an interactive element (link or button)
        const isInteractive = await page.evaluate(() => {
          const el = document.activeElement;
          return el && (el.tagName === 'A' || el.tagName === 'BUTTON');
        });

        if (isInteractive) {
          focusedCount++;
        }
      }

      await page.keyboard.press('Tab');
    }

    // Verify we could focus on interactive elements
    expect(focusedCount).toBeGreaterThan(0);

    // Verify all visible interactive elements have focus styles by checking tabindex isn't negative
    for (const element of focusableElements) {
      const tabIndex = await element.getAttribute('tabindex');
      // Elements shouldn't have negative tabindex if they need to be keyboard accessible
      if (tabIndex !== null) {
        expect(parseInt(tabIndex)).toBeGreaterThanOrEqual(0);
      }
    }
  });

  /**
   * Test Case 4: Check for alt text on images
   * Input: Check for alt text on images
   * Expected: All images have descriptive alt text
   */
  test('TC4: All images have descriptive alt text', async ({ page }) => {
    // Get all images on the page
    const images = await page.locator('img').all();

    // Check each image has alt text
    for (const img of images) {
      const altText = await img.getAttribute('alt');
      const src = await img.getAttribute('src');

      // Every image should have an alt attribute
      expect(altText, `Image ${src} is missing alt attribute`).not.toBeNull();

      // Alt text should not be empty (unless it's decorative, but logos/hero images are not decorative)
      expect(altText.trim().length, `Image ${src} has empty alt text`).toBeGreaterThan(0);

      // Alt text should be descriptive (more than just a filename)
      expect(altText.toLowerCase()).not.toMatch(/\.(jpg|jpeg|png|gif|webp|svg)$/);
    }

    // Run axe-core image-alt rule for additional validation
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withRules(['image-alt'])
      .analyze();

    expect(accessibilityScanResults.violations.length).toBe(0);
  });

  /**
   * Test Case 5: Verify heading hierarchy
   * Input: Verify heading hierarchy
   * Expected: Headings follow proper hierarchy (h1, h2, h3) without skipping levels
   */
  test('TC5: Headings follow proper hierarchy without skipping levels', async ({ page }) => {
    // Get all headings in document order
    const headings = await page.evaluate(() => {
      const headingElements = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      return Array.from(headingElements).map(h => ({
        level: parseInt(h.tagName[1]),
        text: h.textContent.trim().slice(0, 50)
      }));
    });

    // There should be at least one heading
    expect(headings.length).toBeGreaterThan(0);

    // There should be exactly one h1
    const h1Count = headings.filter(h => h.level === 1).length;
    expect(h1Count, 'Page should have exactly one h1').toBe(1);

    // The first heading should be h1
    expect(headings[0].level, 'First heading should be h1').toBe(1);

    // Check for skipped levels (e.g., h1 -> h3 without h2)
    let previousLevel = 0;
    const skippedLevels = [];

    for (const heading of headings) {
      // When going deeper (to a higher level number), shouldn't skip more than 1 level
      if (heading.level > previousLevel + 1 && previousLevel !== 0) {
        skippedLevels.push({
          from: previousLevel,
          to: heading.level,
          text: heading.text
        });
      }
      previousLevel = heading.level;
    }

    // Log skipped levels for debugging
    if (skippedLevels.length > 0) {
      console.log('Skipped heading levels:');
      skippedLevels.forEach(skip => {
        console.log(`  h${skip.from} -> h${skip.to}: "${skip.text}"`);
      });
    }

    expect(skippedLevels.length, 'Should not skip heading levels').toBe(0);

    // Run axe-core heading-order rule
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withRules(['heading-order'])
      .analyze();

    expect(accessibilityScanResults.violations.length).toBe(0);
  });

  /**
   * Test Case 6: Check for skip-to-content link
   * Input: Check for skip-to-content link
   * Expected: Skip-to-content link is present for screen readers
   */
  test('TC6: Skip-to-content link is present for screen readers', async ({ page }) => {
    // Look for skip link variations
    const skipLinkSelectors = [
      'a[href="#main"]',
      'a[href="#content"]',
      'a[href="#main-content"]',
      '.skip-link',
      '.skip-to-content',
      'a.skip-link',
      'a.skip-to-content',
      '[class*="skip"]'
    ];

    let skipLinkFound = false;
    let skipLink = null;

    // Check for any skip link
    for (const selector of skipLinkSelectors) {
      const element = page.locator(selector).first();
      const count = await element.count();
      if (count > 0) {
        skipLinkFound = true;
        skipLink = element;
        break;
      }
    }

    // If no skip link by selector, check for any link with "skip" in text or that links to main content
    if (!skipLinkFound) {
      const allLinks = await page.locator('a').all();
      for (const link of allLinks) {
        const text = await link.textContent();
        const href = await link.getAttribute('href');
        if (text && (text.toLowerCase().includes('skip') || text.toLowerCase().includes('jump to'))) {
          skipLinkFound = true;
          skipLink = link;
          break;
        }
        if (href && (href === '#main' || href === '#content' || href === '#main-content')) {
          skipLinkFound = true;
          skipLink = link;
          break;
        }
      }
    }

    // Assert skip link exists
    expect(skipLinkFound, 'Skip-to-content link should be present').toBe(true);

    if (skipLink) {
      // Skip link should be the first focusable element or become visible on focus
      await page.keyboard.press('Tab');

      // Check if skip link is focusable
      const href = await skipLink.getAttribute('href');
      expect(href, 'Skip link should have a valid href').toBeTruthy();

      // The target of the skip link should exist
      if (href && href.startsWith('#')) {
        const targetId = href.slice(1);
        const target = page.locator(`#${targetId}`);
        await expect(target, `Skip link target ${href} should exist`).toBeAttached();
      }
    }
  });

  /**
   * Additional accessibility tests for comprehensive coverage
   */

  test('All links have accessible names', async ({ page }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withRules(['link-name'])
      .analyze();

    expect(accessibilityScanResults.violations.length).toBe(0);
  });

  test('All buttons have accessible names', async ({ page }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withRules(['button-name'])
      .analyze();

    expect(accessibilityScanResults.violations.length).toBe(0);
  });

  test('Page has valid HTML language attribute', async ({ page }) => {
    const lang = await page.locator('html').getAttribute('lang');
    expect(lang).toBeTruthy();
    expect(lang).toMatch(/^[a-z]{2}(-[A-Z]{2})?$/);
  });

  test('All form inputs have associated labels', async ({ page }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withRules(['label'])
      .analyze();

    expect(accessibilityScanResults.violations.length).toBe(0);
  });

  test('Focus order is logical and sequential', async ({ page }) => {
    // Start from the beginning
    await page.keyboard.press('Tab');

    const focusOrder = [];
    const maxTabs = 50;

    for (let i = 0; i < maxTabs; i++) {
      const activeElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;
        const rect = el.getBoundingClientRect();
        return {
          tag: el.tagName,
          id: el.id,
          top: rect.top,
          left: rect.left
        };
      });

      if (!activeElement) break;
      focusOrder.push(activeElement);
      await page.keyboard.press('Tab');

      // Check if we've looped back
      if (focusOrder.length > 1) {
        const last = focusOrder[focusOrder.length - 1];
        const first = focusOrder[0];
        if (last.id === first.id && last.tag === first.tag) break;
      }
    }

    // Verify focus order generally follows visual reading order (top to bottom, left to right)
    // This is a loose check - elements should generally increase in top position
    let logicalOrder = true;
    for (let i = 1; i < focusOrder.length - 1; i++) {
      const prev = focusOrder[i - 1];
      const curr = focusOrder[i];
      // Allow for elements on the same "row" (within 100px vertical)
      // or elements that are further down
      if (curr.top < prev.top - 100) {
        logicalOrder = false;
        break;
      }
    }

    expect(logicalOrder).toBe(true);
  });

  test('ARIA attributes are used correctly', async ({ page }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withRules(['aria-allowed-attr', 'aria-required-attr', 'aria-valid-attr', 'aria-valid-attr-value'])
      .analyze();

    expect(accessibilityScanResults.violations.length).toBe(0);
  });
});
