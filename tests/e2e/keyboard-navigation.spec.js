// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Accessibility - Keyboard Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Tab through all interactive elements - all buttons and links are reachable via Tab key
  test('TC1: All buttons and links are reachable via Tab key', async ({ page }) => {
    // Get all interactive elements that should be tabbable
    const interactiveElements = await page.locator('a[href], button, [tabindex]:not([tabindex="-1"])').all();

    // Focus on the body first to start from the beginning
    await page.keyboard.press('Tab');

    const focusedElements = [];
    let previousElement = null;
    let maxIterations = 50; // Prevent infinite loop
    let iterations = 0;

    // Tab through all elements until we loop back or exceed max iterations
    while (iterations < maxIterations) {
      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;
        return {
          tagName: el.tagName,
          href: el.getAttribute('href'),
          className: el.className,
          textContent: el.textContent?.trim().substring(0, 50)
        };
      });

      if (!focusedElement) break;

      // Check if we've looped back to the first element
      const elementKey = `${focusedElement.tagName}-${focusedElement.href || ''}-${focusedElement.textContent}`;
      if (focusedElements.length > 0 && focusedElements[0].key === elementKey) {
        break;
      }

      focusedElements.push({ ...focusedElement, key: elementKey });

      await page.keyboard.press('Tab');
      iterations++;
    }

    // Verify we found interactive elements
    expect(focusedElements.length).toBeGreaterThan(0);

    // Verify key interactive elements are present in the tab order:
    // - Get Started button
    // - View on GitHub button
    // - Documentation links
    // - Footer links

    const getStartedFound = focusedElements.some(el =>
      el.textContent && el.textContent.includes('Get Started')
    );
    expect(getStartedFound).toBe(true);

    const githubFound = focusedElements.some(el =>
      el.textContent && (el.textContent.includes('GitHub') || el.href?.includes('github'))
    );
    expect(githubFound).toBe(true);

    // Verify all expected link types are tabbable
    const hasFooterLinks = focusedElements.some(el =>
      el.textContent && (el.textContent.includes('Documentation') || el.textContent.includes('License'))
    );
    expect(hasFooterLinks).toBe(true);
  });

  // Test Case 2: Focus order follows logical reading order
  test('TC2: Focus order follows logical reading order', async ({ page }) => {
    // Tab through elements and record their positions
    await page.keyboard.press('Tab');

    const focusOrder = [];
    let maxIterations = 50;
    let iterations = 0;

    while (iterations < maxIterations) {
      const elementInfo = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;

        const rect = el.getBoundingClientRect();
        return {
          tagName: el.tagName,
          top: rect.top,
          left: rect.left,
          textContent: el.textContent?.trim().substring(0, 30),
          href: el.getAttribute('href')
        };
      });

      if (!elementInfo) break;

      // Check if we've looped (same element as first)
      if (focusOrder.length > 0) {
        const first = focusOrder[0];
        if (elementInfo.tagName === first.tagName &&
            elementInfo.href === first.href &&
            elementInfo.textContent === first.textContent) {
          break;
        }
      }

      focusOrder.push(elementInfo);
      await page.keyboard.press('Tab');
      iterations++;
    }

    // Verify focus order generally follows top-to-bottom reading order
    // Elements should mostly progress down the page (with some horizontal variation)
    expect(focusOrder.length).toBeGreaterThan(0);

    // Check that the hero section elements come before footer elements
    const heroGetStartedIndex = focusOrder.findIndex(el =>
      el.textContent && el.textContent.includes('Get Started')
    );
    const footerGitHubIndex = focusOrder.findIndex(el =>
      el.top > 500 && el.textContent && el.textContent.includes('GitHub')
    );

    // Get Started should appear in tab order before footer links (if footer GitHub link exists)
    if (heroGetStartedIndex !== -1 && footerGitHubIndex !== -1) {
      expect(heroGetStartedIndex).toBeLessThan(footerGitHubIndex);
    }

    // Verify elements roughly follow vertical progression
    // Group elements by approximate vertical position and verify ordering
    let lastMajorSection = -Infinity;
    let sectionCount = 0;

    for (const element of focusOrder) {
      // If element is significantly lower (new section), update lastMajorSection
      if (element.top > lastMajorSection + 200) {
        expect(element.top).toBeGreaterThanOrEqual(lastMajorSection);
        lastMajorSection = element.top;
        sectionCount++;
      }
    }

    // We should traverse multiple sections
    expect(sectionCount).toBeGreaterThan(1);
  });

  // Test Case 3: Activate Get Started button with Enter key
  test('TC3: Get Started button activates when Enter is pressed while focused', async ({ page }) => {
    // Tab to the Get Started button
    let found = false;
    let maxTabs = 20;

    for (let i = 0; i < maxTabs; i++) {
      await page.keyboard.press('Tab');

      const isFocusedOnGetStarted = await page.evaluate(() => {
        const el = document.activeElement;
        return el && el.textContent?.trim() === 'Get Started';
      });

      if (isFocusedOnGetStarted) {
        found = true;
        break;
      }
    }

    expect(found).toBe(true);

    // Get current scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Press Enter to activate the button
    await page.keyboard.press('Enter');

    // Wait for smooth scroll animation
    await page.waitForTimeout(600);

    // Verify the page scrolled to the quickstart section
    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeInViewport();

    // Verify scroll position changed
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);
  });

  // Test Case 4: Check no keyboard traps - user can Tab away from all elements
  test('TC4: User can Tab away from all elements (no keyboard traps)', async ({ page }) => {
    // Tab through all elements and verify we can always move forward
    const visitedElements = new Set();
    let maxIterations = 100;
    let iterations = 0;
    let consecutiveRepeats = 0;
    let lastElementKey = '';

    await page.keyboard.press('Tab');

    while (iterations < maxIterations) {
      const elementInfo = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el) return null;
        return {
          tagName: el.tagName,
          href: el.getAttribute('href'),
          id: el.id,
          className: el.className,
          textContent: el.textContent?.trim().substring(0, 30)
        };
      });

      if (!elementInfo || elementInfo.tagName === 'BODY') {
        // Reached the end or body, which is fine
        break;
      }

      const elementKey = `${elementInfo.tagName}-${elementInfo.href || ''}-${elementInfo.id}-${elementInfo.textContent}`;

      // Check for keyboard trap: same element focused multiple times in a row
      if (elementKey === lastElementKey) {
        consecutiveRepeats++;
        // Allow a few repeats for edge cases, but trap detection if stuck
        if (consecutiveRepeats > 3) {
          throw new Error(`Keyboard trap detected at element: ${elementKey}`);
        }
      } else {
        consecutiveRepeats = 0;
      }

      // If we've seen this element before and completed a full cycle, we're done
      if (visitedElements.has(elementKey) && visitedElements.size > 1) {
        break;
      }

      visitedElements.add(elementKey);
      lastElementKey = elementKey;

      await page.keyboard.press('Tab');
      iterations++;
    }

    // Verify we visited multiple unique elements (no trap)
    expect(visitedElements.size).toBeGreaterThan(3);

    // Also test Shift+Tab (reverse tabbing) to ensure we can go backwards
    await page.keyboard.press('Shift+Tab');

    const afterShiftTab = await page.evaluate(() => {
      const el = document.activeElement;
      return el && el.tagName !== 'BODY';
    });

    // Shift+Tab should still work (proves no trap in reverse direction)
    expect(afterShiftTab).toBe(true);
  });

  // Additional test: Verify focus visibility
  test('Focus styles are visible on interactive elements', async ({ page }) => {
    // Tab to the first interactive element
    await page.keyboard.press('Tab');

    // Wait for focus to settle
    await page.waitForTimeout(100);

    // Check that the focused element has visible focus styles
    const focusInfo = await page.evaluate(() => {
      const el = document.activeElement;
      if (!el || el === document.body) return { found: false };

      const styles = window.getComputedStyle(el);

      // Get outline properties
      const outlineWidth = styles.outlineWidth;
      const outlineStyle = styles.outlineStyle;
      const outlineColor = styles.outlineColor;
      const boxShadow = styles.boxShadow;

      // Check if element has visible outline or box-shadow for focus
      // outlineWidth should be > 0px and style not none
      const hasOutline = outlineWidth && outlineWidth !== '0px' && outlineStyle !== 'none';
      const hasBoxShadow = boxShadow && boxShadow !== 'none';

      return {
        found: true,
        tagName: el.tagName,
        outlineWidth,
        outlineStyle,
        outlineColor,
        boxShadow,
        hasOutline,
        hasBoxShadow
      };
    });

    expect(focusInfo.found).toBe(true);
    // Check that either outline or box-shadow provides visible focus indication
    expect(focusInfo.hasOutline || focusInfo.hasBoxShadow).toBe(true);
  });
});
