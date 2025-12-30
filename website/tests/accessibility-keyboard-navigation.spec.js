// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Accessibility - Keyboard Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: All interactive elements are reachable via Tab key', async ({ page }) => {
    // List of expected interactive elements in tab order
    const expectedElements = [
      { selector: '.skip-link', description: 'Skip to content link' },
      { selector: '.logo', description: 'Logo link' },
      { selector: '.mobile-menu-toggle', description: 'Mobile menu toggle', optional: true },
      { selector: 'nav a[href="#features"]', description: 'Features nav link' },
      { selector: 'nav a[href="#commands"]', description: 'Commands nav link' },
      { selector: 'nav a[href="#quick-start"]', description: 'Quick Start nav link' },
      { selector: 'nav a[href*="github"]', description: 'GitHub nav link' },
      { selector: '.hero-buttons .btn-primary', description: 'Get Started button' },
      { selector: '.hero-buttons .btn-secondary', description: 'View on GitHub button' },
      { selector: '.footer-links a[href*="github"]', description: 'Footer GitHub link' },
      { selector: '.footer-links a[href="#quick-start"]', description: 'Footer Documentation link' },
    ];

    // Start tabbing through the page
    let tabCount = 0;
    const maxTabs = 30; // Safety limit
    const reachedElements = new Set();

    while (tabCount < maxTabs) {
      await page.keyboard.press('Tab');
      tabCount++;

      // Get currently focused element
      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;
        return {
          tagName: el.tagName.toLowerCase(),
          className: el.className,
          href: el.getAttribute('href'),
          text: el.textContent?.trim().substring(0, 50)
        };
      });

      if (!focusedElement) continue;

      // Check if this matches any expected element
      for (const expected of expectedElements) {
        const matches = await page.locator(expected.selector).first().evaluate((el, focused) => {
          return document.activeElement === el;
        }, focusedElement);

        if (matches) {
          reachedElements.add(expected.description);
        }
      }

      // Check if we've looped back to the beginning (skip link or body)
      const isSkipLink = await page.locator('.skip-link').evaluate(el => document.activeElement === el);
      if (isSkipLink && tabCount > 5) break;
    }

    // Verify all required elements were reachable (excluding optional ones)
    const requiredElements = expectedElements.filter(e => !e.optional);
    for (const element of requiredElements) {
      expect(reachedElements.has(element.description),
        `${element.description} should be reachable via Tab key`).toBe(true);
    }
  });

  test('TC2: Each focused element has visible focus indicator', async ({ page }) => {
    // Tab through the page and verify each focused element has a visible indicator
    const maxTabs = 15;
    let focusedElementsWithIndicator = 0;
    const checkedSelectors = new Set();

    // Elements we expect to have focus indicators
    const expectedFocusableElements = [
      '.skip-link',
      '.logo',
      'nav a',
      '.btn-primary',
      '.btn-secondary',
      '.footer-links a'
    ];

    for (let i = 0; i < maxTabs; i++) {
      await page.keyboard.press('Tab');

      // Get the currently focused element and its styles
      const focusInfo = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;

        const computed = window.getComputedStyle(el);
        return {
          tagName: el.tagName.toLowerCase(),
          className: el.className,
          href: el.getAttribute('href'),
          outlineStyle: computed.outlineStyle,
          outlineWidth: computed.outlineWidth,
          outlineColor: computed.outlineColor,
          boxShadow: computed.boxShadow
        };
      });

      if (!focusInfo) continue;

      // Check if this is an interactive element that should have focus indicator
      const isInteractive = focusInfo.tagName === 'a' || focusInfo.tagName === 'button';

      if (isInteractive) {
        // Create a unique identifier for this element
        const elementId = `${focusInfo.tagName}-${focusInfo.className}-${focusInfo.href}`;

        if (!checkedSelectors.has(elementId)) {
          checkedSelectors.add(elementId);

          // Verify it has a visible focus indicator
          const hasVisibleOutline = focusInfo.outlineStyle !== 'none' &&
                                     parseInt(focusInfo.outlineWidth) > 0;
          const hasBoxShadow = focusInfo.boxShadow !== 'none';

          if (hasVisibleOutline || hasBoxShadow) {
            focusedElementsWithIndicator++;
          }

          // Assert that each interactive element has a focus indicator
          expect(hasVisibleOutline || hasBoxShadow,
            `Interactive element (${focusInfo.tagName} with class "${focusInfo.className}") should have visible focus indicator. ` +
            `Got outline: ${focusInfo.outlineStyle} ${focusInfo.outlineWidth} ${focusInfo.outlineColor}, boxShadow: ${focusInfo.boxShadow}`
          ).toBe(true);
        }
      }
    }

    // Ensure we found and checked at least some interactive elements
    expect(focusedElementsWithIndicator).toBeGreaterThan(0);
  });

  test('TC3: Skip to content link appears on first Tab press', async ({ page }) => {
    // Press Tab once to focus the skip link
    await page.keyboard.press('Tab');

    // Get the skip link
    const skipLink = page.locator('.skip-link');

    // Verify skip link exists
    await expect(skipLink).toHaveCount(1);

    // Verify skip link is now visible (focused state moves it into view)
    const isVisible = await skipLink.evaluate(el => {
      const rect = el.getBoundingClientRect();
      const computed = window.getComputedStyle(el);
      // Check if element is in viewport when focused
      return rect.top >= 0 && computed.visibility !== 'hidden';
    });

    expect(isVisible).toBe(true);

    // Verify the skip link has correct text
    await expect(skipLink).toContainText(/skip/i);

    // Verify the skip link points to main content
    const href = await skipLink.getAttribute('href');
    expect(href).toBe('#main-content');

    // Verify clicking the skip link focuses main content
    await skipLink.click();

    // Check that focus or scroll moved to main content
    const mainSection = page.locator('#main-content, main').first();
    await expect(mainSection).toBeVisible();
  });

  test('TC4: Buttons and links can be activated using Enter or Space key', async ({ page }) => {
    // Test Enter key on navigation links
    const featuresLink = page.locator('nav a[href="#features"]').first();
    await featuresLink.focus();
    await page.keyboard.press('Enter');

    // Verify navigation occurred
    await expect(page).toHaveURL(/#features/);

    // Reset page
    await page.goto('/');

    // Test Enter key on primary button (Get Started)
    const getStartedBtn = page.locator('.hero-buttons .btn-primary').first();
    await getStartedBtn.focus();
    await page.keyboard.press('Enter');

    // Verify navigation to quick-start
    await expect(page).toHaveURL(/#quick-start/);

    // Reset page
    await page.goto('/');

    // Test Space key on links (links respond to Enter, not Space by default in HTML)
    // But buttons should respond to Space
    const menuToggle = page.locator('.mobile-menu-toggle');

    // Set viewport to mobile to make menu toggle visible
    await page.setViewportSize({ width: 375, height: 667 });

    // Wait for menu toggle to be visible
    await expect(menuToggle).toBeVisible();

    // Focus and press Space
    await menuToggle.focus();
    await page.keyboard.press('Space');

    // Verify menu opened (aria-expanded changed)
    await expect(menuToggle).toHaveAttribute('aria-expanded', 'true');

    // Press Space again to close
    await page.keyboard.press('Space');
    await expect(menuToggle).toHaveAttribute('aria-expanded', 'false');

    // Press Enter to open
    await page.keyboard.press('Enter');
    await expect(menuToggle).toHaveAttribute('aria-expanded', 'true');
  });
});
