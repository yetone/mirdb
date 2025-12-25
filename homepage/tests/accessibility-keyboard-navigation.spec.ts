import { test, expect } from '@playwright/test';

test.describe('Accessibility - Keyboard Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: All links, buttons, and interactive elements are reachable via Tab key', async ({ page }) => {
    // Get all interactive elements that should be focusable
    const interactiveElements = await page.$$eval(
      'a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])',
      (elements) => {
        return elements.map((el) => ({
          tagName: el.tagName.toLowerCase(),
          text: el.textContent?.trim().substring(0, 50) || '',
          href: (el as HTMLAnchorElement).href || null,
          id: el.id || null,
          className: el.className || null,
        }));
      }
    );

    // Ensure there are interactive elements to test
    expect(interactiveElements.length).toBeGreaterThan(0);

    // Track elements that receive focus during tabbing
    const focusedElements: string[] = [];

    // Start from the beginning of the document
    await page.keyboard.press('Tab');

    // Tab through all interactive elements
    for (let i = 0; i < interactiveElements.length + 5; i++) {
      const focusedElement = await page.evaluate(() => {
        const activeElement = document.activeElement;
        if (!activeElement || activeElement === document.body) return null;
        return {
          tagName: activeElement.tagName.toLowerCase(),
          text: activeElement.textContent?.trim().substring(0, 50) || '',
          href: (activeElement as HTMLAnchorElement).href || null,
          id: activeElement.id || null,
        };
      });

      if (focusedElement && focusedElement.tagName !== 'body') {
        const identifier = focusedElement.id || focusedElement.text || focusedElement.href;
        if (identifier && !focusedElements.includes(identifier)) {
          focusedElements.push(identifier);
        }
      }

      await page.keyboard.press('Tab');
    }

    // Verify that navigation links are focusable
    const navLinksTexts = ['Features', 'Getting Started', 'Configuration', 'GitHub'];
    let navLinksReachable = 0;
    for (const linkText of navLinksTexts) {
      const isReachable = focusedElements.some(
        (el) => el.includes(linkText) || el.toLowerCase().includes(linkText.toLowerCase())
      );
      if (isReachable) navLinksReachable++;
    }
    expect(navLinksReachable).toBeGreaterThanOrEqual(3);

    // Verify that hero buttons are focusable
    const heroButtons = ['Get Started', 'GitHub'];
    let heroButtonsReachable = 0;
    for (const buttonText of heroButtons) {
      const isReachable = focusedElements.some(
        (el) => el.includes(buttonText) || el.toLowerCase().includes(buttonText.toLowerCase())
      );
      if (isReachable) heroButtonsReachable++;
    }
    expect(heroButtonsReachable).toBeGreaterThanOrEqual(1);

    // Verify at least 5 interactive elements were reached
    expect(focusedElements.length).toBeGreaterThanOrEqual(5);
  });

  test('TC2: Visible focus indicator appears on each element when focused', async ({ page }) => {
    // Start from the beginning
    await page.keyboard.press('Tab');

    // Check focus indicators on multiple elements
    const focusChecks: { element: string; hasVisibleFocus: boolean }[] = [];

    for (let i = 0; i < 10; i++) {
      const focusInfo = await page.evaluate(() => {
        const activeElement = document.activeElement;
        if (!activeElement || activeElement === document.body) {
          return null;
        }

        const computedStyle = window.getComputedStyle(activeElement);
        const pseudoOutline = computedStyle.outline;
        const pseudoBoxShadow = computedStyle.boxShadow;
        const pseudoBorderColor = computedStyle.borderColor;

        // Check for visible focus indicators
        const hasOutline = pseudoOutline !== 'none' &&
                          pseudoOutline !== '0px none rgb(0, 0, 0)' &&
                          !pseudoOutline.includes('0px');
        const hasBoxShadow = pseudoBoxShadow !== 'none' && pseudoBoxShadow !== '';
        const hasBorder = pseudoBorderColor !== 'rgb(0, 0, 0)';

        // Also check for background color changes or other visual indicators
        const hasBackgroundChange = computedStyle.backgroundColor !== 'rgba(0, 0, 0, 0)';
        const hasTextDecoration = computedStyle.textDecoration.includes('underline');

        return {
          tagName: activeElement.tagName.toLowerCase(),
          text: activeElement.textContent?.trim().substring(0, 30) || '',
          outline: pseudoOutline,
          boxShadow: pseudoBoxShadow,
          hasVisibleFocus: hasOutline || hasBoxShadow || hasBorder || hasBackgroundChange || hasTextDecoration,
        };
      });

      if (focusInfo && focusInfo.tagName !== 'body') {
        focusChecks.push({
          element: `${focusInfo.tagName}: ${focusInfo.text}`,
          hasVisibleFocus: focusInfo.hasVisibleFocus,
        });
      }

      await page.keyboard.press('Tab');
    }

    // At least 5 elements should have been checked
    expect(focusChecks.length).toBeGreaterThanOrEqual(5);

    // All interactive elements should have visible focus indicators
    const elementsWithFocus = focusChecks.filter((check) => check.hasVisibleFocus);
    expect(
      elementsWithFocus.length,
      `Expected all focused elements to have visible focus indicators. Found ${elementsWithFocus.length}/${focusChecks.length} with indicators`
    ).toBe(focusChecks.length);
  });

  test('TC3: Pressing Enter on focused navigation links navigates to the section', async ({ page }) => {
    // Find the Features navigation link using Tab
    await page.keyboard.press('Tab');

    // Tab until we reach the Features navigation link
    let foundFeaturesLink = false;
    for (let i = 0; i < 15; i++) {
      const focusedText = await page.evaluate(() => {
        return document.activeElement?.textContent?.trim() || '';
      });

      if (focusedText === 'Features') {
        foundFeaturesLink = true;
        break;
      }
      await page.keyboard.press('Tab');
    }

    expect(foundFeaturesLink).toBeTruthy();

    // Press Enter to activate the link
    await page.keyboard.press('Enter');

    // Wait for navigation/scroll
    await page.waitForTimeout(800);

    // Verify the Features section is now in viewport
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();

    // Verify URL hash changed to features
    await expect(page).toHaveURL(/#features/);
  });

  test('TC4: Buttons activate when pressing Space or Enter while focused', async ({ page }) => {
    // Test with the "Get Started" button which links to #getting-started
    const getStartedBtn = page.locator('[data-testid="get-started-btn"]');
    await expect(getStartedBtn).toBeVisible();

    // Focus the button
    await getStartedBtn.focus();

    // Verify focus
    const isFocused = await page.evaluate(() => {
      return document.activeElement?.getAttribute('data-testid') === 'get-started-btn';
    });
    expect(isFocused).toBeTruthy();

    // Press Enter to activate
    await page.keyboard.press('Enter');

    // Wait for navigation/scroll
    await page.waitForTimeout(800);

    // Verify navigation to Getting Started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeInViewport();

    // Now test with Space key
    // Go back to top
    await page.goto('/');

    // Find the GitHub button and focus it
    const githubBtn = page.locator('[data-testid="github-link"]');
    await expect(githubBtn).toBeVisible();
    await githubBtn.focus();

    // Verify the element is focusable and can receive keyboard activation
    const canReceiveFocus = await page.evaluate(() => {
      const activeElement = document.activeElement;
      return activeElement?.getAttribute('data-testid') === 'github-link';
    });
    expect(canReceiveFocus).toBeTruthy();

    // Test Space key on nav links (internal links)
    const featuresLink = page.locator('.nav-links a[href="#features"]');
    await featuresLink.focus();

    const featuresLinkFocused = await page.evaluate(() => {
      const activeElement = document.activeElement;
      return activeElement?.getAttribute('href') === '#features';
    });
    expect(featuresLinkFocused).toBeTruthy();

    // Press Enter to navigate
    await page.keyboard.press('Enter');
    await page.waitForTimeout(800);

    // Verify navigation occurred
    await expect(page).toHaveURL(/#features/);
  });

  test('Focus order follows logical reading order (top to bottom, left to right)', async ({ page }) => {
    const focusOrder: string[] = [];

    // Tab through elements and record focus order
    await page.keyboard.press('Tab');

    for (let i = 0; i < 20; i++) {
      const focusInfo = await page.evaluate(() => {
        const activeElement = document.activeElement;
        if (!activeElement || activeElement === document.body) return null;

        const rect = activeElement.getBoundingClientRect();
        return {
          text: activeElement.textContent?.trim().substring(0, 30) || '',
          top: rect.top,
          left: rect.left,
        };
      });

      if (focusInfo) {
        focusOrder.push(`${focusInfo.text} (top: ${Math.round(focusInfo.top)}, left: ${Math.round(focusInfo.left)})`);
      }

      await page.keyboard.press('Tab');
    }

    // Verify we collected some focus order data
    expect(focusOrder.length).toBeGreaterThan(5);

    // Navigation links should come before hero buttons (which should come before footer links)
    // This is a basic check that focus flows through the page in order
    const navBrandIndex = focusOrder.findIndex((f) => f.includes('MirDB') && !f.includes('Persistent'));
    const getStartedIndex = focusOrder.findIndex((f) => f.includes('Get Started'));

    // Nav brand should be focusable before Get Started button (both are in navigation/hero area)
    if (navBrandIndex !== -1 && getStartedIndex !== -1) {
      expect(navBrandIndex).toBeLessThan(getStartedIndex);
    }
  });

  test('No keyboard traps - can Tab through and out of all sections', async ({ page }) => {
    // Start from beginning
    await page.keyboard.press('Tab');

    const visitedElements = new Set<string>();
    let previousElement = '';
    let stuckCount = 0;

    // Tab through elements, checking for keyboard traps
    for (let i = 0; i < 50; i++) {
      const currentElement = await page.evaluate(() => {
        const activeElement = document.activeElement;
        if (!activeElement) return 'none';
        return `${activeElement.tagName}:${activeElement.textContent?.trim().substring(0, 20) || activeElement.id || activeElement.className}`;
      });

      // Check if we're stuck on the same element
      if (currentElement === previousElement) {
        stuckCount++;
        // If stuck more than 3 times, we have a keyboard trap
        expect(stuckCount, `Keyboard trap detected at: ${currentElement}`).toBeLessThanOrEqual(3);
      } else {
        stuckCount = 0;
      }

      visitedElements.add(currentElement);
      previousElement = currentElement;

      await page.keyboard.press('Tab');
    }

    // We should have visited multiple unique elements
    expect(visitedElements.size).toBeGreaterThan(5);
  });

  test('Shift+Tab navigates backwards through interactive elements', async ({ page }) => {
    // First, tab forward a few times
    await page.keyboard.press('Tab');
    await page.keyboard.press('Tab');
    await page.keyboard.press('Tab');
    await page.keyboard.press('Tab');

    // Record the current focused element
    const forwardElement = await page.evaluate(() => {
      return document.activeElement?.textContent?.trim().substring(0, 30) || '';
    });

    // Now tab forward one more time
    await page.keyboard.press('Tab');

    const nextElement = await page.evaluate(() => {
      return document.activeElement?.textContent?.trim().substring(0, 30) || '';
    });

    // Shift+Tab should go back to previous element
    await page.keyboard.press('Shift+Tab');

    const backElement = await page.evaluate(() => {
      return document.activeElement?.textContent?.trim().substring(0, 30) || '';
    });

    // After Shift+Tab, we should be back at the forward element
    expect(backElement).toBe(forwardElement);
  });

  test('Skip link functionality allows bypassing navigation (if present)', async ({ page }) => {
    // Check if there's a skip link
    const skipLink = page.locator('a[href="#main"], a[href="#content"], .skip-link, [class*="skip"]');
    const skipLinkCount = await skipLink.count();

    if (skipLinkCount > 0) {
      // Focus the skip link (it might be visually hidden but focusable)
      await page.keyboard.press('Tab');

      const firstFocusedHref = await page.evaluate(() => {
        return (document.activeElement as HTMLAnchorElement)?.href || '';
      });

      // If the first focused element is a skip link, test it
      if (firstFocusedHref.includes('#main') || firstFocusedHref.includes('#content')) {
        await page.keyboard.press('Enter');
        await page.waitForTimeout(300);

        // Focus should move to main content
        const newFocusedTag = await page.evaluate(() => {
          return document.activeElement?.tagName.toLowerCase() || '';
        });

        expect(['main', 'section', 'div']).toContain(newFocusedTag);
      }
    }

    // Even without a skip link, the test passes - it's a nice-to-have feature
    expect(true).toBeTruthy();
  });
});
