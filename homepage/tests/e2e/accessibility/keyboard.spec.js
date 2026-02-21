/**
 * Keyboard Navigation E2E Tests
 * Owner: Scenario 12 - Accessibility - Keyboard Navigation
 *
 * Tests verify:
 * - All interactive elements are reachable via Tab key
 * - Focus indicators are visible on all focused elements
 * - Links activate correctly with Enter key
 * - Skip navigation link is present and functional
 */

const { test, expect } = require('@playwright/test');

test.describe('Keyboard Navigation Accessibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: All navigation links are reachable via Tab key', async ({ page }) => {
    // Start from the body and tab through all navigation links
    await page.keyboard.press('Tab'); // Skip nav
    await page.keyboard.press('Tab'); // Logo link
    await page.keyboard.press('Tab'); // Features nav link

    // Verify Features link is focused
    const featuresLink = page.locator('.nav-link[data-section="features"]');
    await expect(featuresLink).toBeFocused();

    await page.keyboard.press('Tab'); // Quick Start nav link
    const quickstartLink = page.locator('.nav-link[data-section="quickstart"]');
    await expect(quickstartLink).toBeFocused();

    await page.keyboard.press('Tab'); // Architecture nav link
    const architectureLink = page.locator('.nav-link[data-section="architecture"]');
    await expect(architectureLink).toBeFocused();

    await page.keyboard.press('Tab'); // GitHub nav link
    const githubNavLink = page.locator('header .nav-link-external');
    await expect(githubNavLink).toBeFocused();
  });

  test('Test Case 2: Get Started button is reachable via Tab key', async ({ page }) => {
    // Tab through to the CTA button
    const ctaButton = page.locator('#get-started-btn');

    // Focus on the CTA button using multiple tabs
    let found = false;
    for (let i = 0; i < 15; i++) {
      await page.keyboard.press('Tab');
      const focusedElement = page.locator(':focus');
      const id = await focusedElement.getAttribute('id');
      if (id === 'get-started-btn') {
        found = true;
        break;
      }
    }

    expect(found).toBe(true);
    await expect(ctaButton).toBeFocused();
  });

  test('Test Case 3: Focused elements have visible focus indicators', async ({ page }) => {
    // Check focus indicator on skip nav link
    await page.keyboard.press('Tab');
    const skipNav = page.locator('.skip-nav');
    await expect(skipNav).toBeFocused();

    // Verify focus indicator is visible (check computed style)
    const skipNavOutline = await skipNav.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        outlineStyle: style.outlineStyle,
        outlineWidth: style.outlineWidth,
        outlineColor: style.outlineColor
      };
    });

    // Focus indicator should have a visible outline
    expect(skipNavOutline.outlineStyle).not.toBe('none');
    expect(parseInt(skipNavOutline.outlineWidth)).toBeGreaterThan(0);

    // Check focus indicator on logo
    await page.keyboard.press('Tab');
    const logoLink = page.locator('.header-logo');
    await expect(logoLink).toBeFocused();

    const logoOutline = await logoLink.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        outlineStyle: style.outlineStyle,
        outlineWidth: style.outlineWidth
      };
    });

    expect(logoOutline.outlineStyle).not.toBe('none');

    // Check focus indicator on navigation link
    await page.keyboard.press('Tab');
    const navLink = page.locator('.nav-link').first();
    await expect(navLink).toBeFocused();

    const navLinkOutline = await navLink.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        outlineStyle: style.outlineStyle,
        outlineWidth: style.outlineWidth
      };
    });

    expect(navLinkOutline.outlineStyle).not.toBe('none');
    expect(parseInt(navLinkOutline.outlineWidth)).toBeGreaterThan(0);
  });

  test('Test Case 4: Link activates correctly with Enter key', async ({ page }) => {
    // Navigate to the quickstart link in the navigation
    const quickstartLink = page.locator('.nav-link[href="#quickstart"]');

    // Tab to the quickstart link
    let found = false;
    for (let i = 0; i < 10; i++) {
      await page.keyboard.press('Tab');
      const focusedHref = await page.locator(':focus').getAttribute('href');
      if (focusedHref === '#quickstart') {
        found = true;
        break;
      }
    }

    expect(found).toBe(true);

    // Get the initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Press Enter to activate the link
    await page.keyboard.press('Enter');

    // Wait for scroll or navigation
    await page.waitForTimeout(500);

    // Verify the URL hash changed or page scrolled
    const currentUrl = page.url();
    const currentScrollY = await page.evaluate(() => window.scrollY);

    // Either the URL should contain #quickstart or the page should have scrolled
    const urlContainsHash = currentUrl.includes('#quickstart');
    const pageScrolled = currentScrollY !== initialScrollY;

    expect(urlContainsHash || pageScrolled).toBe(true);
  });

  test('Test Case 5: Skip to main content link is present and functional', async ({ page }) => {
    // Check skip nav link exists
    const skipNav = page.locator('.skip-nav');
    await expect(skipNav).toBeAttached();

    // Check skip nav has correct href
    const href = await skipNav.getAttribute('href');
    expect(href).toBe('#content');

    // Check skip nav text content
    const text = await skipNav.textContent();
    expect(text?.toLowerCase()).toContain('skip');

    // Tab to skip nav and verify it becomes visible
    await page.keyboard.press('Tab');
    await expect(skipNav).toBeFocused();

    // Wait for CSS transition to complete
    await page.waitForTimeout(300);

    // Verify skip nav is now visible (checking visibility state rather than exact position)
    const skipNavVisibility = await skipNav.evaluate((el) => {
      const style = window.getComputedStyle(el);
      const rect = el.getBoundingClientRect();
      return {
        visibility: style.visibility,
        opacity: parseFloat(style.opacity),
        display: style.display,
        // Check if element is within viewport (even partially)
        isInViewport: rect.bottom > 0 && rect.top < window.innerHeight
      };
    });

    // Skip nav should be visible when focused
    expect(skipNavVisibility.visibility).not.toBe('hidden');
    expect(skipNavVisibility.display).not.toBe('none');
    expect(skipNavVisibility.isInViewport).toBe(true);

    // Press Enter to activate skip link
    await page.keyboard.press('Enter');

    // Wait for the navigation to complete
    await page.waitForTimeout(300);

    // Verify the main content is now in view or focused
    const mainContent = page.locator('#content');
    await expect(mainContent).toBeInViewport();
  });

  test('All interactive elements can be reached in tab order', async ({ page }) => {
    // Tab through all elements and verify we can reach key interactive elements
    const visitedElements = [];
    const maxTabs = 30;

    for (let i = 0; i < maxTabs; i++) {
      await page.keyboard.press('Tab');

      // Check if any element is focused - use evaluate with a null check
      const focusedInfo = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) {
          return null;
        }
        return {
          tagName: el.tagName?.toLowerCase() || '',
          className: el.className || '',
          id: el.id || ''
        };
      });

      if (focusedInfo) {
        visitedElements.push(focusedInfo);
      }
    }

    // Verify we visited navigation links
    const visitedNavLinks = visitedElements.filter(el => el.className.includes('nav-link'));
    expect(visitedNavLinks.length).toBeGreaterThan(0);

    // Verify we visited CTA button
    const visitedCta = visitedElements.some(el => el.id === 'get-started-btn');
    expect(visitedCta).toBe(true);

    // Verify we visited copy buttons
    const visitedCopyBtns = visitedElements.filter(el => el.className.includes('copy-btn'));
    expect(visitedCopyBtns.length).toBeGreaterThan(0);
  });

  test('Focus does not get trapped in any element', async ({ page }) => {
    // Tab through all elements and ensure we eventually wrap back or reach footer
    const maxTabs = 50;
    let reachedFooter = false;

    for (let i = 0; i < maxTabs; i++) {
      await page.keyboard.press('Tab');

      const focused = page.locator(':focus');
      const className = await focused.getAttribute('class') || '';

      if (className.includes('footer-link')) {
        reachedFooter = true;
        break;
      }
    }

    expect(reachedFooter).toBe(true);
  });

  test('Reverse tab (Shift+Tab) works correctly', async ({ page }) => {
    // Tab to a known element
    const ctaButton = page.locator('#get-started-btn');

    // Tab to CTA button
    for (let i = 0; i < 15; i++) {
      await page.keyboard.press('Tab');
      const id = await page.locator(':focus').getAttribute('id');
      if (id === 'get-started-btn') break;
    }

    await expect(ctaButton).toBeFocused();

    // Shift+Tab should move focus backwards
    await page.keyboard.press('Shift+Tab');

    // Should now be on GitHub link or previous element
    const previousFocused = page.locator(':focus');
    const previousId = await previousFocused.getAttribute('id');

    // The previous element should not be the CTA button
    expect(previousId).not.toBe('get-started-btn');

    // Should be a focusable element (link or button)
    const tagName = await previousFocused.evaluate(el => el.tagName.toLowerCase());
    expect(['a', 'button']).toContain(tagName);
  });
});
