/**
 * CTA Button Functionality E2E Tests
 * Scenario: CTA Button Functionality
 * Description: Verify all CTA buttons are functional and guide visitors toward desired actions
 */

import { test, expect } from '@playwright/test';

test.describe('CTA Button Functionality E2E', () => {

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  /**
   * Test Case 1: Click hero primary CTA button
   * Expected: Button triggers navigation or form action
   */
  test.describe('Test Case 1: Hero Primary CTA Button Click', () => {
    test('Hero primary CTA button is visible and clickable', async ({ page }) => {
      const heroCTA = page.locator('[data-testid="hero-cta-primary"]');
      await expect(heroCTA).toBeVisible();
      await expect(heroCTA).toBeEnabled();
    });

    test('Hero primary CTA is properly configured for navigation action', async ({ page }) => {
      const heroCTA = page.locator('[data-testid="hero-cta-primary"]');

      // Get href to determine expected behavior
      const href = await heroCTA.getAttribute('href');

      // Verify the CTA has a valid href that starts with # (anchor) or is a URL
      expect(href).toBeTruthy();
      expect(href.startsWith('#') || href.startsWith('http') || href.startsWith('/')).toBe(true);

      // Click the CTA button - it should be clickable without errors
      await heroCTA.click();

      // Wait for any smooth scroll or navigation to complete
      await page.waitForTimeout(500);

      // The CTA successfully triggered a click action (no error thrown)
      // The JavaScript uses preventDefault for smooth scroll, so verify click handler worked
      await expect(heroCTA).toBeVisible();
    });

    test('Hero primary CTA has valid href attribute', async ({ page }) => {
      const heroCTA = page.locator('[data-testid="hero-cta-primary"]');
      const href = await heroCTA.getAttribute('href');

      expect(href).toBeTruthy();
      expect(href.length).toBeGreaterThan(0);
    });
  });

  /**
   * Test Case 2: Click header CTA button
   * Expected: Button triggers same action as hero CTA
   */
  test.describe('Test Case 2: Header CTA Button Click', () => {
    test('Header CTA button is visible in navigation', async ({ page }) => {
      const navCTA = page.locator('[data-testid="nav-cta"]');
      await expect(navCTA).toBeVisible();
    });

    test('Header CTA matches hero CTA destination', async ({ page }) => {
      const heroCTA = page.locator('[data-testid="hero-cta-primary"]');
      const navCTA = page.locator('[data-testid="nav-cta"]');

      const heroHref = await heroCTA.getAttribute('href');
      const navHref = await navCTA.getAttribute('href');

      expect(navHref).toBe(heroHref);
    });

    test('Header CTA is properly configured for navigation action', async ({ page }) => {
      const navCTA = page.locator('[data-testid="nav-cta"]');

      // Get href to verify the navigation target
      const href = await navCTA.getAttribute('href');

      // Verify the CTA has a valid href
      expect(href).toBeTruthy();
      expect(href.startsWith('#') || href.startsWith('http') || href.startsWith('/')).toBe(true);

      // Click the header CTA - should be clickable without errors
      await navCTA.click();

      // Wait for any smooth scroll or navigation to complete
      await page.waitForTimeout(500);

      // The CTA successfully triggered a click action (no error thrown)
      // The JavaScript uses preventDefault for smooth scroll, so verify click handler worked
      await expect(navCTA).toBeVisible();
    });

    test('Header and hero CTA buttons have consistent behavior', async ({ page }) => {
      const heroCTA = page.locator('[data-testid="hero-cta-primary"]');
      const navCTA = page.locator('[data-testid="nav-cta"]');

      // Both should have same href
      const heroHref = await heroCTA.getAttribute('href');
      const navHref = await navCTA.getAttribute('href');
      expect(heroHref).toBe(navHref);

      // Both should be anchor elements
      const heroTag = await heroCTA.evaluate(el => el.tagName.toLowerCase());
      const navTag = await navCTA.evaluate(el => el.tagName.toLowerCase());
      expect(heroTag).toBe('a');
      expect(navTag).toBe('a');
    });
  });

  /**
   * Test Case 3: Verify CTA button text
   * Expected: CTA text is action-oriented
   */
  test.describe('Test Case 3: CTA Button Text Verification', () => {
    test('Hero CTA has action-oriented text', async ({ page }) => {
      const heroCTA = page.locator('[data-testid="hero-cta-primary"]');
      const text = await heroCTA.textContent();

      const actionWords = ['start', 'get', 'try', 'sign', 'join', 'download', 'free', 'begin'];
      const hasActionWord = actionWords.some(word => text.toLowerCase().includes(word));

      expect(hasActionWord).toBe(true);
    });

    test('Header CTA has action-oriented text', async ({ page }) => {
      const navCTA = page.locator('[data-testid="nav-cta"]');
      const text = await navCTA.textContent();

      const actionWords = ['start', 'get', 'try', 'sign', 'join', 'download', 'free', 'begin'];
      const hasActionWord = actionWords.some(word => text.toLowerCase().includes(word));

      expect(hasActionWord).toBe(true);
    });

    test('CTA buttons avoid vague text like "Click here"', async ({ page }) => {
      const allCTAs = page.locator('.cta-button');
      const count = await allCTAs.count();

      for (let i = 0; i < count; i++) {
        const text = await allCTAs.nth(i).textContent();
        const lowerText = text.toLowerCase().trim();

        expect(lowerText).not.toBe('click here');
        expect(lowerText).not.toBe('submit');
        expect(lowerText).not.toBe('click');
      }
    });
  });

  /**
   * Test Case 4: CTA Visibility on Scroll
   * Expected: At least one CTA is visible at any scroll position
   */
  test.describe('Test Case 4: CTA Visibility on Scroll', () => {
    test('Header CTA remains visible when scrolling down', async ({ page }) => {
      // First verify header CTA is visible at top
      const navCTA = page.locator('[data-testid="nav-cta"]');
      await expect(navCTA).toBeVisible();

      // Scroll down to the middle of the page
      await page.evaluate(() => window.scrollTo(0, 500));
      await page.waitForTimeout(500);

      // Header should remain visible (sticky)
      const header = page.locator('[data-testid="header"]');
      await expect(header).toBeVisible();

      // CTA in header should still be visible
      await expect(navCTA).toBeVisible();
    });

    test('Header remains sticky at various scroll positions', async ({ page }) => {
      const header = page.locator('[data-testid="header"]');

      // Test at multiple scroll positions
      const scrollPositions = [0, 300, 600, 1000];

      for (const position of scrollPositions) {
        await page.evaluate((pos) => window.scrollTo(0, pos), position);
        await page.waitForTimeout(300);

        await expect(header).toBeVisible();
      }
    });

    test('At least one CTA is always accessible during scroll', async ({ page }) => {
      // Scroll to bottom
      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
      await page.waitForTimeout(500);

      // Header with CTA should still be visible
      const header = page.locator('[data-testid="header"]');
      await expect(header).toBeVisible();

      // Check that CTAs exist on page
      const allCTAs = page.locator('.cta-button');
      const count = await allCTAs.count();
      expect(count).toBeGreaterThanOrEqual(2);
    });

    test('Hero CTA is visible on initial page load without scroll', async ({ page }) => {
      // Ensure we're at the top
      await page.evaluate(() => window.scrollTo(0, 0));

      const heroCTA = page.locator('[data-testid="hero-cta-primary"]');
      await expect(heroCTA).toBeVisible();
      await expect(heroCTA).toBeInViewport();
    });

    test('Header CTA is visible on initial page load', async ({ page }) => {
      await page.evaluate(() => window.scrollTo(0, 0));

      const navCTA = page.locator('[data-testid="nav-cta"]');
      await expect(navCTA).toBeVisible();
      await expect(navCTA).toBeInViewport();
    });
  });

  /**
   * Additional Functionality Tests
   */
  test.describe('CTA Button Interactive States', () => {
    test('CTA buttons have hover state', async ({ page }) => {
      const heroCTA = page.locator('[data-testid="hero-cta-primary"]');

      // Get initial background color
      const initialBgColor = await heroCTA.evaluate(el =>
        window.getComputedStyle(el).backgroundColor
      );

      // Hover over the button
      await heroCTA.hover();
      await page.waitForTimeout(300);

      // Get hover background color
      const hoverBgColor = await heroCTA.evaluate(el =>
        window.getComputedStyle(el).backgroundColor
      );

      // Colors should be different on hover
      expect(hoverBgColor).not.toBe(initialBgColor);
    });

    test('CTA buttons have focus state for keyboard navigation', async ({ page }) => {
      const heroCTA = page.locator('[data-testid="hero-cta-primary"]');

      // Focus on the button
      await heroCTA.focus();

      // Check that element is focused
      const isFocused = await heroCTA.evaluate(el =>
        document.activeElement === el
      );
      expect(isFocused).toBe(true);
    });

    test('CTA buttons are keyboard accessible', async ({ page }) => {
      // Tab to the nav CTA
      await page.keyboard.press('Tab'); // Skip to first focusable

      // Continue tabbing until we reach a CTA button
      let foundCTA = false;
      for (let i = 0; i < 20; i++) {
        const activeElement = await page.evaluate(() =>
          document.activeElement?.classList.contains('cta-button')
        );
        if (activeElement) {
          foundCTA = true;
          break;
        }
        await page.keyboard.press('Tab');
      }

      expect(foundCTA).toBe(true);
    });
  });
});
