import { test, expect } from '@playwright/test';

/**
 * Smooth Scroll Navigation - E2E Tests
 *
 * This test file covers REQ-10: Homepage shall provide smooth scroll
 * navigation for single-page sections.
 *
 * Test Cases:
 * 1. E2E: Click 'Features' anchor link → Page smoothly scrolls to Features section
 * 2. E2E: Click 'How It Works' anchor link → Page smoothly scrolls to How It Works section
 * 3. Unit: Verify CSS scroll-behavior: smooth is applied to html/body
 * 4. E2E: Test keyboard navigation with anchor links
 */

test.describe('Smooth Scroll Navigation - E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for the page to fully load
    await page.waitForLoadState('networkidle');
  });

  /**
   * Test Case 1: E2E Test
   * Input: Click 'Features' anchor link
   * Expected: Page smoothly scrolls to Features section
   */
  test.describe('Test Case 1: Features Anchor Link Navigation', () => {
    test('Features link is visible in desktop navigation', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 720 });
      const featuresLink = page.getByTestId('nav-features-desktop');

      await expect(featuresLink).toBeVisible();
      await expect(featuresLink).toHaveAttribute('href', '#features');
    });

    test('clicking Features link scrolls to Features section', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 720 });
      const featuresLink = page.getByTestId('nav-features-desktop');
      const featuresSection = page.locator('#features');

      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);

      // Click the Features link
      await featuresLink.click();

      // Wait for scroll to complete
      await page.waitForTimeout(1000);

      // Verify the Features section is now in view
      await expect(featuresSection).toBeInViewport();

      // Verify scroll occurred
      const finalScrollY = await page.evaluate(() => window.scrollY);
      expect(finalScrollY).toBeGreaterThan(initialScrollY);
    });

    test('Features section header is visible after scroll', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 720 });
      const featuresLink = page.getByTestId('nav-features-desktop');

      await featuresLink.click();
      await page.waitForTimeout(1000);

      const heading = page.locator('#features h2');
      await expect(heading).toBeVisible();
      await expect(heading).toContainText('Powerful Features');
    });

    test('scroll to Features section is smooth (not instant)', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 720 });

      // Track scroll positions over time
      const scrollPositions: number[] = [];

      // Start recording scroll positions
      await page.evaluate(() => {
        (window as Window & { scrollPositions?: number[] }).scrollPositions = [];
        const recordScroll = () => {
          (window as Window & { scrollPositions?: number[] }).scrollPositions?.push(window.scrollY);
        };
        window.addEventListener('scroll', recordScroll);
        // Clean up after 2 seconds
        setTimeout(() => window.removeEventListener('scroll', recordScroll), 2000);
      });

      const featuresLink = page.getByTestId('nav-features-desktop');
      await featuresLink.click();

      // Wait for smooth scroll animation
      await page.waitForTimeout(1000);

      // Get recorded positions
      const positions = await page.evaluate(() => (window as Window & { scrollPositions?: number[] }).scrollPositions || []);

      // Smooth scroll should have multiple intermediate positions
      // (instant jump would have 0-1 positions or instant change)
      expect(positions.length).toBeGreaterThan(1);
    });
  });

  /**
   * Test Case 2: E2E Test
   * Input: Click 'How It Works' anchor link
   * Expected: Page smoothly scrolls to How It Works section
   */
  test.describe('Test Case 2: How It Works Anchor Link Navigation', () => {
    test('How It Works link is visible in desktop navigation', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 720 });
      const howItWorksLink = page.getByTestId('nav-how-it-works-desktop');

      await expect(howItWorksLink).toBeVisible();
      await expect(howItWorksLink).toHaveAttribute('href', '#how-it-works');
    });

    test('clicking How It Works link scrolls to How It Works section', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 720 });
      const howItWorksLink = page.getByTestId('nav-how-it-works-desktop');
      const howItWorksSection = page.locator('#how-it-works');

      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);

      // Click the How It Works link
      await howItWorksLink.click();

      // Wait for scroll to complete
      await page.waitForTimeout(1000);

      // Verify the How It Works section is now in view
      await expect(howItWorksSection).toBeInViewport();

      // Verify scroll occurred
      const finalScrollY = await page.evaluate(() => window.scrollY);
      expect(finalScrollY).toBeGreaterThan(initialScrollY);
    });

    test('How It Works section header is visible after scroll', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 720 });
      const howItWorksLink = page.getByTestId('nav-how-it-works-desktop');

      await howItWorksLink.click();
      await page.waitForTimeout(1000);

      const heading = page.locator('#how-it-works h2');
      await expect(heading).toBeVisible();
      await expect(heading).toContainText('How It Works');
    });

    test('scroll to How It Works section is smooth (not instant)', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 720 });

      // Track scroll positions over time
      await page.evaluate(() => {
        (window as Window & { scrollPositions?: number[] }).scrollPositions = [];
        const recordScroll = () => {
          (window as Window & { scrollPositions?: number[] }).scrollPositions?.push(window.scrollY);
        };
        window.addEventListener('scroll', recordScroll);
        setTimeout(() => window.removeEventListener('scroll', recordScroll), 2000);
      });

      const howItWorksLink = page.getByTestId('nav-how-it-works-desktop');
      await howItWorksLink.click();

      await page.waitForTimeout(1000);

      const positions = await page.evaluate(() => (window as Window & { scrollPositions?: number[] }).scrollPositions || []);

      // Smooth scroll should have multiple intermediate positions
      expect(positions.length).toBeGreaterThan(1);
    });
  });

  /**
   * Test Case 3: Unit Test
   * Input: Verify CSS scroll-behavior property
   * Expected: scroll-behavior: smooth is applied to html/body
   */
  test.describe('Test Case 3: CSS scroll-behavior Property', () => {
    test('html element has scroll-behavior: smooth applied', async ({ page }) => {
      const scrollBehavior = await page.evaluate(() => {
        return window.getComputedStyle(document.documentElement).scrollBehavior;
      });

      expect(scrollBehavior).toBe('smooth');
    });

    test('scroll-behavior is not set to auto (default)', async ({ page }) => {
      const scrollBehavior = await page.evaluate(() => {
        return window.getComputedStyle(document.documentElement).scrollBehavior;
      });

      expect(scrollBehavior).not.toBe('auto');
    });
  });

  /**
   * Test Case 4: E2E Test
   * Input: Test keyboard navigation with anchor links
   * Expected: Anchor links are keyboard accessible and trigger smooth scroll
   */
  test.describe('Test Case 4: Keyboard Navigation with Anchor Links', () => {
    test('Features link is focusable via Tab key', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 720 });

      // Focus on the first element and tab through
      await page.keyboard.press('Tab'); // Logo
      await page.keyboard.press('Tab'); // Features link

      const featuresLink = page.getByTestId('nav-features-desktop');
      await expect(featuresLink).toBeFocused();
    });

    test('How It Works link is focusable via Tab key', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 720 });

      // Focus on the first element and tab through
      await page.keyboard.press('Tab'); // Logo
      await page.keyboard.press('Tab'); // Features link
      await page.keyboard.press('Tab'); // How It Works link

      const howItWorksLink = page.getByTestId('nav-how-it-works-desktop');
      await expect(howItWorksLink).toBeFocused();
    });

    test('Enter key on Features link triggers smooth scroll', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 720 });

      const initialScrollY = await page.evaluate(() => window.scrollY);

      // Navigate to Features link via Tab
      await page.keyboard.press('Tab'); // Logo
      await page.keyboard.press('Tab'); // Features link

      // Press Enter to activate
      await page.keyboard.press('Enter');

      // Wait for scroll
      await page.waitForTimeout(1000);

      const finalScrollY = await page.evaluate(() => window.scrollY);
      expect(finalScrollY).toBeGreaterThan(initialScrollY);

      // Verify Features section is in viewport
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();
    });

    test('Enter key on How It Works link triggers smooth scroll', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 720 });

      const initialScrollY = await page.evaluate(() => window.scrollY);

      // Navigate to How It Works link via Tab
      await page.keyboard.press('Tab'); // Logo
      await page.keyboard.press('Tab'); // Features link
      await page.keyboard.press('Tab'); // How It Works link

      // Press Enter to activate
      await page.keyboard.press('Enter');

      // Wait for scroll
      await page.waitForTimeout(1000);

      const finalScrollY = await page.evaluate(() => window.scrollY);
      expect(finalScrollY).toBeGreaterThan(initialScrollY);

      // Verify How It Works section is in viewport
      const howItWorksSection = page.locator('#how-it-works');
      await expect(howItWorksSection).toBeInViewport();
    });

    test('anchor links have proper accessibility attributes', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 720 });

      const featuresLink = page.getByTestId('nav-features-desktop');
      const howItWorksLink = page.getByTestId('nav-how-it-works-desktop');

      // Links should be accessible (not have tabindex=-1)
      const featurestabindex = await featuresLink.getAttribute('tabindex');
      const howItWorkstabindex = await howItWorksLink.getAttribute('tabindex');

      expect(featurestabindex).not.toBe('-1');
      expect(howItWorkstabindex).not.toBe('-1');

      // Links should have visible text
      await expect(featuresLink).toHaveText('Features');
      await expect(howItWorksLink).toHaveText('How It Works');
    });
  });

  test.describe('Mobile Navigation with Smooth Scroll', () => {
    test('mobile Features link has correct href and closes menu on click', async ({ page }) => {
      // Set mobile viewport and navigate
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Open mobile menu
      const hamburger = page.getByTestId('hamburger-button');
      await hamburger.click();

      // Wait for menu animation
      await page.waitForTimeout(300);

      const featuresLink = page.getByTestId('nav-features-mobile');
      await expect(featuresLink).toBeVisible();
      await expect(featuresLink).toHaveAttribute('href', '#features');

      // Click Features link
      await featuresLink.click();

      // Wait for menu close animation
      await page.waitForTimeout(500);

      // Mobile menu should be closed
      const mobileMenu = page.getByTestId('mobile-menu');
      await expect(mobileMenu).not.toBeVisible();

      // URL should have hash
      await expect(page).toHaveURL(/#features$/);
    });

    test('mobile How It Works link has correct href and closes menu on click', async ({ page }) => {
      // Set mobile viewport and navigate
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Open mobile menu
      const hamburger = page.getByTestId('hamburger-button');
      await hamburger.click();

      // Wait for menu animation
      await page.waitForTimeout(300);

      const howItWorksLink = page.getByTestId('nav-how-it-works-mobile');
      await expect(howItWorksLink).toBeVisible();
      await expect(howItWorksLink).toHaveAttribute('href', '#how-it-works');

      // Click How It Works link
      await howItWorksLink.click();

      // Wait for menu close animation
      await page.waitForTimeout(500);

      // Mobile menu should be closed
      const mobileMenu = page.getByTestId('mobile-menu');
      await expect(mobileMenu).not.toBeVisible();

      // URL should have hash
      await expect(page).toHaveURL(/#how-it-works$/);
    });

    test('mobile anchor links are keyboard accessible', async ({ page }) => {
      // Set mobile viewport and navigate
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Open mobile menu
      const hamburger = page.getByTestId('hamburger-button');
      await hamburger.click();
      await page.waitForTimeout(300);

      const featuresLink = page.getByTestId('nav-features-mobile');
      const howItWorksLink = page.getByTestId('nav-how-it-works-mobile');

      // Verify links are focusable (do not have tabindex=-1)
      const featuresTabindex = await featuresLink.getAttribute('tabindex');
      const howItWorksTabindex = await howItWorksLink.getAttribute('tabindex');

      expect(featuresTabindex).not.toBe('-1');
      expect(howItWorksTabindex).not.toBe('-1');

      // Verify links have visible text
      await expect(featuresLink).toHaveText('Features');
      await expect(howItWorksLink).toHaveText('How It Works');
    });
  });
});
