/**
 * E2E tests for Navigation and Smooth Scroll.
 * Owner: Scenario 14 - Navigation and Smooth Scroll
 *
 * Tests:
 * - Clicking navigation links scrolls to sections
 * - Smooth scroll animation (not instant jump)
 * - URL hash updates after navigation
 * - Direct URL with hash navigates to section
 */

import { test, expect } from '@playwright/test';

test.describe('Navigation and Smooth Scroll', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for navigation to be interactive
    await page.waitForSelector('.nav');
  });

  // Test Case 1: Click 'Features' navigation link
  test('should smoothly scroll to features section when clicking Features link', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the Features navigation link
    await page.click('a[href="#features"]');

    // Wait for scroll to complete (smooth scroll takes time)
    await page.waitForTimeout(500);

    // Verify page scrolled down
    const newScrollY = await page.evaluate(() => window.scrollY);
    expect(newScrollY).toBeGreaterThan(initialScrollY);

    // Verify features section is visible in viewport
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  // Test Case 2: Click 'Quick Start' navigation link
  test('should smoothly scroll to quick start section when clicking Quick Start link', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the Quick Start navigation link
    await page.click('a[href="#quick-start"]');

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Verify page scrolled
    const newScrollY = await page.evaluate(() => window.scrollY);
    expect(newScrollY).toBeGreaterThan(initialScrollY);

    // Verify quick start section is visible
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeInViewport();
  });

  // Test Case 3: Navigate to URL with hash
  test('should scroll to specified section when navigating to URL with hash', async ({ page }) => {
    // Navigate directly to URL with hash
    await page.goto('/#features');

    // Wait for initial scroll
    await page.waitForTimeout(600);

    // Verify features section is visible
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();

    // Verify scroll position is not at top
    const scrollY = await page.evaluate(() => window.scrollY);
    expect(scrollY).toBeGreaterThan(0);
  });

  // Test Case 4: Verify scroll behavior CSS property
  test('should have scroll-behavior: smooth CSS on html element', async ({ page }) => {
    // Get the computed scroll-behavior style
    const scrollBehavior = await page.evaluate(() => {
      const htmlElement = document.documentElement;
      return window.getComputedStyle(htmlElement).scrollBehavior;
    });

    expect(scrollBehavior).toBe('smooth');
  });

  // Additional test: URL hash updates after clicking navigation
  test('should update URL hash after clicking navigation link', async ({ page }) => {
    // Click the Features navigation link
    await page.click('a[href="#features"]');

    // Wait for navigation to complete
    await page.waitForTimeout(500);

    // Verify URL hash updated
    const currentURL = page.url();
    expect(currentURL).toContain('#features');
  });

  // Test: Navigation to Architecture section
  test('should scroll to architecture section when clicking Architecture link', async ({ page }) => {
    // Click the Architecture navigation link
    await page.click('a[href="#architecture"]');

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Verify architecture section is visible
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeInViewport();

    // Verify URL updated
    expect(page.url()).toContain('#architecture');
  });

  // Test: Navigation to Commands section
  test('should scroll to commands section when clicking Commands link', async ({ page }) => {
    // Click the Commands navigation link
    await page.click('a[href="#commands"]');

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Verify commands section is visible
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeInViewport();

    // Verify URL updated
    expect(page.url()).toContain('#commands');
  });

  // Test: Smooth scroll is animated (not instant)
  test('should animate scroll smoothly instead of instant jump', async ({ page }) => {
    // Collect scroll positions over time to verify animation
    const scrollPositions: number[] = [];

    // Set up scroll position tracking
    await page.evaluate(() => {
      (window as any).__scrollPositions = [];
      const trackScroll = () => {
        (window as any).__scrollPositions.push(window.scrollY);
      };
      window.addEventListener('scroll', trackScroll);
      (window as any).__stopTracking = () => {
        window.removeEventListener('scroll', trackScroll);
      };
    });

    // Click navigation link to trigger smooth scroll
    await page.click('a[href="#commands"]');

    // Wait for animation to complete
    await page.waitForTimeout(800);

    // Stop tracking and get positions
    const positions = await page.evaluate(() => {
      (window as any).__stopTracking();
      return (window as any).__scrollPositions;
    });

    // For smooth scroll, we expect multiple intermediate positions
    // An instant jump would only have the start and end position
    // Smooth scroll should have many positions as browser animates
    expect(positions.length).toBeGreaterThan(2);

    // Verify positions are increasing (scrolling down)
    const uniquePositions = [...new Set(positions)];
    expect(uniquePositions.length).toBeGreaterThan(1);
  });

  // Test: Navigation links have correct href attributes
  test('should have navigation links with correct section hrefs', async ({ page }) => {
    // Use .nav-link class to specifically target navigation links
    const featuresLink = page.locator('.nav-link[href="#features"]');
    const quickStartLink = page.locator('.nav-link[href="#quick-start"]');
    const architectureLink = page.locator('.nav-link[href="#architecture"]');
    const commandsLink = page.locator('.nav-link[href="#commands"]');

    await expect(featuresLink).toBeVisible();
    await expect(quickStartLink).toBeVisible();
    await expect(architectureLink).toBeVisible();
    await expect(commandsLink).toBeVisible();
  });

  // Test: Sections have correct IDs for navigation targets
  test('should have sections with correct IDs for navigation', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const quickStartSection = page.locator('#quick-start');
    const architectureSection = page.locator('#architecture');
    const commandsSection = page.locator('#commands');

    await expect(featuresSection).toBeAttached();
    await expect(quickStartSection).toBeAttached();
    await expect(architectureSection).toBeAttached();
    await expect(commandsSection).toBeAttached();
  });

  // Test: Navigation sticky behavior
  test('should keep navigation visible when scrolling', async ({ page }) => {
    // Scroll down the page
    await page.evaluate(() => window.scrollTo(0, 500));
    await page.waitForTimeout(200);

    // Verify navigation is still visible
    const nav = page.locator('.nav');
    await expect(nav).toBeVisible();
    await expect(nav).toBeInViewport();
  });

  // Test: Active state on navigation links (when section is in view)
  test('should apply active class to navigation link when section is in view', async ({ page }) => {
    // Click to scroll to features
    await page.click('a[href="#features"]');
    await page.waitForTimeout(600);

    // Check if the features link has active class
    const featuresLink = page.locator('a[href="#features"]');
    await expect(featuresLink).toHaveClass(/active/);
  });

  // Test: Keyboard navigation works with nav links
  test('should allow keyboard navigation to nav links', async ({ page }) => {
    // Tab to first nav link (skip brand link)
    await page.keyboard.press('Tab'); // Skip link
    await page.keyboard.press('Tab'); // Brand
    await page.keyboard.press('Tab'); // First nav link

    // Verify focus is on a nav link
    const focusedElement = await page.locator(':focus').first();
    const href = await focusedElement.getAttribute('href');
    expect(href).toMatch(/^#/);

    // Press Enter to activate
    await page.keyboard.press('Enter');
    await page.waitForTimeout(500);

    // Verify URL updated
    expect(page.url()).toContain('#');
  });

  // Test: Direct navigation to quick-start via URL
  test('should scroll to quick-start section when navigating to URL with #quick-start', async ({ page }) => {
    await page.goto('/#quick-start');
    await page.waitForTimeout(600);

    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeInViewport();
  });
});
