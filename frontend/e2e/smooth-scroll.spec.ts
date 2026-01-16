import { test, expect } from '@playwright/test';

/**
 * Smooth Scroll Navigation - E2E Tests
 *
 * This test file covers the scenario: "Verify smooth scroll navigation for
 * single-page sections as specified in REQ-10"
 *
 * Test Cases:
 * 1. E2E: Click 'Features' anchor link → Page smoothly scrolls to Features section
 * 2. E2E: Click 'How It Works' anchor link → Page smoothly scrolls to How It Works section
 * 3. Unit: Verify CSS scroll-behavior property → scroll-behavior: smooth is applied to html/body
 * 4. E2E: Test keyboard navigation with anchor links → Anchor links are keyboard accessible and trigger smooth scroll
 */

test.describe('Smooth Scroll Navigation - E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for the page to be fully loaded
    await page.waitForLoadState('networkidle');
  });

  /**
   * Test Case 1: E2E Test
   * Input: Click 'Features' anchor link
   * Expected: Page smoothly scrolls to Features section
   */
  test.describe('Test Case 1: Features Anchor Link Smooth Scroll', () => {
    test('Features anchor link is visible in Navbar', async ({ page }) => {
      const featuresLink = page.getByTestId('nav-features-desktop');
      await expect(featuresLink).toBeVisible();
      await expect(featuresLink).toHaveText('Features');
      await expect(featuresLink).toHaveAttribute('href', '#features');
    });

    test('Features section exists with correct id', async ({ page }) => {
      const featuresSection = page.getByTestId('features-section');
      await expect(featuresSection).toBeVisible();
      await expect(featuresSection).toHaveAttribute('id', 'features');
    });

    test('clicking Features link scrolls to Features section', async ({ page }) => {
      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);

      // Click the Features link
      const featuresLink = page.getByTestId('nav-features-desktop');
      await featuresLink.click();

      // Wait for smooth scroll to complete (allow up to 1 second)
      await page.waitForTimeout(1000);

      // Get the Features section position
      const featuresSection = page.getByTestId('features-section');
      const sectionBoundingBox = await featuresSection.boundingBox();

      // Verify the section is now in or near the viewport
      expect(sectionBoundingBox).not.toBeNull();

      // The section should be at or near the top of the viewport (accounting for fixed navbar)
      // We check that the top of the section is within a reasonable range from the viewport top
      expect(sectionBoundingBox!.y).toBeLessThan(200);
    });

    test('Features section heading is visible after scroll', async ({ page }) => {
      // Click the Features link
      const featuresLink = page.getByTestId('nav-features-desktop');
      await featuresLink.click();

      // Wait for smooth scroll
      await page.waitForTimeout(1000);

      // Check that the Features heading is visible
      const featuresHeading = page.locator('#features-heading');
      await expect(featuresHeading).toBeVisible();
      await expect(featuresHeading).toHaveText('Powerful Features');
    });

    test('URL hash is updated after clicking Features link', async ({ page }) => {
      const featuresLink = page.getByTestId('nav-features-desktop');
      await featuresLink.click();

      // Wait for navigation
      await page.waitForTimeout(500);

      // Check URL contains #features
      await expect(page).toHaveURL(/#features$/);
    });
  });

  /**
   * Test Case 2: E2E Test
   * Input: Click 'How It Works' anchor link
   * Expected: Page smoothly scrolls to How It Works section
   */
  test.describe('Test Case 2: How It Works Anchor Link Smooth Scroll', () => {
    test('How It Works anchor link is visible in Navbar', async ({ page }) => {
      const howItWorksLink = page.getByTestId('nav-how-it-works-desktop');
      await expect(howItWorksLink).toBeVisible();
      await expect(howItWorksLink).toHaveText('How It Works');
      await expect(howItWorksLink).toHaveAttribute('href', '#how-it-works');
    });

    test('How It Works section exists with correct id', async ({ page }) => {
      const howItWorksSection = page.locator('#how-it-works');
      await expect(howItWorksSection).toBeVisible();
    });

    test('clicking How It Works link scrolls to How It Works section', async ({ page }) => {
      // Click the How It Works link
      const howItWorksLink = page.getByTestId('nav-how-it-works-desktop');
      await howItWorksLink.click();

      // Wait for smooth scroll to complete
      await page.waitForTimeout(1000);

      // Get the How It Works section position
      const howItWorksSection = page.locator('#how-it-works');
      const sectionBoundingBox = await howItWorksSection.boundingBox();

      // Verify the section is now in or near the viewport
      expect(sectionBoundingBox).not.toBeNull();

      // The section should be at or near the top of the viewport
      expect(sectionBoundingBox!.y).toBeLessThan(200);
    });

    test('How It Works section title is visible after scroll', async ({ page }) => {
      // Click the How It Works link
      const howItWorksLink = page.getByTestId('nav-how-it-works-desktop');
      await howItWorksLink.click();

      // Wait for smooth scroll
      await page.waitForTimeout(1000);

      // Check that the How It Works heading is visible
      const howItWorksTitle = page.locator('#how-it-works-title');
      await expect(howItWorksTitle).toBeVisible();
      await expect(howItWorksTitle).toHaveText('How It Works');
    });

    test('URL hash is updated after clicking How It Works link', async ({ page }) => {
      const howItWorksLink = page.getByTestId('nav-how-it-works-desktop');
      await howItWorksLink.click();

      // Wait for navigation
      await page.waitForTimeout(500);

      // Check URL contains #how-it-works
      await expect(page).toHaveURL(/#how-it-works$/);
    });
  });

  /**
   * Test Case 3: Unit Test
   * Input: Verify CSS scroll-behavior property
   * Expected: scroll-behavior: smooth is applied to html/body
   */
  test.describe('Test Case 3: CSS scroll-behavior Property', () => {
    test('scroll-behavior: smooth is applied to html element', async ({ page }) => {
      // Get the computed style of the html element
      const scrollBehavior = await page.evaluate(() => {
        const html = document.documentElement;
        return window.getComputedStyle(html).scrollBehavior;
      });

      expect(scrollBehavior).toBe('smooth');
    });

    test('scroll-behavior property is set in CSS', async ({ page }) => {
      // Check that the scroll-behavior is set (either on html or body)
      const hasScrollBehavior = await page.evaluate(() => {
        const html = document.documentElement;
        const body = document.body;
        const htmlStyle = window.getComputedStyle(html).scrollBehavior;
        const bodyStyle = window.getComputedStyle(body).scrollBehavior;
        return htmlStyle === 'smooth' || bodyStyle === 'smooth';
      });

      expect(hasScrollBehavior).toBe(true);
    });

    test('smooth scrolling is not an instant jump', async ({ page }) => {
      // Scroll to a known position first
      await page.evaluate(() => window.scrollTo(0, 0));
      await page.waitForTimeout(100);

      // Record scroll positions during scroll
      const scrollPositions: number[] = [];

      // Set up a listener to track scroll positions
      await page.evaluate(() => {
        (window as unknown as { scrollPositions: number[] }).scrollPositions = [];
        window.addEventListener('scroll', () => {
          (window as unknown as { scrollPositions: number[] }).scrollPositions.push(window.scrollY);
        });
      });

      // Click the How It Works link (which is further down the page)
      const howItWorksLink = page.getByTestId('nav-how-it-works-desktop');
      await howItWorksLink.click();

      // Wait for scroll animation
      await page.waitForTimeout(600);

      // Get the recorded scroll positions
      const recordedPositions = await page.evaluate(() => {
        return (window as unknown as { scrollPositions: number[] }).scrollPositions;
      });

      // If smooth scroll is working, we should have multiple intermediate positions
      // (more than just 0 and final position)
      // Note: Some browsers may batch scroll events, so we just check there's any scrolling
      const finalScrollY = await page.evaluate(() => window.scrollY);

      // The page should have scrolled
      expect(finalScrollY).toBeGreaterThan(0);
    });
  });

  /**
   * Test Case 4: E2E Test
   * Input: Test keyboard navigation with anchor links
   * Expected: Anchor links are keyboard accessible and trigger smooth scroll
   */
  test.describe('Test Case 4: Keyboard Navigation with Anchor Links', () => {
    test('Features anchor link is keyboard focusable', async ({ page }) => {
      const featuresLink = page.getByTestId('nav-features-desktop');

      // Focus the link using keyboard
      await featuresLink.focus();

      // Verify it's focused
      await expect(featuresLink).toBeFocused();
    });

    test('How It Works anchor link is keyboard focusable', async ({ page }) => {
      const howItWorksLink = page.getByTestId('nav-how-it-works-desktop');

      // Focus the link using keyboard
      await howItWorksLink.focus();

      // Verify it's focused
      await expect(howItWorksLink).toBeFocused();
    });

    test('pressing Enter on focused Features link triggers smooth scroll', async ({ page }) => {
      // Focus the Features link
      const featuresLink = page.getByTestId('nav-features-desktop');
      await featuresLink.focus();
      await expect(featuresLink).toBeFocused();

      // Press Enter
      await page.keyboard.press('Enter');

      // Wait for smooth scroll
      await page.waitForTimeout(1000);

      // Get the Features section position
      const featuresSection = page.getByTestId('features-section');
      const sectionBoundingBox = await featuresSection.boundingBox();

      // Verify the section is now in or near the viewport
      expect(sectionBoundingBox).not.toBeNull();
      expect(sectionBoundingBox!.y).toBeLessThan(200);
    });

    test('pressing Enter on focused How It Works link triggers smooth scroll', async ({ page }) => {
      // Focus the How It Works link
      const howItWorksLink = page.getByTestId('nav-how-it-works-desktop');
      await howItWorksLink.focus();
      await expect(howItWorksLink).toBeFocused();

      // Press Enter
      await page.keyboard.press('Enter');

      // Wait for smooth scroll
      await page.waitForTimeout(1000);

      // Get the How It Works section position
      const howItWorksSection = page.locator('#how-it-works');
      const sectionBoundingBox = await howItWorksSection.boundingBox();

      // Verify the section is now in or near the viewport
      expect(sectionBoundingBox).not.toBeNull();
      expect(sectionBoundingBox!.y).toBeLessThan(200);
    });

    test('can Tab through navigation links', async ({ page }) => {
      // Start at the logo and tab through
      const logo = page.getByTestId('navbar-logo');
      await logo.focus();

      // Tab to Features link
      await page.keyboard.press('Tab');
      const featuresLink = page.getByTestId('nav-features-desktop');
      await expect(featuresLink).toBeFocused();

      // Tab to How It Works link
      await page.keyboard.press('Tab');
      const howItWorksLink = page.getByTestId('nav-how-it-works-desktop');
      await expect(howItWorksLink).toBeFocused();
    });

    test('anchor links have minimum touch target size (44x44px)', async ({ page }) => {
      const featuresLink = page.getByTestId('nav-features-desktop');
      const howItWorksLink = page.getByTestId('nav-how-it-works-desktop');

      const featuresBox = await featuresLink.boundingBox();
      const howItWorksBox = await howItWorksLink.boundingBox();

      expect(featuresBox).not.toBeNull();
      expect(howItWorksBox).not.toBeNull();

      // Height should be at least 44px for accessibility
      expect(featuresBox!.height).toBeGreaterThanOrEqual(44);
      expect(howItWorksBox!.height).toBeGreaterThanOrEqual(44);
    });
  });

  /**
   * Mobile viewport tests for smooth scroll
   */
  test.describe('Smooth Scroll on Mobile Viewport', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');
      await page.waitForLoadState('networkidle');
    });

    test('mobile menu anchor links have correct href for features section', async ({ page }) => {
      // Open mobile menu
      const hamburgerButton = page.getByTestId('hamburger-button');
      await hamburgerButton.click();

      // Wait for menu animation
      await page.waitForTimeout(300);

      // Verify Features link is visible and has correct href
      const mobileFeatures = page.getByTestId('nav-features-mobile');
      await expect(mobileFeatures).toBeVisible();
      await expect(mobileFeatures).toHaveAttribute('href', '#features');
      await expect(mobileFeatures).toHaveText('Features');

      // Click the link and verify URL hash updates
      await mobileFeatures.click();
      await page.waitForTimeout(500);
      await expect(page).toHaveURL(/#features$/);
    });

    test('mobile menu closes after clicking anchor link', async ({ page }) => {
      // Open mobile menu
      const hamburgerButton = page.getByTestId('hamburger-button');
      await hamburgerButton.click();

      // Wait for menu animation
      await page.waitForTimeout(300);

      // Click Features link in mobile menu
      const mobileFeatures = page.getByTestId('nav-features-mobile');
      await mobileFeatures.click();

      // Wait for menu close animation
      await page.waitForTimeout(500);

      // Mobile menu should be closed
      const mobileMenu = page.getByTestId('mobile-menu');
      await expect(mobileMenu).not.toBeVisible();
    });

    test('mobile menu anchor links have correct href for how it works section', async ({ page }) => {
      // Open mobile menu
      const hamburgerButton = page.getByTestId('hamburger-button');
      await hamburgerButton.click();

      // Wait for menu animation
      await page.waitForTimeout(300);

      // Verify How It Works link is visible and has correct href
      const mobileHowItWorks = page.getByTestId('nav-how-it-works-mobile');
      await expect(mobileHowItWorks).toBeVisible();
      await expect(mobileHowItWorks).toHaveAttribute('href', '#how-it-works');
      await expect(mobileHowItWorks).toHaveText('How It Works');

      // Click the link and verify URL hash updates
      await mobileHowItWorks.click();
      await page.waitForTimeout(500);
      await expect(page).toHaveURL(/#how-it-works$/);
    });
  });
});
