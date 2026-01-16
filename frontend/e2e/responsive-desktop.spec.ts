import { test, expect } from '@playwright/test';

/**
 * Responsive Design - Desktop View E2E Tests
 *
 * This test file covers the scenario: "Verify homepage displays correctly on desktop
 * devices (> 1024px) as specified in REQ-8"
 *
 * Test Cases:
 * 1. E2E: Render homepage at 1440px viewport width → Full desktop layout with 4-column feature grid
 * 2. E2E: Render homepage at 2560px viewport width → Content remains readable with max-width constraints
 * 3. E2E: Check navigation at desktop size → Full navigation bar displayed without hamburger menu
 */

test.describe('Responsive Design - Desktop View', () => {
  /**
   * Test Case 1: E2E Test
   * Input: Render homepage at 1440px viewport width
   * Expected: Full desktop layout with 4-column feature grid
   */
  test.describe('Test Case 1: 1440px Viewport - Desktop Layout', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 1440, height: 900 });
      await page.goto('/');
    });

    test('homepage renders correctly at 1440px width', async ({ page }) => {
      // Verify the page loads
      await expect(page).toHaveTitle(/URL/i);

      // Verify hero section is visible
      const heroSection = page.getByTestId('hero-section');
      await expect(heroSection).toBeVisible();
    });

    test('features section displays 4-column grid at 1440px', async ({ page }) => {
      // Navigate to features section
      const featuresSection = page.getByTestId('features-section');
      await expect(featuresSection).toBeVisible();

      // Check that the features grid has the 4-column layout class
      const featuresGrid = page.getByTestId('features-grid');
      await expect(featuresGrid).toBeVisible();
      await expect(featuresGrid).toHaveClass(/lg:grid-cols-4/);

      // Verify all 4 feature cards are visible
      const featureCards = page.getByTestId('glassmorphism-card');
      await expect(featureCards).toHaveCount(4);

      // Verify all feature cards are visible
      for (let i = 0; i < 4; i++) {
        await expect(featureCards.nth(i)).toBeVisible();
      }
    });

    test('features are displayed in a horizontal row at 1440px', async ({ page }) => {
      // Get bounding boxes of all feature cards
      const featureCards = page.getByTestId('glassmorphism-card');
      await expect(featureCards).toHaveCount(4);

      const card1Box = await featureCards.nth(0).boundingBox();
      const card2Box = await featureCards.nth(1).boundingBox();
      const card3Box = await featureCards.nth(2).boundingBox();
      const card4Box = await featureCards.nth(3).boundingBox();

      expect(card1Box).not.toBeNull();
      expect(card2Box).not.toBeNull();
      expect(card3Box).not.toBeNull();
      expect(card4Box).not.toBeNull();

      // On desktop (1440px), all 4 cards should be on the same row (similar y positions)
      const yTolerance = 50;
      expect(Math.abs(card1Box!.y - card2Box!.y)).toBeLessThan(yTolerance);
      expect(Math.abs(card2Box!.y - card3Box!.y)).toBeLessThan(yTolerance);
      expect(Math.abs(card3Box!.y - card4Box!.y)).toBeLessThan(yTolerance);

      // Cards should be arranged horizontally (x positions increase)
      expect(card1Box!.x).toBeLessThan(card2Box!.x);
      expect(card2Box!.x).toBeLessThan(card3Box!.x);
      expect(card3Box!.x).toBeLessThan(card4Box!.x);
    });

    test('all homepage sections are visible at 1440px', async ({ page }) => {
      // Hero section
      const heroSection = page.getByTestId('hero-section');
      await expect(heroSection).toBeVisible();

      // Features section
      const featuresSection = page.getByTestId('features-section');
      await expect(featuresSection).toBeVisible();

      // How It Works section
      const howItWorksSection = page.locator('section#how-it-works');
      await expect(howItWorksSection).toBeVisible();

      // CTA buttons in hero
      const ctaGetStarted = page.getByTestId('cta-get-started');
      const ctaLogin = page.getByTestId('cta-login');
      await expect(ctaGetStarted).toBeVisible();
      await expect(ctaLogin).toBeVisible();
    });

    test('hero CTA buttons are displayed horizontally at 1440px', async ({ page }) => {
      const ctaGetStarted = page.getByTestId('cta-get-started');
      const ctaLogin = page.getByTestId('cta-login');

      const getStartedBox = await ctaGetStarted.boundingBox();
      const loginBox = await ctaLogin.boundingBox();

      expect(getStartedBox).not.toBeNull();
      expect(loginBox).not.toBeNull();

      // Buttons should be on the same row (similar y position)
      const yTolerance = 30;
      expect(Math.abs(getStartedBox!.y - loginBox!.y)).toBeLessThan(yTolerance);
    });
  });

  /**
   * Test Case 2: E2E Test
   * Input: Render homepage at 2560px viewport width
   * Expected: Content remains readable with appropriate max-width constraints
   */
  test.describe('Test Case 2: 2560px Viewport - Ultra-wide Support', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 2560, height: 1440 });
      await page.goto('/');
    });

    test('homepage renders correctly at 2560px ultra-wide width', async ({ page }) => {
      // Verify the page loads
      const heroSection = page.getByTestId('hero-section');
      await expect(heroSection).toBeVisible();
    });

    test('content has max-width constraint at 2560px', async ({ page }) => {
      // Features section should have max-width container
      const featuresSection = page.getByTestId('features-section');
      await expect(featuresSection).toBeVisible();

      // The max-w-7xl class constrains content to ~80rem (1280px)
      const maxWidthContainer = featuresSection.locator('.max-w-7xl');
      await expect(maxWidthContainer).toBeVisible();

      const containerBox = await maxWidthContainer.boundingBox();
      expect(containerBox).not.toBeNull();

      // Container width should be limited (max-w-7xl = 80rem = 1280px typically)
      // At 2560px viewport, the container should not stretch to full width
      expect(containerBox!.width).toBeLessThan(2000);
    });

    test('content is centered at 2560px width', async ({ page }) => {
      const featuresSection = page.getByTestId('features-section');
      const maxWidthContainer = featuresSection.locator('.max-w-7xl');

      const containerBox = await maxWidthContainer.boundingBox();
      expect(containerBox).not.toBeNull();

      // Container should be centered (left margin > 0 when viewport is 2560px)
      // mx-auto centers the container
      expect(containerBox!.x).toBeGreaterThan(100);

      // Calculate expected centering
      const viewportWidth = 2560;
      const expectedLeftMargin = (viewportWidth - containerBox!.width) / 2;
      const marginTolerance = 100;
      expect(Math.abs(containerBox!.x - expectedLeftMargin)).toBeLessThan(marginTolerance);
    });

    test('features remain in 4-column grid at 2560px', async ({ page }) => {
      const featuresGrid = page.getByTestId('features-grid');
      await expect(featuresGrid).toBeVisible();
      await expect(featuresGrid).toHaveClass(/lg:grid-cols-4/);

      const featureCards = page.getByTestId('glassmorphism-card');
      await expect(featureCards).toHaveCount(4);

      // Verify horizontal arrangement
      const card1Box = await featureCards.nth(0).boundingBox();
      const card4Box = await featureCards.nth(3).boundingBox();

      expect(card1Box).not.toBeNull();
      expect(card4Box).not.toBeNull();

      // Cards should still be in a row
      const yTolerance = 50;
      expect(Math.abs(card1Box!.y - card4Box!.y)).toBeLessThan(yTolerance);
    });

    test('text remains readable at 2560px', async ({ page }) => {
      // Hero headline and subheadline should be visible and properly constrained
      const headline = page.getByTestId('hero-headline');
      const subheadline = page.getByTestId('hero-subheadline');

      await expect(headline).toBeVisible();
      await expect(subheadline).toBeVisible();

      // Subheadline has max-w-2xl class for readability
      await expect(subheadline).toHaveClass(/max-w-2xl/);

      const subheadlineBox = await subheadline.boundingBox();
      expect(subheadlineBox).not.toBeNull();

      // Text container should be constrained (max-w-2xl = 42rem = ~672px)
      expect(subheadlineBox!.width).toBeLessThan(1000);
    });
  });

  /**
   * Test Case 3: E2E Test
   * Input: Check navigation at desktop size
   * Expected: Full navigation bar displayed without hamburger menu
   */
  test.describe('Test Case 3: Desktop Navigation', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 1440, height: 900 });
      await page.goto('/');
    });

    test('navbar is visible at desktop size', async ({ page }) => {
      const navbar = page.getByTestId('navbar');
      await expect(navbar).toBeVisible();
    });

    test('full navigation is displayed without hamburger menu', async ({ page }) => {
      // Logo should be visible
      const logo = page.getByTestId('navbar-logo');
      await expect(logo).toBeVisible();
      await expect(logo).toHaveText('URL Shortener');

      // Login link should be visible
      const loginLink = page.getByTestId('navbar-login');
      await expect(loginLink).toBeVisible();
      await expect(loginLink).toHaveText('Login');

      // Get Started button should be visible
      const getStartedBtn = page.getByTestId('navbar-get-started');
      await expect(getStartedBtn).toBeVisible();
      await expect(getStartedBtn).toHaveText('Get Started');
    });

    test('navbar links are horizontally aligned at desktop size', async ({ page }) => {
      const logo = page.getByTestId('navbar-logo');
      const loginLink = page.getByTestId('navbar-login');
      const getStartedBtn = page.getByTestId('navbar-get-started');

      const logoBox = await logo.boundingBox();
      const loginBox = await loginLink.boundingBox();
      const getStartedBox = await getStartedBtn.boundingBox();

      expect(logoBox).not.toBeNull();
      expect(loginBox).not.toBeNull();
      expect(getStartedBox).not.toBeNull();

      // All items should be on the same horizontal line (similar y positions)
      const yTolerance = 30;
      expect(Math.abs(logoBox!.y - loginBox!.y)).toBeLessThan(yTolerance);
      expect(Math.abs(loginBox!.y - getStartedBox!.y)).toBeLessThan(yTolerance);

      // Login and Get Started should be to the right of the logo
      expect(loginBox!.x).toBeGreaterThan(logoBox!.x);
      expect(getStartedBox!.x).toBeGreaterThan(logoBox!.x);
    });

    test('no hamburger menu is present at desktop size', async ({ page }) => {
      // Check that there's no hamburger menu button
      // Common selectors for hamburger menus
      const hamburgerMenu = page.locator('[data-testid="hamburger-menu"], .hamburger, [aria-label="Menu"], button:has(svg[class*="menu"])');

      // The hamburger menu should not be visible at desktop size
      const count = await hamburgerMenu.count();
      if (count > 0) {
        // If hamburger elements exist, they should be hidden
        for (let i = 0; i < count; i++) {
          await expect(hamburgerMenu.nth(i)).not.toBeVisible();
        }
      }
    });

    test('navbar navigation links are functional', async ({ page }) => {
      // Test Login link navigation
      const loginLink = page.getByTestId('navbar-login');
      await expect(loginLink).toHaveAttribute('href', '/login');

      // Test Get Started button navigation
      const getStartedBtn = page.getByTestId('navbar-get-started');
      await expect(getStartedBtn).toHaveAttribute('href', '/register');
    });

    test('navbar is fixed at top of viewport', async ({ page }) => {
      const navbar = page.getByTestId('navbar');
      await expect(navbar).toHaveClass(/fixed/);
      await expect(navbar).toHaveClass(/top-0/);

      // Wait for animation to complete
      await page.waitForTimeout(500);

      const navbarBox = await navbar.boundingBox();
      expect(navbarBox).not.toBeNull();
      // Allow small tolerance for animation/rendering
      expect(navbarBox!.y).toBeLessThanOrEqual(5);
      expect(navbarBox!.y).toBeGreaterThanOrEqual(-5);
    });

    test('navigation at 2560px also shows full navbar', async ({ page }) => {
      await page.setViewportSize({ width: 2560, height: 1440 });
      await page.goto('/');

      const navbar = page.getByTestId('navbar');
      await expect(navbar).toBeVisible();

      const logo = page.getByTestId('navbar-logo');
      const loginLink = page.getByTestId('navbar-login');
      const getStartedBtn = page.getByTestId('navbar-get-started');

      await expect(logo).toBeVisible();
      await expect(loginLink).toBeVisible();
      await expect(getStartedBtn).toBeVisible();
    });
  });

  /**
   * Additional desktop viewport tests for comprehensive coverage
   */
  test.describe('Additional Desktop Viewport Tests', () => {
    test('homepage at 1024px (minimum desktop) shows desktop layout', async ({ page }) => {
      await page.setViewportSize({ width: 1024, height: 768 });
      await page.goto('/');

      // Features should show 4-column grid at lg breakpoint (1024px)
      const featuresGrid = page.getByTestId('features-grid');
      await expect(featuresGrid).toBeVisible();
      await expect(featuresGrid).toHaveClass(/lg:grid-cols-4/);

      // Navbar should be visible
      const navbar = page.getByTestId('navbar');
      await expect(navbar).toBeVisible();

      const loginLink = page.getByTestId('navbar-login');
      const getStartedBtn = page.getByTestId('navbar-get-started');
      await expect(loginLink).toBeVisible();
      await expect(getStartedBtn).toBeVisible();
    });

    test('How It Works section displays correctly at desktop size', async ({ page }) => {
      await page.setViewportSize({ width: 1440, height: 900 });
      await page.goto('/');

      const howItWorksSection = page.locator('section#how-it-works');
      await expect(howItWorksSection).toBeVisible();

      // All 3 steps should be visible
      const step1 = page.getByTestId('step-1');
      const step2 = page.getByTestId('step-2');
      const step3 = page.getByTestId('step-3');

      await expect(step1).toBeVisible();
      await expect(step2).toBeVisible();
      await expect(step3).toBeVisible();

      // Steps should be in a horizontal row at desktop
      const step1Box = await step1.boundingBox();
      const step3Box = await step3.boundingBox();

      expect(step1Box).not.toBeNull();
      expect(step3Box).not.toBeNull();

      // Steps should have similar y positions (horizontal layout)
      const yTolerance = 50;
      expect(Math.abs(step1Box!.y - step3Box!.y)).toBeLessThan(yTolerance);
    });

    test('theme toggle is accessible at desktop size', async ({ page }) => {
      await page.setViewportSize({ width: 1440, height: 900 });
      await page.goto('/');

      // Theme toggle in hero section has data-testid="hero-theme-toggle"
      const themeToggle = page.getByTestId('hero-theme-toggle');
      await expect(themeToggle).toBeVisible();
    });
  });
});
