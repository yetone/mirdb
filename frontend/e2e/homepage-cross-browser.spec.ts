import { test, expect } from '@playwright/test';

/**
 * Cross-Browser Compatibility Tests for Homepage
 *
 * These tests verify that the homepage renders correctly across different browsers
 * (Chrome, Firefox, Safari, Edge) with proper styling and functionality.
 */

test.describe('Homepage Cross-Browser Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('homepage renders with all main sections', async ({ page }) => {
    // Verify the main homepage container exists
    const homepage = page.locator('[data-testid="homepage"]');
    await expect(homepage).toBeVisible();

    // Verify Hero Section renders
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify Features Section renders
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Verify How It Works Section renders
    const howItWorksSection = page.locator('[data-testid="how-it-works-section"]');
    await expect(howItWorksSection).toBeVisible();

    // Verify Footer renders
    const footer = page.locator('[data-testid="footer"]');
    await expect(footer).toBeVisible();
  });

  test('Hero Section displays correct content', async ({ page }) => {
    const heroSection = page.locator('[data-testid="hero-section"]');

    // Check for headline text
    const headline = heroSection.locator('h1');
    await expect(headline).toBeVisible();
    await expect(headline).toContainText(/shorten|links|amplify/i);

    // Check for Get Started CTA button
    const ctaButton = heroSection.getByRole('link', { name: /get started/i });
    await expect(ctaButton).toBeVisible();

    // Check for Sign In link
    const signInLink = heroSection.getByRole('link', { name: /sign in/i });
    await expect(signInLink).toBeVisible();
  });

  test('Features Section displays feature cards', async ({ page }) => {
    const featuresSection = page.locator('[data-testid="features-section"]');

    // Check for section heading
    const heading = featuresSection.locator('h2');
    await expect(heading).toBeVisible();

    // Check that feature cards exist
    const featureCards = page.locator('[data-testid="feature-card"]');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(3);

    // Verify first feature card has content
    const firstCard = featureCards.first();
    await expect(firstCard).toBeVisible();
  });

  test('Navigation links are functional', async ({ page }) => {
    // Test Get Started button navigates to register
    const getStartedLink = page.getByRole('link', { name: /get started/i }).first();
    await expect(getStartedLink).toHaveAttribute('href', /register/i);

    // Test Sign In link navigates to login
    const signInLink = page.getByRole('link', { name: /sign in/i }).first();
    await expect(signInLink).toHaveAttribute('href', /login/i);
  });

  test('Footer section renders correctly', async ({ page }) => {
    const footer = page.locator('[data-testid="footer"]');

    // Footer should be visible
    await expect(footer).toBeVisible();

    // Footer should contain navigation links
    const footerLinks = footer.getByRole('link');
    const linkCount = await footerLinks.count();
    expect(linkCount).toBeGreaterThanOrEqual(1);
  });

  test('Page has proper semantic structure', async ({ page }) => {
    // Check for main element
    const main = page.locator('main');
    await expect(main).toBeVisible();

    // Check heading hierarchy (h1 should exist)
    const h1 = page.locator('h1').first();
    await expect(h1).toBeVisible();

    // Check h2 elements exist for sections
    const h2Elements = page.locator('h2');
    const h2Count = await h2Elements.count();
    expect(h2Count).toBeGreaterThanOrEqual(2);
  });

  test('CSS styling is applied correctly', async ({ page }) => {
    // Verify Tailwind CSS classes are working by checking computed styles
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify the page has content with proper layout
    const headline = page.locator('h1').first();
    const box = await headline.boundingBox();

    // Headline should have reasonable size (not collapsed)
    expect(box).not.toBeNull();
    if (box) {
      expect(box.width).toBeGreaterThan(100);
      expect(box.height).toBeGreaterThan(20);
    }
  });

  test('Interactive elements are clickable', async ({ page }) => {
    // Verify buttons/links are clickable (have proper pointer events)
    const ctaButton = page.getByRole('link', { name: /get started/i }).first();

    // Element should be enabled and clickable
    await expect(ctaButton).toBeEnabled();

    // Verify cursor style indicates clickability
    const cursor = await ctaButton.evaluate((el) => {
      return window.getComputedStyle(el).cursor;
    });
    expect(cursor).toBe('pointer');
  });

  test('Page loads without console errors', async ({ page }) => {
    const errors: string[] = [];

    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        errors.push(msg.text());
      }
    });

    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Filter out known benign errors (like favicon or development warnings)
    const criticalErrors = errors.filter(
      (error) => !error.includes('favicon') && !error.includes('DevTools')
    );

    expect(criticalErrors.length).toBe(0);
  });

  test('Responsive layout renders correctly on desktop', async ({ page }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.reload();

    // Verify features grid has multi-column layout
    const featuresGrid = page.locator('[data-testid="features-grid"]');
    await expect(featuresGrid).toBeVisible();

    // Check grid has proper width for desktop
    const gridBox = await featuresGrid.boundingBox();
    expect(gridBox).not.toBeNull();
    if (gridBox) {
      expect(gridBox.width).toBeGreaterThan(800);
    }
  });

  test('Responsive layout adapts for mobile', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await page.reload();

    // Verify page content is still visible
    const homepage = page.locator('[data-testid="homepage"]');
    await expect(homepage).toBeVisible();

    // Hero section should still be visible
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();
  });
});
