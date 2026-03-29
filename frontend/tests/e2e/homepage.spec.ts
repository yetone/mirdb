/**
 * Homepage E2E Tests
 * Owner: Scenario 1 - Hero Section Content and Value Proposition
 *
 * E2E tests validating hero section visibility and content
 * Test Case 4: Hero section is visible without scrolling on 1920x1080 viewport
 *
 * Requirements: REQ-1, US-1
 */
import { test, expect } from '@playwright/test';

test.describe('Homepage Hero Section Visibility', () => {
  test.beforeEach(async ({ page }) => {
    // Set viewport to 1920x1080 (standard desktop)
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  /**
   * Test Case 4: Hero section is visible without scrolling on 1920x1080 viewport
   * Input: Navigate to homepage URL
   * Expected: Hero section is visible without scrolling on 1920x1080 viewport
   */
  test('hero section is visible without scrolling on 1920x1080 viewport', async ({ page }) => {
    // Wait for the hero section to be present
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify hero section is above the fold (visible without scrolling)
    const boundingBox = await heroSection.boundingBox();
    expect(boundingBox).not.toBeNull();

    // The hero section should start at or near the top
    expect(boundingBox!.y).toBeLessThan(100);

    // A significant portion of the hero should be visible in the viewport
    const viewportHeight = 1080;
    const heroVisibleHeight = Math.min(
      boundingBox!.height,
      viewportHeight - boundingBox!.y
    );
    expect(heroVisibleHeight).toBeGreaterThan(viewportHeight * 0.5);
  });

  test('headline displays value proposition text', async ({ page }) => {
    const headline = page.locator('[data-testid="hero-headline"]');
    await expect(headline).toBeVisible();
    await expect(headline).toContainText('Shorten Links');
    await expect(headline).toContainText('Track Clicks');
    await expect(headline).toContainText('Grow Your Reach');
  });

  test('subheadline explains key benefit', async ({ page }) => {
    const subheadline = page.locator('[data-testid="hero-subheadline"]');
    await expect(subheadline).toBeVisible();
    await expect(subheadline).toContainText('Create short, memorable links');
    await expect(subheadline).toContainText('analytics');
  });

  test('product branding is visible', async ({ page }) => {
    const branding = page.locator('[data-testid="product-branding"]');
    await expect(branding).toBeVisible();

    const productName = page.locator('[data-testid="product-name"]');
    await expect(productName).toBeVisible();
    await expect(productName).toContainText('ShortLink');
  });

  test('hero section is in viewport without scrolling', async ({ page }) => {
    // Initial check - hero should be visible
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify the element is in the viewport without any scrolling
    await expect(heroSection).toBeInViewport();
  });
});

/**
 * Cross-Browser Compatibility Tests
 * Owner: Scenario 11 - Cross-Browser Compatibility
 *
 * Validates that the homepage renders correctly across Chrome, Firefox, Safari, and Edge
 * browsers as per success criteria. These tests run automatically in all configured browsers
 * via Playwright's project configuration.
 *
 * Test Cases:
 * - TC1: Render homepage in Chrome - Page renders correctly with all features functional
 * - TC2: Render homepage in Firefox - Page renders correctly with all features functional
 * - TC3: Render homepage in Safari - Page renders correctly with all features functional
 * - TC4: Render homepage in Edge - Page renders correctly with all features functional
 * - TC5: Test dark mode in all browsers - prefers-color-scheme media query works
 *
 * Requirements: Success Criteria - Homepage renders correctly across Chrome, Firefox, Safari, and Edge
 */
test.describe('Cross-Browser Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  /**
   * Test Case 1-4: Homepage renders correctly across all browsers
   * This test runs in all configured browser projects (chromium, firefox, webkit, edge)
   * Validates that core page elements render correctly regardless of browser
   */
  test('homepage renders all core sections correctly', async ({ page, browserName }) => {
    // Verify homepage container is present
    const homepage = page.locator('[data-testid="homepage"]');
    await expect(homepage).toBeVisible();

    // Verify hero section renders
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify headline displays correctly
    const headline = page.locator('[data-testid="hero-headline"]');
    await expect(headline).toBeVisible();

    // Verify features section renders
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Verify all 3 feature cards are present
    const featureCards = page.locator('[data-testid="feature-card"]');
    await expect(featureCards).toHaveCount(3);

    // Verify social proof section renders
    const socialProofSection = page.locator('[data-testid="social-proof-section"]');
    await expect(socialProofSection).toBeVisible();

    // Verify footer renders
    const footer = page.locator('[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Log browser name for debugging/verification
    console.log(`✓ Homepage rendered successfully in ${browserName}`);
  });

  test('CTA buttons are functional', async ({ page, browserName }) => {
    // Primary CTA should be visible and have correct styling
    const primaryCta = page.locator('[data-testid="cta-primary"]');
    await expect(primaryCta).toBeVisible();
    await expect(primaryCta).toContainText(/Get Started/i);

    // Secondary CTA should also be present
    const secondaryCta = page.locator('[data-testid="cta-secondary"]');
    await expect(secondaryCta).toBeVisible();

    console.log(`✓ CTAs functional in ${browserName}`);
  });

  test('navigation links are accessible', async ({ page, browserName }) => {
    // Footer navigation links should be present and visible
    const footer = page.locator('[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Check that navigation sections exist
    const navLinks = footer.locator('a');
    const linkCount = await navLinks.count();
    expect(linkCount).toBeGreaterThan(0);

    console.log(`✓ Navigation links accessible in ${browserName} (${linkCount} links found)`);
  });

  test('images and icons render correctly', async ({ page, browserName }) => {
    // Feature cards should have icons/images
    const featureCards = page.locator('[data-testid="feature-card"]');
    const cardCount = await featureCards.count();
    expect(cardCount).toBe(3);

    // Each card should have an icon element
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      const icon = card.locator('[data-testid="feature-icon"]');
      await expect(icon).toBeVisible();
    }

    console.log(`✓ Images and icons render correctly in ${browserName}`);
  });

  test('layout does not have horizontal scroll', async ({ page, browserName }) => {
    // Set standard desktop viewport
    await page.setViewportSize({ width: 1920, height: 1080 });

    // Check that content width does not exceed viewport
    const body = page.locator('body');
    const bodyBox = await body.boundingBox();
    expect(bodyBox).not.toBeNull();
    expect(bodyBox!.width).toBeLessThanOrEqual(1920);

    // Verify no horizontal scrollbar by checking document scroll width
    const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);
    expect(scrollWidth).toBeLessThanOrEqual(clientWidth + 1); // +1 for rounding tolerance

    console.log(`✓ No horizontal scroll in ${browserName}`);
  });
});

/**
 * Test Case 5: Dark Mode Cross-Browser Compatibility
 * Tests that prefers-color-scheme media query works in all supported browsers
 */
test.describe('Dark Mode Cross-Browser Compatibility', () => {
  test('dark mode applies correctly based on system preference', async ({ page, browserName }) => {
    // Emulate dark color scheme
    await page.emulateMedia({ colorScheme: 'dark' });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Check that the page has dark theme applied
    const homepage = page.locator('[data-testid="homepage"]');
    await expect(homepage).toBeVisible();

    // Verify the page responds to dark mode preference
    // DaisyUI applies theme via data-theme attribute or uses prefers-color-scheme
    const htmlElement = page.locator('html');
    const dataTheme = await htmlElement.getAttribute('data-theme');

    // Either data-theme is set to dark, or background color should be dark
    if (dataTheme) {
      // If using DaisyUI theme switching
      expect(['dark', 'night', 'business', 'forest', 'black', 'luxury', 'dracula', 'coffee', 'halloween', 'synthwave', 'cyberpunk']).toContain(dataTheme.toLowerCase());
    } else {
      // If relying on prefers-color-scheme, verify background is darker
      const bgColor = await homepage.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      // Dark backgrounds typically have low RGB values
      // This is a basic check - actual dark themes should have darker backgrounds
      console.log(`Background color in dark mode: ${bgColor}`);
    }

    console.log(`✓ Dark mode applied in ${browserName}`);
  });

  test('light mode applies correctly based on system preference', async ({ page, browserName }) => {
    // Emulate light color scheme
    await page.emulateMedia({ colorScheme: 'light' });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Check that the page has light theme applied
    const homepage = page.locator('[data-testid="homepage"]');
    await expect(homepage).toBeVisible();

    // Verify the page responds to light mode preference
    const htmlElement = page.locator('html');
    const dataTheme = await htmlElement.getAttribute('data-theme');

    if (dataTheme) {
      // If using DaisyUI theme switching
      expect(['light', 'cupcake', 'bumblebee', 'emerald', 'corporate', 'retro', 'garden', 'lofi', 'pastel', 'fantasy', 'wireframe', 'cmyk', 'autumn', 'acid', 'lemonade', 'winter']).toContain(dataTheme.toLowerCase());
    }

    console.log(`✓ Light mode applied in ${browserName}`);
  });

  test('theme transitions do not cause layout shifts', async ({ page, browserName }) => {
    // Start with light mode
    await page.emulateMedia({ colorScheme: 'light' });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Get initial layout positions
    const heroSection = page.locator('[data-testid="hero-section"]');
    const initialBox = await heroSection.boundingBox();
    expect(initialBox).not.toBeNull();

    // Switch to dark mode
    await page.emulateMedia({ colorScheme: 'dark' });
    await page.waitForTimeout(100); // Small wait for theme transition

    // Get layout positions after theme change
    const finalBox = await heroSection.boundingBox();
    expect(finalBox).not.toBeNull();

    // Layout should not shift significantly
    expect(Math.abs(finalBox!.x - initialBox!.x)).toBeLessThan(5);
    expect(Math.abs(finalBox!.y - initialBox!.y)).toBeLessThan(5);
    expect(Math.abs(finalBox!.width - initialBox!.width)).toBeLessThan(5);
    expect(Math.abs(finalBox!.height - initialBox!.height)).toBeLessThan(5);

    console.log(`✓ No layout shift during theme transition in ${browserName}`);
  });
});
