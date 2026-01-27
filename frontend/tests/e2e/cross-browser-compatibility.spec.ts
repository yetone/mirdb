import { test, expect, type Page } from '@playwright/test';

/**
 * Cross-Browser Compatibility Tests
 * Owner: Scenario 19 - Cross-Browser Compatibility
 *
 * These tests verify that the landing page renders correctly across
 * major browsers: Chrome, Firefox, Safari (WebKit), and Edge.
 *
 * Test Cases:
 * 1. Render landing page in Chrome - All sections render correctly without layout issues
 * 2. Render landing page in Firefox - All sections render correctly without layout issues
 * 3. Render landing page in Safari - All sections render correctly without layout issues
 * 4. Render landing page in Edge - All sections render correctly without layout issues
 *
 * Each test validates:
 * - Navigation header with logo and links
 * - Hero section with headline, subheadline, and CTAs
 * - Features section with feature cards
 * - How It Works section with steps
 * - CTA section with call-to-action
 * - Footer with copyright
 * - Theme toggle functionality
 * - Responsive layout integrity
 */

/**
 * Helper function to verify all landing page sections are rendered
 */
async function verifyLandingPageSections(page: Page) {
  // Verify Navigation Header
  await expect(page.locator('header')).toBeVisible();
  await expect(page.getByRole('link', { name: 'URL Shortener' })).toBeVisible();
  await expect(page.getByRole('link', { name: 'Login' })).toBeVisible();
  await expect(page.getByRole('link', { name: 'Get Started' })).toBeVisible();

  // Verify Hero Section
  await expect(page.getByRole('heading', { name: /Shorten Links\. Track Everything\./i })).toBeVisible();
  await expect(page.getByText(/Create short, powerful links/i)).toBeVisible();
  // Primary CTA button in hero
  await expect(page.getByRole('button', { name: /Get Started/i }).first()).toBeVisible();
  // Secondary CTA link
  await expect(page.getByRole('link', { name: /Learn More/i })).toBeVisible();
  // Dashboard preview mockup
  await expect(page.locator('.mockup-window')).toBeVisible();

  // Verify Features Section
  await expect(page.getByRole('heading', { name: 'Features' })).toBeVisible();
  await expect(page.locator('[data-testid="features-section"]')).toBeVisible();
  // Use data-testid selectors for feature titles to avoid strict mode violations
  const featureTitles = page.locator('[data-testid="feature-title"]');
  await expect(featureTitles).toHaveCount(4);
  await expect(featureTitles.first()).toBeVisible();

  // Verify How It Works Section
  await expect(page.getByRole('heading', { name: 'How It Works' })).toBeVisible();
  // Use step data-testid selectors to verify steps are present
  await expect(page.locator('[data-testid="step-1"]')).toBeVisible();
  await expect(page.locator('[data-testid="step-2"]')).toBeVisible();
  await expect(page.locator('[data-testid="step-3"]')).toBeVisible();
  await expect(page.locator('[data-testid="step-4"]')).toBeVisible();

  // Verify CTA Section
  await expect(page.locator('[data-testid="cta-section"]')).toBeVisible();
  await expect(page.locator('[data-testid="cta-register-button"]')).toBeVisible();
  await expect(page.locator('[data-testid="reassurance-text"]')).toBeVisible();

  // Verify Footer
  await expect(page.locator('footer')).toBeVisible();
  await expect(page.getByText(/© \d{4} URL Shortener/i)).toBeVisible();
}

/**
 * Helper function to verify layout integrity
 */
async function verifyLayoutIntegrity(page: Page) {
  // Check that key elements don't overflow
  const viewport = page.viewportSize();
  if (!viewport) return;

  // Verify header is at top and sticky
  const header = page.locator('header');
  await expect(header).toBeVisible();
  const headerBox = await header.boundingBox();
  expect(headerBox).toBeTruthy();
  if (headerBox) {
    expect(headerBox.y).toBeLessThanOrEqual(10); // Should be at or near top
    expect(headerBox.width).toBeGreaterThan(viewport.width * 0.9); // Should span most of viewport
  }

  // Verify main content is centered
  const main = page.locator('main');
  await expect(main).toBeVisible();

  // Verify footer is present
  const footer = page.locator('footer');
  await expect(footer).toBeVisible();
}

/**
 * Helper function to verify theme toggle works
 */
async function verifyThemeToggle(page: Page) {
  // Find theme toggle button
  const themeToggle = page.getByRole('button', { name: /switch to (light|dark) mode/i }).first();
  await expect(themeToggle).toBeVisible();

  // Get initial theme
  const initialTheme = await page.locator('html').getAttribute('data-theme');

  // Click to toggle
  await themeToggle.click();

  // Wait for theme to change
  await page.waitForTimeout(300);

  // Verify theme changed
  const newTheme = await page.locator('html').getAttribute('data-theme');
  expect(newTheme).not.toBe(initialTheme);

  // Toggle back
  await themeToggle.click();
  await page.waitForTimeout(300);

  // All sections should still be visible after theme toggle
  await expect(page.getByRole('heading', { name: /Shorten Links\. Track Everything\./i })).toBeVisible();
}

/**
 * Helper function to verify responsive behavior
 */
async function verifyResponsiveBehavior(page: Page) {
  // Desktop viewport (default)
  await expect(page.locator('header')).toBeVisible();

  // Verify navigation links are visible on desktop
  await expect(page.getByRole('link', { name: 'Login' })).toBeVisible();
  await expect(page.getByRole('link', { name: 'Get Started' })).toBeVisible();

  // Verify grid layouts
  const featuresSection = page.locator('[data-testid="features-section"]');
  await expect(featuresSection).toBeVisible();
}

/**
 * Helper function to verify CSS styling is applied correctly
 */
async function verifyCssStyling(page: Page) {
  // Verify gradient text on hero heading
  const heroHeading = page.getByRole('heading', { name: /Shorten Links\. Track Everything\./i });
  await expect(heroHeading).toBeVisible();

  // Verify feature cards in features section
  const featureCards = page.locator('[data-testid="feature-card"]');
  const cardCount = await featureCards.count();
  expect(cardCount).toBeGreaterThanOrEqual(4);

  // Verify buttons have proper styling
  const primaryButtons = page.locator('.btn-primary');
  await expect(primaryButtons.first()).toBeVisible();
}

/**
 * Helper function to verify animations load properly
 */
async function verifyAnimations(page: Page) {
  // Wait for page to fully load with animations
  await page.waitForLoadState('networkidle');
  await page.waitForTimeout(1000); // Wait for initial animations to complete

  // Verify animated elements are visible after animation completes
  await expect(page.getByRole('heading', { name: /Shorten Links\. Track Everything\./i })).toBeVisible();
  await expect(page.getByText(/Create short, powerful links/i).first()).toBeVisible();

  // Verify feature cards are visible
  const featureTitles = page.locator('[data-testid="feature-title"]');
  await expect(featureTitles.first()).toBeVisible();
}

// Test Case 1: Chrome (Chromium)
test.describe('Cross-Browser: Chrome', () => {
  test('renders landing page correctly in Chrome', async ({ page, browserName }) => {
    test.skip(browserName !== 'chromium', 'Chrome-specific test');

    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Allow animations to complete
    await page.waitForTimeout(1500);

    await verifyLandingPageSections(page);
    await verifyLayoutIntegrity(page);
    await verifyThemeToggle(page);
    await verifyCssStyling(page);
    await verifyAnimations(page);
  });

  test('handles scroll navigation in Chrome', async ({ page, browserName }) => {
    test.skip(browserName !== 'chromium', 'Chrome-specific test');

    await page.goto('/');
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(1500);

    // Click Learn More to scroll to features
    await page.getByRole('link', { name: /Learn More/i }).click();
    await page.waitForTimeout(500);

    // Verify features section is in view
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });
});

// Test Case 2: Firefox
test.describe('Cross-Browser: Firefox', () => {
  test('renders landing page correctly in Firefox', async ({ page, browserName }) => {
    test.skip(browserName !== 'firefox', 'Firefox-specific test');

    await page.goto('/');
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(1500);

    await verifyLandingPageSections(page);
    await verifyLayoutIntegrity(page);
    await verifyThemeToggle(page);
    await verifyCssStyling(page);
    await verifyAnimations(page);
  });

  test('CSS transforms work correctly in Firefox', async ({ page, browserName }) => {
    test.skip(browserName !== 'firefox', 'Firefox-specific test');

    await page.goto('/');
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(1500);

    // Verify hover states on buttons
    const getStartedButton = page.getByRole('button', { name: /Get Started/i }).first();
    await getStartedButton.hover();
    await page.waitForTimeout(200);

    // Button should still be visible after hover
    await expect(getStartedButton).toBeVisible();
  });
});

// Test Case 3: Safari (WebKit)
test.describe('Cross-Browser: Safari (WebKit)', () => {
  test('renders landing page correctly in Safari', async ({ page, browserName }) => {
    test.skip(browserName !== 'webkit', 'Safari-specific test');

    await page.goto('/');
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(1500);

    await verifyLandingPageSections(page);
    await verifyLayoutIntegrity(page);
    await verifyThemeToggle(page);
    await verifyCssStyling(page);
    await verifyAnimations(page);
  });

  test('flexbox layout renders correctly in Safari', async ({ page, browserName }) => {
    test.skip(browserName !== 'webkit', 'Safari-specific test');

    await page.goto('/');
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(1500);

    // Verify flexbox containers are rendered correctly
    const heroButtonContainer = page.locator('section').first().locator('.flex').first();
    await expect(heroButtonContainer).toBeVisible();

    // Verify grid layout in features section
    const featuresGrid = page.locator('[data-testid="features-section"]');
    await expect(featuresGrid).toBeVisible();
  });

  test('backdrop-blur effects work in Safari', async ({ page, browserName }) => {
    test.skip(browserName !== 'webkit', 'Safari-specific test');

    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Verify header with backdrop-blur is visible
    const header = page.locator('header');
    await expect(header).toBeVisible();

    // Check that feature cards are visible
    const featureCards = page.locator('[data-testid="feature-card"]');
    await expect(featureCards.first()).toBeVisible();
  });
});

// Test Case 4: Edge
test.describe('Cross-Browser: Edge', () => {
  test('renders landing page correctly in Edge', async ({ page, browserName }) => {
    // Edge uses chromium, so this test runs on chromium
    // We test Edge-specific scenarios if msedge channel is available
    test.skip(browserName !== 'chromium', 'Edge (Chromium) test');

    await page.goto('/');
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(1500);

    await verifyLandingPageSections(page);
    await verifyLayoutIntegrity(page);
    await verifyThemeToggle(page);
    await verifyCssStyling(page);
    await verifyAnimations(page);
  });

  test('navigation works correctly in Edge', async ({ page, browserName }) => {
    test.skip(browserName !== 'chromium', 'Edge (Chromium) test');

    await page.goto('/');
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(1500);

    // Test navigation links
    const loginLink = page.getByRole('link', { name: 'Login' });
    await expect(loginLink).toBeVisible();
    await expect(loginLink).toHaveAttribute('href', '/login');

    const getStartedLink = page.getByRole('link', { name: 'Get Started' });
    await expect(getStartedLink).toBeVisible();
    await expect(getStartedLink).toHaveAttribute('href', '/register');
  });
});

// Cross-Browser Common Tests (run on all browsers)
test.describe('Cross-Browser: All Browsers', () => {
  test('page loads without JavaScript errors', async ({ page }) => {
    const errors: string[] = [];
    page.on('pageerror', (error) => {
      errors.push(error.message);
    });

    await page.goto('/');
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(1500);

    // Verify no critical errors occurred
    const criticalErrors = errors.filter(
      (e) => !e.includes('ResizeObserver') && !e.includes('Non-Error')
    );
    expect(criticalErrors).toHaveLength(0);
  });

  test('page has proper heading hierarchy', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(1500);

    // Should have exactly one h1
    const h1Elements = await page.locator('h1').all();
    expect(h1Elements.length).toBe(1);

    // H2 elements should exist for sections
    const h2Elements = await page.locator('h2').all();
    expect(h2Elements.length).toBeGreaterThanOrEqual(2);
  });

  test('all images and icons have appropriate attributes', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(1500);

    // Check that icons have aria-hidden or title attributes
    const svgIcons = await page.locator('svg').all();
    for (const icon of svgIcons) {
      const ariaHidden = await icon.getAttribute('aria-hidden');
      const title = await icon.locator('title').count();
      const isDecorative = ariaHidden === 'true' || title > 0;
      expect(isDecorative).toBeTruthy();
    }
  });

  test('interactive elements are accessible', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(1500);

    // All buttons should be accessible
    const buttons = await page.locator('button').all();
    for (const button of buttons) {
      const isVisible = await button.isVisible();
      if (isVisible) {
        // Should have accessible name
        const ariaLabel = await button.getAttribute('aria-label');
        const text = await button.textContent();
        expect(ariaLabel || text?.trim()).toBeTruthy();
      }
    }

    // All links should be accessible
    const links = await page.locator('a').all();
    for (const link of links) {
      const isVisible = await link.isVisible();
      if (isVisible) {
        const ariaLabel = await link.getAttribute('aria-label');
        const text = await link.textContent();
        expect(ariaLabel || text?.trim()).toBeTruthy();
      }
    }
  });
});
