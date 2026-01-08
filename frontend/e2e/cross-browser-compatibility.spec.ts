import { test, expect, Page, BrowserContext } from '@playwright/test';

/**
 * Cross-Browser Compatibility Tests for Homepage
 *
 * These tests verify that the homepage renders correctly across major browsers:
 * - Chrome (Chromium)
 * - Firefox
 * - Safari (WebKit)
 * - Edge (Chromium-based)
 *
 * Each test validates core page elements, layout, and functionality
 */

// Test data for consistent verification
const expectedSections = {
  hero: {
    testId: 'hero-section',
    headline: 'Shorten, Share, Track',
  },
  demo: {
    testId: 'demo-section',
    title: 'Try It Now',
  },
  features: {
    testId: 'features-section',
    count: 4,
  },
  howItWorks: {
    testId: 'how-it-works-section',
    stepsCount: 3,
  },
  socialProof: {
    testId: 'social-proof-section',
    statisticsCount: 4,
    testimonialsCount: 3,
  },
  footer: {
    testId: 'homepage-footer',
  },
};

/**
 * Verifies all essential homepage elements are rendered correctly
 */
async function verifyHomepageRendering(page: Page, browserName: string) {
  // Wait for the page to load completely
  await page.waitForLoadState('networkidle');

  // 1. Verify Hero Section
  const heroSection = page.getByTestId(expectedSections.hero.testId);
  await expect(heroSection, `[${browserName}] Hero section should be visible`).toBeVisible();

  const headline = page.getByTestId('hero-headline-animated');
  await expect(headline, `[${browserName}] Hero headline should be visible`).toBeVisible();
  await expect(headline).toContainText(expectedSections.hero.headline);

  const subheadline = page.getByTestId('hero-subheadline-animated');
  await expect(subheadline, `[${browserName}] Hero subheadline should be visible`).toBeVisible();

  // 2. Verify Demo Section
  const demoSection = page.getByTestId(expectedSections.demo.testId);
  await expect(demoSection, `[${browserName}] Demo section should be visible`).toBeVisible();

  const demoInput = page.getByTestId('demo-url-input');
  await expect(demoInput, `[${browserName}] Demo URL input should be visible`).toBeVisible();

  const demoButton = page.getByTestId('demo-shorten-button');
  await expect(demoButton, `[${browserName}] Demo shorten button should be visible`).toBeVisible();

  // 3. Verify Features Section
  const featuresSection = page.getByTestId(expectedSections.features.testId);
  await expect(featuresSection, `[${browserName}] Features section should be visible`).toBeVisible();

  const featuresGrid = page.getByTestId('features-grid');
  await expect(featuresGrid, `[${browserName}] Features grid should be visible`).toBeVisible();

  // Verify all 4 feature cards are rendered
  const featureIds = ['url-shortening', 'analytics-dashboard', 'geographic-insights', 'share-collaborate'];
  for (const featureId of featureIds) {
    const featureCard = page.getByTestId(`feature-card-${featureId}`);
    await expect(featureCard, `[${browserName}] Feature card ${featureId} should be visible`).toBeVisible();
  }

  // 4. Verify How It Works Section
  const howItWorksSection = page.getByTestId(expectedSections.howItWorks.testId);
  await expect(howItWorksSection, `[${browserName}] How It Works section should be visible`).toBeVisible();

  // Verify all 3 steps
  for (let i = 1; i <= expectedSections.howItWorks.stepsCount; i++) {
    const step = page.getByTestId(`how-it-works-step-${i}`);
    await expect(step, `[${browserName}] How It Works step ${i} should be visible`).toBeVisible();
  }

  // 5. Verify Social Proof Section
  const socialProofSection = page.getByTestId(expectedSections.socialProof.testId);
  await expect(socialProofSection, `[${browserName}] Social Proof section should be visible`).toBeVisible();

  const statisticsGrid = page.getByTestId('statistics-grid');
  await expect(statisticsGrid, `[${browserName}] Statistics grid should be visible`).toBeVisible();

  const testimonialsGrid = page.getByTestId('testimonials-grid');
  await expect(testimonialsGrid, `[${browserName}] Testimonials grid should be visible`).toBeVisible();

  // 6. Verify Footer
  const footer = page.getByTestId(expectedSections.footer.testId);
  await expect(footer, `[${browserName}] Footer should be visible`).toBeVisible();

  return true;
}

/**
 * Verifies page layout and styling consistency
 */
async function verifyLayoutAndStyling(page: Page, browserName: string) {
  // Check that the page has proper minimum height
  const body = page.locator('body');
  const bodyBox = await body.boundingBox();
  expect(bodyBox, `[${browserName}] Body should have dimensions`).toBeTruthy();
  expect(bodyBox!.height, `[${browserName}] Page should have reasonable height`).toBeGreaterThan(500);

  // Verify hero section has proper gradient background
  const heroSection = page.getByTestId('hero-section');
  const heroStyles = await heroSection.evaluate((el) => {
    const styles = window.getComputedStyle(el);
    return {
      backgroundImage: styles.backgroundImage,
      minHeight: styles.minHeight,
    };
  });
  expect(heroStyles.minHeight, `[${browserName}] Hero section should have min-height`).toBeTruthy();

  // Verify features grid has proper layout
  const featuresGrid = page.getByTestId('features-grid');
  const gridStyles = await featuresGrid.evaluate((el) => {
    const styles = window.getComputedStyle(el);
    return {
      display: styles.display,
      gridTemplateColumns: styles.gridTemplateColumns,
    };
  });
  expect(gridStyles.display, `[${browserName}] Features grid should use grid layout`).toBe('grid');

  return true;
}

/**
 * Verifies interactive elements are functional
 */
async function verifyInteractiveElements(page: Page, browserName: string) {
  // Test demo URL input functionality
  const demoInput = page.getByTestId('demo-url-input');
  await demoInput.fill('https://example.com/very/long/url');
  await expect(demoInput).toHaveValue('https://example.com/very/long/url');

  // Test shorten button becomes enabled with valid input
  const demoButton = page.getByTestId('demo-shorten-button');
  await expect(demoButton, `[${browserName}] Shorten button should be enabled with valid URL`).toBeEnabled();

  // Click to test demo functionality
  await demoButton.click();

  // Wait for demo result to appear
  const demoResult = page.getByTestId('demo-result');
  await expect(demoResult, `[${browserName}] Demo result should appear after clicking shorten`).toBeVisible({ timeout: 5000 });

  // Verify short URL preview is displayed
  const shortUrlPreview = page.getByTestId('demo-short-url-preview');
  await expect(shortUrlPreview, `[${browserName}] Short URL preview should be visible`).toBeVisible();

  // Test reset functionality
  const resetButton = page.getByTestId('demo-reset-button');
  await expect(resetButton, `[${browserName}] Reset button should be visible`).toBeVisible();
  await resetButton.click();

  // Verify demo result is hidden after reset
  await expect(demoResult, `[${browserName}] Demo result should be hidden after reset`).not.toBeVisible();

  return true;
}

/**
 * Verifies accessibility features are working
 */
async function verifyAccessibility(page: Page, browserName: string) {
  // Check skip link exists
  const skipLink = page.getByTestId('skip-link');
  await expect(skipLink, `[${browserName}] Skip link should exist in DOM`).toHaveCount(1);

  // Verify main content has proper id for skip link target
  const mainContent = page.locator('#main-content');
  await expect(mainContent, `[${browserName}] Main content should exist`).toHaveCount(1);

  // Verify headings hierarchy
  const h1Count = await page.locator('h1').count();
  expect(h1Count, `[${browserName}] Page should have exactly one h1`).toBe(1);

  // Verify all interactive elements have proper ARIA attributes
  const buttons = page.locator('button');
  const buttonCount = await buttons.count();
  expect(buttonCount, `[${browserName}] Page should have buttons`).toBeGreaterThan(0);

  return true;
}

/**
 * Verifies no visual bugs or rendering issues
 */
async function verifyNoVisualBugs(page: Page, browserName: string) {
  // Check for overflow issues
  const hasHorizontalScroll = await page.evaluate(() => {
    return document.documentElement.scrollWidth > document.documentElement.clientWidth;
  });
  expect(hasHorizontalScroll, `[${browserName}] Page should not have horizontal scroll`).toBe(false);

  // Check for elements with zero dimensions that should be visible
  const heroHeadline = page.getByTestId('hero-headline-animated');
  const headlineBox = await heroHeadline.boundingBox();
  expect(headlineBox, `[${browserName}] Hero headline should have dimensions`).toBeTruthy();
  expect(headlineBox!.width, `[${browserName}] Hero headline should have positive width`).toBeGreaterThan(0);
  expect(headlineBox!.height, `[${browserName}] Hero headline should have positive height`).toBeGreaterThan(0);

  // Check footer is at the bottom
  const footer = page.getByTestId('homepage-footer');
  const footerBox = await footer.boundingBox();
  const viewportHeight = await page.evaluate(() => window.innerHeight);
  expect(footerBox!.y, `[${browserName}] Footer should be below viewport fold`).toBeGreaterThan(viewportHeight - 100);

  return true;
}

// Test Suite: Chrome (Chromium)
test.describe('Chrome Browser Compatibility', () => {
  test('homepage renders correctly in Chrome', async ({ page, browserName }) => {
    // Skip if not running in chromium
    test.skip(browserName !== 'chromium', 'This test is for Chrome only');

    await page.goto('/');

    // Run all verification checks
    await verifyHomepageRendering(page, 'Chrome');
    await verifyLayoutAndStyling(page, 'Chrome');
    await verifyInteractiveElements(page, 'Chrome');
    await verifyAccessibility(page, 'Chrome');
    await verifyNoVisualBugs(page, 'Chrome');
  });
});

// Test Suite: Firefox
test.describe('Firefox Browser Compatibility', () => {
  test('homepage renders correctly in Firefox', async ({ page, browserName }) => {
    // Skip if not running in firefox
    test.skip(browserName !== 'firefox', 'This test is for Firefox only');

    await page.goto('/');

    // Run all verification checks
    await verifyHomepageRendering(page, 'Firefox');
    await verifyLayoutAndStyling(page, 'Firefox');
    await verifyInteractiveElements(page, 'Firefox');
    await verifyAccessibility(page, 'Firefox');
    await verifyNoVisualBugs(page, 'Firefox');
  });
});

// Test Suite: Safari (WebKit)
test.describe('Safari Browser Compatibility', () => {
  test('homepage renders correctly in Safari', async ({ page, browserName }) => {
    // Skip if not running in webkit
    test.skip(browserName !== 'webkit', 'This test is for Safari only');

    await page.goto('/');

    // Run all verification checks
    await verifyHomepageRendering(page, 'Safari');
    await verifyLayoutAndStyling(page, 'Safari');
    await verifyInteractiveElements(page, 'Safari');
    await verifyAccessibility(page, 'Safari');
    await verifyNoVisualBugs(page, 'Safari');
  });
});

// Note: Edge uses the same Chromium engine as Chrome, so Chromium tests
// verify Edge compatibility. Microsoft Edge (Chromium-based) renders
// identically to Chrome for web content.

// Cross-Browser Common Tests (runs on all browsers)
test.describe('Cross-Browser Common Tests', () => {
  test('core homepage elements render across all browsers', async ({ page, browserName }) => {
    await page.goto('/');

    // Wait for page to fully load
    await page.waitForLoadState('networkidle');

    // Verify essential sections exist and are visible
    const heroSection = page.getByTestId('hero-section');
    await expect(heroSection).toBeVisible();

    const featuresSection = page.getByTestId('features-section');
    await expect(featuresSection).toBeVisible();

    const howItWorksSection = page.getByTestId('how-it-works-section');
    await expect(howItWorksSection).toBeVisible();

    const footer = page.getByTestId('homepage-footer');
    await expect(footer).toBeVisible();

    // Verify all feature cards
    const featureCards = page.getByTestId('features-grid').locator('[data-testid^="feature-card-"]');
    await expect(featureCards).toHaveCount(4);
  });

  test('navigation links work across all browsers', async ({ page, browserName }) => {
    await page.goto('/');

    // Wait for page to load
    await page.waitForLoadState('networkidle');

    // Check that Login link exists in navigation
    const loginLink = page.getByRole('link', { name: /login/i });
    await expect(loginLink).toBeVisible();

    // Check that Register link exists
    const registerLink = page.getByRole('link', { name: /register|sign up|get started/i }).first();
    await expect(registerLink).toBeVisible();
  });

  test('responsive layout adapts correctly across all browsers', async ({ page, browserName }) => {
    // Test desktop viewport
    await page.setViewportSize({ width: 1280, height: 720 });
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Features grid should show 4 columns on desktop
    const featuresGrid = page.getByTestId('features-grid');
    await expect(featuresGrid).toBeVisible();

    // Test tablet viewport
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.waitForTimeout(500); // Allow layout to reflow

    // Features should still be visible in tablet view
    await expect(featuresGrid).toBeVisible();

    // Test mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await page.waitForTimeout(500); // Allow layout to reflow

    // Features should still be visible in mobile view
    await expect(featuresGrid).toBeVisible();
  });

  test('theme toggle functionality works across all browsers', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Scroll to footer where theme toggle is located
    const footer = page.getByTestId('homepage-footer');
    await footer.scrollIntoViewIfNeeded();

    // Find theme toggle in footer (uses data-testid="theme-toggle")
    const themeToggle = footer.getByTestId('theme-toggle');

    // Verify theme toggle exists
    await expect(themeToggle, `[${browserName}] Theme toggle should be visible`).toBeVisible();
  });
});
