import { test, expect } from '@playwright/test';

/**
 * E2E Tests for US-1: Understand Product Value
 *
 * User Story:
 * As a new visitor
 * I want to quickly understand what the URL shortening service offers
 * So that I can decide if it meets my needs
 *
 * Acceptance Criteria:
 * - Given I am on the homepage
 * - When the page loads
 * - Then I see a clear headline explaining the service
 * - And I see a subheadline with supporting value proposition
 * - And I can identify at least 3 key features within 5 seconds
 */

test.describe('US-1: New Visitor Understands Product Value', () => {
  test.beforeEach(async ({ page }) => {
    // Simulate new visitor arriving on homepage
    await page.goto('/');
    // Wait for page to be fully loaded
    await page.waitForLoadState('networkidle');
  });

  /**
   * Test Case 1: Clear headline explaining URL shortening service is visible above the fold
   *
   * Input: Load homepage and check hero section visibility
   * Expected: Clear headline explaining URL shortening service is visible above the fold
   */
  test('TC1: Hero section headline is visible above the fold', async ({ page }) => {
    // Verify hero section is present and visible
    const heroSection = page.getByTestId('hero-section');
    await expect(heroSection).toBeVisible();

    // Verify headline is visible
    const headline = page.getByTestId('hero-headline');
    await expect(headline).toBeVisible();

    // Verify headline contains meaningful text about URL shortening
    const headlineText = await headline.textContent();
    expect(headlineText).toBeTruthy();
    expect(headlineText!.length).toBeGreaterThan(5);

    // Verify headline is "above the fold" (in initial viewport)
    const headlineBox = await headline.boundingBox();
    expect(headlineBox).not.toBeNull();

    // Get viewport height
    const viewportSize = page.viewportSize();
    expect(viewportSize).not.toBeNull();

    // Headline should be fully visible in initial viewport
    expect(headlineBox!.y).toBeGreaterThanOrEqual(0);
    expect(headlineBox!.y + headlineBox!.height).toBeLessThanOrEqual(viewportSize!.height);

    // Verify headline explains the URL shortening service concept
    // "Shorten. Track. Share." clearly communicates the core features
    expect(headlineText).toMatch(/shorten|track|share|url|link/i);
  });

  /**
   * Test Case 2: Subheadline with value proposition is visible
   *
   * Input: Check for supporting value proposition
   * Expected: Subheadline with value proposition is visible
   */
  test('TC2: Subheadline with value proposition is visible', async ({ page }) => {
    // Wait for animations to complete
    await page.waitForTimeout(700);

    // Verify subheadline is visible
    const subheadline = page.getByTestId('hero-subheadline');
    await expect(subheadline).toBeVisible();

    // Verify subheadline contains value proposition content
    const subheadlineText = await subheadline.textContent();
    expect(subheadlineText).toBeTruthy();
    expect(subheadlineText!.length).toBeGreaterThan(20); // Should be descriptive

    // Verify subheadline explains the value proposition
    // Should mention benefits like: URLs, short links, track, clicks, analytics, manage
    expect(subheadlineText).toMatch(/url|link|track|click|analytics|manage|short/i);

    // Verify subheadline is in the hero section
    const heroSection = page.getByTestId('hero-section');
    await expect(heroSection).toContainText(subheadlineText!);
  });

  /**
   * Test Case 3: At least 3 key features are identifiable
   *
   * Input: Count identifiable features within initial viewport and first scroll
   * Expected: At least 3 key features are identifiable (URL shortening, analytics, link management)
   */
  test('TC3: At least 3 key features are identifiable', async ({ page }) => {
    // Scroll down to features section to ensure it's loaded
    await page.getByTestId('features-section').scrollIntoViewIfNeeded();

    // Wait for animations to complete
    await page.waitForTimeout(600);

    // Verify features section is visible
    const featuresSection = page.getByTestId('features-section');
    await expect(featuresSection).toBeVisible();

    // Count feature cards
    const featureCardsContainer = page.getByTestId('feature-cards-container');
    await expect(featureCardsContainer).toBeVisible();

    // Get all feature cards
    const urlShorteningCard = page.getByTestId('feature-card-url-shortening');
    const analyticsCard = page.getByTestId('feature-card-analytics');
    const linkManagementCard = page.getByTestId('feature-card-link-management');

    // Verify at least 3 key features are present
    await expect(urlShorteningCard).toBeVisible();
    await expect(analyticsCard).toBeVisible();
    await expect(linkManagementCard).toBeVisible();

    // Verify each feature has a title that identifies the feature
    const urlShorteningTitle = page.getByTestId('feature-title-url-shortening');
    await expect(urlShorteningTitle).toHaveText(/url shortening/i);

    const analyticsTitle = page.getByTestId('feature-title-analytics');
    await expect(analyticsTitle).toHaveText(/analytics/i);

    const linkManagementTitle = page.getByTestId('feature-title-link-management');
    await expect(linkManagementTitle).toHaveText(/link management/i);

    // Verify each feature has a description
    const urlShorteningDescription = page.getByTestId('feature-description-url-shortening');
    await expect(urlShorteningDescription).not.toBeEmpty();

    const analyticsDescription = page.getByTestId('feature-description-analytics');
    await expect(analyticsDescription).not.toBeEmpty();

    const linkManagementDescription = page.getByTestId('feature-description-link-management');
    await expect(linkManagementDescription).not.toBeEmpty();
  });

  /**
   * Additional Test: Full user flow - new visitor quickly understands the product
   *
   * This test simulates the complete journey of a new visitor:
   * 1. Land on homepage
   * 2. View hero content
   * 3. Identify key features within 5 seconds
   */
  test('Full US-1 flow: New visitor can quickly understand product offering', async ({ page }) => {
    // Step 1: Land on homepage (already done in beforeEach)
    await expect(page).toHaveURL('/');

    // Step 2: View hero content - should be immediately visible
    const heroContent = page.getByTestId('hero-content');
    await expect(heroContent).toBeVisible();

    // Verify headline is visible
    const headline = page.getByTestId('hero-headline');
    await expect(headline).toBeVisible();
    await expect(headline).toHaveText('Shorten. Track. Share.');

    // Verify subheadline with value proposition
    const subheadline = page.getByTestId('hero-subheadline');
    await expect(subheadline).toBeVisible();
    const subheadlineText = await subheadline.textContent();
    expect(subheadlineText).toContain('Transform long URLs');
    expect(subheadlineText).toContain('Track clicks');

    // Step 3: Identify key features - scroll to features section
    await page.getByTestId('features-section').scrollIntoViewIfNeeded();

    // Wait for feature animations to complete
    await page.waitForTimeout(500);

    // Count visible features to ensure at least 3 are identifiable
    const featureCards = page.locator('[data-testid^="feature-card-"]');
    const visibleFeatureCount = await featureCards.count();
    expect(visibleFeatureCount).toBeGreaterThanOrEqual(3);

    // Verify the key features mentioned in acceptance criteria
    await expect(page.getByTestId('feature-card-url-shortening')).toBeVisible();
    await expect(page.getByTestId('feature-card-analytics')).toBeVisible();
    await expect(page.getByTestId('feature-card-link-management')).toBeVisible();
  });

  /**
   * Test: Hero section CTA buttons are visible and accessible
   */
  test('Hero section CTA buttons are visible for next steps', async ({ page }) => {
    // Wait for animations
    await page.waitForTimeout(500);

    // Verify CTA buttons are visible
    const ctaButtons = page.getByTestId('hero-cta-buttons');
    await expect(ctaButtons).toBeVisible();

    // Verify "Get Started Free" button is visible
    const getStartedBtn = page.getByTestId('get-started-btn');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toContainText('Get Started Free');

    // Verify "Login" button is visible for returning users
    const loginBtn = page.getByTestId('login-btn');
    await expect(loginBtn).toBeVisible();
    await expect(loginBtn).toContainText('Login');
  });

  /**
   * Test: Page loads within acceptable time (performance validation)
   */
  test('Page loads and displays hero content quickly', async ({ page }) => {
    // Navigate with performance timing
    const startTime = Date.now();
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
    const loadTime = Date.now() - startTime;

    // Page should load within 2 seconds (NFR-1)
    expect(loadTime).toBeLessThan(2000);

    // Hero content should be visible immediately after load
    await expect(page.getByTestId('hero-section')).toBeVisible();
    await expect(page.getByTestId('hero-headline')).toBeVisible();
  });
});
