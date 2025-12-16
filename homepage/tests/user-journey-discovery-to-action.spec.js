// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('User Journey - Discovery to Action', () => {
  /**
   * Test Case 1: Complete user journey from hero to GitHub link
   * User can navigate from hero → features → architecture → getting started → GitHub in logical flow
   */
  test('should allow complete navigation from hero through all sections to GitHub', async ({ page }) => {
    // Step 1: Discovery - User lands on homepage
    await page.goto('/');

    // Verify hero section is visible (initial page load)
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();
    await expect(heroSection).toBeInViewport();

    // Step 2: Understanding - User reads hero section to understand what MirDB is
    const productName = page.locator('[data-testid="product-name"]');
    await expect(productName).toBeVisible();
    await expect(productName).toHaveText('MirDB');

    const tagline = page.locator('[data-testid="tagline"]');
    await expect(tagline).toBeVisible();
    const taglineText = await tagline.textContent();
    expect(taglineText).toContain('persistent key-value store');

    // Step 3: Navigate to features section (evaluation)
    const learnMoreBtn = page.locator('[data-testid="secondary-cta"]');
    await learnMoreBtn.click();
    await page.waitForTimeout(500); // Wait for smooth scroll

    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeInViewport();

    // Step 4: Navigate to architecture section (deep dive)
    // Use navigation link or scroll
    const architectureNavLink = page.locator('a[href="#architecture"]').first();
    await architectureNavLink.click();
    await page.waitForTimeout(500);

    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeInViewport();

    // Step 5: Navigate to getting started section
    const gettingStartedNavLink = page.locator('a[href="#getting-started"]').first();
    await gettingStartedNavLink.click();
    await page.waitForTimeout(500);

    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeInViewport();

    // Step 6: Action - User can access GitHub repository
    const primaryCTA = page.locator('[data-testid="primary-cta"]');
    const gitHubHref = await primaryCTA.getAttribute('href');
    expect(gitHubHref).toContain('github.com');

    // Also verify footer GitHub link is accessible
    const footerGitHubLink = page.locator('footer a[href*="github.com"]');
    await expect(footerGitHubLink).toBeVisible();
  });

  /**
   * Test Case 2: Test scroll-to-section functionality
   * Clicking Learn More scrolls smoothly to features section
   */
  test('should scroll smoothly to features section when clicking Learn More', async ({ page }) => {
    await page.goto('/');

    // Verify we start at the top of the page
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeInViewport();

    // Click Learn More button
    const learnMoreBtn = page.locator('[data-testid="secondary-cta"]');
    await expect(learnMoreBtn).toBeVisible();
    await expect(learnMoreBtn).toHaveText('Learn More');

    // Verify href attribute
    const href = await learnMoreBtn.getAttribute('href');
    expect(href).toBe('#features');

    // Click and verify smooth scroll
    await learnMoreBtn.click();

    // Wait for smooth scroll animation to complete
    await page.waitForTimeout(500);

    // Verify features section is now in viewport
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeInViewport();

    // Verify scroll actually happened by checking hero is less visible
    // The features section should now be the primary visible content
    const featureHeading = featuresSection.locator('h2');
    await expect(featureHeading).toBeInViewport();
  });

  /**
   * Test Case 3: Verify clear path to GitHub repository
   * GitHub link is accessible from multiple locations (hero CTA, footer)
   */
  test('should provide GitHub link access from hero CTA and footer', async ({ page }) => {
    await page.goto('/');

    // Verify primary CTA in hero section has GitHub link
    const primaryCTA = page.locator('[data-testid="primary-cta"]');
    await expect(primaryCTA).toBeVisible();

    const primaryHref = await primaryCTA.getAttribute('href');
    expect(primaryHref).toContain('github.com');
    expect(primaryHref).toContain('mirdb');

    // Verify primary CTA text indicates GitHub
    const primaryText = await primaryCTA.textContent();
    expect(primaryText?.toLowerCase()).toContain('github');

    // Scroll to footer
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Verify footer has GitHub link
    const footerGitHubLink = page.locator('footer a[href*="github.com"]');
    await expect(footerGitHubLink).toBeVisible();

    const footerHref = await footerGitHubLink.getAttribute('href');
    expect(footerHref).toContain('github.com');
    expect(footerHref).toContain('mirdb');

    // Both links should point to the same repository
    expect(primaryHref).toBe(footerHref);
  });

  /**
   * Test Case 4: Measure time to understand value proposition (manual verification via test)
   * Hero section clearly communicates what MirDB is within first viewport
   *
   * Note: This test verifies that all key information is visible in the hero section
   * without scrolling - enabling users to understand the value proposition immediately.
   */
  test('should display clear value proposition in hero section within first viewport', async ({ page }) => {
    await page.goto('/');

    // Verify hero section is in viewport on page load
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeInViewport();

    // Verify product name is visible and in viewport
    const productName = page.locator('[data-testid="product-name"]');
    await expect(productName).toBeVisible();
    await expect(productName).toBeInViewport();
    await expect(productName).toHaveText('MirDB');

    // Verify tagline with full value proposition is visible and in viewport
    const tagline = page.locator('[data-testid="tagline"]');
    await expect(tagline).toBeVisible();
    await expect(tagline).toBeInViewport();

    // Verify tagline contains key value proposition elements
    const taglineText = await tagline.textContent();

    // What it is
    expect(taglineText).toContain('persistent key-value store');

    // What makes it special
    expect(taglineText).toContain('Memcached protocol compatibility');

    // Technology
    expect(taglineText).toContain('Rust');

    // Verify CTAs are visible for immediate action
    const primaryCTA = page.locator('[data-testid="primary-cta"]');
    const secondaryCTA = page.locator('[data-testid="secondary-cta"]');

    await expect(primaryCTA).toBeVisible();
    await expect(primaryCTA).toBeInViewport();
    await expect(secondaryCTA).toBeVisible();
    await expect(secondaryCTA).toBeInViewport();
  });

  /**
   * Additional test: Navigation bar allows quick access to all sections
   */
  test('should have navigation bar with links to all main sections', async ({ page }) => {
    await page.goto('/');

    // Verify navigation exists
    const nav = page.locator('nav');
    await expect(nav).toBeVisible();

    // Verify navigation links to all main sections
    const featuresLink = page.locator('nav a[href="#features"]');
    const architectureLink = page.locator('nav a[href="#architecture"]');
    const commandsLink = page.locator('nav a[href="#commands"]');
    const gettingStartedLink = page.locator('nav a[href="#getting-started"]');

    await expect(featuresLink).toBeVisible();
    await expect(architectureLink).toBeVisible();
    await expect(commandsLink).toBeVisible();
    await expect(gettingStartedLink).toBeVisible();
  });

  /**
   * Additional test: User can navigate back to top after scrolling
   */
  test('should allow navigation between all sections in any order', async ({ page }) => {
    await page.goto('/');

    // Navigate to getting started first
    const gettingStartedLink = page.locator('nav a[href="#getting-started"]');
    await gettingStartedLink.click();
    await page.waitForTimeout(500);

    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeInViewport();

    // Navigate back to features
    const featuresLink = page.locator('nav a[href="#features"]');
    await featuresLink.click();
    await page.waitForTimeout(500);

    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeInViewport();

    // Navigate to architecture
    const architectureLink = page.locator('nav a[href="#architecture"]');
    await architectureLink.click();
    await page.waitForTimeout(500);

    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeInViewport();
  });
});
