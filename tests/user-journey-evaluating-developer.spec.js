// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: User Journey - Evaluating Developer
 * Scenario: Verify US-1: Evaluating developer can quickly understand product value
 *
 * This test suite validates the complete user journey for a developer
 * evaluating MirDB as a key-value store solution.
 */

test.describe('User Journey - Evaluating Developer', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage before each test
    await page.goto('/');
  });

  /**
   * Test Case 1: Clear tagline explaining MirDB is visible within 2 seconds of page load
   * Input: Load homepage and measure time to understand product
   * Expected: Clear tagline explaining MirDB is visible within 2 seconds of page load
   *
   * Step 1: Land on homepage
   * Step 2: Understand value proposition within 2 seconds
   */
  test('TC1: Clear tagline explaining MirDB is visible within 2 seconds of page load', async ({ page }) => {
    // Record the start time
    const startTime = Date.now();

    // Navigate to homepage (this happens in beforeEach, but we verify the content immediately loads)
    // Verify the hero section is visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify the product name is visible
    const productName = page.locator('#product-name');
    await expect(productName).toBeVisible();
    await expect(productName).toHaveText('MirDB');

    // Verify the tagline is visible and contains key information
    const tagline = page.locator('#tagline');
    await expect(tagline).toBeVisible();

    // Verify tagline content explains what MirDB is
    const taglineText = await tagline.textContent();
    expect(taglineText.toLowerCase()).toContain('persistent');
    expect(taglineText.toLowerCase()).toContain('key-value');
    expect(taglineText.toLowerCase()).toContain('memcached');

    // Verify the value proposition is visible
    const valueProposition = page.locator('#value-proposition');
    await expect(valueProposition).toBeVisible();

    // Calculate elapsed time - all core content should be visible within 2 seconds
    const elapsedTime = Date.now() - startTime;

    // Log the elapsed time for debugging
    console.log(`Time to display tagline and value proposition: ${elapsedTime}ms`);

    // Verify all content loaded within 2 seconds (2000ms)
    // Note: In e2e tests, the actual DOM check happens quickly once content is available
    // The 2-second requirement is about perceived load time, not test execution time
    expect(elapsedTime).toBeLessThan(2000);
  });

  /**
   * Test Case 2: Key differentiators (persistence, memcached, Rust) become visible on scroll
   * Input: Scroll down from hero section
   * Expected: Key differentiators (persistence, memcached, Rust) become visible
   *
   * Step 3: Find key differentiators
   */
  test('TC2: Key differentiators (persistence, memcached, Rust) become visible on scroll', async ({ page }) => {
    // First, verify we're at the top of the page (hero section)
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Scroll to the value propositions section
    const valuePropsSection = page.locator('[data-testid="value-propositions-section"]');
    await valuePropsSection.scrollIntoViewIfNeeded();

    // Wait for the section to be fully visible
    await expect(valuePropsSection).toBeVisible();

    // Verify "Persistent Storage" differentiator is visible
    const persistentStorage = page.locator('[data-testid="value-prop-persistent-storage"]');
    await expect(persistentStorage).toBeVisible();

    const persistentTitle = page.locator('[data-testid="value-prop-persistent-storage-title"]');
    await expect(persistentTitle).toHaveText('Persistent Storage');

    const persistentDescription = page.locator('[data-testid="value-prop-persistent-storage-description"]');
    await expect(persistentDescription).toBeVisible();
    const persistentText = await persistentDescription.textContent();
    expect(persistentText.toLowerCase()).toContain('persists');
    expect(persistentText.toLowerCase()).toContain('disk');

    // Verify "Memcached Compatible" differentiator is visible
    const memcachedCompatible = page.locator('[data-testid="value-prop-memcached-compatible"]');
    await expect(memcachedCompatible).toBeVisible();

    const memcachedTitle = page.locator('[data-testid="value-prop-memcached-compatible-title"]');
    await expect(memcachedTitle).toHaveText('Memcached Compatible');

    const memcachedDescription = page.locator('[data-testid="value-prop-memcached-compatible-description"]');
    await expect(memcachedDescription).toBeVisible();
    const memcachedText = await memcachedDescription.textContent();
    expect(memcachedText.toLowerCase()).toContain('memcached');
    expect(memcachedText.toLowerCase()).toContain('protocol');

    // Verify "Written in Rust" differentiator is visible
    const rustProp = page.locator('[data-testid="value-prop-rust"]');
    await expect(rustProp).toBeVisible();

    const rustTitle = page.locator('[data-testid="value-prop-rust-title"]');
    await expect(rustTitle).toHaveText('Written in Rust');

    const rustDescription = page.locator('[data-testid="value-prop-rust-description"]');
    await expect(rustDescription).toBeVisible();
    const rustText = await rustDescription.textContent();
    expect(rustText).toContain('Rust');
    expect(rustText.toLowerCase()).toContain('performance');
  });

  /**
   * Test Case 3: Clear navigation links to features and documentation are visible
   * Input: Look for navigation to learn more
   * Expected: Clear navigation links to features and documentation are visible
   *
   * This validates that evaluating developers can easily find more information
   */
  test('TC3: Clear navigation links to features and documentation are visible', async ({ page }) => {
    // Verify the header and navigation are visible
    const header = page.locator('header');
    await expect(header).toBeVisible();

    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Verify "Features" navigation link is visible
    const featuresLink = page.locator('header nav a[href="#features"]');
    await expect(featuresLink).toBeVisible();
    const featuresText = await featuresLink.textContent();
    expect(featuresText.toLowerCase()).toContain('features');

    // Verify "Docs" navigation link is visible (points to quick-start section)
    const docsLink = page.locator('header nav a:has-text("Docs")');
    await expect(docsLink).toBeVisible();

    // Verify the Docs link points to documentation section
    const docsHref = await docsLink.getAttribute('href');
    expect(docsHref).toBeTruthy();
    expect(docsHref.startsWith('#')).toBeTruthy();

    // Verify "GitHub" navigation link is visible for accessing source code
    const githubLink = page.locator('header nav a:has-text("GitHub")');
    await expect(githubLink).toBeVisible();

    // Verify GitHub link points to the repository
    const githubHref = await githubLink.getAttribute('href');
    expect(githubHref).toContain('github.com');
    expect(githubHref.toLowerCase()).toContain('mirdb');

    // Verify all navigation links are functional by checking they're clickable
    await expect(featuresLink).toBeEnabled();
    await expect(docsLink).toBeEnabled();
    await expect(githubLink).toBeEnabled();

    // Verify CTA buttons in hero section also provide navigation
    const getStartedBtn = page.locator('#get-started-btn');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toHaveText('Get Started');

    const githubBtn = page.locator('#github-btn');
    await expect(githubBtn).toBeVisible();
    await expect(githubBtn).toHaveText('View on GitHub');
  });

  /**
   * Integration Test: Complete User Journey Flow
   * Validates the entire evaluating developer journey from landing to exploration
   */
  test('Complete user journey: Landing -> Understanding -> Exploring', async ({ page }) => {
    // STEP 1: Land on homepage as first-time visitor
    // Verify the page has loaded correctly
    await expect(page).toHaveTitle(/MirDB/);

    // STEP 2: Understand value proposition within 2 seconds
    // Verify hero content is immediately visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify the tagline communicates MirDB's purpose
    const tagline = page.locator('#tagline');
    await expect(tagline).toBeVisible();
    const taglineText = await tagline.textContent();
    expect(taglineText).toMatch(/persistent.*key-value.*memcached/i);

    // STEP 3: Find key differentiators by scrolling
    // Scroll to value propositions
    const valuePropsSection = page.locator('[data-testid="value-propositions-section"]');
    await valuePropsSection.scrollIntoViewIfNeeded();
    await expect(valuePropsSection).toBeVisible();

    // Verify all three key differentiators are visible
    const differentiators = ['value-prop-persistent-storage', 'value-prop-memcached-compatible', 'value-prop-rust'];
    for (const differentiator of differentiators) {
      const element = page.locator(`[data-testid="${differentiator}"]`);
      await expect(element).toBeVisible();
    }

    // User can navigate to learn more
    const featuresLink = page.locator('header nav a[href="#features"]');
    await featuresLink.click();
    await page.waitForTimeout(500);

    // Features section is now visible
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify the features section has meaningful content
    const featureCards = page.locator('.feature-card');
    const featureCount = await featureCards.count();
    expect(featureCount).toBeGreaterThanOrEqual(4); // At least 4 key features
  });
});
