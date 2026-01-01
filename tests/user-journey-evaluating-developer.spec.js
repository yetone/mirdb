const { test, expect } = require('@playwright/test');

/**
 * User Journey - Evaluating Developer (US-1)
 *
 * Scenario: Verify an evaluating developer can quickly understand what MirDB does
 *
 * Steps:
 * 1. Land on homepage - First-time visitor arrives at the landing page
 * 2. View hero section - Read tagline and value proposition
 * 3. Scroll to features - Review key differentiators
 */
test.describe('User Journey - Evaluating Developer (US-1)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Time to understand product purpose from hero section
   * Input: Time to understand product purpose from hero section
   * Expected: Clear tagline and value proposition visible within 5 seconds of landing
   */
  test('TC1: Clear tagline and value proposition visible within 5 seconds of landing', async ({ page }) => {
    // Start timing from page load
    const startTime = Date.now();

    // Step 1: Land on homepage - First-time visitor arrives at the landing page
    // Wait for hero section to be visible
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Step 2: View hero section - Read tagline and value proposition
    // Verify product name "MirDB" is prominently displayed
    const productName = page.locator('.hero h1');
    await expect(productName).toBeVisible();
    await expect(productName).toHaveText('MirDB');

    // Verify tagline clearly communicates the product purpose
    const tagline = page.locator('.hero .tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Persistent Key-Value Store');
    await expect(tagline).toContainText('Memcached Protocol');

    // Verify CTAs are visible to guide the user
    const getStartedBtn = page.locator('.hero-buttons .btn-primary');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toContainText('Get Started');

    const githubBtn = page.locator('.hero-buttons .btn-secondary');
    await expect(githubBtn).toBeVisible();
    await expect(githubBtn).toContainText('GitHub');

    // Verify all key elements loaded within 5 seconds
    const loadTime = Date.now() - startTime;
    expect(loadTime).toBeLessThan(5000);
  });

  /**
   * Test Case 2: Can user identify three key features quickly?
   * Input: Can user identify three key features quickly?
   * Expected: Key features (memcached compat, persistence, performance) are easily identifiable
   */
  test('TC2: Key features (memcached compat, persistence, performance) are easily identifiable', async ({ page }) => {
    // Step 3: Scroll to features - Review key differentiators
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Verify features section is visible
    await expect(featuresSection).toBeVisible();

    // Verify section title indicates this is about MirDB's differentiators
    const sectionTitle = featuresSection.locator('.section-title');
    await expect(sectionTitle).toHaveText('Why MirDB?');

    // Verify features grid contains the three key feature cards
    const featuresGrid = page.locator('[data-testid="features-grid"]');
    await expect(featuresGrid).toBeVisible();

    // Feature 1: Memcached Compatibility
    const memcachedFeature = page.locator('[data-testid="feature-memcached"]');
    await expect(memcachedFeature).toBeVisible();
    const memcachedTitle = memcachedFeature.locator('h3');
    await expect(memcachedTitle).toContainText('Memcached');
    const memcachedDesc = memcachedFeature.locator('p');
    await expect(memcachedDesc).toContainText('protocol');
    await expect(memcachedDesc).toContainText('SET');
    await expect(memcachedDesc).toContainText('GET');

    // Feature 2: Data Persistence
    const persistenceFeature = page.locator('[data-testid="feature-persistence"]');
    await expect(persistenceFeature).toBeVisible();
    const persistenceTitle = persistenceFeature.locator('h3');
    await expect(persistenceTitle).toContainText('Persistence');
    const persistenceDesc = persistenceFeature.locator('p');
    await expect(persistenceDesc).toContainText('survives restarts');

    // Feature 3: High Performance
    const performanceFeature = page.locator('[data-testid="feature-performance"]');
    await expect(performanceFeature).toBeVisible();
    const performanceTitle = performanceFeature.locator('h3');
    await expect(performanceTitle).toContainText('Performance');
    const performanceDesc = performanceFeature.locator('p');
    await expect(performanceDesc).toContainText('LSM tree');

    // Verify all three features are easily identifiable (total count >= 3)
    const featureCards = featuresGrid.locator('.feature-card');
    const count = await featureCards.count();
    expect(count).toBeGreaterThanOrEqual(3);
  });

  /**
   * Test Case 3: Is the persistence advantage clear vs standard memcached?
   * Input: Is the persistence advantage clear vs standard memcached?
   * Expected: Persistence is highlighted as key differentiator from standard memcached
   */
  test('TC3: Persistence is highlighted as key differentiator from standard memcached', async ({ page }) => {
    // Step 3: Scroll to features - Review key differentiators
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Locate the persistence feature card
    const persistenceFeature = page.locator('[data-testid="feature-persistence"]');
    await expect(persistenceFeature).toBeVisible();

    // Verify title clearly states "Data Persistence"
    const persistenceTitle = persistenceFeature.locator('h3');
    await expect(persistenceTitle).toHaveText('Data Persistence');

    // Verify description explicitly contrasts with standard memcached
    const persistenceDesc = persistenceFeature.locator('p');
    await expect(persistenceDesc).toContainText('Unlike standard memcached');

    // Verify key persistence benefits are highlighted
    await expect(persistenceDesc).toContainText('survives restarts');
    await expect(persistenceDesc).toContainText('Write-Ahead Log');
    await expect(persistenceDesc).toContainText('durability');

    // Additional check: Verify the persistence feature explains the technology
    await expect(persistenceDesc).toContainText('SSTable');
  });

  /**
   * Full User Journey Test: Evaluating Developer Flow
   * This test simulates the complete user journey as described in the scenario steps
   */
  test('Full User Journey: Developer can understand MirDB within 5 seconds and identify key differentiators', async ({ page }) => {
    const startTime = Date.now();

    // Step 1: Land on homepage - First-time visitor arrives at the landing page
    // (Context: Developer is researching key-value stores)
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Step 2: View hero section - Read tagline and value proposition
    // (Context: Should understand MirDB's purpose within 5 seconds)
    const productName = page.locator('.hero h1');
    await expect(productName).toHaveText('MirDB');

    const tagline = page.locator('.hero .tagline');
    await expect(tagline).toContainText('Persistent Key-Value Store');
    await expect(tagline).toContainText('Memcached Protocol');

    // Verify understanding is possible within 5 seconds
    const heroLoadTime = Date.now() - startTime;
    expect(heroLoadTime).toBeLessThan(5000);

    // Step 3: Scroll to features - Review key differentiators
    // (Context: Should understand persistence advantage over memcached)
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Verify all three key differentiators are present
    await expect(page.locator('[data-testid="feature-memcached"]')).toBeVisible();
    await expect(page.locator('[data-testid="feature-persistence"]')).toBeVisible();
    await expect(page.locator('[data-testid="feature-performance"]')).toBeVisible();

    // Verify persistence advantage is clear
    const persistenceDesc = page.locator('[data-testid="feature-persistence"] p');
    await expect(persistenceDesc).toContainText('Unlike standard memcached');

    // Complete journey success - evaluating developer can understand MirDB's value
  });
});
