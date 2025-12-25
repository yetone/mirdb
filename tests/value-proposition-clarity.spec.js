// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

/**
 * Test Suite: Value Proposition Clarity
 *
 * This test suite validates that users can understand MirDB's value proposition
 * within 30 seconds of landing on the homepage. It verifies:
 * - Above-fold content communicates MirDB is a persistent key-value store
 * - Hero section prominently highlights persistence as key differentiator
 * - Comparison/differentiation from memcached/Redis highlighting persistence
 * - Rust implementation is mentioned for performance/safety messaging
 */

test.describe('Value Proposition Clarity', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage
    await page.goto('file://' + path.join(__dirname, '..', 'index.html'));
  });

  /**
   * Test Case 1: View first viewport without scrolling
   * Expected: User can understand MirDB is a persistent key-value store compatible with Memcached
   * Type: manual (automated as e2e)
   */
  test('TC1: Above-fold content clearly communicates value proposition', async ({ page }) => {
    // Verify the hero section is visible without scrolling
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Verify the tagline mentions both persistence and Memcached
    const tagline = page.locator('.tagline');
    await expect(tagline).toBeVisible();
    const taglineText = await tagline.textContent();
    expect(taglineText?.toLowerCase()).toContain('persistent');
    expect(taglineText?.toLowerCase()).toContain('memcached');

    // Verify the description reinforces the value proposition
    const description = page.locator('.hero .description');
    await expect(description).toBeVisible();
    const descriptionText = await description.textContent();

    // User should understand it's a persistent key-value store
    expect(descriptionText?.toLowerCase()).toContain('persistent');
    expect(
      descriptionText?.toLowerCase().includes('key-value') ||
      descriptionText?.toLowerCase().includes('key value')
    ).toBeTruthy();

    // User should understand Memcached compatibility
    expect(descriptionText?.toLowerCase()).toContain('memcached');
  });

  /**
   * Test Case 2: Check hero section messaging
   * Expected: Persistence benefit is prominently highlighted as key differentiator
   * Type: e2e
   */
  test('TC2: Hero section prominently highlights persistence as key differentiator', async ({ page }) => {
    // Check the hero section
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Verify tagline contains "Persistent" prominently
    const tagline = page.locator('.tagline');
    await expect(tagline).toBeVisible();
    const taglineText = await tagline.textContent();
    expect(taglineText).toContain('Persistent');

    // Verify description emphasizes durability/persistence
    const description = page.locator('.hero .description');
    await expect(description).toBeVisible();
    const descriptionText = await description.textContent();

    // Check for persistence-related keywords
    const persistenceKeywords = ['persistent', 'durable', 'storage', 'disk', 'survives'];
    const hasPersistenceKeyword = persistenceKeywords.some(keyword =>
      descriptionText?.toLowerCase().includes(keyword)
    );
    expect(hasPersistenceKeyword).toBeTruthy();
  });

  /**
   * Test Case 3: Look for comparison with alternatives
   * Expected: Comparison or differentiation from memcached/Redis highlighting persistence is present
   * Type: e2e
   */
  test('TC3: Comparison section differentiates from memcached/Redis with persistence focus', async ({ page }) => {
    // Look for comparison section - either a dedicated section or feature cards that differentiate
    const comparisonSection = page.locator('[data-testid="comparison-section"], .comparison, .comparison-section');
    const persistenceCard = page.locator('[data-testid="feature-card-persistence"]');

    // Either a comparison section or persistence feature card should exist
    const comparisonVisible = await comparisonSection.isVisible().catch(() => false);
    const persistenceCardVisible = await persistenceCard.isVisible().catch(() => false);

    expect(comparisonVisible || persistenceCardVisible).toBeTruthy();

    if (comparisonVisible) {
      // If comparison section exists, verify it highlights persistence
      const comparisonText = await comparisonSection.textContent();
      expect(comparisonText?.toLowerCase()).toContain('persist');

      // Should mention alternatives (memcached or Redis)
      expect(
        comparisonText?.toLowerCase().includes('memcached') ||
        comparisonText?.toLowerCase().includes('redis') ||
        comparisonText?.toLowerCase().includes('in-memory')
      ).toBeTruthy();
    }

    if (persistenceCardVisible) {
      // Verify persistence feature card differentiates from in-memory caches
      const persistenceText = await persistenceCard.textContent();

      // Should mention that data survives/persists unlike in-memory alternatives
      expect(
        persistenceText?.toLowerCase().includes('unlike') ||
        persistenceText?.toLowerCase().includes('survives') ||
        persistenceText?.toLowerCase().includes('in-memory') ||
        persistenceText?.toLowerCase().includes('disk')
      ).toBeTruthy();
    }
  });

  /**
   * Test Case 4: Check for Rust implementation mention
   * Expected: Rust implementation is mentioned for performance/safety messaging
   * Type: e2e
   */
  test('TC4: Rust implementation is mentioned for performance/safety messaging', async ({ page }) => {
    // Get all page content
    const bodyText = await page.locator('body').textContent();

    // Verify Rust is mentioned somewhere on the page
    expect(bodyText?.toLowerCase()).toContain('rust');

    // Check that Rust mention is associated with performance or safety
    const heroDescription = page.locator('.hero .description');
    const rustCard = page.locator('[data-testid="feature-card-async"]');

    let rustContextFound = false;

    // Check hero description for Rust mention
    if (await heroDescription.isVisible()) {
      const heroText = await heroDescription.textContent();
      if (heroText?.toLowerCase().includes('rust')) {
        // Verify it's mentioned with performance/safety context
        const performanceKeywords = ['performance', 'fast', 'speed', 'efficient', 'safety', 'memory', 'safe'];
        rustContextFound = performanceKeywords.some(keyword =>
          heroText?.toLowerCase().includes(keyword)
        );
      }
    }

    // Check async networking card for Rust context
    if (await rustCard.isVisible()) {
      const cardText = await rustCard.textContent();
      if (cardText?.toLowerCase().includes('rust')) {
        const performanceKeywords = ['performance', 'fast', 'async', 'safety', 'memory', 'zero-cost'];
        rustContextFound = rustContextFound || performanceKeywords.some(keyword =>
          cardText?.toLowerCase().includes(keyword)
        );
      }
    }

    // Also check for meta description containing Rust
    const metaDescription = await page.locator('meta[name="description"]').getAttribute('content');
    if (metaDescription?.toLowerCase().includes('rust')) {
      rustContextFound = true;
    }

    expect(rustContextFound).toBeTruthy();
  });

  /**
   * Additional test: Value proposition is visible within first viewport
   */
  test('TC5: Key messaging is visible without scrolling (above the fold)', async ({ page }) => {
    // Get viewport height
    const viewportHeight = page.viewportSize()?.height || 720;

    // Check that hero section is within the viewport
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Get hero section bounding box
    const heroBoundingBox = await heroSection.boundingBox();
    expect(heroBoundingBox).not.toBeNull();

    // Verify hero section starts within first viewport
    expect(heroBoundingBox.y).toBeLessThan(viewportHeight);

    // Verify tagline is visible in first viewport
    const tagline = page.locator('.tagline');
    const taglineBoundingBox = await tagline.boundingBox();
    expect(taglineBoundingBox).not.toBeNull();
    expect(taglineBoundingBox.y).toBeLessThan(viewportHeight);

    // Verify description is visible in first viewport
    const description = page.locator('.hero .description');
    const descriptionBoundingBox = await description.boundingBox();
    expect(descriptionBoundingBox).not.toBeNull();
    expect(descriptionBoundingBox.y).toBeLessThan(viewportHeight);
  });
});
