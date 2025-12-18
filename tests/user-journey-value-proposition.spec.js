// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

/**
 * User Journey - Understanding Product Value (US-1)
 *
 * Tests the complete user journey for understanding MirDB's value proposition:
 * 1. User lands on homepage
 * 2. User reads hero content and understands MirDB's purpose
 * 3. User identifies persistence as the key differentiator from memcached
 *
 * Acceptance Criteria:
 * - User should see a clear headline explaining MirDB's purpose
 * - Key differentiator (persistence) should be highlighted
 * - User should understand the value within 10 seconds of reading
 */

const pageUrl = 'file://' + path.join(__dirname, '..', 'index.html');

test.describe('User Journey - Understanding Product Value (US-1)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(pageUrl);
  });

  /**
   * Test Case 1: Complete user journey - land on page and identify purpose
   * Input: Complete user journey: land on page and identify purpose
   * Expected: User can understand MirDB is a persistent key-value store within 10 seconds
   */
  test('TC1: User can understand MirDB is a persistent key-value store from hero section', async ({ page }) => {
    // Step 1: User lands on homepage - verify page loads successfully
    await expect(page).toHaveTitle(/MirDB/);

    // Step 2: User reads hero content - hero section should be immediately visible
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Verify headline - should clearly show product name
    const headline = heroSection.locator('h1');
    await expect(headline).toBeVisible();
    await expect(headline).toHaveText('MirDB');

    // Verify tagline - should communicate core value proposition
    const tagline = heroSection.locator('.tagline');
    await expect(tagline).toBeVisible();
    const taglineText = await tagline.textContent();

    // Tagline should mention "Persistent Key-Value Store" to communicate purpose
    expect(taglineText).toContain('Persistent Key-Value Store');

    // Tagline should mention "Memcached Protocol" to show compatibility
    expect(taglineText).toContain('Memcached Protocol');

    // Verify description provides context
    const description = heroSection.locator('.description');
    await expect(description).toBeVisible();
    const descriptionText = await description.textContent();

    // Description should mention "drop-in replacement" for clarity
    expect(descriptionText?.toLowerCase()).toContain('drop-in replacement');

    // Description should mention durability/persistence
    expect(descriptionText?.toLowerCase()).toContain('durable');

    // Step 3: Verify CTAs are available for next steps
    const getStartedBtn = heroSection.locator('.btn-primary');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toHaveText('Get Started');

    const githubBtn = heroSection.locator('.btn-secondary');
    await expect(githubBtn).toBeVisible();
    await expect(githubBtn).toHaveText('View on GitHub');
  });

  /**
   * Test Case 2: Identify key differentiator from hero
   * Input: Identify key differentiator from hero
   * Expected: Persistence is clearly highlighted as differentiator from memcached
   */
  test('TC2: Persistence is clearly highlighted as differentiator from memcached', async ({ page }) => {
    // Navigate to hero section
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Check tagline emphasizes persistence
    const tagline = heroSection.locator('.tagline');
    const taglineText = await tagline.textContent();

    // "Persistent" should be the first differentiating word in the tagline
    expect(taglineText).toMatch(/Persistent/i);

    // Check description differentiates from memcached
    const description = heroSection.locator('.description');
    const descriptionText = await description.textContent();

    // Description mentions memcached as comparison point
    expect(descriptionText?.toLowerCase()).toContain('memcached');

    // Description mentions durable storage as differentiator
    expect(descriptionText?.toLowerCase()).toContain('durable storage');

    // Now verify the features section reinforces the persistence differentiator
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the persistent storage feature card
    const persistentCard = page.locator('[data-testid="feature-persistent"]');
    await expect(persistentCard).toBeVisible();

    // Verify title highlights persistence
    const persistentTitle = persistentCard.locator('h3');
    await expect(persistentTitle).toHaveText('Persistent Storage');

    // Verify description explicitly differentiates from memcached
    const persistentDescription = persistentCard.locator('p');
    const persistentDescText = await persistentDescription.textContent();

    // Should explicitly compare to memcached
    expect(persistentDescText?.toLowerCase()).toContain('unlike memcached');

    // Should mention data surviving restarts (key persistence benefit)
    expect(persistentDescText?.toLowerCase()).toContain('survives restarts');

    // Should mention SSTables for technical credibility
    expect(persistentDescText).toContain('SSTables');
  });

  /**
   * Additional test: Hero section is above the fold (immediately visible)
   * This ensures the user can understand the value within 10 seconds
   */
  test('TC3: Hero content is immediately visible without scrolling', async ({ page }) => {
    // Set standard viewport
    await page.setViewportSize({ width: 1280, height: 720 });

    // Hero section should be visible without scrolling
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Key elements should be in viewport
    const headline = heroSection.locator('h1');
    const tagline = heroSection.locator('.tagline');
    const description = heroSection.locator('.description');
    const ctaButtons = heroSection.locator('.cta-buttons');

    // All critical elements visible
    await expect(headline).toBeVisible();
    await expect(tagline).toBeVisible();
    await expect(description).toBeVisible();
    await expect(ctaButtons).toBeVisible();

    // Verify elements are actually in the viewport (above the fold)
    const headlineBox = await headline.boundingBox();
    const taglineBox = await tagline.boundingBox();
    const descriptionBox = await description.boundingBox();

    // All elements should have their top within the initial viewport
    expect(headlineBox?.y).toBeLessThan(720);
    expect(taglineBox?.y).toBeLessThan(720);
    expect(descriptionBox?.y).toBeLessThan(720);
  });

  /**
   * Test that the user journey flow is complete and coherent
   */
  test('TC4: Complete value proposition flow from hero to features', async ({ page }) => {
    // 1. Start at hero - get initial understanding
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // 2. Hero communicates: "Persistent Key-Value Store with Memcached Protocol"
    const tagline = await page.locator('.hero .tagline').textContent();
    expect(tagline).toContain('Persistent');
    expect(tagline).toContain('Key-Value Store');
    expect(tagline).toContain('Memcached');

    // 3. Features section reinforces the three main value propositions
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Three feature cards present
    const featureCards = featuresSection.locator('.feature-card');
    await expect(featureCards).toHaveCount(3);

    // Feature 1: Memcached Protocol Compatible
    const memcachedCard = page.locator('[data-testid="feature-memcached"]');
    await expect(memcachedCard).toBeVisible();
    await expect(memcachedCard.locator('h3')).toContainText('Memcached Protocol');

    // Feature 2: Persistent Storage (KEY DIFFERENTIATOR)
    const persistentCard = page.locator('[data-testid="feature-persistent"]');
    await expect(persistentCard).toBeVisible();
    await expect(persistentCard.locator('h3')).toContainText('Persistent Storage');

    // Feature 3: LSM Tree Architecture
    const lsmCard = page.locator('[data-testid="feature-lsm"]');
    await expect(lsmCard).toBeVisible();
    await expect(lsmCard.locator('h3')).toContainText('LSM Tree');

    // 4. User can proceed to Getting Started for more information
    const getStartedLink = page.locator('.hero .btn-primary');
    await expect(getStartedLink).toHaveAttribute('href', '#getting-started');
  });
});
