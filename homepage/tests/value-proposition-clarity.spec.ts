import { test, expect } from '@playwright/test';

/**
 * Value Proposition Clarity Tests
 *
 * Scenario: Verify users can understand MirDB's value proposition within 30 seconds of landing
 *
 * This test suite validates that:
 * 1. Users can quickly understand what MirDB is and its main benefit
 * 2. Tagline clearly mentions persistence capability
 * 3. Tagline clearly mentions Memcached compatibility
 * 4. Hero content is fully visible above the fold on 1080p displays
 * 5. Users understand MirDB is for key-value data storage
 */

test.describe('Value Proposition Clarity - Scenario 15', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('file://' + process.cwd() + '/public/index.html');
  });

  /**
   * Test Case 1 (Manual Test - Verified via content inspection)
   * Input: View hero section for 30 seconds
   * Expected: User can explain what MirDB is and its main benefit
   *
   * This test verifies that all necessary content elements are present
   * for a user to understand MirDB's value proposition within 30 seconds
   */
  test('TC1: Hero section contains all elements needed to understand MirDB value proposition', async ({ page }) => {
    // Verify hero section is visible
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Check product name is prominently displayed
    const productName = heroSection.locator('h1');
    await expect(productName).toBeVisible();
    await expect(productName).toContainText('MirDB');

    // Check tagline is visible and communicates the core value
    const tagline = page.getByTestId('tagline');
    await expect(tagline).toBeVisible();

    // Check description provides additional context
    const description = heroSection.locator('.hero-description');
    await expect(description).toBeVisible();

    // Verify description explains the main benefit (drop-in replacement with persistence)
    const descriptionText = await description.textContent();
    expect(descriptionText?.toLowerCase()).toContain('drop-in replacement');
    expect(descriptionText?.toLowerCase()).toContain('persistence');

    // Verify CTA is available for users to take action
    const cta = page.getByTestId('cta-button');
    await expect(cta).toBeVisible();

    // Verify key differentiators are mentioned in hero content
    const heroContent = await heroSection.textContent();
    expect(heroContent?.toLowerCase()).toMatch(/memcached/i);
    expect(heroContent?.toLowerCase()).toMatch(/persist/i);
    expect(heroContent?.toLowerCase()).toMatch(/key-value|key value/i);
  });

  /**
   * Test Case 2 (E2E)
   * Input: Check tagline mentions persistence
   * Expected: Tagline or hero content mentions persistent storage capability
   */
  test('TC2: Tagline mentions persistent storage capability', async ({ page }) => {
    // Get the tagline element
    const tagline = page.getByTestId('tagline');
    await expect(tagline).toBeVisible();

    // Get tagline text
    const taglineText = await tagline.textContent();

    // Verify persistence is mentioned in the tagline
    expect(taglineText?.toLowerCase()).toMatch(/persistent/i);

    // Also verify in hero description for completeness
    const heroDescription = page.locator('.hero-description');
    const descriptionText = await heroDescription.textContent();
    expect(descriptionText?.toLowerCase()).toMatch(/persist/i);
  });

  /**
   * Test Case 3 (E2E)
   * Input: Check tagline mentions Memcached compatibility
   * Expected: Tagline or hero content mentions Memcached protocol compatibility
   */
  test('TC3: Tagline mentions Memcached protocol compatibility', async ({ page }) => {
    // Get the tagline element
    const tagline = page.getByTestId('tagline');
    await expect(tagline).toBeVisible();

    // Get tagline text
    const taglineText = await tagline.textContent();

    // Verify Memcached compatibility is mentioned in the tagline
    expect(taglineText?.toLowerCase()).toMatch(/memcached.*compatible|memcached-compatible/i);

    // Also check for protocol mention in hero description
    const heroDescription = page.locator('.hero-description');
    const descriptionText = await heroDescription.textContent();
    // Description should mention Memcached as a drop-in replacement
    expect(descriptionText?.toLowerCase()).toMatch(/memcached/i);
  });

  /**
   * Test Case 4 (E2E)
   * Input: Verify hero content is above the fold
   * Expected: Complete hero section visible without scrolling on 1080p display
   */
  test('TC4: Hero section is fully visible above the fold on 1080p display', async ({ page }) => {
    // Set viewport to standard 1080p dimensions (1920x1080)
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.goto('file://' + process.cwd() + '/public/index.html');

    // Get the hero section
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Get hero section bounding box
    const boundingBox = await heroSection.boundingBox();
    expect(boundingBox).toBeTruthy();

    // Verify the entire hero section fits within the 1080p viewport
    // Account for navbar height (approximately 60-80px)
    const viewportHeight = 1080;

    // The hero section should start after the navbar
    expect(boundingBox!.y).toBeGreaterThanOrEqual(0);

    // The bottom of the hero section should be within the viewport
    // This ensures the complete hero content is visible without scrolling
    expect(boundingBox!.y + boundingBox!.height).toBeLessThanOrEqual(viewportHeight);

    // Verify all key hero elements are within the viewport
    const heroContent = page.locator('.hero-content');
    const heroContentBox = await heroContent.boundingBox();
    expect(heroContentBox).toBeTruthy();
    expect(heroContentBox!.y + heroContentBox!.height).toBeLessThanOrEqual(viewportHeight);

    // Verify CTA buttons are visible without scrolling
    const ctaGroup = page.locator('.hero-cta-group');
    const ctaBox = await ctaGroup.boundingBox();
    expect(ctaBox).toBeTruthy();
    expect(ctaBox!.y + ctaBox!.height).toBeLessThanOrEqual(viewportHeight);
  });

  /**
   * Test Case 5 (Manual Test - Verified via content inspection)
   * Input: Check for clear use case indication
   * Expected: User understands MirDB is for key-value data storage
   */
  test('TC5: Hero content clearly indicates MirDB is for key-value data storage', async ({ page }) => {
    // Get hero section content
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Get tagline
    const tagline = page.getByTestId('tagline');
    const taglineText = await tagline.textContent();

    // Verify key-value store is mentioned in tagline
    expect(taglineText?.toLowerCase()).toMatch(/key-value|key value/i);

    // Verify "store" is mentioned to clarify it's a data storage solution
    expect(taglineText?.toLowerCase()).toMatch(/store/i);

    // Verify the description provides additional context about data storage
    const description = page.locator('.hero-description');
    const descriptionText = await description.textContent();

    // Should mention storage-related concepts
    expect(descriptionText?.toLowerCase()).toMatch(/storage|data|store/i);

    // The full tagline should clearly communicate: "Persistent Memcached-Compatible Key-Value Store"
    expect(taglineText).toContain('Key-Value Store');
  });

  /**
   * Additional test: Verify value proposition elements have good visual hierarchy
   * This ensures users can quickly scan and understand the content
   */
  test('TC-EXTRA: Value proposition elements have proper visual hierarchy', async ({ page }) => {
    // Verify h1 (product name) is the most prominent element
    const h1 = page.locator('.hero-content h1');
    await expect(h1).toBeVisible();

    // Verify tagline (h2) follows the product name
    const tagline = page.locator('.hero-content h2.tagline');
    await expect(tagline).toBeVisible();

    // Verify description comes after tagline
    const description = page.locator('.hero-content .hero-description');
    await expect(description).toBeVisible();

    // Verify CTA buttons are at the bottom of the hero content
    const ctaGroup = page.locator('.hero-content .hero-cta-group');
    await expect(ctaGroup).toBeVisible();

    // Verify the reading order makes sense (top to bottom)
    const h1Box = await h1.boundingBox();
    const taglineBox = await tagline.boundingBox();
    const descriptionBox = await description.boundingBox();
    const ctaBox = await ctaGroup.boundingBox();

    expect(h1Box).toBeTruthy();
    expect(taglineBox).toBeTruthy();
    expect(descriptionBox).toBeTruthy();
    expect(ctaBox).toBeTruthy();

    // Elements should be in vertical order: h1 -> tagline -> description -> CTA
    expect(h1Box!.y).toBeLessThan(taglineBox!.y);
    expect(taglineBox!.y).toBeLessThan(descriptionBox!.y);
    expect(descriptionBox!.y).toBeLessThan(ctaBox!.y);
  });

  /**
   * Test key differentiators visibility (from scenario step 3)
   * Verify that persistence and Memcached compatibility are immediately apparent
   */
  test('TC-EXTRA: Key differentiators (persistence and Memcached) are immediately apparent', async ({ page }) => {
    // Set a typical viewport
    await page.setViewportSize({ width: 1280, height: 720 });
    await page.goto('file://' + process.cwd() + '/public/index.html');

    // All key differentiators should be visible in hero section without scrolling
    const heroSection = page.locator('[data-testid="hero-section"]');
    const heroText = await heroSection.textContent();

    // Key differentiator 1: Persistence
    expect(heroText?.toLowerCase()).toMatch(/persistent|persistence|survives restarts/i);

    // Key differentiator 2: Memcached compatibility
    expect(heroText?.toLowerCase()).toMatch(/memcached.*compatible|memcached-compatible|drop-in replacement/i);

    // Verify these are mentioned early in the page (in hero, not buried below)
    const tagline = page.getByTestId('tagline');
    const taglineText = await tagline.textContent();

    // Both key differentiators should be in the tagline for immediate visibility
    expect(taglineText?.toLowerCase()).toContain('persistent');
    expect(taglineText?.toLowerCase()).toContain('memcached');
  });
});
