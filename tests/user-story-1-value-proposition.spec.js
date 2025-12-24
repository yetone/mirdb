// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for User Story 1 - Understand Product Value
 *
 * Scenario: Verify users can quickly understand MirDB's value proposition
 *
 * As Developer Dan, I want to quickly understand what MirDB does and how it
 * differs from memcached, so that I can decide if it's suitable for my project.
 *
 * Steps:
 * 1. Land on homepage
 * 2. View hero section
 * 3. Identify differentiator (persistence vs memcached in-memory)
 */

test.describe('User Story 1 - Understand Product Value', () => {
  test.beforeEach(async ({ page }) => {
    // Step 1: Land on homepage - Load the landing page as a new visitor
    await page.goto('/');
  });

  /**
   * Test Case 1: Read hero section tagline
   * Input: Read hero section tagline
   * Expected: Tagline clearly states MirDB is a persistent key-value store with memcached compatibility
   */
  test('TC1: Hero tagline clearly states MirDB is a persistent key-value store with memcached compatibility', async ({ page }) => {
    // Step 2: View hero section
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify hero section has a visible tagline
    const tagline = page.locator('#hero .tagline');
    await expect(tagline).toBeVisible();

    // Verify tagline content mentions "persistent" and "key-value store" and "memcached"
    const taglineText = await tagline.textContent();

    // Should contain "Persistent" (case insensitive check)
    expect(taglineText?.toLowerCase()).toContain('persistent');

    // Should contain "Key-Value Store" (case insensitive check)
    expect(taglineText?.toLowerCase()).toContain('key-value store');

    // Should contain "Memcached" (case insensitive check)
    expect(taglineText?.toLowerCase()).toContain('memcached');

    // Verify the exact expected tagline
    await expect(tagline).toHaveText('Persistent Key-Value Store with Memcached Protocol');
  });

  /**
   * Test Case 2: Check for persistence differentiator
   * Input: Check for persistence differentiator
   * Expected: Hero or features section highlights persistence as key differentiator from memcached
   */
  test('TC2: Persistence is highlighted as key differentiator from memcached', async ({ page }) => {
    // Check hero section description for persistence differentiator
    const heroDescription = page.locator('#hero .description');
    await expect(heroDescription).toBeVisible();

    const heroDescText = await heroDescription.textContent();

    // Hero description should mention persistence as differentiator
    expect(heroDescText?.toLowerCase()).toContain('persistent');

    // Should also reference memcached for comparison context
    expect(heroDescText?.toLowerCase()).toContain('memcached');

    // Alternative: check features section for explicit persistence mention
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the Persistent Storage feature card
    const persistentStorageCard = page.locator('#features .feature-card:has-text("Persistent Storage")');
    await expect(persistentStorageCard).toBeVisible();

    // Verify it mentions memcached comparison (unlike memcached, survives restarts)
    const persistentCardText = await persistentStorageCard.textContent();
    expect(persistentCardText?.toLowerCase()).toContain('memcached');
    expect(persistentCardText?.toLowerCase()).toContain('persist');
  });

  /**
   * Test Case 3: Assess time to understand value proposition (page structure test)
   * Input: Assess time to understand value proposition
   * Expected: Value proposition is clear within 10 seconds of page load
   *
   * Note: This is marked as "manual" type test in the scenario, but we can
   * partially validate it by checking that key elements are visible above the fold
   * and that the page loads quickly with critical content immediately visible.
   */
  test('TC3: Value proposition is clear within page load - above the fold verification', async ({ page }) => {
    // Set a desktop viewport to check above-the-fold content
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto('/');

    // Measure page load time - value proposition should be immediately visible
    const startTime = Date.now();

    // Wait for hero section to be visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    const loadTime = Date.now() - startTime;

    // Verify critical elements are visible immediately (without scrolling)
    const productName = page.locator('#hero h1');
    const tagline = page.locator('#hero .tagline');
    const description = page.locator('#hero .description');

    await expect(productName).toBeInViewport();
    await expect(tagline).toBeInViewport();
    await expect(description).toBeInViewport();

    // The product name should clearly identify this as MirDB
    await expect(productName).toHaveText('MirDB');

    // The tagline should communicate the core value proposition
    const taglineText = await tagline.textContent();
    expect(taglineText?.toLowerCase()).toContain('persistent');
    expect(taglineText?.toLowerCase()).toContain('memcached');

    // Description provides additional context
    const descText = await description.textContent();
    expect(descText?.toLowerCase()).toContain('rust');
    expect(descText?.toLowerCase()).toContain('lsm tree');

    // Page should load reasonably quickly (under 10 seconds threshold)
    // Note: In CI environments this may vary, so we use a generous threshold
    expect(loadTime).toBeLessThan(10000);
  });

  /**
   * Additional test: Hero section content is accessible
   * Ensures the value proposition can be understood by screen readers
   */
  test('Hero section content is accessible and properly structured', async ({ page }) => {
    // Hero section should have proper heading structure
    const h1 = page.locator('h1');
    await expect(h1).toBeVisible();
    await expect(h1).toHaveText('MirDB');

    // Tagline should be immediately after or near the h1
    const tagline = page.locator('#hero .tagline');
    await expect(tagline).toBeVisible();

    // Description should be present and readable
    const description = page.locator('#hero .description');
    await expect(description).toBeVisible();

    // Verify the text is not empty and has meaningful content
    const descText = await description.textContent();
    expect(descText?.length).toBeGreaterThan(50);
  });
});
