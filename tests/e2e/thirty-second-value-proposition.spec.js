// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: 30-Second Value Proposition
 *
 * Verifies users can understand MirDB's value proposition within 30 seconds of landing.
 * Tests that critical information is visible above the fold without scrolling.
 *
 * Scenario ID: 25
 * Requirements: REQ-1, REQ-2
 */

test.describe('30-Second Value Proposition - Above the Fold Content', () => {

  test.beforeEach(async ({ page }) => {
    // Navigate to the landing page
    await page.goto('/');
    // Set a standard desktop viewport to test above-the-fold content
    await page.setViewportSize({ width: 1280, height: 800 });
  });

  /**
   * Test Case 1: Check above-the-fold content
   * Input: Check above-the-fold content
   * Expected: Product name, tagline, and key value proposition visible without scrolling on desktop
   */
  test('should display product name, tagline, and value proposition above the fold on desktop', async ({ page }) => {
    // Get the viewport height
    const viewportHeight = 800;

    // Check that the h1 with product name is visible
    const productName = page.locator('h1');
    await expect(productName).toBeVisible();
    await expect(productName).toContainText('MirDB');

    // Verify product name is above the fold (within viewport)
    const h1BoundingBox = await productName.boundingBox();
    expect(h1BoundingBox).not.toBeNull();
    expect(h1BoundingBox.y + h1BoundingBox.height).toBeLessThan(viewportHeight);

    // Check that the tagline is visible above the fold
    const tagline = page.locator('.tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Persistent Key-Value Store with Memcached Protocol');

    // Verify tagline is above the fold
    const taglineBoundingBox = await tagline.boundingBox();
    expect(taglineBoundingBox).not.toBeNull();
    expect(taglineBoundingBox.y + taglineBoundingBox.height).toBeLessThan(viewportHeight);

    // Check that the value proposition description is visible above the fold
    const description = page.locator('.hero .description');
    await expect(description).toBeVisible();

    // Verify description is above the fold
    const descBoundingBox = await description.boundingBox();
    expect(descBoundingBox).not.toBeNull();
    expect(descBoundingBox.y + descBoundingBox.height).toBeLessThan(viewportHeight);

    // Verify the description contains key value proposition elements
    const descText = await description.textContent();
    expect(descText.toLowerCase()).toContain('memcached');
    expect(descText.toLowerCase()).toContain('persist');
  });

  /**
   * Test Case 2: Verify clear messaging
   * Input: Verify clear messaging
   * Expected: Hero section clearly communicates: persistent storage + Memcached compatibility
   * Type: manual (but we verify programmatically for completeness)
   */
  test('should clearly communicate persistent storage and Memcached compatibility in hero', async ({ page }) => {
    // Get the hero section
    const heroSection = page.locator('section.hero, .hero, [data-testid="hero"]').first();
    await expect(heroSection).toBeVisible();

    // Get all text content from the hero section
    const heroText = await heroSection.textContent();
    const heroTextLower = heroText.toLowerCase();

    // Verify clear messaging about persistent storage
    const hasPersistentStorage =
      heroTextLower.includes('persistent') ||
      heroTextLower.includes('persists') ||
      heroTextLower.includes('persist');
    expect(hasPersistentStorage).toBeTruthy();

    // Verify clear messaging about Memcached compatibility
    const hasMemcachedCompatibility =
      heroTextLower.includes('memcached') &&
      (heroTextLower.includes('protocol') ||
       heroTextLower.includes('compatible') ||
       heroTextLower.includes('replacement'));
    expect(hasMemcachedCompatibility).toBeTruthy();

    // Verify both concepts appear in close proximity (within hero section)
    // This ensures the value proposition is cohesive
    expect(heroTextLower).toMatch(/memcached/);
    expect(heroTextLower).toMatch(/persist/);

    // Verify the tagline specifically mentions both key concepts
    const tagline = page.locator('.tagline');
    const taglineText = await tagline.textContent();
    expect(taglineText.toLowerCase()).toContain('persistent');
    expect(taglineText.toLowerCase()).toContain('memcached');
  });

  /**
   * Test Case 3: Check CTA visibility
   * Input: Check CTA visibility
   * Expected: Primary call-to-action is visible above the fold
   */
  test('should have primary CTA visible above the fold', async ({ page }) => {
    const viewportHeight = 800;

    // Look for the primary CTA button
    const primaryCTA = page.locator('.cta-primary, [data-testid="cta"]').first();
    await expect(primaryCTA).toBeVisible();

    // Verify CTA is above the fold
    const ctaBoundingBox = await primaryCTA.boundingBox();
    expect(ctaBoundingBox).not.toBeNull();
    expect(ctaBoundingBox.y + ctaBoundingBox.height).toBeLessThan(viewportHeight);

    // Verify CTA has meaningful text
    const ctaText = await primaryCTA.textContent();
    const validCTATexts = ['get started', 'view on github', 'github', 'documentation', 'learn more', 'try it'];
    const hasValidCTAText = validCTATexts.some(text => ctaText.toLowerCase().includes(text));
    expect(hasValidCTAText).toBeTruthy();

    // Verify CTA is clickable (has href for link)
    const tagName = await primaryCTA.evaluate(el => el.tagName.toLowerCase());
    if (tagName === 'a') {
      const href = await primaryCTA.getAttribute('href');
      expect(href).toBeTruthy();
    } else {
      await expect(primaryCTA).toBeEnabled();
    }

    // Additionally, verify any secondary CTA is also above the fold
    const secondaryCTA = page.locator('.cta-secondary').first();
    if (await secondaryCTA.count() > 0) {
      await expect(secondaryCTA).toBeVisible();
      const secondaryCtaBoundingBox = await secondaryCTA.boundingBox();
      expect(secondaryCtaBoundingBox).not.toBeNull();
      expect(secondaryCtaBoundingBox.y + secondaryCtaBoundingBox.height).toBeLessThan(viewportHeight);
    }
  });

  /**
   * Additional test: Verify content hierarchy enables quick comprehension
   * This ensures the information architecture supports 30-second understanding
   */
  test('should have proper content hierarchy for quick comprehension', async ({ page }) => {
    // Verify h1 comes before description
    const h1 = page.locator('h1');
    const tagline = page.locator('.tagline');
    const description = page.locator('.hero .description');

    const h1Box = await h1.boundingBox();
    const taglineBox = await tagline.boundingBox();
    const descBox = await description.boundingBox();

    // H1 should be at the top
    expect(h1Box.y).toBeLessThan(taglineBox.y);
    // Tagline should come before description
    expect(taglineBox.y).toBeLessThan(descBox.y);

    // Verify the hero section uses semantic HTML for accessibility
    const heroSection = page.locator('section.hero');
    await expect(heroSection).toBeVisible();
  });

  /**
   * Test with common desktop viewport sizes
   */
  test('should show key content above fold on various desktop viewport sizes', async ({ page }) => {
    const desktopViewports = [
      { width: 1920, height: 1080, name: 'Full HD' },
      { width: 1366, height: 768, name: 'Common Laptop' },
      { width: 1280, height: 800, name: 'Standard Desktop' }
    ];

    for (const viewport of desktopViewports) {
      await page.setViewportSize({ width: viewport.width, height: viewport.height });

      // Check that core elements are visible above the fold
      const h1 = page.locator('h1');
      const tagline = page.locator('.tagline');
      const cta = page.locator('.cta-primary').first();

      await expect(h1).toBeVisible();
      await expect(tagline).toBeVisible();
      await expect(cta).toBeVisible();

      // Verify they're within viewport
      const h1Box = await h1.boundingBox();
      const taglineBox = await tagline.boundingBox();
      const ctaBox = await cta.boundingBox();

      expect(h1Box.y + h1Box.height).toBeLessThan(viewport.height);
      expect(taglineBox.y + taglineBox.height).toBeLessThan(viewport.height);
      expect(ctaBox.y + ctaBox.height).toBeLessThan(viewport.height);
    }
  });
});
