import { test, expect } from '@playwright/test';

/**
 * Value Proposition Communication Tests
 *
 * These tests verify that the homepage clearly communicates MirDB's value proposition
 * within 30 seconds (i.e., immediately upon page load through the hero section).
 *
 * Scenario: Value Proposition Communication
 * - Verify homepage communicates MirDB's value proposition clearly
 * - Check that persistent memcached alternative is clearly communicated
 * - Verify above-the-fold content conveys product purpose
 * - Verify clear problem-solution messaging
 */

test.describe('Value Proposition Communication', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Hero section clearly states MirDB is a persistent alternative to memcached', async ({ page }) => {
    // Test Case 1 (Manual converted to E2E): Hero clearly states MirDB is a persistent alternative to memcached
    // This test validates that within the hero section, the core value proposition is communicated

    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Get all text content from hero section
    const heroText = await heroSection.textContent();
    expect(heroText).not.toBeNull();

    const heroTextLower = heroText!.toLowerCase();

    // Verify that the hero section mentions MirDB (product name)
    expect(heroTextLower).toContain('mirdb');

    // Verify that 'persistent' concept is communicated
    expect(
      heroTextLower.includes('persistent') || heroTextLower.includes('persistence')
    ).toBe(true);

    // Verify that 'memcached' concept is communicated
    expect(
      heroTextLower.includes('memcached')
    ).toBe(true);

    // Verify the tagline specifically conveys the persistent + memcached value proposition
    const tagline = heroSection.locator('[data-testid="tagline"]');
    await expect(tagline).toBeVisible();
    const taglineText = await tagline.textContent();
    expect(taglineText).not.toBeNull();

    // Tagline should mention both persistent and memcached
    const taglineLower = taglineText!.toLowerCase();
    expect(taglineLower).toContain('persistent');
    expect(taglineLower).toContain('memcached');
  });

  test('TC2: Persistent keyword is prominent in hero/tagline area', async ({ page }) => {
    // Test Case 2: Word 'persistent' or 'persistence' appears in hero/tagline area

    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Check tagline first (most prominent)
    const tagline = heroSection.locator('[data-testid="tagline"]');
    await expect(tagline).toBeVisible();
    const taglineText = await tagline.textContent();

    // Check value proposition paragraph
    const valueProposition = heroSection.locator('.value-proposition');
    const hasValueProp = await valueProposition.count() > 0;
    let valuePropositionText = '';
    if (hasValueProp) {
      valuePropositionText = (await valueProposition.textContent()) || '';
    }

    // Combine texts and check for 'persistent' or 'persistence'
    const combinedText = `${taglineText} ${valuePropositionText}`.toLowerCase();
    const hasPersistent = combinedText.includes('persistent') || combinedText.includes('persistence');

    expect(hasPersistent).toBe(true);
  });

  test('TC3: Memcached keyword is prominent in hero/tagline area', async ({ page }) => {
    // Test Case 3: Word 'memcached' or 'Memcached' appears in hero/tagline area

    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Check tagline first (most prominent)
    const tagline = heroSection.locator('[data-testid="tagline"]');
    await expect(tagline).toBeVisible();
    const taglineText = await tagline.textContent();

    // Check value proposition paragraph
    const valueProposition = heroSection.locator('.value-proposition');
    const hasValueProp = await valueProposition.count() > 0;
    let valuePropositionText = '';
    if (hasValueProp) {
      valuePropositionText = (await valueProposition.textContent()) || '';
    }

    // Combine texts and check for 'memcached' (case-insensitive)
    const combinedText = `${taglineText} ${valuePropositionText}`.toLowerCase();
    const hasMemcached = combinedText.includes('memcached');

    expect(hasMemcached).toBe(true);
  });

  test('TC4: Key differentiators are above the fold on 1280x720 viewport', async ({ page }) => {
    // Test Case 4: Product name, tagline, and primary CTA visible without scrolling on 1280x720

    // Set viewport to standard desktop size (1280x720)
    await page.setViewportSize({ width: 1280, height: 720 });
    await page.goto('/');

    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // 1. Verify product name (MirDB) is visible without scrolling
    const productName = heroSection.locator('h1');
    await expect(productName).toBeVisible();
    await expect(productName).toContainText('MirDB');

    const productNameBox = await productName.boundingBox();
    expect(productNameBox).not.toBeNull();
    expect(productNameBox!.y).toBeGreaterThanOrEqual(0);
    expect(productNameBox!.y + productNameBox!.height).toBeLessThanOrEqual(720);

    // 2. Verify tagline is visible without scrolling
    const tagline = heroSection.locator('[data-testid="tagline"]');
    await expect(tagline).toBeVisible();

    const taglineBox = await tagline.boundingBox();
    expect(taglineBox).not.toBeNull();
    expect(taglineBox!.y).toBeGreaterThanOrEqual(0);
    expect(taglineBox!.y + taglineBox!.height).toBeLessThanOrEqual(720);

    // 3. Verify primary CTA (Get Started) is visible without scrolling
    const ctaButton = page.locator('[data-testid="cta-get-started"]');
    await expect(ctaButton).toBeVisible();

    const ctaBox = await ctaButton.boundingBox();
    expect(ctaBox).not.toBeNull();
    expect(ctaBox!.y).toBeGreaterThanOrEqual(0);
    expect(ctaBox!.y + ctaBox!.height).toBeLessThanOrEqual(720);

    // 4. Verify value proposition text is visible without scrolling
    const valueProposition = heroSection.locator('.value-proposition');
    const hasValueProp = await valueProposition.count() > 0;
    if (hasValueProp) {
      await expect(valueProposition).toBeVisible();
      const valueBox = await valueProposition.boundingBox();
      expect(valueBox).not.toBeNull();
      expect(valueBox!.y).toBeGreaterThanOrEqual(0);
      expect(valueBox!.y + valueBox!.height).toBeLessThanOrEqual(720);
    }

    // 5. Verify GitHub CTA is also visible
    const githubButton = page.locator('[data-testid="cta-github"]');
    await expect(githubButton).toBeVisible();

    const githubBox = await githubButton.boundingBox();
    expect(githubBox).not.toBeNull();
    expect(githubBox!.y).toBeGreaterThanOrEqual(0);
    expect(githubBox!.y + githubBox!.height).toBeLessThanOrEqual(720);
  });

  test('Value proposition can be understood within 30 seconds of page load', async ({ page }) => {
    // Additional test to verify key differentiators are immediately visible and readable

    const startTime = Date.now();

    // Page should load quickly
    await page.goto('/');

    // All value proposition elements should be visible immediately
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Product name visible
    await expect(heroSection.locator('h1')).toBeVisible();

    // Tagline visible
    await expect(heroSection.locator('[data-testid="tagline"]')).toBeVisible();

    // CTA visible
    await expect(page.locator('[data-testid="cta-get-started"]')).toBeVisible();

    const endTime = Date.now();
    const loadTime = endTime - startTime;

    // All key elements should be visible within 30 seconds (30000ms)
    // In practice, this should happen in under 3 seconds
    expect(loadTime).toBeLessThan(30000);
  });

  test('Clear problem-solution messaging is communicated', async ({ page }) => {
    // Verify the problem MirDB solves is clearly stated (persistence for memcached)

    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // The value proposition should communicate:
    // 1. The problem: memcached lacks persistence
    // 2. The solution: MirDB provides persistence with memcached compatibility

    const valueProposition = heroSection.locator('.value-proposition');
    const hasValueProp = await valueProposition.count() > 0;

    if (hasValueProp) {
      const valueText = await valueProposition.textContent();
      expect(valueText).not.toBeNull();
      const valueLower = valueText!.toLowerCase();

      // Should mention persistence or disk
      const mentionsPersistence = valueLower.includes('persist') || valueLower.includes('disk');
      expect(mentionsPersistence).toBe(true);

      // Should mention memcached compatibility/replacement
      const mentionsMemcached = valueLower.includes('memcached');
      expect(mentionsMemcached).toBe(true);
    }

    // Also check the tagline covers the basics
    const tagline = heroSection.locator('[data-testid="tagline"]');
    const taglineText = await tagline.textContent();
    expect(taglineText).not.toBeNull();

    // Tagline should communicate the core value proposition
    const taglineLower = taglineText!.toLowerCase();
    expect(taglineLower).toContain('persistent');
    expect(taglineLower).toContain('memcached');
  });
});
