/**
 * Value Proposition Communication E2E Tests
 * Owner: Scenario 16 - Value Proposition Communication
 *
 * Tests:
 * - "persistent" keyword in hero
 * - "Memcached" protocol mention
 * - "key-value" identification
 * - Above-fold content completeness
 *
 * Success Metric: Clear within 10 seconds
 */

const { test, expect } = require('@playwright/test');

test.describe('Value Proposition Communication', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Hero section contains the word "persistent" prominently', async ({ page }) => {
    // Verify hero section exists
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Get all text content from hero section
    const heroText = await heroSection.textContent();

    // Check for "persistent" keyword (case-insensitive)
    expect(heroText.toLowerCase()).toContain('persistent');

    // Verify "persistent" appears in prominent areas (tagline or description)
    const tagline = heroSection.locator('.hero-tagline');
    const description = heroSection.locator('.hero-description');

    const taglineText = await tagline.textContent();
    const descriptionText = await description.textContent();

    // At least one of tagline or description should contain "persistent"
    const hasInTagline = taglineText.toLowerCase().includes('persistent');
    const hasInDescription = descriptionText.toLowerCase().includes('persistent');

    expect(hasInTagline || hasInDescription).toBeTruthy();

    // Verify the element containing "persistent" is visible
    if (hasInTagline) {
      await expect(tagline).toBeVisible();
    }
    if (hasInDescription) {
      await expect(description).toBeVisible();
    }
  });

  test('Test Case 2: Hero section mentions Memcached protocol compatibility', async ({ page }) => {
    // Verify hero section exists
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Get all text content from hero section
    const heroText = await heroSection.textContent();

    // Check for "Memcached" or "memcached" keyword
    const hasMemcached = heroText.toLowerCase().includes('memcached');
    expect(hasMemcached).toBeTruthy();

    // Verify "Memcached" appears in prominent areas (tagline or description)
    const tagline = heroSection.locator('.hero-tagline');
    const description = heroSection.locator('.hero-description');

    const taglineText = await tagline.textContent();
    const descriptionText = await description.textContent();

    // At least one of tagline or description should mention Memcached
    const hasInTagline = taglineText.toLowerCase().includes('memcached');
    const hasInDescription = descriptionText.toLowerCase().includes('memcached');

    expect(hasInTagline || hasInDescription).toBeTruthy();

    // Verify that the mention indicates protocol compatibility
    expect(
      heroText.toLowerCase().includes('memcached protocol') ||
      heroText.toLowerCase().includes('memcached compatibility') ||
      heroText.toLowerCase().includes('memcached simplicity')
    ).toBeTruthy();
  });

  test('Test Case 3: Hero section identifies MirDB as a key-value store', async ({ page }) => {
    // Verify hero section exists
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Get all text content from hero section
    const heroText = await heroSection.textContent();

    // Check for "key-value" keyword (with or without hyphen)
    const hasKeyValue = heroText.toLowerCase().includes('key-value') ||
                        heroText.toLowerCase().includes('keyvalue') ||
                        heroText.toLowerCase().includes('key value');
    expect(hasKeyValue).toBeTruthy();

    // Verify "key-value" appears in prominent areas (tagline or description)
    const tagline = heroSection.locator('.hero-tagline');
    const description = heroSection.locator('.hero-description');

    const taglineText = await tagline.textContent();
    const descriptionText = await description.textContent();

    // At least one of tagline or description should identify it as key-value store
    const hasInTagline = taglineText.toLowerCase().includes('key-value') ||
                         taglineText.toLowerCase().includes('key value');
    const hasInDescription = descriptionText.toLowerCase().includes('key-value') ||
                             descriptionText.toLowerCase().includes('key value');

    expect(hasInTagline || hasInDescription).toBeTruthy();
  });

  test('Test Case 4: Above-fold content completeness - project name, tagline, and CTAs visible without scrolling on desktop', async ({ page }) => {
    // Set desktop viewport (1280x800)
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto('/');

    // Wait for page to fully render
    await page.waitForLoadState('networkidle');

    // Verify hero section is visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // 1. Verify project name (MirDB) is visible in viewport
    const h1 = heroSection.locator('h1');
    await expect(h1).toBeVisible();
    await expect(h1).toContainText('MirDB');
    await expect(h1).toBeInViewport();

    // 2. Verify tagline is visible in viewport
    const tagline = heroSection.locator('.hero-tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toBeInViewport();

    // 3. Verify description is visible in viewport
    const description = heroSection.locator('.hero-description');
    await expect(description).toBeVisible();
    await expect(description).toBeInViewport();

    // 4. Verify CTA buttons are visible in viewport
    const getStartedBtn = heroSection.locator('[data-testid="cta-get-started"]');
    const githubBtn = heroSection.locator('[data-testid="cta-github"]');

    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toBeInViewport();
    await expect(githubBtn).toBeVisible();
    await expect(githubBtn).toBeInViewport();

    // Verify all above-fold content can be read without scrolling
    // Check that scroll position is at top
    const scrollPosition = await page.evaluate(() => window.scrollY);
    expect(scrollPosition).toBe(0);

    // Verify all elements are fully above the fold (within viewport height)
    const viewportHeight = 800;

    const ctaBox = await getStartedBtn.boundingBox();
    expect(ctaBox.y + ctaBox.height).toBeLessThan(viewportHeight);
  });

  test('Value proposition elements are positioned prominently at the top', async ({ page }) => {
    const heroSection = page.locator('#hero');

    // Hero section should be at the top of the page content (accounting for header)
    const boundingBox = await heroSection.boundingBox();
    expect(boundingBox).not.toBeNull();

    // Hero should start within the first 150px (after header)
    expect(boundingBox.y).toBeLessThan(150);
  });

  test('Value proposition can be understood within 10 seconds - all key elements immediately visible', async ({ page }) => {
    // This test verifies that all key value proposition elements are immediately
    // visible without any interaction or waiting, supporting the 10-second rule

    // Navigate and wait for page to be fully loaded
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Immediately check visibility of all key elements (no waiting for animations)
    const heroSection = page.locator('#hero');
    const h1 = heroSection.locator('h1');
    const tagline = heroSection.locator('.hero-tagline');
    const description = heroSection.locator('.hero-description');

    // All should be visible without any delays
    await expect(heroSection).toBeVisible({ timeout: 1000 });
    await expect(h1).toBeVisible({ timeout: 1000 });
    await expect(tagline).toBeVisible({ timeout: 1000 });
    await expect(description).toBeVisible({ timeout: 1000 });

    // Verify all three key differentiators are present in hero text
    const heroText = await heroSection.textContent();
    const heroTextLower = heroText.toLowerCase();

    expect(heroTextLower).toContain('persistent');
    expect(heroTextLower).toContain('memcached');
    expect(heroTextLower).toContain('key-value');
  });

  test('Core differentiators are prominently displayed in hero section', async ({ page }) => {
    // Verify all core differentiators mentioned in the PRD are present:
    // - persistent key-value store
    // - Memcached protocol compatibility

    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    const heroText = await heroSection.textContent();
    const heroTextLower = heroText.toLowerCase();

    // Check for the complete value proposition phrase or its components
    const hasPersistentKVStore =
      (heroTextLower.includes('persistent') && heroTextLower.includes('key-value')) ||
      heroTextLower.includes('persistent key-value store');

    const hasMemcachedCompat =
      heroTextLower.includes('memcached protocol') ||
      heroTextLower.includes('memcached compatibility') ||
      heroTextLower.includes('memcached simplicity');

    expect(hasPersistentKVStore).toBeTruthy();
    expect(hasMemcachedCompat).toBeTruthy();
  });
});
