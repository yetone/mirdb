const { test, expect } = require('@playwright/test');

test.describe('Hero Section Content Validation', () => {
  test.beforeEach(async ({ page }) => {
    // Use file protocol to serve the HTML locally
    await page.goto('file:///workspace/docs/index.html');
  });

  test('TC-001: Verify hero section displays clear value proposition', async ({ page }) => {
    // Wait for the page to load
    await page.waitForLoadState('domcontentloaded');

    // Check the H1 title
    const title = await page.textContent('h1');
    expect(title).toContain('MirDB');

    // Check the tagline
    const tagline = await page.textContent('.tagline');
    const trimmedTagline = tagline.trim().replace(/\s+/g, ' ');
    const lowerTagline = trimmedTagline.toLowerCase();
    const isValidValueProp = lowerTagline.includes('persistent key-value store') ||
                             lowerTagline.includes('persistent key-value store');

    console.log('Tagline found:', tagline);
    console.log('Trimmed tagline:', trimmedTagline);
    console.log('Lowercase tagline:', lowerTagline);
    expect(isValidValueProp).toBe(true);

    // Check the value proposition box
    const valueProp = await page.textContent('.value-prop');
    console.log('Value proposition found:', valueProp);

    expect(valueProp).toContain('MirDB');
    expect(valueProp).toContain('persistent key-value store');
    expect(valueProp).toContain('Memcached protocol');
    expect(valueProp).toContain('LSM tree architecture');
  });

  test('TC-002: Verify hero section displays three key differentiators', async ({ page }) => {
    await page.waitForLoadState('domcontentloaded');

    // Navigate to the features section (hero-adjacent as mentioned in PRD)
    // This is the section RIGHT after the hero that contains feature cards
    const featuresSection = await page.locator('.features');
    await expect(featuresSection).toBeVisible();

    // Check for persistence feature
    const featureCards = await page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(3);

    // Extract all feature card titles and descriptions
    const featureText = await page.locator('.feature-card *').allTextContents();
    const allText = featureText.join(' ').toLowerCase();

    console.log('Feature section text:', allText);

    // Validate presence of all three key differentiators
    const hasPersistence = allText.includes('persistent');
    const hasLsmTree = allText.includes('lsm tree');
    const hasMemcachedCompatibility = allText.includes('memcached');

    // Detailed assertions for better error messages
    expect(hasPersistence).toBe(true);
    expect(hasLsmTree).toBe(true);
    expect(hasMemcachedCompatibility).toBe(true);
  });

  test('TC-003: Verify persistence feature has detailed description', async ({ page }) => {
    await page.waitForLoadState('domcontentloaded');

    // Check the persistence feature card specifically
    const persistenceCard = await page.locator('.feature-card:has-text("Persistent")');
    await expect(persistenceCard).toBeVisible();

    const persistenceText = await persistenceCard.textContent();
    expect(persistenceText).toContain('data survives restarts');
  });

  test('TC-004: Verify LSM tree feature has detailed description', async ({ page }) => {
    await page.waitForLoadState('domcontentloaded');

    // Check the LSM tree feature card specifically
    const lsmCard = await page.locator('.feature-card:has-text("LSM Tree")');
    await expect(lsmCard).toBeVisible();

    const lsmText = await lsmCard.textContent();
    expect(lsmText).toContain('Log-Structured Merge');
    expect(lsmText).toContain('efficient write');
  });

  test('TC-005: Verify Memcached compatibility feature has detailed description', async ({ page }) => {
    await page.waitForLoadState('domcontentloaded');

    // Check the Memcached compatibility feature card specifically
    const memcachedTitle = await page.locator('.feature-card >> text="Memcached Compatibility"');
    await expect(memcachedTitle).toBeVisible();

    const memcachedCard = await memcachedTitle.locator('xpath=ancestor::div[@class="feature-card"]');
    const memcachedText = await memcachedCard.textContent();
    expect(memcachedText).toContain('Memcached protocol');
    expect(memcachedText).toContain('clients');
  });
});
