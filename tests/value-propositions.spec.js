const { test, expect } = require('@playwright/test');

test.describe('Key Value Propositions Display (REQ-2)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Page contains memcached-related text', async ({ page }) => {
    // Navigate to features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    // Check for memcached protocol support text
    const memcachedFeature = page.locator('[data-testid="feature-memcached"]');
    await expect(memcachedFeature).toBeVisible();

    // Verify the heading mentions Memcached Protocol Support
    const heading = memcachedFeature.locator('h3');
    await expect(heading).toContainText(/Memcached Protocol Support/i);

    // Verify the description mentions compatibility
    const description = memcachedFeature.locator('p');
    await expect(description).toContainText(/compatibility/i);

    // Additional check for memcached-related content anywhere on page
    const pageContent = await page.textContent('body');
    expect(pageContent.toLowerCase()).toContain('memcached');
  });

  test('TC2: Page contains persistence-related text with SSTable', async ({ page }) => {
    // Navigate to features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    // Check for persistence feature card
    const persistenceFeature = page.locator('[data-testid="feature-persistence"]');
    await expect(persistenceFeature).toBeVisible();

    // Verify the heading mentions Persistent Storage
    const heading = persistenceFeature.locator('h3');
    await expect(heading).toContainText(/Persistent Storage/i);

    // Verify the description mentions SSTable
    const description = persistenceFeature.locator('p');
    await expect(description).toContainText(/SSTable/i);

    // Additional check for persistence content
    const pageContent = await page.textContent('body');
    expect(pageContent.toLowerCase()).toContain('persistent');
    expect(pageContent).toContain('SSTable');
  });

  test('TC3: Page contains LSM-tree text with compaction', async ({ page }) => {
    // Navigate to features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    // Check for LSM feature card
    const lsmFeature = page.locator('[data-testid="feature-lsm"]');
    await expect(lsmFeature).toBeVisible();

    // Verify the heading mentions LSM Tree Architecture
    const heading = lsmFeature.locator('h3');
    await expect(heading).toContainText(/LSM Tree Architecture/i);

    // Verify the description mentions compaction
    const description = lsmFeature.locator('p');
    await expect(description).toContainText(/compaction/i);

    // Additional check for write-heavy workload mention
    await expect(description).toContainText(/write-heavy/i);
  });

  test('TC4: Features section has three distinct feature blocks', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Check that the features section is visible
    await expect(featuresSection).toBeVisible();

    // Get all feature cards
    const featureCards = page.locator('.feature-card');

    // Verify exactly 3 feature cards exist
    await expect(featureCards).toHaveCount(3);

    // Verify each card is visually distinct (has its own heading)
    const memcachedCard = page.locator('[data-testid="feature-memcached"]');
    const persistenceCard = page.locator('[data-testid="feature-persistence"]');
    const lsmCard = page.locator('[data-testid="feature-lsm"]');

    await expect(memcachedCard).toBeVisible();
    await expect(persistenceCard).toBeVisible();
    await expect(lsmCard).toBeVisible();

    // Verify all three have distinct headings (key pillars)
    await expect(memcachedCard.locator('h3')).toHaveText('Memcached Protocol Support');
    await expect(persistenceCard.locator('h3')).toHaveText('Persistent Storage');
    await expect(lsmCard.locator('h3')).toHaveText('LSM Tree Architecture');
  });

  test('Features section is easily discoverable from hero', async ({ page }) => {
    // Verify the hero section links to features
    const featuresLink = page.locator('a[href="#features"]');
    await expect(featuresLink).toBeVisible();

    // Click the features link and verify navigation
    await featuresLink.first().click();

    // Features section should be in view
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });
});
