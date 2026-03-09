/**
 * Features Section E2E Tests
 * Owner: Scenario 2 - Features Section Display
 *
 * Tests:
 * - 5 feature items displayed
 * - Memcached protocol feature present
 * - Persistence feature present
 * - LSM tree feature present
 * - Skip list memtable feature present
 * - Automatic compaction feature present
 */

const { test, expect } = require('@playwright/test');

test.describe('Features Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC-1: Features section contains exactly 5 feature elements', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Query for feature items
    const featureCards = featuresSection.locator('.feature-card');

    // Verify exactly 5 feature elements
    await expect(featureCards).toHaveCount(5);
  });

  test('TC-2: Memcached protocol feature is present with proper description', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the Memcached feature card
    const memcachedFeature = featuresSection.locator('.feature-card[data-feature="memcached"]');
    await expect(memcachedFeature).toBeVisible();

    // Check for 'Memcached' keyword in heading
    const heading = memcachedFeature.locator('h3');
    await expect(heading).toContainText('Memcached');

    // Check for protocol-related description
    const description = memcachedFeature.locator('p');
    await expect(description).toContainText('protocol');
  });

  test('TC-3: Persistence feature is present with description of durable storage', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the Persistence feature card
    const persistenceFeature = featuresSection.locator('.feature-card[data-feature="persistence"]');
    await expect(persistenceFeature).toBeVisible();

    // Check for 'Persistence' keyword in heading
    const heading = persistenceFeature.locator('h3');
    await expect(heading).toContainText('Persistence');

    // Check for durable storage description (mentions disk, persist, or durable)
    const description = persistenceFeature.locator('p');
    const descText = await description.textContent();
    const hasDurableContent = descText.toLowerCase().includes('disk') ||
                              descText.toLowerCase().includes('persist') ||
                              descText.toLowerCase().includes('durable');
    expect(hasDurableContent).toBe(true);
  });

  test('TC-4: LSM tree feature is present with description of storage architecture', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the LSM feature card
    const lsmFeature = featuresSection.locator('.feature-card[data-feature="lsm"]');
    await expect(lsmFeature).toBeVisible();

    // Check for 'LSM' keyword in heading
    const heading = lsmFeature.locator('h3');
    await expect(heading).toContainText('LSM');

    // Check for storage architecture description
    const description = lsmFeature.locator('p');
    const descText = await description.textContent();
    const hasArchitectureContent = descText.toLowerCase().includes('tree') ||
                                   descText.toLowerCase().includes('structure') ||
                                   descText.toLowerCase().includes('architecture') ||
                                   descText.toLowerCase().includes('storage');
    expect(hasArchitectureContent).toBe(true);
  });

  test('TC-5: Skip list memtable feature is present with description of in-memory structure', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the Skip list feature card
    const skiplistFeature = featuresSection.locator('.feature-card[data-feature="skiplist"]');
    await expect(skiplistFeature).toBeVisible();

    // Check for 'Skip list' or 'Skip List' keyword in heading
    const heading = skiplistFeature.locator('h3');
    const headingText = await heading.textContent();
    expect(headingText.toLowerCase()).toContain('skip list');

    // Check for in-memory structure description
    const description = skiplistFeature.locator('p');
    const descText = await description.textContent();
    const hasInMemoryContent = descText.toLowerCase().includes('memory') ||
                               descText.toLowerCase().includes('in-memory');
    expect(hasInMemoryContent).toBe(true);
  });

  test('TC-6: Automatic compaction feature is present with description of maintenance', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the Compaction feature card
    const compactionFeature = featuresSection.locator('.feature-card[data-feature="compaction"]');
    await expect(compactionFeature).toBeVisible();

    // Check for 'Compaction' keyword in heading
    const heading = compactionFeature.locator('h3');
    await expect(heading).toContainText('Compaction');

    // Check for automatic maintenance description
    const description = compactionFeature.locator('p');
    const descText = await description.textContent();
    const hasMaintenanceContent = descText.toLowerCase().includes('automatic') ||
                                  descText.toLowerCase().includes('background') ||
                                  descText.toLowerCase().includes('maintenance') ||
                                  descText.toLowerCase().includes('merge');
    expect(hasMaintenanceContent).toBe(true);
  });

  test('Features section is accessible via anchor navigation', async ({ page }) => {
    // Click on features nav link
    const featuresLink = page.locator('a[href="#features"]').first();
    await featuresLink.click();

    // Verify features section is in viewport
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  test('Feature cards have proper visual structure', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check that each feature card has an icon, heading, and description
    const featureCards = featuresSection.locator('.feature-card');
    const count = await featureCards.count();

    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);

      // Each card should have an icon container
      const icon = card.locator('.feature-icon');
      await expect(icon).toBeVisible();

      // Each card should have a heading (h3)
      const heading = card.locator('h3');
      await expect(heading).toBeVisible();

      // Each card should have a description (p)
      const description = card.locator('p');
      await expect(description).toBeVisible();
    }
  });
});
