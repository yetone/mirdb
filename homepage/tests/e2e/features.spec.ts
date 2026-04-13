/**
 * Features Section E2E Tests
 * Owner: Scenario 2 - Features Section Display
 *
 * Tests:
 * - All required features displayed
 * - Feature descriptions present
 * - Grid/list layout
 */
import { test, expect } from '@playwright/test';

test.describe('Features Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should display Memcached protocol feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check for Memcached protocol feature
    const memcachedTitle = page.locator('.feature-title', { hasText: /Memcached Protocol/i });
    await expect(memcachedTitle).toBeVisible();

    // Check description mentions protocol compatibility
    const memcachedCard = page.locator('[data-testid="feature-card"]', { hasText: /Memcached Protocol/i });
    await expect(memcachedCard.locator('.feature-description')).toContainText(/protocol/i);
  });

  test('should display persistence feature with SSTable mention', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check for disk persistence feature
    const persistenceTitle = page.locator('.feature-title', { hasText: /Disk Persistence/i });
    await expect(persistenceTitle).toBeVisible();

    // Check description mentions SSTable
    const persistenceCard = page.locator('[data-testid="feature-card"]', { hasText: /Disk Persistence/i });
    await expect(persistenceCard.locator('.feature-description')).toContainText(/SSTable/i);
  });

  test('should display LSM-tree feature', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check for LSM-tree feature
    const lsmTitle = page.locator('.feature-title', { hasText: /LSM-Tree/i });
    await expect(lsmTitle).toBeVisible();

    // Check description mentions LSM-tree architecture
    const lsmCard = page.locator('[data-testid="feature-card"]', { hasText: /LSM-Tree/i });
    await expect(lsmCard.locator('.feature-description')).toContainText(/Log-Structured Merge-Tree/i);
  });

  test('should display skip list feature', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check for skip list feature
    const skipListTitle = page.locator('.feature-title', { hasText: /Skip List/i });
    await expect(skipListTitle).toBeVisible();

    // Check description mentions skip list memtables
    const skipListCard = page.locator('[data-testid="feature-card"]', { hasText: /Skip List/i });
    await expect(skipListCard.locator('.feature-description')).toContainText(/skip list/i);
  });

  test('should display compaction feature with minor and major mention', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check for compaction feature
    const compactionTitle = page.locator('.feature-title', { hasText: /Compaction/i });
    await expect(compactionTitle).toBeVisible();

    // Check description mentions minor and major compaction
    const compactionCard = page.locator('[data-testid="feature-card"]', { hasText: /Compaction/i });
    await expect(compactionCard.locator('.feature-description')).toContainText(/minor and major compaction/i);
  });

  test('should display at least 5 features', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Count feature cards
    const featureCards = page.locator('[data-testid="feature-card"]');
    const count = await featureCards.count();
    expect(count).toBeGreaterThanOrEqual(5);
  });

  test('should display features in organized grid layout', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check grid container exists
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Verify grid display style
    const gridDisplay = await featuresGrid.evaluate((el) => window.getComputedStyle(el).display);
    expect(gridDisplay).toBe('grid');

    // Each feature card should have title and description
    const featureCards = page.locator('[data-testid="feature-card"]');
    const count = await featureCards.count();

    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);
      await expect(card.locator('.feature-title')).toBeVisible();
      await expect(card.locator('.feature-description')).toBeVisible();
    }
  });

  test('should have accessible section heading', async ({ page }) => {
    const featuresHeading = page.locator('#features-heading');
    await expect(featuresHeading).toBeVisible();
    await expect(featuresHeading).toHaveText('Features');

    // Check section is labelled by heading
    const section = page.locator('#features');
    await expect(section).toHaveAttribute('aria-labelledby', 'features-heading');
  });
});
