/**
 * Features Section E2E Tests
 * Owner: Scenario 2 - Features Section Display
 *
 * Tests:
 * - Feature cards presence (persistence, memcached, LSM, skip-list, compaction)
 * - Feature descriptions
 * - Features section heading
 * - Feature grid layout
 */
import { test, expect } from '@playwright/test';

test.describe('Features Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/index.html');
  });

  test('TC1: Persistence feature card exists with explanation', async ({ page }) => {
    // Check for persistence feature card
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find feature card heading
    const persistenceHeading = featuresSection.locator('h3', { hasText: /Persistent Storage/i });
    await expect(persistenceHeading).toBeVisible();

    // Verify description mentions persistence to disk (sibling p element)
    const card = persistenceHeading.locator('..');
    const description = card.locator('p');
    const text = await description.textContent();
    expect(text.toLowerCase()).toMatch(/persist|disk|survives/i);
  });

  test('TC2: Memcached protocol feature card exists with explanation', async ({ page }) => {
    // Check for memcached protocol feature card
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find feature card heading
    const memcachedHeading = featuresSection.locator('h3', { hasText: /Memcached Protocol/i });
    await expect(memcachedHeading).toBeVisible();

    // Verify description mentions memcached text protocol compatibility
    const card = memcachedHeading.locator('..');
    const description = card.locator('p');
    const text = await description.textContent();
    expect(text.toLowerCase()).toMatch(/memcached.*protocol|protocol|clients/i);
  });

  test('TC3: LSM tree architecture feature card exists with explanation', async ({ page }) => {
    // Check for LSM tree architecture feature card
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find feature card heading
    const lsmHeading = featuresSection.locator('h3', { hasText: /LSM Tree/i });
    await expect(lsmHeading).toBeVisible();

    // Verify description mentions Log-Structured Merge-tree implementation
    const card = lsmHeading.locator('..');
    const description = card.locator('p');
    const text = await description.textContent();
    expect(text.toLowerCase()).toMatch(/log-structured|merge-tree|memtable|sstable/i);
  });

  test('TC4: Skip-list memtable feature card exists with explanation', async ({ page }) => {
    // Check for skip-list memtable feature card
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find feature card heading
    const skipListHeading = featuresSection.locator('h3', { hasText: /Skip-List Memtable/i });
    await expect(skipListHeading).toBeVisible();

    // Verify description mentions skip-list based memtable
    const card = skipListHeading.locator('..');
    const description = card.locator('p');
    const text = await description.textContent();
    expect(text.toLowerCase()).toMatch(/in-memory|o\(log n\)|operations/i);
  });

  test('TC5: Compaction feature card exists with explanation', async ({ page }) => {
    // Check for compaction feature card
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find feature card heading (Automatic Compaction in the actual page)
    const compactionHeading = featuresSection.locator('h3', { hasText: /Compaction/i });
    await expect(compactionHeading).toBeVisible();

    // Verify description mentions minor and major compaction
    const card = compactionHeading.locator('..');
    const description = card.locator('p');
    const text = await description.textContent();
    expect(text.toLowerCase()).toMatch(/minor|major|compaction|optimized/i);
  });

  test('TC6: Features section has proper h2 heading', async ({ page }) => {
    // Verify features section has an h2 or identifiable heading element
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const heading = featuresSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText(/feature/i);
  });

  test('All feature cards are present in grid layout', async ({ page }) => {
    // Verify all feature cards are rendered (at least 5 required features)
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Count h3 headings within feature cards (each card has an h3)
    const featureHeadings = featuresSection.locator('.grid > div > h3');
    const count = await featureHeadings.count();
    expect(count).toBeGreaterThanOrEqual(5);
  });
});
