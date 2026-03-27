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

    const persistenceCard = featuresSection.locator('[data-feature="persistence"]');
    await expect(persistenceCard).toBeVisible();

    // Verify heading
    const heading = persistenceCard.locator('h3');
    await expect(heading).toContainText(/persist/i);

    // Verify description mentions persistence to disk
    const description = persistenceCard.locator('p');
    const text = await description.textContent();
    expect(text.toLowerCase()).toMatch(/persist|disk|durability/i);
  });

  test('TC2: Memcached protocol feature card exists with explanation', async ({ page }) => {
    // Check for memcached protocol feature card
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const memcachedCard = featuresSection.locator('[data-feature="memcached"]');
    await expect(memcachedCard).toBeVisible();

    // Verify heading
    const heading = memcachedCard.locator('h3');
    await expect(heading).toContainText(/memcached|protocol/i);

    // Verify description mentions memcached text protocol compatibility
    const description = memcachedCard.locator('p');
    const text = await description.textContent();
    expect(text.toLowerCase()).toMatch(/memcached.*protocol|protocol.*memcached|compatibility/i);
  });

  test('TC3: LSM tree architecture feature card exists with explanation', async ({ page }) => {
    // Check for LSM tree architecture feature card
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const lsmCard = featuresSection.locator('[data-feature="lsm-tree"]');
    await expect(lsmCard).toBeVisible();

    // Verify heading
    const heading = lsmCard.locator('h3');
    await expect(heading).toContainText(/lsm/i);

    // Verify description mentions Log-Structured Merge-tree implementation
    const description = lsmCard.locator('p');
    const text = await description.textContent();
    expect(text.toLowerCase()).toMatch(/log-structured merge|lsm/i);
  });

  test('TC4: Skip-list memtable feature card exists with explanation', async ({ page }) => {
    // Check for skip-list memtable feature card
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const skipListCard = featuresSection.locator('[data-feature="skip-list"]');
    await expect(skipListCard).toBeVisible();

    // Verify heading
    const heading = skipListCard.locator('h3');
    await expect(heading).toContainText(/skip-list|memtable/i);

    // Verify description mentions skip-list based memtable
    const description = skipListCard.locator('p');
    const text = await description.textContent();
    expect(text.toLowerCase()).toMatch(/skip-list|memtable/i);
  });

  test('TC5: Compaction feature card exists with explanation', async ({ page }) => {
    // Check for compaction feature card
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const compactionCard = featuresSection.locator('[data-feature="compaction"]');
    await expect(compactionCard).toBeVisible();

    // Verify heading
    const heading = compactionCard.locator('h3');
    await expect(heading).toContainText(/compact/i);

    // Verify description mentions minor and major compaction
    const description = compactionCard.locator('p');
    const text = await description.textContent();
    expect(text.toLowerCase()).toMatch(/minor.*major|major.*minor|compaction/i);
  });

  test('TC6: Features section has proper h2 heading', async ({ page }) => {
    // Verify features section has an h2 or identifiable heading element
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const heading = featuresSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText(/feature/i);
  });

  test('All five feature cards are present', async ({ page }) => {
    // Verify all feature cards are rendered
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const featureCards = featuresSection.locator('.feature-card');
    await expect(featureCards).toHaveCount(5);
  });
});
