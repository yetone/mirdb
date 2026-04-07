/**
 * Features Section E2E Tests
 * Owner: Scenario 2 - Core Features Section Display
 *
 * Test coverage:
 * - Memcached protocol feature presence
 * - Disk persistence feature presence
 * - LSM-tree architecture feature presence
 * - Grid layout verification
 */
import { test, expect } from '@playwright/test';

test.describe('Core Features Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('http://127.0.0.1:1111/');
  });

  test('TC1: Features section contains Memcached protocol feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check for Memcached protocol feature card
    const memcachedCard = page.locator('[data-feature="memcached-protocol"]');
    await expect(memcachedCard).toBeVisible();

    // Verify it has a title
    const memcachedTitle = memcachedCard.locator('h3');
    await expect(memcachedTitle).toContainText('Memcached Protocol');

    // Verify it has a description mentioning protocol compatibility
    const memcachedDescription = memcachedCard.locator('p');
    await expect(memcachedDescription).toContainText('compatibility');

    // Verify it has an icon
    const memcachedIcon = memcachedCard.locator('.feature-icon');
    await expect(memcachedIcon).toBeVisible();
  });

  test('TC2: Features section contains disk persistence feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check for disk persistence feature card
    const persistenceCard = page.locator('[data-feature="disk-persistence"]');
    await expect(persistenceCard).toBeVisible();

    // Verify it has a title
    const persistenceTitle = persistenceCard.locator('h3');
    await expect(persistenceTitle).toContainText('Disk Persistence');

    // Verify it has a description mentioning persistence
    const persistenceDescription = persistenceCard.locator('p');
    await expect(persistenceDescription).toContainText('persists');

    // Verify it has an icon
    const persistenceIcon = persistenceCard.locator('.feature-icon');
    await expect(persistenceIcon).toBeVisible();
  });

  test('TC3: Features section contains LSM-tree architecture feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check for LSM-tree architecture feature card
    const lsmCard = page.locator('[data-feature="lsm-tree"]');
    await expect(lsmCard).toBeVisible();

    // Verify it has a title
    const lsmTitle = lsmCard.locator('h3');
    await expect(lsmTitle).toContainText('LSM-tree');

    // Verify it has a description mentioning LSM-tree storage
    const lsmDescription = lsmCard.locator('p');
    await expect(lsmDescription).toContainText('Log-Structured Merge-tree');

    // Verify it has an icon
    const lsmIcon = lsmCard.locator('.feature-icon');
    await expect(lsmIcon).toBeVisible();
  });

  test('TC4: Features are displayed in grid layout with icons and descriptions', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check for features grid
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Verify grid has display: grid CSS
    const gridDisplay = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(gridDisplay).toBe('grid');

    // Verify all three feature cards exist
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(3);

    // Verify each card has an icon and readable description
    for (const card of await featureCards.all()) {
      // Check for icon
      const icon = card.locator('.feature-icon');
      await expect(icon).toBeVisible();

      // Check for title (h3)
      const title = card.locator('h3');
      await expect(title).toBeVisible();
      const titleText = await title.textContent();
      expect(titleText?.length).toBeGreaterThan(0);

      // Check for description (p)
      const description = card.locator('p');
      await expect(description).toBeVisible();
      const descText = await description.textContent();
      expect(descText?.length).toBeGreaterThan(20); // Readable description should have content
    }
  });

  test('Features section is navigable from header', async ({ page }) => {
    // Click the Features link in navigation
    await page.click('nav a[href="#features"]');

    // Verify the features section is in view
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });
});
