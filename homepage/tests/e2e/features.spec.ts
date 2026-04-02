/**
 * E2E tests for Features section.
 * Owner: Scenario 3 - Features Section Display
 */
import { test, expect } from '@playwright/test';

test.describe('Features Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Features section exists with heading', async ({ page }) => {
    // Test case 1: Check Features section exists
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const heading = featuresSection.locator('h2');
    await expect(heading).toHaveText('Features');
  });

  test('Memcached Protocol feature is displayed', async ({ page }) => {
    // Test case 2: Verify Memcached Protocol feature
    const featuresSection = page.locator('#features');

    const memcachedCard = featuresSection.locator('[data-testid="feature-card"]').filter({
      hasText: 'Memcached Protocol',
    });

    await expect(memcachedCard).toBeVisible();

    // Check it has a description about protocol compatibility
    await expect(memcachedCard).toContainText('Memcached');
    await expect(memcachedCard).toContainText('protocol');
  });

  test('Persistence feature is displayed', async ({ page }) => {
    // Test case 3: Verify Persistence feature
    const featuresSection = page.locator('#features');

    const persistenceCard = featuresSection.locator('[data-testid="feature-card"]').filter({
      hasText: 'Persistence',
    });

    await expect(persistenceCard).toBeVisible();

    // Check it has a description about disk persistence
    await expect(persistenceCard).toContainText('disk');
  });

  test('LSM Tree feature is displayed', async ({ page }) => {
    // Test case 4: Verify LSM Tree feature
    const featuresSection = page.locator('#features');

    const lsmTreeCard = featuresSection.locator('[data-testid="feature-card"]').filter({
      hasText: 'LSM Tree',
    });

    await expect(lsmTreeCard).toBeVisible();

    // Check it has a description about architecture
    await expect(lsmTreeCard).toContainText('architecture');
  });

  test('Features are displayed in a responsive grid layout', async ({ page }) => {
    // Test case 5: Check feature cards grid layout
    const featuresGrid = page.locator('[data-testid="features-grid"]');
    await expect(featuresGrid).toBeVisible();

    // Check that there are exactly 3 feature cards
    const featureCards = featuresGrid.locator('[data-testid="feature-card"]');
    await expect(featureCards).toHaveCount(3);

    // Verify grid display on desktop
    const gridStyle = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(gridStyle).toBe('grid');
  });

  test('Feature cards have icons', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const featureCards = featuresSection.locator('[data-testid="feature-card"]');

    // Check each card has an icon
    for (let i = 0; i < 3; i++) {
      const card = featureCards.nth(i);
      const icon = card.locator('[aria-hidden="true"]');
      await expect(icon).toBeVisible();
    }
  });

  test('Features section is accessible via anchor link', async ({ page }) => {
    // Navigate directly to features section
    await page.goto('/#features');

    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();
  });
});
