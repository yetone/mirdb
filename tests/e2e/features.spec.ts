/**
 * Features Section E2E Tests
 * Owner: Scenario 3 - Features Section Content
 *
 * Test coverage:
 * - Features section presence
 * - Memcached Protocol feature card
 * - Persistence feature card
 * - LSM Tree feature card
 * - Feature descriptions
 */

import { test, expect } from '@playwright/test';
import { navigateToSection, waitForPageLoad } from './test-utils';

test.describe('Features Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);
  });

  test('TC1: Features section exists with id="features"', async ({ page }) => {
    // Test Case 1: Check features section exists
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();
    await expect(featuresSection).toHaveAttribute('id', 'features');
  });

  test('TC2: Memcached Protocol feature card exists', async ({ page }) => {
    // Test Case 2: Check for Memcached Protocol feature
    await navigateToSection(page, 'features');

    const memcachedFeature = page.locator('[data-testid="feature-memcached"]');
    await expect(memcachedFeature).toBeVisible();

    // Verify text mentions 'Memcached' and 'protocol'
    const featureText = await memcachedFeature.textContent();
    expect(featureText?.toLowerCase()).toContain('memcached');
    expect(featureText?.toLowerCase()).toContain('protocol');
  });

  test('TC3: Persistence feature card exists', async ({ page }) => {
    // Test Case 3: Check for Persistence feature
    await navigateToSection(page, 'features');

    const persistenceFeature = page.locator('[data-testid="feature-persistence"]');
    await expect(persistenceFeature).toBeVisible();

    // Verify text mentions 'Persistent' or 'Persistence'
    const featureText = await persistenceFeature.textContent();
    const hasPeristence = featureText?.toLowerCase().includes('persistent') ||
                          featureText?.toLowerCase().includes('persistence');
    expect(hasPeristence).toBe(true);
  });

  test('TC4: LSM Tree feature card exists', async ({ page }) => {
    // Test Case 4: Check for LSM Tree feature
    await navigateToSection(page, 'features');

    const lsmFeature = page.locator('[data-testid="feature-lsm"]');
    await expect(lsmFeature).toBeVisible();

    // Verify text mentions 'LSM' or 'Log-Structured Merge'
    const featureText = await lsmFeature.textContent();
    const hasLsm = featureText?.toLowerCase().includes('lsm') ||
                   featureText?.toLowerCase().includes('log-structured merge');
    expect(hasLsm).toBe(true);
  });

  test('TC5: Each feature card has a description (minimum 20 characters)', async ({ page }) => {
    // Test Case 5: Verify each feature has a description
    await navigateToSection(page, 'features');

    const featureCards = page.locator('.feature-card');
    const count = await featureCards.count();
    expect(count).toBeGreaterThanOrEqual(3);

    // Check each feature card has a description paragraph with at least 20 characters
    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);
      const description = card.locator('p');
      await expect(description).toBeVisible();

      const descriptionText = await description.textContent();
      expect(descriptionText?.length).toBeGreaterThanOrEqual(20);
    }
  });

  test('Features section has proper heading', async ({ page }) => {
    await navigateToSection(page, 'features');

    const heading = page.locator('#features h2');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('Features');
  });

  test('All three required feature cards are displayed', async ({ page }) => {
    await navigateToSection(page, 'features');

    // Verify all three specific feature cards exist
    await expect(page.locator('[data-testid="feature-memcached"]')).toBeVisible();
    await expect(page.locator('[data-testid="feature-persistence"]')).toBeVisible();
    await expect(page.locator('[data-testid="feature-lsm"]')).toBeVisible();
  });
});
