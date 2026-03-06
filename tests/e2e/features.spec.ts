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

  test('features section exists with correct id', async ({ page }) => {
    // Test Case 1: Check features section exists
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();
    await expect(featuresSection).toHaveAttribute('id', 'features');
  });

  test('displays Memcached Protocol feature card', async ({ page }) => {
    // Test Case 2: Check for Memcached Protocol feature
    const featuresSection = page.locator('#features');
    const memcachedFeature = featuresSection.locator('.feature-card', {
      hasText: /memcached/i
    });

    await expect(memcachedFeature).toBeVisible();

    // Verify it mentions protocol
    const featureText = await memcachedFeature.textContent();
    expect(featureText?.toLowerCase()).toContain('protocol');
  });

  test('displays Persistence feature card', async ({ page }) => {
    // Test Case 3: Check for Persistence feature
    const featuresSection = page.locator('#features');
    const persistenceFeature = featuresSection.locator('.feature-card', {
      hasText: /persist/i
    });

    await expect(persistenceFeature).toBeVisible();

    // Verify it contains persistence-related text
    const featureText = await persistenceFeature.textContent();
    expect(featureText?.toLowerCase()).toMatch(/persist(ent|ence)/i);
  });

  test('displays LSM Tree feature card', async ({ page }) => {
    // Test Case 4: Check for LSM Tree feature
    const featuresSection = page.locator('#features');
    const lsmFeature = featuresSection.locator('.feature-card', {
      hasText: /lsm|log-structured merge/i
    });

    await expect(lsmFeature).toBeVisible();

    // Verify it contains LSM-related text
    const featureText = await lsmFeature.textContent();
    expect(featureText?.toLowerCase()).toMatch(/lsm|log-structured/i);
  });

  test('each feature card has a description of minimum 20 characters', async ({ page }) => {
    // Test Case 5: Verify each feature has a description
    const featuresSection = page.locator('#features');
    const featureCards = featuresSection.locator('.feature-card');

    // Expect exactly 3 feature cards
    await expect(featureCards).toHaveCount(3);

    // Check each card has a description with at least 20 characters
    for (let i = 0; i < 3; i++) {
      const card = featureCards.nth(i);
      const description = card.locator('.feature-description');
      await expect(description).toBeVisible();

      const descText = await description.textContent();
      expect(descText?.length).toBeGreaterThanOrEqual(20);
    }
  });

  test('feature cards have visible titles', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const featureTitles = featuresSection.locator('.feature-title');

    await expect(featureTitles).toHaveCount(3);

    for (let i = 0; i < 3; i++) {
      const title = featureTitles.nth(i);
      await expect(title).toBeVisible();
      const titleText = await title.textContent();
      expect(titleText?.trim().length).toBeGreaterThan(0);
    }
  });

  test('features section has proper heading', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const heading = featuresSection.locator('h2.features-heading, .features-heading');

    await expect(heading).toBeVisible();
    await expect(heading).toContainText(/features/i);
  });
});
