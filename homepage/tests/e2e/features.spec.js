/**
 * Features Section E2E Tests
 * Owner: Scenario 2 - Features Section with Status Indicators
 *
 * Tests for:
 * - Feature cards display
 * - Status indicators (completed/planned)
 * - Feature descriptions and icons
 */

import { test, expect } from '@playwright/test';

test.describe('Features Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('features section is visible on the page', async ({ page }) => {
    const features = page.locator('#features');
    await expect(features).toBeVisible();
    await expect(features.locator('.features__title')).toHaveText('Key Features');
  });

  test('displays 4 feature cards', async ({ page }) => {
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(4);
  });

  // Test Case 1: Check for Memcached Protocol feature card
  test('displays Memcached Compatible feature card with checkmark status', async ({ page }) => {
    const memcachedCard = page.locator('.feature-card[data-feature="memcached"]');
    await expect(memcachedCard).toBeVisible();

    // Check title contains "Memcached Compatible"
    const title = memcachedCard.locator('.feature-card__title');
    await expect(title).toHaveText('Memcached Compatible');

    // Check for completed status badge with checkmark
    const statusBadge = memcachedCard.locator('.status-badge--completed');
    await expect(statusBadge).toBeVisible();
    await expect(statusBadge).toContainText('Completed');

    // Verify checkmark SVG is present
    const checkIcon = statusBadge.locator('svg');
    await expect(checkIcon).toBeVisible();
  });

  // Test Case 2: Check for Persistence feature card
  test('displays Persistent Storage feature card with checkmark status', async ({ page }) => {
    const persistenceCard = page.locator('.feature-card[data-feature="persistence"]');
    await expect(persistenceCard).toBeVisible();

    // Check title contains "Persistent Storage"
    const title = persistenceCard.locator('.feature-card__title');
    await expect(title).toHaveText('Persistent Storage');

    // Check for completed status badge with checkmark
    const statusBadge = persistenceCard.locator('.status-badge--completed');
    await expect(statusBadge).toBeVisible();
    await expect(statusBadge).toContainText('Completed');
  });

  // Test Case 3: Check for LSM Tree feature card
  test('displays LSM Tree Architecture feature card with description', async ({ page }) => {
    const lsmCard = page.locator('.feature-card[data-feature="lsm-tree"]');
    await expect(lsmCard).toBeVisible();

    // Check title contains "LSM Tree"
    const title = lsmCard.locator('.feature-card__title');
    await expect(title).toContainText('LSM Tree');

    // Check description mentions architecture
    const description = lsmCard.locator('.feature-card__description');
    await expect(description).toBeVisible();
    const descText = await description.textContent();
    expect(descText.toLowerCase()).toContain('architecture');
  });

  // Test Case 4: Check for Raft consensus feature card with planned indicator
  test('displays Raft Consensus feature card with planned indicator', async ({ page }) => {
    const raftCard = page.locator('.feature-card[data-feature="raft"]');
    await expect(raftCard).toBeVisible();

    // Check title contains "Raft Consensus"
    const title = raftCard.locator('.feature-card__title');
    await expect(title).toContainText('Raft Consensus');

    // Check for planned status badge (not completed)
    const statusBadge = raftCard.locator('.status-badge--planned');
    await expect(statusBadge).toBeVisible();
    await expect(statusBadge).toContainText('Planned');

    // Verify clock icon is present for planned items
    const clockIcon = statusBadge.locator('svg');
    await expect(clockIcon).toBeVisible();
  });

  test('feature cards have icons', async ({ page }) => {
    const featureCards = page.locator('.feature-card');

    for (let i = 0; i < 4; i++) {
      const card = featureCards.nth(i);
      const icon = card.locator('.feature-card__icon svg');
      await expect(icon).toBeVisible();
    }
  });

  test('feature cards have descriptions', async ({ page }) => {
    const descriptions = page.locator('.feature-card__description');
    await expect(descriptions).toHaveCount(4);

    for (let i = 0; i < 4; i++) {
      const desc = descriptions.nth(i);
      await expect(desc).toBeVisible();
      const text = await desc.textContent();
      expect(text.length).toBeGreaterThan(20);
    }
  });

  test('all completed features have status-badge--completed class', async ({ page }) => {
    const completedCards = page.locator('.feature-card[data-status="completed"]');
    await expect(completedCards).toHaveCount(3);

    for (let i = 0; i < 3; i++) {
      const card = completedCards.nth(i);
      const badge = card.locator('.status-badge--completed');
      await expect(badge).toBeVisible();
    }
  });

  test('all planned features have status-badge--planned class', async ({ page }) => {
    const plannedCards = page.locator('.feature-card[data-status="planned"]');
    await expect(plannedCards).toHaveCount(1);

    const badge = plannedCards.locator('.status-badge--planned');
    await expect(badge).toBeVisible();
  });

  test('features section has proper accessibility attributes', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toHaveAttribute('aria-labelledby', 'features-title');

    const title = page.locator('#features-title');
    await expect(title).toBeVisible();
  });

  test('status badges have aria-label for accessibility', async ({ page }) => {
    const completedBadge = page.locator('.status-badge--completed').first();
    await expect(completedBadge).toHaveAttribute('aria-label', 'Feature completed');

    const plannedBadge = page.locator('.status-badge--planned').first();
    await expect(plannedBadge).toHaveAttribute('aria-label', 'Feature planned');
  });
});
