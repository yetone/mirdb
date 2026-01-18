import { test, expect } from '@playwright/test';

test.describe('Features Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Grid layout contains 4-6 feature cards with icons', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Verify the grid layout exists
    const featuresGrid = page.locator('[data-testid="features-grid"]');
    await expect(featuresGrid).toBeVisible();

    // Count feature cards - should be 4-6
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(4);
    expect(cardCount).toBeLessThanOrEqual(6);

    // Verify each card has an icon
    const featureIcons = page.locator('.feature-icon');
    const iconCount = await featureIcons.count();
    expect(iconCount).toBe(cardCount);

    // Verify grid has correct CSS class for layout
    await expect(featuresGrid).toHaveClass(/grid/);
  });

  test('TC2: Memcached Protocol Compatibility feature card is present', async ({ page }) => {
    const featureCard = page.locator('[data-testid="feature-card-memcached-protocol"]');
    await expect(featureCard).toBeVisible();

    // Check title
    const title = page.locator('[data-testid="feature-title-memcached-protocol"]');
    await expect(title).toBeVisible();
    await expect(title).toContainText('Memcached Protocol Compatibility');

    // Check icon exists
    const icon = page.locator('[data-testid="feature-icon-memcached-protocol"]');
    await expect(icon).toBeVisible();

    // Check description
    const description = page.locator('[data-testid="feature-description-memcached-protocol"]');
    await expect(description).toBeVisible();
    await expect(description).not.toBeEmpty();
  });

  test('TC3: LSM Tree Architecture feature card is present', async ({ page }) => {
    const featureCard = page.locator('[data-testid="feature-card-lsm-tree"]');
    await expect(featureCard).toBeVisible();

    // Check title
    const title = page.locator('[data-testid="feature-title-lsm-tree"]');
    await expect(title).toBeVisible();
    await expect(title).toContainText('LSM Tree Architecture');

    // Check icon exists
    const icon = page.locator('[data-testid="feature-icon-lsm-tree"]');
    await expect(icon).toBeVisible();

    // Check description
    const description = page.locator('[data-testid="feature-description-lsm-tree"]');
    await expect(description).toBeVisible();
    await expect(description).not.toBeEmpty();
  });

  test('TC4: Rust Performance feature card is present', async ({ page }) => {
    const featureCard = page.locator('[data-testid="feature-card-rust-performance"]');
    await expect(featureCard).toBeVisible();

    // Check title
    const title = page.locator('[data-testid="feature-title-rust-performance"]');
    await expect(title).toBeVisible();
    await expect(title).toContainText('Rust Performance');

    // Check icon exists
    const icon = page.locator('[data-testid="feature-icon-rust-performance"]');
    await expect(icon).toBeVisible();

    // Check description
    const description = page.locator('[data-testid="feature-description-rust-performance"]');
    await expect(description).toBeVisible();
    await expect(description).not.toBeEmpty();
  });

  test('TC5: Write-Ahead Log feature card is present', async ({ page }) => {
    const featureCard = page.locator('[data-testid="feature-card-wal"]');
    await expect(featureCard).toBeVisible();

    // Check title
    const title = page.locator('[data-testid="feature-title-wal"]');
    await expect(title).toBeVisible();
    await expect(title).toContainText('Write-Ahead Log');

    // Check icon exists
    const icon = page.locator('[data-testid="feature-icon-wal"]');
    await expect(icon).toBeVisible();

    // Check description
    const description = page.locator('[data-testid="feature-description-wal"]');
    await expect(description).toBeVisible();
    await expect(description).not.toBeEmpty();
  });
});
