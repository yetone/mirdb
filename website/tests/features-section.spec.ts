import { test, expect } from '@playwright/test';

test.describe('Features Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Memcached Protocol Compatibility feature card displays with title and description', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Query for Memcached Protocol Compatibility feature card
    const featureCard = page.locator('[data-testid="feature-memcached-protocol"]');
    await expect(featureCard).toBeVisible();

    // Verify title
    const title = featureCard.locator('h3');
    await expect(title).toHaveText('Memcached Protocol Compatibility');

    // Verify description about memcached compatibility
    const description = featureCard.locator('p');
    await expect(description).toBeVisible();
    const descriptionText = await description.textContent();
    expect(descriptionText).toContain('memcached');
    expect(descriptionText).toContain('protocol');
    expect(descriptionText?.length).toBeGreaterThan(50); // Ensure meaningful description
  });

  test('TC2: Persistent Storage feature card displays with title and description about SSTable persistence', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Query for Persistent Storage feature card
    const featureCard = page.locator('[data-testid="feature-persistent-storage"]');
    await expect(featureCard).toBeVisible();

    // Verify title
    const title = featureCard.locator('h3');
    await expect(title).toHaveText('Persistent Storage');

    // Verify description about SSTable persistence
    const description = featureCard.locator('p');
    await expect(description).toBeVisible();
    const descriptionText = await description.textContent();
    expect(descriptionText).toContain('persist');
    expect(descriptionText).toContain('SSTable');
    expect(descriptionText?.length).toBeGreaterThan(50);
  });

  test('TC3: LSM Tree Architecture feature card displays with title and description', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Query for LSM Tree Architecture feature card
    const featureCard = page.locator('[data-testid="feature-lsm-tree"]');
    await expect(featureCard).toBeVisible();

    // Verify title
    const title = featureCard.locator('h3');
    await expect(title).toHaveText('LSM Tree Architecture');

    // Verify description about LSM tree structure
    const description = featureCard.locator('p');
    await expect(description).toBeVisible();
    const descriptionText = await description.textContent();
    expect(descriptionText).toContain('LSM');
    expect(descriptionText).toContain('Log-Structured Merge-tree');
    expect(descriptionText?.length).toBeGreaterThan(50);
  });

  test('TC4: Async Networking feature card displays with title and description about Tokio', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Query for Async Networking feature card
    const featureCard = page.locator('[data-testid="feature-async-networking"]');
    await expect(featureCard).toBeVisible();

    // Verify title
    const title = featureCard.locator('h3');
    await expect(title).toHaveText('Async Networking');

    // Verify description about Tokio async runtime
    const description = featureCard.locator('p');
    await expect(description).toBeVisible();
    const descriptionText = await description.textContent();
    expect(descriptionText).toContain('Tokio');
    expect(descriptionText).toContain('async');
    expect(descriptionText?.length).toBeGreaterThan(50);
  });

  test('TC5: Write-Ahead Logging feature card displays with title and description about WAL durability', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Query for Write-Ahead Logging feature card
    const featureCard = page.locator('[data-testid="feature-wal"]');
    await expect(featureCard).toBeVisible();

    // Verify title
    const title = featureCard.locator('h3');
    await expect(title).toHaveText('Write-Ahead Logging');

    // Verify description about WAL durability
    const description = featureCard.locator('p');
    await expect(description).toBeVisible();
    const descriptionText = await description.textContent();
    expect(descriptionText).toContain('Write-Ahead Log');
    expect(descriptionText).toContain('durability');
    expect(descriptionText?.length).toBeGreaterThan(50);
  });

  test('TC6: Configurable Compaction feature card displays with title and description about compaction strategies', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Query for Configurable Compaction feature card
    const featureCard = page.locator('[data-testid="feature-compaction"]');
    await expect(featureCard).toBeVisible();

    // Verify title
    const title = featureCard.locator('h3');
    await expect(title).toHaveText('Configurable Compaction');

    // Verify description about compaction strategies
    const description = featureCard.locator('p');
    await expect(description).toBeVisible();
    const descriptionText = await description.textContent();
    expect(descriptionText).toContain('compaction');
    expect(descriptionText).toContain('Minor compaction');
    expect(descriptionText).toContain('major compaction');
    expect(descriptionText?.length).toBeGreaterThan(50);
  });

  test('All six feature cards are present in the features section', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify all six feature cards are present
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(6);

    // Verify each specific card exists
    await expect(page.locator('[data-testid="feature-memcached-protocol"]')).toBeVisible();
    await expect(page.locator('[data-testid="feature-persistent-storage"]')).toBeVisible();
    await expect(page.locator('[data-testid="feature-lsm-tree"]')).toBeVisible();
    await expect(page.locator('[data-testid="feature-async-networking"]')).toBeVisible();
    await expect(page.locator('[data-testid="feature-wal"]')).toBeVisible();
    await expect(page.locator('[data-testid="feature-compaction"]')).toBeVisible();
  });

  test('Features section has proper heading', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const heading = featuresSection.locator('h2');
    await expect(heading).toHaveText('Key Features');
  });

  test('Feature cards have icons', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Each feature card should have an icon
    const icons = page.locator('.feature-card .feature-icon');
    await expect(icons).toHaveCount(6);
  });
});
