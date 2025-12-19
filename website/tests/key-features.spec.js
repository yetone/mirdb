// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

// Base URL for the static HTML file
const BASE_URL = 'file://' + path.resolve(__dirname, '../dist/index.html');

test.describe('Key Features Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(BASE_URL);
  });

  test('Test Case 1: Check Memcached Compatible feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Check feature card
    const featureCard = page.locator('[data-testid="feature-memcached"]');
    await expect(featureCard).toBeVisible();

    // Check title
    const title = page.locator('[data-testid="feature-memcached-title"]');
    await expect(title).toBeVisible();
    await expect(title).toHaveText('Memcached Compatible');

    // Check description mentions drop-in replacement
    const description = page.locator('[data-testid="feature-memcached-description"]');
    await expect(description).toBeVisible();
    const text = await description.textContent();
    expect(text.toLowerCase()).toContain('drop-in replacement');
  });

  test('Test Case 2: Check Persistent Storage feature', async ({ page }) => {
    // Check feature card
    const featureCard = page.locator('[data-testid="feature-persistent"]');
    await expect(featureCard).toBeVisible();

    // Check title
    const title = page.locator('[data-testid="feature-persistent-title"]');
    await expect(title).toBeVisible();
    await expect(title).toHaveText('Persistent Storage');

    // Check description mentions SSTable architecture
    const description = page.locator('[data-testid="feature-persistent-description"]');
    await expect(description).toBeVisible();
    const text = await description.textContent();
    expect(text).toContain('SSTable');
  });

  test('Test Case 3: Check High Performance feature', async ({ page }) => {
    // Check feature card
    const featureCard = page.locator('[data-testid="feature-performance"]');
    await expect(featureCard).toBeVisible();

    // Check title
    const title = page.locator('[data-testid="feature-performance-title"]');
    await expect(title).toBeVisible();
    await expect(title).toHaveText('High Performance');

    // Check description mentions LSM tree optimization
    const description = page.locator('[data-testid="feature-performance-description"]');
    await expect(description).toBeVisible();
    const text = await description.textContent();
    expect(text).toContain('LSM tree');
  });

  test('Test Case 4: Check Simple Configuration feature', async ({ page }) => {
    // Check feature card
    const featureCard = page.locator('[data-testid="feature-config"]');
    await expect(featureCard).toBeVisible();

    // Check title
    const title = page.locator('[data-testid="feature-config-title"]');
    await expect(title).toBeVisible();
    await expect(title).toHaveText('Simple Configuration');

    // Check description mentions TOML-based configuration
    const description = page.locator('[data-testid="feature-config-description"]');
    await expect(description).toBeVisible();
    const text = await description.textContent();
    expect(text).toContain('TOML');
  });

  test('Test Case 5: Verify feature icons presence', async ({ page }) => {
    // Check all four feature icons exist and are visible
    const icons = [
      { testId: 'feature-memcached-icon', name: 'Memcached Compatible' },
      { testId: 'feature-persistent-icon', name: 'Persistent Storage' },
      { testId: 'feature-performance-icon', name: 'High Performance' },
      { testId: 'feature-config-icon', name: 'Simple Configuration' }
    ];

    for (const { testId, name } of icons) {
      const icon = page.locator(`[data-testid="${testId}"]`);
      await expect(icon, `${name} feature should have an icon`).toBeVisible();

      // Check that the icon contains an SVG
      const svg = icon.locator('svg');
      await expect(svg, `${name} icon should contain an SVG`).toBeVisible();
    }
  });
});
