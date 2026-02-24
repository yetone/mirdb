/**
 * Features Section Integration Tests
 * Owner: Scenario 3 - Features Grid Section
 *
 * Test cases:
 * - Four feature cards are rendered
 * - Each feature has icon and description
 * - Memcached Protocol feature present
 * - Persistence feature present
 * - LSM Tree feature present
 * - Rust Implementation feature present
 * - Hover effects on feature cards
 */

import { test, expect } from '@playwright/test';

test.describe('Features Grid Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Four feature cards are rendered in a grid layout', async ({ page }) => {
    // Check features section exists
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check that exactly four feature cards are rendered
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(4);

    // Check grid layout is applied
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toHaveClass(/grid/);
    await expect(featuresGrid).toHaveClass(/grid-4/);
  });

  test('TC2: Memcached Protocol feature card with icon and description present', async ({ page }) => {
    // Find the Memcached feature card
    const memcachedCard = page.locator('.feature-card[data-feature="memcached"]');
    await expect(memcachedCard).toBeVisible();

    // Check icon is present
    const icon = memcachedCard.locator('.feature-icon img');
    await expect(icon).toBeVisible();
    await expect(icon).toHaveAttribute('src', /memcached\.svg/);

    // Check title
    const title = memcachedCard.locator('.feature-title');
    await expect(title).toContainText('Memcached Protocol');

    // Check description
    const description = memcachedCard.locator('.feature-description');
    await expect(description).toBeVisible();
    await expect(description).not.toBeEmpty();
  });

  test('TC3: Persistence (SSTables) feature card with icon and description present', async ({ page }) => {
    // Find the Persistence feature card
    const persistenceCard = page.locator('.feature-card[data-feature="persistence"]');
    await expect(persistenceCard).toBeVisible();

    // Check icon is present
    const icon = persistenceCard.locator('.feature-icon img');
    await expect(icon).toBeVisible();
    await expect(icon).toHaveAttribute('src', /persistence\.svg/);

    // Check title contains both Persistence and SSTables
    const title = persistenceCard.locator('.feature-title');
    await expect(title).toContainText('Persistence');
    await expect(title).toContainText('SSTables');

    // Check description
    const description = persistenceCard.locator('.feature-description');
    await expect(description).toBeVisible();
    await expect(description).not.toBeEmpty();
  });

  test('TC4: LSM Tree Architecture feature card with icon and description present', async ({ page }) => {
    // Find the LSM Tree feature card
    const lsmCard = page.locator('.feature-card[data-feature="lsm-tree"]');
    await expect(lsmCard).toBeVisible();

    // Check icon is present
    const icon = lsmCard.locator('.feature-icon img');
    await expect(icon).toBeVisible();
    await expect(icon).toHaveAttribute('src', /lsm-tree\.svg/);

    // Check title
    const title = lsmCard.locator('.feature-title');
    await expect(title).toContainText('LSM Tree Architecture');

    // Check description
    const description = lsmCard.locator('.feature-description');
    await expect(description).toBeVisible();
    await expect(description).not.toBeEmpty();
  });

  test('TC5: Rust Implementation feature card with icon and description present', async ({ page }) => {
    // Find the Rust feature card
    const rustCard = page.locator('.feature-card[data-feature="rust"]');
    await expect(rustCard).toBeVisible();

    // Check icon is present
    const icon = rustCard.locator('.feature-icon img');
    await expect(icon).toBeVisible();
    await expect(icon).toHaveAttribute('src', /rust\.svg/);

    // Check title
    const title = rustCard.locator('.feature-title');
    await expect(title).toContainText('Rust Implementation');

    // Check description
    const description = rustCard.locator('.feature-description');
    await expect(description).toBeVisible();
    await expect(description).not.toBeEmpty();
  });

  test('TC6: Card displays hover effect (scale, shadow, or color change)', async ({ page }) => {
    // Get a feature card
    const featureCard = page.locator('.feature-card').first();
    await expect(featureCard).toBeVisible();

    // Get the initial computed styles
    const initialTransform = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });
    const initialBoxShadow = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).boxShadow;
    });

    // Hover over the card
    await featureCard.hover();

    // Wait for CSS transition to complete
    await page.waitForTimeout(300);

    // Get the styles after hover
    const hoverTransform = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });
    const hoverBoxShadow = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).boxShadow;
    });

    // Check that at least one style changed (transform or box-shadow)
    const transformChanged = initialTransform !== hoverTransform;
    const shadowChanged = initialBoxShadow !== hoverBoxShadow;

    expect(transformChanged || shadowChanged).toBe(true);
  });

  test('Feature section has proper heading hierarchy', async ({ page }) => {
    // Check h2 title exists
    const sectionTitle = page.locator('#features-title');
    await expect(sectionTitle).toBeVisible();
    await expect(sectionTitle).toHaveText('Features');

    // Check feature card titles are h3
    const featureH3s = page.locator('.feature-card h3.feature-title');
    await expect(featureH3s).toHaveCount(4);
  });

  test('Feature section is accessible with ARIA attributes', async ({ page }) => {
    // Check section has aria-labelledby
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toHaveAttribute('aria-labelledby', 'features-title');

    // Check icons are decorative (aria-hidden)
    const icons = page.locator('.feature-icon img');
    const iconCount = await icons.count();
    for (let i = 0; i < iconCount; i++) {
      await expect(icons.nth(i)).toHaveAttribute('aria-hidden', 'true');
    }
  });

  test('Feature cards use article element for semantic markup', async ({ page }) => {
    // Each feature card should be an article
    const articles = page.locator('#features article.feature-card');
    await expect(articles).toHaveCount(4);
  });

  test('Feature descriptions mention key capabilities', async ({ page }) => {
    // Memcached - should mention drop-in replacement or compatibility
    const memcachedDesc = page.locator('.feature-card[data-feature="memcached"] .feature-description');
    await expect(memcachedDesc).toContainText(/drop-in|replacement|clients/i);

    // Persistence - should mention durable or storage
    const persistenceDesc = page.locator('.feature-card[data-feature="persistence"] .feature-description');
    await expect(persistenceDesc).toContainText(/durable|storage|persist|survives/i);

    // LSM Tree - should mention write or compaction
    const lsmDesc = page.locator('.feature-card[data-feature="lsm-tree"] .feature-description');
    await expect(lsmDesc).toContainText(/write|compaction|merge/i);

    // Rust - should mention safety or performance
    const rustDesc = page.locator('.feature-card[data-feature="rust"] .feature-description');
    await expect(rustDesc).toContainText(/safety|performance|fast/i);
  });
});
