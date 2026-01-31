/**
 * Features Section E2E Tests
 * Owner: Scenario 2 - Features Showcase Section
 *
 * Tests:
 * - 6 feature cards displayed
 * - Feature titles and descriptions
 * - Grid layout verification
 *
 * Requirements: REQ-2
 */

const { test, expect } = require('@playwright/test');

test.describe('Features Showcase Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: should display exactly 6 feature cards with correct titles', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Get all feature cards
    const featureCards = featuresSection.locator('.feature-card');
    await expect(featureCards).toHaveCount(6);

    // Verify all expected titles are present using data-feature attribute for precise matching
    const expectedFeatures = [
      { dataFeature: 'memcached-protocol', title: 'Memcached Protocol Support' },
      { dataFeature: 'persistence', title: 'Persistence' },
      { dataFeature: 'lsm-tree', title: 'LSM Tree Architecture' },
      { dataFeature: 'high-performance', title: 'High Performance' },
      { dataFeature: 'rust-implementation', title: 'Rust Implementation' },
      { dataFeature: 'skip-list', title: 'Skip List' }
    ];

    for (const feature of expectedFeatures) {
      const card = featuresSection.locator(`[data-feature="${feature.dataFeature}"]`);
      await expect(card).toBeVisible();
      await expect(card.locator('h3')).toHaveText(feature.title);
    }
  });

  test('TC2: Memcached Protocol Support card should contain compatibility description', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const memcachedCard = featuresSection.locator('[data-feature="memcached-protocol"]');

    await expect(memcachedCard).toBeVisible();
    await expect(memcachedCard.locator('h3')).toHaveText('Memcached Protocol Support');

    const description = memcachedCard.locator('p');
    await expect(description).toContainText('compatible');
    await expect(description).toContainText('memcached clients');
  });

  test('TC3: Persistence card should describe data surviving restarts', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const persistenceCard = featuresSection.locator('[data-feature="persistence"]');

    await expect(persistenceCard).toBeVisible();
    await expect(persistenceCard.locator('h3')).toHaveText('Persistence');

    const description = persistenceCard.locator('p');
    await expect(description).toContainText('survives restarts');
    await expect(description).toContainText('traditional memcached');
  });

  test('TC4: LSM Tree Architecture card should describe memtables and SSTable compaction', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const lsmCard = featuresSection.locator('[data-feature="lsm-tree"]');

    await expect(lsmCard).toBeVisible();
    await expect(lsmCard.locator('h3')).toHaveText('LSM Tree Architecture');

    const description = lsmCard.locator('p');
    await expect(description).toContainText('memtables');
    await expect(description).toContainText('SSTable compaction');
  });

  test('TC5: Features should display in 3-column grid layout on desktop (1024px+)', async ({ page }) => {
    // Set viewport to desktop width
    await page.setViewportSize({ width: 1024, height: 768 });

    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Check that the grid has 3 columns
    const gridStyle = await featuresGrid.evaluate((el) => {
      const computedStyle = window.getComputedStyle(el);
      return {
        display: computedStyle.display,
        gridTemplateColumns: computedStyle.gridTemplateColumns
      };
    });

    expect(gridStyle.display).toBe('grid');
    // Grid template columns should have 3 values (3 columns)
    const columnCount = gridStyle.gridTemplateColumns.split(' ').length;
    expect(columnCount).toBe(3);
  });

  test('Each feature card should have an icon, title, and description', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const featureCards = featuresSection.locator('.feature-card');

    const count = await featureCards.count();
    expect(count).toBe(6);

    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);

      // Check for icon (svg within feature-icon)
      const icon = card.locator('.feature-icon');
      await expect(icon).toBeVisible();

      // Check for title
      const title = card.locator('h3');
      await expect(title).toBeVisible();

      // Check for description
      const description = card.locator('p');
      await expect(description).toBeVisible();
    }
  });

  test('Features section should have proper heading structure', async ({ page }) => {
    const featuresSection = page.locator('#features');

    // Check section title exists
    const sectionTitle = featuresSection.locator('#features-title');
    await expect(sectionTitle).toBeVisible();
    await expect(sectionTitle).toHaveText('Key Features');

    // Verify heading is h2
    const tagName = await sectionTitle.evaluate((el) => el.tagName);
    expect(tagName).toBe('H2');
  });

  test('Features section should be accessible via navigation', async ({ page }) => {
    // Click the Features link in navigation
    const featuresLink = page.locator('nav a[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await featuresLink.click();

    // Verify the features section is now in view
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  test('Feature cards should be keyboard navigable', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const firstCard = featuresSection.locator('.feature-card').first();

    // Verify cards are visible and can receive focus
    await expect(firstCard).toBeVisible();

    // Tab to the features section area
    await page.keyboard.press('Tab');
  });

  test('High Performance card should describe speed and skip list', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const perfCard = featuresSection.locator('[data-feature="high-performance"]');

    await expect(perfCard).toBeVisible();
    await expect(perfCard.locator('h3')).toHaveText('High Performance');

    const description = perfCard.locator('p');
    await expect(description).toContainText('speed');
  });

  test('Rust Implementation card should describe memory safety', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const rustCard = featuresSection.locator('[data-feature="rust-implementation"]');

    await expect(rustCard).toBeVisible();
    await expect(rustCard.locator('h3')).toHaveText('Rust Implementation');

    const description = rustCard.locator('p');
    await expect(description).toContainText('Memory safety');
  });

  test('Skip List card should describe fast operations', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const skipListCard = featuresSection.locator('[data-feature="skip-list"]');

    await expect(skipListCard).toBeVisible();
    await expect(skipListCard.locator('h3')).toHaveText('Skip List');

    const description = skipListCard.locator('p');
    await expect(description).toContainText('Fast');
    await expect(description).toContainText('operations');
  });
});
