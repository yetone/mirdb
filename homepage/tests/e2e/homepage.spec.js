/**
 * Homepage E2E Tests
 * Owner: Multiple scenarios contribute to this file
 *
 * Test Sections:
 * - Hero section tests (Scenario 1)
 * - Features section tests (Scenario 2)
 * - Quick Start section tests (Scenario 3)
 * - Status section tests (Scenario 4)
 * - Theme toggle tests (Scenario 8)
 *
 * Framework: Playwright
 */

const { test, expect } = require('@playwright/test');

// ============================================
// Features Section Tests (Scenario 2)
// ============================================
test.describe('Features Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Features section with id="features" or role="region" is present', async ({ page }) => {
    // Check for section with id='features'
    const featuresSection = page.locator('section#features');
    await expect(featuresSection).toBeVisible();

    // Verify it has role="region" for accessibility
    await expect(featuresSection).toHaveAttribute('role', 'region');
  });

  test('TC2: Memcached Protocol Compatible feature is displayed', async ({ page }) => {
    const featuresSection = page.locator('section#features');

    // Check for feature card with Memcached Protocol
    const featureCard = featuresSection.locator('.feature-card', {
      has: page.locator('h3', { hasText: /memcached protocol/i })
    });
    await expect(featureCard).toBeVisible();

    // Verify it has a description about protocol compatibility
    const description = featureCard.locator('p');
    await expect(description).toContainText(/protocol|compatible|compatibility/i);
  });

  test('TC3: Persistent Storage feature is displayed', async ({ page }) => {
    const featuresSection = page.locator('section#features');

    // Check for feature card with Persistent Storage
    const featureCard = featuresSection.locator('.feature-card', {
      has: page.locator('h3', { hasText: /persistent/i })
    });
    await expect(featureCard).toBeVisible();

    // Verify it has description about storage
    const description = featureCard.locator('p');
    await expect(description).toContainText(/storage|durable|persist/i);
  });

  test('TC4: LSM Tree feature is displayed', async ({ page }) => {
    const featuresSection = page.locator('section#features');

    // Check for feature card mentioning LSM or Log-Structured Merge
    const featureCard = featuresSection.locator('.feature-card', {
      has: page.locator('h3', { hasText: /lsm|log-structured merge/i })
    });
    await expect(featureCard).toBeVisible();

    // Verify content mentions tree architecture
    const cardText = await featureCard.textContent();
    expect(cardText.toLowerCase()).toMatch(/lsm|log-structured merge/i);
  });

  test('TC5: Skiplist Memtable feature is displayed', async ({ page }) => {
    const featuresSection = page.locator('section#features');

    // Check for feature card mentioning Skiplist
    const featureCard = featuresSection.locator('.feature-card', {
      has: page.locator('h3', { hasText: /skiplist/i })
    });
    await expect(featureCard).toBeVisible();

    // Verify content mentions memtable
    const cardText = await featureCard.textContent();
    expect(cardText.toLowerCase()).toContain('memtable');
  });

  test('TC6: Compaction feature is displayed', async ({ page }) => {
    const featuresSection = page.locator('section#features');

    // Check for feature card mentioning Compaction
    const featureCard = featuresSection.locator('.feature-card', {
      has: page.locator('h3', { hasText: /compaction/i })
    });
    await expect(featureCard).toBeVisible();

    // Verify it describes minor and major compaction
    const cardText = await featureCard.textContent();
    expect(cardText.toLowerCase()).toMatch(/minor.*major|major.*minor|automatic/i);
  });

  test('TC7: Features are displayed in a grid or card layout', async ({ page }) => {
    const featuresSection = page.locator('section#features');
    await expect(featuresSection).toBeVisible();

    // Verify features-grid container exists
    const grid = featuresSection.locator('.features-grid');
    await expect(grid).toBeVisible();

    // Verify it uses CSS grid or flexbox (not plain list)
    const displayStyle = await grid.evaluate(el => {
      const computed = window.getComputedStyle(el);
      return computed.display;
    });
    expect(['grid', 'flex']).toContain(displayStyle);

    // Verify at least 5 feature cards exist
    const cards = grid.locator('.feature-card');
    const cardCount = await cards.count();
    expect(cardCount).toBeGreaterThanOrEqual(5);
  });

  test('Features section has h2 heading', async ({ page }) => {
    const heading = page.locator('section#features h2');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText(/features/i);
  });

  test('Each feature card has a title and description', async ({ page }) => {
    const cards = page.locator('section#features .feature-card');
    const cardCount = await cards.count();

    // Verify each card has h3 (title) and p (description)
    for (let i = 0; i < cardCount; i++) {
      const card = cards.nth(i);
      const title = card.locator('h3');
      const description = card.locator('p');

      await expect(title).toBeVisible();
      await expect(description).toBeVisible();

      // Ensure title has text
      const titleText = await title.textContent();
      expect(titleText.trim().length).toBeGreaterThan(0);

      // Ensure description has text
      const descText = await description.textContent();
      expect(descText.trim().length).toBeGreaterThan(0);
    }
  });
});
