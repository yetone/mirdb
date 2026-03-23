/**
 * Features Section Tests
 * Owner: Scenario 2 - Key Features Showcase Section
 *
 * Test cases:
 * - Memcached protocol feature card
 * - Disk persistence feature card
 * - LSM tree architecture feature card
 * - Rust implementation feature card
 * - Grid/card layout consistency
 */
const { test, expect } = require('@playwright/test');

test.describe('Features Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('http://localhost:3000/index.html');
  });

  test('should display Memcached protocol feature with SET, GET, DELETE commands', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the Memcached protocol feature card by its title
    const memcachedCard = featuresSection.locator('.features__card').filter({
      has: page.locator('.features__card-title, h3', { hasText: /^Memcached Protocol$/i })
    });
    await expect(memcachedCard).toBeVisible();

    // Verify it has a title
    const cardTitle = memcachedCard.locator('.features__card-title, h3');
    await expect(cardTitle).toBeVisible();

    // Verify description mentions SET, GET, DELETE commands
    const cardDescription = memcachedCard.locator('.features__card-description, p');
    await expect(cardDescription).toBeVisible();

    const descriptionText = await memcachedCard.textContent();
    expect(descriptionText.toLowerCase()).toMatch(/set|get|delete/i);
  });

  test('should display disk persistence feature with durability benefits', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the disk persistence feature card by its title
    const persistenceCard = featuresSection.locator('.features__card').filter({
      has: page.locator('.features__card-title, h3', { hasText: /^Disk Persistence$/i })
    });
    await expect(persistenceCard).toBeVisible();

    // Verify it has a title
    const cardTitle = persistenceCard.locator('.features__card-title, h3');
    await expect(cardTitle).toBeVisible();

    // Verify description mentions durability or persistence benefits
    const cardText = await persistenceCard.textContent();
    expect(cardText.toLowerCase()).toMatch(/persist|durabl|disk|stor|surviv/i);
  });

  test('should display LSM tree feature with WAL, Memtable, and SSTables', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the LSM tree feature card by its title
    const lsmCard = featuresSection.locator('.features__card').filter({
      has: page.locator('.features__card-title, h3', { hasText: /LSM Tree/i })
    });
    await expect(lsmCard).toBeVisible();

    // Verify it has a title
    const cardTitle = lsmCard.locator('.features__card-title, h3');
    await expect(cardTitle).toBeVisible();

    // Verify description mentions WAL, Memtable, or SSTables
    const cardText = await lsmCard.textContent();
    const lowerText = cardText.toLowerCase();
    const hasArchitectureTerms =
      lowerText.includes('wal') ||
      lowerText.includes('write-ahead') ||
      lowerText.includes('memtable') ||
      lowerText.includes('sstable');
    expect(hasArchitectureTerms).toBeTruthy();
  });

  test('should display Rust implementation feature with safety, performance, or async', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the Rust implementation feature card by its title
    const rustCard = featuresSection.locator('.features__card').filter({
      has: page.locator('.features__card-title, h3', { hasText: /Rust|Built with Rust/i })
    });
    await expect(rustCard).toBeVisible();

    // Verify it has a title
    const cardTitle = rustCard.locator('.features__card-title, h3');
    await expect(cardTitle).toBeVisible();

    // Verify description mentions safety, performance, or async/Tokio
    const cardText = await rustCard.textContent();
    const lowerText = cardText.toLowerCase();
    const hasRustBenefits =
      lowerText.includes('safe') ||
      lowerText.includes('performance') ||
      lowerText.includes('async') ||
      lowerText.includes('tokio') ||
      lowerText.includes('fast') ||
      lowerText.includes('memory');
    expect(hasRustBenefits).toBeTruthy();
  });

  test('should display features in grid or card layout with consistent styling', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify there's a grid container for the cards
    const cardsGrid = featuresSection.locator('.features__grid');
    await expect(cardsGrid).toBeVisible();

    // Verify there are at least 4 feature cards (one for each key feature)
    const featureCards = featuresSection.locator('.features__card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(4);

    // Verify each card has consistent structure (title and description)
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      await expect(card).toBeVisible();

      // Each card should have a title
      const title = card.locator('.features__card-title, h3');
      await expect(title).toBeVisible();

      // Each card should have a description
      const description = card.locator('.features__card-description, p');
      await expect(description).toBeVisible();
    }

    // Verify grid layout by checking CSS display property
    const gridDisplay = await cardsGrid.evaluate(el => {
      const styles = window.getComputedStyle(el);
      return styles.display;
    });
    expect(['grid', 'flex']).toContain(gridDisplay);
  });
});
