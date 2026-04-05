/**
 * Features Section E2E Tests
 * Owner: Scenario 2 - Features Section Display
 */
import { test, expect } from '@playwright/test';

test.describe('Features Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should render exactly 3 feature cards', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check for exactly 3 feature cards
    const featureCards = featuresSection.locator('.features__card');
    await expect(featureCards).toHaveCount(3);
  });

  test('should display Protocol Compatibility feature with icon and description', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Look for Protocol Compatibility or Memcached Protocol feature
    const protocolCard = featuresSection.locator('.features__card[data-feature="protocol"]');
    await expect(protocolCard).toBeVisible();

    // Verify title contains expected text
    const cardTitle = protocolCard.locator('.features__card-title');
    await expect(cardTitle).toHaveText('Memcached Protocol');

    // Verify icon exists
    const icon = protocolCard.locator('.features__icon');
    await expect(icon).toBeVisible();
    const iconSvg = icon.locator('svg');
    await expect(iconSvg).toBeVisible();

    // Verify description exists and is not empty
    const description = protocolCard.locator('.features__card-description');
    await expect(description).toBeVisible();
    const descText = await description.textContent();
    expect(descText).toBeTruthy();
    expect(descText!.length).toBeGreaterThan(10);
  });

  test('should display Data Persistence feature with icon and description', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Look for Persistence feature
    const persistenceCard = featuresSection.locator('.features__card[data-feature="persistence"]');
    await expect(persistenceCard).toBeVisible();

    // Verify title contains "Persistence"
    const cardTitle = persistenceCard.locator('.features__card-title');
    await expect(cardTitle).toContainText('Persistence');

    // Verify icon exists
    const icon = persistenceCard.locator('.features__icon');
    await expect(icon).toBeVisible();
    const iconSvg = icon.locator('svg');
    await expect(iconSvg).toBeVisible();

    // Verify description exists and is not empty
    const description = persistenceCard.locator('.features__card-description');
    await expect(description).toBeVisible();
    const descText = await description.textContent();
    expect(descText).toBeTruthy();
    expect(descText!.length).toBeGreaterThan(10);
  });

  test('should display LSM Tree feature with icon and description', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Look for LSM Tree feature
    const lsmCard = featuresSection.locator('.features__card[data-feature="lsm"]');
    await expect(lsmCard).toBeVisible();

    // Verify title contains "LSM"
    const cardTitle = lsmCard.locator('.features__card-title');
    await expect(cardTitle).toContainText('LSM');

    // Verify icon exists
    const icon = lsmCard.locator('.features__icon');
    await expect(icon).toBeVisible();
    const iconSvg = icon.locator('svg');
    await expect(iconSvg).toBeVisible();

    // Verify description exists and is not empty
    const description = lsmCard.locator('.features__card-description');
    await expect(description).toBeVisible();
    const descText = await description.textContent();
    expect(descText).toBeTruthy();
    expect(descText!.length).toBeGreaterThan(10);
  });

  test('should display feature cards in three-column layout on desktop viewport', async ({ page }) => {
    // Set desktop viewport (>= 1024px)
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto('/');

    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Get the features grid
    const featuresGrid = featuresSection.locator('.features__grid');
    await expect(featuresGrid).toBeVisible();

    // Get all feature cards
    const featureCards = featuresSection.locator('.features__card');
    await expect(featureCards).toHaveCount(3);

    // Get bounding boxes of all cards
    const card1 = featureCards.nth(0);
    const card2 = featureCards.nth(1);
    const card3 = featureCards.nth(2);

    const box1 = await card1.boundingBox();
    const box2 = await card2.boundingBox();
    const box3 = await card3.boundingBox();

    expect(box1).not.toBeNull();
    expect(box2).not.toBeNull();
    expect(box3).not.toBeNull();

    // Verify cards are in a horizontal row (same Y position, different X positions)
    // Allow small tolerance for Y alignment
    const tolerance = 5;
    expect(Math.abs(box1!.y - box2!.y)).toBeLessThan(tolerance);
    expect(Math.abs(box2!.y - box3!.y)).toBeLessThan(tolerance);

    // Verify cards are positioned horizontally (increasing X positions)
    expect(box2!.x).toBeGreaterThan(box1!.x);
    expect(box3!.x).toBeGreaterThan(box2!.x);
  });

  test('should have documentation link in features section', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check for documentation link
    const docsLink = featuresSection.locator('.features__docs-link');
    await expect(docsLink).toBeVisible();

    // Verify link has valid href
    const href = await docsLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toContain('doc');
  });
});
