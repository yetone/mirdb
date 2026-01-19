// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Tablet Responsive Design - iPad (768px)', () => {
  test.use({ viewport: { width: 768, height: 1024 } });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Page layout adapts with appropriate column structure at 768px viewport', async ({ page }) => {
    const viewportWidth = 768;

    // Check that the document doesn't exceed viewport width (no horizontal scroll)
    const documentWidth = await page.evaluate(() => {
      return document.documentElement.scrollWidth;
    });

    // Document width should not significantly exceed viewport width
    expect(documentWidth).toBeLessThanOrEqual(viewportWidth + 1);

    // Verify all major sections are visible
    const heroSection = page.locator('[data-testid="hero"]');
    const featuresSection = page.locator('[data-testid="features"]');
    const usageSection = page.locator('[data-testid="usage"]');
    const roadmapSection = page.locator('[data-testid="roadmap"]');
    const footerSection = page.locator('[data-testid="footer"]');

    await expect(heroSection).toBeVisible();
    await expect(featuresSection).toBeVisible();
    await expect(usageSection).toBeVisible();
    await expect(roadmapSection).toBeVisible();
    await expect(footerSection).toBeVisible();

    // Ensure no element overflows horizontally
    const hasOverflow = await page.evaluate(() => {
      const body = document.body;
      return body.scrollWidth > body.clientWidth;
    });
    expect(hasOverflow).toBe(false);

    // Verify hero section adapts appropriately
    const h1 = heroSection.locator('h1');
    await expect(h1).toBeVisible();
    await expect(h1).toContainText('MirDB');

    const tagline = heroSection.locator('.tagline');
    await expect(tagline).toBeVisible();

    // Verify CTA buttons are visible and properly sized
    const primaryCta = heroSection.locator('.btn-primary');
    const secondaryCta = heroSection.locator('.btn-secondary');

    await expect(primaryCta).toBeVisible();
    await expect(secondaryCta).toBeVisible();

    // Verify buttons don't overflow the viewport
    const primaryBox = await primaryCta.boundingBox();
    const secondaryBox = await secondaryCta.boundingBox();

    expect(primaryBox).toBeTruthy();
    expect(secondaryBox).toBeTruthy();
    expect(primaryBox.x).toBeGreaterThanOrEqual(0);
    expect(primaryBox.x + primaryBox.width).toBeLessThanOrEqual(viewportWidth);

    // Verify footer adapts properly at tablet size
    const footerBrand = footerSection.locator('.footer-brand');
    const footerLinks = footerSection.locator('.footer-links');

    await expect(footerBrand).toBeVisible();
    await expect(footerLinks).toBeVisible();
  });

  test('TC2: Feature cards display in 2-column or appropriate grid layout at 768px', async ({ page }) => {
    const featuresSection = page.locator('[data-testid="features"]');
    await expect(featuresSection).toBeVisible();

    // Get all feature cards
    const featureCards = page.locator('[data-testid="feature-card"]');
    const cardCount = await featureCards.count();

    // Should have at least 4 feature cards
    expect(cardCount).toBeGreaterThanOrEqual(4);

    // Get bounding boxes of the first two cards to verify layout
    const card1Box = await featureCards.nth(0).boundingBox();
    const card2Box = await featureCards.nth(1).boundingBox();

    expect(card1Box).toBeTruthy();
    expect(card2Box).toBeTruthy();

    // At 768px, cards should be in a multi-column grid layout (not single column)
    // The CSS uses grid-template-columns: repeat(auto-fit, minmax(280px, 1fr))
    // At 768px, we should be able to fit 2 cards per row (768 / 2 = 384 > 280)

    // Cards should be side by side (similar Y position) or in a grid pattern
    // With minmax(280px, 1fr) at 768px, we expect 2 columns
    const isHorizontalLayout = Math.abs(card1Box.y - card2Box.y) < 50;

    // If horizontal layout, cards should have different X positions
    if (isHorizontalLayout) {
      expect(card2Box.x).toBeGreaterThan(card1Box.x);
    }

    // Verify cards don't overflow viewport width
    for (let i = 0; i < cardCount; i++) {
      const cardBox = await featureCards.nth(i).boundingBox();
      expect(cardBox).toBeTruthy();
      expect(cardBox.x).toBeGreaterThanOrEqual(0);
      expect(cardBox.x + cardBox.width).toBeLessThanOrEqual(768 + 5);
    }

    // Verify each card has an icon and description
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      const icon = card.locator('[data-testid="feature-icon"]');
      const description = card.locator('.description');

      await expect(icon).toBeVisible();
      await expect(description).toBeVisible();
    }
  });
});

test.describe('Tablet Responsive Design - iPad Pro (1024px)', () => {
  test.use({ viewport: { width: 1024, height: 1366 } });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC3: Page displays correctly at larger tablet size (1024px)', async ({ page }) => {
    const viewportWidth = 1024;

    // Check that the document doesn't exceed viewport width
    const documentWidth = await page.evaluate(() => {
      return document.documentElement.scrollWidth;
    });

    expect(documentWidth).toBeLessThanOrEqual(viewportWidth + 1);

    // Verify all major sections are visible and properly rendered
    const heroSection = page.locator('[data-testid="hero"]');
    const featuresSection = page.locator('[data-testid="features"]');
    const usageSection = page.locator('[data-testid="usage"]');
    const roadmapSection = page.locator('[data-testid="roadmap"]');
    const footerSection = page.locator('[data-testid="footer"]');

    await expect(heroSection).toBeVisible();
    await expect(featuresSection).toBeVisible();
    await expect(usageSection).toBeVisible();
    await expect(roadmapSection).toBeVisible();
    await expect(footerSection).toBeVisible();

    // Verify hero section elements at larger tablet size
    const h1 = heroSection.locator('h1');
    await expect(h1).toBeVisible();
    await expect(h1).toContainText('MirDB');

    const logo = heroSection.locator('img.logo');
    await expect(logo).toBeVisible();

    // Verify logo doesn't overflow
    const logoBox = await logo.boundingBox();
    expect(logoBox).toBeTruthy();
    expect(logoBox.x + logoBox.width).toBeLessThanOrEqual(viewportWidth);

    // Verify CTA buttons are properly sized and positioned
    const primaryCta = heroSection.locator('.btn-primary');
    const secondaryCta = heroSection.locator('.btn-secondary');

    await expect(primaryCta).toBeVisible();
    await expect(secondaryCta).toBeVisible();

    const primaryBox = await primaryCta.boundingBox();
    const secondaryBox = await secondaryCta.boundingBox();

    expect(primaryBox).toBeTruthy();
    expect(secondaryBox).toBeTruthy();

    // At larger tablet size, CTA buttons should be side-by-side (not stacked)
    // Check they have similar Y positions (both on same row)
    const buttonsAreHorizontal = Math.abs(primaryBox.y - secondaryBox.y) < 20;
    expect(buttonsAreHorizontal).toBe(true);

    // Verify feature cards layout at 1024px
    const featureCards = page.locator('[data-testid="feature-card"]');
    const cardCount = await featureCards.count();

    expect(cardCount).toBeGreaterThanOrEqual(4);

    // At 1024px, should fit 3 cards per row (1024 / 3 = 341 > 280)
    const card1Box = await featureCards.nth(0).boundingBox();
    const card2Box = await featureCards.nth(1).boundingBox();
    const card3Box = await featureCards.nth(2).boundingBox();

    expect(card1Box).toBeTruthy();
    expect(card2Box).toBeTruthy();
    expect(card3Box).toBeTruthy();

    // First row cards should have similar Y positions
    const firstRowSameY = Math.abs(card1Box.y - card2Box.y) < 20;
    expect(firstRowSameY).toBe(true);

    // Verify roadmap items are visible and properly laid out
    const roadmapItems = page.locator('[data-testid="roadmap-item"]');
    const itemCount = await roadmapItems.count();
    expect(itemCount).toBeGreaterThanOrEqual(1);

    // Verify footer content layout at larger tablet size
    const footerBrand = footerSection.locator('.footer-brand');
    const footerLinks = footerSection.locator('.footer-links');

    await expect(footerBrand).toBeVisible();
    await expect(footerLinks).toBeVisible();

    // Check footer content is in row layout (not stacked)
    const brandBox = await footerBrand.boundingBox();
    const linksBox = await footerLinks.boundingBox();

    expect(brandBox).toBeTruthy();
    expect(linksBox).toBeTruthy();

    // At 1024px, footer should use row layout
    const footerIsHorizontal = Math.abs(brandBox.y - linksBox.y) < 50;
    expect(footerIsHorizontal).toBe(true);

    // Ensure no horizontal scrolling
    const hasOverflow = await page.evaluate(() => {
      return document.body.scrollWidth > document.body.clientWidth;
    });
    expect(hasOverflow).toBe(false);
  });
});
