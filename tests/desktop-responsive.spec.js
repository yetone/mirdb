// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Desktop Responsive Design - 1280px Standard Desktop', () => {
  test.use({ viewport: { width: 1280, height: 800 } });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Page displays full desktop layout with multi-column features grid at 1280px viewport', async ({ page }) => {
    const viewportWidth = 1280;

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

    // Verify hero section displays correctly with large desktop styling
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

    // Verify buttons are side-by-side at desktop size (not stacked)
    const primaryBox = await primaryCta.boundingBox();
    const secondaryBox = await secondaryCta.boundingBox();

    expect(primaryBox).toBeTruthy();
    expect(secondaryBox).toBeTruthy();

    // CTA buttons should have similar Y positions (both on same row)
    const buttonsAreHorizontal = Math.abs(primaryBox.y - secondaryBox.y) < 20;
    expect(buttonsAreHorizontal).toBe(true);

    // Verify feature cards layout - at 1280px should have 4 cards in a row or 2x2 grid
    const featureCards = page.locator('[data-testid="feature-card"]');
    const cardCount = await featureCards.count();

    expect(cardCount).toBeGreaterThanOrEqual(4);

    // Get bounding boxes of the feature cards to verify multi-column layout
    const card1Box = await featureCards.nth(0).boundingBox();
    const card2Box = await featureCards.nth(1).boundingBox();
    const card3Box = await featureCards.nth(2).boundingBox();

    expect(card1Box).toBeTruthy();
    expect(card2Box).toBeTruthy();
    expect(card3Box).toBeTruthy();

    // At 1280px with minmax(280px, 1fr), we should fit 4 cards per row
    // (1200 max-width - padding) / 280 = ~4 cards
    // First three cards should have similar Y positions (same row)
    const firstRowSameY =
      Math.abs(card1Box.y - card2Box.y) < 20 &&
      Math.abs(card2Box.y - card3Box.y) < 20;
    expect(firstRowSameY).toBe(true);

    // Cards should be in different X positions (horizontal layout)
    expect(card2Box.x).toBeGreaterThan(card1Box.x);
    expect(card3Box.x).toBeGreaterThan(card2Box.x);
  });
});

test.describe('Desktop Responsive Design - 1920px Full HD', () => {
  test.use({ viewport: { width: 1920, height: 1080 } });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC2: Page content is centered/constrained appropriately at 1920px viewport, no excessive stretching', async ({ page }) => {
    const viewportWidth = 1920;

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

    // Verify hero section elements at large viewport
    const h1 = heroSection.locator('h1');
    await expect(h1).toBeVisible();
    await expect(h1).toContainText('MirDB');

    const logo = heroSection.locator('img.logo');
    await expect(logo).toBeVisible();

    // Verify logo has max-width constraint (should not stretch excessively)
    const logoBox = await logo.boundingBox();
    expect(logoBox).toBeTruthy();
    expect(logoBox.width).toBeLessThanOrEqual(400); // CSS max-width: 400px

    // Verify features section container is centered and constrained
    const featuresContainer = featuresSection.locator('.container');
    const featuresContainerBox = await featuresContainer.boundingBox();

    expect(featuresContainerBox).toBeTruthy();
    // Container should be centered - check margins are roughly equal on both sides
    const leftMargin = featuresContainerBox.x;
    const rightMargin = viewportWidth - (featuresContainerBox.x + featuresContainerBox.width);

    // Margins should be roughly equal (within 10% tolerance for subpixel rendering)
    const marginDiff = Math.abs(leftMargin - rightMargin);
    expect(marginDiff).toBeLessThan(viewportWidth * 0.1);

    // Verify the container has max-width constraint (1200px)
    expect(featuresContainerBox.width).toBeLessThanOrEqual(1200 + 64); // 1200px + padding

    // Verify usage section is also constrained
    const usageContainer = usageSection.locator('.container');
    const usageContainerBox = await usageContainer.boundingBox();

    expect(usageContainerBox).toBeTruthy();
    expect(usageContainerBox.width).toBeLessThanOrEqual(1000 + 64); // 1000px max-width + padding

    // Verify roadmap section is constrained
    const roadmapContainer = roadmapSection.locator('.container');
    const roadmapContainerBox = await roadmapContainer.boundingBox();

    expect(roadmapContainerBox).toBeTruthy();
    expect(roadmapContainerBox.width).toBeLessThanOrEqual(800 + 64); // 800px max-width + padding

    // Verify footer is centered
    const footerContainer = footerSection.locator('.container');
    const footerContainerBox = await footerContainer.boundingBox();

    expect(footerContainerBox).toBeTruthy();
    const footerLeftMargin = footerContainerBox.x;
    const footerRightMargin = viewportWidth - (footerContainerBox.x + footerContainerBox.width);
    const footerMarginDiff = Math.abs(footerLeftMargin - footerRightMargin);
    expect(footerMarginDiff).toBeLessThan(viewportWidth * 0.1);

    // Ensure no horizontal scrolling
    const hasOverflow = await page.evaluate(() => {
      return document.body.scrollWidth > document.body.clientWidth;
    });
    expect(hasOverflow).toBe(false);
  });
});

test.describe('Desktop Responsive Design - Max-Width Constraints', () => {
  test.use({ viewport: { width: 1440, height: 900 } });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC3: Content has reasonable max-width constraints to maintain readability', async ({ page }) => {
    const viewportWidth = 1440;

    // Verify all sections have appropriate max-width constraints
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

    // Check hero tagline has max-width constraint (600px in CSS)
    const tagline = heroSection.locator('.tagline');
    await expect(tagline).toBeVisible();
    const taglineBox = await tagline.boundingBox();
    expect(taglineBox).toBeTruthy();
    expect(taglineBox.width).toBeLessThanOrEqual(600);

    // Check features container is constrained to 1200px
    const featuresContainer = featuresSection.locator('.container');
    const featuresBox = await featuresContainer.boundingBox();
    expect(featuresBox).toBeTruthy();
    expect(featuresBox.width).toBeLessThanOrEqual(1200 + 64); // max-width + padding

    // Check section descriptions have max-width constraints
    const featureSectionDesc = featuresSection.locator('.section-description');
    const featureDescBox = await featureSectionDesc.boundingBox();
    expect(featureDescBox).toBeTruthy();
    expect(featureDescBox.width).toBeLessThanOrEqual(600);

    // Check usage container is constrained to 1000px
    const usageContainer = usageSection.locator('.container');
    const usageBox = await usageContainer.boundingBox();
    expect(usageBox).toBeTruthy();
    expect(usageBox.width).toBeLessThanOrEqual(1000 + 64);

    // Check usage section description has max-width constraint (700px)
    const usageSectionDesc = usageSection.locator('.section-description');
    const usageDescBox = await usageSectionDesc.boundingBox();
    expect(usageDescBox).toBeTruthy();
    expect(usageDescBox.width).toBeLessThanOrEqual(700);

    // Check code block has max-width constraint (700px)
    const codeBlock = page.locator('[data-testid="code-block"]');
    const codeBlockBox = await codeBlock.boundingBox();
    expect(codeBlockBox).toBeTruthy();
    expect(codeBlockBox.width).toBeLessThanOrEqual(700);

    // Check roadmap container is constrained to 800px
    const roadmapContainer = roadmapSection.locator('.container');
    const roadmapBox = await roadmapContainer.boundingBox();
    expect(roadmapBox).toBeTruthy();
    expect(roadmapBox.width).toBeLessThanOrEqual(800 + 64);

    // Check roadmap section description has max-width constraint (600px)
    const roadmapSectionDesc = roadmapSection.locator('.section-description');
    const roadmapDescBox = await roadmapSectionDesc.boundingBox();
    expect(roadmapDescBox).toBeTruthy();
    expect(roadmapDescBox.width).toBeLessThanOrEqual(600);

    // Check footer container is constrained to 1200px
    const footerContainer = footerSection.locator('.container');
    const footerBox = await footerContainer.boundingBox();
    expect(footerBox).toBeTruthy();
    expect(footerBox.width).toBeLessThanOrEqual(1200 + 64);

    // Verify footer has horizontal layout at desktop size (not stacked)
    const footerBrand = footerSection.locator('.footer-brand');
    const footerLinks = footerSection.locator('.footer-links');

    await expect(footerBrand).toBeVisible();
    await expect(footerLinks).toBeVisible();

    const brandBox = await footerBrand.boundingBox();
    const linksBox = await footerLinks.boundingBox();

    expect(brandBox).toBeTruthy();
    expect(linksBox).toBeTruthy();

    // At desktop, footer should use row layout (similar Y positions)
    const footerIsHorizontal = Math.abs(brandBox.y - linksBox.y) < 50;
    expect(footerIsHorizontal).toBe(true);

    // Verify feature cards form multi-column grid
    const featureCards = page.locator('[data-testid="feature-card"]');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(4);

    // Check that feature cards are in multi-column layout
    const card1Box = await featureCards.nth(0).boundingBox();
    const card2Box = await featureCards.nth(1).boundingBox();

    expect(card1Box).toBeTruthy();
    expect(card2Box).toBeTruthy();

    // Cards should be side by side (similar Y, different X)
    const cardsAreHorizontal = Math.abs(card1Box.y - card2Box.y) < 20;
    expect(cardsAreHorizontal).toBe(true);
    expect(card2Box.x).toBeGreaterThan(card1Box.x);

    // Ensure no horizontal scrolling
    const hasOverflow = await page.evaluate(() => {
      return document.body.scrollWidth > document.body.clientWidth;
    });
    expect(hasOverflow).toBe(false);
  });
});
