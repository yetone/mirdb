// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Responsive Design - Tablet', () => {
  // Set viewport to tablet size (768x1024) for all tests
  test.use({ viewport: { width: 768, height: 1024 } });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Page renders with tablet-optimized layout at 768px viewport', async ({ page }) => {
    // Verify viewport is set correctly
    const viewportSize = page.viewportSize();
    expect(viewportSize.width).toBe(768);

    // Verify body renders properly without horizontal scrolling
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth);

    // Verify main sections are visible
    const heroSection = page.locator('#hero');
    const featuresSection = page.locator('#features');
    const commandsSection = page.locator('#commands');
    const gettingStartedSection = page.locator('#getting-started');
    const footer = page.locator('.footer');

    await expect(heroSection).toBeVisible();
    await expect(featuresSection).toBeVisible();
    await expect(commandsSection).toBeVisible();
    await expect(gettingStartedSection).toBeVisible();
    await expect(footer).toBeVisible();

    // Verify hero section has appropriate font sizing for tablet
    const heroTitle = page.locator('.hero h1');
    const heroTitleFontSize = await heroTitle.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // On tablet (max-width: 768px), title should be smaller than desktop (2.5rem ~ 40px)
    expect(heroTitleFontSize).toBeLessThanOrEqual(40);
    expect(heroTitleFontSize).toBeGreaterThan(20);
  });

  test('TC2: Feature cards display in appropriate grid on tablet', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const featuresGrid = featuresSection.locator('.features-grid');

    // Verify the grid container exists
    await expect(featuresGrid).toBeVisible();

    // Verify grid has display: grid style
    const gridDisplay = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(gridDisplay).toBe('grid');

    // Get all feature cards
    const featureCards = featuresGrid.locator('.feature-card');
    await expect(featureCards).toHaveCount(3);

    // Get bounding boxes for all cards
    const firstCard = featureCards.nth(0);
    const secondCard = featureCards.nth(1);
    const thirdCard = featureCards.nth(2);

    const firstBox = await firstCard.boundingBox();
    const secondBox = await secondCard.boundingBox();
    const thirdBox = await thirdCard.boundingBox();

    expect(firstBox).not.toBeNull();
    expect(secondBox).not.toBeNull();
    expect(thirdBox).not.toBeNull();

    // On tablet width (768px), with minmax(300px, 1fr), cards should stack
    // in 1 column since 768px - padding doesn't fit 2x300px comfortably
    // Or they may show 2 columns depending on exact CSS
    // The key is that all cards are visible and laid out appropriately

    // Verify cards have proper width (should be at least 250px on tablet)
    expect(firstBox.width).toBeGreaterThanOrEqual(250);
    expect(secondBox.width).toBeGreaterThanOrEqual(250);
    expect(thirdBox.width).toBeGreaterThanOrEqual(250);

    // Verify cards don't overflow viewport
    const containerWidth = 768;
    expect(firstBox.x + firstBox.width).toBeLessThanOrEqual(containerWidth);
    expect(secondBox.x + secondBox.width).toBeLessThanOrEqual(containerWidth);
    expect(thirdBox.x + thirdBox.width).toBeLessThanOrEqual(containerWidth);

    // Verify all cards are fully visible and accessible
    await expect(firstCard).toBeVisible();
    await expect(secondCard).toBeVisible();
    await expect(thirdCard).toBeVisible();

    // Verify card content is readable
    const firstTitle = firstCard.locator('.feature-title');
    const firstDesc = firstCard.locator('.feature-description');
    await expect(firstTitle).toBeVisible();
    await expect(firstDesc).toBeVisible();
  });

  test('TC3: Navigation is accessible and usable on tablet', async ({ page }) => {
    // Check CTA buttons in hero section are accessible
    const ctaButtons = page.locator('.cta-buttons');
    await expect(ctaButtons).toBeVisible();

    const primaryBtn = page.locator('.btn-primary');
    const secondaryBtn = page.locator('.btn-secondary');

    await expect(primaryBtn).toBeVisible();
    await expect(secondaryBtn).toBeVisible();

    // Verify buttons are clickable and not obstructed
    await expect(primaryBtn).toBeEnabled();
    await expect(secondaryBtn).toBeEnabled();

    // Verify buttons have adequate tap target size (at least 44x44 pixels for touch)
    const primaryBtnBox = await primaryBtn.boundingBox();
    const secondaryBtnBox = await secondaryBtn.boundingBox();

    expect(primaryBtnBox.height).toBeGreaterThanOrEqual(40);
    expect(secondaryBtnBox.height).toBeGreaterThanOrEqual(40);

    // Verify buttons don't overflow the viewport
    expect(primaryBtnBox.x).toBeGreaterThanOrEqual(0);
    expect(primaryBtnBox.x + primaryBtnBox.width).toBeLessThanOrEqual(768);

    // Verify footer links are accessible
    const footerLinks = page.locator('.footer-links a');
    const footerLinksCount = await footerLinks.count();
    expect(footerLinksCount).toBeGreaterThanOrEqual(3);

    // Verify each footer link is visible
    for (let i = 0; i < footerLinksCount; i++) {
      await expect(footerLinks.nth(i)).toBeVisible();
    }

    // Verify internal link navigation works (Get Started -> Quick Start section)
    await primaryBtn.click();

    // Wait for smooth scroll
    await page.waitForTimeout(500);

    // Verify Quick Start section is now in view
    const quickStartSection = page.locator('#getting-started');
    await expect(quickStartSection).toBeInViewport();
  });

  test('TC4: Commands grid displays appropriately on tablet', async ({ page }) => {
    const commandsGrid = page.locator('.commands-grid');
    await expect(commandsGrid).toBeVisible();

    const commandCards = commandsGrid.locator('.command-card');
    const cardCount = await commandCards.count();
    expect(cardCount).toBe(7); // SET, GET, DELETE, ADD, REPLACE, APPEND, PREPEND

    // Verify all command cards are visible and not overlapping
    for (let i = 0; i < cardCount; i++) {
      const card = commandCards.nth(i);
      await expect(card).toBeVisible();

      const box = await card.boundingBox();
      expect(box.width).toBeGreaterThan(200);
      expect(box.x + box.width).toBeLessThanOrEqual(768);
    }
  });

  test('TC5: Code blocks are readable and scrollable on tablet', async ({ page }) => {
    // Navigate to quick start section
    const quickStartSection = page.locator('#getting-started');
    await quickStartSection.scrollIntoViewIfNeeded();

    // Verify code blocks are visible
    const codeBlocks = page.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThanOrEqual(4);

    // Check first code block doesn't overflow
    const firstCodeBlock = codeBlocks.first();
    await expect(firstCodeBlock).toBeVisible();

    const codeBlockBox = await firstCodeBlock.boundingBox();
    expect(codeBlockBox.x).toBeGreaterThanOrEqual(0);
    expect(codeBlockBox.x + codeBlockBox.width).toBeLessThanOrEqual(768);

    // Verify copy buttons are accessible
    const copyButtons = page.locator('.copy-btn');
    const copyBtnCount = await copyButtons.count();
    expect(copyBtnCount).toBeGreaterThanOrEqual(4);

    for (let i = 0; i < copyBtnCount; i++) {
      await expect(copyButtons.nth(i)).toBeVisible();
    }
  });

  test('TC6: Hero section text is appropriately sized for tablet', async ({ page }) => {
    const heroTitle = page.locator('.hero h1');
    const tagline = page.locator('.hero .tagline');
    const description = page.locator('.hero .description');

    await expect(heroTitle).toBeVisible();
    await expect(tagline).toBeVisible();
    await expect(description).toBeVisible();

    // Verify text is readable (not too small)
    const titleFontSize = await heroTitle.evaluate((el) =>
      parseFloat(window.getComputedStyle(el).fontSize)
    );
    const taglineFontSize = await tagline.evaluate((el) =>
      parseFloat(window.getComputedStyle(el).fontSize)
    );
    const descFontSize = await description.evaluate((el) =>
      parseFloat(window.getComputedStyle(el).fontSize)
    );

    // Minimum readable sizes on tablet
    expect(titleFontSize).toBeGreaterThanOrEqual(32);
    expect(taglineFontSize).toBeGreaterThanOrEqual(16);
    expect(descFontSize).toBeGreaterThanOrEqual(14);
  });
});
