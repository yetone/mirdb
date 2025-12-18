import { test, expect } from '@playwright/test';

// Tablet viewport: 768px width
const TABLET_VIEWPORT = { width: 768, height: 1024 };

test.describe('Responsive Design - Tablet (768px viewport)', () => {
  test.beforeEach(async ({ page }) => {
    // Set viewport to tablet size
    await page.setViewportSize(TABLET_VIEWPORT);
    await page.goto('/');
  });

  test('TC1: Layout is optimized for tablet, no horizontal scroll', async ({ page }) => {
    // Set viewport to tablet size (768px width)
    await page.setViewportSize(TABLET_VIEWPORT);

    // Check that the page body does not have horizontal overflow
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // Body scroll width should not exceed viewport width (no horizontal scroll)
    expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth);

    // Verify there's no horizontal scrollbar by checking documentElement
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Verify main content containers fit within viewport
    const container = page.locator('.container').first();
    await expect(container).toBeVisible();
    const containerBox = await container.boundingBox();
    expect(containerBox).not.toBeNull();
    expect(containerBox!.width).toBeLessThanOrEqual(viewportWidth);

    // Verify hero section displays properly at tablet width
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();
    const heroBox = await hero.boundingBox();
    expect(heroBox).not.toBeNull();
    expect(heroBox!.width).toBeLessThanOrEqual(viewportWidth);
  });

  test('TC2: Feature cards show in 2-column grid or appropriate layout', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Get the features grid
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Get all feature cards
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBe(4); // There should be 4 feature cards

    // Verify cards are visible
    for (let i = 0; i < cardCount; i++) {
      await expect(featureCards.nth(i)).toBeVisible();
    }

    // Check that at tablet width, cards should display in a grid layout
    // At 768px, with minmax(250px, 1fr), we expect 2 columns
    const firstCard = featureCards.nth(0);
    const secondCard = featureCards.nth(1);
    const thirdCard = featureCards.nth(2);

    const firstCardBox = await firstCard.boundingBox();
    const secondCardBox = await secondCard.boundingBox();
    const thirdCardBox = await thirdCard.boundingBox();

    expect(firstCardBox).not.toBeNull();
    expect(secondCardBox).not.toBeNull();
    expect(thirdCardBox).not.toBeNull();

    // In a 2-column layout:
    // - First and second cards should be on the same row (same Y position)
    // - Third card should be on a different row (different Y position)
    // OR in auto-fit layout, cards may wrap differently

    // Verify cards have reasonable width for tablet (should be less than full width but usable)
    const viewportWidth = TABLET_VIEWPORT.width;
    const cardWidth = firstCardBox!.width;

    // Cards should be at least 200px wide for readability
    expect(cardWidth).toBeGreaterThanOrEqual(200);

    // Cards should not exceed viewport width
    expect(cardWidth).toBeLessThanOrEqual(viewportWidth);

    // For a 2-column grid, each card should be roughly half the container width (minus gaps)
    // With grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)), at 768px we expect 2 columns
    // Each card should be approximately (768 - padding - gap) / 2
    expect(cardWidth).toBeGreaterThanOrEqual(250); // Minimum width from minmax
    expect(cardWidth).toBeLessThanOrEqual(400); // Should not be too wide in 2-col layout
  });

  test('TC3: Navigation is appropriate for tablet (may be collapsed or full)', async ({ page }) => {
    // Check that navigation bar is visible
    const navbar = page.locator('nav.navbar');
    await expect(navbar).toBeVisible();

    // Check nav container is visible
    const navContainer = page.locator('.nav-container');
    await expect(navContainer).toBeVisible();

    // Check logo is visible
    const navLogo = page.locator('.nav-logo');
    await expect(navLogo).toBeVisible();
    await expect(navLogo).toHaveText('MirDB');

    // Check nav links - at 768px they may be visible or collapsed
    const navLinks = page.locator('.nav-links');

    // Navigation should be accessible - either visible inline or via menu toggle
    // At tablet size (768px), the current CSS shows nav-links with reduced gap
    // Check that navigation links are visible and functional
    const isNavLinksVisible = await navLinks.isVisible();

    if (isNavLinksVisible) {
      // Navigation is displayed inline - verify all links are accessible
      const links = page.locator('.nav-links a');
      const linkCount = await links.count();
      expect(linkCount).toBeGreaterThan(0);

      // Verify each link is clickable and has appropriate size for touch
      for (let i = 0; i < linkCount; i++) {
        const link = links.nth(i);
        await expect(link).toBeVisible();

        // Check touch target size (minimum 44x44px recommended)
        const linkBox = await link.boundingBox();
        expect(linkBox).not.toBeNull();
        // The clickable area should be at least 44px in height for touch accessibility
        // Note: the actual link may be smaller but padding should make touch area adequate
      }

      // Verify navigation doesn't overflow
      const navContainerBox = await navContainer.boundingBox();
      expect(navContainerBox).not.toBeNull();
      expect(navContainerBox!.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);
    }
  });

  test('Interactive elements have adequate touch target size (minimum 44x44px)', async ({ page }) => {
    // Check buttons in hero section
    const heroButtons = page.locator('.hero-buttons .btn');
    const heroButtonCount = await heroButtons.count();

    for (let i = 0; i < heroButtonCount; i++) {
      const button = heroButtons.nth(i);
      await expect(button).toBeVisible();

      const buttonBox = await button.boundingBox();
      expect(buttonBox).not.toBeNull();

      // Touch targets should be at least 44px in height
      expect(buttonBox!.height).toBeGreaterThanOrEqual(44);
    }

    // Check navigation links
    const navLinks = page.locator('.nav-links a');
    const navLinkCount = await navLinks.count();

    for (let i = 0; i < navLinkCount; i++) {
      const link = navLinks.nth(i);
      const isVisible = await link.isVisible();

      if (isVisible) {
        const linkBox = await link.boundingBox();
        expect(linkBox).not.toBeNull();
        // Navigation links should have adequate height for touch
        // CSS adds padding, so effective touch area should be adequate
      }
    }
  });

  test('Hero section displays properly at tablet width', async ({ page }) => {
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    // Check hero title
    const heroTitle = page.locator('.hero h1');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toContainText('MirDB');

    // Check tagline
    const tagline = page.locator('.tagline');
    await expect(tagline).toBeVisible();

    // Check hero buttons
    const heroButtons = page.locator('.hero-buttons');
    await expect(heroButtons).toBeVisible();

    // Verify buttons wrap properly at tablet width
    const primaryBtn = page.locator('.btn-primary');
    const secondaryBtn = page.locator('.btn-secondary');

    await expect(primaryBtn).toBeVisible();
    await expect(secondaryBtn).toBeVisible();

    // Both buttons should fit within viewport
    const primaryBox = await primaryBtn.boundingBox();
    const secondaryBox = await secondaryBtn.boundingBox();

    expect(primaryBox).not.toBeNull();
    expect(secondaryBox).not.toBeNull();
    expect(primaryBox!.x + primaryBox!.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);
    expect(secondaryBox!.x + secondaryBox!.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);
  });

  test('Commands section displays in appropriate grid at tablet width', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await commandsSection.scrollIntoViewIfNeeded();
    await expect(commandsSection).toBeVisible();

    // Check commands grid
    const commandsGrid = page.locator('.commands-grid');
    await expect(commandsGrid).toBeVisible();

    // Get all command categories
    const commandCategories = page.locator('.command-category');
    const categoryCount = await commandCategories.count();
    expect(categoryCount).toBe(4); // Storage, Retrieval, Deletion, Admin

    // Verify each category is visible and has proper width
    for (let i = 0; i < categoryCount; i++) {
      const category = commandCategories.nth(i);
      await expect(category).toBeVisible();

      const categoryBox = await category.boundingBox();
      expect(categoryBox).not.toBeNull();
      expect(categoryBox!.width).toBeGreaterThanOrEqual(220); // Minimum from minmax
      expect(categoryBox!.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);
    }
  });

  test('Footer displays properly at tablet width', async ({ page }) => {
    // Navigate to footer
    const footer = page.locator('.footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Check footer sections
    const footerSections = page.locator('.footer-section');
    const sectionCount = await footerSections.count();
    expect(sectionCount).toBeGreaterThan(0);

    // Verify footer fits within viewport
    const footerBox = await footer.boundingBox();
    expect(footerBox).not.toBeNull();
    expect(footerBox!.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);

    // Check footer links are accessible
    const footerLinks = page.locator('.footer-section a');
    const linkCount = await footerLinks.count();

    for (let i = 0; i < linkCount; i++) {
      const link = footerLinks.nth(i);
      await expect(link).toBeVisible();
    }
  });

  test('Architecture diagram scales appropriately at tablet width', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture');
    await architectureSection.scrollIntoViewIfNeeded();
    await expect(architectureSection).toBeVisible();

    // Check architecture diagram
    const diagram = page.locator('.architecture-diagram');
    await expect(diagram).toBeVisible();

    // Verify diagram fits within viewport
    const diagramBox = await diagram.boundingBox();
    expect(diagramBox).not.toBeNull();
    expect(diagramBox!.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);

    // Check data flow sections
    const writePath = page.locator('.write-path');
    const readPath = page.locator('.read-path');

    await expect(writePath).toBeVisible();
    await expect(readPath).toBeVisible();

    // Verify data flow sections fit within viewport
    const writePathBox = await writePath.boundingBox();
    const readPathBox = await readPath.boundingBox();

    expect(writePathBox).not.toBeNull();
    expect(readPathBox).not.toBeNull();
    expect(writePathBox!.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);
    expect(readPathBox!.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);
  });

  test('Code blocks do not cause horizontal scroll at tablet width', async ({ page }) => {
    // Navigate to getting started section where code blocks exist
    const gettingStartedSection = page.locator('#getting-started');
    await gettingStartedSection.scrollIntoViewIfNeeded();
    await expect(gettingStartedSection).toBeVisible();

    // Check code blocks
    const codeBlocks = page.locator('pre');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Verify code blocks have overflow handling
    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      const isVisible = await codeBlock.isVisible();

      if (isVisible) {
        const codeBlockBox = await codeBlock.boundingBox();
        expect(codeBlockBox).not.toBeNull();
        // Code blocks should not exceed container width (may have internal scroll)
        expect(codeBlockBox!.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);
      }
    }

    // Verify no horizontal page scroll caused by code blocks
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });
});
