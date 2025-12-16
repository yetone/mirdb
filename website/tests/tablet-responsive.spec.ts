import { test, expect } from '@playwright/test';

test.describe('Responsive Design - Tablet', () => {
  // Tablet viewport width (common iPad width)
  const TABLET_VIEWPORT_WIDTH = 768;
  const TABLET_VIEWPORT_HEIGHT = 1024;

  test.beforeEach(async ({ page }) => {
    // Set viewport to tablet dimensions (768px width)
    await page.setViewportSize({
      width: TABLET_VIEWPORT_WIDTH,
      height: TABLET_VIEWPORT_HEIGHT,
    });
    await page.goto('/');
  });

  test('TC1: Page displays correctly without layout issues at 768px width', async ({ page }) => {
    // Set viewport to 768px width and load page
    // Expected: Page displays correctly without layout issues

    // Check that the document width doesn't exceed viewport width (no horizontal scrollbar)
    const hasHorizontalScrollbar = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });

    expect(hasHorizontalScrollbar).toBe(false);

    // Verify page loads successfully
    const title = await page.title();
    expect(title).toContain('MirDB');

    // Check main elements are present and visible
    const heroSection = page.locator('.hero-section');
    await expect(heroSection).toBeVisible();

    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();
  });

  test('TC2: Features display in appropriate grid (2-3 columns) on tablet', async ({ page }) => {
    // Check feature grid layout on tablet
    // Expected: Features display in appropriate grid (2-3 columns)

    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    // Should have 7 feature cards
    expect(cardCount).toBe(7);

    // Get positions of first few cards to determine column layout
    const firstCard = featureCards.nth(0);
    const secondCard = featureCards.nth(1);
    const thirdCard = featureCards.nth(2);

    const firstBox = await firstCard.boundingBox();
    const secondBox = await secondCard.boundingBox();
    const thirdBox = await thirdCard.boundingBox();

    expect(firstBox).not.toBeNull();
    expect(secondBox).not.toBeNull();
    expect(thirdBox).not.toBeNull();

    if (firstBox && secondBox && thirdBox) {
      // Check if cards are on the same row (indicating multi-column layout)
      // Cards should be on the same row if their Y positions are similar (within tolerance)
      const sameLine = Math.abs(firstBox.y - secondBox.y) < 10;

      // On tablet (768px), we expect 2 columns based on minmax(300px, 1fr) grid
      // This is because 768px can fit 2x300px columns with gaps
      expect(sameLine).toBe(true);

      // The third card should be on a different row or same row depending on viewport
      // With 768px viewport and minmax(300px, 1fr), we should get 2 columns
      // So third card should be on a new row
      const thirdOnNewRow = thirdBox.y > firstBox.y + firstBox.height / 2;
      expect(thirdOnNewRow).toBe(true);
    }

    // Verify cards fit within viewport width
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      await card.scrollIntoViewIfNeeded();
      const box = await card.boundingBox();
      expect(box).not.toBeNull();
      if (box) {
        // Card should fit within viewport with some margin
        expect(box.width).toBeLessThanOrEqual(TABLET_VIEWPORT_WIDTH);
        // Card should have reasonable width for tablet
        expect(box.width).toBeGreaterThanOrEqual(280);
      }
    }
  });

  test('TC3: Code blocks do not overflow container and are scrollable if needed', async ({ page }) => {
    // Verify code blocks fit within viewport
    // Expected: Code blocks do not overflow container and are scrollable if needed

    const quickstartSection = page.locator('#quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    const codeBlocks = page.locator('.code-block');
    const codeCount = await codeBlocks.count();

    // Should have code blocks in quickstart section
    expect(codeCount).toBeGreaterThan(0);

    for (let i = 0; i < codeCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      await codeBlock.scrollIntoViewIfNeeded();
      await expect(codeBlock).toBeVisible();

      const box = await codeBlock.boundingBox();
      expect(box).not.toBeNull();

      if (box) {
        // Code block should not exceed viewport width
        expect(box.width).toBeLessThanOrEqual(TABLET_VIEWPORT_WIDTH);
      }

      // Verify code block has overflow-x auto for horizontal scrolling
      const overflowX = await codeBlock.evaluate((el) => {
        return window.getComputedStyle(el).overflowX;
      });
      expect(['auto', 'scroll']).toContain(overflowX);

      // Check that code block content doesn't overflow its container visually
      const overflowsContainer = await codeBlock.evaluate((el) => {
        return el.scrollWidth > el.clientWidth;
      });

      // If content overflows, it should be scrollable (which we verified above)
      // This is expected behavior for code blocks with long lines
    }
  });

  test('TC4: Hero content is well-proportioned for tablet viewport', async ({ page }) => {
    // Check hero section layout on tablet
    // Expected: Hero content is well-proportioned for tablet viewport

    const heroSection = page.locator('.hero-section');
    await expect(heroSection).toBeVisible();

    const heroBox = await heroSection.boundingBox();
    expect(heroBox).not.toBeNull();

    if (heroBox) {
      // Hero section should span full viewport width
      expect(heroBox.width).toBe(TABLET_VIEWPORT_WIDTH);
    }

    // Check hero title is visible and properly sized
    const heroTitle = page.locator('.hero-section h1');
    await expect(heroTitle).toBeVisible();

    const titleFontSize = await heroTitle.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // Title should be reasonably sized for tablet (not too small, not too large)
    expect(titleFontSize).toBeGreaterThanOrEqual(36); // At least 36px
    expect(titleFontSize).toBeLessThanOrEqual(80); // Not larger than 80px

    // Check tagline is visible and readable
    const tagline = page.locator('.tagline');
    await expect(tagline).toBeVisible();

    const taglineFontSize = await tagline.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    expect(taglineFontSize).toBeGreaterThanOrEqual(16); // At least 16px for readability

    // Check CTA buttons are visible and well-positioned
    const ctaButtons = page.locator('.cta-buttons');
    await expect(ctaButtons).toBeVisible();

    const ctaBox = await ctaButtons.boundingBox();
    expect(ctaBox).not.toBeNull();

    if (ctaBox) {
      // CTA buttons should fit within viewport
      expect(ctaBox.width).toBeLessThanOrEqual(TABLET_VIEWPORT_WIDTH);
    }

    // Verify buttons are side by side (not stacked) on tablet
    const primaryButton = page.locator('.cta-primary');
    const secondaryButton = page.locator('.cta-secondary');

    const primaryBox = await primaryButton.boundingBox();
    const secondaryBox = await secondaryButton.boundingBox();

    expect(primaryBox).not.toBeNull();
    expect(secondaryBox).not.toBeNull();

    if (primaryBox && secondaryBox) {
      // On tablet, buttons should be on the same row (y positions similar)
      const buttonsOnSameRow = Math.abs(primaryBox.y - secondaryBox.y) < 10;
      expect(buttonsOnSameRow).toBe(true);
    }
  });

  test('TC5: Architecture section displays properly on tablet', async ({ page }) => {
    // Additional test for architecture section layout
    const architectureSection = page.locator('#architecture');
    await architectureSection.scrollIntoViewIfNeeded();
    await expect(architectureSection).toBeVisible();

    // Check diagram fits within viewport
    const diagram = page.locator('.architecture-diagram');
    await expect(diagram).toBeVisible();

    const diagramBox = await diagram.boundingBox();
    expect(diagramBox).not.toBeNull();
    if (diagramBox) {
      expect(diagramBox.width).toBeLessThanOrEqual(TABLET_VIEWPORT_WIDTH);
    }

    // Check explanation cards
    const explanationCards = page.locator('.explanation-card');
    const cardCount = await explanationCards.count();
    expect(cardCount).toBeGreaterThan(0);

    // On tablet (768px), with minmax(350px, 1fr) grid, cards should stack vertically
    // because 768px can't fit 2x350px cards with padding
    for (let i = 0; i < cardCount; i++) {
      const card = explanationCards.nth(i);
      await card.scrollIntoViewIfNeeded();
      const box = await card.boundingBox();
      expect(box).not.toBeNull();
      if (box) {
        expect(box.width).toBeLessThanOrEqual(TABLET_VIEWPORT_WIDTH);
      }
    }
  });

  test('TC6: Project status section displays properly on tablet', async ({ page }) => {
    // Test project status section layout
    const statusSection = page.locator('#project-status');
    await statusSection.scrollIntoViewIfNeeded();
    await expect(statusSection).toBeVisible();

    const statusGrid = page.locator('.status-grid');
    await expect(statusGrid).toBeVisible();

    const statusCategories = page.locator('.status-category');
    const categoryCount = await statusCategories.count();
    expect(categoryCount).toBe(2); // Completed and Planned

    // Check categories fit within viewport
    for (let i = 0; i < categoryCount; i++) {
      const category = statusCategories.nth(i);
      await category.scrollIntoViewIfNeeded();
      const box = await category.boundingBox();
      expect(box).not.toBeNull();
      if (box) {
        expect(box.width).toBeLessThanOrEqual(TABLET_VIEWPORT_WIDTH);
        // Categories should have reasonable width on tablet
        expect(box.width).toBeGreaterThanOrEqual(300);
      }
    }
  });

  test('TC7: All content remains readable and accessible on tablet', async ({ page }) => {
    // Verify content readability
    // Expected: No content should be cut off or overlapping

    // Check all main sections are visible and properly displayed
    const sections = [
      { selector: '.hero-section', name: 'Hero Section' },
      { selector: '#features', name: 'Features Section' },
      { selector: '#architecture', name: 'Architecture Section' },
      { selector: '#quickstart', name: 'Quick Start Section' },
      { selector: '#project-status', name: 'Project Status Section' },
    ];

    for (const section of sections) {
      const sectionElement = page.locator(section.selector);
      await sectionElement.scrollIntoViewIfNeeded();
      await expect(sectionElement).toBeVisible();

      const box = await sectionElement.boundingBox();
      expect(box).not.toBeNull();
      if (box) {
        // Section should fit within viewport width
        expect(box.width).toBeLessThanOrEqual(TABLET_VIEWPORT_WIDTH);
      }
    }

    // Check that section titles are visible
    const sectionTitles = page.locator('.section-title');
    const titleCount = await sectionTitles.count();
    expect(titleCount).toBeGreaterThan(0);

    for (let i = 0; i < titleCount; i++) {
      const title = sectionTitles.nth(i);
      await title.scrollIntoViewIfNeeded();
      await expect(title).toBeVisible();

      // Check font size is readable
      const fontSize = await title.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      expect(fontSize).toBeGreaterThanOrEqual(24); // At least 24px for section titles
    }
  });

  test('TC8: Commands grid displays properly on tablet', async ({ page }) => {
    // Test commands grid layout
    const quickstartSection = page.locator('#quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    const commandsGrid = page.locator('.commands-grid');
    await expect(commandsGrid).toBeVisible();

    const commandCategories = page.locator('.command-category');
    const categoryCount = await commandCategories.count();
    expect(categoryCount).toBe(3); // Getter, Setter, Other commands

    // Check command categories fit within viewport
    for (let i = 0; i < categoryCount; i++) {
      const category = commandCategories.nth(i);
      await category.scrollIntoViewIfNeeded();
      const box = await category.boundingBox();
      expect(box).not.toBeNull();
      if (box) {
        expect(box.width).toBeLessThanOrEqual(TABLET_VIEWPORT_WIDTH);
      }
    }
  });
});
