// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Responsive Design - Tablet (768px)', () => {
  test.beforeEach(async ({ page }) => {
    // Set tablet viewport (768px width)
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/');
  });

  // Test Case 1: Page renders without horizontal scroll at 768px viewport
  test('TC1: page renders without horizontal scroll at 768px viewport', async ({ page }) => {
    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');

    // Get the body and viewport dimensions
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // Body width should not exceed viewport width (no horizontal scroll)
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth);

    // Verify no horizontal overflow on HTML element
    const htmlScrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    expect(htmlScrollWidth).toBeLessThanOrEqual(viewportWidth);

    // Check that there is no horizontal scrollbar by comparing clientWidth to scrollWidth
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });

  // Test Case 2: Feature cards adjust to 2-column or single-column layout at 768px
  test('TC2: feature cards adjust to appropriate layout at 768px', async ({ page }) => {
    // Wait for the features section to be visible
    const featuresSection = page.locator('#features, .features');
    await expect(featuresSection).toBeVisible();

    // Get the feature cards
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBe(4); // Should have 4 feature cards

    // Get the features grid
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Check the computed grid layout
    const gridStyle = await featuresGrid.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        display: computed.display,
        gridTemplateColumns: computed.gridTemplateColumns
      };
    });

    // Should be using grid display
    expect(gridStyle.display).toBe('grid');

    // At 768px, the CSS should apply single-column layout (grid-template-columns: 1fr)
    // The grid-template-columns should be a single value or "1fr"
    const columns = gridStyle.gridTemplateColumns.split(' ').filter(c => c.trim() !== '');

    // At 768px width (below the 768px media query breakpoint),
    // grid-template-columns should be 1fr (single column)
    // or auto-fit with minmax may result in 2 columns depending on card width
    expect(columns.length).toBeGreaterThanOrEqual(1);
    expect(columns.length).toBeLessThanOrEqual(2);

    // Verify cards are stacked properly by checking their positions
    const firstCard = featureCards.first();
    const secondCard = featureCards.nth(1);

    const firstCardBox = await firstCard.boundingBox();
    const secondCardBox = await secondCard.boundingBox();

    expect(firstCardBox).not.toBeNull();
    expect(secondCardBox).not.toBeNull();

    // In single-column or 2-column layout, cards should fit within the viewport
    expect(firstCardBox.x).toBeGreaterThanOrEqual(0);
    expect(firstCardBox.x + firstCardBox.width).toBeLessThanOrEqual(768);
  });

  // Test Case 3: Navigation remains accessible and usable at 768px
  test('TC3: navigation remains accessible and usable at 768px', async ({ page }) => {
    // Check that the hero section CTA buttons are visible and usable
    const ctaButtons = page.locator('.cta-buttons');
    await expect(ctaButtons).toBeVisible();

    // Check Get Started button (specifically in the CTA buttons container)
    const getStartedBtn = ctaButtons.locator('a.btn-primary');
    await expect(getStartedBtn).toBeVisible();

    // Check GitHub button (specifically in the hero section CTA buttons)
    const githubBtn = ctaButtons.locator('a.btn-secondary');
    await expect(githubBtn).toBeVisible();

    // Verify buttons are clickable (have valid href)
    const getStartedHref = await getStartedBtn.getAttribute('href');
    expect(getStartedHref).toBeTruthy();

    const githubHref = await githubBtn.getAttribute('href');
    expect(githubHref).toContain('github.com');

    // Check button dimensions - should be reasonably sized for touch
    const getStartedBox = await getStartedBtn.boundingBox();
    expect(getStartedBox).not.toBeNull();
    expect(getStartedBox.height).toBeGreaterThanOrEqual(40); // Min touch target

    // Verify footer navigation links are accessible
    const footerLinks = page.locator('.footer-links a');
    const footerLinkCount = await footerLinks.count();
    expect(footerLinkCount).toBeGreaterThanOrEqual(1);

    // Check that footer links are visible
    for (let i = 0; i < footerLinkCount; i++) {
      await expect(footerLinks.nth(i)).toBeVisible();
    }

    // Test that Get Started button scrolls to quickstart section
    await getStartedBtn.click();
    await page.waitForTimeout(500); // Wait for smooth scroll

    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeInViewport();
  });

  // Test Case 4: Code blocks are scrollable if too wide at 768px
  test('TC4: code blocks are scrollable if too wide at 768px', async ({ page }) => {
    // Navigate to quick start section where code blocks are
    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeVisible();
    await quickStartSection.scrollIntoViewIfNeeded();

    // Get all code blocks
    const codeBlocks = page.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Check that code blocks have overflow-x: auto for scrolling
    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      await expect(codeBlock).toBeVisible();

      const overflowStyle = await codeBlock.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return computed.overflowX;
      });

      // overflow-x should be 'auto' or 'scroll' for scrollable code blocks
      expect(['auto', 'scroll']).toContain(overflowStyle);
    }

    // Verify code blocks don't overflow their container
    const firstCodeBlock = codeBlocks.first();
    const codeBlockBox = await firstCodeBlock.boundingBox();

    expect(codeBlockBox).not.toBeNull();
    // Code block should fit within viewport width
    expect(codeBlockBox.x).toBeGreaterThanOrEqual(0);
    expect(codeBlockBox.x + codeBlockBox.width).toBeLessThanOrEqual(768 + 20); // Allow small margin

    // Check that pre elements inside code blocks are properly contained
    const preElements = page.locator('.code-block pre');
    const preCount = await preElements.count();

    for (let i = 0; i < Math.min(preCount, 3); i++) {
      const pre = preElements.nth(i);
      await pre.scrollIntoViewIfNeeded();

      // Pre element might be wider than container, but container should scroll
      const preBox = await pre.boundingBox();
      expect(preBox).not.toBeNull();
    }
  });

  // Additional test: Verify section headings adapt to tablet width
  test('section headings are properly sized for tablet viewport', async ({ page }) => {
    // Check that all major section headings are visible and properly sized
    const headings = page.locator('h2');
    const headingCount = await headings.count();
    expect(headingCount).toBeGreaterThan(0);

    for (let i = 0; i < headingCount; i++) {
      const heading = headings.nth(i);
      await heading.scrollIntoViewIfNeeded();
      await expect(heading).toBeVisible();

      // Verify heading doesn't overflow viewport
      const headingBox = await heading.boundingBox();
      expect(headingBox).not.toBeNull();
      expect(headingBox.x).toBeGreaterThanOrEqual(0);
      expect(headingBox.x + headingBox.width).toBeLessThanOrEqual(768);
    }
  });

  // Additional test: Verify commands grid adapts properly
  test('commands grid adapts to tablet viewport', async ({ page }) => {
    const commandsSection = page.locator('#commands, .commands-section');
    await expect(commandsSection).toBeVisible();
    await commandsSection.scrollIntoViewIfNeeded();

    const commandsGrid = page.locator('.commands-grid');
    await expect(commandsGrid).toBeVisible();

    // Check grid layout
    const gridStyle = await commandsGrid.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return computed.gridTemplateColumns;
    });

    // Grid should adapt to viewport - auto-fit with minmax(280px, 1fr)
    // At 768px, this should result in 2 columns or fewer
    const columns = gridStyle.split(' ').filter(c => c.trim() !== '');
    expect(columns.length).toBeLessThanOrEqual(3);

    // Verify command categories don't overflow
    const commandCategories = page.locator('.command-category');
    const categoryCount = await commandCategories.count();

    for (let i = 0; i < categoryCount; i++) {
      const category = commandCategories.nth(i);
      const categoryBox = await category.boundingBox();
      expect(categoryBox).not.toBeNull();
      expect(categoryBox.x + categoryBox.width).toBeLessThanOrEqual(768);
    }
  });

  // Additional test: Verify roadmap grid adapts properly
  test('roadmap grid adapts to tablet viewport', async ({ page }) => {
    const roadmapSection = page.locator('#roadmap, .roadmap');
    await expect(roadmapSection).toBeVisible();
    await roadmapSection.scrollIntoViewIfNeeded();

    const roadmapGrid = page.locator('.roadmap-grid');
    await expect(roadmapGrid).toBeVisible();

    // At 768px (matching the media query), roadmap should be single column
    const gridStyle = await roadmapGrid.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return computed.gridTemplateColumns;
    });

    // Should be single column at 768px width
    const columns = gridStyle.split(' ').filter(c => c.trim() !== '');
    expect(columns.length).toBeLessThanOrEqual(2);

    // Verify roadmap columns don't overflow
    const roadmapColumns = page.locator('.roadmap-column');
    const columnCount = await roadmapColumns.count();

    for (let i = 0; i < columnCount; i++) {
      const column = roadmapColumns.nth(i);
      const columnBox = await column.boundingBox();
      expect(columnBox).not.toBeNull();
      expect(columnBox.x + columnBox.width).toBeLessThanOrEqual(768);
    }
  });
});
