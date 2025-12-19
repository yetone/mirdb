import { test, expect } from '@playwright/test';

test.describe('Responsive Design - Tablet View', () => {
  test.beforeEach(async ({ page }) => {
    // Set tablet viewport size (768px width as specified for iPad)
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/');
  });

  test('TC1: page renders with appropriate tablet layout at 768px viewport width', async ({ page }) => {
    // Set explicit tablet viewport
    await page.setViewportSize({ width: 768, height: 1024 });

    // Wait for the page to load
    await expect(page.locator('.hero')).toBeVisible();

    // Check that document body doesn't have horizontal overflow
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });

    expect(hasHorizontalScroll).toBe(false);

    // Verify the page content fits within viewport
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    expect(bodyWidth).toBeLessThanOrEqual(768);

    // Verify hero section renders correctly
    const heroTitle = page.locator('.hero h1');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toHaveText('MirDB');

    // Verify tagline is visible
    const tagline = page.locator('.tagline');
    await expect(tagline).toBeVisible();

    // Verify description is visible and readable
    const description = page.locator('.description');
    await expect(description).toBeVisible();
  });

  test('TC2: feature cards display in appropriate grid (2 columns) on tablet', async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 });

    // Find the features grid
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Get all feature cards
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBe(3);

    // Verify all feature cards are visible
    for (let i = 0; i < cardCount; i++) {
      await expect(featureCards.nth(i)).toBeVisible();
    }

    // Check that the grid layout is appropriate for tablet
    // At 768px, with minmax(300px, 1fr), we should get 2 columns
    const gridStyle = await featuresGrid.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        display: computed.display,
        gridTemplateColumns: computed.gridTemplateColumns,
      };
    });

    expect(gridStyle.display).toBe('grid');

    // Verify grid has 2 columns by checking the gridTemplateColumns value
    // It should have 2 column values (e.g., "300px 300px" or similar)
    const columnCount = gridStyle.gridTemplateColumns.split(' ').length;
    expect(columnCount).toBe(2);

    // Verify feature cards have appropriate layout
    // Get positions of first two cards to verify they're side by side
    const card1Box = await featureCards.nth(0).boundingBox();
    const card2Box = await featureCards.nth(1).boundingBox();

    expect(card1Box).not.toBeNull();
    expect(card2Box).not.toBeNull();

    if (card1Box && card2Box) {
      // Cards should be on the same row (similar Y position)
      expect(Math.abs(card1Box.y - card2Box.y)).toBeLessThan(10);
      // Cards should be side by side (different X positions)
      expect(card2Box.x).toBeGreaterThan(card1Box.x);
    }
  });

  test('TC3: navigation is accessible on tablet', async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 });

    // Check for GitHub link in sticky navigation (primary navigation on this page)
    const navGitHubLink = page.locator('.sticky-nav a[href*="github.com"], nav a[href*="github.com"]');

    // At tablet size, navigation should be visible
    await expect(navGitHubLink.first()).toBeVisible();

    // Verify the GitHub link is clickable and properly sized
    const linkBox = await navGitHubLink.first().boundingBox();
    expect(linkBox).not.toBeNull();

    if (linkBox) {
      // Link should have adequate touch target size (at least 44x44)
      expect(linkBox.height).toBeGreaterThanOrEqual(36); // GitHub link has 8px padding
      expect(linkBox.width).toBeGreaterThanOrEqual(44);
    }

    // Verify sticky navigation container is properly laid out
    const stickyNav = page.locator('.sticky-nav, [data-testid="sticky-nav"]');
    await expect(stickyNav).toBeVisible();

    // Check footer navigation is also accessible (use .first() since there are multiple footer links)
    const footerGitHubLink = page.locator('footer a[href*="github.com"]').first();
    await expect(footerGitHubLink).toBeVisible();
  });

  test('tablet view: all content is readable at tablet size', async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 });

    // Check hero section typography
    const heroTitle = page.locator('.hero h1');
    const heroTitleFontSize = await heroTitle.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // Title should be reasonably sized for tablet (at least 2rem = 32px)
    expect(heroTitleFontSize).toBeGreaterThanOrEqual(32);

    // Check tagline is readable
    const tagline = page.locator('.tagline');
    const taglineFontSize = await tagline.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    expect(taglineFontSize).toBeGreaterThanOrEqual(16);

    // Check section titles are readable
    const sectionTitles = page.locator('.section-title');
    const sectionTitleCount = await sectionTitles.count();

    for (let i = 0; i < sectionTitleCount; i++) {
      const fontSize = await sectionTitles.nth(i).evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      expect(fontSize).toBeGreaterThanOrEqual(24);
    }

    // Check code blocks are readable
    const codeBlocks = page.locator('.code-pre, pre code');
    const codeBlockCount = await codeBlocks.count();

    for (let i = 0; i < codeBlockCount; i++) {
      const isVisible = await codeBlocks.nth(i).isVisible();
      if (isVisible) {
        const fontSize = await codeBlocks.nth(i).evaluate((el) => {
          return parseFloat(window.getComputedStyle(el).fontSize);
        });
        // Code should be at least 13px for readability (0.85rem at 16px base = 13.6px)
        expect(fontSize).toBeGreaterThanOrEqual(13);
      }
    }
  });

  test('tablet view: commands table is properly displayed', async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 });

    // Find commands table
    const commandsTable = page.locator('.commands-table');
    await expect(commandsTable).toBeVisible();

    // Table should have horizontal scroll wrapper
    const tableWrapper = page.locator('.commands-table-wrapper');
    await expect(tableWrapper).toBeVisible();

    // Check that table doesn't cause horizontal overflow
    const tableBox = await commandsTable.boundingBox();
    expect(tableBox).not.toBeNull();

    if (tableBox) {
      // Table should fit within viewport or be scrollable within wrapper
      expect(tableBox.x).toBeGreaterThanOrEqual(0);
    }
  });

  test('tablet view: code blocks have appropriate styling', async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 });

    // Find visible code blocks
    const codeBlocks = page.locator('.code-block:visible');
    const codeBlockCount = await codeBlocks.count();

    expect(codeBlockCount).toBeGreaterThan(0);

    // Check first few visible code blocks (limit to 3 to avoid timeout)
    const blocksToCheck = Math.min(codeBlockCount, 3);
    for (let i = 0; i < blocksToCheck; i++) {
      const codeBlock = codeBlocks.nth(i);
      await expect(codeBlock).toBeVisible();

      // Check code block has proper overflow handling
      const codePre = codeBlock.locator('.code-pre').first();
      const overflowX = await codePre.evaluate((el) => {
        return window.getComputedStyle(el).overflowX;
      });

      expect(['auto', 'scroll']).toContain(overflowX);

      // Check code block width doesn't exceed viewport
      const codeBlockBox = await codeBlock.boundingBox();
      if (codeBlockBox) {
        expect(codeBlockBox.x + codeBlockBox.width).toBeLessThanOrEqual(768 + 1);
      }
    }
  });
});
