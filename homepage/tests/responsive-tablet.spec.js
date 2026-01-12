// @ts-check
const { test, expect } = require('@playwright/test');

// Configure tests to use tablet viewport (768x1024)
test.describe('Responsive Design - Tablet Viewport', () => {
  test.use({
    viewport: { width: 768, height: 1024 }
  });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Layout adapts to tablet size, no horizontal scrollbar
  test('TC1: should have no horizontal scrollbar at 768x1024 tablet viewport', async ({ page }) => {
    // Verify the viewport dimensions are correct
    const viewportSize = page.viewportSize();
    expect(viewportSize?.width).toBe(768);
    expect(viewportSize?.height).toBe(1024);

    // Get the document's scroll width and client width
    const scrollInfo = await page.evaluate(() => {
      return {
        scrollWidth: document.documentElement.scrollWidth,
        clientWidth: document.documentElement.clientWidth,
        bodyScrollWidth: document.body.scrollWidth,
        bodyClientWidth: document.body.clientWidth,
        hasHorizontalScrollbar: document.documentElement.scrollWidth > document.documentElement.clientWidth
      };
    });

    // Verify no horizontal scrollbar (scroll width should not exceed client width)
    expect(scrollInfo.hasHorizontalScrollbar).toBe(false);
    expect(scrollInfo.scrollWidth).toBeLessThanOrEqual(scrollInfo.clientWidth + 1); // +1 for rounding

    // Verify hero section is visible and adapts
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Verify hero title adjusts for tablet
    const heroTitle = page.locator('.hero-title');
    await expect(heroTitle).toBeVisible();
    const titleBox = await heroTitle.boundingBox();
    expect(titleBox).not.toBeNull();
    if (titleBox) {
      expect(titleBox.width).toBeLessThanOrEqual(768);
    }

    // Verify features section is visible
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify footer is present
    const footer = page.locator('.footer');
    await expect(footer).toBeVisible();

    // Check that all main sections fit within viewport width
    const sections = ['.hero', '#quick-start', '#features', '#architecture', '#configuration', '#commands', '#project-status', '.footer'];
    for (const selector of sections) {
      const section = page.locator(selector);
      const isVisible = await section.isVisible().catch(() => false);
      if (isVisible) {
        const box = await section.boundingBox();
        if (box) {
          expect(box.width).toBeLessThanOrEqual(768 + 1); // +1 for rounding tolerance
        }
      }
    }
  });

  // Test Case 2: Feature cards adapt to 2-column or stacked layout
  test('TC2: should display feature cards in 2-column or stacked layout at tablet', async ({ page }) => {
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
    expect(cardCount).toBeGreaterThanOrEqual(3);

    // Check grid layout - verify cards adapt for tablet
    const gridStyles = await featuresGrid.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        display: computed.display,
        gridTemplateColumns: computed.gridTemplateColumns
      };
    });

    // Verify it's using grid display
    expect(gridStyles.display).toBe('grid');

    // Get positions of all cards to verify layout
    const cardPositions = [];
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      const boundingBox = await card.boundingBox();
      if (boundingBox) {
        cardPositions.push({
          x: boundingBox.x,
          y: boundingBox.y,
          width: boundingBox.width,
          height: boundingBox.height
        });
      }
    }

    // Verify cards have reasonable width at tablet viewport
    for (const pos of cardPositions) {
      // Cards should fit within the viewport
      expect(pos.width).toBeLessThanOrEqual(768);
      expect(pos.width).toBeGreaterThan(100);
    }

    // At 768px tablet, we should have either:
    // - 2-column layout (cards side by side) OR
    // - Stacked layout (single column)
    // Both are acceptable for tablet, but cards should not overflow

    // Count unique X positions (columns)
    const uniqueXPositions = new Set(cardPositions.map(pos => Math.round(pos.x / 10) * 10)); // Group by ~10px tolerance

    // At tablet viewport, should have 1-2 columns max
    expect(uniqueXPositions.size).toBeLessThanOrEqual(2);

    // Verify cards don't overlap horizontally
    if (uniqueXPositions.size === 2) {
      // 2-column layout - verify cards in same row don't overlap
      for (let i = 0; i < cardPositions.length - 1; i++) {
        for (let j = i + 1; j < cardPositions.length; j++) {
          const card1 = cardPositions[i];
          const card2 = cardPositions[j];

          // If cards are on the same row (similar Y position)
          if (Math.abs(card1.y - card2.y) < 20) {
            // They should not overlap horizontally
            const card1Right = card1.x + card1.width;
            const card2Right = card2.x + card2.width;
            const overlaps = !(card1Right <= card2.x || card2Right <= card1.x);
            expect(overlaps).toBe(false);
          }
        }
      }
    }
  });

  // Test Case 3: Navigation is accessible at tablet
  test('TC3: should have accessible navigation at tablet viewport', async ({ page }) => {
    // Verify hero CTA buttons are visible and accessible
    const ctaButtons = page.locator('.hero-ctas a');
    const ctaCount = await ctaButtons.count();
    expect(ctaCount).toBeGreaterThanOrEqual(2);

    const getStartedBtn = page.locator('#get-started-btn');
    const githubBtn = page.locator('#github-btn');

    await expect(getStartedBtn).toBeVisible();
    await expect(githubBtn).toBeVisible();

    // Verify buttons are accessible (have proper text content)
    await expect(getStartedBtn).toHaveText('Get Started');
    await expect(githubBtn).toHaveText('View on GitHub');

    // Verify buttons fit within viewport at tablet size
    const getStartedBox = await getStartedBtn.boundingBox();
    const githubBox = await githubBtn.boundingBox();

    if (getStartedBox) {
      expect(getStartedBox.x).toBeGreaterThanOrEqual(0);
      expect(getStartedBox.x + getStartedBox.width).toBeLessThanOrEqual(768);
    }

    if (githubBox) {
      expect(githubBox.x).toBeGreaterThanOrEqual(0);
      expect(githubBox.x + githubBox.width).toBeLessThanOrEqual(768);
    }

    // At tablet viewport (768px), buttons may be side-by-side or stacked
    // Both are acceptable as long as they're visible and accessible
    if (getStartedBox && githubBox) {
      // Verify both buttons are fully visible within viewport
      expect(getStartedBox.y).toBeGreaterThanOrEqual(0);
      expect(githubBox.y).toBeGreaterThanOrEqual(0);
    }

    // Verify section navigation works - "Get Started" should scroll to quick-start
    await getStartedBtn.click();
    await page.waitForTimeout(500); // Wait for smooth scroll

    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeInViewport();

    // Verify footer navigation link is accessible
    await page.locator('.footer').scrollIntoViewIfNeeded();
    const footerLink = page.locator('#footer-github-link');
    await expect(footerLink).toBeVisible();
    await expect(footerLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
  });

  // Additional test: Verify tables are responsive at tablet
  test('should handle command tables appropriately at tablet viewport', async ({ page }) => {
    // Scroll to commands section
    const commandsSection = page.locator('#commands');
    await commandsSection.scrollIntoViewIfNeeded();
    await expect(commandsSection).toBeVisible();

    // Check command tables
    const tables = page.locator('.commands-table');
    const tableCount = await tables.count();
    expect(tableCount).toBeGreaterThan(0);

    for (let i = 0; i < tableCount; i++) {
      const table = tables.nth(i);
      await table.scrollIntoViewIfNeeded();

      const isVisible = await table.isVisible().catch(() => false);
      if (!isVisible) continue;

      const tableBox = await table.boundingBox();
      if (tableBox) {
        // Table should fit within or have overflow handling
        const overflowInfo = await table.evaluate((el) => {
          const parent = el.parentElement;
          const parentComputed = parent ? window.getComputedStyle(parent) : null;
          const tableComputed = window.getComputedStyle(el);
          return {
            parentOverflowX: parentComputed?.overflowX,
            tableWidth: el.scrollWidth,
            parentWidth: parent?.clientWidth || 0
          };
        });

        // Either table fits or parent handles overflow
        const fitsOrHandled = tableBox.width <= 768 + 20 ||
                              overflowInfo.parentOverflowX === 'auto' ||
                              overflowInfo.parentOverflowX === 'scroll';
        expect(fitsOrHandled).toBe(true);
      }
    }
  });

  // Verify code blocks are responsive at tablet
  test('should display code blocks appropriately at tablet viewport', async ({ page }) => {
    // Scroll to quick start section which contains code blocks
    const quickStartSection = page.locator('#quick-start');
    await quickStartSection.scrollIntoViewIfNeeded();

    const codeBlocks = page.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      await codeBlock.scrollIntoViewIfNeeded();

      const isVisible = await codeBlock.isVisible().catch(() => false);
      if (!isVisible) continue;

      const boundingBox = await codeBlock.boundingBox();
      if (!boundingBox) continue;

      // Verify code block fits or has overflow handling
      const overflowInfo = await codeBlock.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          overflow: computed.overflow,
          overflowX: computed.overflowX,
          scrollWidth: el.scrollWidth,
          clientWidth: el.clientWidth
        };
      });

      // Code blocks should handle overflow gracefully
      const hasOverflowHandling =
        overflowInfo.overflowX === 'auto' ||
        overflowInfo.overflowX === 'scroll' ||
        overflowInfo.overflowX === 'hidden' ||
        overflowInfo.overflow === 'auto' ||
        overflowInfo.overflow === 'scroll' ||
        overflowInfo.overflow === 'hidden' ||
        overflowInfo.scrollWidth <= overflowInfo.clientWidth + 5;

      expect(hasOverflowHandling).toBe(true);
    }
  });

  // Verify architecture section adapts at tablet
  test('should adapt architecture section layout at tablet viewport', async ({ page }) => {
    // Scroll to architecture section
    const architectureSection = page.locator('#architecture');
    await architectureSection.scrollIntoViewIfNeeded();
    await expect(architectureSection).toBeVisible();

    // Check data flow explanation grid
    const dataFlowExplanation = page.locator('.data-flow-explanation');
    const isVisible = await dataFlowExplanation.isVisible().catch(() => false);

    if (isVisible) {
      const gridStyles = await dataFlowExplanation.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          display: computed.display,
          gridTemplateColumns: computed.gridTemplateColumns
        };
      });

      // At tablet, the grid should adapt (could be 1 or 2 columns)
      expect(gridStyles.display).toBe('grid');
    }

    // Verify path explanations are visible
    const writePath = page.locator('#write-path');
    const readPath = page.locator('#read-path');

    await expect(writePath).toBeVisible();
    await expect(readPath).toBeVisible();

    // Verify they fit within viewport
    const writePathBox = await writePath.boundingBox();
    const readPathBox = await readPath.boundingBox();

    if (writePathBox) {
      expect(writePathBox.width).toBeLessThanOrEqual(768);
    }
    if (readPathBox) {
      expect(readPathBox.width).toBeLessThanOrEqual(768);
    }
  });

  // Verify project status section adapts at tablet
  test('should adapt project status section at tablet viewport', async ({ page }) => {
    // Scroll to project status section
    const statusSection = page.locator('#project-status');
    await statusSection.scrollIntoViewIfNeeded();
    await expect(statusSection).toBeVisible();

    // Check status grid
    const statusGrid = page.locator('.status-grid');
    await expect(statusGrid).toBeVisible();

    // Get status cards
    const statusCards = page.locator('.status-card');
    const cardCount = await statusCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(1);

    // Verify cards fit within viewport
    for (let i = 0; i < cardCount; i++) {
      const card = statusCards.nth(i);
      const cardBox = await card.boundingBox();

      if (cardBox) {
        expect(cardBox.width).toBeLessThanOrEqual(768);
      }
    }
  });
});
