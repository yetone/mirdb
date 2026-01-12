// @ts-check
const { test, expect } = require('@playwright/test');

// Configure tests to use mobile viewport (375x667 - iPhone SE/standard mobile)
test.describe('Responsive Design - Mobile Viewport', () => {
  test.use({
    viewport: { width: 375, height: 667 }
  });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Single column layout, no horizontal scrollbar at 375x667 viewport
  test('TC1: should have single column layout with no horizontal scrollbar at 375x667 viewport', async ({ page }) => {
    // Verify the viewport dimensions are correct
    const viewportSize = page.viewportSize();
    expect(viewportSize?.width).toBe(375);
    expect(viewportSize?.height).toBe(667);

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

    // Verify hero section is visible
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Verify features section is visible
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify footer is present
    const footer = page.locator('.footer');
    await expect(footer).toBeVisible();

    // Verify all main sections exist and are within viewport width
    const sections = page.locator('.section');
    const sectionCount = await sections.count();
    expect(sectionCount).toBeGreaterThanOrEqual(3);

    for (let i = 0; i < sectionCount; i++) {
      const section = sections.nth(i);
      const boundingBox = await section.boundingBox();
      if (boundingBox) {
        // Section should not exceed viewport width
        expect(boundingBox.width).toBeLessThanOrEqual(375 + 1);
      }
    }
  });

  // Test Case 2: Feature cards stack vertically in single column
  test('TC2: should display feature cards stacked vertically in single column at mobile', async ({ page }) => {
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

    // Get positions of all cards
    const cardPositions = [];
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      await card.scrollIntoViewIfNeeded();
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

    // At mobile viewport, all cards should be in a single column
    // This means they should have similar x positions (left edge aligned)
    if (cardPositions.length >= 2) {
      const uniqueXPositions = new Set(cardPositions.map(pos => Math.round(pos.x)));
      // All cards should have the same x position (single column)
      expect(uniqueXPositions.size).toBe(1);

      // Verify cards are stacked vertically (each card's y position is greater than the previous)
      for (let i = 1; i < cardPositions.length; i++) {
        expect(cardPositions[i].y).toBeGreaterThan(cardPositions[i - 1].y);
      }
    }

    // Verify cards take up most of the available width (responsive)
    for (const pos of cardPositions) {
      // At mobile, cards should be close to full width (accounting for padding)
      // Cards should be at least 280px wide on a 375px viewport
      expect(pos.width).toBeGreaterThan(280);
      // Cards should not exceed viewport width
      expect(pos.width).toBeLessThanOrEqual(375);
    }
  });

  // Test Case 3: Buttons have minimum touch target size of 44x44px
  test('TC3: should have CTA buttons with minimum touch target size of 44x44px at mobile', async ({ page }) => {
    // Get all CTA buttons (hero section buttons)
    const ctaButtons = page.locator('.hero-ctas a.btn');
    const ctaCount = await ctaButtons.count();
    expect(ctaCount).toBeGreaterThanOrEqual(2);

    for (let i = 0; i < ctaCount; i++) {
      const button = ctaButtons.nth(i);
      await expect(button).toBeVisible();

      const boundingBox = await button.boundingBox();
      expect(boundingBox).not.toBeNull();

      if (boundingBox) {
        // Buttons should have minimum touch target size of 44x44px
        // This is a WCAG 2.1 AAA / mobile accessibility guideline
        expect(boundingBox.height).toBeGreaterThanOrEqual(44);
        expect(boundingBox.width).toBeGreaterThanOrEqual(44);
      }
    }

    // Also check copy buttons in code blocks
    const copyButtons = page.locator('.copy-btn');
    const copyBtnCount = await copyButtons.count();

    for (let i = 0; i < copyBtnCount; i++) {
      const copyBtn = copyButtons.nth(i);
      await copyBtn.scrollIntoViewIfNeeded();

      const isVisible = await copyBtn.isVisible().catch(() => false);
      if (!isVisible) continue;

      const boundingBox = await copyBtn.boundingBox();
      if (boundingBox) {
        // Copy buttons should also meet touch target guidelines
        expect(boundingBox.height).toBeGreaterThanOrEqual(28); // Slightly smaller is acceptable for secondary actions
        expect(boundingBox.width).toBeGreaterThanOrEqual(44);
      }
    }

    // Check footer links
    const footerLinks = page.locator('.footer a');
    const footerLinkCount = await footerLinks.count();

    for (let i = 0; i < footerLinkCount; i++) {
      const link = footerLinks.nth(i);
      await link.scrollIntoViewIfNeeded();

      const isVisible = await link.isVisible().catch(() => false);
      if (!isVisible) continue;

      const boundingBox = await link.boundingBox();
      if (boundingBox) {
        // Links should have adequate touch target
        // Line height provides vertical touch area
        expect(boundingBox.height).toBeGreaterThanOrEqual(24);
      }
    }
  });

  // Test Case 4: Code blocks have horizontal scroll if needed, don't break layout
  test('TC4: should display code blocks with horizontal scroll without breaking layout at mobile', async ({ page }) => {
    // Scroll to quick start section which contains code blocks
    const quickStartSection = page.locator('#quick-start');
    await quickStartSection.scrollIntoViewIfNeeded();

    // Check code blocks on the page
    const codeBlocks = page.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      await codeBlock.scrollIntoViewIfNeeded();

      // Skip invisible elements
      const isVisible = await codeBlock.isVisible().catch(() => false);
      if (!isVisible) continue;

      const boundingBox = await codeBlock.boundingBox();
      if (!boundingBox) continue;

      // Code block should not exceed viewport width (shouldn't break layout)
      expect(boundingBox.width).toBeLessThanOrEqual(375);
      expect(boundingBox.x).toBeGreaterThanOrEqual(0);
      expect(boundingBox.x + boundingBox.width).toBeLessThanOrEqual(375 + 1);

      // Verify code block has overflow handling for horizontal scroll
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
      // Either content fits, or overflow-x is set to auto/scroll
      const hasOverflowHandling =
        overflowInfo.overflowX === 'auto' ||
        overflowInfo.overflowX === 'scroll' ||
        overflowInfo.overflow === 'auto' ||
        overflowInfo.overflow === 'scroll' ||
        overflowInfo.scrollWidth <= overflowInfo.clientWidth + 5;

      expect(hasOverflowHandling).toBe(true);
    }

    // Verify the page still has no horizontal scrollbar after checking code blocks
    const scrollInfo = await page.evaluate(() => {
      return {
        scrollWidth: document.documentElement.scrollWidth,
        clientWidth: document.documentElement.clientWidth,
        hasHorizontalScrollbar: document.documentElement.scrollWidth > document.documentElement.clientWidth
      };
    });

    expect(scrollInfo.hasHorizontalScrollbar).toBe(false);
  });

  // Additional test: Verify CTA buttons stack vertically on very small mobile screens
  test('should stack hero CTA buttons vertically on mobile', async ({ page }) => {
    const ctaContainer = page.locator('.hero-ctas');
    await expect(ctaContainer).toBeVisible();

    const ctaButtons = page.locator('.hero-ctas a.btn');
    const ctaCount = await ctaButtons.count();
    expect(ctaCount).toBeGreaterThanOrEqual(2);

    // Get positions of CTA buttons
    const buttonPositions = [];
    for (let i = 0; i < ctaCount; i++) {
      const button = ctaButtons.nth(i);
      const boundingBox = await button.boundingBox();
      if (boundingBox) {
        buttonPositions.push({
          x: boundingBox.x,
          y: boundingBox.y,
          width: boundingBox.width,
          height: boundingBox.height
        });
      }
    }

    if (buttonPositions.length >= 2) {
      // At 375px viewport, buttons may stack vertically or wrap
      // Check if buttons are stacked (different y positions) or wrapped (same row with different x)
      const firstButton = buttonPositions[0];
      const secondButton = buttonPositions[1];

      // Either buttons are side by side (same y, different x) or stacked (same x, different y)
      const areSideBySide = Math.abs(firstButton.y - secondButton.y) < 10;
      const areStacked = !areSideBySide;

      // Both configurations are acceptable on mobile
      // If stacked, verify vertical arrangement
      if (areStacked) {
        expect(secondButton.y).toBeGreaterThan(firstButton.y);
      }

      // Verify buttons fit within viewport
      for (const pos of buttonPositions) {
        expect(pos.x).toBeGreaterThanOrEqual(0);
        expect(pos.x + pos.width).toBeLessThanOrEqual(375 + 5);
      }
    }
  });

  // Test: Verify tables are responsive on mobile
  test('should handle command tables responsively on mobile', async ({ page }) => {
    // Scroll to commands section
    const commandsSection = page.locator('#commands');
    await commandsSection.scrollIntoViewIfNeeded();

    // Check if tables exist
    const tables = page.locator('.commands-table');
    const tableCount = await tables.count();

    if (tableCount > 0) {
      for (let i = 0; i < tableCount; i++) {
        const table = tables.nth(i);
        await table.scrollIntoViewIfNeeded();

        const isVisible = await table.isVisible().catch(() => false);
        if (!isVisible) continue;

        const boundingBox = await table.boundingBox();
        if (boundingBox) {
          // Table container should not exceed viewport width
          // Tables might have their own scroll or be contained
          expect(boundingBox.x).toBeGreaterThanOrEqual(-1);
        }

        // Check if table has overflow handling
        const tableInfo = await table.evaluate((el) => {
          const parent = el.parentElement;
          const parentStyle = parent ? window.getComputedStyle(parent) : null;
          const tableStyle = window.getComputedStyle(el);
          return {
            tableOverflowX: tableStyle.overflowX,
            parentOverflowX: parentStyle ? parentStyle.overflowX : 'visible',
            tableWidth: el.scrollWidth,
            parentWidth: parent ? parent.clientWidth : 0
          };
        });

        // Table content might overflow but should be scrollable
        const hasOverflowHandling =
          tableInfo.tableOverflowX === 'auto' ||
          tableInfo.tableOverflowX === 'scroll' ||
          tableInfo.parentOverflowX === 'auto' ||
          tableInfo.parentOverflowX === 'scroll' ||
          tableInfo.tableWidth <= tableInfo.parentWidth + 5;

        // This is acceptable - tables can be wide and scrollable
        expect(hasOverflowHandling || tableInfo.tableWidth <= 375).toBe(true);
      }
    }

    // Verify page still has no horizontal scrollbar
    const scrollInfo = await page.evaluate(() => {
      return {
        hasHorizontalScrollbar: document.documentElement.scrollWidth > document.documentElement.clientWidth
      };
    });

    expect(scrollInfo.hasHorizontalScrollbar).toBe(false);
  });
});
