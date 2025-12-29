// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Responsive Design E2E Tests
 * Tests NFR-2 from PRD: Design must be responsive and functional on mobile devices (320px+)
 */

test.describe('Responsive Design - Mobile Devices', () => {

  // Test Case 1: Test layout at 320px width
  test('TC1: All content is visible without horizontal scroll at 320px viewport', async ({ page }) => {
    // Set viewport to 320px width (minimum supported mobile width)
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto('/');

    // Wait for page to fully load
    await page.waitForLoadState('networkidle');

    // Check that the document width does not exceed viewport width
    const documentWidth = await page.evaluate(() => {
      return Math.max(
        document.body.scrollWidth,
        document.documentElement.scrollWidth
      );
    });

    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // Document width should not significantly exceed viewport width
    // Allow small tolerance for browser rendering differences
    expect(documentWidth).toBeLessThanOrEqual(viewportWidth + 5);

    // Verify no horizontal scrollbar is present
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Verify all major sections are visible
    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('.features')).toBeVisible();
    await expect(page.locator('.code-examples')).toBeVisible();
    await expect(page.locator('.getting-started')).toBeVisible();
    await expect(page.locator('.footer')).toBeVisible();
  });

  // Test Case 2: Test layout at 768px width (tablet)
  test('TC2: Layout adapts appropriately for tablet viewport (768px)', async ({ page }) => {
    // Set viewport to 768px width (tablet breakpoint)
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/');

    await page.waitForLoadState('networkidle');

    // Check that the document width does not exceed viewport width
    const documentWidth = await page.evaluate(() => {
      return Math.max(
        document.body.scrollWidth,
        document.documentElement.scrollWidth
      );
    });

    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(documentWidth).toBeLessThanOrEqual(viewportWidth + 5);

    // Verify all major sections are visible
    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('.features')).toBeVisible();
    await expect(page.locator('.code-examples')).toBeVisible();

    // Verify feature cards are visible
    const featureCards = page.locator('.feature-card');
    const featureCardCount = await featureCards.count();
    expect(featureCardCount).toBeGreaterThanOrEqual(3);

    // Verify code blocks are visible
    const codeBlocks = page.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThanOrEqual(2);
  });

  // Test Case 3: Test layout at 1024px width (desktop)
  test('TC3: Layout displays correctly at desktop viewport (1024px)', async ({ page }) => {
    // Set viewport to 1024px width (desktop viewport)
    await page.setViewportSize({ width: 1024, height: 768 });
    await page.goto('/');

    await page.waitForLoadState('networkidle');

    // Check that the document width does not exceed viewport width
    const documentWidth = await page.evaluate(() => {
      return Math.max(
        document.body.scrollWidth,
        document.documentElement.scrollWidth
      );
    });

    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(documentWidth).toBeLessThanOrEqual(viewportWidth + 5);

    // Verify all sections are visible
    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('.features')).toBeVisible();
    await expect(page.locator('.code-examples')).toBeVisible();
    await expect(page.locator('.getting-started')).toBeVisible();
    await expect(page.locator('.configuration')).toBeVisible();
    await expect(page.locator('.commands')).toBeVisible();
    await expect(page.locator('.footer')).toBeVisible();

    // Verify hero title is visible with appropriate font size
    const heroTitle = page.locator('.hero-title');
    await expect(heroTitle).toBeVisible();

    const fontSize = await heroTitle.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    expect(fontSize).toBeGreaterThanOrEqual(32);
  });

  // Test Case 4: Verify navigation is accessible on mobile
  test('TC4: Navigation menu is accessible on mobile (hamburger menu or visible links)', async ({ page }) => {
    // Set viewport to mobile width
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto('/');

    await page.waitForLoadState('networkidle');

    // Check for navigation elements
    // The page may have either:
    // 1. A hamburger menu (nav button/icon)
    // 2. Visible navigation links
    // 3. CTA buttons that serve as primary navigation

    // Check for hero CTA buttons which are the main navigation elements
    const getStartedBtn = page.locator('.hero-cta a, .hero .btn').first();
    await expect(getStartedBtn).toBeVisible();

    // Verify the CTA buttons are tappable (within viewport)
    const ctaButtons = page.locator('.hero-cta a, .hero .btn');
    const ctaCount = await ctaButtons.count();
    expect(ctaCount).toBeGreaterThanOrEqual(1);

    for (let i = 0; i < ctaCount; i++) {
      const btn = ctaButtons.nth(i);
      await expect(btn).toBeVisible();

      // Verify button is within viewport
      const box = await btn.boundingBox();
      expect(box).not.toBeNull();
      if (box) {
        expect(box.x).toBeGreaterThanOrEqual(0);
        expect(box.x + box.width).toBeLessThanOrEqual(320);
      }
    }

    // Verify footer navigation is accessible
    const footerLinks = page.locator('.footer a');
    const footerLinkCount = await footerLinks.count();
    expect(footerLinkCount).toBeGreaterThanOrEqual(1);
    await expect(footerLinks.first()).toBeVisible();
  });

  // Test Case 5: Verify code blocks on mobile are horizontally scrollable
  test('TC5: Code blocks are scrollable horizontally within container on mobile', async ({ page }) => {
    // Set viewport to mobile width
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto('/');

    await page.waitForLoadState('networkidle');

    // Navigate to code examples section
    await page.locator('#code-examples').scrollIntoViewIfNeeded();

    // Check that code blocks have overflow-x: auto or scroll
    const codeBlockPres = page.locator('.code-block pre');
    const preCount = await codeBlockPres.count();
    expect(preCount).toBeGreaterThanOrEqual(1);

    for (let i = 0; i < preCount; i++) {
      const pre = codeBlockPres.nth(i);

      const overflowX = await pre.evaluate((el) => {
        return window.getComputedStyle(el).overflowX;
      });

      // Overflow should be 'auto' or 'scroll' for horizontal scrolling
      expect(['auto', 'scroll']).toContain(overflowX);
    }

    // Verify code blocks are contained within the viewport width
    const codeBlocks = page.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();

    for (let i = 0; i < codeBlockCount; i++) {
      const block = codeBlocks.nth(i);
      const box = await block.boundingBox();

      if (box) {
        // Code block should not extend beyond viewport
        expect(box.x).toBeGreaterThanOrEqual(0);
        expect(box.x + box.width).toBeLessThanOrEqual(320 + 5); // small tolerance
      }
    }
  });

});
