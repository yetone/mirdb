// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Mobile Responsiveness (NFR-1)', () => {
  test.describe('320px viewport (smallest mobile)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');
    });

    test('Test Case 1: No horizontal scrollbar appears at 320px width', async ({ page }) => {
      // Wait for the page to fully load
      await page.waitForLoadState('domcontentloaded');

      // Check that body doesn't overflow horizontally
      const scrollWidth = await page.evaluate(() => document.body.scrollWidth);
      const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);

      // scrollWidth should be <= clientWidth (no horizontal overflow)
      expect(scrollWidth).toBeLessThanOrEqual(clientWidth);

      // Also verify no horizontal scrollbar is visible
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasHorizontalScroll).toBe(false);
    });

    test('Test Case 2: All sections are vertically scrollable and readable at 320px', async ({ page }) => {
      // Test hero section visibility
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      // Test features section visibility
      const features = page.locator('#features');
      await features.scrollIntoViewIfNeeded();
      await expect(features).toBeVisible();

      // Test quick-start section visibility
      const quickStart = page.locator('#quick-start');
      await quickStart.scrollIntoViewIfNeeded();
      await expect(quickStart).toBeVisible();

      // Test architecture section visibility
      const architecture = page.locator('#architecture');
      await architecture.scrollIntoViewIfNeeded();
      await expect(architecture).toBeVisible();

      // Test configuration section visibility
      const config = page.locator('#config');
      await config.scrollIntoViewIfNeeded();
      await expect(config).toBeVisible();

      // Test project status section visibility
      const projectStatus = page.locator('#project-status');
      await projectStatus.scrollIntoViewIfNeeded();
      await expect(projectStatus).toBeVisible();

      // Test footer visibility
      const footer = page.locator('.footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();

      // Verify all content fits within viewport width
      const sections = ['hero', '#features', '#quick-start', '#architecture', '#config', '#project-status', '.footer'];
      for (const selector of sections) {
        const sectionEl = page.locator(selector.startsWith('.') || selector.startsWith('#') ? selector : `.${selector}`);
        const box = await sectionEl.boundingBox();
        if (box) {
          expect(box.width).toBeLessThanOrEqual(320);
        }
      }
    });
  });

  test.describe('768px viewport (tablet)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');
    });

    test('Test Case 3: Layout adapts appropriately for tablet size (768px)', async ({ page }) => {
      await page.waitForLoadState('domcontentloaded');

      // Verify hero section displays properly
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      // Verify feature grid layout adapts (should have multi-column layout possible at 768px)
      const featureGrid = page.locator('.feature-grid');
      await featureGrid.scrollIntoViewIfNeeded();
      await expect(featureGrid).toBeVisible();

      // Check that feature cards are present and visible
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();
      expect(cardCount).toBe(4);

      // Verify all cards are visible
      for (let i = 0; i < cardCount; i++) {
        await expect(featureCards.nth(i)).toBeVisible();
      }

      // Verify no horizontal overflow at tablet size
      const scrollWidth = await page.evaluate(() => document.body.scrollWidth);
      const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);
      expect(scrollWidth).toBeLessThanOrEqual(clientWidth);

      // Verify status grid layout
      const statusGrid = page.locator('.status-grid');
      await statusGrid.scrollIntoViewIfNeeded();
      await expect(statusGrid).toBeVisible();
    });
  });

  test.describe('Touch targets on mobile', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');
    });

    test('Test Case 4: Interactive elements meet minimum touch target size (44x44 pixels)', async ({ page }) => {
      await page.waitForLoadState('domcontentloaded');

      // Get all buttons
      const buttons = page.locator('.btn');
      const buttonCount = await buttons.count();

      for (let i = 0; i < buttonCount; i++) {
        const button = buttons.nth(i);
        const box = await button.boundingBox();

        if (box) {
          // Apple's Human Interface Guidelines recommend 44x44 minimum touch target
          expect(box.height).toBeGreaterThanOrEqual(44);
          expect(box.width).toBeGreaterThanOrEqual(44);
        }
      }

      // Get all footer links and verify they have adequate clickable area
      const footerLinks = page.locator('.footer-links a');
      const footerLinkCount = await footerLinks.count();

      for (let i = 0; i < footerLinkCount; i++) {
        const link = footerLinks.nth(i);
        const box = await link.boundingBox();

        if (box) {
          // Links should have at least 44px height (via padding)
          expect(box.height).toBeGreaterThanOrEqual(44);
        }
      }
    });
  });

  test.describe('Code blocks on mobile', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');
    });

    test('Test Case 5: Code blocks are scrollable horizontally without breaking layout', async ({ page }) => {
      await page.waitForLoadState('domcontentloaded');

      // Navigate to quick start section with code block
      const quickStart = page.locator('#quick-start');
      await quickStart.scrollIntoViewIfNeeded();

      // Check code block container
      const codeBlock = page.locator('.code-block');
      await expect(codeBlock).toBeVisible();

      // Get the code block's bounding box - it should not overflow the viewport
      const codeBlockBox = await codeBlock.boundingBox();
      if (codeBlockBox) {
        expect(codeBlockBox.width).toBeLessThanOrEqual(320);
      }

      // Check that pre element has overflow-x: auto for horizontal scrolling
      const preOverflow = await page.locator('.code-block pre').evaluate((el) => {
        return window.getComputedStyle(el).overflowX;
      });
      expect(preOverflow).toBe('auto');

      // Verify the overall page doesn't have horizontal scroll
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);
    });
  });

  test.describe('Desktop viewport (1024px+)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 800 });
      await page.goto('/');
    });

    test('Full desktop experience at 1024px+ width', async ({ page }) => {
      await page.waitForLoadState('domcontentloaded');

      // Verify hero section displays properly
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      // Verify buttons are side by side (not stacked) on desktop
      const heroCtas = page.locator('.hero-ctas');
      const ctasStyle = await heroCtas.evaluate((el) => {
        return window.getComputedStyle(el).flexDirection;
      });
      expect(ctasStyle).toBe('row');

      // Verify footer is horizontal layout
      const footerContent = page.locator('.footer-content');
      const footerStyle = await footerContent.evaluate((el) => {
        return window.getComputedStyle(el).flexDirection;
      });
      expect(footerStyle).toBe('row');

      // Verify no horizontal overflow
      const scrollWidth = await page.evaluate(() => document.body.scrollWidth);
      const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);
      expect(scrollWidth).toBeLessThanOrEqual(clientWidth);
    });
  });
});
