// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Responsive Design - Mobile Tests
 *
 * Verifies the homepage displays correctly on mobile viewport sizes:
 * - 320px width (smallest supported mobile screen)
 * - 375px width (iPhone-sized screens)
 */

test.describe('Responsive Design - Mobile', () => {
  test.describe('320px Viewport (Smallest Mobile)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');
    });

    // Test Case 1: Page renders without horizontal scroll at 320px
    test('TC1: page renders without horizontal scroll at 320px viewport width', async ({ page }) => {
      await page.waitForLoadState('domcontentloaded');

      // Check document width doesn't exceed viewport
      const documentWidth = await page.evaluate(() => document.documentElement.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);

      expect(documentWidth).toBeLessThanOrEqual(viewportWidth);

      // Verify no horizontal scrollbar
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasHorizontalScroll).toBe(false);

      // Check body doesn't overflow
      const bodyOverflow = await page.evaluate(() => {
        const body = document.body;
        return body.scrollWidth > body.clientWidth;
      });

      expect(bodyOverflow).toBe(false);
    });

    // Test Case 2: Feature cards display in single column at 320px
    test('TC2: feature cards display in single column at 320px', async ({ page }) => {
      const featuresSection = page.locator('#features, .features');
      await expect(featuresSection).toBeVisible();

      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();
      expect(cardCount).toBe(4);

      // Get positions of all cards
      const cardPositions = [];
      for (let i = 0; i < cardCount; i++) {
        const box = await featureCards.nth(i).boundingBox();
        cardPositions.push(box);
      }

      // In single column layout, all cards should have roughly the same x position
      // (only one card per row)
      const firstCardX = Math.round(cardPositions[0].x);

      // All cards should have the same x position (single column)
      for (let i = 1; i < cardCount; i++) {
        expect(Math.round(cardPositions[i].x)).toBe(firstCardX);
      }

      // Cards should be stacked vertically (each subsequent card has greater y value)
      for (let i = 1; i < cardCount; i++) {
        expect(cardPositions[i].y).toBeGreaterThan(cardPositions[i - 1].y);
      }

      // Each card should fit within viewport width
      for (let i = 0; i < cardCount; i++) {
        expect(cardPositions[i].x).toBeGreaterThanOrEqual(0);
        expect(cardPositions[i].x + cardPositions[i].width).toBeLessThanOrEqual(320);
      }
    });

    // Test Case 3: Touch targets have minimum 44px tap targets at 320px
    test('TC3: all buttons and links have minimum 44px tap targets at 320px', async ({ page }) => {
      // Check CTA buttons in hero section
      const ctaButtons = page.locator('.cta-buttons .btn');
      const ctaButtonCount = await ctaButtons.count();

      for (let i = 0; i < ctaButtonCount; i++) {
        const button = ctaButtons.nth(i);
        await expect(button).toBeVisible();

        const box = await button.boundingBox();
        expect(box).not.toBeNull();

        // WCAG 2.5.5 recommends minimum 44px for touch targets
        expect(box.height).toBeGreaterThanOrEqual(44);
      }

      // Check footer links
      const footerLinks = page.locator('.footer-links a');
      const footerLinkCount = await footerLinks.count();

      for (let i = 0; i < footerLinkCount; i++) {
        const link = footerLinks.nth(i);
        const box = await link.boundingBox();

        if (box) {
          // Links should have reasonable tap target size
          // Checking combined height including line-height/padding
          expect(box.height).toBeGreaterThanOrEqual(44);
        }
      }

      // Check docs links
      const docsLinks = page.locator('.docs-link');
      const docsLinkCount = await docsLinks.count();

      for (let i = 0; i < docsLinkCount; i++) {
        const link = docsLinks.nth(i);
        await link.scrollIntoViewIfNeeded();

        const box = await link.boundingBox();
        if (box) {
          expect(box.height).toBeGreaterThanOrEqual(44);
        }
      }
    });

    // Test Case 4: Text is readable without horizontal scroll at 320px
    test('TC4: text is readable without horizontal scroll at 320px', async ({ page }) => {
      // Check hero text
      const heroDescription = page.locator('.hero .description');
      await expect(heroDescription).toBeVisible();

      const heroDescBox = await heroDescription.boundingBox();
      expect(heroDescBox).not.toBeNull();
      expect(heroDescBox.x).toBeGreaterThanOrEqual(0);
      expect(heroDescBox.x + heroDescBox.width).toBeLessThanOrEqual(320);

      // Check tagline
      const tagline = page.locator('.hero .tagline');
      await expect(tagline).toBeVisible();

      const taglineBox = await tagline.boundingBox();
      expect(taglineBox).not.toBeNull();
      expect(taglineBox.x + taglineBox.width).toBeLessThanOrEqual(320);

      // Check feature card text
      const featureCardText = page.locator('.feature-card p').first();
      await expect(featureCardText).toBeVisible();

      const featureTextBox = await featureCardText.boundingBox();
      expect(featureTextBox).not.toBeNull();
      expect(featureTextBox.x + featureTextBox.width).toBeLessThanOrEqual(320);

      // Check section headings
      const headings = page.locator('h2');
      const headingCount = await headings.count();

      for (let i = 0; i < headingCount; i++) {
        const heading = headings.nth(i);
        await heading.scrollIntoViewIfNeeded();
        await expect(heading).toBeVisible();

        const headingBox = await heading.boundingBox();
        expect(headingBox).not.toBeNull();
        expect(headingBox.x).toBeGreaterThanOrEqual(0);
        expect(headingBox.x + headingBox.width).toBeLessThanOrEqual(320);
      }

      // Verify no text overflows container
      const textOverflow = await page.evaluate(() => {
        const paragraphs = document.querySelectorAll('p');
        for (const p of paragraphs) {
          if (p.scrollWidth > p.clientWidth) {
            return true;
          }
        }
        return false;
      });

      expect(textOverflow).toBe(false);
    });

    // Additional tests for 320px viewport
    test('code blocks are horizontally scrollable at 320px', async ({ page }) => {
      const quickStartSection = page.locator('#quickstart');
      await quickStartSection.scrollIntoViewIfNeeded();

      const codeBlocks = page.locator('.code-block');
      const codeBlockCount = await codeBlocks.count();
      expect(codeBlockCount).toBeGreaterThan(0);

      for (let i = 0; i < codeBlockCount; i++) {
        const codeBlock = codeBlocks.nth(i);
        await codeBlock.scrollIntoViewIfNeeded();

        // Code block should have overflow-x: auto for scrollable content
        const overflowStyle = await codeBlock.evaluate((el) => {
          return window.getComputedStyle(el).overflowX;
        });

        expect(['auto', 'scroll']).toContain(overflowStyle);

        // Code block container should fit within viewport
        const box = await codeBlock.boundingBox();
        expect(box).not.toBeNull();
        expect(box.x).toBeGreaterThanOrEqual(0);
        expect(box.x + box.width).toBeLessThanOrEqual(320);
      }
    });

    test('config table is readable at 320px', async ({ page }) => {
      const configSection = page.locator('#config');
      await configSection.scrollIntoViewIfNeeded();

      const configTable = page.locator('.config-table');
      await expect(configTable).toBeVisible();

      const tableBox = await configTable.boundingBox();
      expect(tableBox).not.toBeNull();

      // Table should fit or be scrollable within viewport
      expect(tableBox.x).toBeGreaterThanOrEqual(0);
    });

    test('CTA buttons stack vertically at 320px', async ({ page }) => {
      const ctaButtons = page.locator('.cta-buttons .btn');
      const buttonCount = await ctaButtons.count();
      expect(buttonCount).toBeGreaterThanOrEqual(2);

      const buttonPositions = [];
      for (let i = 0; i < buttonCount; i++) {
        const box = await ctaButtons.nth(i).boundingBox();
        buttonPositions.push(box);
      }

      // At 320px, buttons should be stacked vertically (different y positions)
      expect(buttonPositions[1].y).toBeGreaterThan(buttonPositions[0].y);

      // Buttons should be full width or centered
      for (const box of buttonPositions) {
        expect(box.x).toBeGreaterThanOrEqual(0);
        expect(box.x + box.width).toBeLessThanOrEqual(320);
      }
    });
  });

  test.describe('375px Viewport (iPhone-sized)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');
    });

    // Test Case 5: Page displays correctly on iPhone-sized screens
    test('TC5: page displays correctly on iPhone-sized screens at 375px', async ({ page }) => {
      await page.waitForLoadState('domcontentloaded');

      // Verify no horizontal scroll
      const documentWidth = await page.evaluate(() => document.documentElement.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);
      expect(documentWidth).toBeLessThanOrEqual(viewportWidth);

      // Verify all major sections are visible and accessible
      await expect(page.locator('.hero')).toBeVisible();
      await expect(page.locator('#features')).toBeVisible();
      await expect(page.locator('#quickstart')).toBeVisible();
      await expect(page.locator('#commands')).toBeVisible();
      await expect(page.locator('#roadmap')).toBeVisible();
      await expect(page.locator('#config')).toBeVisible();
      await expect(page.locator('.footer')).toBeVisible();

      // Verify hero section renders correctly
      const heroH1 = page.locator('.hero h1');
      await expect(heroH1).toBeVisible();
      const h1Box = await heroH1.boundingBox();
      expect(h1Box).not.toBeNull();
      expect(h1Box.x + h1Box.width).toBeLessThanOrEqual(375);

      // Verify feature cards are properly displayed
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();
      expect(cardCount).toBe(4);

      // Verify each card fits within viewport
      for (let i = 0; i < cardCount; i++) {
        const card = featureCards.nth(i);
        await card.scrollIntoViewIfNeeded();

        const cardBox = await card.boundingBox();
        expect(cardBox).not.toBeNull();
        expect(cardBox.x).toBeGreaterThanOrEqual(0);
        expect(cardBox.x + cardBox.width).toBeLessThanOrEqual(375);
      }

      // Verify CTA buttons are accessible
      const ctaButtons = page.locator('.cta-buttons .btn');
      for (let i = 0; i < await ctaButtons.count(); i++) {
        const btn = ctaButtons.nth(i);
        await expect(btn).toBeVisible();

        const btnBox = await btn.boundingBox();
        expect(btnBox.height).toBeGreaterThanOrEqual(44);
      }

      // Verify footer is properly displayed
      const footerLinks = page.locator('.footer-links a');
      const footerLinkCount = await footerLinks.count();
      expect(footerLinkCount).toBeGreaterThanOrEqual(1);

      // Test navigation works
      const getStartedBtn = page.locator('.cta-buttons .btn-primary');
      await getStartedBtn.click();
      await page.waitForTimeout(500);

      const quickStartSection = page.locator('#quickstart');
      await expect(quickStartSection).toBeInViewport();
    });

    test('roadmap section displays correctly at 375px', async ({ page }) => {
      const roadmapSection = page.locator('#roadmap');
      await roadmapSection.scrollIntoViewIfNeeded();

      const roadmapGrid = page.locator('.roadmap-grid');
      await expect(roadmapGrid).toBeVisible();

      // Roadmap columns should be stacked (single column)
      const roadmapColumns = page.locator('.roadmap-column');
      const columnCount = await roadmapColumns.count();

      const columnPositions = [];
      for (let i = 0; i < columnCount; i++) {
        const box = await roadmapColumns.nth(i).boundingBox();
        columnPositions.push(box);
      }

      // At 375px, columns should be stacked vertically
      if (columnCount > 1) {
        expect(columnPositions[1].y).toBeGreaterThan(columnPositions[0].y);
      }

      // Each column should fit within viewport
      for (const box of columnPositions) {
        expect(box.x).toBeGreaterThanOrEqual(0);
        expect(box.x + box.width).toBeLessThanOrEqual(375);
      }
    });

    test('commands section displays correctly at 375px', async ({ page }) => {
      const commandsSection = page.locator('#commands');
      await commandsSection.scrollIntoViewIfNeeded();

      const commandsGrid = page.locator('.commands-grid');
      await expect(commandsGrid).toBeVisible();

      const commandCategories = page.locator('.command-category');
      const categoryCount = await commandCategories.count();

      // Each command category should fit within viewport
      for (let i = 0; i < categoryCount; i++) {
        const category = commandCategories.nth(i);
        await category.scrollIntoViewIfNeeded();

        const categoryBox = await category.boundingBox();
        expect(categoryBox).not.toBeNull();
        expect(categoryBox.x).toBeGreaterThanOrEqual(0);
        expect(categoryBox.x + categoryBox.width).toBeLessThanOrEqual(375);
      }
    });

    test('all content sections fit within 375px viewport', async ({ page }) => {
      const sections = [
        '.hero',
        '#features',
        '#quickstart',
        '#commands',
        '#roadmap',
        '#config',
        '.footer'
      ];

      for (const selector of sections) {
        const section = page.locator(selector);
        await section.scrollIntoViewIfNeeded();
        await expect(section).toBeVisible();

        const box = await section.boundingBox();
        expect(box).not.toBeNull();
        expect(box.x).toBeGreaterThanOrEqual(0);
        expect(box.width).toBeLessThanOrEqual(375);
      }
    });
  });
});
