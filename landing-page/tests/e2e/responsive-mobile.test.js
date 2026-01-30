/**
 * MirDB Landing Page - Mobile Responsive Design Tests
 * Owner: Scenario 9 - Responsive Design - Mobile
 *
 * Tests verify:
 * 1. Page renders without horizontal scrollbar at 375px viewport
 * 2. Feature cards stack vertically in single column
 * 3. Navigation is accessible (scrollable)
 * 4. Font size is at least 16px
 * 5. Buttons have minimum 44px height for touch targets
 * 6. Code blocks are horizontally scrollable without breaking layout
 */

const { test, expect } = require('@playwright/test');
const { setupPage, setViewport, VIEWPORTS } = require('../helpers/test-utils');

test.describe('Responsive Design - Mobile (< 768px)', () => {
  test.beforeEach(async ({ page }) => {
    // Set mobile viewport before navigation
    await setViewport(page, 'mobile');
    await setupPage(page);
  });

  test('Test Case 1: Page renders without horizontal scrollbar at 375px viewport', async ({ page }) => {
    // Verify viewport is set to mobile width
    const viewportSize = page.viewportSize();
    expect(viewportSize.width).toBe(VIEWPORTS.mobile.width);

    // Check that body does not have horizontal overflow
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const bodyClientWidth = await page.evaluate(() => document.body.clientWidth);

    // scrollWidth should not be greater than clientWidth (no horizontal scroll)
    expect(bodyScrollWidth).toBeLessThanOrEqual(bodyClientWidth + 1); // +1 for potential rounding

    // Check document-level overflow
    const htmlScrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    const htmlClientWidth = await page.evaluate(() => document.documentElement.clientWidth);

    expect(htmlScrollWidth).toBeLessThanOrEqual(htmlClientWidth + 1);
  });

  test('Test Case 2: Feature cards stack vertically in single column', async ({ page }) => {
    // Navigate to features section
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Get computed grid styles
    const gridStyles = await featuresGrid.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        gridTemplateColumns: styles.gridTemplateColumns,
        display: styles.display
      };
    });

    // Verify it's a grid display
    expect(gridStyles.display).toBe('grid');

    // On mobile, grid-template-columns should be 1fr (single column)
    // The computed value might be a pixel value for single column
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThan(0);

    // Get positions of first two cards
    const firstCard = featureCards.first();
    const secondCard = featureCards.nth(1);

    const firstBox = await firstCard.boundingBox();
    const secondBox = await secondCard.boundingBox();

    // Cards should be stacked vertically (second card below first)
    // So second card's top should be greater than first card's bottom
    expect(secondBox.y).toBeGreaterThan(firstBox.y + firstBox.height - 1);

    // Cards should have same x position (left-aligned in single column)
    expect(Math.abs(firstBox.x - secondBox.x)).toBeLessThan(5);
  });

  test('Test Case 3: Navigation is accessible (scrollable)', async ({ page }) => {
    const nav = page.locator('.nav');
    await expect(nav).toBeVisible();

    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Check that navigation links are accessible
    const links = page.locator('.nav-links a');
    const linkCount = await links.count();
    expect(linkCount).toBeGreaterThan(0);

    // Verify each link is visible and accessible
    for (let i = 0; i < linkCount; i++) {
      const link = links.nth(i);
      // Link should either be visible or become visible after scrolling
      const linkBox = await link.boundingBox();
      expect(linkBox).not.toBeNull();
      expect(linkBox.width).toBeGreaterThan(0);
      expect(linkBox.height).toBeGreaterThan(0);
    }

    // Check that nav-links has overflow-x: auto for scrollability
    const overflowStyle = await navLinks.evaluate((el) => {
      return window.getComputedStyle(el).overflowX;
    });
    expect(overflowStyle).toBe('auto');
  });

  test('Test Case 4: Body text font size is at least 16px', async ({ page }) => {
    // Check body font size
    const bodyFontSize = await page.evaluate(() => {
      const styles = window.getComputedStyle(document.body);
      return parseFloat(styles.fontSize);
    });
    expect(bodyFontSize).toBeGreaterThanOrEqual(16);

    // Check paragraph font sizes
    const paragraphs = page.locator('p');
    const pCount = await paragraphs.count();

    // Check a sample of paragraphs
    const sampleSize = Math.min(pCount, 5);
    for (let i = 0; i < sampleSize; i++) {
      const p = paragraphs.nth(i);
      const isVisible = await p.isVisible();
      if (isVisible) {
        const fontSize = await p.evaluate((el) => {
          return parseFloat(window.getComputedStyle(el).fontSize);
        });
        expect(fontSize).toBeGreaterThanOrEqual(16);
      }
    }
  });

  test('Test Case 5: CTA buttons have minimum 44px height for touch targets', async ({ page }) => {
    // Check hero CTA buttons
    const heroButtons = page.locator('.hero-cta .btn');
    const buttonCount = await heroButtons.count();
    expect(buttonCount).toBeGreaterThan(0);

    for (let i = 0; i < buttonCount; i++) {
      const button = heroButtons.nth(i);
      const boundingBox = await button.boundingBox();

      // Verify minimum height of 44px for touch accessibility
      expect(boundingBox.height).toBeGreaterThanOrEqual(44);

      // Also check CSS min-height
      const minHeight = await button.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        const minH = parseFloat(styles.minHeight);
        return isNaN(minH) ? 0 : minH;
      });
      expect(minHeight).toBeGreaterThanOrEqual(44);
    }
  });

  test('Test Case 6: Code blocks are horizontally scrollable without breaking layout', async ({ page }) => {
    // Find code blocks
    const codeBlocks = page.locator('pre, .code-block');
    const codeBlockCount = await codeBlocks.count();

    // Should have at least one code block
    expect(codeBlockCount).toBeGreaterThan(0);

    // Check first few code blocks
    const sampleSize = Math.min(codeBlockCount, 3);
    for (let i = 0; i < sampleSize; i++) {
      const codeBlock = codeBlocks.nth(i);
      const isVisible = await codeBlock.isVisible();

      if (isVisible) {
        // Check overflow-x is auto or scroll
        const overflowX = await codeBlock.evaluate((el) => {
          return window.getComputedStyle(el).overflowX;
        });
        expect(['auto', 'scroll']).toContain(overflowX);

        // Verify the code block doesn't break the layout
        const boundingBox = await codeBlock.boundingBox();
        const viewportWidth = VIEWPORTS.mobile.width;

        // Code block should not extend beyond viewport visually
        // (even if content inside is scrollable)
        expect(boundingBox.x).toBeGreaterThanOrEqual(0);
        expect(boundingBox.x + boundingBox.width).toBeLessThanOrEqual(viewportWidth + 5); // +5 for rounding
      }
    }

    // Verify page still has no horizontal scroll despite code blocks
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const bodyClientWidth = await page.evaluate(() => document.body.clientWidth);
    expect(bodyScrollWidth).toBeLessThanOrEqual(bodyClientWidth + 1);
  });

  // Additional tests for comprehensive mobile coverage

  test('Footer links are touch-friendly', async ({ page }) => {
    const footerLinks = page.locator('.footer-link');
    const linkCount = await footerLinks.count();

    for (let i = 0; i < linkCount; i++) {
      const link = footerLinks.nth(i);
      const isVisible = await link.isVisible();

      if (isVisible) {
        const boundingBox = await link.boundingBox();
        // Minimum touch target size
        expect(boundingBox.height).toBeGreaterThanOrEqual(44);
      }
    }
  });

  test('Container has appropriate padding on mobile', async ({ page }) => {
    const container = page.locator('.container').first();
    const padding = await container.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        left: parseFloat(styles.paddingLeft),
        right: parseFloat(styles.paddingRight)
      };
    });

    // Should have reasonable padding on mobile
    expect(padding.left).toBeGreaterThan(0);
    expect(padding.right).toBeGreaterThan(0);
  });

  test('All sections are visible and properly stacked', async ({ page }) => {
    const sections = ['#hero', '#features', '#architecture', '#configuration', '#getting-started'];

    for (const sectionId of sections) {
      const section = page.locator(sectionId);
      const exists = await section.count() > 0;

      if (exists) {
        await expect(section).toBeVisible();

        // Verify section is within viewport width
        const boundingBox = await section.boundingBox();
        expect(boundingBox.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width + 1);
      }
    }
  });
});
