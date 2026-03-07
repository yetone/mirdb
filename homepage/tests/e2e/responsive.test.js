/**
 * Responsive Design E2E Tests
 * Owner: Scenarios 7, 8, 9 - Responsive Design
 *
 * Tests:
 * - Mobile layout (320px) - Scenario 7
 * - Tablet layout (768px) - Scenario 8
 * - Desktop layout (1024px+) - Scenario 9
 * - Features grid column counts
 * - No horizontal scrolling
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

// Mobile viewport (320px width)
const MOBILE_VIEWPORT = { width: 320, height: 568 };

// Helper to get the file URL
const getFileUrl = () => {
  const indexPath = path.resolve(__dirname, '../../index.html');
  return `file://${indexPath}`;
};

test.describe('Scenario 7: Mobile Responsive Design (320px)', () => {
  test.beforeEach(async ({ page }) => {
    // Set viewport to mobile width
    await page.setViewportSize(MOBILE_VIEWPORT);
    await page.goto(getFileUrl());
  });

  test('TC1: Page renders without horizontal scrolling at 320px viewport', async ({ page }) => {
    // Get the document body scroll width and viewport width
    const scrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // The scroll width should not exceed the viewport width significantly
    // Allow small tolerance (1px) for rendering differences
    expect(scrollWidth).toBeLessThanOrEqual(viewportWidth + 1);

    // Also verify there's no horizontal overflow on the html element
    const htmlScrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    expect(htmlScrollWidth).toBeLessThanOrEqual(viewportWidth + 1);

    // Verify no horizontal scrollbar by checking if page has overflow
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });

  test('TC2: Features grid displays in 1-column layout at 320px', async ({ page }) => {
    // Find the features grid
    const featuresGrid = page.locator('.features__grid');
    await expect(featuresGrid).toBeVisible();

    // Get computed style to check grid-template-columns
    const gridColumns = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).getPropertyValue('grid-template-columns');
    });

    // Should be 1 column (single value or "1fr")
    // The value will be the actual computed width in pixels (single column)
    const columnCount = gridColumns.trim().split(/\s+/).length;
    expect(columnCount).toBe(1);

    // Verify each feature card takes full width
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThan(0);

    // Check that cards are stacked vertically (all have similar X position)
    const cardPositions = [];
    for (let i = 0; i < Math.min(cardCount, 4); i++) {
      const card = featureCards.nth(i);
      const box = await card.boundingBox();
      if (box) {
        cardPositions.push(box.x);
      }
    }

    // All cards should have similar x position (single column)
    const allSameColumn = cardPositions.every((x) => Math.abs(x - cardPositions[0]) < 5);
    expect(allSameColumn).toBe(true);
  });

  test('TC3: All text is readable without zooming at 320px', async ({ page }) => {
    // Minimum readable font size is typically 12px
    const MIN_READABLE_FONT_SIZE = 12;

    // Check hero title
    const heroTitle = page.locator('.hero__title');
    if (await heroTitle.count() > 0) {
      const heroTitleFontSize = await heroTitle.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      expect(heroTitleFontSize).toBeGreaterThanOrEqual(MIN_READABLE_FONT_SIZE);
    }

    // Check hero tagline
    const heroTagline = page.locator('.hero__tagline');
    if (await heroTagline.count() > 0) {
      const taglineFontSize = await heroTagline.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      expect(taglineFontSize).toBeGreaterThanOrEqual(MIN_READABLE_FONT_SIZE);
    }

    // Check hero description
    const heroDesc = page.locator('.hero__description');
    if (await heroDesc.count() > 0) {
      const descFontSize = await heroDesc.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      expect(descFontSize).toBeGreaterThanOrEqual(MIN_READABLE_FONT_SIZE);
    }

    // Check feature card titles
    const featureTitle = page.locator('.feature-card__title').first();
    if (await featureTitle.count() > 0) {
      const featureTitleFontSize = await featureTitle.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      expect(featureTitleFontSize).toBeGreaterThanOrEqual(MIN_READABLE_FONT_SIZE);
    }

    // Check feature card descriptions
    const featureDesc = page.locator('.feature-card__description').first();
    if (await featureDesc.count() > 0) {
      const featureDescFontSize = await featureDesc.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      expect(featureDescFontSize).toBeGreaterThanOrEqual(MIN_READABLE_FONT_SIZE);
    }

    // Check section titles
    const sectionTitle = page.locator('.section__title').first();
    if (await sectionTitle.count() > 0) {
      const sectionTitleFontSize = await sectionTitle.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      expect(sectionTitleFontSize).toBeGreaterThanOrEqual(MIN_READABLE_FONT_SIZE);
    }

    // Check body text is readable
    const bodyFontSize = await page.evaluate(() => {
      return parseFloat(window.getComputedStyle(document.body).fontSize);
    });
    expect(bodyFontSize).toBeGreaterThanOrEqual(MIN_READABLE_FONT_SIZE);
  });

  test('TC4: Hero section content fits within viewport at 320px', async ({ page }) => {
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Get hero bounding box
    const heroBox = await heroSection.boundingBox();
    expect(heroBox).not.toBeNull();

    // Hero width should not exceed viewport width
    expect(heroBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width + 1);

    // Check hero container
    const heroContainer = page.locator('.hero__container');
    const containerBox = await heroContainer.boundingBox();
    expect(containerBox).not.toBeNull();
    expect(containerBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);

    // Check hero logo is visible and not overflowing
    const heroLogo = page.locator('.hero__logo');
    if (await heroLogo.count() > 0) {
      const logoBox = await heroLogo.boundingBox();
      if (logoBox) {
        expect(logoBox.x).toBeGreaterThanOrEqual(0);
        expect(logoBox.x + logoBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width + 1);
      }
    }

    // Check hero title is visible and fits
    const heroTitle = page.locator('.hero__title');
    if (await heroTitle.count() > 0) {
      await expect(heroTitle).toBeVisible();
      const titleBox = await heroTitle.boundingBox();
      if (titleBox) {
        expect(titleBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
      }
    }

    // Verify no horizontal overflow in hero section
    const heroOverflow = await heroSection.evaluate((el) => {
      return el.scrollWidth > el.clientWidth;
    });
    expect(heroOverflow).toBe(false);
  });

  test('TC5: All interactive elements have minimum 44x44px touch targets', async ({ page }) => {
    // WCAG 2.1 AAA recommends 44x44px minimum touch target size
    const MIN_TOUCH_TARGET = 44;

    // Check theme toggle button
    const themeToggle = page.locator('.theme-toggle');
    if (await themeToggle.count() > 0) {
      const themeBox = await themeToggle.boundingBox();
      if (themeBox) {
        expect(themeBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
        expect(themeBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
      }
    }

    // Check GitHub link
    const githubLink = page.locator('.github-link');
    if (await githubLink.count() > 0) {
      const githubBox = await githubLink.boundingBox();
      if (githubBox) {
        expect(githubBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
        expect(githubBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
      }
    }

    // Check copy buttons
    const copyButtons = page.locator('.code-block__copy-btn');
    const copyCount = await copyButtons.count();
    for (let i = 0; i < copyCount; i++) {
      const copyBtn = copyButtons.nth(i);
      if (await copyBtn.isVisible()) {
        const copyBox = await copyBtn.boundingBox();
        if (copyBox) {
          expect(copyBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
          expect(copyBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
        }
      }
    }

    // Check header logo link (should be tappable)
    const headerLogo = page.locator('.header__logo');
    if (await headerLogo.count() > 0) {
      const logoBox = await headerLogo.boundingBox();
      if (logoBox) {
        // Logo link should have adequate touch target
        expect(logoBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
      }
    }
  });

  test('Mobile layout: navigation is hidden on mobile', async ({ page }) => {
    // Navigation list should be hidden on mobile
    const navList = page.locator('.nav__list');
    await expect(navList).toBeHidden();
  });

  test('Mobile layout: container has proper mobile padding', async ({ page }) => {
    const container = page.locator('.container').first();
    const padding = await container.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        left: parseFloat(style.paddingLeft),
        right: parseFloat(style.paddingRight)
      };
    });

    // Container should have mobile-appropriate padding (not too large)
    expect(padding.left).toBeGreaterThan(0);
    expect(padding.right).toBeGreaterThan(0);
    // Padding shouldn't take up too much space on a 320px screen
    expect(padding.left + padding.right).toBeLessThan(80);
  });

  test('Mobile layout: terminal demo is readable and fits', async ({ page }) => {
    const terminal = page.locator('.terminal');
    if (await terminal.count() > 0) {
      await expect(terminal).toBeVisible();

      // Terminal should fit within viewport
      const terminalBox = await terminal.boundingBox();
      if (terminalBox) {
        expect(terminalBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
      }

      // Terminal body text should be readable
      const terminalBody = page.locator('.terminal__body');
      const terminalFontSize = await terminalBody.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      expect(terminalFontSize).toBeGreaterThanOrEqual(10); // Monospace can be slightly smaller
    }
  });

  test('Mobile layout: code blocks are scrollable horizontally if needed', async ({ page }) => {
    const codeBlocks = page.locator('.code-block__code');
    const count = await codeBlocks.count();

    for (let i = 0; i < count; i++) {
      const codeBlock = codeBlocks.nth(i);
      const overflow = await codeBlock.evaluate((el) => {
        return window.getComputedStyle(el).overflowX;
      });
      // Code blocks should allow horizontal scrolling for long lines
      expect(['auto', 'scroll']).toContain(overflow);
    }
  });
});
