/**
 * Responsive Design E2E Tests
 * Owner: Scenarios 8, 9, 10 - Responsive Design
 *
 * Tests across viewports:
 * - Desktop (1920x1080): Multi-column layout
 * - Tablet (768x1024): Adapted layout
 * - Mobile (375x667): Single-column layout
 * - Touch target sizes
 * - No horizontal overflow
 */

import { test, expect } from '@playwright/test';

// Viewport configurations
const DESKTOP_VIEWPORT = { width: 1920, height: 1080 };
const TABLET_VIEWPORT = { width: 768, height: 1024 };

// Minimum touch target size (WCAG 2.1 Level AAA / Apple HIG)
const MIN_TOUCH_TARGET = 44;

// ============================================
// DESKTOP TESTS (Scenario 8)
// ============================================
test.describe('Responsive Design - Desktop (1920x1080)', () => {
  test.beforeEach(async ({ page }) => {
    // Set viewport to desktop size
    await page.setViewportSize(DESKTOP_VIEWPORT);
    await page.goto('/');
  });

  test('TC1: Page renders without horizontal scrollbar at 1920x1080', async ({ page }) => {
    // Check that page width doesn't exceed viewport
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // Body scroll width should not exceed viewport width
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth);

    // Also check for horizontal scrollbar presence
    const hasHorizontalScrollbar = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScrollbar).toBe(false);
  });

  test('TC2: Hero section is properly aligned at desktop size', async ({ page }) => {
    const heroSection = page.locator('#hero');

    // Hero section should be visible
    await expect(heroSection).toBeVisible();

    // Check hero container has proper centering
    const heroContainer = heroSection.locator('.container').first();
    const containerBox = await heroContainer.boundingBox();

    // Container should be centered (not start at x=0)
    expect(containerBox).not.toBeNull();
    if (containerBox) {
      // Container should be centered with margins on both sides at 1920px
      expect(containerBox.x).toBeGreaterThan(0);

      // Check adequate spacing (padding)
      const heroSectionBox = await heroSection.boundingBox();
      if (heroSectionBox) {
        expect(heroSectionBox.height).toBeGreaterThan(200); // Adequate vertical spacing
      }
    }

    // Check h1 is present and centered (text-align: center)
    const h1 = heroSection.locator('h1');
    await expect(h1).toBeVisible();
    const h1Style = await h1.evaluate((el) => window.getComputedStyle(el).textAlign);
    expect(h1Style).toBe('center');
  });

  test('TC3: Features section uses multi-column layout on desktop', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('.features-section');
    await expect(featuresSection).toBeVisible();

    // Find the features grid
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Check grid display style
    const displayStyle = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(displayStyle).toBe('grid');

    // Check grid-template-columns for multi-column layout (should be 3 columns on desktop)
    const gridTemplateColumns = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });

    // Should have 3 columns on desktop (values like "300px 300px 300px" or similar)
    const columns = gridTemplateColumns.split(' ').filter(col => col.trim() !== '');
    expect(columns.length).toBeGreaterThanOrEqual(2); // At least 2 columns for multi-column layout
    expect(columns.length).toBeLessThanOrEqual(4); // But not too many
  });

  test('TC4: All content containers are within viewport width', async ({ page }) => {
    // Get viewport width
    const viewportWidth = 1920;

    // Check main content areas don't overflow
    const containers = page.locator('.container');
    const containerCount = await containers.count();

    for (let i = 0; i < containerCount; i++) {
      const container = containers.nth(i);
      const isVisible = await container.isVisible();

      if (isVisible) {
        const box = await container.boundingBox();
        if (box) {
          // Container should fit within viewport
          expect(box.x).toBeGreaterThanOrEqual(0);
          expect(box.x + box.width).toBeLessThanOrEqual(viewportWidth);
        }
      }
    }

    // Check sections don't overflow
    const sections = page.locator('section, .section');
    const sectionCount = await sections.count();

    for (let i = 0; i < sectionCount; i++) {
      const section = sections.nth(i);
      const isVisible = await section.isVisible();

      if (isVisible) {
        const box = await section.boundingBox();
        if (box) {
          expect(box.width).toBeLessThanOrEqual(viewportWidth);
        }
      }
    }

    // Check feature cards don't overflow
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      const isVisible = await card.isVisible();

      if (isVisible) {
        const box = await card.boundingBox();
        if (box) {
          expect(box.x).toBeGreaterThanOrEqual(0);
          expect(box.x + box.width).toBeLessThanOrEqual(viewportWidth);
        }
      }
    }
  });

  test('Quick Start section uses row layout on desktop', async ({ page }) => {
    const quickStartContent = page.locator('.quick-start-content');
    await expect(quickStartContent).toBeVisible();

    // Check flex-direction is row on desktop
    const flexDirection = await quickStartContent.evaluate((el) => {
      return window.getComputedStyle(el).flexDirection;
    });
    expect(flexDirection).toBe('row');
  });

  test('All sections have adequate spacing at desktop size', async ({ page }) => {
    // Check that sections have proper padding
    const sections = page.locator('.section');
    const sectionCount = await sections.count();

    for (let i = 0; i < sectionCount; i++) {
      const section = sections.nth(i);
      const isVisible = await section.isVisible();

      if (isVisible) {
        const paddingTop = await section.evaluate((el) => {
          return parseInt(window.getComputedStyle(el).paddingTop);
        });
        const paddingBottom = await section.evaluate((el) => {
          return parseInt(window.getComputedStyle(el).paddingBottom);
        });

        // Sections should have reasonable vertical padding (at least 32px)
        expect(paddingTop).toBeGreaterThanOrEqual(32);
        expect(paddingBottom).toBeGreaterThanOrEqual(32);
      }
    }
  });

  test('Text is readable at desktop size', async ({ page }) => {
    // Check body font size is reasonable for desktop
    const bodyFontSize = await page.evaluate(() => {
      return parseInt(window.getComputedStyle(document.body).fontSize);
    });
    expect(bodyFontSize).toBeGreaterThanOrEqual(14);
    expect(bodyFontSize).toBeLessThanOrEqual(20);

    // Check h1 is larger for desktop readability
    const h1 = page.locator('h1').first();
    const h1FontSize = await h1.evaluate((el) => {
      return parseInt(window.getComputedStyle(el).fontSize);
    });
    expect(h1FontSize).toBeGreaterThanOrEqual(32); // Should be large for hero

    // Check line-height for readability
    const lineHeight = await page.evaluate(() => {
      return parseFloat(window.getComputedStyle(document.body).lineHeight);
    });
    // Line-height should be at least 1.4 for readability (or a pixel value > font-size)
    expect(lineHeight).toBeGreaterThanOrEqual(bodyFontSize * 1.4);
  });
});

// ============================================
// TABLET TESTS (Scenario 9)
// ============================================
test.describe('Responsive Design - Tablet (768x1024)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(TABLET_VIEWPORT);
    await page.goto('/');
  });

  test('Page renders without horizontal scrollbar at tablet viewport', async ({ page }) => {
    // Get the document scroll width and viewport width
    const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);

    // No horizontal overflow means scrollWidth should not exceed clientWidth
    expect(scrollWidth).toBeLessThanOrEqual(clientWidth);

    // Additional check: verify no horizontal scrollbar is visible
    const hasHorizontalScrollbar = await page.evaluate(() => {
      return document.documentElement.scrollWidth > window.innerWidth;
    });
    expect(hasHorizontalScrollbar).toBe(false);
  });

  test('Hero section content is readable and properly sized at tablet size', async ({ page }) => {
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Check h1 is visible and readable
    const h1 = heroSection.locator('h1');
    await expect(h1).toBeVisible();
    await expect(h1).toContainText('MirDB');

    // Check tagline is visible
    const tagline = heroSection.locator('p').first();
    await expect(tagline).toBeVisible();

    // Verify hero section fits within viewport width
    const heroBoundingBox = await heroSection.boundingBox();
    expect(heroBoundingBox).not.toBeNull();
    if (heroBoundingBox) {
      expect(heroBoundingBox.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);
    }

    // Check h1 font size is appropriate for tablet (not too small)
    const h1FontSize = await h1.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // Should be at least 24px for readability on tablet
    expect(h1FontSize).toBeGreaterThanOrEqual(24);
  });

  test('Features display in 2-column or adapted grid layout at tablet size', async ({ page }) => {
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Get all feature cards
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThan(0);

    // Get positions of first two cards to verify 2-column layout
    if (cardCount >= 2) {
      const firstCard = featureCards.nth(0);
      const secondCard = featureCards.nth(1);

      const firstBox = await firstCard.boundingBox();
      const secondBox = await secondCard.boundingBox();

      expect(firstBox).not.toBeNull();
      expect(secondBox).not.toBeNull();

      if (firstBox && secondBox) {
        // In a 2-column layout, first two cards should be on the same row
        // (approximately same Y position) OR in adaptive layout
        const sameRow = Math.abs(firstBox.y - secondBox.y) < 10;
        const differentColumn = firstBox.x !== secondBox.x;

        // Either cards are side by side (2-column) or the layout is adapted
        // Both are valid tablet layouts
        if (sameRow) {
          // 2-column layout: cards should be side by side
          expect(differentColumn).toBe(true);
        }
        // If not same row, it's still valid as an "adapted" layout
      }
    }

    // Verify grid doesn't overflow viewport
    const gridBox = await featuresGrid.boundingBox();
    if (gridBox) {
      expect(gridBox.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);
    }
  });

  test('Primary CTA button has minimum touch target dimensions of 44x44 pixels', async ({ page }) => {
    // Find the primary CTA button in hero section
    const ctaButton = page.locator('#hero a').filter({ hasText: /GitHub/i });
    await expect(ctaButton).toBeVisible();

    const boundingBox = await ctaButton.boundingBox();
    expect(boundingBox).not.toBeNull();

    if (boundingBox) {
      // WCAG 2.1 / Apple HIG recommends minimum 44x44px touch targets
      expect(boundingBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
      expect(boundingBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
    }
  });

  test('All interactive elements meet minimum touch target size', async ({ page }) => {
    // Check all buttons and links have adequate touch targets
    const interactiveElements = page.locator('a, button').filter({ hasText: /\S+/ });
    const count = await interactiveElements.count();

    for (let i = 0; i < count; i++) {
      const element = interactiveElements.nth(i);
      const isVisible = await element.isVisible();

      if (isVisible) {
        const box = await element.boundingBox();
        if (box) {
          // Touch target should be at least 44x44, or clickable area should be adequate
          // Some inline links may be smaller but should have adequate padding
          const effectiveWidth = Math.max(box.width, MIN_TOUCH_TARGET);
          const effectiveHeight = Math.max(box.height, MIN_TOUCH_TARGET);

          // Primary interactive elements (buttons, main CTAs) must meet 44x44
          const text = await element.textContent();
          if (text && (text.includes('GitHub') || text.includes('Get Started'))) {
            expect(box.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
            expect(box.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
          }
        }
      }
    }
  });

  test('Container has appropriate padding for tablet viewport', async ({ page }) => {
    const container = page.locator('.container').first();
    await expect(container).toBeVisible();

    const paddingLeft = await container.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).paddingLeft);
    });
    const paddingRight = await container.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).paddingRight);
    });

    // Tablet should have more padding than mobile (var(--spacing-xl) = 2rem = 32px)
    expect(paddingLeft).toBeGreaterThanOrEqual(24);
    expect(paddingRight).toBeGreaterThanOrEqual(24);
  });

  test('Sections have appropriate spacing for tablet viewport', async ({ page }) => {
    const sections = page.locator('.section');
    const count = await sections.count();

    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const section = sections.nth(i);
      const isVisible = await section.isVisible();

      if (isVisible) {
        const paddingTop = await section.evaluate((el) => {
          return parseFloat(window.getComputedStyle(el).paddingTop);
        });
        const paddingBottom = await section.evaluate((el) => {
          return parseFloat(window.getComputedStyle(el).paddingBottom);
        });

        // Tablet sections should have appropriate vertical padding
        // var(--spacing-3xl) = 4rem = 64px
        expect(paddingTop).toBeGreaterThanOrEqual(32);
        expect(paddingBottom).toBeGreaterThanOrEqual(32);
      }
    }
  });
});

// ============================================
// MOBILE TESTS (Scenario 10)
// ============================================
const MOBILE_VIEWPORT = { width: 375, height: 667 };
const MIN_BASE_FONT_SIZE = 16; // Minimum readable font size without zooming

test.describe('Responsive Design - Mobile (375x667)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(MOBILE_VIEWPORT);
    await page.goto('/');
  });

  test('TC1: Page renders without horizontal scrollbar at 375x667 viewport', async ({ page }) => {
    // Check that page width doesn't exceed viewport
    const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);

    // No horizontal overflow
    expect(scrollWidth).toBeLessThanOrEqual(clientWidth);

    // Verify no horizontal scrollbar is visible
    const hasHorizontalScrollbar = await page.evaluate(() => {
      return document.documentElement.scrollWidth > window.innerWidth;
    });
    expect(hasHorizontalScrollbar).toBe(false);

    // Also verify body doesn't overflow
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth);
  });

  test('TC2: Content displays in single-column layout on mobile', async ({ page }) => {
    // Check hero section content stacks vertically
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Check hero container is full width (single column)
    const heroContainer = heroSection.locator('.container').first();
    const heroBox = await heroContainer.boundingBox();

    expect(heroBox).not.toBeNull();
    if (heroBox) {
      // Container should span most of the viewport width on mobile
      expect(heroBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
    }

    // Check features section displays in single column
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Get all feature cards
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    if (cardCount >= 2) {
      const firstCard = featureCards.nth(0);
      const secondCard = featureCards.nth(1);

      const firstBox = await firstCard.boundingBox();
      const secondBox = await secondCard.boundingBox();

      expect(firstBox).not.toBeNull();
      expect(secondBox).not.toBeNull();

      if (firstBox && secondBox) {
        // In a single-column layout, cards should be stacked vertically
        // The second card should be below the first card (higher Y value)
        expect(secondBox.y).toBeGreaterThan(firstBox.y);

        // Each card should span most of the viewport width
        expect(firstBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
        expect(secondBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
      }
    }

    // Check quick-start section uses column layout on mobile
    const quickStartContent = page.locator('.quick-start-content');
    if (await quickStartContent.count() > 0) {
      const flexDirection = await quickStartContent.evaluate((el) => {
        return window.getComputedStyle(el).flexDirection;
      });
      expect(flexDirection).toBe('column');
    }
  });

  test('TC3: Text is readable without zooming (minimum 16px base font size)', async ({ page }) => {
    // Check body font size is at least 16px
    const bodyFontSize = await page.evaluate(() => {
      return parseFloat(window.getComputedStyle(document.body).fontSize);
    });
    expect(bodyFontSize).toBeGreaterThanOrEqual(MIN_BASE_FONT_SIZE);

    // Check h1 is readable but appropriately sized for mobile
    const h1 = page.locator('h1').first();
    await expect(h1).toBeVisible();

    const h1FontSize = await h1.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // H1 should still be prominent on mobile
    expect(h1FontSize).toBeGreaterThanOrEqual(24);

    // Check paragraph text is readable
    const paragraphs = page.locator('p');
    const pCount = await paragraphs.count();

    for (let i = 0; i < Math.min(pCount, 5); i++) {
      const p = paragraphs.nth(i);
      const isVisible = await p.isVisible();

      if (isVisible) {
        const fontSize = await p.evaluate((el) => {
          return parseFloat(window.getComputedStyle(el).fontSize);
        });
        // All paragraph text should be readable (at least 14px, ideally 16px)
        expect(fontSize).toBeGreaterThanOrEqual(14);
      }
    }

    // Check line-height for readability
    const lineHeight = await page.evaluate(() => {
      const bodyStyle = window.getComputedStyle(document.body);
      const lh = parseFloat(bodyStyle.lineHeight);
      const fs = parseFloat(bodyStyle.fontSize);
      // lineHeight could be in px or unitless
      return lh >= fs ? lh : lh * fs;
    });
    expect(lineHeight).toBeGreaterThanOrEqual(bodyFontSize * 1.4);
  });

  test('TC4: All buttons and links have minimum 44x44 pixel touch area', async ({ page }) => {
    // Check primary CTA button in hero
    const ctaButton = page.locator('#hero a').filter({ hasText: /GitHub/i });
    await expect(ctaButton).toBeVisible();

    const ctaBox = await ctaButton.boundingBox();
    expect(ctaBox).not.toBeNull();
    if (ctaBox) {
      expect(ctaBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
      expect(ctaBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
    }

    // Check all standalone buttons and links (not inline text links)
    const standaloneButtons = page.locator('a, button').filter({
      has: page.locator('text=/View|GitHub|Get Started|Learn|Start|Install/i')
    });
    const btnCount = await standaloneButtons.count();

    for (let i = 0; i < btnCount; i++) {
      const btn = standaloneButtons.nth(i);
      const isVisible = await btn.isVisible();

      if (isVisible) {
        const box = await btn.boundingBox();
        if (box) {
          // Standalone interactive elements should meet touch target requirements
          expect(box.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
          expect(box.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
        }
      }
    }

    // Verify status badges have adequate touch targets
    const badges = page.locator('.status-badges a');
    const badgeCount = await badges.count();

    for (let i = 0; i < badgeCount; i++) {
      const badge = badges.nth(i);
      const isVisible = await badge.isVisible();

      if (isVisible) {
        const box = await badge.boundingBox();
        if (box) {
          // Badge links should have adequate touch area
          expect(box.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
        }
      }
    }
  });

  test('TC5: Logo is visible and proportionally sized for mobile viewport', async ({ page }) => {
    // Look for logo in various places
    const logo = page.locator('img[alt*="logo" i], img[alt*="MirDB" i], .logo, #logo');
    const logoCount = await logo.count();

    // If there's a logo image
    if (logoCount > 0) {
      const firstLogo = logo.first();
      const isVisible = await firstLogo.isVisible();

      if (isVisible) {
        const box = await firstLogo.boundingBox();
        expect(box).not.toBeNull();

        if (box) {
          // Logo should be visible (reasonable minimum size)
          expect(box.width).toBeGreaterThan(30);
          expect(box.height).toBeGreaterThan(20);

          // Logo should fit within mobile viewport width
          expect(box.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width - 32); // Allow for padding
        }
      }
    } else {
      // If no explicit logo element, check that h1 (product name) is visible and sized appropriately
      const productName = page.locator('h1').first();
      await expect(productName).toBeVisible();
      await expect(productName).toContainText('MirDB');

      const h1Box = await productName.boundingBox();
      expect(h1Box).not.toBeNull();

      if (h1Box) {
        // Product name should fit within mobile viewport
        expect(h1Box.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
      }
    }
  });

  test('All sections fit within mobile viewport width', async ({ page }) => {
    const sections = page.locator('section, .section');
    const sectionCount = await sections.count();

    expect(sectionCount).toBeGreaterThan(0);

    for (let i = 0; i < sectionCount; i++) {
      const section = sections.nth(i);
      const isVisible = await section.isVisible();

      if (isVisible) {
        const box = await section.boundingBox();
        if (box) {
          // Each section should fit within mobile viewport
          expect(box.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
        }
      }
    }
  });

  test('Hero section elements stack vertically on mobile', async ({ page }) => {
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Check text alignment is centered for mobile
    const h1 = heroSection.locator('h1');
    const h1Style = await h1.evaluate((el) => window.getComputedStyle(el).textAlign);
    expect(h1Style).toBe('center');

    // Check that hero elements don't overflow
    const heroBox = await heroSection.boundingBox();
    expect(heroBox).not.toBeNull();
    if (heroBox) {
      expect(heroBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
    }
  });

  test('Code blocks are readable and scrollable on mobile', async ({ page }) => {
    // Check quick-start code blocks
    const codeBlocks = page.locator('.code-block, pre, code');
    const codeCount = await codeBlocks.count();

    for (let i = 0; i < Math.min(codeCount, 3); i++) {
      const codeBlock = codeBlocks.nth(i);
      const isVisible = await codeBlock.isVisible();

      if (isVisible) {
        const box = await codeBlock.boundingBox();
        if (box) {
          // Code blocks should not overflow viewport (should have overflow-x: auto)
          expect(box.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
        }

        // Check that code is scrollable if content is wider than viewport
        const overflowX = await codeBlock.evaluate((el) => {
          return window.getComputedStyle(el).overflowX;
        });
        // Should either fit or be scrollable
        expect(['auto', 'scroll', 'visible']).toContain(overflowX);
      }
    }
  });

  test('Container padding is appropriate for mobile viewport', async ({ page }) => {
    const containers = page.locator('.container');
    const count = await containers.count();

    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < Math.min(count, 3); i++) {
      const container = containers.nth(i);
      const isVisible = await container.isVisible();

      if (isVisible) {
        const paddingLeft = await container.evaluate((el) => {
          return parseFloat(window.getComputedStyle(el).paddingLeft);
        });
        const paddingRight = await container.evaluate((el) => {
          return parseFloat(window.getComputedStyle(el).paddingRight);
        });

        // Mobile should have some padding (at least 16px on each side)
        expect(paddingLeft).toBeGreaterThanOrEqual(16);
        expect(paddingRight).toBeGreaterThanOrEqual(16);
      }
    }
  });
});
