/**
 * Responsive Design E2E Tests
 * Owner: Scenarios 6 & 7
 *
 * Test cases for mobile viewport (375px) and tablet viewport (768px):
 * - No horizontal scrollbar at tablet and mobile widths
 * - Body text >= 16px on mobile
 * - Tap targets >= 44px
 * - Features layout adapts at breakpoints
 * - Code blocks scroll horizontally, not page
 * - All sections are accessible and visible
 */

const { test, expect } = require('@playwright/test');
const { VIEWPORTS, waitForPageLoad } = require('./test-utils');

// ========================================
// MOBILE RESPONSIVE DESIGN TESTS (375px)
// ========================================

test.describe('Mobile Responsive Design (375px viewport)', () => {
  test.beforeEach(async ({ page }) => {
    // Set viewport to mobile size (375px width)
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.goto('/');
    await waitForPageLoad(page);
  });

  test('Test Case 1: Page renders without horizontal scrollbar at 375px', async ({ page }) => {
    // Check that document width does not exceed viewport width
    const { documentWidth, viewportWidth, hasHorizontalScrollbar } = await page.evaluate(() => {
      const docWidth = document.documentElement.scrollWidth;
      const vpWidth = window.innerWidth;
      const hasScroll = docWidth > vpWidth;
      return {
        documentWidth: docWidth,
        viewportWidth: vpWidth,
        hasHorizontalScrollbar: hasScroll
      };
    });

    expect(hasHorizontalScrollbar).toBe(false);
    expect(documentWidth).toBeLessThanOrEqual(viewportWidth);
  });

  test('Test Case 2: Body text is at least 16px font size', async ({ page }) => {
    // Check the body font size
    const bodyFontSize = await page.evaluate(() => {
      const body = document.body;
      const computed = window.getComputedStyle(body);
      return parseFloat(computed.fontSize);
    });

    expect(bodyFontSize).toBeGreaterThanOrEqual(16);

    // Also check paragraph text within sections
    const paragraphFontSize = await page.evaluate(() => {
      const paragraph = document.querySelector('.feature-description, .hero__tagline, p');
      if (!paragraph) return 16; // Default if no paragraph found
      const computed = window.getComputedStyle(paragraph);
      return parseFloat(computed.fontSize);
    });

    expect(paragraphFontSize).toBeGreaterThanOrEqual(16);
  });

  test('Test Case 3: CTA button tap target is at least 44px in height', async ({ page }) => {
    const ctaButton = page.locator('.hero__cta, .cta-button').first();
    await expect(ctaButton).toBeVisible();

    const buttonDimensions = await ctaButton.evaluate((el) => {
      const rect = el.getBoundingClientRect();
      return {
        width: rect.width,
        height: rect.height
      };
    });

    // Button should have at least 44px height for touch-friendly tap targets
    expect(buttonDimensions.height).toBeGreaterThanOrEqual(44);
  });

  test('Test Case 4: All sections are accessible and content is visible', async ({ page }) => {
    // Check that all main sections are present and visible
    const sections = [
      { id: '#hero', name: 'Hero section' },
      { id: '#features', name: 'Features section' },
      { id: '#quickstart', name: 'Quick Start section' }
    ];

    for (const section of sections) {
      const sectionElement = page.locator(section.id);
      await expect(sectionElement, `${section.name} should exist`).toBeAttached();

      // Scroll to the section
      await sectionElement.scrollIntoViewIfNeeded();

      // Check that section content is visible
      await expect(sectionElement, `${section.name} should be visible`).toBeVisible();
    }

    // Verify we can scroll through the entire page
    await page.evaluate(() => {
      window.scrollTo(0, document.body.scrollHeight);
    });

    // Wait for scroll to complete
    await page.waitForTimeout(300);

    // Check that we're at the bottom of the page
    const scrollPosition = await page.evaluate(() => {
      return {
        scrollY: window.scrollY,
        documentHeight: document.body.scrollHeight,
        viewportHeight: window.innerHeight
      };
    });

    // We should be able to scroll to near the bottom (with tolerance for smooth scrolling/rendering)
    expect(scrollPosition.scrollY + scrollPosition.viewportHeight).toBeGreaterThanOrEqual(
      scrollPosition.documentHeight - 50
    );
  });

  test('Test Case 5: Code blocks have horizontal scroll if needed, not page overflow', async ({ page }) => {
    // Navigate to quickstart section
    const quickstartSection = page.locator('#quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    // Find code blocks
    const codeBlocks = page.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();

    expect(codeBlockCount).toBeGreaterThan(0);

    // Check each code block for proper overflow handling
    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);

      const overflowStyle = await codeBlock.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        const rect = el.getBoundingClientRect();
        const parent = el.closest('.code-block-wrapper');
        const parentRect = parent ? parent.getBoundingClientRect() : rect;

        return {
          overflowX: computed.overflowX,
          scrollWidth: el.scrollWidth,
          clientWidth: el.clientWidth,
          hasOverflow: el.scrollWidth > el.clientWidth,
          parentOverflowX: parent ? window.getComputedStyle(parent).overflowX : 'none',
          blockRight: rect.right,
          viewportWidth: window.innerWidth
        };
      });

      // Code block or its wrapper should have overflow-x: auto or scroll
      const hasProperOverflow =
        overflowStyle.overflowX === 'auto' ||
        overflowStyle.overflowX === 'scroll' ||
        overflowStyle.parentOverflowX === 'auto' ||
        overflowStyle.parentOverflowX === 'scroll';

      expect(hasProperOverflow, `Code block ${i + 1} should have horizontal scroll enabled`).toBe(true);

      // Code block should not extend beyond viewport
      expect(
        overflowStyle.blockRight,
        `Code block ${i + 1} should not extend beyond viewport`
      ).toBeLessThanOrEqual(overflowStyle.viewportWidth + 1);
    }

    // Verify page still has no horizontal scroll after checking code blocks
    const pageHasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > window.innerWidth;
    });

    expect(pageHasHorizontalScroll, 'Page should not have horizontal scroll').toBe(false);
  });

  test('Features grid displays in single column on mobile', async ({ page }) => {
    const featuresGrid = page.locator('.features-grid');
    await featuresGrid.scrollIntoViewIfNeeded();

    const gridStyle = await featuresGrid.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        display: computed.display,
        gridTemplateColumns: computed.gridTemplateColumns
      };
    });

    expect(gridStyle.display).toBe('grid');

    // Should be single column (1fr or single value)
    const columns = gridStyle.gridTemplateColumns.split(' ').filter(c => c && c !== '0px');
    expect(columns.length).toBe(1);
  });

  test('Feature cards stack vertically on mobile', async ({ page }) => {
    const cards = page.locator('.feature-card');
    const cardCount = await cards.count();

    expect(cardCount).toBeGreaterThanOrEqual(3);

    const cardPositions = await cards.evaluateAll((cards) => {
      return cards.map(card => {
        const rect = card.getBoundingClientRect();
        return { top: rect.top, left: rect.left, width: rect.width };
      });
    });

    // All cards should have similar left positions (single column)
    const leftPositions = cardPositions.map(p => Math.round(p.left));
    const uniqueLeftPositions = [...new Set(leftPositions)];
    expect(uniqueLeftPositions.length).toBe(1);

    // Cards should have increasing top positions (stacked vertically)
    for (let i = 1; i < cardPositions.length; i++) {
      expect(cardPositions[i].top).toBeGreaterThan(cardPositions[i - 1].top);
    }
  });

  test('Navigation toggle (hamburger) is visible on mobile', async ({ page }) => {
    const navToggle = page.locator('.nav-toggle');

    // Check if nav toggle exists and has display: flex (visible)
    const isDisplayed = await navToggle.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return computed.display !== 'none';
    });

    expect(isDisplayed).toBe(true);
  });

  test('All interactive elements meet minimum tap target size', async ({ page }) => {
    // Check various interactive elements
    const interactiveSelectors = [
      '.hero__cta',
      '.copy-button',
      '.nav-toggle'
    ];

    for (const selector of interactiveSelectors) {
      const element = page.locator(selector).first();
      const elementCount = await element.count();

      if (elementCount > 0) {
        const dimensions = await element.evaluate((el) => {
          const rect = el.getBoundingClientRect();
          return {
            width: rect.width,
            height: rect.height
          };
        });

        // Minimum tap target should be 44px in either dimension
        expect(
          Math.max(dimensions.width, dimensions.height),
          `${selector} should have at least 44px tap target`
        ).toBeGreaterThanOrEqual(44);
      }
    }
  });
});

// ========================================
// TABLET RESPONSIVE DESIGN TESTS (768px)
// ========================================

test.describe('Responsive Design - Tablet (768px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.tablet);
    await page.goto('/');
    await waitForPageLoad(page);
  });

  test('page renders with tablet-appropriate layout at 768px', async ({ page }) => {
    // Verify viewport is set correctly
    const viewportSize = page.viewportSize();
    expect(viewportSize.width).toBe(768);

    // Verify page loads without errors
    const body = page.locator('body');
    await expect(body).toBeVisible();

    // Verify main content is visible
    const mainContent = page.locator('#main-content');
    await expect(mainContent).toBeVisible();

    // Verify all major sections are visible
    const hero = page.locator('#hero');
    const features = page.locator('#features');
    const quickstart = page.locator('#quickstart');
    const status = page.locator('#status');

    await expect(hero).toBeVisible();
    await expect(features).toBeVisible();
    await expect(quickstart).toBeVisible();
    await expect(status).toBeVisible();
  });

  test('features display in 2 column grid at 768px tablet width', async ({ page }) => {
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Get computed style for grid-template-columns
    const gridStyle = await featuresGrid.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        display: computed.display,
        gridTemplateColumns: computed.gridTemplateColumns,
      };
    });

    // Verify it's using grid display
    expect(gridStyle.display).toBe('grid');

    // At 768px, the features grid should have 2 columns
    // gridTemplateColumns will be computed to actual pixel values like "329.5px 329.5px"
    const columnCount = gridStyle.gridTemplateColumns.split(' ').length;
    expect(columnCount).toBe(2);
  });

  test('all content is accessible at tablet width - no horizontal scrollbar', async ({ page }) => {
    // Check that body doesn't have horizontal overflow
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });

    expect(hasHorizontalScroll).toBe(false);
  });

  test('all major sections are within viewport bounds at 768px', async ({ page }) => {
    const viewportWidth = 768;

    // Check each section's width
    const sections = ['#hero', '#features', '#quickstart', '#status'];

    for (const selector of sections) {
      const section = page.locator(selector);
      const boundingBox = await section.boundingBox();

      // Section should not extend beyond viewport
      expect(boundingBox.x).toBeGreaterThanOrEqual(0);
      expect(boundingBox.x + boundingBox.width).toBeLessThanOrEqual(viewportWidth + 1); // +1 for rounding
    }
  });

  test('navigation is visible and accessible at tablet width', async ({ page }) => {
    const nav = page.locator('.nav');
    await expect(nav).toBeVisible();

    // Check if navigation links are visible or hamburger is shown
    const navMenu = page.locator('.nav-menu');
    const navToggle = page.locator('.nav-toggle');

    // At 768px, either the full menu or hamburger should be visible
    const navMenuVisible = await navMenu.isVisible();
    const navToggleDisplayStyle = await navToggle.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });

    // Either full menu is visible OR hamburger is shown (display: flex)
    // At tablet, navigation may still be visible or collapse to hamburger
    expect(navMenuVisible || navToggleDisplayStyle !== 'none').toBe(true);
  });

  test('hero section displays correctly at tablet width', async ({ page }) => {
    const heroLogo = page.locator('.hero__logo');
    const heroTitle = page.locator('.hero__title');
    const heroTagline = page.locator('.hero__tagline');
    const heroCta = page.locator('.hero__cta');

    await expect(heroLogo).toBeVisible();
    await expect(heroTitle).toBeVisible();
    await expect(heroTagline).toBeVisible();
    await expect(heroCta).toBeVisible();

    // Verify elements are within viewport
    const viewportWidth = 768;
    for (const element of [heroLogo, heroTitle, heroTagline, heroCta]) {
      const boundingBox = await element.boundingBox();
      expect(boundingBox.x).toBeGreaterThanOrEqual(0);
      expect(boundingBox.x + boundingBox.width).toBeLessThanOrEqual(viewportWidth + 1);
    }
  });

  test('feature cards are all visible and accessible', async ({ page }) => {
    const featureCards = page.locator('.feature-card');
    const count = await featureCards.count();

    // Should have 3 feature cards
    expect(count).toBe(3);

    // All cards should be visible
    for (let i = 0; i < count; i++) {
      await expect(featureCards.nth(i)).toBeVisible();
    }
  });

  test('code blocks do not cause horizontal page scroll', async ({ page }) => {
    const codeBlocks = page.locator('.code-block');
    const count = await codeBlocks.count();

    // Verify code blocks exist
    expect(count).toBeGreaterThan(0);

    // Check that code blocks have overflow-x handling
    for (let i = 0; i < count; i++) {
      const overflowStyle = await codeBlocks.nth(i).evaluate((el) => {
        return window.getComputedStyle(el).overflowX;
      });

      // Code blocks should handle overflow internally (auto or scroll)
      expect(['auto', 'scroll', 'hidden']).toContain(overflowStyle);
    }

    // Verify no horizontal page scroll
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });

  test('text remains readable at tablet width (>= 16px base font)', async ({ page }) => {
    // Check body font size
    const bodyFontSize = await page.locator('body').evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });

    expect(bodyFontSize).toBeGreaterThanOrEqual(16);
  });

  test('quick start section displays correctly at tablet width', async ({ page }) => {
    const quickstart = page.locator('#quickstart');
    await expect(quickstart).toBeVisible();

    // Verify code blocks are present and visible
    const installCode = page.locator('#install-code');
    const usageCode = page.locator('#usage-code');

    await expect(installCode).toBeVisible();
    await expect(usageCode).toBeVisible();

    // Verify copy buttons are accessible
    const copyButtons = page.locator('.copy-button');
    const buttonCount = await copyButtons.count();
    expect(buttonCount).toBeGreaterThanOrEqual(2);
  });

  test('status section displays correctly at tablet width', async ({ page }) => {
    const status = page.locator('#status');
    await expect(status).toBeVisible();

    // Verify feature checklist is visible
    const checklist = page.locator('.feature-checklist');
    await expect(checklist).toBeVisible();

    // Verify checklist items are visible
    const checklistItems = page.locator('.feature-item');
    const itemCount = await checklistItems.count();
    expect(itemCount).toBeGreaterThan(0);

    for (let i = 0; i < itemCount; i++) {
      await expect(checklistItems.nth(i)).toBeVisible();
    }
  });

  test('interactive elements have adequate tap targets (>= 44px)', async ({ page }) => {
    // Check CTA button
    const ctaButton = page.locator('.cta-button');
    const ctaBoundingBox = await ctaButton.boundingBox();
    expect(ctaBoundingBox.height).toBeGreaterThanOrEqual(44);

    // Check copy buttons
    const copyButtons = page.locator('.copy-button');
    const copyButtonCount = await copyButtons.count();

    for (let i = 0; i < copyButtonCount; i++) {
      const boundingBox = await copyButtons.nth(i).boundingBox();
      // At least one dimension should be >= 44px for touch accessibility
      expect(Math.max(boundingBox.height, boundingBox.width)).toBeGreaterThanOrEqual(44);
    }
  });

  test('footer element exists and adapts appropriately at tablet width', async ({ page }) => {
    const footer = page.locator('footer');

    // Footer element should exist in the DOM
    await expect(footer).toHaveCount(1);

    // Footer should be within viewport bounds if it has dimensions
    const viewportWidth = 768;
    const boundingBox = await footer.boundingBox();

    if (boundingBox && boundingBox.height > 0) {
      expect(boundingBox.x).toBeGreaterThanOrEqual(0);
      expect(boundingBox.x + boundingBox.width).toBeLessThanOrEqual(viewportWidth + 1);
    }
  });
});

// ========================================
// LAYOUT TRANSITIONS TESTS
// ========================================

test.describe('Responsive Design - Layout Transitions', () => {
  test('features grid transitions from 3-col desktop to 2-col tablet', async ({ page }) => {
    // First check desktop (3 columns)
    await page.setViewportSize(VIEWPORTS.desktop);
    await page.goto('/');
    await waitForPageLoad(page);

    const featuresGrid = page.locator('.features-grid');

    const desktopColumns = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns.split(' ').length;
    });
    expect(desktopColumns).toBe(3);

    // Then check tablet (2 columns)
    await page.setViewportSize(VIEWPORTS.tablet);
    await page.waitForTimeout(100); // Allow for reflow

    const tabletColumns = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns.split(' ').length;
    });
    expect(tabletColumns).toBe(2);
  });

  test('features grid transitions from 2-col tablet to 1-col mobile', async ({ page }) => {
    // First check tablet (2 columns)
    await page.setViewportSize(VIEWPORTS.tablet);
    await page.goto('/');
    await waitForPageLoad(page);

    const featuresGrid = page.locator('.features-grid');

    const tabletColumns = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns.split(' ').length;
    });
    expect(tabletColumns).toBe(2);

    // Then check mobile (1 column)
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.waitForTimeout(100); // Allow for reflow

    const mobileColumns = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns.split(' ').filter(c => c && c !== '0px').length;
    });
    expect(mobileColumns).toBe(1);
  });

  test('content reflows smoothly between desktop and tablet', async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.tablet);
    await page.goto('/');
    await waitForPageLoad(page);

    // No horizontal overflow at any point
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // All sections should be visible
    const sections = ['#hero', '#features', '#quickstart', '#status'];
    for (const selector of sections) {
      await expect(page.locator(selector)).toBeVisible();
    }
  });
});
