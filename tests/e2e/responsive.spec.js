/**
 * Responsive Design E2E Tests
 * Owner: Scenarios 6 & 7
 *
 * Test cases for mobile viewport (375px) responsive design:
 * - No horizontal scrollbar at 375px
 * - Body text >= 16px on mobile
 * - Tap targets >= 44px
 * - All sections are accessible and visible
 * - Code blocks scroll horizontally, not page overflow
 */

const { test, expect } = require('@playwright/test');
const { VIEWPORTS, waitForPageLoad } = require('./test-utils');

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

    // We should be able to scroll to near the bottom
    expect(scrollPosition.scrollY + scrollPosition.viewportHeight).toBeGreaterThanOrEqual(
      scrollPosition.documentHeight - 10
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
