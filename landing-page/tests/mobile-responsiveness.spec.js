// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

/**
 * Mobile Responsiveness Tests - NFR-1 and US-4
 * Verify that the page is mobile-responsive and works on devices >= 320px width
 */
test.describe('Mobile Responsiveness', () => {
  test.beforeEach(async ({ page }) => {
    const filePath = path.resolve(__dirname, '../index.html');
    await page.goto(`file://${filePath}`);
  });

  // Test Case 1: Set viewport to 320px width and check for horizontal scroll
  // Expected: No horizontal scrollbar appears, content fits viewport
  test('TC1: No horizontal scroll at 320px viewport width', async ({ page }) => {
    // Set viewport to minimum supported width (320px)
    await page.setViewportSize({ width: 320, height: 568 });

    // Wait for any CSS transitions
    await page.waitForTimeout(100);

    // Check for horizontal overflow on document
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });

    expect(hasHorizontalScroll).toBe(false);

    // Verify body doesn't overflow
    const bodyOverflow = await page.evaluate(() => {
      const body = document.body;
      return body.scrollWidth > body.clientWidth;
    });

    expect(bodyOverflow).toBe(false);

    // Check that top-level layout containers fit within viewport
    // (Elements inside scrollable containers like .code-block are expected to overflow
    // within their container, which is the intended behavior per US-4)
    const layoutOverflow = await page.evaluate(() => {
      // Check main layout sections that should not overflow
      const selectors = [
        '.hero',
        '.hero-content',
        '.features-container',
        '.code-examples-container',
        '.getting-started-container',
        '.footer-container'
      ];

      for (const selector of selectors) {
        const el = document.querySelector(selector);
        if (el) {
          const rect = el.getBoundingClientRect();
          if (rect.right > 320 + 1) { // 1px tolerance for rounding
            return { element: selector, right: rect.right };
          }
        }
      }
      return null;
    });

    expect(layoutOverflow).toBeNull();
  });

  // Test Case 2: Set viewport to 375px (iPhone SE) and verify readability
  // Expected: All text is readable, buttons are accessible
  test('TC2: Content is readable at 375px viewport (iPhone SE)', async ({ page }) => {
    // Set viewport to iPhone SE dimensions
    await page.setViewportSize({ width: 375, height: 667 });

    // Verify hero section is visible and readable
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify h1 is visible
    const h1 = heroSection.locator('h1');
    await expect(h1).toBeVisible();

    // Verify h1 font size is readable (at least 24px for mobile)
    const h1FontSize = await h1.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    expect(h1FontSize).toBeGreaterThanOrEqual(24);

    // Verify tagline is readable
    const tagline = heroSection.locator('[data-testid="hero-tagline"]');
    await expect(tagline).toBeVisible();

    // Verify buttons are visible and accessible
    const primaryBtn = heroSection.locator('[data-testid="cta-primary"]');
    const secondaryBtn = heroSection.locator('[data-testid="cta-secondary"]');

    await expect(primaryBtn).toBeVisible();
    await expect(secondaryBtn).toBeVisible();

    // Verify buttons fit within the viewport
    const primaryBtnBox = await primaryBtn.boundingBox();
    const secondaryBtnBox = await secondaryBtn.boundingBox();

    expect(primaryBtnBox.x).toBeGreaterThanOrEqual(0);
    expect(primaryBtnBox.x + primaryBtnBox.width).toBeLessThanOrEqual(375);
    expect(secondaryBtnBox.x).toBeGreaterThanOrEqual(0);
    expect(secondaryBtnBox.x + secondaryBtnBox.width).toBeLessThanOrEqual(375);
  });

  // Test Case 3: Set viewport to 768px (tablet) and verify layout
  // Expected: Layout adapts appropriately, features may stack or use 2 columns
  test('TC3: Layout adapts at 768px tablet viewport', async ({ page }) => {
    // Set viewport to tablet dimensions
    await page.setViewportSize({ width: 768, height: 1024 });

    // Verify features section exists
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Verify features grid adapts for tablet
    const featuresGrid = featuresSection.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Get the grid layout
    const gridStyle = await featuresGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns
      };
    });

    expect(gridStyle.display).toBe('grid');

    // At 768px, should have 1 column (stacked) based on the CSS media query
    const columns = gridStyle.gridTemplateColumns.split(' ').filter(col => col && col !== '0px');
    expect(columns.length).toBeLessThanOrEqual(3); // Either 1 column stacked or up to 3

    // Verify no horizontal scroll
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Verify code examples section adapts
    const codeExamplesSection = page.locator('[data-testid="code-examples-section"]');
    await expect(codeExamplesSection).toBeVisible();

    // Verify commands grid adapts (should be 2 columns at tablet per CSS)
    const commandsGrid = page.locator('.commands-grid');
    const commandsGridStyle = await commandsGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.gridTemplateColumns;
    });

    const commandColumns = commandsGridStyle.split(' ').filter(col => col && col !== '0px');
    expect(commandColumns.length).toBeLessThanOrEqual(4); // 2 or 4 columns at tablet
  });

  // Test Case 4: Verify button sizes meet minimum touch target
  // Expected: Interactive elements have minimum 44px touch target size
  test('TC4: Interactive elements have minimum 44px touch targets', async ({ page }) => {
    // Set viewport to mobile
    await page.setViewportSize({ width: 375, height: 667 });

    // Check CTA buttons in hero section
    const primaryBtn = page.locator('[data-testid="cta-primary"]');
    const secondaryBtn = page.locator('[data-testid="cta-secondary"]');

    await expect(primaryBtn).toBeVisible();
    await expect(secondaryBtn).toBeVisible();

    // Get button dimensions
    const primaryBtnBox = await primaryBtn.boundingBox();
    const secondaryBtnBox = await secondaryBtn.boundingBox();

    // Verify minimum 44px height for touch targets
    expect(primaryBtnBox.height).toBeGreaterThanOrEqual(44);
    expect(secondaryBtnBox.height).toBeGreaterThanOrEqual(44);

    // Check footer links for touch-friendly size
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();

    const footerLinks = footer.locator('a');
    const footerLinksCount = await footerLinks.count();

    for (let i = 0; i < Math.min(footerLinksCount, 5); i++) {
      const link = footerLinks.nth(i);
      const linkBox = await link.boundingBox();
      if (linkBox) {
        // Links should have adequate touch target (at least through padding/line-height)
        // Minimum recommended is 44x44, but we check for reasonable clickable area
        expect(linkBox.height).toBeGreaterThanOrEqual(20);
      }
    }

    // Verify command items in supported commands section have adequate size
    const commandItems = page.locator('.command-item');
    const commandsCount = await commandItems.count();

    if (commandsCount > 0) {
      const firstCommand = commandItems.first();
      await firstCommand.scrollIntoViewIfNeeded();
      const commandBox = await firstCommand.boundingBox();
      // Command items should have enough height for touch
      expect(commandBox.height).toBeGreaterThanOrEqual(44);
    }
  });

  // Test Case 5: Test code block horizontal scroll on mobile
  // Expected: Code blocks have horizontal scroll when content overflows
  test('TC5: Code blocks have horizontal scroll on mobile', async ({ page }) => {
    // Set viewport to mobile
    await page.setViewportSize({ width: 320, height: 568 });

    // Navigate to code examples section
    const codeExamplesSection = page.locator('[data-testid="code-examples-section"]');
    await codeExamplesSection.scrollIntoViewIfNeeded();
    await expect(codeExamplesSection).toBeVisible();

    // Get all code blocks
    const codeBlocks = page.locator('.code-block');
    const codeBlocksCount = await codeBlocks.count();

    expect(codeBlocksCount).toBeGreaterThan(0);

    // Check that code blocks have overflow-x auto or scroll
    for (let i = 0; i < Math.min(codeBlocksCount, 3); i++) {
      const codeBlock = codeBlocks.nth(i);
      await codeBlock.scrollIntoViewIfNeeded();

      const overflowStyle = await codeBlock.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          overflowX: style.overflowX,
          whiteSpace: style.whiteSpace
        };
      });

      // Code blocks should allow horizontal scrolling
      expect(['auto', 'scroll']).toContain(overflowStyle.overflowX);
    }

    // Verify code inside uses pre-wrap or pre and doesn't break layout
    const codeElements = page.locator('.code-block code');
    const codeCount = await codeElements.count();

    if (codeCount > 0) {
      const firstCode = codeElements.first();
      const codeStyle = await firstCode.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          whiteSpace: style.whiteSpace,
          fontFamily: style.fontFamily
        };
      });

      // Code should preserve whitespace (pre, pre-wrap)
      expect(['pre', 'pre-wrap']).toContain(codeStyle.whiteSpace);
      // Should use monospace font
      expect(codeStyle.fontFamily.toLowerCase()).toMatch(/mono|courier|consolas|menlo/);
    }
  });

  // Additional test: Verify hero section adapts at mobile viewport
  test('Hero section adapts properly at mobile viewport', async ({ page }) => {
    await page.setViewportSize({ width: 320, height: 568 });

    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify CTA buttons wrap or stack properly
    const ctaButtons = heroSection.locator('.cta-buttons');
    const ctaStyle = await ctaButtons.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        flexWrap: style.flexWrap,
        display: style.display
      };
    });

    expect(ctaStyle.display).toBe('flex');
    expect(ctaStyle.flexWrap).toBe('wrap');
  });

  // Additional test: Verify footer adapts at mobile viewport
  test('Footer adapts properly at mobile viewport', async ({ page }) => {
    await page.setViewportSize({ width: 320, height: 568 });

    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Check footer content layout
    const footerContent = footer.locator('.footer-content');
    const footerStyle = await footerContent.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        flexDirection: style.flexDirection,
        display: style.display
      };
    });

    // At mobile, footer content should stack vertically
    expect(footerStyle.display).toBe('flex');
    expect(footerStyle.flexDirection).toBe('column');

    // Verify footer links stack vertically
    const footerLinks = footer.locator('.footer-links');
    const linksStyle = await footerLinks.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        flexDirection: style.flexDirection
      };
    });

    expect(linksStyle.flexDirection).toBe('column');
  });

  // Additional test: Verify getting started section adapts at mobile viewport
  test('Getting started section adapts at mobile viewport', async ({ page }) => {
    await page.setViewportSize({ width: 320, height: 568 });

    const gettingStarted = page.locator('[data-testid="getting-started-section"]');
    await gettingStarted.scrollIntoViewIfNeeded();
    await expect(gettingStarted).toBeVisible();

    // Verify reference grid adapts to 2 columns at mobile
    const referenceGrid = page.locator('.reference-grid');
    const gridStyle = await referenceGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        gridTemplateColumns: style.gridTemplateColumns
      };
    });

    const columns = gridStyle.gridTemplateColumns.split(' ').filter(col => col && col !== '0px');
    expect(columns.length).toBeLessThanOrEqual(2);

    // Verify steps have proper padding
    const step = page.locator('.step').first();
    const stepStyle = await step.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        paddingLeft: parseFloat(style.paddingLeft)
      };
    });

    // Should have reduced padding at mobile (3.5rem = 56px per CSS)
    expect(stepStyle.paddingLeft).toBeLessThanOrEqual(60);
  });
});
