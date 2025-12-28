// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: Responsive Design - Tablet
 * Scenario: Verify the homepage renders correctly on tablet devices
 *
 * Tests verify:
 * 1. Page displays without horizontal scrollbar at 768px width
 * 2. Grid layouts adapt to intermediate width (2 columns or stacked)
 * 3. Navigation remains accessible and functional
 */

// Tablet viewport dimensions (iPad portrait)
const TABLET_VIEWPORT = { width: 768, height: 1024 };

test.describe('Responsive Design - Tablet', () => {
  test.beforeEach(async ({ page }) => {
    // Set tablet viewport before each test
    await page.setViewportSize(TABLET_VIEWPORT);
    // Navigate to the homepage
    await page.goto('/');
  });

  /**
   * Test Case 1: Page displays without horizontal scrollbar at 768px width
   * Input: Load page at 768px width (tablet)
   * Expected: Page displays without horizontal scrollbar
   */
  test('TC1: Page displays without horizontal scrollbar at 768px width', async ({ page }) => {
    // Verify the page is loaded
    await expect(page).toHaveURL(/\//);

    // Check that there's no horizontal scrollbar by comparing document width to viewport width
    const hasHorizontalScrollbar = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScrollbar).toBe(false);

    // Verify body element doesn't overflow horizontally
    const bodyOverflow = await page.evaluate(() => {
      const body = document.body;
      const bodyWidth = body.scrollWidth;
      const viewportWidth = window.innerWidth;
      return bodyWidth > viewportWidth;
    });
    expect(bodyOverflow).toBe(false);

    // Verify no elements cause horizontal overflow
    const overflowingElements = await page.evaluate(() => {
      const elements = document.querySelectorAll('*');
      const viewportWidth = window.innerWidth;
      const overflowing = [];

      elements.forEach((el) => {
        const rect = el.getBoundingClientRect();
        if (rect.right > viewportWidth + 1) { // Allow 1px tolerance
          overflowing.push({
            tag: el.tagName,
            class: el.className,
            right: rect.right,
            viewportWidth: viewportWidth
          });
        }
      });

      return overflowing;
    });

    // Log any overflowing elements for debugging
    if (overflowingElements.length > 0) {
      console.log('Overflowing elements:', overflowingElements);
    }

    expect(overflowingElements.length).toBe(0);
  });

  /**
   * Test Case 2: Grid layouts adapt to intermediate width (2 columns or stacked)
   * Input: Check layout adaptations on tablet
   * Expected: Grid layouts adapt to intermediate width (2 columns or stacked)
   */
  test('TC2: Grid layouts adapt to intermediate width', async ({ page }) => {
    // Check value propositions grid - should stack to single column at tablet width
    const valuePropsGrid = page.locator('[data-testid="value-props-grid"]');
    await expect(valuePropsGrid).toBeVisible();

    // Get the computed grid-template-columns for value props
    const valuePropsGridStyles = await valuePropsGrid.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        display: computed.display,
        gridTemplateColumns: computed.gridTemplateColumns
      };
    });

    // At 768px, the grid should be single column (per CSS media query)
    expect(valuePropsGridStyles.display).toBe('grid');
    const valuePropsColumnCount = valuePropsGridStyles.gridTemplateColumns.split(' ').length;
    // At tablet width (768px), should be 1 column due to media query
    expect(valuePropsColumnCount).toBe(1);

    // Check features grid - uses auto-fit with minmax
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    const featuresGridStyles = await featuresGrid.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        display: computed.display,
        gridTemplateColumns: computed.gridTemplateColumns
      };
    });

    expect(featuresGridStyles.display).toBe('grid');
    // Features grid uses auto-fit with minmax(250px, 1fr), so at 768px should be 2-3 columns
    const featuresColumnCount = featuresGridStyles.gridTemplateColumns.split(' ').length;
    // At 768px with minmax(250px, 1fr), expect 2-3 columns
    expect(featuresColumnCount).toBeGreaterThanOrEqual(2);
    expect(featuresColumnCount).toBeLessThanOrEqual(3);

    // Check footer grid
    const footerContent = page.locator('.footer-content');
    await expect(footerContent).toBeVisible();

    const footerGridStyles = await footerContent.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        display: computed.display,
        gridTemplateColumns: computed.gridTemplateColumns
      };
    });

    expect(footerGridStyles.display).toBe('grid');
    // Footer uses auto-fit with minmax(200px, 1fr)
    const footerColumnCount = footerGridStyles.gridTemplateColumns.split(' ').length;
    // At 768px with minmax(200px, 1fr), expect 2-3 columns
    expect(footerColumnCount).toBeGreaterThanOrEqual(2);
    expect(footerColumnCount).toBeLessThanOrEqual(3);
  });

  /**
   * Test Case 3: Navigation remains accessible and functional
   * Input: Check navigation on tablet
   * Expected: Navigation remains accessible and functional
   */
  test('TC3: Navigation remains accessible and functional', async ({ page }) => {
    // Verify the header is visible
    const header = page.locator('header');
    await expect(header).toBeVisible();

    // Verify the navigation container is visible
    const navContainer = page.locator('.nav-container');
    await expect(navContainer).toBeVisible();

    // Verify all navigation links are visible
    const navLinks = page.locator('.nav-links a');
    const navLinksCount = await navLinks.count();
    expect(navLinksCount).toBeGreaterThanOrEqual(3); // Features, Docs, GitHub

    // Verify each link is visible and clickable
    for (let i = 0; i < navLinksCount; i++) {
      const link = navLinks.nth(i);
      await expect(link).toBeVisible();
    }

    // Test Features link functionality
    const featuresLink = page.locator('header nav a[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await featuresLink.click();
    await page.waitForTimeout(500);

    // Verify URL hash changed
    await expect(page).toHaveURL(/#features$/);

    // Verify features section is in viewport
    const featuresSection = page.locator('#features');
    const isInViewport = await featuresSection.evaluate((el) => {
      const rect = el.getBoundingClientRect();
      return rect.top >= -100 && rect.top <= window.innerHeight;
    });
    expect(isInViewport).toBe(true);

    // Test Docs link functionality
    const docsLink = page.locator('header nav a:has-text("Docs")');
    await expect(docsLink).toBeVisible();
    await docsLink.click();
    await page.waitForTimeout(500);

    // Verify the docs/quick-start section is in viewport
    const href = await docsLink.getAttribute('href');
    const targetSection = page.locator(href);
    await expect(targetSection).toBeVisible();

    // Test GitHub link has correct attributes
    const githubLink = page.locator('header nav a:has-text("GitHub")');
    await expect(githubLink).toBeVisible();

    const githubHref = await githubLink.getAttribute('href');
    expect(githubHref).toContain('github.com');

    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');

    // Verify logo link is visible and functional
    const logoLink = page.locator('header .logo-link');
    await expect(logoLink).toBeVisible();

    const logoImg = page.locator('header .logo-link .nav-logo');
    await expect(logoImg).toBeVisible();
  });

  /**
   * Additional Test: Hero section displays correctly on tablet
   */
  test('Hero section adapts to tablet width', async ({ page }) => {
    const heroSection = page.locator('.hero-section');
    await expect(heroSection).toBeVisible();

    // Verify hero content is visible and properly sized
    const heroContent = page.locator('.hero-content');
    await expect(heroContent).toBeVisible();

    // Verify product name is visible
    const productName = page.locator('.product-name');
    await expect(productName).toBeVisible();

    // Check font size is adjusted for tablet (should be 2.5rem = 40px per CSS)
    const fontSize = await productName.evaluate((el) => {
      return window.getComputedStyle(el).fontSize;
    });
    expect(fontSize).toBe('40px'); // 2.5rem at default 16px root

    // Verify CTA buttons are visible and stacked vertically on tablet
    const ctaButtons = page.locator('.cta-buttons');
    await expect(ctaButtons).toBeVisible();

    // Check that CTA buttons are displayed in column layout
    const ctaFlexDirection = await ctaButtons.evaluate((el) => {
      return window.getComputedStyle(el).flexDirection;
    });
    expect(ctaFlexDirection).toBe('column');

    // Verify buttons are centered
    const ctaAlignItems = await ctaButtons.evaluate((el) => {
      return window.getComputedStyle(el).alignItems;
    });
    expect(ctaAlignItems).toBe('center');
  });

  /**
   * Additional Test: Configuration table adapts to tablet width
   */
  test('Configuration parameters table adapts to tablet width', async ({ page }) => {
    // Navigate to configuration section
    await page.goto('/#configuration');
    await page.waitForTimeout(500);

    const configTable = page.locator('.config-params-table');
    await expect(configTable).toBeVisible();

    // Check that config param rows are single column layout on tablet
    const configParamRow = page.locator('.config-param-row').first();
    await expect(configParamRow).toBeVisible();

    const gridTemplateColumns = await configParamRow.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });

    // At 768px, should be single column layout (1fr)
    const columnCount = gridTemplateColumns.split(' ').length;
    expect(columnCount).toBe(1);
  });

  /**
   * Additional Test: Quick start section displays correctly on tablet
   */
  test('Quick start section displays correctly on tablet', async ({ page }) => {
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Verify all code blocks are visible and don't overflow
    const codeBlocks = page.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Check each code block doesn't cause horizontal overflow
    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      await expect(codeBlock).toBeVisible();

      const overflows = await codeBlock.evaluate((el) => {
        const rect = el.getBoundingClientRect();
        const viewportWidth = window.innerWidth;
        return rect.right > viewportWidth;
      });
      expect(overflows).toBe(false);
    }
  });

  /**
   * Additional Test: Commands section displays correctly on tablet
   */
  test('Commands section displays correctly on tablet', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Verify command items are visible and properly sized
    const commandItems = page.locator('.command-item');
    const commandItemCount = await commandItems.count();
    expect(commandItemCount).toBeGreaterThan(0);

    // Check first command item doesn't overflow
    const firstCommandItem = commandItems.first();
    await expect(firstCommandItem).toBeVisible();

    const commandItemBox = await firstCommandItem.boundingBox();
    expect(commandItemBox.width).toBeLessThanOrEqual(768);
  });
});
