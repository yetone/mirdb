// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Scenario 8: Responsive Design
 * Tests for verifying the homepage displays correctly across
 * mobile, tablet, and desktop viewports.
 */

test.describe('Responsive Design', () => {
  /**
   * Test Case 1: Mobile viewport (320px) - content readable, no horizontal scroll
   * Input: Set viewport to 320px width (mobile)
   * Expected: Content is readable, no horizontal scrolling required
   */
  test('TC1: Mobile viewport (320px) - content readable, no horizontal scroll', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto('/');

    // Verify page loads
    await expect(page.locator('body')).toBeVisible();

    // Check that there's no horizontal overflow
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const windowWidth = await page.evaluate(() => window.innerWidth);
    expect(bodyScrollWidth).toBeLessThanOrEqual(windowWidth + 5); // Allow small tolerance

    // Verify main content sections are visible
    await expect(page.locator('header')).toBeVisible();
    await expect(page.locator('#hero')).toBeVisible();

    // Verify text content is readable (elements are properly contained)
    const heroHeading = page.locator('#hero h1');
    await expect(heroHeading).toBeVisible();

    // Check that the hero heading doesn't overflow its container
    const heroBox = await heroHeading.boundingBox();
    expect(heroBox).not.toBeNull();
    if (heroBox) {
      expect(heroBox.width).toBeLessThanOrEqual(320);
    }
  });

  /**
   * Test Case 2: Tablet viewport (768px) - layout adapts appropriately
   * Input: Set viewport to 768px width (tablet)
   * Expected: Layout adapts appropriately for tablet size
   */
  test('TC2: Tablet viewport (768px) - layout adapts', async ({ page }) => {
    // Set tablet viewport
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/');

    // Verify page loads
    await expect(page.locator('body')).toBeVisible();

    // At 768px (md breakpoint), desktop nav should be visible
    const desktopNav = page.locator('#desktop-nav');
    await expect(desktopNav).toBeVisible();

    // Mobile menu button should be hidden at tablet size
    const mobileMenuButton = page.locator('#mobile-menu-button');
    await expect(mobileMenuButton).toBeHidden();

    // Feature cards should display in 2-column grid at tablet size
    const featureGrid = page.locator('[data-testid="feature-cards-grid"]');
    await expect(featureGrid).toBeVisible();

    // Get computed style of the grid
    const gridStyle = await featureGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns
      };
    });

    // At md breakpoint, should have 2 columns
    expect(gridStyle.display).toBe('grid');
    // Grid should have multiple columns (not single column)
    const columnCount = gridStyle.gridTemplateColumns.split(' ').filter(col => col !== '').length;
    expect(columnCount).toBeGreaterThanOrEqual(2);

    // Verify no horizontal scroll
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const windowWidth = await page.evaluate(() => window.innerWidth);
    expect(bodyScrollWidth).toBeLessThanOrEqual(windowWidth + 5);
  });

  /**
   * Test Case 3: Desktop viewport (1920px) - full desktop layout
   * Input: Set viewport to 1920px width (desktop)
   * Expected: Full desktop layout displays with proper spacing
   */
  test('TC3: Desktop viewport (1920px) - full desktop layout', async ({ page }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.goto('/');

    // Verify page loads
    await expect(page.locator('body')).toBeVisible();

    // Desktop navigation should be fully visible
    const desktopNav = page.locator('#desktop-nav');
    await expect(desktopNav).toBeVisible();

    // All navigation links should be visible
    await expect(page.locator('nav a[href="#features"]').first()).toBeVisible();
    await expect(page.locator('nav a[href="#quickstart"]').first()).toBeVisible();
    await expect(page.locator('nav a[href="#configuration"]').first()).toBeVisible();
    await expect(page.locator('nav a[href="https://github.com/yetone/mirdb"]').first()).toBeVisible();

    // Mobile menu button should be hidden on desktop
    const mobileMenuButton = page.locator('#mobile-menu-button');
    await expect(mobileMenuButton).toBeHidden();

    // Feature cards should display in 4-column grid at desktop
    const featureGrid = page.locator('[data-testid="feature-cards-grid"]');
    await expect(featureGrid).toBeVisible();

    // Get computed style of the grid
    const gridStyle = await featureGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns
      };
    });

    // At lg breakpoint and above, should have 4 columns
    expect(gridStyle.display).toBe('grid');
    const columnCount = gridStyle.gridTemplateColumns.split(' ').filter(col => col !== '').length;
    expect(columnCount).toBe(4);

    // Verify proper spacing (content is centered with max-width)
    const mainContent = page.locator('.max-w-7xl').first();
    await expect(mainContent).toBeVisible();

    // Verify no horizontal scroll
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const windowWidth = await page.evaluate(() => window.innerWidth);
    expect(bodyScrollWidth).toBeLessThanOrEqual(windowWidth + 5);
  });

  /**
   * Test Case 4: Hamburger menu appears on mobile viewport
   * Input: Check hamburger menu on mobile
   * Expected: Hamburger menu icon appears on mobile viewport
   */
  test('TC4: Hamburger menu icon appears on mobile', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto('/');

    // Mobile menu button should be visible on mobile
    const mobileMenuButton = page.locator('#mobile-menu-button');
    await expect(mobileMenuButton).toBeVisible();

    // Verify the hamburger icon (SVG with menu lines) is visible
    const hamburgerIcon = mobileMenuButton.locator('svg');
    await expect(hamburgerIcon).toBeVisible();

    // Desktop nav should be hidden on mobile
    const desktopNav = page.locator('#desktop-nav');
    await expect(desktopNav).toBeHidden();
  });

  /**
   * Test Case 5: Navigation menu opens/closes on hamburger click
   * Input: Click hamburger menu
   * Expected: Navigation menu opens/closes on click
   */
  test('TC5: Hamburger menu opens and closes on click', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto('/');

    const mobileMenuButton = page.locator('#mobile-menu-button');
    const mobileMenu = page.locator('#mobile-menu');

    // Initially, mobile menu content should be hidden
    // The menu has "hidden md:hidden" class initially, so its links are not visible
    const featuresLink = mobileMenu.locator('a[href="#features"]');
    await expect(featuresLink).toBeHidden();

    // Click to open the menu
    await mobileMenuButton.click();

    // Menu links should now be visible after clicking
    await expect(featuresLink).toBeVisible();
    await expect(mobileMenu.locator('a[href="#quickstart"]')).toBeVisible();
    await expect(mobileMenu.locator('a[href="#configuration"]')).toBeVisible();

    // Click again to close the menu
    await mobileMenuButton.click();

    // Menu links should be hidden again
    await expect(featuresLink).toBeHidden();
  });

  /**
   * Test Case 6: Feature cards stack vertically on mobile
   * Input: Check feature cards on mobile
   * Expected: Feature cards stack vertically on mobile
   */
  test('TC6: Feature cards stack vertically on mobile', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto('/');

    // Navigate to features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    // Get all feature cards
    const featureCards = page.locator('[data-testid="feature-card"]');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(1);

    // Check that feature grid is single column on mobile
    const featureGrid = page.locator('[data-testid="feature-cards-grid"]');
    await expect(featureGrid).toBeVisible();

    const gridStyle = await featureGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns
      };
    });

    // On mobile (320px), should have single column
    expect(gridStyle.display).toBe('grid');

    // Single column means only one column width definition
    const columnCount = gridStyle.gridTemplateColumns.split(' ').filter(col => col !== '').length;
    expect(columnCount).toBe(1);

    // Verify cards are stacked (each card takes full width)
    if (cardCount >= 2) {
      const firstCard = featureCards.nth(0);
      const secondCard = featureCards.nth(1);

      const firstBox = await firstCard.boundingBox();
      const secondBox = await secondCard.boundingBox();

      // Cards should be stacked vertically (second card below first)
      if (firstBox && secondBox) {
        expect(secondBox.y).toBeGreaterThan(firstBox.y);
        // Cards should have similar x positions (aligned vertically)
        expect(Math.abs(firstBox.x - secondBox.x)).toBeLessThan(10);
      }
    }
  });

  /**
   * Additional test: Verify content sections are visible at all viewports
   */
  test('All main sections visible across viewports', async ({ page }) => {
    const viewports = [
      { width: 320, height: 568, name: 'mobile' },
      { width: 768, height: 1024, name: 'tablet' },
      { width: 1920, height: 1080, name: 'desktop' }
    ];

    for (const viewport of viewports) {
      await page.setViewportSize({ width: viewport.width, height: viewport.height });
      await page.goto('/');

      // Verify all main sections exist and are in the DOM
      await expect(page.locator('header')).toBeAttached();
      await expect(page.locator('#hero')).toBeAttached();
      await expect(page.locator('#features')).toBeAttached();
      await expect(page.locator('#quickstart')).toBeAttached();
      await expect(page.locator('#configuration')).toBeAttached();
      await expect(page.locator('#status')).toBeAttached();
      await expect(page.locator('footer')).toBeAttached();
    }
  });

  /**
   * Additional test: Verify responsive images don't overflow
   */
  test('Images are responsive and contained', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto('/');

    // Get all images
    const images = page.locator('img');
    const imageCount = await images.count();

    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i);
      if (await img.isVisible()) {
        const box = await img.boundingBox();
        if (box) {
          // Image should not exceed viewport width
          expect(box.width).toBeLessThanOrEqual(320 + 5);
        }
      }
    }
  });
});
