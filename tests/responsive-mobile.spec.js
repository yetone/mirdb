const { test, expect } = require('@playwright/test');

/**
 * Responsive Design - Mobile Tests
 * Scenario: Validate the homepage displays correctly on mobile devices (320px-767px viewport)
 */

test.describe('Responsive Design - Mobile', () => {
  // Configure all tests in this suite to use mobile viewport
  test.use({
    viewport: { width: 375, height: 667 }
  });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Page renders without horizontal scrollbar at 375px width viewport', async ({ page }) => {
    // Check that the page doesn't have horizontal overflow
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // Body scroll width should not exceed viewport width (no horizontal scroll)
    expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth);

    // Also verify documentElement (html) doesn't overflow
    const htmlScrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    expect(htmlScrollWidth).toBeLessThanOrEqual(viewportWidth);

    // Verify the overflow-x is not visible (no horizontal scrollbar)
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.body.scrollWidth > document.body.clientWidth ||
             document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBeFalsy();

    // Verify the page is fully visible and functional
    await expect(page.locator('header')).toBeVisible();
    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('footer')).toBeVisible();
  });

  test('Test Case 2: Three columns stack vertically on mobile viewport', async ({ page }) => {
    // Navigate to value proposition section
    const valuePropositionSection = page.locator('#value-proposition');
    await expect(valuePropositionSection).toBeVisible();

    // Get the value proposition columns
    const columns = page.locator('.value-prop-column');
    const columnCount = await columns.count();
    expect(columnCount).toBe(3);

    // Verify the grid switches to single column layout on mobile
    const gridStyle = await page.locator('.value-prop-grid').evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });

    // On mobile, grid-template-columns should be "1fr" (single column)
    // When columns stack, they should have the same x-position
    const firstColumnBox = await columns.nth(0).boundingBox();
    const secondColumnBox = await columns.nth(1).boundingBox();
    const thirdColumnBox = await columns.nth(2).boundingBox();

    // All columns should have the same x position (stacked vertically)
    expect(firstColumnBox.x).toBe(secondColumnBox.x);
    expect(secondColumnBox.x).toBe(thirdColumnBox.x);

    // Second column should be below the first column
    expect(secondColumnBox.y).toBeGreaterThan(firstColumnBox.y);

    // Third column should be below the second column
    expect(thirdColumnBox.y).toBeGreaterThan(secondColumnBox.y);

    // All columns should be visible
    for (let i = 0; i < 3; i++) {
      await expect(columns.nth(i)).toBeVisible();
    }
  });

  test('Test Case 3: Navigation is accessible via mobile-friendly menu (hamburger or collapsed)', async ({ page }) => {
    // Check that navigation exists and is accessible on mobile
    const header = page.locator('header');
    await expect(header).toBeVisible();

    const nav = header.locator('nav');
    await expect(nav).toBeVisible();

    // Navigation links should be accessible (either through hamburger menu or wrapped nav)
    const navLinks = nav.locator('a');
    const linkCount = await navLinks.count();

    // Should have at least 3 navigation links
    expect(linkCount).toBeGreaterThanOrEqual(3);

    // Check that navigation links are either:
    // 1. Visible and clickable (wrapped navigation)
    // 2. Accessible via hamburger menu

    // First check if there's a hamburger menu button
    const hamburgerButton = header.locator('[class*="hamburger"], [class*="mobile-menu"], button[aria-label*="menu"]');
    const hasHamburger = await hamburgerButton.count() > 0;

    if (hasHamburger) {
      // If hamburger menu exists, click it and verify links become visible
      await hamburgerButton.click();
      for (let i = 0; i < linkCount; i++) {
        await expect(navLinks.nth(i)).toBeVisible();
      }
    } else {
      // Navigation wraps on mobile - verify links are accessible
      // The current implementation uses flex-wrap, so links should still be visible
      const navList = nav.locator('ul');
      await expect(navList).toBeVisible();

      // Verify nav list uses flex-wrap for mobile
      const flexWrap = await navList.evaluate((el) => {
        return window.getComputedStyle(el).flexWrap;
      });
      expect(flexWrap).toBe('wrap');

      // All links should be visible
      for (let i = 0; i < linkCount; i++) {
        await expect(navLinks.nth(i)).toBeVisible();
      }
    }

    // Verify the Features link is functional
    const featuresLink = nav.locator('a[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await featuresLink.click();
    await expect(page.locator('#features')).toBeVisible();
  });

  test('Test Case 4: Logo and demo GIF scale appropriately for mobile viewport', async ({ page }) => {
    const viewportWidth = 375;

    // Check the hero logo
    const heroLogo = page.locator('.hero-logo');
    await expect(heroLogo).toBeVisible();

    // Get logo dimensions
    const logoBox = await heroLogo.boundingBox();

    // Logo should not exceed viewport width (with some margin for padding)
    expect(logoBox.width).toBeLessThanOrEqual(viewportWidth - 40);

    // Logo max-width should be set appropriately for mobile
    const logoMaxWidth = await heroLogo.evaluate((el) => {
      return window.getComputedStyle(el).maxWidth;
    });
    // On mobile, logo should have a max-width of 200px (as defined in CSS)
    expect(logoMaxWidth).toBe('200px');

    // Check the demo GIF
    const demoGif = page.locator('.demo-gif');
    await expect(demoGif).toBeVisible();

    // Get demo GIF dimensions
    const demoBox = await demoGif.boundingBox();

    // Demo GIF should not exceed viewport width (with some margin for container padding)
    expect(demoBox.width).toBeLessThanOrEqual(viewportWidth - 40);

    // Verify the demo GIF has max-width: 100% to be responsive
    const demoMaxWidth = await demoGif.evaluate((el) => {
      return window.getComputedStyle(el).maxWidth;
    });
    expect(demoMaxWidth).toBe('100%');

    // Verify images maintain aspect ratio (height should be proportional)
    // This is ensured by height: auto in CSS
    const logoHeight = await heroLogo.evaluate((el) => {
      return window.getComputedStyle(el).height;
    });
    expect(logoHeight).not.toBe('0px');

    const demoHeight = await demoGif.evaluate((el) => {
      return window.getComputedStyle(el).height;
    });
    expect(demoHeight).not.toBe('0px');
  });

  // Additional test for text readability
  test('Text is readable without zooming on mobile', async ({ page }) => {
    // Check that the main headline font size is appropriate for mobile
    const headline = page.locator('.hero-headline');
    await expect(headline).toBeVisible();

    const headlineFontSize = await headline.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });

    // Mobile headline should be at least 1.75rem (28px at default 16px base)
    // At mobile viewport, it should be >= 24px (1.5rem at minimum)
    expect(headlineFontSize).toBeGreaterThanOrEqual(24);

    // Check subheadline
    const subheadline = page.locator('.hero-subheadline');
    await expect(subheadline).toBeVisible();

    const subheadlineFontSize = await subheadline.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });

    // Subheadline should be at least 1rem (16px)
    expect(subheadlineFontSize).toBeGreaterThanOrEqual(16);

    // Check body text in value proposition
    const valuePropText = page.locator('.value-prop-column p').first();
    await expect(valuePropText).toBeVisible();

    const bodyFontSize = await valuePropText.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });

    // Body text should be at least 14px for readability
    expect(bodyFontSize).toBeGreaterThanOrEqual(14);
  });
});
