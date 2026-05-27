/**
 * Responsive Design E2E Tests
 * Scenario 10 - Responsive Design
 *
 * Validates the homepage renders correctly across all required viewports
 * from 320px mobile to 2560px desktop.
 */

const { test, expect } = require('@playwright/test');

const SITE_URL = 'http://localhost:8080/index.html';

const VIEWPORTS = {
  mobile320: { width: 320, height: 568 },
  mobile375: { width: 375, height: 667 },
  tablet768: { width: 768, height: 1024 },
  desktop1024: { width: 1024, height: 768 },
  desktop1920: { width: 1920, height: 1080 },
  ultrawide2560: { width: 2560, height: 1440 },
};

test.describe('Responsive Design', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(SITE_URL);
    await page.waitForLoadState('networkidle');
  });

  // ── TC1: Mobile 320px ──
  test('mobile layout at 320px - single column, hamburger, no overflow', async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.mobile320);
    await page.waitForTimeout(300);

    // Hamburger menu visible
    const hamburger = page.locator('.mobile-menu-toggle');
    await expect(hamburger).toBeVisible();

    // Nav links hidden (collapsed)
    const navLinks = page.locator('.nav-links');
    const isHidden = await navLinks.evaluate((el) => getComputedStyle(el).display === 'none');
    expect(isHidden).toBe(true);

    // Features grid is single column
    const featuresGrid = page.locator('.features-grid');
    const gridStyle = await featuresGrid.evaluate((el) => getComputedStyle(el).gridTemplateColumns);
    const columns = gridStyle.split(' ').filter((c) => c !== '');
    expect(columns.length).toBe(1);

    // No horizontal scroll
    const hasOverflow = await page.evaluate(() =>
      document.documentElement.scrollWidth > document.documentElement.clientWidth
    );
    expect(hasOverflow).toBe(false);

    // Tap targets >= 44x44px for interactive elements
    const interactiveElements = await page.locator('button, a.btn, .mobile-menu-toggle').all();
    for (const el of interactiveElements) {
      const box = await el.boundingBox();
      if (box && box.width > 0 && box.height > 0) {
        expect(box.width).toBeGreaterThanOrEqual(44);
        expect(box.height).toBeGreaterThanOrEqual(44);
      }
    }

    // CTA button is full-width on mobile
    const ctaBtn = page.locator('.hero-actions .btn-primary');
    const heroActions = page.locator('.hero-actions');
    const ctaBox = await ctaBtn.boundingBox();
    const heroBox = await heroActions.boundingBox();
    if (ctaBox && heroBox) {
      expect(ctaBox.width).toBeGreaterThanOrEqual(heroBox.width * 0.85);
    }
  });

  // ── TC2: Mobile 375px ──
  test('mobile layout at 375px - readable font, hero fits, prominent CTA', async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.mobile375);
    await page.waitForTimeout(300);

    // Base font size is 16px
    const bodyFontSize = await page.evaluate(() =>
      parseFloat(getComputedStyle(document.body).fontSize)
    );
    expect(bodyFontSize).toBe(16);

    // Hero section fits within viewport height
    const hero = page.locator('.hero');
    const heroBox = await hero.boundingBox();
    expect(heroBox.height).toBeLessThanOrEqual(667 * 1.1);

    // CTA button is prominent
    const ctaBtn = page.locator('.hero-actions .btn-primary');
    await expect(ctaBtn).toBeVisible();
    const ctaBox = await ctaBtn.boundingBox();
    expect(ctaBox.width).toBeGreaterThanOrEqual(200);
    expect(ctaBox.height).toBeGreaterThanOrEqual(44);
  });

  // ── TC3: Tablet 768px ──
  test('tablet layout at 768px - 2-col features, text nav, multi-col footer', async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.tablet768);
    await page.waitForTimeout(300);

    // Hamburger hidden
    const hamburger = page.locator('.mobile-menu-toggle');
    await expect(hamburger).not.toBeVisible();

    // Nav links visible
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Features grid is 2 columns
    const featuresGrid = page.locator('.features-grid');
    const gridStyle = await featuresGrid.evaluate((el) => getComputedStyle(el).gridTemplateColumns);
    const columns = gridStyle.split(' ').filter((c) => c !== '');
    expect(columns.length).toBe(2);

    // Footer has 3 columns
    const footerGrid = page.locator('.footer-grid');
    const footerStyle = await footerGrid.evaluate((el) => getComputedStyle(el).gridTemplateColumns);
    const footerColumns = footerStyle.split(' ').filter((c) => c !== '');
    expect(footerColumns.length).toBe(3);
  });

  // ── TC4: Desktop 1024px ──
  test('desktop layout at 1024px - 4-col features, full nav, architecture side-by-side', async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.desktop1024);
    await page.waitForTimeout(300);

    // Hamburger hidden
    const hamburger = page.locator('.mobile-menu-toggle');
    await expect(hamburger).not.toBeVisible();

    // Nav links visible
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Features grid is 4 columns
    const featuresGrid = page.locator('.features-grid');
    const gridStyle = await featuresGrid.evaluate((el) => getComputedStyle(el).gridTemplateColumns);
    const columns = gridStyle.split(' ').filter((c) => c !== '');
    expect(columns.length).toBe(4);

    // Docs grid is 3 columns
    const docsGrid = page.locator('.docs-grid');
    const docsStyle = await docsGrid.evaluate((el) => getComputedStyle(el).gridTemplateColumns);
    const docsColumns = docsStyle.split(' ').filter((c) => c !== '');
    expect(docsColumns.length).toBe(3);
  });

  // ── TC5: Desktop 1920px ──
  test('desktop layout at 1920px - centered content, max-width, whitespace', async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.desktop1920);
    await page.waitForTimeout(300);

    // Container is centered
    const container = page.locator('.container').first();
    const containerBox = await container.boundingBox();
    const leftMargin = containerBox.x;
    const rightMargin = 1920 - containerBox.x - containerBox.width;
    expect(Math.abs(leftMargin - rightMargin)).toBeLessThanOrEqual(50);

    // Container max-width should be around 1200px
    expect(containerBox.width).toBeLessThanOrEqual(1250);

    // Features grid is 4 columns
    const featuresGrid = page.locator('.features-grid');
    const gridStyle = await featuresGrid.evaluate((el) => getComputedStyle(el).gridTemplateColumns);
    const columns = gridStyle.split(' ').filter((c) => c !== '');
    expect(columns.length).toBe(4);
  });

  // ── TC6: Ultrawide 2560px ──
  test('ultrawide layout at 2560px - constrained width, centered, readable lines', async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.ultrawide2560);
    await page.waitForTimeout(300);

    // Container is centered
    const container = page.locator('.container').first();
    const containerBox = await container.boundingBox();
    const leftMargin = containerBox.x;
    const rightMargin = 2560 - containerBox.x - containerBox.width;
    expect(Math.abs(leftMargin - rightMargin)).toBeLessThanOrEqual(50);

    // Container constrained to max-width
    expect(containerBox.width).toBeLessThanOrEqual(1250);

    // Features grid is 4 columns
    const featuresGrid = page.locator('.features-grid');
    const gridStyle = await featuresGrid.evaluate((el) => getComputedStyle(el).gridTemplateColumns);
    const columns = gridStyle.split(' ').filter((c) => c !== '');
    expect(columns.length).toBe(4);

    // No horizontal overflow
    const hasOverflow = await page.evaluate(() =>
      document.documentElement.scrollWidth > document.documentElement.clientWidth
    );
    expect(hasOverflow).toBe(false);
  });

  // ── TC7: Orientation changes ──
  test('orientation changes - layout adapts, no overflow', async ({ page }) => {
    // Portrait
    await page.setViewportSize({ width: 375, height: 667 });
    await page.waitForTimeout(300);
    let hasOverflow = await page.evaluate(() =>
      document.documentElement.scrollWidth > document.documentElement.clientWidth
    );
    expect(hasOverflow).toBe(false);

    // Landscape
    await page.setViewportSize({ width: 667, height: 375 });
    await page.waitForTimeout(300);
    hasOverflow = await page.evaluate(() =>
      document.documentElement.scrollWidth > document.documentElement.clientWidth
    );
    expect(hasOverflow).toBe(false);

    // Tablet landscape
    await page.setViewportSize({ width: 1024, height: 768 });
    await page.waitForTimeout(300);
    hasOverflow = await page.evaluate(() =>
      document.documentElement.scrollWidth > document.documentElement.clientWidth
    );
    expect(hasOverflow).toBe(false);

    // Tablet portrait
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.waitForTimeout(300);
    hasOverflow = await page.evaluate(() =>
      document.documentElement.scrollWidth > document.documentElement.clientWidth
    );
    expect(hasOverflow).toBe(false);
  });

  // ── TC8: Breakpoint transitions ──
  test('breakpoint transitions - grids change at correct widths', async ({ page }) => {
    const testWidth = async (width, expectedFeatureCols) => {
      await page.setViewportSize({ width, height: 800 });
      await page.waitForTimeout(200);

      const featuresGrid = page.locator('.features-grid');
      const gridStyle = await featuresGrid.evaluate((el) => getComputedStyle(el).gridTemplateColumns);
      const columns = gridStyle.split(' ').filter((c) => c !== '');
      expect(columns.length).toBe(expectedFeatureCols);
    };

    // Mobile: 1 column
    await testWidth(320, 1);
    await testWidth(500, 1);
    await testWidth(767, 1);

    // Tablet: 2 columns
    await testWidth(768, 2);
    await testWidth(900, 2);
    await testWidth(1023, 2);

    // Desktop: 4 columns
    await testWidth(1024, 4);
    await testWidth(1440, 4);
    await testWidth(1920, 4);
    await testWidth(2559, 4);
  });
});
