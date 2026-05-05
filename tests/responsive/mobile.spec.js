const { test, expect } = require('@playwright/test');

// Mobile Responsiveness Tests

test.describe('Mobile Viewport (320px width)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto('http://localhost:8080');
  });

  test('content should be readable without horizontal scrolling', async ({ page }) => {
    const pageWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(pageWidth).toBeLessThanOrEqual(viewportWidth);

    // Check text is readable
    const heroText = await page.textContent('.hero h1');
    expect(heroText).toContain('MirDB');
  });

  test('hamburger menu is accessible', async ({ page }) => {
    const hamburger = await page.$('.hamburger');
    expect(hamburger).toBeTruthy();

    await page.click('.hamburger');
    const navMenu = await page.$('.nav-menu');
    await expect(navMenu).toBeVisible();
  });
});

test.describe('Tablet Viewport (768px width)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('http://localhost:8080');
  });

  test('layout maintains readability', async ({ page }) => {
    const h1FontSize = await page.evaluate(() => {
      const style = window.getComputedStyle(document.querySelector('h1'));
      return parseInt(style.fontSize);
    });
    expect(h1FontSize).toBe(28); // 1.8rem at base font-size

    const navItems = await page.$$eval('.nav-menu a', els => els.length);
    expect(navItems).toBeGreaterThan(0);
  });
});

test.describe('Navigation Accessibility', () => {
  test('navigation works on all screen sizes', async ({ page }) => {
    // Test on mobile
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto('http://localhost:8080');
    await page.click('.hamburger');
    await page.click('.nav-menu a[href="/#features"]');
    await expect(page.locator('#features')).toBeInViewport();

    // Test on desktop
    await page.setViewportSize({ width: 1024, height: 768 });
    await page.goto('http://localhost:8080');
    await page.click('.nav-menu a[href="/#quickstart"]');
    await expect(page.locator('#quickstart')).toBeInViewport();
  });
});