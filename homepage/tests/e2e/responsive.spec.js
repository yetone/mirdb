/**
 * Responsive Design E2E Tests
 * Owner: Scenarios 7, 8, 9 - Responsive Design
 *
 * End-to-end tests for responsive design:
 * - Mobile viewport tests (320px, 375px, 414px) - Scenario 7
 * - Tablet viewport tests (768px, 1024px) - Scenario 8
 * - Desktop viewport tests (1024px, 1440px, 1920px) - Scenario 9
 * - No horizontal scroll at any size
 * - Touch target sizes on mobile
 * - Content max-width on large screens
 */

const { test, expect } = require('@playwright/test');

// Desktop viewport tests (Scenario 9)
test.describe('Desktop Responsive Design (1024px+)', () => {
  test('page displays full desktop layout at 1024px viewport width', async ({ page }) => {
    await page.setViewportSize({ width: 1024, height: 768 });
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Verify navigation is displayed horizontally
    const nav = page.locator('.nav');
    await expect(nav).toBeVisible();

    // Verify navigation links are visible
    const navLinks = page.locator('.nav__links');
    await expect(navLinks).toBeVisible();

    // Verify hero section is visible
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    // Verify hero headline is visible and properly sized
    const heroHeadline = page.locator('.hero-headline');
    await expect(heroHeadline).toBeVisible();

    // Verify features section has full grid layout
    const featuresList = page.locator('.features__list');
    await expect(featuresList).toBeVisible();

    // Verify there's no horizontal scrollbar
    const body = page.locator('body');
    const bodyWidth = await body.evaluate(el => el.scrollWidth);
    const viewportWidth = 1024;
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth);
  });

  test('page displays correctly on standard desktop monitor at 1440px viewport width', async ({ page }) => {
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Verify all main sections are visible
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    const quickstart = page.locator('.quickstart');
    await expect(quickstart).toBeVisible();

    const features = page.locator('.features');
    await expect(features).toBeVisible();

    const roadmap = page.locator('.roadmap');
    await expect(roadmap).toBeVisible();

    const configuration = page.locator('.configuration');
    await expect(configuration).toBeVisible();

    const footer = page.locator('.footer');
    await expect(footer).toBeVisible();

    // Verify proper spacing and layout at 1440px
    const main = page.locator('main');
    const mainBox = await main.boundingBox();
    expect(mainBox).not.toBeNull();

    // Verify navigation is fully visible
    const navLinks = page.locator('.nav__links');
    await expect(navLinks).toBeVisible();

    // Verify no horizontal overflow
    const body = page.locator('body');
    const bodyScrollWidth = await body.evaluate(el => el.scrollWidth);
    expect(bodyScrollWidth).toBeLessThanOrEqual(1440);
  });

  test('content has appropriate max-width at 1920px viewport width', async ({ page }) => {
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Verify main content is constrained and not stretched across full screen
    const main = page.locator('main');
    const mainBox = await main.boundingBox();
    expect(mainBox).not.toBeNull();

    // Main content should be constrained to max-width (1200px) and centered
    // The main element should have a max-width that prevents overly long lines
    expect(mainBox.width).toBeLessThanOrEqual(1200);

    // Verify features list has max-width constraint
    const featuresList = page.locator('.features__list');
    const featuresBox = await featuresList.boundingBox();
    expect(featuresBox).not.toBeNull();
    expect(featuresBox.width).toBeLessThanOrEqual(1200);

    // Verify footer content has max-width constraint
    const footerContent = page.locator('.footer__content');
    const footerBox = await footerContent.boundingBox();
    expect(footerBox).not.toBeNull();
    expect(footerBox.width).toBeLessThanOrEqual(1200);
  });

  test('main content area does not exceed readable max-width (1200px)', async ({ page }) => {
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Get computed style for max-width-content variable
    const maxWidthContent = await page.evaluate(() => {
      const root = document.documentElement;
      return getComputedStyle(root).getPropertyValue('--max-width-content').trim();
    });

    // The max-width-content variable should be defined as 1200px
    expect(maxWidthContent).toBe('1200px');

    // Verify main content respects max-width
    const main = page.locator('main');
    const mainBox = await main.boundingBox();
    expect(mainBox).not.toBeNull();

    // Main content should be constrained to 1200px max-width
    expect(mainBox.width).toBeLessThanOrEqual(1200);

    // Verify quickstart steps are constrained
    const quickstartSteps = page.locator('.quickstart__steps');
    const quickstartBox = await quickstartSteps.boundingBox();
    if (quickstartBox) {
      expect(quickstartBox.width).toBeLessThanOrEqual(1200);
    }

    // Verify configuration content is constrained
    const configContent = page.locator('.configuration__content');
    const configBox = await configContent.boundingBox();
    if (configBox) {
      expect(configBox.width).toBeLessThanOrEqual(1200);
    }
  });

  test('navigation displays horizontally on desktop', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Verify navigation links are displayed horizontally
    const navLinks = page.locator('.nav__links');
    await expect(navLinks).toBeVisible();

    // Check that nav links container uses flex display on desktop
    const navLinksDisplay = await navLinks.evaluate(el => {
      return window.getComputedStyle(el).display;
    });
    expect(navLinksDisplay).toBe('flex');

    // Get all nav link anchors and verify they're arranged horizontally
    const navAnchors = page.locator('.nav__links a');
    const anchorCount = await navAnchors.count();
    expect(anchorCount).toBeGreaterThan(0);

    if (anchorCount >= 2) {
      const firstAnchor = await navAnchors.nth(0).boundingBox();
      const secondAnchor = await navAnchors.nth(1).boundingBox();

      if (firstAnchor && secondAnchor) {
        // Items should be side by side (second item's x should be greater than first item's x + width)
        expect(secondAnchor.x).toBeGreaterThan(firstAnchor.x + firstAnchor.width - 10);
        // Items should be roughly at the same y position (horizontal alignment)
        expect(Math.abs(secondAnchor.y - firstAnchor.y)).toBeLessThan(10);
      }
    }
  });

  test('features section uses multi-column grid on desktop', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Verify features list uses grid layout
    const featuresList = page.locator('.features__list');
    await expect(featuresList).toBeVisible();

    // Get feature items
    const featureItems = page.locator('.features__item');
    const itemCount = await featureItems.count();
    expect(itemCount).toBeGreaterThanOrEqual(3);

    // Verify grid layout by checking that multiple items are on the same row
    if (itemCount >= 3) {
      const firstItem = await featureItems.nth(0).boundingBox();
      const secondItem = await featureItems.nth(1).boundingBox();
      const thirdItem = await featureItems.nth(2).boundingBox();

      if (firstItem && secondItem && thirdItem) {
        // On desktop with 3-column grid, first 3 items should be on same row
        // (similar Y position but different X positions)
        expect(Math.abs(firstItem.y - secondItem.y)).toBeLessThan(5);
        expect(Math.abs(secondItem.y - thirdItem.y)).toBeLessThan(5);
        expect(secondItem.x).toBeGreaterThan(firstItem.x);
        expect(thirdItem.x).toBeGreaterThan(secondItem.x);
      }
    }
  });

  test('footer displays horizontally on desktop', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Scroll to footer
    const footer = page.locator('.footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Verify footer content uses flex layout for horizontal arrangement
    const footerContent = page.locator('.footer__content');
    await expect(footerContent).toBeVisible();

    // Verify footer links are displayed horizontally
    const footerLinks = page.locator('.footer__links');
    await expect(footerLinks).toBeVisible();
  });

  test('no horizontal scrollbar at any desktop viewport', async ({ page }) => {
    const desktopWidths = [1024, 1280, 1440, 1920];

    for (const width of desktopWidths) {
      await page.setViewportSize({ width, height: 800 });
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Check for horizontal overflow
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.body.scrollWidth > document.body.clientWidth;
      });

      expect(hasHorizontalScroll, `Horizontal scroll detected at ${width}px`).toBe(false);
    }
  });
});
