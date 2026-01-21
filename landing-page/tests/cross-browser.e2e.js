/**
 * Cross-Browser Compatibility E2E Tests
 * Testing NFR-4: Cross-browser compatibility (Chrome, Firefox, Safari, Edge)
 *
 * These tests verify that the landing page displays correctly and all features
 * function properly across all major browsers.
 */

import { test, expect } from '@playwright/test';

test.describe('Cross-Browser Compatibility Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  /**
   * Test Case 1-4: Page displays correctly with all features functional
   * This test runs across all configured browsers (Chrome, Firefox, Safari, Edge)
   */
  test('page should load and display all major sections', async ({ page, browserName }) => {
    // Verify page title loads
    const title = await page.title();
    expect(title.length).toBeGreaterThan(0);

    // Verify header is visible
    const header = page.locator('header');
    await expect(header).toBeVisible();

    // Verify hero section is visible
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    // Verify features section is visible
    const features = page.locator('.features');
    await expect(features).toBeVisible();

    // Verify footer is visible
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();
  });

  test('navigation should work correctly', async ({ page, browserName }) => {
    // Test header navigation
    const navLinks = page.locator('.nav-links a');
    const linkCount = await navLinks.count();
    expect(linkCount).toBeGreaterThanOrEqual(1);

    // Test CTA button is clickable
    const navCta = page.locator('.nav-cta');
    await expect(navCta).toBeVisible();
    await expect(navCta).toBeEnabled();
  });

  test('hero section should display CTA buttons correctly', async ({ page, browserName }) => {
    const primaryCta = page.locator('.cta-primary');
    await expect(primaryCta).toBeVisible();
    await expect(primaryCta).toBeEnabled();

    // Check button has proper styling
    const backgroundColor = await primaryCta.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    expect(backgroundColor).not.toBe('rgba(0, 0, 0, 0)');
    expect(backgroundColor).not.toBe('transparent');
  });

  test('feature cards should be visible and styled', async ({ page, browserName }) => {
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(3);

    // Each card should be visible
    for (let i = 0; i < cardCount; i++) {
      await expect(featureCards.nth(i)).toBeVisible();
    }
  });

  /**
   * Test Case 5: Verify CSS Grid/Flexbox support
   * Layout renders correctly in all browsers
   */
  test('CSS Grid layout should render correctly in features section', async ({ page, browserName }) => {
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Verify grid display is applied
    const display = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(display).toBe('grid');

    // Verify grid-template-columns is set
    const gridTemplateColumns = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });
    expect(gridTemplateColumns).not.toBe('none');
  });

  test('CSS Flexbox layout should render correctly in navigation', async ({ page, browserName }) => {
    const nav = page.locator('.nav');
    await expect(nav).toBeVisible();

    // Verify flex display is applied
    const display = await nav.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(display).toBe('flex');

    // Verify align-items is working
    const alignItems = await nav.evaluate((el) => {
      return window.getComputedStyle(el).alignItems;
    });
    expect(alignItems).toBe('center');
  });

  test('CSS Flexbox layout should render correctly in hero CTA container', async ({ page, browserName }) => {
    const ctaContainer = page.locator('.hero-cta-container');
    await expect(ctaContainer).toBeVisible();

    // Verify flex display is applied
    const display = await ctaContainer.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(display).toBe('flex');
  });

  test('CSS Grid layout should render correctly in testimonials', async ({ page, browserName }) => {
    const testimonialsGrid = page.locator('.testimonials-grid');
    await expect(testimonialsGrid).toBeVisible();

    // Verify grid display is applied
    const display = await testimonialsGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(display).toBe('grid');
  });

  test('CSS Grid layout should render correctly in footer', async ({ page, browserName }) => {
    const footerContent = page.locator('.footer-content');
    await expect(footerContent).toBeVisible();

    // Verify grid display is applied
    const display = await footerContent.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(display).toBe('grid');
  });

  test('fixed header positioning should work correctly', async ({ page, browserName }) => {
    const header = page.locator('header');

    // Verify fixed position
    const position = await header.evaluate((el) => {
      return window.getComputedStyle(el).position;
    });
    expect(position).toBe('fixed');

    // Scroll and verify header stays visible
    await page.evaluate(() => window.scrollBy(0, 500));
    await page.waitForTimeout(300);
    await expect(header).toBeVisible();

    const headerBox = await header.boundingBox();
    expect(headerBox.y).toBeLessThanOrEqual(10);
  });

  test('CSS custom properties (variables) should be applied', async ({ page, browserName }) => {
    const primaryColor = await page.evaluate(() => {
      return getComputedStyle(document.documentElement).getPropertyValue('--primary-color').trim();
    });
    expect(primaryColor.length).toBeGreaterThan(0);
  });

  test('smooth scrolling should be enabled', async ({ page, browserName }) => {
    const scrollBehavior = await page.evaluate(() => {
      return window.getComputedStyle(document.documentElement).scrollBehavior;
    });
    expect(scrollBehavior).toBe('smooth');
  });

  test('images should load correctly', async ({ page, browserName }) => {
    // Wait for images to load
    await page.waitForLoadState('load');

    const images = page.locator('img');
    const imageCount = await images.count();
    expect(imageCount).toBeGreaterThan(0);

    // Verify at least one image is visible
    const showcaseImage = page.locator('.showcase-image');
    if (await showcaseImage.count() > 0) {
      await expect(showcaseImage.first()).toBeVisible();
    }
  });

  test('typography and font styles should be applied', async ({ page, browserName }) => {
    const body = page.locator('body');

    // Verify font-family is set
    const fontFamily = await body.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });
    expect(fontFamily.length).toBeGreaterThan(0);

    // Verify line-height is set
    const lineHeight = await body.evaluate((el) => {
      return window.getComputedStyle(el).lineHeight;
    });
    expect(lineHeight).not.toBe('normal');
  });

  test('box-sizing should be border-box', async ({ page, browserName }) => {
    const boxSizing = await page.evaluate(() => {
      const el = document.querySelector('.feature-card');
      return el ? window.getComputedStyle(el).boxSizing : null;
    });
    expect(boxSizing).toBe('border-box');
  });

  test('focus states should be visible for accessibility', async ({ page, browserName }) => {
    const ctaButton = page.locator('.cta-primary');
    await ctaButton.focus();

    // Verify focus is applied
    const outlineWidth = await ctaButton.evaluate((el) => {
      return window.getComputedStyle(el).outlineWidth;
    });
    // Either outline is visible or focus state exists
    expect(outlineWidth).not.toBe('');
  });

  test('transitions should be defined on interactive elements', async ({ page, browserName }) => {
    const ctaButton = page.locator('.cta-primary');

    const transition = await ctaButton.evaluate((el) => {
      return window.getComputedStyle(el).transition;
    });
    expect(transition).not.toBe('none');
    expect(transition.length).toBeGreaterThan(0);
  });
});
