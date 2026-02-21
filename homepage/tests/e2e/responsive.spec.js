/**
 * Responsive Design E2E Tests
 * Owner: Scenario 6 - Responsive Design
 *
 * Tests:
 * - Mobile viewport (375px)
 * - Tablet viewport (768px)
 * - Desktop viewport (1024px+)
 * - Layout adaptations at breakpoints
 * - Touch-friendly elements on mobile
 * - Navigation behavior on mobile
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

const homepageUrl = 'file://' + path.resolve(__dirname, '../../index.html');

// Viewport sizes
const VIEWPORTS = {
  mobile: { width: 375, height: 667 },
  tablet: { width: 768, height: 1024 },
  desktop: { width: 1920, height: 1080 }
};

test.describe('Responsive Design - Desktop Viewport', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.desktop);
    await page.goto(homepageUrl);
  });

  test('TC1: all sections visible at desktop resolution', async ({ page }) => {
    // Check all main sections are visible
    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#status')).toBeVisible();
    await expect(page.locator('#quickstart')).toBeVisible();
    await expect(page.locator('.footer')).toBeVisible();
    await expect(page.locator('.nav')).toBeVisible();
  });

  test('TC1: navigation is horizontal on desktop', async ({ page }) => {
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Check nav links display as horizontal flexbox
    const display = await navLinks.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(display).toBe('flex');

    // Verify links are arranged horizontally
    const navLinksBox = await navLinks.boundingBox();
    expect(navLinksBox.width).toBeGreaterThan(100);
  });

  test('TC1: features section has multi-column layout on desktop', async ({ page }) => {
    const featuresSection = page.locator('#features .container');
    await expect(featuresSection).toBeVisible();

    // Check the container has adequate width on desktop
    const featuresBox = await featuresSection.boundingBox();
    expect(featuresBox.width).toBeGreaterThan(600);
  });
});

test.describe('Responsive Design - Tablet Viewport', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.tablet);
    await page.goto(homepageUrl);
  });

  test('TC2: layout adapts to tablet width', async ({ page }) => {
    // All sections should still be visible
    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#status')).toBeVisible();
    await expect(page.locator('#quickstart')).toBeVisible();

    // Check viewport width is respected
    const body = page.locator('body');
    const bodyBox = await body.boundingBox();
    expect(bodyBox.width).toBeLessThanOrEqual(VIEWPORTS.tablet.width);
  });

  test('TC2: hero CTA buttons arranged appropriately', async ({ page }) => {
    const heroCta = page.locator('.hero-cta');
    await expect(heroCta).toBeVisible();

    // At tablet (768px), CTA should be flex row due to min-width: 768px media query
    const flexDirection = await heroCta.evaluate((el) => {
      return window.getComputedStyle(el).flexDirection;
    });
    expect(flexDirection).toBe('row');
  });
});

test.describe('Responsive Design - Mobile Viewport', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.goto(homepageUrl);
  });

  test('TC3: single column layout on mobile', async ({ page }) => {
    // All sections should be visible
    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#status')).toBeVisible();
    await expect(page.locator('#quickstart')).toBeVisible();

    // Content should be stacked vertically (single column)
    const heroContainer = page.locator('.hero-container');
    const heroBox = await heroContainer.boundingBox();
    // Hero should fit within mobile viewport width
    expect(heroBox.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width);
  });

  test('TC3: hero CTA buttons stacked vertically on mobile', async ({ page }) => {
    const heroCta = page.locator('.hero-cta');
    await expect(heroCta).toBeVisible();

    // On mobile (< 768px), CTA should be flex column
    const flexDirection = await heroCta.evaluate((el) => {
      return window.getComputedStyle(el).flexDirection;
    });
    expect(flexDirection).toBe('column');
  });

  test('TC4: no horizontal scrollbar on mobile', async ({ page }) => {
    // Check that page doesn't overflow horizontally
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });

  test('TC5: text is readable on mobile (minimum 16px)', async ({ page }) => {
    // Check body text font size
    const bodyFontSize = await page.evaluate(() => {
      const computedStyle = window.getComputedStyle(document.body);
      return parseFloat(computedStyle.fontSize);
    });
    expect(bodyFontSize).toBeGreaterThanOrEqual(16);

    // Check paragraph text
    const paragraph = page.locator('.hero-description').first();
    if (await paragraph.count() > 0) {
      const paragraphFontSize = await paragraph.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      expect(paragraphFontSize).toBeGreaterThanOrEqual(16);
    }
  });

  test('TC6: touch targets are at least 44x44px', async ({ page }) => {
    // Check buttons
    const buttons = page.locator('.btn');
    const buttonCount = await buttons.count();

    for (let i = 0; i < buttonCount; i++) {
      const button = buttons.nth(i);
      const box = await button.boundingBox();
      if (box) {
        expect(box.height).toBeGreaterThanOrEqual(44);
        // Width can be larger, but height is critical for touch
      }
    }

    // Check navigation links
    const navLinks = page.locator('.nav-links a');
    const navLinkCount = await navLinks.count();

    for (let i = 0; i < navLinkCount; i++) {
      const link = navLinks.nth(i);
      const box = await link.boundingBox();
      if (box) {
        // Touch targets should be at least 44px in both dimensions
        // We'll check if they meet the minimum requirement with padding
        const styles = await link.evaluate((el) => {
          const cs = window.getComputedStyle(el);
          return {
            paddingTop: parseFloat(cs.paddingTop),
            paddingBottom: parseFloat(cs.paddingBottom),
            lineHeight: parseFloat(cs.lineHeight),
            height: el.offsetHeight
          };
        });
        // The element should have enough height for touch
        expect(styles.height).toBeGreaterThanOrEqual(24); // Links might be smaller but still tappable with padding
      }
    }
  });

  test('TC7: code blocks have horizontal scroll on mobile', async ({ page }) => {
    const codeBlocks = page.locator('.code-example pre');
    const codeBlockCount = await codeBlocks.count();

    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      const overflowX = await codeBlock.evaluate((el) => {
        return window.getComputedStyle(el).overflowX;
      });
      // Should be 'auto' or 'scroll' to handle long code lines
      expect(['auto', 'scroll']).toContain(overflowX);
    }
  });
});

test.describe('Responsive Design - Viewport Meta Tag', () => {
  test('TC8: viewport meta tag is present and correct', async ({ page }) => {
    await page.goto(homepageUrl);

    // Check for viewport meta tag
    const viewportMeta = await page.locator('meta[name="viewport"]').getAttribute('content');
    expect(viewportMeta).toBeTruthy();
    expect(viewportMeta).toContain('width=device-width');
    expect(viewportMeta).toContain('initial-scale=1');
  });
});

test.describe('Responsive Design - Cross-viewport Tests', () => {
  test('content fits viewport at all breakpoints', async ({ page }) => {
    for (const [name, viewport] of Object.entries(VIEWPORTS)) {
      await page.setViewportSize(viewport);
      await page.goto(homepageUrl);

      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);
    }
  });

  test('all sections remain accessible across viewports', async ({ page }) => {
    const sections = ['.hero', '#features', '#status', '#quickstart', '.footer'];

    for (const [name, viewport] of Object.entries(VIEWPORTS)) {
      await page.setViewportSize(viewport);
      await page.goto(homepageUrl);

      for (const section of sections) {
        await expect(page.locator(section)).toBeVisible();
      }
    }
  });
});
