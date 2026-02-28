/**
 * E2E tests for responsive design
 * Owner: Scenario 6 - Responsive Design
 *
 * Expected test suites:
 * - Mobile viewport (375x667)
 * - Tablet viewport (768x1024)
 * - Desktop viewport (1920x1080)
 * - Navigation collapse behavior
 * - Grid layout changes
 */

const { test, expect } = require('@playwright/test');

// Viewport definitions
const VIEWPORTS = {
  mobile: { width: 375, height: 667 },
  tablet: { width: 768, height: 1024 },
  desktop: { width: 1920, height: 1080 }
};

// Minimum touch target size (WCAG 2.5.5)
const MIN_TOUCH_TARGET = 44;

test.describe('Responsive Design', () => {

  // Test Case 1: Desktop viewport renders without horizontal scroll
  test.describe('Desktop Viewport (1920x1080)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.desktop);
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');
    });

    test('TC1: page renders without horizontal scroll and all content visible', async ({ page }) => {
      // Check for horizontal overflow
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);

      // Verify key sections are visible
      await expect(page.locator('#hero')).toBeVisible();
      await expect(page.locator('#features')).toBeVisible();
      await expect(page.locator('#status')).toBeVisible();
      await expect(page.locator('#usage')).toBeVisible();
      await expect(page.locator('#quickstart')).toBeVisible();
      await expect(page.locator('footer')).toBeVisible();
    });

    test('navigation is fully visible on desktop', async ({ page }) => {
      // Mobile menu toggle should be hidden
      const menuToggle = page.locator('.mobile-menu-toggle');
      await expect(menuToggle).toBeHidden();

      // Navigation should be visible
      const nav = page.locator('.header-nav');
      await expect(nav).toBeVisible();

      // All navigation links should be visible
      const navLinks = page.locator('.header-nav a');
      const count = await navLinks.count();
      expect(count).toBeGreaterThan(0);

      for (let i = 0; i < count; i++) {
        await expect(navLinks.nth(i)).toBeVisible();
      }
    });

    test('features grid displays in multi-column layout', async ({ page }) => {
      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();

      // Check grid has multiple columns
      const gridColumns = await featuresGrid.evaluate((el) => {
        return window.getComputedStyle(el).gridTemplateColumns;
      });

      // Should have multiple columns (more than 1fr)
      const columnCount = gridColumns.split(' ').filter(col => col !== '').length;
      expect(columnCount).toBeGreaterThan(1);
    });
  });

  // Test Case 2: Tablet viewport renders correctly
  test.describe('Tablet Viewport (768x1024)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.tablet);
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');
    });

    test('TC2: page renders without horizontal scroll and layout adjusts', async ({ page }) => {
      // Check for horizontal overflow
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);

      // Verify page elements are visible
      await expect(page.locator('#hero')).toBeVisible();
      await expect(page.locator('#features')).toBeVisible();
    });

    test('TC6: feature grid adjusts to 2 columns', async ({ page }) => {
      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();

      // Check grid columns - should be 2 on tablet
      const gridColumns = await featuresGrid.evaluate((el) => {
        return window.getComputedStyle(el).gridTemplateColumns;
      });

      const columnCount = gridColumns.split(' ').filter(col => col !== '').length;
      expect(columnCount).toBe(2);
    });

    test('navigation displays on tablet', async ({ page }) => {
      // On tablet (768px+), full nav should be visible
      const nav = page.locator('.header-nav');
      await expect(nav).toBeVisible();

      // Mobile toggle should be hidden on tablet
      const menuToggle = page.locator('.mobile-menu-toggle');
      await expect(menuToggle).toBeHidden();
    });
  });

  // Test Case 3: Mobile viewport renders correctly
  test.describe('Mobile Viewport (375x667)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');
    });

    test('TC3: page renders without horizontal scroll with single column layout', async ({ page }) => {
      // Check for horizontal overflow
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);

      // Verify page elements are visible
      await expect(page.locator('#hero')).toBeVisible();
    });

    test('TC4: navigation is collapsed with hamburger menu visible', async ({ page }) => {
      // Mobile menu toggle should be visible
      const menuToggle = page.locator('.mobile-menu-toggle');
      await expect(menuToggle).toBeVisible();

      // Navigation should be hidden initially
      const nav = page.locator('.header-nav');
      // Check that nav is not visible (either hidden or has opacity 0)
      const isNavVisible = await nav.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.visibility !== 'hidden' && style.opacity !== '0';
      });
      expect(isNavVisible).toBe(false);
    });

    test('TC5: mobile menu expands on click', async ({ page }) => {
      const menuToggle = page.locator('.mobile-menu-toggle');
      const nav = page.locator('.header-nav');

      // Click to open menu
      await menuToggle.click();
      await page.waitForTimeout(300); // Wait for transition

      // Navigation should now be visible
      const isNavVisible = await nav.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.visibility !== 'hidden' && style.opacity !== '0';
      });
      expect(isNavVisible).toBe(true);

      // Verify aria-expanded is updated
      const ariaExpanded = await menuToggle.getAttribute('aria-expanded');
      expect(ariaExpanded).toBe('true');
    });

    test('TC7: feature grid displays as single column', async ({ page }) => {
      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();

      // Check grid columns - should be 1 on mobile
      const gridColumns = await featuresGrid.evaluate((el) => {
        return window.getComputedStyle(el).gridTemplateColumns;
      });

      const columnCount = gridColumns.split(' ').filter(col => col !== '').length;
      expect(columnCount).toBe(1);
    });

    test('TC8: hero section is readable with appropriately sized text', async ({ page }) => {
      const heroTitle = page.locator('.hero-title');
      const heroTagline = page.locator('.hero-tagline');

      await expect(heroTitle).toBeVisible();
      await expect(heroTagline).toBeVisible();

      // Check font sizes are appropriate for mobile
      const titleFontSize = await heroTitle.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });

      // Title should be at least 24px on mobile for readability
      expect(titleFontSize).toBeGreaterThanOrEqual(24);

      const taglineFontSize = await heroTagline.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });

      // Tagline should be at least 14px for readability
      expect(taglineFontSize).toBeGreaterThanOrEqual(14);
    });

    test('TC9: code blocks are horizontally scrollable', async ({ page }) => {
      // Scroll to usage section
      await page.locator('#usage').scrollIntoViewIfNeeded();

      const codeBlock = page.locator('.code-block').first();
      await expect(codeBlock).toBeVisible();

      // Check that code block has overflow-x auto or scroll
      const overflowX = await codeBlock.evaluate((el) => {
        return window.getComputedStyle(el).overflowX;
      });

      expect(['auto', 'scroll']).toContain(overflowX);

      // Verify code block doesn't break page layout
      const codeBlockWidth = await codeBlock.evaluate((el) => {
        return el.getBoundingClientRect().width;
      });

      expect(codeBlockWidth).toBeLessThanOrEqual(VIEWPORTS.mobile.width);
    });

    test('TC10: interactive elements have minimum 44x44px touch targets', async ({ page }) => {
      // Test mobile menu toggle
      const menuToggle = page.locator('.mobile-menu-toggle');
      const toggleBox = await menuToggle.boundingBox();
      expect(toggleBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
      expect(toggleBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);

      // Test CTA button
      const ctaButton = page.locator('.hero-cta');
      if (await ctaButton.isVisible()) {
        const ctaBox = await ctaButton.boundingBox();
        expect(ctaBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
      }

      // Open menu and test nav links
      await menuToggle.click();
      await page.waitForTimeout(300);

      const navLinks = page.locator('.header-nav a');
      const count = await navLinks.count();

      for (let i = 0; i < Math.min(count, 3); i++) {
        const link = navLinks.nth(i);
        if (await link.isVisible()) {
          const linkBox = await link.boundingBox();
          if (linkBox) {
            expect(linkBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
          }
        }
      }
    });

    test('footer displays in single column on mobile', async ({ page }) => {
      await page.locator('footer').scrollIntoViewIfNeeded();

      const footerContent = page.locator('.footer-content');
      await expect(footerContent).toBeVisible();

      // Check flex direction is column on mobile
      const flexDirection = await footerContent.evaluate((el) => {
        return window.getComputedStyle(el).flexDirection;
      });

      expect(flexDirection).toBe('column');
    });
  });

  // Cross-viewport tests
  test.describe('Cross-Viewport Behavior', () => {

    test('page adjusts layout when viewport changes', async ({ page }) => {
      // Start at desktop
      await page.setViewportSize(VIEWPORTS.desktop);
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Verify desktop layout
      let menuToggle = page.locator('.mobile-menu-toggle');
      await expect(menuToggle).toBeHidden();

      // Resize to mobile
      await page.setViewportSize(VIEWPORTS.mobile);
      await page.waitForTimeout(100);

      // Verify mobile layout
      await expect(menuToggle).toBeVisible();
    });

    test('no content overflow at any viewport', async ({ page }) => {
      const viewports = [VIEWPORTS.mobile, VIEWPORTS.tablet, VIEWPORTS.desktop];

      for (const viewport of viewports) {
        await page.setViewportSize(viewport);
        await page.goto('/');
        await page.waitForLoadState('domcontentloaded');

        const hasOverflow = await page.evaluate(() => {
          return document.documentElement.scrollWidth > document.documentElement.clientWidth;
        });

        expect(hasOverflow).toBe(false);
      }
    });
  });

  // Tech Stack Grid Tests
  test.describe('Tech Stack Grid Responsiveness', () => {

    test('tech stack grid is single column on mobile', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);
      await page.goto('/');
      await page.locator('#tech-stack').scrollIntoViewIfNeeded();

      const techGrid = page.locator('.tech-stack-grid');
      const gridColumns = await techGrid.evaluate((el) => {
        return window.getComputedStyle(el).gridTemplateColumns;
      });

      const columnCount = gridColumns.split(' ').filter(col => col !== '').length;
      expect(columnCount).toBe(1);
    });

    test('tech stack grid is 2 columns on tablet', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.tablet);
      await page.goto('/');
      await page.locator('#tech-stack').scrollIntoViewIfNeeded();

      const techGrid = page.locator('.tech-stack-grid');
      const gridColumns = await techGrid.evaluate((el) => {
        return window.getComputedStyle(el).gridTemplateColumns;
      });

      const columnCount = gridColumns.split(' ').filter(col => col !== '').length;
      expect(columnCount).toBe(2);
    });

    test('tech stack grid has 4 columns on desktop', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.desktop);
      await page.goto('/');
      await page.locator('#tech-stack').scrollIntoViewIfNeeded();

      const techGrid = page.locator('.tech-stack-grid');
      const gridColumns = await techGrid.evaluate((el) => {
        return window.getComputedStyle(el).gridTemplateColumns;
      });

      const columnCount = gridColumns.split(' ').filter(col => col !== '').length;
      expect(columnCount).toBe(4);
    });
  });

  // Status Container Responsiveness
  test.describe('Status Container Responsiveness', () => {

    test('status container is single column on mobile', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);
      await page.goto('/');
      await page.locator('#status').scrollIntoViewIfNeeded();

      const statusContainer = page.locator('.status-container');
      const gridColumns = await statusContainer.evaluate((el) => {
        return window.getComputedStyle(el).gridTemplateColumns;
      });

      const columnCount = gridColumns.split(' ').filter(col => col !== '').length;
      expect(columnCount).toBe(1);
    });

    test('status container is 2 columns on tablet', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.tablet);
      await page.goto('/');
      await page.locator('#status').scrollIntoViewIfNeeded();

      const statusContainer = page.locator('.status-container');
      const gridColumns = await statusContainer.evaluate((el) => {
        return window.getComputedStyle(el).gridTemplateColumns;
      });

      const columnCount = gridColumns.split(' ').filter(col => col !== '').length;
      expect(columnCount).toBe(2);
    });
  });
});
