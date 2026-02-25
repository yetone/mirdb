/**
 * Responsive Design E2E Tests
 * Owner: Scenario 9 (mobile), Scenario 10 (tablet/desktop)
 *
 * Test cases:
 * - Mobile: hamburger menu, single column, touch targets
 * - Tablet: appropriate layout
 * - Desktop: full navigation, multi-column
 */

const { test, expect } = require('@playwright/test');

// ============================================
// SECTION: Mobile Tests (Scenario 9)
// Viewport: 320px - 767px
// ============================================

test.describe('Mobile Responsive Design (375px)', () => {
  test.beforeEach(async ({ page }) => {
    // Set mobile viewport (iPhone size)
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');
  });

  test('TC1: Page loads without horizontal scrollbar, content fits within viewport', async ({ page }) => {
    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');

    // Check that page width does not exceed viewport
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // Body should not be wider than viewport (no horizontal scroll)
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 1); // Allow 1px tolerance

    // Check that there is no horizontal scrollbar
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Verify main content sections are visible
    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
  });

  test('TC2: Navigation shows hamburger menu icon instead of full navigation links', async ({ page }) => {
    // Hamburger menu button should be visible on mobile
    const navToggle = page.locator('.nav-toggle');
    await expect(navToggle).toBeVisible();

    // Navigation links should be hidden by default on mobile
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).not.toBeVisible();

    // Check that hamburger has three bars
    const bars = page.locator('.nav-toggle__bar');
    await expect(bars).toHaveCount(3);
  });

  test('TC3: Click hamburger menu opens mobile navigation with all links visible', async ({ page }) => {
    const navToggle = page.locator('.nav-toggle');
    const navLinks = page.locator('.nav-links');

    // Initial state: menu is closed
    await expect(navLinks).not.toBeVisible();
    await expect(navToggle).toHaveAttribute('aria-expanded', 'false');

    // Click hamburger to open menu
    await navToggle.click();

    // Menu should now be open
    await expect(navLinks).toBeVisible();
    await expect(navToggle).toHaveAttribute('aria-expanded', 'true');

    // All navigation links should be visible
    const links = navLinks.locator('.nav-link');
    const linkCount = await links.count();
    expect(linkCount).toBeGreaterThanOrEqual(3); // At least Features, Quick Start, GitHub

    // Check each link is visible
    for (let i = 0; i < linkCount; i++) {
      await expect(links.nth(i)).toBeVisible();
    }

    // Close menu by clicking toggle again
    await navToggle.click();
    await expect(navLinks).not.toBeVisible();
    await expect(navToggle).toHaveAttribute('aria-expanded', 'false');
  });

  test('TC4: All buttons and links have minimum 44x44px touch target area', async ({ page }) => {
    // Get all interactive elements
    const interactiveSelectors = [
      '.btn',
      '.nav-toggle',
      '.nav-link',
      '.link-card',
      '.code-block__copy'
    ];

    for (const selector of interactiveSelectors) {
      const elements = page.locator(selector);
      const count = await elements.count();

      for (let i = 0; i < count; i++) {
        const element = elements.nth(i);

        // Only check visible elements
        if (await element.isVisible()) {
          const boundingBox = await element.boundingBox();

          if (boundingBox) {
            // Touch targets should be at least 44x44 pixels
            expect(boundingBox.width).toBeGreaterThanOrEqual(44);
            expect(boundingBox.height).toBeGreaterThanOrEqual(44);
          }
        }
      }
    }
  });

  test('TC5: Body text is at least 16px font size and readable without zooming', async ({ page }) => {
    // Check body font size
    const bodyFontSize = await page.evaluate(() => {
      const body = document.body;
      const computedStyle = window.getComputedStyle(body);
      return parseFloat(computedStyle.fontSize);
    });
    expect(bodyFontSize).toBeGreaterThanOrEqual(16);

    // Check paragraph text in main content areas
    const contentParagraphs = page.locator('.hero__description, .what-is__description, .feature-description');
    const paragraphCount = await contentParagraphs.count();

    for (let i = 0; i < Math.min(paragraphCount, 5); i++) {
      const paragraph = contentParagraphs.nth(i);
      if (await paragraph.isVisible()) {
        const fontSize = await paragraph.evaluate(el => {
          return parseFloat(window.getComputedStyle(el).fontSize);
        });
        expect(fontSize).toBeGreaterThanOrEqual(16);
      }
    }

    // Verify the page doesn't require horizontal scrolling for main content
    // (code blocks may have intentional horizontal scroll, so we check document level)
    const hasPageHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasPageHorizontalScroll).toBe(false);
  });

  test('Hamburger menu closes when clicking outside', async ({ page }) => {
    const navToggle = page.locator('.nav-toggle');
    const navLinks = page.locator('.nav-links');

    // Open menu
    await navToggle.click();
    await expect(navLinks).toBeVisible();

    // Click outside the menu
    await page.locator('.hero').click();

    // Menu should close
    await expect(navLinks).not.toBeVisible();
  });

  test('Hamburger menu closes on Escape key', async ({ page }) => {
    const navToggle = page.locator('.nav-toggle');
    const navLinks = page.locator('.nav-links');

    // Open menu
    await navToggle.click();
    await expect(navLinks).toBeVisible();

    // Press Escape
    await page.keyboard.press('Escape');

    // Menu should close
    await expect(navLinks).not.toBeVisible();
  });

  test('Mobile menu links close menu when clicked', async ({ page }) => {
    const navToggle = page.locator('.nav-toggle');
    const navLinks = page.locator('.nav-links');

    // Open menu
    await navToggle.click();
    await expect(navLinks).toBeVisible();

    // Click on Features link
    const featuresLink = navLinks.locator('a[href="#features"]');
    await featuresLink.click();

    // Wait for click to register and menu to close
    await page.waitForTimeout(500);

    // Menu should close after clicking a link (key mobile UX behavior)
    await expect(navLinks).not.toBeVisible();

    // Navigation toggle should show collapsed state
    await expect(navToggle).toHaveAttribute('aria-expanded', 'false');
  });

  test('Feature cards display in single column on mobile', async ({ page }) => {
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Check grid column style
    const gridColumns = await featuresGrid.evaluate(el => {
      const style = window.getComputedStyle(el);
      return style.gridTemplateColumns;
    });

    // On mobile, grid should be single column (1fr)
    expect(gridColumns).toMatch(/^[\d.]+px$/); // Single column width
  });

  test('Steps display vertically on mobile', async ({ page }) => {
    const steps = page.locator('.step');
    const stepCount = await steps.count();

    if (stepCount > 1) {
      // Check that steps are stacked vertically (flex-direction: column)
      const firstStep = steps.first();
      const flexDirection = await firstStep.evaluate(el => {
        return window.getComputedStyle(el).flexDirection;
      });
      expect(flexDirection).toBe('column');
    }
  });

  test('Hero CTA buttons stack vertically on mobile', async ({ page }) => {
    const heroCta = page.locator('.hero__cta');

    if (await heroCta.isVisible()) {
      const flexDirection = await heroCta.evaluate(el => {
        return window.getComputedStyle(el).flexDirection;
      });
      expect(flexDirection).toBe('column');
    }
  });
});

// Additional viewport tests for edge cases
test.describe('Mobile Edge Cases', () => {
  test('Works at minimum mobile width (320px)', async ({ page }) => {
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto('/');

    // Page should still be usable
    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('.nav-toggle')).toBeVisible();

    // No horizontal scroll
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });

  test('Works at upper mobile boundary (767px)', async ({ page }) => {
    await page.setViewportSize({ width: 767, height: 1024 });
    await page.goto('/');

    // Hamburger menu should still be visible at 767px
    const navToggle = page.locator('.nav-toggle');
    await expect(navToggle).toBeVisible();

    // Nav links should be hidden
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).not.toBeVisible();
  });
});

// Tablet/Desktop tests placeholder (Scenario 10)
test.describe('Tablet/Desktop Responsive Design (Scenario 10)', () => {
  // These tests are owned by Scenario 10
  test.skip('Tablet breakpoint shows proper layout', async () => {
    // To be implemented by Scenario 10
  });

  test.skip('Desktop shows full navigation', async () => {
    // To be implemented by Scenario 10
  });
});
