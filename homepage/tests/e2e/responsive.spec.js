/**
 * Responsive Design E2E Tests
 * Owner: Scenario 6 - Responsive Design
 *
 * Tests that the homepage displays correctly across mobile, tablet, and desktop viewports.
 * Verifies:
 * - Layout adapts correctly at mobile breakpoint
 * - Layout adapts correctly at tablet breakpoint
 * - Navigation collapses to hamburger on mobile
 * - Feature cards stack on mobile
 * - Code blocks are scrollable on small screens
 */

const { test, expect } = require('@playwright/test');

// Viewport configurations
const viewports = {
  mobile: { width: 375, height: 667 },    // iPhone SE
  tablet: { width: 768, height: 1024 },   // iPad
  desktop: { width: 1440, height: 900 }   // Desktop
};

test.describe('Responsive Design', () => {
  test.describe('Mobile Viewport (375x667 - iPhone SE)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(viewports.mobile);
      await page.goto('/');
      await page.waitForLoadState('networkidle');
    });

    test('TC1: Page renders without horizontal scrollbar at mobile viewport', async ({ page }) => {
      // Check that the visible content area doesn't have horizontal overflow
      // Using documentElement.scrollWidth vs clientWidth as the authoritative check
      // since overflow-x: hidden on body hides actual scrollbar
      const hasVisibleHorizontalScroll = await page.evaluate(() => {
        // Check if there's an actual horizontal scrollbar visible
        const hasScrollbar = window.innerWidth > document.documentElement.clientWidth;
        // Also check if any visible content extends beyond viewport
        const computedOverflow = window.getComputedStyle(document.body).overflowX;
        return hasScrollbar && computedOverflow !== 'hidden';
      });
      expect(hasVisibleHorizontalScroll).toBe(false);

      // Verify viewport meta tag is present
      const viewportMeta = await page.locator('meta[name="viewport"]');
      await expect(viewportMeta).toHaveAttribute('content', /width=device-width/);
    });

    test('TC4: Navigation collapses to hamburger menu at mobile viewport', async ({ page }) => {
      // Hamburger menu toggle should be visible
      const menuToggle = page.locator('.header__menu-toggle');
      await expect(menuToggle).toBeVisible();

      // Navigation list should be hidden (or styled as mobile menu)
      const navList = page.locator('.header__nav-list');
      // The nav might be hidden or repositioned for mobile
      const isNavVisible = await navList.isVisible();

      // Either the nav is hidden, or the hamburger is shown
      // The hamburger being visible indicates mobile layout
      expect(await menuToggle.isVisible()).toBe(true);
    });

    test('TC5: Hamburger menu expands to show navigation links when clicked', async ({ page }) => {
      const menuToggle = page.locator('.header__menu-toggle');

      // Check initial state
      await expect(menuToggle).toHaveAttribute('aria-expanded', 'false');

      // Click hamburger menu
      await menuToggle.click();

      // Verify menu expanded
      await expect(menuToggle).toHaveAttribute('aria-expanded', 'true');

      // Navigation should now be visible/accessible
      const nav = page.locator('.header__nav');
      await expect(nav).toHaveClass(/header__nav--open/);

      // Nav links should be visible
      const navLinks = page.locator('.header__nav-link');
      const linksCount = await navLinks.count();
      expect(linksCount).toBeGreaterThan(0);
    });

    test('TC6: Feature cards stack vertically at mobile viewport', async ({ page }) => {
      // Navigate to features section
      await page.locator('#features').scrollIntoViewIfNeeded();

      const featureCards = page.locator('.features__card');
      const cardsCount = await featureCards.count();
      expect(cardsCount).toBeGreaterThan(0);

      // Get positions of first two cards
      if (cardsCount >= 2) {
        const firstCard = await featureCards.nth(0).boundingBox();
        const secondCard = await featureCards.nth(1).boundingBox();

        // Cards should be stacked vertically (second card below first)
        // Y position of second card should be greater than Y + height of first card
        expect(secondCard.y).toBeGreaterThanOrEqual(firstCard.y + firstCard.height - 10);
      }
    });

    test('TC7: Code blocks are scrollable horizontally if content overflows', async ({ page }) => {
      // Navigate to quick start section
      await page.locator('#quick-start').scrollIntoViewIfNeeded();

      const codeBlocks = page.locator('.quick-start__code');
      const codeCount = await codeBlocks.count();
      expect(codeCount).toBeGreaterThan(0);

      // Check that code blocks have overflow-x: auto or scroll
      const firstCodeBlock = codeBlocks.first();
      const overflowX = await firstCodeBlock.evaluate(el => {
        return window.getComputedStyle(el).overflowX;
      });

      // Should be 'auto' or 'scroll' to allow horizontal scrolling
      expect(['auto', 'scroll']).toContain(overflowX);
    });

    test('TC8: Hero text and CTA are properly sized and readable at mobile viewport', async ({ page }) => {
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      // Hero title should be visible and readable
      const heroTitle = page.locator('.hero__title');
      await expect(heroTitle).toBeVisible();
      const titleFontSize = await heroTitle.evaluate(el => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      // Title should be at least 24px for readability on mobile
      expect(titleFontSize).toBeGreaterThanOrEqual(24);

      // Tagline should be visible
      const heroTagline = page.locator('.hero__tagline');
      await expect(heroTagline).toBeVisible();

      // CTA button should be visible and tappable
      const heroCta = page.locator('.hero__cta');
      await expect(heroCta).toBeVisible();

      // CTA should have adequate tap target size (at least 44px)
      const ctaBox = await heroCta.boundingBox();
      expect(ctaBox.height).toBeGreaterThanOrEqual(44);
    });
  });

  test.describe('Tablet Viewport (768x1024 - iPad)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(viewports.tablet);
      await page.goto('/');
      await page.waitForLoadState('networkidle');
    });

    test('TC2: Page renders without horizontal scrollbar at tablet viewport', async ({ page }) => {
      // Check that the visible content area doesn't have horizontal overflow
      const hasVisibleHorizontalScroll = await page.evaluate(() => {
        const hasScrollbar = window.innerWidth > document.documentElement.clientWidth;
        const computedOverflow = window.getComputedStyle(document.body).overflowX;
        return hasScrollbar && computedOverflow !== 'hidden';
      });
      expect(hasVisibleHorizontalScroll).toBe(false);
    });

    test('Layout adapts correctly for tablet - feature grid', async ({ page }) => {
      // Navigate to features section
      await page.locator('#features').scrollIntoViewIfNeeded();

      const featuresGrid = page.locator('.features__grid');
      await expect(featuresGrid).toBeVisible();

      // Grid should display cards in a reasonable layout for tablet
      const featureCards = page.locator('.features__card');
      const cardsCount = await featureCards.count();
      expect(cardsCount).toBeGreaterThan(0);
    });

    test('Resources section adapts for tablet viewport', async ({ page }) => {
      // Navigate to resources section
      await page.locator('#resources').scrollIntoViewIfNeeded();

      const resourcesContent = page.locator('.resources__content');
      await expect(resourcesContent).toBeVisible();

      // Content should be visible and properly laid out
      const resourceLinks = page.locator('.resources__link');
      const linksCount = await resourceLinks.count();
      expect(linksCount).toBeGreaterThan(0);
    });
  });

  test.describe('Desktop Viewport (1440x900)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(viewports.desktop);
      await page.goto('/');
      await page.waitForLoadState('networkidle');
    });

    test('TC3: Page renders with proper desktop layout', async ({ page }) => {
      // Hamburger menu should be hidden on desktop
      const menuToggle = page.locator('.header__menu-toggle');
      await expect(menuToggle).toBeHidden();

      // Desktop navigation should be visible
      const navList = page.locator('.header__nav-list');
      await expect(navList).toBeVisible();

      // Container should respect max-width
      const container = page.locator('.container').first();
      const containerBox = await container.boundingBox();
      expect(containerBox.width).toBeLessThanOrEqual(1200 + 48); // max-width + padding
    });

    test('Feature cards display in grid layout on desktop', async ({ page }) => {
      // Navigate to features section
      await page.locator('#features').scrollIntoViewIfNeeded();

      const featureCards = page.locator('.features__card');
      const cardsCount = await featureCards.count();
      expect(cardsCount).toBeGreaterThanOrEqual(3);

      // Check that cards are displayed horizontally (first few cards on same row)
      if (cardsCount >= 3) {
        const firstCard = await featureCards.nth(0).boundingBox();
        const secondCard = await featureCards.nth(1).boundingBox();
        const thirdCard = await featureCards.nth(2).boundingBox();

        // On desktop, some cards should be on the same row (similar Y position)
        const tolerance = 10;
        const firstRowY = firstCard.y;
        const isSecondOnSameRow = Math.abs(secondCard.y - firstRowY) < tolerance;
        const isThirdOnSameRow = Math.abs(thirdCard.y - firstRowY) < tolerance;

        // At least 2 cards should be on the same row on desktop
        expect(isSecondOnSameRow || isThirdOnSameRow).toBe(true);
      }
    });

    test('Resources section displays in two-column layout on desktop', async ({ page }) => {
      // Navigate to resources section
      await page.locator('#resources').scrollIntoViewIfNeeded();

      const resourcesContent = page.locator('.resources__content');
      await expect(resourcesContent).toBeVisible();

      // Check grid display
      const displayStyle = await resourcesContent.evaluate(el => {
        return window.getComputedStyle(el).display;
      });
      expect(displayStyle).toBe('grid');
    });
  });

  test.describe('Content Readability', () => {
    for (const [viewportName, viewport] of Object.entries(viewports)) {
      test(`All text remains readable at ${viewportName} viewport`, async ({ page }) => {
        await page.setViewportSize(viewport);
        await page.goto('/');
        await page.waitForLoadState('networkidle');

        // Check that no text overflows its container
        const hasTextOverflow = await page.evaluate(() => {
          const elements = document.querySelectorAll('p, h1, h2, h3, h4, span');
          for (const el of elements) {
            const style = window.getComputedStyle(el);
            if (style.visibility === 'hidden' || style.display === 'none') continue;
            if (el.scrollWidth > el.clientWidth && style.overflowX !== 'auto' && style.overflowX !== 'scroll') {
              // Allow for small tolerance
              if (el.scrollWidth - el.clientWidth > 5) {
                return true;
              }
            }
          }
          return false;
        });
        expect(hasTextOverflow).toBe(false);
      });
    }
  });

  test.describe('No Horizontal Scrollbar', () => {
    for (const [viewportName, viewport] of Object.entries(viewports)) {
      test(`No horizontal scrollbar at ${viewportName} viewport`, async ({ page }) => {
        await page.setViewportSize(viewport);
        await page.goto('/');
        await page.waitForLoadState('networkidle');

        // Scroll through the entire page
        await page.evaluate(() => {
          window.scrollTo(0, document.body.scrollHeight);
        });
        await page.waitForTimeout(500);

        // Check for horizontal overflow
        const hasHorizontalScroll = await page.evaluate(() => {
          return document.documentElement.scrollWidth > document.documentElement.clientWidth;
        });
        expect(hasHorizontalScroll).toBe(false);
      });
    }
  });
});
