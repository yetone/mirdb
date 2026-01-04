// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Responsive Design - Desktop', () => {
  test.describe('1920x1080 Viewport', () => {
    test.use({ viewport: { width: 1920, height: 1080 } });

    test('should render page correctly without horizontal scrolling at 1920x1080', async ({ page }) => {
      await page.goto('/');

      // Check page renders
      await expect(page).toHaveTitle(/MirDB/);

      // Verify no horizontal scrollbar by checking scrollWidth equals clientWidth
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);

      // Verify content is centered - check hero section
      const heroContent = page.locator('.hero-content');
      await expect(heroContent).toBeVisible();

      // Check that hero content is horizontally centered
      const heroBoundingBox = await heroContent.boundingBox();
      const viewportWidth = 1920;
      if (heroBoundingBox) {
        const leftMargin = heroBoundingBox.x;
        const rightMargin = viewportWidth - (heroBoundingBox.x + heroBoundingBox.width);
        // Content should be roughly centered (margins within 50% difference)
        const marginDifference = Math.abs(leftMargin - rightMargin);
        expect(marginDifference).toBeLessThan(viewportWidth * 0.1);
      }

      // Verify main content is readable (hero text visible)
      const heroTitle = page.locator('.hero h1');
      await expect(heroTitle).toBeVisible();
      await expect(heroTitle).toContainText('MirDB');

      const tagline = page.locator('.hero .tagline');
      await expect(tagline).toBeVisible();
    });
  });

  test.describe('1366x768 Viewport', () => {
    test.use({ viewport: { width: 1366, height: 768 } });

    test('should render page correctly at 1366x768 desktop resolution', async ({ page }) => {
      await page.goto('/');

      // Check page renders properly
      await expect(page).toHaveTitle(/MirDB/);

      // Verify no horizontal scrollbar
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);

      // Verify hero section is visible and properly rendered
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      // Verify hero content is readable
      const heroTitle = page.locator('.hero h1');
      await expect(heroTitle).toBeVisible();

      const tagline = page.locator('.hero .tagline');
      await expect(tagline).toBeVisible();

      const description = page.locator('.hero .description');
      await expect(description).toBeVisible();

      // Verify CTA buttons are visible
      const ctaButtons = page.locator('.cta-buttons .cta');
      await expect(ctaButtons.first()).toBeVisible();

      // Verify features section is accessible
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();
      await expect(featuresSection).toBeVisible();
    });
  });

  test.describe('Feature Cards Layout', () => {
    test.use({ viewport: { width: 1920, height: 1080 } });

    test('should display feature cards in grid or row layout appropriate for desktop', async ({ page }) => {
      await page.goto('/');

      // Navigate to features section
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();

      // Get all feature cards
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();

      // Verify there are multiple feature cards
      expect(cardCount).toBeGreaterThanOrEqual(2);

      // Get bounding boxes to verify grid/row layout
      const firstCard = featureCards.first();
      const secondCard = featureCards.nth(1);

      await expect(firstCard).toBeVisible();
      await expect(secondCard).toBeVisible();

      const firstBox = await firstCard.boundingBox();
      const secondBox = await secondCard.boundingBox();

      // On desktop, cards should be displayed side by side (same Y position or row layout)
      // OR in a grid where multiple cards fit per row
      if (firstBox && secondBox) {
        // Cards should either be in same row (similar Y) or in a grid pattern
        // For a grid layout, the second card should start to the right of the first
        // or below if row wraps
        const isHorizontalLayout = Math.abs(firstBox.y - secondBox.y) < 50;
        const isGridLayout = secondBox.x > firstBox.x || secondBox.y > firstBox.y;
        expect(isHorizontalLayout || isGridLayout).toBe(true);
      }

      // Verify feature-grid uses CSS Grid layout
      const featureGrid = page.locator('.feature-grid');
      const gridDisplay = await featureGrid.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(gridDisplay).toBe('grid');
    });
  });

  test.describe('Navigation Visibility', () => {
    test.use({ viewport: { width: 1920, height: 1080 } });

    test('should display navigation links without hamburger menu on desktop', async ({ page }) => {
      await page.goto('/');

      // Verify navbar is visible
      const navbar = page.locator('.navbar');
      await expect(navbar).toBeVisible();

      // Verify nav-links container is visible (not hidden for mobile)
      const navLinks = page.locator('.nav-links');
      await expect(navLinks).toBeVisible();

      // Verify nav-links is displayed (not hidden)
      const navLinksDisplay = await navLinks.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(navLinksDisplay).not.toBe('none');

      // Verify individual navigation links are visible
      const featuresLink = page.locator('.nav-links a[href="#features"]');
      const gettingStartedLink = page.locator('.nav-links a[href="#getting-started"]');
      const githubLink = page.locator('.nav-links a[href*="github"]');

      await expect(featuresLink).toBeVisible();
      await expect(gettingStartedLink).toBeVisible();
      await expect(githubLink).toBeVisible();

      // Verify there's no hamburger menu button visible (common mobile pattern)
      const hamburgerMenu = page.locator('.hamburger, .menu-toggle, .mobile-menu-btn, [aria-label="Toggle menu"]');
      const hamburgerCount = await hamburgerMenu.count();
      if (hamburgerCount > 0) {
        // If a hamburger exists, it should be hidden on desktop
        const hamburgerVisible = await hamburgerMenu.first().isVisible();
        expect(hamburgerVisible).toBe(false);
      }

      // Verify nav links are displayed horizontally (flex layout)
      const navLinksFlexDisplay = await navLinks.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(navLinksFlexDisplay).toBe('flex');
    });

    test.describe('at 1366x768', () => {
      test.use({ viewport: { width: 1366, height: 768 } });

      test('should display navigation links without hamburger menu at 1366x768', async ({ page }) => {
        await page.goto('/');

        // Verify nav-links is visible
        const navLinks = page.locator('.nav-links');
        await expect(navLinks).toBeVisible();

        // Verify links are displayed horizontally
        const navLinksDisplay = await navLinks.evaluate((el) => {
          return window.getComputedStyle(el).display;
        });
        expect(navLinksDisplay).toBe('flex');

        // Verify all nav links are visible
        const featuresLink = page.locator('.nav-links a[href="#features"]');
        const gettingStartedLink = page.locator('.nav-links a[href="#getting-started"]');

        await expect(featuresLink).toBeVisible();
        await expect(gettingStartedLink).toBeVisible();
      });
    });
  });
});
