// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test suite for mobile responsive design (NFR-1)
 * Verifies page renders correctly on mobile devices at various viewports
 */

test.describe('Responsive Design - Mobile', () => {

  test.describe('iPhone SE Viewport (375x667)', () => {
    test.use({ viewport: { width: 375, height: 667 } });

    test('should render page in single-column layout with all content readable', async ({ page }) => {
      await page.goto('/');

      // Page should load successfully
      await expect(page).toHaveTitle(/MirDB/);

      // Body should fit within viewport width
      const body = page.locator('body');
      await expect(body).toBeVisible();
      const bodyBox = await body.boundingBox();
      expect(bodyBox.width).toBeLessThanOrEqual(375);

      // Hero section should be visible and properly sized
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();
      const heroBox = await hero.boundingBox();
      expect(heroBox.width).toBeLessThanOrEqual(375);

      // Hero title should be visible and readable
      const heroTitle = page.locator('.hero h1');
      await expect(heroTitle).toBeVisible();
      await expect(heroTitle).toContainText('MirDB');

      // Tagline should be visible
      const tagline = page.locator('.hero .tagline');
      await expect(tagline).toBeVisible();

      // Description text should be visible and readable
      const description = page.locator('.hero .description');
      await expect(description).toBeVisible();

      // CTA buttons should be visible and accessible
      const ctaButtons = page.locator('.cta-buttons .cta');
      await expect(ctaButtons.first()).toBeVisible();

      // Features section should be visible
      const features = page.locator('#features');
      await expect(features).toBeVisible();

      // Getting started section should be visible
      const gettingStarted = page.locator('#getting-started');
      await expect(gettingStarted).toBeVisible();

      // Footer should be visible
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();
    });
  });

  test.describe('iPhone 11 Pro Max Viewport (414x896)', () => {
    test.use({ viewport: { width: 414, height: 896 } });

    test('should render page correctly on larger mobile screen', async ({ page }) => {
      await page.goto('/');

      // Page should load successfully
      await expect(page).toHaveTitle(/MirDB/);

      // Body should fit within viewport width
      const body = page.locator('body');
      await expect(body).toBeVisible();
      const bodyBox = await body.boundingBox();
      expect(bodyBox.width).toBeLessThanOrEqual(414);

      // Hero section should be visible and properly sized
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();
      const heroBox = await hero.boundingBox();
      expect(heroBox.width).toBeLessThanOrEqual(414);

      // Hero content should be fully visible
      const heroContent = page.locator('.hero-content');
      await expect(heroContent).toBeVisible();

      // Hero title should be visible
      const heroTitle = page.locator('.hero h1');
      await expect(heroTitle).toBeVisible();
      await expect(heroTitle).toContainText('MirDB');

      // Tagline should be visible
      const tagline = page.locator('.hero .tagline');
      await expect(tagline).toBeVisible();

      // All main sections should be accessible via scroll
      const sections = ['#features', '#getting-started', '.commands', 'footer'];
      for (const selector of sections) {
        const section = page.locator(selector);
        await expect(section).toBeVisible();
      }
    });
  });

  test.describe('Mobile Navigation', () => {
    test.use({ viewport: { width: 375, height: 667 } });

    test('should have accessible navigation on mobile (hamburger menu or scrollable nav)', async ({ page }) => {
      await page.goto('/');

      // Navbar should be visible
      const navbar = page.locator('.navbar');
      await expect(navbar).toBeVisible();

      // Check if there's a hamburger menu or scrollable nav
      const hamburgerMenu = page.locator('.hamburger, .menu-toggle, .mobile-menu-btn, [aria-label="Toggle menu"]');
      const navLinks = page.locator('.nav-links');

      const hamburgerCount = await hamburgerMenu.count();
      const navLinksVisible = await navLinks.isVisible();

      // Either hamburger menu exists OR nav links are visible (scrollable nav)
      if (hamburgerCount > 0) {
        // If hamburger exists, it should be visible on mobile
        const hamburger = hamburgerMenu.first();
        const isHamburgerVisible = await hamburger.isVisible();
        expect(isHamburgerVisible).toBe(true);
      } else {
        // Nav links should be visible (stacked or scrollable)
        expect(navLinksVisible).toBe(true);
      }

      // Navigation should contain the essential links
      const featuresLink = page.locator('.nav-links a[href="#features"]');
      const gettingStartedLink = page.locator('.nav-links a[href="#getting-started"]');

      // Links should be in the DOM
      await expect(featuresLink).toBeAttached();
      await expect(gettingStartedLink).toBeAttached();
    });
  });

  test.describe('Touch Target Sizes', () => {
    test.use({ viewport: { width: 375, height: 667 } });

    test('should have minimum 44x44px touch targets for buttons and links', async ({ page }) => {
      await page.goto('/');

      // Check CTA buttons have adequate touch target size
      const ctaButtons = page.locator('.cta-buttons .cta');
      const ctaCount = await ctaButtons.count();

      for (let i = 0; i < ctaCount; i++) {
        const button = ctaButtons.nth(i);
        await expect(button).toBeVisible();
        const box = await button.boundingBox();
        // Minimum touch target should be 44x44px
        expect(box.width).toBeGreaterThanOrEqual(44);
        expect(box.height).toBeGreaterThanOrEqual(44);
      }

      // Check navigation links have adequate touch target size
      const navLinks = page.locator('.nav-links a');
      const navLinkCount = await navLinks.count();

      for (let i = 0; i < navLinkCount; i++) {
        const link = navLinks.nth(i);
        const isVisible = await link.isVisible();
        if (isVisible) {
          const box = await link.boundingBox();
          // Touch targets should be at least 44px in height (width can vary for text links)
          expect(box.height).toBeGreaterThanOrEqual(44);
        }
      }

      // Check footer links have adequate touch target size
      const footerLinks = page.locator('.footer-links a');
      const footerLinkCount = await footerLinks.count();

      for (let i = 0; i < footerLinkCount; i++) {
        const link = footerLinks.nth(i);
        const isVisible = await link.isVisible();
        if (isVisible) {
          const box = await link.boundingBox();
          expect(box.height).toBeGreaterThanOrEqual(44);
        }
      }
    });
  });

  test.describe('No Horizontal Scroll', () => {
    test.use({ viewport: { width: 375, height: 667 } });

    test('should not require horizontal scrolling at 375px width', async ({ page }) => {
      await page.goto('/');

      // Check for horizontal overflow
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);

      // Verify scrollWidth equals clientWidth (no horizontal overflow)
      const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
      const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);
      expect(scrollWidth).toBeLessThanOrEqual(clientWidth + 1); // Allow 1px tolerance
    });

    test('should not require horizontal scrolling at 414px width', async ({ page }) => {
      await page.setViewportSize({ width: 414, height: 896 });
      await page.goto('/');

      // Check for horizontal overflow
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);

      // Verify scrollWidth equals clientWidth
      const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
      const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);
      expect(scrollWidth).toBeLessThanOrEqual(clientWidth + 1);
    });
  });

  test.describe('Feature Cards Mobile Layout', () => {
    test.use({ viewport: { width: 375, height: 667 } });

    test('should display feature cards in single-column layout on mobile', async ({ page }) => {
      await page.goto('/');

      // Navigate to features section
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();

      // Feature grid should be visible
      const featureGrid = page.locator('.feature-grid');
      await expect(featureGrid).toBeVisible();

      // Get all feature cards
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();
      expect(cardCount).toBe(4);

      // All cards should be visible
      for (let i = 0; i < cardCount; i++) {
        await expect(featureCards.nth(i)).toBeVisible();
      }

      // At 375px width, cards should stack vertically (single column)
      // Check that cards are stacked (each subsequent card is below the previous)
      const firstCardBox = await featureCards.first().boundingBox();
      const secondCardBox = await featureCards.nth(1).boundingBox();

      // Cards should be stacked vertically (second card Y > first card Y + height)
      expect(secondCardBox.y).toBeGreaterThan(firstCardBox.y);

      // Cards should have similar width (single column layout)
      const widthDifference = Math.abs(firstCardBox.width - secondCardBox.width);
      expect(widthDifference).toBeLessThan(10); // Within 10px tolerance
    });

    test('should make feature card content readable on mobile', async ({ page }) => {
      await page.goto('/');

      const featureCards = page.locator('.feature-card');
      const cardTitles = [
        'Memcached Compatibility',
        'Persistent Storage',
        'LSM Tree Architecture',
        'Built with Rust'
      ];

      for (let i = 0; i < cardTitles.length; i++) {
        const card = featureCards.nth(i);
        await card.scrollIntoViewIfNeeded();

        const heading = card.locator('h3');
        const paragraph = card.locator('p');

        await expect(heading).toBeVisible();
        await expect(heading).toContainText(cardTitles[i]);
        await expect(paragraph).toBeVisible();
      }
    });
  });

  test.describe('Code Examples Mobile', () => {
    test.use({ viewport: { width: 375, height: 667 } });

    test('should make code examples scrollable without breaking page layout', async ({ page }) => {
      await page.goto('/');

      // Navigate to getting started section
      const gettingStarted = page.locator('#getting-started');
      await gettingStarted.scrollIntoViewIfNeeded();

      // Code examples should be visible
      const codeExamples = page.locator('.code-example');
      const codeCount = await codeExamples.count();
      expect(codeCount).toBeGreaterThan(0);

      // Each code example should have overflow handling
      for (let i = 0; i < codeCount; i++) {
        const codeExample = codeExamples.nth(i);
        await codeExample.scrollIntoViewIfNeeded();
        await expect(codeExample).toBeVisible();

        // Code example should not exceed viewport width
        const box = await codeExample.boundingBox();
        expect(box.width).toBeLessThanOrEqual(375);
      }

      // Page should still not have horizontal scroll
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);
    });
  });

  test.describe('Footer Mobile Layout', () => {
    test.use({ viewport: { width: 375, height: 667 } });

    test('should display footer content properly on mobile', async ({ page }) => {
      await page.goto('/');

      // Scroll to footer
      const footer = page.locator('footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();

      // Footer content should be visible
      const footerContent = page.locator('.footer-content');
      await expect(footerContent).toBeVisible();

      // Footer should fit within viewport
      const footerBox = await footer.boundingBox();
      expect(footerBox.width).toBeLessThanOrEqual(375);

      // Footer links should be accessible
      const footerLinks = page.locator('.footer-links a');
      const linkCount = await footerLinks.count();
      expect(linkCount).toBeGreaterThan(0);
    });
  });
});
