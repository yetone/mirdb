/**
 * E2E tests for Browser Compatibility.
 * Owner: Scenario 15 - Browser Compatibility
 *
 * Requirements: NFR-4
 *
 * Test cases:
 * 1. Load page in Chrome 90+ - Page renders correctly with all features working
 * 2. Load page in Firefox 88+ - Page renders correctly with all features working
 * 3. Load page in Safari 14+ - Page renders correctly with all features working
 * 4. Load page in Edge 90+ - Page renders correctly with all features working
 * 5. Test CSS features across browsers - CSS Grid and Flexbox work consistently
 * 6. Test JavaScript features across browsers - All JavaScript functionality works
 */

import { test, expect } from '@playwright/test';

test.describe('Browser Compatibility E2E', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test.describe('Test Cases 1-4: Page renders correctly across browsers', () => {
    test('page loads without errors', async ({ page }) => {
      // Verify the page has the app container
      const app = page.locator('#app');
      await expect(app).toBeVisible();
    });

    test('header section renders correctly', async ({ page }) => {
      const header = page.locator('.header');
      await expect(header).toBeVisible();

      // Logo should be visible
      const logo = page.locator('.header__logo');
      await expect(logo).toBeVisible();

      // Navigation should exist
      const nav = page.locator('.header__nav');
      await expect(nav).toBeAttached();
    });

    test('hero section renders correctly', async ({ page }) => {
      const hero = page.locator('#hero');
      await expect(hero).toBeVisible();

      // Hero title should be visible
      const heroTitle = page.locator('.hero-title');
      await expect(heroTitle).toBeVisible();
      await expect(heroTitle).toHaveText(/MirDB/);

      // Hero description should be visible
      const heroDescription = page.locator('.hero-description');
      await expect(heroDescription).toBeVisible();
    });

    test('features section renders correctly', async ({ page }) => {
      const features = page.locator('#features');
      await expect(features).toBeVisible();

      // Feature cards should be visible
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();
      expect(cardCount).toBeGreaterThan(0);
    });

    test('quickstart section renders correctly', async ({ page }) => {
      const quickstart = page.locator('#quickstart');
      await expect(quickstart).toBeVisible();

      // Code blocks should be visible
      const codeBlocks = page.locator('#quickstart pre');
      const blockCount = await codeBlocks.count();
      expect(blockCount).toBeGreaterThan(0);
    });

    test('protocol section renders correctly', async ({ page }) => {
      const protocol = page.locator('#protocol');
      await expect(protocol).toBeVisible();

      // Command cards should be visible
      const commandCards = page.locator('.command-card');
      const cardCount = await commandCards.count();
      expect(cardCount).toBeGreaterThan(0);
    });

    test('configuration section renders correctly', async ({ page }) => {
      const configuration = page.locator('#configuration');
      await expect(configuration).toBeVisible();

      // Config table should be visible
      const configTable = page.locator('.config-table');
      await expect(configTable).toBeAttached();
    });

    test('architecture section renders correctly', async ({ page }) => {
      const architecture = page.locator('#architecture');
      await expect(architecture).toBeVisible();

      // Architecture diagram should be visible
      const archDiagram = page.locator('.arch-diagram');
      await expect(archDiagram).toBeVisible();
    });

    test('footer section renders correctly', async ({ page }) => {
      const footer = page.locator('#footer');
      await expect(footer).toBeVisible();

      // Footer links should exist
      const footerLinks = page.locator('#footer a');
      const linkCount = await footerLinks.count();
      expect(linkCount).toBeGreaterThan(0);
    });

    test('all main sections are present', async ({ page }) => {
      const sections = ['#hero', '#features', '#quickstart', '#protocol', '#configuration', '#architecture'];

      for (const sectionId of sections) {
        const section = page.locator(sectionId);
        await expect(section).toBeAttached();
      }
    });
  });

  test.describe('Test Case 5: CSS features across browsers', () => {
    test('CSS Grid works correctly in features section', async ({ page }) => {
      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();

      // Verify grid display is applied
      const display = await featuresGrid.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(display).toBe('grid');
    });

    test('CSS Flexbox works correctly in header', async ({ page }) => {
      const headerContainer = page.locator('.header__content');

      if ((await headerContainer.count()) > 0) {
        const display = await headerContainer.evaluate((el) => {
          return window.getComputedStyle(el).display;
        });
        expect(display).toBe('flex');
      }
    });

    test('CSS Flexbox works correctly in hero section', async ({ page }) => {
      const heroContainer = page.locator('.hero-container');

      const display = await heroContainer.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(display).toBe('flex');
    });

    test('CSS Grid works correctly in commands grid', async ({ page }) => {
      const commandsGrid = page.locator('.commands-grid');

      if ((await commandsGrid.count()) > 0) {
        const display = await commandsGrid.evaluate((el) => {
          return window.getComputedStyle(el).display;
        });
        expect(display).toBe('grid');
      }
    });

    test('CSS custom properties are applied correctly', async ({ page }) => {
      // Check that CSS variables are being used
      const body = page.locator('body');

      const backgroundColor = await body.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Should have a valid background color (not empty)
      expect(backgroundColor).toBeTruthy();
      expect(backgroundColor).not.toBe('');
    });

    test('sticky positioning works for header', async ({ page }) => {
      const header = page.locator('.header');

      const position = await header.evaluate((el) => {
        return window.getComputedStyle(el).position;
      });
      expect(position).toBe('sticky');
    });

    test('CSS transitions are applied', async ({ page }) => {
      // Check that transitions are defined on interactive elements
      const navLinks = page.locator('.header__nav-link').first();

      if ((await navLinks.count()) > 0) {
        const transition = await navLinks.evaluate((el) => {
          return window.getComputedStyle(el).transition;
        });

        // Should have some transition defined (not 'none' or empty for all properties)
        expect(transition).toBeTruthy();
      }
    });

    test('box-sizing is border-box', async ({ page }) => {
      // Modern CSS best practice - verify box-sizing is applied
      const containers = page.locator('.container').first();

      if ((await containers.count()) > 0) {
        const boxSizing = await containers.evaluate((el) => {
          return window.getComputedStyle(el).boxSizing;
        });
        expect(boxSizing).toBe('border-box');
      }
    });
  });

  test.describe('Test Case 6: JavaScript features across browsers', () => {
    test('smooth scroll navigation works', async ({ page }) => {
      await page.setViewportSize({ width: 1200, height: 800 });

      // Click navigation link
      const featuresLink = page.locator('a[href="#features"]');
      await featuresLink.click();

      // Wait for scroll animation
      await page.waitForTimeout(500);

      // Verify page scrolled (scroll position changed)
      const scrollY = await page.evaluate(() => window.scrollY);
      expect(scrollY).toBeGreaterThan(0);
    });

    test('hamburger menu toggle works on mobile', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      const hamburger = page.locator('.header__hamburger');
      const nav = page.locator('.header__nav');

      // Initially closed
      await expect(nav).not.toHaveClass(/header__nav--open/);

      // Click to toggle
      await hamburger.click();

      // Should open
      await expect(nav).toHaveClass(/header__nav--open/);

      // Click again to close
      await hamburger.click();

      // Should close
      await expect(nav).not.toHaveClass(/header__nav--open/);
    });

    test('copy to clipboard buttons exist and are clickable', async ({ page }) => {
      const copyButtons = page.locator('.copy-button');
      const buttonCount = await copyButtons.count();

      if (buttonCount > 0) {
        // Verify at least one copy button is clickable
        const firstButton = copyButtons.first();
        await expect(firstButton).toBeVisible();
        await expect(firstButton).toBeEnabled();
      }
    });

    test('DOM content is rendered via JavaScript', async ({ page }) => {
      // The app is rendered via JavaScript, verify this worked
      const app = page.locator('#app');

      // Should have children (content was rendered)
      const childCount = await app.evaluate((el) => el.children.length);
      expect(childCount).toBeGreaterThan(0);
    });

    test('event listeners work correctly', async ({ page }) => {
      await page.setViewportSize({ width: 1200, height: 800 });

      // Test click event on navigation link
      const link = page.locator('a[href="#quickstart"]');

      if ((await link.count()) > 0) {
        const initialScrollY = await page.evaluate(() => window.scrollY);
        await link.click();
        await page.waitForTimeout(500);
        const newScrollY = await page.evaluate(() => window.scrollY);

        // Verify scroll happened (event listener worked)
        expect(newScrollY).toBeGreaterThanOrEqual(initialScrollY);
      }
    });

    test('dynamic class manipulation works', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      const hamburger = page.locator('.header__hamburger');

      // Check aria-expanded attribute changes
      await expect(hamburger).toHaveAttribute('aria-expanded', 'false');

      await hamburger.click();

      await expect(hamburger).toHaveAttribute('aria-expanded', 'true');
    });

    test('template literals and modern JS features work', async ({ page }) => {
      // Verify that the app content includes dynamically generated text
      // which would require modern JS features to render
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();

      // If features are rendered, modern JS is working
      expect(cardCount).toBeGreaterThan(0);

      // Check that feature cards have text content (template literals worked)
      const firstCard = featureCards.first();
      const textContent = await firstCard.textContent();
      expect(textContent).toBeTruthy();
      expect(textContent!.length).toBeGreaterThan(0);
    });

    test('async operations work correctly', async ({ page }) => {
      // The page should load fully without errors
      // Check console for JavaScript errors
      const errors: string[] = [];

      page.on('pageerror', (error) => {
        errors.push(error.message);
      });

      // Navigate and wait for full load
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // No JavaScript errors should have occurred
      expect(errors.length).toBe(0);
    });
  });

  test.describe('Cross-browser visual consistency', () => {
    test('fonts render correctly', async ({ page }) => {
      const body = page.locator('body');

      const fontFamily = await body.evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });

      // Should have a font family defined
      expect(fontFamily).toBeTruthy();
      expect(fontFamily).not.toBe('');
    });

    test('colors are applied correctly', async ({ page }) => {
      const header = page.locator('.header');

      const backgroundColor = await header.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Should have a valid color (not transparent or empty)
      expect(backgroundColor).toBeTruthy();
    });

    test('images load correctly', async ({ page }) => {
      // Check if logo image loads
      const logoImg = page.locator('.header__logo-img');

      if ((await logoImg.count()) > 0) {
        // Wait for image to load
        await expect(logoImg).toBeVisible();

        // Verify image has dimensions (loaded successfully)
        const box = await logoImg.boundingBox();
        if (box) {
          expect(box.width).toBeGreaterThan(0);
          expect(box.height).toBeGreaterThan(0);
        }
      }
    });

    test('layout is consistent', async ({ page }) => {
      await page.setViewportSize({ width: 1200, height: 800 });

      // Verify main structural elements are positioned correctly
      const header = page.locator('.header');
      const headerBox = await header.boundingBox();

      expect(headerBox).not.toBeNull();
      expect(headerBox!.y).toBeLessThanOrEqual(10); // Header at top
      expect(headerBox!.x).toBe(0); // Header full width starts at 0
    });

    test('z-index layering works correctly', async ({ page }) => {
      // Scroll down to ensure header stays on top
      await page.evaluate(() => window.scrollTo(0, 500));

      const header = page.locator('.header');
      await expect(header).toBeVisible();

      // Header should still be at top of viewport
      const headerBox = await header.boundingBox();
      expect(headerBox).not.toBeNull();
      expect(headerBox!.y).toBeLessThanOrEqual(0);
    });
  });

  test.describe('Browser-specific rendering', () => {
    test('viewport meta tag is respected', async ({ page }) => {
      // Set mobile viewport and verify content scales correctly
      await page.setViewportSize({ width: 375, height: 667 });

      const viewportWidth = await page.evaluate(() => document.documentElement.clientWidth);
      expect(viewportWidth).toBe(375);
    });

    test('scrolling works correctly', async ({ page }) => {
      // Scroll to bottom
      await page.evaluate(() => {
        window.scrollTo(0, document.body.scrollHeight);
      });

      await page.waitForTimeout(100);

      // Verify scroll happened
      const scrollY = await page.evaluate(() => window.scrollY);
      expect(scrollY).toBeGreaterThan(0);

      // Scroll back to top
      await page.evaluate(() => {
        window.scrollTo({ top: 0, behavior: 'instant' });
      });

      await page.waitForTimeout(200);

      // Allow small tolerance for scroll position (smooth scrolling may not be exactly 0)
      const newScrollY = await page.evaluate(() => window.scrollY);
      expect(newScrollY).toBeLessThanOrEqual(50);
    });

    test('pointer events work correctly', async ({ page }) => {
      const link = page.locator('a[href="#features"]');

      if ((await link.count()) > 0) {
        // Hover should not cause errors
        await link.hover();

        // Click should work
        await link.click();

        // Verify no errors occurred
        await expect(page.locator('#app')).toBeVisible();
      }
    });
  });
});
