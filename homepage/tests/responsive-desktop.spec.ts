import { test, expect, Page } from '@playwright/test';

/**
 * E2E tests for responsive design on desktop viewport sizes
 * Tests layout, navigation, and feature cards at various desktop resolutions
 */

// Desktop viewport configurations
const DESKTOP_VIEWPORTS = {
  FULL_HD: { width: 1920, height: 1080 },
  SMALL_DESKTOP: { width: 1440, height: 900 },
};

test.describe('Responsive Design - Desktop', () => {
  test.describe('Test Case 1: Homepage at 1920x1080 Resolution', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(DESKTOP_VIEWPORTS.FULL_HD);
      await page.goto('/');
    });

    test('should render page correctly at 1920x1080', async ({ page }) => {
      // Verify page loads successfully
      await expect(page).toHaveTitle(/MirDB/);
    });

    test('should have proper layout without horizontal scroll at 1920x1080', async ({ page }) => {
      // Check that body width doesn't exceed viewport width (no horizontal scroll)
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);
    });

    test('should have readable content with appropriate font sizes at 1920x1080', async ({ page }) => {
      // Check hero title font size
      const heroTitle = page.locator('.hero-title');
      await expect(heroTitle).toBeVisible();
      const titleFontSize = await heroTitle.evaluate(el =>
        parseFloat(window.getComputedStyle(el).fontSize)
      );
      // Title should be at least 40px on desktop (4rem at base 16px)
      expect(titleFontSize).toBeGreaterThanOrEqual(40);

      // Check tagline font size
      const tagline = page.locator('.hero-tagline');
      await expect(tagline).toBeVisible();
      const taglineFontSize = await tagline.evaluate(el =>
        parseFloat(window.getComputedStyle(el).fontSize)
      );
      // Tagline should be at least 20px on desktop
      expect(taglineFontSize).toBeGreaterThanOrEqual(20);
    });

    test('should display hero section within viewport at 1920x1080', async ({ page }) => {
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Hero should be within viewport bounds
      const heroBox = await heroSection.boundingBox();
      expect(heroBox).toBeTruthy();
      expect(heroBox!.x).toBeGreaterThanOrEqual(0);
      expect(heroBox!.width).toBeLessThanOrEqual(1920);
    });

    test('should have proper content width constraints at 1920x1080', async ({ page }) => {
      // Container elements should have max-width set
      const container = page.locator('.container').first();
      await expect(container).toBeVisible();

      const containerBox = await container.boundingBox();
      expect(containerBox).toBeTruthy();
      // Container max-width is 1200px
      expect(containerBox!.width).toBeLessThanOrEqual(1200);
    });
  });

  test.describe('Test Case 2: Homepage at 1440x900 Resolution', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(DESKTOP_VIEWPORTS.SMALL_DESKTOP);
      await page.goto('/');
    });

    test('should render page correctly at 1440x900', async ({ page }) => {
      // Verify page loads successfully
      await expect(page).toHaveTitle(/MirDB/);
    });

    test('should have proper layout without horizontal scroll at 1440x900', async ({ page }) => {
      // Check that body width doesn't exceed viewport width
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);
    });

    test('should adapt layout for smaller desktop screen at 1440x900', async ({ page }) => {
      // All main sections should be visible
      await expect(page.locator('#hero')).toBeVisible();
      await expect(page.locator('#features')).toBeVisible();
      await expect(page.locator('#getting-started')).toBeVisible();
      await expect(page.locator('#configuration')).toBeVisible();

      // Check that content fits within viewport width
      const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
      expect(bodyScrollWidth).toBeLessThanOrEqual(1440);
    });

    test('should maintain readable typography at 1440x900', async ({ page }) => {
      // Check body text is readable
      const heroDescription = page.locator('.hero-description');
      await expect(heroDescription).toBeVisible();

      const fontSize = await heroDescription.evaluate(el =>
        parseFloat(window.getComputedStyle(el).fontSize)
      );
      // Body text should be at least 14px for readability
      expect(fontSize).toBeGreaterThanOrEqual(14);
    });

    test('should display all sections properly at 1440x900', async ({ page }) => {
      // Verify all main sections exist and are accessible
      const sections = ['#hero', '#features', '#architecture', '#getting-started', '#configuration', '#status'];

      for (const section of sections) {
        const element = page.locator(section);
        await expect(element).toBeAttached();
      }
    });
  });

  test.describe('Test Case 3: Navigation Menu at Desktop Resolution', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(DESKTOP_VIEWPORTS.FULL_HD);
      await page.goto('/');
    });

    test('should display full navigation menu at desktop resolution', async ({ page }) => {
      const navLinks = page.locator('.nav-links');
      await expect(navLinks).toBeVisible();
    });

    test('should not show hamburger menu at desktop resolution', async ({ page }) => {
      // Hamburger menu should not exist on desktop
      // Check that nav-links is visible and displayed as flex (not hidden)
      const navLinksDisplay = await page.locator('.nav-links').evaluate(el =>
        window.getComputedStyle(el).display
      );
      expect(navLinksDisplay).toBe('flex');
    });

    test('should display all navigation items horizontally', async ({ page }) => {
      const navLinks = page.locator('.nav-links');
      const display = await navLinks.evaluate(el => window.getComputedStyle(el).display);
      expect(display).toBe('flex');

      // Check that nav items are displayed
      const navItems = page.locator('.nav-links li');
      const count = await navItems.count();
      expect(count).toBeGreaterThanOrEqual(3); // Features, Getting Started, Configuration, GitHub

      // All nav items should be visible
      for (let i = 0; i < count; i++) {
        await expect(navItems.nth(i)).toBeVisible();
      }
    });

    test('should show brand logo in navigation', async ({ page }) => {
      const navBrand = page.locator('.nav-brand');
      await expect(navBrand).toBeVisible();
      await expect(navBrand).toContainText('MirDB');
    });

    test('should have functional navigation links', async ({ page }) => {
      // Check Features link
      const featuresLink = page.locator('.nav-links a[href="#features"]');
      await expect(featuresLink).toBeVisible();

      // Check Getting Started link
      const gettingStartedLink = page.locator('.nav-links a[href="#getting-started"]');
      await expect(gettingStartedLink).toBeVisible();

      // Check Configuration link
      const configLink = page.locator('.nav-links a[href="#configuration"]');
      await expect(configLink).toBeVisible();

      // Check GitHub link
      const githubLink = page.locator('[data-testid="nav-github-link"]');
      await expect(githubLink).toBeVisible();
    });

    test('should have navigation links visible at 1440x900 as well', async ({ page }) => {
      await page.setViewportSize(DESKTOP_VIEWPORTS.SMALL_DESKTOP);

      const navLinks = page.locator('.nav-links');
      await expect(navLinks).toBeVisible();

      const display = await navLinks.evaluate(el => window.getComputedStyle(el).display);
      expect(display).toBe('flex');
    });
  });

  test.describe('Test Case 4: Feature Cards Layout at Desktop', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(DESKTOP_VIEWPORTS.FULL_HD);
      await page.goto('/');
    });

    test('should display feature cards in multi-column grid layout', async ({ page }) => {
      const featureGrid = page.locator('.feature-grid');
      await expect(featureGrid).toBeVisible();

      // Check grid display property
      const display = await featureGrid.evaluate(el => window.getComputedStyle(el).display);
      expect(display).toBe('grid');
    });

    test('should have multiple feature cards visible', async ({ page }) => {
      const featureCards = page.locator('.feature-card');
      const count = await featureCards.count();

      // Should have 4 feature cards (Memcached Protocol, Persistent Storage, LSM Tree, Rust)
      expect(count).toBe(4);

      // All cards should be visible
      for (let i = 0; i < count; i++) {
        await expect(featureCards.nth(i)).toBeVisible();
      }
    });

    test('should arrange feature cards in multiple columns at 1920x1080', async ({ page }) => {
      const featureCards = page.locator('.feature-card');

      // Get the bounding boxes of first two cards
      const firstCard = await featureCards.nth(0).boundingBox();
      const secondCard = await featureCards.nth(1).boundingBox();

      expect(firstCard).toBeTruthy();
      expect(secondCard).toBeTruthy();

      // On desktop, cards should be side by side (same Y position or close)
      // If they were stacked, the second card would have a significantly higher Y
      const yDifference = Math.abs(firstCard!.y - secondCard!.y);
      expect(yDifference).toBeLessThan(50); // Cards should be roughly on the same row
    });

    test('should maintain multi-column layout at 1440x900', async ({ page }) => {
      await page.setViewportSize(DESKTOP_VIEWPORTS.SMALL_DESKTOP);

      const featureCards = page.locator('.feature-card');

      // Get the bounding boxes of first two cards
      const firstCard = await featureCards.nth(0).boundingBox();
      const secondCard = await featureCards.nth(1).boundingBox();

      expect(firstCard).toBeTruthy();
      expect(secondCard).toBeTruthy();

      // On desktop (even smaller), cards should still be in multiple columns
      const yDifference = Math.abs(firstCard!.y - secondCard!.y);
      expect(yDifference).toBeLessThan(50);
    });

    test('should have consistent feature card styling', async ({ page }) => {
      const featureCards = page.locator('.feature-card');

      // Check that all cards have similar dimensions
      const firstCardBox = await featureCards.nth(0).boundingBox();
      const secondCardBox = await featureCards.nth(1).boundingBox();

      expect(firstCardBox).toBeTruthy();
      expect(secondCardBox).toBeTruthy();

      // Cards should have similar widths (within 10px tolerance for grid gap)
      const widthDifference = Math.abs(firstCardBox!.width - secondCardBox!.width);
      expect(widthDifference).toBeLessThan(10);
    });

    test('should display feature card content correctly', async ({ page }) => {
      const featureCards = page.locator('.feature-card');

      // Check first card has title and description
      const firstCardTitle = featureCards.nth(0).locator('h3');
      const firstCardDesc = featureCards.nth(0).locator('p');

      await expect(firstCardTitle).toBeVisible();
      await expect(firstCardDesc).toBeVisible();

      // Verify content is not empty
      const titleText = await firstCardTitle.textContent();
      const descText = await firstCardDesc.textContent();

      expect(titleText).toBeTruthy();
      expect(descText).toBeTruthy();
      expect(titleText!.length).toBeGreaterThan(0);
      expect(descText!.length).toBeGreaterThan(0);
    });
  });

  test.describe('Additional Desktop Responsive Checks', () => {
    test('should have proper code block formatting at desktop resolution', async ({ page }) => {
      await page.setViewportSize(DESKTOP_VIEWPORTS.FULL_HD);
      await page.goto('/');

      // Navigate to getting started section
      const codeBlocks = page.locator('pre code');
      const count = await codeBlocks.count();
      expect(count).toBeGreaterThan(0);

      // Check first code block is visible and formatted
      const firstCodeBlock = codeBlocks.first();
      await expect(firstCodeBlock).toBeVisible();

      // Check overflow handling
      const preElement = page.locator('pre').first();
      const overflow = await preElement.evaluate(el => window.getComputedStyle(el).overflowX);
      expect(overflow).toBe('auto');
    });

    test('should have proper table layout at desktop resolution', async ({ page }) => {
      await page.setViewportSize(DESKTOP_VIEWPORTS.FULL_HD);
      await page.goto('/');

      const configTable = page.locator('.config-table');
      await expect(configTable).toBeVisible();

      // Table should fit within container
      const tableBox = await configTable.boundingBox();
      expect(tableBox).toBeTruthy();
      expect(tableBox!.width).toBeLessThanOrEqual(1200);
    });

    test('should have sticky navigation at desktop resolution', async ({ page }) => {
      await page.setViewportSize(DESKTOP_VIEWPORTS.FULL_HD);
      await page.goto('/');

      const navbar = page.locator('.navbar');
      const position = await navbar.evaluate(el => window.getComputedStyle(el).position);
      expect(position).toBe('sticky');
    });

    test('should maintain footer visibility at desktop resolution', async ({ page }) => {
      await page.setViewportSize(DESKTOP_VIEWPORTS.FULL_HD);
      await page.goto('/');

      // Scroll to bottom
      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));

      const footer = page.locator('[data-testid="footer-section"]');
      await expect(footer).toBeVisible();
    });
  });
});
