/**
 * Responsive Design E2E Tests
 * Owner: Scenario 6 - Responsive Design
 *
 * Test cases:
 * - Viewport meta tag exists
 * - Desktop (1920x1080) layout works
 * - Tablet (768x1024) layout adapts
 * - Mobile (375x667) layout is usable
 * - No horizontal scrolling at any size
 * - CSS media queries are applied
 */

const { test, expect } = require('@playwright/test');
const { getPageUrl, viewportSizes } = require('../test-utils/helpers');

test.describe('Responsive Design Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(getPageUrl());
  });

  test.describe('Test Case 1: Viewport Meta Tag', () => {
    test('Meta viewport tag with width=device-width exists in head', async ({ page }) => {
      const viewportMeta = page.locator('meta[name="viewport"]');
      await expect(viewportMeta).toHaveCount(1);

      const content = await viewportMeta.getAttribute('content');
      expect(content).toContain('width=device-width');
    });
  });

  test.describe('Test Case 2: Desktop Layout (1920x1080)', () => {
    test('All content is visible and properly laid out at desktop size', async ({ page }) => {
      await page.setViewportSize(viewportSizes.desktop);

      // Header/Hero section is visible
      const header = page.locator('header');
      await expect(header).toBeVisible();

      // Logo is visible
      const logo = page.locator('.logo');
      await expect(logo).toBeVisible();

      // H1 title is visible
      const title = page.locator('h1');
      await expect(title).toBeVisible();
      await expect(title).toHaveText('MirDB');

      // Tagline is visible
      const tagline = page.locator('.tagline');
      await expect(tagline).toBeVisible();

      // CTA buttons are visible
      const ctaButtons = page.locator('.cta-buttons .btn');
      await expect(ctaButtons).toHaveCount(2);

      // Main sections are visible
      const features = page.locator('#features');
      await expect(features).toBeVisible();

      const demo = page.locator('#demo');
      await expect(demo).toBeVisible();

      const quickstart = page.locator('#quickstart');
      await expect(quickstart).toBeVisible();

      // Footer is visible
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();
    });
  });

  test.describe('Test Case 3: Tablet Layout (768x1024)', () => {
    test('Layout adapts appropriately to tablet size without breaking', async ({ page }) => {
      await page.setViewportSize(viewportSizes.tablet);

      // Header/Hero section adapts
      const header = page.locator('header');
      await expect(header).toBeVisible();

      // Logo is still visible
      const logo = page.locator('.logo');
      await expect(logo).toBeVisible();

      // Title adapts and remains readable
      const title = page.locator('h1');
      await expect(title).toBeVisible();

      // CTA buttons remain usable
      const ctaButtons = page.locator('.cta-buttons .btn');
      await expect(ctaButtons.first()).toBeVisible();
      await expect(ctaButtons.last()).toBeVisible();

      // All main sections are accessible
      const features = page.locator('#features');
      await expect(features).toBeVisible();

      const demo = page.locator('#demo');
      await expect(demo).toBeVisible();

      const quickstart = page.locator('#quickstart');
      await expect(quickstart).toBeVisible();

      // Footer is visible
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();

      // Features list items are visible
      const featureItems = page.locator('.features-list li');
      const count = await featureItems.count();
      expect(count).toBeGreaterThan(0);
      for (let i = 0; i < count; i++) {
        await expect(featureItems.nth(i)).toBeVisible();
      }
    });
  });

  test.describe('Test Case 4: Mobile Layout (375x667)', () => {
    test('Layout is fully usable on mobile, buttons/links are tappable', async ({ page }) => {
      await page.setViewportSize(viewportSizes.mobile);

      // Header/Hero section works on mobile
      const header = page.locator('header');
      await expect(header).toBeVisible();

      // Logo is visible
      const logo = page.locator('.logo');
      await expect(logo).toBeVisible();

      // Title is visible and readable
      const title = page.locator('h1');
      await expect(title).toBeVisible();

      // Tagline is visible
      const tagline = page.locator('.tagline');
      await expect(tagline).toBeVisible();

      // CTA buttons are tappable (visible and have proper size)
      const viewOnGithub = page.locator('.btn-primary');
      await expect(viewOnGithub).toBeVisible();
      const githubBox = await viewOnGithub.boundingBox();
      expect(githubBox.height).toBeGreaterThanOrEqual(44); // Minimum tappable size

      const getStarted = page.locator('.btn-secondary');
      await expect(getStarted).toBeVisible();
      const startedBox = await getStarted.boundingBox();
      expect(startedBox.height).toBeGreaterThanOrEqual(44); // Minimum tappable size

      // All main sections are visible
      const features = page.locator('#features');
      await expect(features).toBeVisible();

      const demo = page.locator('#demo');
      await expect(demo).toBeVisible();

      const quickstart = page.locator('#quickstart');
      await expect(quickstart).toBeVisible();

      // Footer is visible
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();

      // Copy buttons are tappable
      const copyButtons = page.locator('.copy-btn');
      const copyCount = await copyButtons.count();
      for (let i = 0; i < copyCount; i++) {
        await expect(copyButtons.nth(i)).toBeVisible();
        const box = await copyButtons.nth(i).boundingBox();
        expect(box.height).toBeGreaterThanOrEqual(32); // Reasonable tap target
      }
    });
  });

  test.describe('Test Case 5: CSS Media Queries', () => {
    test('CSS includes media queries for responsive breakpoints', async ({ page }) => {
      // Read the CSS file content directly using fetch
      const fs = require('fs');
      const path = require('path');
      const cssPath = path.join(process.cwd(), 'css', 'styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf-8');

      // Extract media query rules using regex
      const mediaQueryRegex = /@media\s*\([^)]+\)/g;
      const mediaQueries = cssContent.match(mediaQueryRegex) || [];

      // Check that media queries exist for responsive breakpoints
      expect(mediaQueries.length).toBeGreaterThan(0);

      // Verify at least one tablet breakpoint (around 768px)
      const hasTabletBreakpoint = mediaQueries.some(q =>
        q.includes('768') || q.includes('769') || q.includes('800')
      );
      expect(hasTabletBreakpoint).toBe(true);

      // Verify at least one mobile breakpoint (around 480px or smaller)
      const hasMobileBreakpoint = mediaQueries.some(q =>
        q.includes('480') || q.includes('375') || q.includes('320')
      );
      expect(hasMobileBreakpoint).toBe(true);
    });
  });

  test.describe('Test Case 6: No Horizontal Scrolling', () => {
    test('No horizontal scrollbar appears at 375px width', async ({ page }) => {
      await page.setViewportSize(viewportSizes.mobile);

      // Wait for content to load
      await page.waitForLoadState('domcontentloaded');

      // Check for horizontal overflow
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasHorizontalScroll).toBe(false);
    });

    test('No horizontal scrollbar at tablet size (768px)', async ({ page }) => {
      await page.setViewportSize(viewportSizes.tablet);

      await page.waitForLoadState('domcontentloaded');

      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasHorizontalScroll).toBe(false);
    });

    test('No horizontal scrollbar at desktop size (1920px)', async ({ page }) => {
      await page.setViewportSize(viewportSizes.desktop);

      await page.waitForLoadState('domcontentloaded');

      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasHorizontalScroll).toBe(false);
    });
  });

  test.describe('Layout Responsiveness', () => {
    test('Code blocks adapt to mobile layout', async ({ page }) => {
      await page.setViewportSize(viewportSizes.mobile);

      const codeBlocks = page.locator('.code-block');
      const count = await codeBlocks.count();

      for (let i = 0; i < count; i++) {
        const codeBlock = codeBlocks.nth(i);
        await expect(codeBlock).toBeVisible();

        // Verify code block fits within viewport
        const box = await codeBlock.boundingBox();
        expect(box.width).toBeLessThanOrEqual(viewportSizes.mobile.width);
      }
    });

    test('Images scale properly at mobile size', async ({ page }) => {
      await page.setViewportSize(viewportSizes.mobile);

      // Logo should scale within viewport
      const logo = page.locator('.logo');
      const logoBox = await logo.boundingBox();
      expect(logoBox.width).toBeLessThanOrEqual(viewportSizes.mobile.width);

      // Usage GIF should scale within viewport
      const usageGif = page.locator('.usage-gif');
      const gifBox = await usageGif.boundingBox();
      expect(gifBox.width).toBeLessThanOrEqual(viewportSizes.mobile.width);
    });

    test('Buttons remain visible and usable at all sizes', async ({ page }) => {
      const sizes = [viewportSizes.mobile, viewportSizes.tablet, viewportSizes.desktop];

      for (const size of sizes) {
        await page.setViewportSize(size);

        // CTA buttons visible
        const viewOnGithub = page.locator('.btn-primary');
        await expect(viewOnGithub).toBeVisible();

        const getStarted = page.locator('.btn-secondary');
        await expect(getStarted).toBeVisible();

        // Copy buttons visible
        const copyButtons = page.locator('.copy-btn');
        const count = await copyButtons.count();
        expect(count).toBeGreaterThan(0);
      }
    });
  });
});
