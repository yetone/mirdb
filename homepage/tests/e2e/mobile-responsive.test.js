/**
 * Mobile Responsiveness E2E Tests
 * Owner: Scenario 10 - Mobile Responsiveness
 *
 * Test cases:
 * - No horizontal scroll at 320px
 * - Content readable at 375px
 * - Features stack vertically on mobile
 * - Code blocks horizontally scrollable
 * - Touch targets minimum 44x44px
 */

import { test, expect } from '@playwright/test';

test.describe('Mobile Responsiveness', () => {
  test.describe('Test Case 1: No horizontal scroll at 320px viewport width', () => {
    test('Page renders without horizontal scrollbar on body at 320px', async ({ page }) => {
      // Set viewport to minimum supported mobile width
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');

      // Wait for page to fully load
      await page.waitForLoadState('domcontentloaded');

      // Check that body does not have horizontal scrollbar
      // The key metric is whether documentElement.scrollWidth exceeds clientWidth
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasHorizontalScroll).toBe(false);

      // Additionally verify the body doesn't overflow significantly
      // Due to browser rendering, scrollWidth may be slightly larger,
      // but visible content should not exceed viewport
      const overflow = await page.evaluate(() => {
        const body = document.body;
        const style = window.getComputedStyle(body);
        // Check if overflow-x is hidden or auto (allowing scroll within elements)
        return style.overflowX;
      });

      // Body should have overflow-x: hidden to prevent horizontal scroll
      expect(['hidden', 'auto']).toContain(overflow);
    });

    test('All sections fit within 320px viewport', async ({ page }) => {
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Check all main sections
      const sections = ['#hero', '#features', '#quickstart', '#architecture', '#protocol', '#status'];

      for (const section of sections) {
        const sectionElement = page.locator(section);
        if (await sectionElement.count() > 0) {
          const boundingBox = await sectionElement.boundingBox();
          if (boundingBox) {
            expect(boundingBox.width).toBeLessThanOrEqual(320);
          }
        }
      }
    });
  });

  test.describe('Test Case 2: Content readable at 375px (iPhone)', () => {
    test('All sections are readable and properly stacked at 375px', async ({ page }) => {
      // iPhone viewport size
      await page.setViewportSize({ width: 375, height: 812 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Verify hero section is visible
      const heroTitle = page.locator('.hero-title');
      await expect(heroTitle).toBeVisible();

      // Verify hero headline is visible
      const heroHeadline = page.locator('.hero-headline');
      await expect(heroHeadline).toBeVisible();

      // Verify hero subheadline is readable
      const heroSubheadline = page.locator('.hero-subheadline');
      await expect(heroSubheadline).toBeVisible();

      // Verify features section is visible
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      // Verify quickstart section is visible
      const quickstartSection = page.locator('#quickstart');
      await expect(quickstartSection).toBeVisible();

      // Verify sections don't overflow horizontally
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);
    });

    test('Typography is readable at mobile viewport', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 812 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Check that hero title has appropriate font size for mobile
      const heroTitleFontSize = await page.locator('.hero-title').evaluate((el) => {
        return parseInt(window.getComputedStyle(el).fontSize);
      });

      // Font size should be at least 24px for readability
      expect(heroTitleFontSize).toBeGreaterThanOrEqual(24);

      // Check body text font size
      const bodyTextFontSize = await page.locator('.hero-subheadline').evaluate((el) => {
        return parseInt(window.getComputedStyle(el).fontSize);
      });

      // Body text should be at least 14px
      expect(bodyTextFontSize).toBeGreaterThanOrEqual(14);
    });
  });

  test.describe('Test Case 3: Features grid stacks vertically on mobile', () => {
    test('Features stack vertically (single column) on mobile', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 812 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Get the features grid
      const featuresGrid = page.locator('.features-grid');

      // Check grid template columns is single column
      const gridTemplateColumns = await featuresGrid.evaluate((el) => {
        return window.getComputedStyle(el).gridTemplateColumns;
      });

      // Should be a single column (1fr or single pixel value)
      const columnCount = gridTemplateColumns.split(' ').filter(c => c !== '').length;
      expect(columnCount).toBe(1);
    });

    test('Feature cards have appropriate width on mobile', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 812 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Get all feature cards
      const featureCards = page.locator('.feature-card');
      const count = await featureCards.count();

      expect(count).toBeGreaterThan(0);

      // Check each card takes full width (minus padding)
      for (let i = 0; i < count; i++) {
        const card = featureCards.nth(i);
        const boundingBox = await card.boundingBox();
        if (boundingBox) {
          // Card should be at least 280px wide (accounting for container padding)
          expect(boundingBox.width).toBeGreaterThanOrEqual(280);
        }
      }
    });
  });

  test.describe('Test Case 4: Code blocks have horizontal scrolling', () => {
    test('Code blocks have overflow-x: auto for horizontal scrolling', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 812 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Find code blocks
      const codeBlocks = page.locator('.code-block pre');
      const count = await codeBlocks.count();

      expect(count).toBeGreaterThan(0);

      // Check each code block has overflow-x: auto
      for (let i = 0; i < count; i++) {
        const codeBlock = codeBlocks.nth(i);
        const overflowX = await codeBlock.evaluate((el) => {
          return window.getComputedStyle(el).overflowX;
        });

        expect(['auto', 'scroll']).toContain(overflowX);
      }
    });

    test('Code blocks do not break page layout', async ({ page }) => {
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Code blocks should have overflow-x: auto to allow scrolling
      // The key is that code blocks don't cause document-level horizontal scroll
      const codeBlocks = page.locator('.code-block pre');
      const count = await codeBlocks.count();

      expect(count).toBeGreaterThan(0);

      for (let i = 0; i < count; i++) {
        const codeBlock = codeBlocks.nth(i);
        const overflowX = await codeBlock.evaluate((el) => {
          return window.getComputedStyle(el).overflowX;
        });
        // Code block pre should have overflow-x: auto for horizontal scrolling
        expect(['auto', 'scroll']).toContain(overflowX);
      }

      // Verify no horizontal scroll on document level
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);
    });
  });

  test.describe('Test Case 5: Touch targets have minimum 44x44px size', () => {
    test('CTA buttons have minimum 44x44px touch target size', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 812 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Check Get Started button
      const getStartedBtn = page.locator('#cta-get-started');
      const getStartedBox = await getStartedBtn.boundingBox();

      if (getStartedBox) {
        expect(getStartedBox.width).toBeGreaterThanOrEqual(44);
        expect(getStartedBox.height).toBeGreaterThanOrEqual(44);
      }

      // Check View on GitHub button
      const githubBtn = page.locator('#cta-github');
      const githubBox = await githubBtn.boundingBox();

      if (githubBox) {
        expect(githubBox.width).toBeGreaterThanOrEqual(44);
        expect(githubBox.height).toBeGreaterThanOrEqual(44);
      }
    });

    test('Hamburger menu button has minimum touch target size', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 812 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Check hamburger button
      const hamburgerBtn = page.locator('#hamburger-btn');
      const hamburgerBox = await hamburgerBtn.boundingBox();

      if (hamburgerBox) {
        expect(hamburgerBox.width).toBeGreaterThanOrEqual(44);
        expect(hamburgerBox.height).toBeGreaterThanOrEqual(44);
      }
    });

    test('Navigation links have adequate touch target size', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 812 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Open mobile nav
      const hamburgerBtn = page.locator('#hamburger-btn');
      await hamburgerBtn.click();

      // Wait for mobile nav to open
      await page.waitForTimeout(300);

      // Check mobile nav links
      const mobileNavLinks = page.locator('.mobile-nav-link');
      const count = await mobileNavLinks.count();

      for (let i = 0; i < count; i++) {
        const link = mobileNavLinks.nth(i);
        const boundingBox = await link.boundingBox();

        if (boundingBox) {
          // Height should be at least 44px for touch targets
          expect(boundingBox.height).toBeGreaterThanOrEqual(44);
        }
      }
    });

    test('Copy buttons have adequate touch target size', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 812 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Check copy buttons
      const copyBtns = page.locator('.copy-btn');
      const count = await copyBtns.count();

      for (let i = 0; i < count; i++) {
        const btn = copyBtns.nth(i);
        const boundingBox = await btn.boundingBox();

        if (boundingBox) {
          // Copy buttons might be smaller, but should still be tappable
          // Using 32px as minimum since these are secondary actions
          expect(boundingBox.width).toBeGreaterThanOrEqual(32);
          expect(boundingBox.height).toBeGreaterThanOrEqual(32);
        }
      }
    });
  });

  test.describe('Additional Mobile Responsiveness Tests', () => {
    test('Header is properly styled on mobile', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 812 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Check header is visible
      const header = page.locator('.header');
      await expect(header).toBeVisible();

      // Desktop nav links should be hidden
      const navLinks = page.locator('.nav-links');
      const navLinksDisplay = await navLinks.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(navLinksDisplay).toBe('none');

      // Hamburger button should be visible
      const hamburgerBtn = page.locator('#hamburger-btn');
      await expect(hamburgerBtn).toBeVisible();
    });

    test('Footer layout adapts to mobile', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 812 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Scroll to footer
      await page.locator('.footer').scrollIntoViewIfNeeded();

      // Footer should be visible
      const footer = page.locator('.footer');
      await expect(footer).toBeVisible();

      // Footer content should stack on mobile
      const footerContent = page.locator('.footer-content');
      const gridTemplateColumns = await footerContent.evaluate((el) => {
        return window.getComputedStyle(el).gridTemplateColumns;
      });

      // Should be single column on mobile
      const columnCount = gridTemplateColumns.split(' ').filter(c => c !== '').length;
      expect(columnCount).toBe(1);
    });

    test('Protocol table is horizontally scrollable', async ({ page }) => {
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Check protocol table wrapper has overflow-x: auto
      const tableWrapper = page.locator('.protocol-table-wrapper');
      const overflowX = await tableWrapper.evaluate((el) => {
        return window.getComputedStyle(el).overflowX;
      });

      expect(['auto', 'scroll']).toContain(overflowX);
    });

    test('Status grid stacks vertically on mobile', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 812 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Check status grid layout
      const statusGrid = page.locator('.status-grid');
      const gridTemplateColumns = await statusGrid.evaluate((el) => {
        return window.getComputedStyle(el).gridTemplateColumns;
      });

      // Should be single column on mobile
      const columnCount = gridTemplateColumns.split(' ').filter(c => c !== '').length;
      expect(columnCount).toBe(1);
    });
  });
});
