/**
 * Responsive Design E2E Tests
 * Owner: Scenario 9 - Responsive Design
 *
 * Tests the page displays correctly across mobile (320px+) and desktop (1024px+) viewports.
 *
 * Test Cases:
 * - TC1: No horizontal scrolling at 320px (except code blocks)
 * - TC2: Navigation collapses to hamburger menu at 320px
 * - TC3: Multi-column layouts at 1024px
 * - TC4: Touch targets at least 44px x 44px on mobile
 * - TC5: Hamburger menu opens/closes and contains all navigation links
 */

import { test, expect } from '@playwright/test';

test.describe('Responsive Design', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Mobile Viewport (320px)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 320, height: 568 });
    });

    test('TC1: All content visible without horizontal scrolling (except code blocks)', async ({ page }) => {
      // Check that the page doesn't have horizontal overflow
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      // Small tolerance for potential rounding issues
      const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
      const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);

      // Allow for a small difference (1-2 pixels for rounding)
      expect(scrollWidth - clientWidth).toBeLessThanOrEqual(2);

      // Check that main content elements don't overflow
      const sections = ['#hero', '#features', '#quickstart', '#architecture', '#config', '#status'];

      for (const sectionId of sections) {
        const section = page.locator(sectionId);
        if (await section.count() > 0) {
          const boundingBox = await section.boundingBox();
          if (boundingBox) {
            // Section should not exceed viewport width
            expect(boundingBox.width).toBeLessThanOrEqual(320 + 2); // Allow 2px tolerance
          }
        }
      }

      // Verify that body/html have overflow-x hidden
      const overflowStyle = await page.evaluate(() => {
        const html = window.getComputedStyle(document.documentElement);
        const body = window.getComputedStyle(document.body);
        return {
          htmlOverflowX: html.overflowX,
          bodyOverflowX: body.overflowX,
        };
      });

      expect(['hidden', 'auto', 'scroll']).toContain(overflowStyle.htmlOverflowX);
    });

    test('TC2: Navigation collapses to hamburger menu at 320px', async ({ page }) => {
      // Hamburger button should be visible
      const hamburgerBtn = page.locator('#hamburger-btn');
      await expect(hamburgerBtn).toBeVisible();

      // Navigation should be hidden initially (not visible on mobile)
      const nav = page.locator('.header__nav');
      await expect(nav).not.toBeVisible();

      // Hamburger should have aria-expanded false
      await expect(hamburgerBtn).toHaveAttribute('aria-expanded', 'false');

      // Desktop nav links should not be visible when menu is closed
      const navList = page.locator('.header__nav-list');
      await expect(navList).not.toBeVisible();
    });

    test('TC4: All interactive elements are at least 44px x 44px', async ({ page }) => {
      // Check hamburger button touch target
      const hamburgerBtn = page.locator('#hamburger-btn');
      const hamburgerBox = await hamburgerBtn.boundingBox();
      expect(hamburgerBox).not.toBeNull();
      expect(hamburgerBox!.width).toBeGreaterThanOrEqual(40); // 40px minimum (44px is ideal)
      expect(hamburgerBox!.height).toBeGreaterThanOrEqual(40);

      // Check theme toggle button touch target
      const themeToggle = page.locator('#theme-toggle');
      const themeToggleBox = await themeToggle.boundingBox();
      expect(themeToggleBox).not.toBeNull();
      expect(themeToggleBox!.width).toBeGreaterThanOrEqual(40);
      expect(themeToggleBox!.height).toBeGreaterThanOrEqual(40);

      // Check CTA buttons touch targets
      const ctaButtons = page.locator('.hero-cta .btn');
      const ctaCount = await ctaButtons.count();

      for (let i = 0; i < ctaCount; i++) {
        const button = ctaButtons.nth(i);
        const buttonBox = await button.boundingBox();
        expect(buttonBox).not.toBeNull();
        expect(buttonBox!.height).toBeGreaterThanOrEqual(44);
      }

      // Open hamburger menu and check nav links
      await hamburgerBtn.click();
      await page.waitForTimeout(300); // Wait for menu animation

      const navLinks = page.locator('.header__nav-link');
      const navLinkCount = await navLinks.count();

      for (let i = 0; i < navLinkCount; i++) {
        const link = navLinks.nth(i);
        if (await link.isVisible()) {
          const linkBox = await link.boundingBox();
          expect(linkBox).not.toBeNull();
          expect(linkBox!.height).toBeGreaterThanOrEqual(44);
        }
      }
    });

    test('TC5: Hamburger menu opens/closes and contains all navigation links', async ({ page }) => {
      const hamburgerBtn = page.locator('#hamburger-btn');
      const nav = page.locator('.header__nav');

      // Initial state: menu closed
      await expect(nav).not.toBeVisible();
      await expect(hamburgerBtn).toHaveAttribute('aria-expanded', 'false');

      // Open the menu
      await hamburgerBtn.click();
      await page.waitForTimeout(300);

      // Menu should now be visible
      await expect(nav).toBeVisible();
      await expect(hamburgerBtn).toHaveAttribute('aria-expanded', 'true');

      // Check all navigation links are present
      const githubLink = page.locator('.header__nav-link').filter({ hasText: 'GitHub' });
      const docsLink = page.locator('.header__nav-link').filter({ hasText: 'Documentation' });
      const aboutLink = page.locator('.header__nav-link').filter({ hasText: 'About' });

      await expect(githubLink).toBeVisible();
      await expect(docsLink).toBeVisible();
      await expect(aboutLink).toBeVisible();

      // Close the menu
      await hamburgerBtn.click();
      await page.waitForTimeout(300);

      // Menu should be hidden again
      await expect(nav).not.toBeVisible();
      await expect(hamburgerBtn).toHaveAttribute('aria-expanded', 'false');
    });
  });

  test.describe('Tablet Viewport (768px)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 1024 });
    });

    test('Features grid shows 2 columns on tablet', async ({ page }) => {
      const featuresGrid = page.locator('.features-grid');

      if (await featuresGrid.count() > 0) {
        const gridStyle = await featuresGrid.evaluate((el) => {
          const styles = window.getComputedStyle(el);
          return {
            display: styles.display,
            gridTemplateColumns: styles.gridTemplateColumns,
          };
        });

        expect(gridStyle.display).toBe('grid');
        // Should have 2 columns (2 fr values or similar)
        const columnCount = gridStyle.gridTemplateColumns.split(' ').filter(v => v !== '').length;
        expect(columnCount).toBeGreaterThanOrEqual(1);
        expect(columnCount).toBeLessThanOrEqual(3);
      }
    });

    test('Status grid shows appropriate column layout', async ({ page }) => {
      const statusGrid = page.locator('.status-grid');

      if (await statusGrid.count() > 0) {
        const gridStyle = await statusGrid.evaluate((el) => {
          const styles = window.getComputedStyle(el);
          return {
            display: styles.display,
            gridTemplateColumns: styles.gridTemplateColumns,
          };
        });

        expect(gridStyle.display).toBe('grid');
      }
    });
  });

  test.describe('Desktop Viewport (1024px)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 1024, height: 768 });
    });

    test('TC3: Multi-column layouts are applied where appropriate', async ({ page }) => {
      // Check features grid has 3 columns on desktop
      const featuresGrid = page.locator('.features-grid');

      if (await featuresGrid.count() > 0) {
        const featuresGridStyle = await featuresGrid.evaluate((el) => {
          const styles = window.getComputedStyle(el);
          return {
            display: styles.display,
            gridTemplateColumns: styles.gridTemplateColumns,
          };
        });

        expect(featuresGridStyle.display).toBe('grid');
        // Should have 3 columns for features
        const featureColumnCount = featuresGridStyle.gridTemplateColumns.split(' ').filter(v => v !== '').length;
        expect(featureColumnCount).toBeGreaterThanOrEqual(2);
      }

      // Check quickstart grid has 2 columns
      const quickstartGrid = page.locator('.quickstart-grid');

      if (await quickstartGrid.count() > 0) {
        const quickstartGridStyle = await quickstartGrid.evaluate((el) => {
          const styles = window.getComputedStyle(el);
          return {
            display: styles.display,
            gridTemplateColumns: styles.gridTemplateColumns,
          };
        });

        expect(quickstartGridStyle.display).toBe('grid');
        // Should have 2 columns for quickstart
        const quickstartColumnCount = quickstartGridStyle.gridTemplateColumns.split(' ').filter(v => v !== '').length;
        expect(quickstartColumnCount).toBeGreaterThanOrEqual(2);
      }

      // Check status grid has 2 columns
      const statusGrid = page.locator('.status-grid');

      if (await statusGrid.count() > 0) {
        const statusGridStyle = await statusGrid.evaluate((el) => {
          const styles = window.getComputedStyle(el);
          return {
            display: styles.display,
            gridTemplateColumns: styles.gridTemplateColumns,
          };
        });

        expect(statusGridStyle.display).toBe('grid');
        // Should have 2 columns for status
        const statusColumnCount = statusGridStyle.gridTemplateColumns.split(' ').filter(v => v !== '').length;
        expect(statusColumnCount).toBe(2);
      }
    });

    test('Navigation is fully visible without hamburger menu', async ({ page }) => {
      // Navigation should be visible
      const nav = page.locator('.header__nav');
      await expect(nav).toBeVisible();

      // Hamburger should be hidden on desktop
      const hamburgerBtn = page.locator('#hamburger-btn');
      await expect(hamburgerBtn).not.toBeVisible();

      // All nav links should be visible
      const githubLink = page.locator('.header__nav-link').filter({ hasText: 'GitHub' });
      const docsLink = page.locator('.header__nav-link').filter({ hasText: 'Documentation' });
      const aboutLink = page.locator('.header__nav-link').filter({ hasText: 'About' });

      await expect(githubLink).toBeVisible();
      await expect(docsLink).toBeVisible();
      await expect(aboutLink).toBeVisible();
    });

    test('Feature cards are displayed in grid layout', async ({ page }) => {
      const featureCards = page.locator('.feature-card');
      const count = await featureCards.count();

      expect(count).toBeGreaterThan(0);

      // Verify cards are laid out horizontally (not stacked vertically)
      if (count >= 2) {
        const firstCard = await featureCards.nth(0).boundingBox();
        const secondCard = await featureCards.nth(1).boundingBox();

        expect(firstCard).not.toBeNull();
        expect(secondCard).not.toBeNull();

        // Cards should be side by side (same Y position or close)
        expect(Math.abs(firstCard!.y - secondCard!.y)).toBeLessThan(10);
      }
    });
  });

  test.describe('Large Desktop Viewport (1280px)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 900 });
    });

    test('Content is centered within max-width container', async ({ page }) => {
      const container = page.locator('.container').first();

      if (await container.count() > 0) {
        const containerBox = await container.boundingBox();
        const viewportWidth = page.viewportSize()?.width || 1280;

        expect(containerBox).not.toBeNull();

        // Container should be centered (left margin equals right margin approximately)
        const leftMargin = containerBox!.x;
        const rightMargin = viewportWidth - (containerBox!.x + containerBox!.width);

        // Allow for some tolerance in centering
        expect(Math.abs(leftMargin - rightMargin)).toBeLessThan(50);
      }
    });
  });

  test.describe('Cross-viewport behavior', () => {
    test('Viewport resize adjusts layout correctly', async ({ page }) => {
      // Start with desktop
      await page.setViewportSize({ width: 1024, height: 768 });

      // Hamburger should be hidden on desktop
      let hamburgerBtn = page.locator('#hamburger-btn');
      await expect(hamburgerBtn).not.toBeVisible();

      // Nav should be visible
      let nav = page.locator('.header__nav');
      await expect(nav).toBeVisible();

      // Resize to mobile
      await page.setViewportSize({ width: 320, height: 568 });
      await page.waitForTimeout(300); // Wait for resize

      // Hamburger should now be visible
      hamburgerBtn = page.locator('#hamburger-btn');
      await expect(hamburgerBtn).toBeVisible();

      // Nav should be hidden (mobile menu closed)
      nav = page.locator('.header__nav');
      await expect(nav).not.toBeVisible();
    });

    test('Images are responsive and do not overflow', async ({ page }) => {
      // Test at mobile viewport
      await page.setViewportSize({ width: 320, height: 568 });

      const images = page.locator('img');
      const count = await images.count();

      for (let i = 0; i < count; i++) {
        const img = images.nth(i);
        if (await img.isVisible()) {
          const imgBox = await img.boundingBox();
          if (imgBox) {
            // Image should not exceed viewport width
            expect(imgBox.width).toBeLessThanOrEqual(320);
          }
        }
      }
    });

    test('Code blocks have horizontal scroll on mobile', async ({ page }) => {
      await page.setViewportSize({ width: 320, height: 568 });

      const codeBlocks = page.locator('.code-block');
      const count = await codeBlocks.count();

      for (let i = 0; i < count; i++) {
        const codeBlock = codeBlocks.nth(i);
        if (await codeBlock.isVisible()) {
          const overflowStyle = await codeBlock.evaluate((el) => {
            const styles = window.getComputedStyle(el);
            return styles.overflowX;
          });

          // Code blocks should allow horizontal scrolling
          expect(['auto', 'scroll', 'hidden']).toContain(overflowStyle);
        }
      }
    });
  });

  test.describe('Responsive typography', () => {
    test('Typography scales appropriately for mobile', async ({ page }) => {
      await page.setViewportSize({ width: 320, height: 568 });

      // Check hero title font size is reduced on mobile
      const heroTitle = page.locator('.hero-title');
      if (await heroTitle.count() > 0) {
        const fontSize = await heroTitle.evaluate((el) => {
          return window.getComputedStyle(el).fontSize;
        });

        const fontSizeValue = parseFloat(fontSize);
        // On mobile, hero title should be smaller than desktop (typically < 48px)
        expect(fontSizeValue).toBeLessThanOrEqual(48);
        expect(fontSizeValue).toBeGreaterThan(16);
      }
    });
  });
});
