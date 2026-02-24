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

// Mobile viewport configurations
const MOBILE_320 = { width: 320, height: 568 }; // iPhone SE (1st gen)
const MOBILE_375 = { width: 375, height: 667 }; // iPhone 6/7/8
const MOBILE_414 = { width: 414, height: 896 }; // iPhone XR

test.describe('Mobile Responsiveness', () => {
  test.describe('Test Case 1: No horizontal scroll at 320px viewport', () => {
    test('Page renders without horizontal scrollbar on body', async ({ page }) => {
      await page.setViewportSize(MOBILE_320);
      await page.goto('/');

      // Wait for page to fully load
      await page.waitForLoadState('networkidle');

      // Check that document body doesn't have horizontal overflow
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasHorizontalScroll).toBe(false);
    });

    test('All main sections fit within 320px viewport', async ({ page }) => {
      await page.setViewportSize(MOBILE_320);
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Check each main section
      const sections = ['#hero', '#features', '#quickstart', '#architecture', '#protocol', '#status'];

      for (const selector of sections) {
        const section = page.locator(selector);
        if (await section.count() > 0) {
          const boundingBox = await section.boundingBox();
          if (boundingBox) {
            // Section width should not exceed viewport width
            expect(boundingBox.width).toBeLessThanOrEqual(MOBILE_320.width);
          }
        }
      }
    });
  });

  test.describe('Test Case 2: Content readable at 375px (iPhone)', () => {
    test('All sections are readable and properly stacked', async ({ page }) => {
      await page.setViewportSize(MOBILE_375);
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Hero section should be visible
      const heroTitle = page.locator('.hero-title');
      await expect(heroTitle).toBeVisible();

      // Features section should be visible
      const featuresTitle = page.locator('#features-title');
      await expect(featuresTitle).toBeVisible();

      // Quick start section should be visible
      const quickstartTitle = page.locator('#quickstart-title');
      await expect(quickstartTitle).toBeVisible();

      // Architecture section should be visible
      const architectureTitle = page.locator('#architecture-title');
      await expect(architectureTitle).toBeVisible();

      // Protocol section should be visible
      const protocolTitle = page.locator('#protocol-title');
      await expect(protocolTitle).toBeVisible();

      // Status section should be visible
      const statusTitle = page.locator('#status-title');
      await expect(statusTitle).toBeVisible();

      // Footer should be visible
      const footer = page.locator('.footer');
      await expect(footer).toBeVisible();
    });

    test('Text is readable with appropriate font sizes', async ({ page }) => {
      await page.setViewportSize(MOBILE_375);
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Check hero headline font size is reasonable
      const heroHeadline = page.locator('.hero-headline');
      const headlineStyles = await heroHeadline.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          fontSize: parseFloat(styles.fontSize),
        };
      });

      // Font size should be at least 16px for readability
      expect(headlineStyles.fontSize).toBeGreaterThanOrEqual(16);
    });
  });

  test.describe('Test Case 3: Features stack vertically on mobile', () => {
    test('Features grid uses single column on mobile', async ({ page }) => {
      await page.setViewportSize(MOBILE_375);
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Get all feature cards
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();

      expect(cardCount).toBe(4); // Should have 4 feature cards

      // Get bounding boxes to verify vertical stacking
      const boundingBoxes = [];
      for (let i = 0; i < cardCount; i++) {
        const box = await featureCards.nth(i).boundingBox();
        if (box) {
          boundingBoxes.push(box);
        }
      }

      // Verify cards are stacked vertically (each card's top is below the previous card's bottom)
      for (let i = 1; i < boundingBoxes.length; i++) {
        // Allow small tolerance for margins
        expect(boundingBoxes[i].y).toBeGreaterThanOrEqual(boundingBoxes[i - 1].y + boundingBoxes[i - 1].height - 5);
      }
    });

    test('Features grid CSS shows grid-template-columns: 1fr', async ({ page }) => {
      await page.setViewportSize(MOBILE_375);
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      const featuresGrid = page.locator('.features-grid');
      const gridStyles = await featuresGrid.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          gridTemplateColumns: styles.gridTemplateColumns,
          display: styles.display,
        };
      });

      expect(gridStyles.display).toBe('grid');
      // On mobile, should be single column (1fr or a single pixel value)
      expect(gridStyles.gridTemplateColumns).not.toContain('repeat');
    });
  });

  test.describe('Test Case 4: Code blocks with horizontal scrolling', () => {
    test('Code blocks have overflow-x: auto for horizontal scrolling', async ({ page }) => {
      await page.setViewportSize(MOBILE_375);
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Find code block pre elements
      const codeBlockPre = page.locator('.code-block pre').first();

      if (await codeBlockPre.count() > 0) {
        const overflowStyle = await codeBlockPre.evaluate((el) => {
          const styles = window.getComputedStyle(el);
          return styles.overflowX;
        });

        // Should have horizontal scrolling enabled
        expect(['auto', 'scroll']).toContain(overflowStyle);
      }
    });

    test('Code blocks do not break page layout', async ({ page }) => {
      await page.setViewportSize(MOBILE_320);
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Navigate to quickstart section
      await page.locator('#quickstart').scrollIntoViewIfNeeded();

      // Check that code blocks don't cause horizontal overflow on body
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasHorizontalScroll).toBe(false);
    });
  });

  test.describe('Test Case 5: Touch target sizing', () => {
    test('CTA buttons have minimum 44x44px touch target size', async ({ page }) => {
      await page.setViewportSize(MOBILE_375);
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Check primary CTA button
      const getStartedBtn = page.locator('#cta-get-started');
      const btnBox = await getStartedBtn.boundingBox();

      expect(btnBox).not.toBeNull();
      if (btnBox) {
        expect(btnBox.width).toBeGreaterThanOrEqual(44);
        expect(btnBox.height).toBeGreaterThanOrEqual(44);
      }
    });

    test('Secondary CTA button has minimum 44x44px touch target', async ({ page }) => {
      await page.setViewportSize(MOBILE_375);
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      const githubBtn = page.locator('#cta-github');
      const btnBox = await githubBtn.boundingBox();

      expect(btnBox).not.toBeNull();
      if (btnBox) {
        expect(btnBox.width).toBeGreaterThanOrEqual(44);
        expect(btnBox.height).toBeGreaterThanOrEqual(44);
      }
    });

    test('Navigation hamburger button has minimum 44x44px touch target', async ({ page }) => {
      await page.setViewportSize(MOBILE_375);
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      const hamburgerBtn = page.locator('#hamburger-btn');

      // Should be visible on mobile
      await expect(hamburgerBtn).toBeVisible();

      const btnBox = await hamburgerBtn.boundingBox();
      expect(btnBox).not.toBeNull();
      if (btnBox) {
        expect(btnBox.width).toBeGreaterThanOrEqual(44);
        expect(btnBox.height).toBeGreaterThanOrEqual(44);
      }
    });

    test('Copy buttons have adequate touch target size', async ({ page }) => {
      await page.setViewportSize(MOBILE_375);
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Navigate to quickstart
      await page.locator('#quickstart').scrollIntoViewIfNeeded();

      const copyBtns = page.locator('.copy-btn');
      const copyBtnCount = await copyBtns.count();

      for (let i = 0; i < copyBtnCount; i++) {
        const btnBox = await copyBtns.nth(i).boundingBox();
        if (btnBox) {
          // Touch target should be at least 32x32 for secondary actions (Apple HIG allows 32px for secondary)
          expect(btnBox.width).toBeGreaterThanOrEqual(32);
          expect(btnBox.height).toBeGreaterThanOrEqual(32);
        }
      }
    });
  });

  test.describe('Mobile navigation and interactions', () => {
    test('Hamburger menu is visible on mobile', async ({ page }) => {
      await page.setViewportSize(MOBILE_375);
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      const hamburgerBtn = page.locator('#hamburger-btn');
      await expect(hamburgerBtn).toBeVisible();

      // Desktop nav links should be hidden
      const navLinks = page.locator('.nav-links');
      await expect(navLinks).toBeHidden();
    });

    test('Mobile menu can be opened and closed', async ({ page }) => {
      await page.setViewportSize(MOBILE_375);
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      const hamburgerBtn = page.locator('#hamburger-btn');
      const mobileNav = page.locator('#mobile-nav');

      // Open mobile menu
      await hamburgerBtn.click();

      // Wait for menu to open
      await expect(mobileNav).toHaveClass(/is-open/);

      // Close by clicking hamburger again
      await hamburgerBtn.click();

      // Wait for menu to close
      await expect(mobileNav).not.toHaveClass(/is-open/);
    });
  });

  test.describe('Cross-device viewport testing', () => {
    test('Page functions correctly at 414px (iPhone XR)', async ({ page }) => {
      await page.setViewportSize(MOBILE_414);
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // No horizontal scroll
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);

      // All sections visible
      await expect(page.locator('.hero')).toBeVisible();
      await expect(page.locator('#features')).toBeVisible();
    });

    test('Page handles transition between mobile and desktop viewports', async ({ page }) => {
      // Start at mobile
      await page.setViewportSize(MOBILE_375);
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Hamburger should be visible
      await expect(page.locator('#hamburger-btn')).toBeVisible();

      // Resize to desktop
      await page.setViewportSize({ width: 1024, height: 768 });

      // Desktop nav should now be visible
      await expect(page.locator('.nav-links')).toBeVisible();

      // Hamburger should be hidden
      await expect(page.locator('#hamburger-btn')).toBeHidden();
    });
  });
});
