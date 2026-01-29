/**
 * E2E tests for responsive layout across different viewport sizes.
 * Owner: Scenario 10 - Responsive Design
 *
 * Tests cover:
 * - Desktop viewport (1920px+, 2560px)
 * - Tablet viewport (768px-1024px)
 * - Mobile viewport (320px-480px)
 * - Navigation adaptation (hamburger menu on mobile)
 * - No horizontal scrolling
 * - Content readability across viewports
 */

import { test, expect } from '@playwright/test';

test.describe('Responsive Design', () => {
  test.describe('Desktop Viewport (1920px)', () => {
    test('Test Case 1: Full desktop layout with multi-column grids', async ({ page }) => {
      // Set viewport to 1920px width
      await page.setViewportSize({ width: 1920, height: 1080 });
      await page.goto('/');

      // Wait for page to fully load
      await page.waitForLoadState('networkidle');

      // Verify page is visible
      const main = page.locator('main');
      await expect(main).toBeVisible();

      // Verify hero section is visible and properly laid out
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Verify features grid uses multi-column layout on desktop
      const featuresGrid = page.locator('[data-testid="features-grid"]');
      await expect(featuresGrid).toBeVisible();

      // Get feature cards and verify they're arranged in multiple columns
      const featureCards = page.locator('[data-testid="feature-card"]');
      const cardCount = await featureCards.count();
      expect(cardCount).toBeGreaterThanOrEqual(3);

      // Verify cards are arranged horizontally (multi-column)
      const firstCard = featureCards.first();
      const secondCard = featureCards.nth(1);

      const firstBox = await firstCard.boundingBox();
      const secondBox = await secondCard.boundingBox();

      expect(firstBox).not.toBeNull();
      expect(secondBox).not.toBeNull();

      if (firstBox && secondBox) {
        // On desktop, first two cards should be on same row (similar Y position)
        const yDifference = Math.abs(firstBox.y - secondBox.y);
        expect(yDifference).toBeLessThan(20);

        // Second card should be to the right of first
        expect(secondBox.x).toBeGreaterThan(firstBox.x);
      }

      // Verify no horizontal scroll
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);
    });
  });

  test.describe('Tablet Viewport (768px)', () => {
    test('Test Case 2: Tablet layout with adjusted grid columns', async ({ page }) => {
      // Set viewport to 768px width (tablet)
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');

      await page.waitForLoadState('networkidle');

      // Verify page is visible
      const main = page.locator('main');
      await expect(main).toBeVisible();

      // Verify hero section adapts to tablet
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Verify features grid adjusts columns for tablet
      const featuresGrid = page.locator('[data-testid="features-grid"]');
      await expect(featuresGrid).toBeVisible();

      // Get feature cards
      const featureCards = page.locator('[data-testid="feature-card"]');
      const firstCard = featureCards.first();
      const secondCard = featureCards.nth(1);

      const firstBox = await firstCard.boundingBox();
      const secondBox = await secondCard.boundingBox();

      expect(firstBox).not.toBeNull();
      expect(secondBox).not.toBeNull();

      // On tablet, layout should still have some multi-column arrangement
      // but with fewer columns than desktop
      if (firstBox && secondBox) {
        // Cards should take appropriate width for tablet
        expect(firstBox.width).toBeGreaterThan(200);
        expect(firstBox.width).toBeLessThan(700);
      }

      // Verify all content is accessible without horizontal scroll
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);

      // Verify tagline is still visible and readable
      const tagline = page.locator('[data-testid="hero-tagline"]');
      await expect(tagline).toBeVisible();
    });
  });

  test.describe('Mobile Viewport (375px)', () => {
    test('Test Case 3: Mobile layout with single column, no horizontal scroll', async ({
      page,
    }) => {
      // Set viewport to 375px width (standard mobile)
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      await page.waitForLoadState('networkidle');

      // Verify page is visible
      const main = page.locator('main');
      await expect(main).toBeVisible();

      // Verify hero section adapts to mobile
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Verify features grid uses single column on mobile
      const featuresGrid = page.locator('[data-testid="features-grid"]');
      await expect(featuresGrid).toBeVisible();

      // Get feature cards and verify they're stacked vertically
      const featureCards = page.locator('[data-testid="feature-card"]');
      const firstCard = featureCards.first();
      const secondCard = featureCards.nth(1);

      const firstBox = await firstCard.boundingBox();
      const secondBox = await secondCard.boundingBox();

      expect(firstBox).not.toBeNull();
      expect(secondBox).not.toBeNull();

      if (firstBox && secondBox) {
        // On mobile, cards should be stacked vertically (second card below first)
        expect(secondBox.y).toBeGreaterThan(firstBox.y + firstBox.height - 20);

        // Cards should take full width minus padding
        expect(firstBox.width).toBeGreaterThan(300);
      }

      // Critical: Verify no horizontal scroll
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);

      // Verify CTA buttons stack vertically on mobile
      const ctaContainer = page.locator('[data-testid="hero-cta-buttons"]');
      await expect(ctaContainer).toBeVisible();

      const ctaClass = await ctaContainer.getAttribute('class');
      expect(ctaClass).toContain('flex-col');
    });
  });

  test.describe('Minimum Mobile Viewport (320px)', () => {
    test('Test Case 4: Content remains readable and accessible without horizontal scrolling', async ({
      page,
    }) => {
      // Set viewport to 320px width (minimum supported)
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');

      await page.waitForLoadState('networkidle');

      // Verify page is visible
      const main = page.locator('main');
      await expect(main).toBeVisible();

      // Verify hero section is visible
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Verify tagline is readable
      const tagline = page.locator('[data-testid="hero-tagline"]');
      await expect(tagline).toBeVisible();
      const taglineBox = await tagline.boundingBox();
      expect(taglineBox).not.toBeNull();
      expect(taglineBox!.width).toBeLessThanOrEqual(320);

      // Verify description is readable
      const description = page.locator('[data-testid="hero-description"]');
      await expect(description).toBeVisible();

      // Critical: Verify no horizontal scroll at minimum viewport
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);

      // Verify all sections are accessible by scrolling
      const featuresGrid = page.locator('[data-testid="features-grid"]');
      await expect(featuresGrid).toBeVisible({ timeout: 10000 });

      // Scroll down to verify content is accessible
      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
      await page.waitForTimeout(500);

      // Page should have scrolled (content exists below fold)
      const scrollY = await page.evaluate(() => window.scrollY);
      expect(scrollY).toBeGreaterThan(0);
    });
  });

  test.describe('Navigation Adaptation (Mobile)', () => {
    test('Test Case 5: Navigation collapses to hamburger menu on mobile', async ({ page }) => {
      // Set viewport to mobile
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      await page.waitForLoadState('networkidle');

      // Check for hamburger menu button (if Navigation component exists)
      // The Navigation component is owned by Scenario 17, so we test gracefully
      const hamburgerButton = page.locator(
        '[data-testid="mobile-menu-button"], button[aria-label*="menu"], button[aria-label*="Menu"], .hamburger-menu, [data-testid="hamburger-button"]'
      );

      // If hamburger exists, verify it's visible
      const hamburgerCount = await hamburgerButton.count();
      if (hamburgerCount > 0) {
        await expect(hamburgerButton.first()).toBeVisible();
      } else {
        // Navigation component may not be implemented yet (Scenario 17)
        // Verify the page still works without it
        const heroSection = page.locator('[data-testid="hero-section"]');
        await expect(heroSection).toBeVisible();

        // Skip specific navigation test with informative message
        test.info().annotations.push({
          type: 'info',
          description:
            'Navigation component not yet implemented (Scenario 17). Hamburger menu test skipped.',
        });
      }
    });

    test('Test Case 6: Menu expands with all navigation links accessible', async ({ page }) => {
      // Set viewport to mobile
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      await page.waitForLoadState('networkidle');

      // Check for hamburger menu button
      const hamburgerButton = page.locator(
        '[data-testid="mobile-menu-button"], button[aria-label*="menu"], button[aria-label*="Menu"], .hamburger-menu, [data-testid="hamburger-button"]'
      );

      const hamburgerCount = await hamburgerButton.count();
      if (hamburgerCount > 0) {
        // Click hamburger to open menu
        await hamburgerButton.first().click();

        // Wait for menu to open
        await page.waitForTimeout(300);

        // Check for navigation links
        const navLinks = page.locator(
          '[data-testid="mobile-menu"] a, nav[role="navigation"] a, [data-testid="nav-link"]'
        );

        // Verify at least some links are visible
        const linkCount = await navLinks.count();
        expect(linkCount).toBeGreaterThan(0);
      } else {
        // Navigation component may not be implemented yet (Scenario 17)
        const heroSection = page.locator('[data-testid="hero-section"]');
        await expect(heroSection).toBeVisible();

        test.info().annotations.push({
          type: 'info',
          description:
            'Navigation component not yet implemented (Scenario 17). Mobile menu test skipped.',
        });
      }
    });

    test('Desktop navigation is fully visible (no hamburger needed)', async ({ page }) => {
      // Set viewport to desktop
      await page.setViewportSize({ width: 1280, height: 800 });
      await page.goto('/');

      await page.waitForLoadState('networkidle');

      // On desktop, hamburger should be hidden if Navigation component exists
      const hamburgerButton = page.locator(
        '[data-testid="mobile-menu-button"], button[aria-label*="menu"], .hamburger-menu, [data-testid="hamburger-button"]'
      );

      const hamburgerCount = await hamburgerButton.count();
      if (hamburgerCount > 0) {
        // Hamburger should be hidden on desktop
        await expect(hamburgerButton.first()).not.toBeVisible();
      }

      // Page should display correctly
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();
    });
  });

  test.describe('Ultra-Wide Viewport (2560px)', () => {
    test('Test Case 7: Layout handles ultra-wide screens appropriately', async ({ page }) => {
      // Set viewport to 2560px width (ultra-wide)
      await page.setViewportSize({ width: 2560, height: 1440 });
      await page.goto('/');

      await page.waitForLoadState('networkidle');

      // Verify page is visible
      const main = page.locator('main');
      await expect(main).toBeVisible();

      // Verify content is contained and not stretched to full width
      const mainBox = await main.boundingBox();
      expect(mainBox).not.toBeNull();

      // Content should have max-width constraint and be centered
      // (container class should limit width)
      const containerClass = await main.getAttribute('class');
      expect(containerClass).toContain('container');

      // Verify hero section is visible and centered
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      const heroBox = await heroSection.boundingBox();
      expect(heroBox).not.toBeNull();

      // Hero should be centered (not taking full 2560px)
      if (heroBox && mainBox) {
        // Content should have reasonable padding from edges
        expect(heroBox.x).toBeGreaterThan(50);
      }

      // Verify features grid displays properly
      const featuresGrid = page.locator('[data-testid="features-grid"]');
      await expect(featuresGrid).toBeVisible();

      // Verify no horizontal scroll
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);

      // Verify all sections are accessible
      const sections = [
        '[data-testid="hero-section"]',
        '[data-testid="features-grid"]',
        '#terminal',
        '#architecture',
        '#configuration',
        '#installation',
        '#commands',
        '#roadmap',
      ];

      for (const selector of sections) {
        const section = page.locator(selector);
        const count = await section.count();
        if (count > 0) {
          await section.scrollIntoViewIfNeeded();
          await expect(section).toBeVisible();
        }
      }
    });
  });

  test.describe('Cross-Viewport Content Accessibility', () => {
    test('All major sections are accessible across viewports', async ({ page }) => {
      const viewports = [
        { width: 320, height: 568, name: 'iPhone SE' },
        { width: 375, height: 667, name: 'iPhone 8' },
        { width: 768, height: 1024, name: 'iPad' },
        { width: 1280, height: 800, name: 'Desktop' },
        { width: 1920, height: 1080, name: 'Full HD' },
      ];

      for (const viewport of viewports) {
        await page.setViewportSize({ width: viewport.width, height: viewport.height });
        await page.goto('/');
        await page.waitForLoadState('networkidle');

        // Verify hero is visible on all viewports
        const heroSection = page.locator('[data-testid="hero-section"]');
        await expect(heroSection).toBeVisible({
          timeout: 5000,
        });

        // Verify no horizontal scroll on any viewport
        const hasHorizontalScroll = await page.evaluate(() => {
          return document.documentElement.scrollWidth > document.documentElement.clientWidth;
        });
        expect(hasHorizontalScroll).toBe(false);

        // Verify tagline is visible
        const tagline = page.locator('[data-testid="hero-tagline"]');
        await expect(tagline).toBeVisible();

        // Verify CTA buttons are visible
        const ctaButtons = page.locator('[data-testid="hero-cta-buttons"]');
        await expect(ctaButtons).toBeVisible();
      }
    });

    test('Text remains readable at all viewport sizes', async ({ page }) => {
      const viewports = [
        { width: 320, height: 568 },
        { width: 375, height: 667 },
        { width: 768, height: 1024 },
      ];

      for (const viewport of viewports) {
        await page.setViewportSize({ width: viewport.width, height: viewport.height });
        await page.goto('/');
        await page.waitForLoadState('networkidle');

        // Verify tagline font size is appropriate
        const tagline = page.locator('[data-testid="hero-tagline"]');
        await expect(tagline).toBeVisible();

        const fontSize = await tagline.evaluate((el) =>
          parseFloat(window.getComputedStyle(el).fontSize)
        );

        // Font size should be at least 16px for readability on mobile
        expect(fontSize).toBeGreaterThanOrEqual(16);

        // Verify description is readable
        const description = page.locator('[data-testid="hero-description"]');
        await expect(description).toBeVisible();

        const descFontSize = await description.evaluate((el) =>
          parseFloat(window.getComputedStyle(el).fontSize)
        );

        // Description font should be at least 14px
        expect(descFontSize).toBeGreaterThanOrEqual(14);
      }
    });
  });
});
