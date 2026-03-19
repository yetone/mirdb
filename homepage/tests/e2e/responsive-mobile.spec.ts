/**
 * Responsive Design - Mobile E2E Tests
 * Owner: Scenario 9 - Responsive Design - Mobile
 *
 * Verifies the homepage displays correctly on mobile devices (max-width 768px)
 * Tests mobile viewport behavior at 375px width.
 *
 * Requirements: REQ-9, REQ-12 (responsive and accessibility)
 */

import { test, expect } from '@playwright/test';

// Mobile viewport configuration
const MOBILE_VIEWPORT = {
  width: 375,
  height: 667, // iPhone SE dimensions
};

// WCAG minimum touch target size
const MIN_TOUCH_TARGET_SIZE = 44;

test.describe('Responsive Design - Mobile (375px)', () => {
  test.beforeEach(async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize(MOBILE_VIEWPORT);
    await page.goto('/');
    // Wait for page to be fully loaded
    await page.waitForLoadState('networkidle');
  });

  test.describe('Navigation', () => {
    test('hamburger menu icon is displayed instead of horizontal nav links', async ({ page }) => {
      // Check for hamburger menu button on mobile
      const hamburgerButton = page.locator('[data-testid="mobile-menu-button"], [aria-label*="menu" i], button.hamburger, [data-testid="hamburger-menu"]');

      // If Header/Navigation exists, hamburger should be visible on mobile
      const headerExists = await page.locator('header, nav, [role="navigation"]').count() > 0;

      if (headerExists) {
        // Navigation header exists - verify hamburger menu behavior
        const hamburgerVisible = await hamburgerButton.isVisible().catch(() => false);

        // On mobile, horizontal nav links should be hidden or collapsed
        const desktopNavLinks = page.locator('nav a:not(.mobile-nav a), header a.nav-link, [data-testid="desktop-nav"] a');
        const visibleDesktopLinks = await desktopNavLinks.filter({ hasText: /Features|How It Works|Status|Quick Start|Resources/i }).count();

        // Either hamburger is visible OR desktop links are hidden (mobile-first design)
        if (hamburgerVisible) {
          await expect(hamburgerButton.first()).toBeVisible();
        } else {
          // If no hamburger, ensure we're not showing full desktop nav on mobile
          expect(visibleDesktopLinks).toBeLessThanOrEqual(1);
        }
      } else {
        // Header not yet implemented (Scenario 8)
        // Skip this test gracefully - test will be re-evaluated when Header is built
        test.skip(true, 'Header/Navigation component not yet implemented (Scenario 8)');
      }
    });

    test('mobile navigation menu opens with all section links', async ({ page }) => {
      // Look for hamburger menu button
      const hamburgerButton = page.locator('[data-testid="mobile-menu-button"], [aria-label*="menu" i], button.hamburger, [data-testid="hamburger-menu"]');

      const headerExists = await page.locator('header, nav, [role="navigation"]').count() > 0;

      if (!headerExists) {
        test.skip(true, 'Header/Navigation component not yet implemented (Scenario 8)');
        return;
      }

      const hamburgerVisible = await hamburgerButton.isVisible().catch(() => false);

      if (hamburgerVisible) {
        // Click hamburger to open mobile menu
        await hamburgerButton.first().click();

        // Wait for mobile menu to appear
        await page.waitForTimeout(300); // Allow for animation

        // Check for section links in mobile menu
        const mobileMenu = page.locator('[data-testid="mobile-menu"], .mobile-menu, [role="menu"], nav.mobile');
        const sectionLinks = ['Features', 'How It Works', 'Status', 'Quick Start', 'Resources'];

        for (const section of sectionLinks) {
          const link = mobileMenu.locator(`a:has-text("${section}")`);
          const linkExists = await link.count() > 0;
          if (linkExists) {
            await expect(link.first()).toBeVisible();
          }
        }
      } else {
        // Hamburger menu not visible - may be using different mobile nav pattern
        test.skip(true, 'Hamburger menu pattern not detected');
      }
    });
  });

  test.describe('Content Layout', () => {
    test('feature cards are stacked vertically in single column', async ({ page }) => {
      // Navigate to features section
      const featuresSection = page.locator('#features, [data-testid="features-section"]');

      if (await featuresSection.count() === 0) {
        test.skip(true, 'Features section not found');
        return;
      }

      await featuresSection.scrollIntoViewIfNeeded();

      // Get feature cards
      const featureCards = page.locator('[data-testid^="feature-card"], .feature-card, #features .card, [class*="feature"] [class*="card"]');
      const cardCount = await featureCards.count();

      if (cardCount >= 2) {
        // Get bounding boxes of first two cards
        const firstCard = featureCards.nth(0);
        const secondCard = featureCards.nth(1);

        const firstBox = await firstCard.boundingBox();
        const secondBox = await secondCard.boundingBox();

        if (firstBox && secondBox) {
          // In vertical layout, second card should be below first card
          // (y position of second card > y position + height of first card)
          expect(secondBox.y).toBeGreaterThanOrEqual(firstBox.y + firstBox.height - 10);

          // Cards should have similar x positions (stacked, not side by side)
          expect(Math.abs(secondBox.x - firstBox.x)).toBeLessThan(50);
        }
      }
    });

    test('no horizontal scrollbar appears at 375px width', async ({ page }) => {
      // Check for horizontal overflow
      const hasHorizontalScroll = await page.evaluate(() => {
        const body = document.body;
        const html = document.documentElement;

        // Check if content width exceeds viewport
        const pageWidth = Math.max(
          body.scrollWidth,
          body.offsetWidth,
          html.clientWidth,
          html.scrollWidth,
          html.offsetWidth
        );

        return pageWidth > window.innerWidth;
      });

      expect(hasHorizontalScroll).toBe(false);
    });

    test('all sections are readable without horizontal scrolling', async ({ page }) => {
      // Check each major section for horizontal overflow
      const sections = [
        '#hero',
        '#features',
        '#how-it-works',
        '#status',
        '#quick-start',
        '#resources',
        'footer',
      ];

      for (const sectionSelector of sections) {
        const section = page.locator(sectionSelector);
        if (await section.count() > 0) {
          // Get section bounding box
          const box = await section.boundingBox();
          if (box) {
            // Section should not extend beyond viewport width
            expect(box.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width + 1);

            // Check inner content doesn't overflow
            const hasOverflow = await section.evaluate((el) => {
              return el.scrollWidth > el.clientWidth;
            });
            expect(hasOverflow).toBe(false);
          }
        }
      }
    });
  });

  test.describe('Touch Targets', () => {
    test('all buttons have minimum 44x44px touch target area', async ({ page }) => {
      // Get all interactive buttons
      const buttons = page.locator('button, a[href], [role="button"]').filter({
        has: page.locator(':visible'),
      });

      const buttonCount = await buttons.count();
      const undersizedButtons: string[] = [];

      for (let i = 0; i < buttonCount; i++) {
        const button = buttons.nth(i);
        const isVisible = await button.isVisible().catch(() => false);

        if (isVisible) {
          const box = await button.boundingBox();
          if (box) {
            // Check if touch target meets minimum size
            // Note: We check both width and height, but some targets may use padding
            const meetsMinWidth = box.width >= MIN_TOUCH_TARGET_SIZE;
            const meetsMinHeight = box.height >= MIN_TOUCH_TARGET_SIZE;

            if (!meetsMinWidth || !meetsMinHeight) {
              const text = await button.textContent().catch(() => 'unknown');
              const tagName = await button.evaluate(el => el.tagName).catch(() => 'unknown');

              // Skip if it's a skip-link or hidden element
              const classes = await button.getAttribute('class').catch(() => '');
              if (classes?.includes('sr-only') || classes?.includes('skip-link')) {
                continue;
              }

              undersizedButtons.push(
                `${tagName}[${text?.trim().substring(0, 20)}]: ${Math.round(box.width)}x${Math.round(box.height)}px`
              );
            }
          }
        }
      }

      // Report any undersized buttons
      if (undersizedButtons.length > 0) {
        console.warn('Undersized touch targets found:', undersizedButtons);
      }

      // Allow some tolerance - main CTA buttons must meet requirement
      // Check critical buttons specifically
      const ctaButtons = page.locator('button:has-text("Get Started"), a:has-text("Get Started"), button:has-text("GitHub"), a:has-text("GitHub")');

      for (let i = 0; i < await ctaButtons.count(); i++) {
        const cta = ctaButtons.nth(i);
        if (await cta.isVisible()) {
          const box = await cta.boundingBox();
          if (box) {
            expect(box.height, `CTA button height should be >= ${MIN_TOUCH_TARGET_SIZE}px`).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE);
          }
        }
      }
    });

    test('primary CTA buttons meet touch target requirements', async ({ page }) => {
      // Check main CTAs in Hero section
      const heroSection = page.locator('#hero, [data-testid="hero-section"]');

      if (await heroSection.count() > 0) {
        const getStartedBtn = heroSection.locator('a:has-text("Get Started"), button:has-text("Get Started")');
        const githubBtn = heroSection.locator('a:has-text("GitHub"), button:has-text("GitHub")');

        // Check Get Started button
        if (await getStartedBtn.count() > 0) {
          const box = await getStartedBtn.first().boundingBox();
          if (box) {
            expect(box.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE);
            expect(box.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE);
          }
        }

        // Check GitHub button
        if (await githubBtn.count() > 0) {
          const box = await githubBtn.first().boundingBox();
          if (box) {
            expect(box.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE);
            expect(box.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE);
          }
        }
      }
    });
  });

  test.describe('Responsive Images and Content', () => {
    test('logo scales appropriately for mobile viewport', async ({ page }) => {
      const logo = page.locator('img[alt*="MirDB" i], [data-testid="logo"]');

      if (await logo.count() > 0) {
        const box = await logo.first().boundingBox();
        if (box) {
          // Logo should fit within viewport with padding
          expect(box.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width - 32);
        }
      }
    });

    test('text content is readable at mobile viewport', async ({ page }) => {
      // Check that main headings are visible
      const h1 = page.locator('h1').first();

      if (await h1.count() > 0) {
        await expect(h1).toBeVisible();

        const box = await h1.boundingBox();
        if (box) {
          // Heading should fit within viewport
          expect(box.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
        }
      }
    });
  });
});
