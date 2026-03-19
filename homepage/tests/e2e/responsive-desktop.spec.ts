/**
 * Responsive Design - Tablet and Desktop E2E Tests
 * Owner: Scenario 10 - Responsive Design - Tablet and Desktop
 *
 * Verifies the homepage displays correctly on tablet (768-1024px) and desktop (1024px+) viewports.
 * Tests viewport behaviors at 768px (tablet), 1440px (desktop), and 1920px (large desktop).
 *
 * Requirements: REQ-9 (responsive design)
 */

import { test, expect } from '@playwright/test';

// Viewport configurations
const TABLET_VIEWPORT = {
  width: 768,
  height: 1024, // iPad dimensions
};

const DESKTOP_VIEWPORT = {
  width: 1440,
  height: 900, // Standard desktop
};

const LARGE_DESKTOP_VIEWPORT = {
  width: 1920,
  height: 1080, // Full HD
};

// Tailwind max-w-6xl is 72rem = 1152px
const MAX_CONTENT_WIDTH = 1152;

test.describe('Responsive Design - Tablet (768px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(TABLET_VIEWPORT);
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('navigation displays horizontal links (no hamburger menu)', async ({ page }) => {
    // At 768px (md breakpoint), horizontal nav links should be visible
    // The Navigation component uses 'hidden md:flex' for desktop nav
    // and 'md:hidden' for hamburger button

    // Check that desktop navigation links are visible
    const desktopNavContainer = page.locator('ul.md\\:flex, nav ul[role="menubar"]');

    // Verify nav links are visible at tablet width
    const navLinks = page.locator('nav a[role="menuitem"], header nav a');
    const visibleNavLinks = await navLinks.filter({ hasText: /Features|How It Works|Status|Quick Start|Resources/i });

    // At 768px, we should see horizontal nav links
    const linkCount = await visibleNavLinks.count();
    expect(linkCount).toBeGreaterThanOrEqual(3);

    // First visible link should be Features
    const featuresLink = page.locator('nav a:has-text("Features")');
    await expect(featuresLink.first()).toBeVisible();

    // Hamburger menu button should be hidden at tablet width (md breakpoint)
    const hamburgerButton = page.locator('button[aria-label*="menu" i], button.md\\:hidden');

    // At 768px (exactly md breakpoint), hamburger should be hidden
    const hamburgerVisible = await hamburgerButton.first().isVisible().catch(() => false);
    expect(hamburgerVisible).toBe(false);
  });

  test('layout adapts appropriately for tablet screen', async ({ page }) => {
    // Features section should be visible
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Get feature cards
    const featureCards = page.locator('[data-testid^="feature-card"], .feature-card');
    const cardCount = await featureCards.count();

    expect(cardCount).toBeGreaterThanOrEqual(3);

    // At 768px (md breakpoint), Features uses md:grid-cols-3
    // So all 3 cards should be in a row (same y position)
    if (cardCount >= 3) {
      const firstCard = featureCards.nth(0);
      const secondCard = featureCards.nth(1);
      const thirdCard = featureCards.nth(2);

      const firstBox = await firstCard.boundingBox();
      const secondBox = await secondCard.boundingBox();
      const thirdBox = await thirdCard.boundingBox();

      if (firstBox && secondBox && thirdBox) {
        // All cards should be on the same row (similar y positions)
        const yTolerance = 50; // Allow some tolerance for alignment
        expect(Math.abs(secondBox.y - firstBox.y)).toBeLessThan(yTolerance);
        expect(Math.abs(thirdBox.y - firstBox.y)).toBeLessThan(yTolerance);

        // Cards should be side by side (x positions increase)
        expect(secondBox.x).toBeGreaterThan(firstBox.x);
        expect(thirdBox.x).toBeGreaterThan(secondBox.x);
      }
    }
  });

  test('content fits within tablet viewport without horizontal scroll', async ({ page }) => {
    const hasHorizontalScroll = await page.evaluate(() => {
      const body = document.body;
      const html = document.documentElement;
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
});

test.describe('Responsive Design - Desktop (1440px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(DESKTOP_VIEWPORT);
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('feature cards display in three-column grid layout', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Get feature cards
    const featureCards = page.locator('[data-testid^="feature-card"], .feature-card');
    const cardCount = await featureCards.count();

    expect(cardCount).toBeGreaterThanOrEqual(3);

    // Get bounding boxes of all three cards
    const firstCard = featureCards.nth(0);
    const secondCard = featureCards.nth(1);
    const thirdCard = featureCards.nth(2);

    const firstBox = await firstCard.boundingBox();
    const secondBox = await secondCard.boundingBox();
    const thirdBox = await thirdCard.boundingBox();

    expect(firstBox).toBeTruthy();
    expect(secondBox).toBeTruthy();
    expect(thirdBox).toBeTruthy();

    if (firstBox && secondBox && thirdBox) {
      // Verify three-column layout:
      // 1. All cards should be on the same row (similar y positions)
      const yTolerance = 10;
      expect(Math.abs(secondBox.y - firstBox.y)).toBeLessThan(yTolerance);
      expect(Math.abs(thirdBox.y - firstBox.y)).toBeLessThan(yTolerance);

      // 2. Cards should be arranged horizontally (increasing x positions)
      expect(secondBox.x).toBeGreaterThan(firstBox.x + firstBox.width / 2);
      expect(thirdBox.x).toBeGreaterThan(secondBox.x + secondBox.width / 2);

      // 3. Cards should have similar widths (proper grid sizing)
      const widthTolerance = 20;
      expect(Math.abs(secondBox.width - firstBox.width)).toBeLessThan(widthTolerance);
      expect(Math.abs(thirdBox.width - firstBox.width)).toBeLessThan(widthTolerance);
    }
  });

  test('navigation shows all horizontal links', async ({ page }) => {
    // All nav links should be visible in horizontal layout
    const navLinks = ['Features', 'How It Works', 'Status', 'Quick Start', 'Resources'];

    for (const linkText of navLinks) {
      const link = page.locator(`nav a:has-text("${linkText}")`);
      await expect(link.first()).toBeVisible();
    }
  });

  test('hero section displays properly at desktop width', async ({ page }) => {
    const heroSection = page.locator('#hero, [data-testid="hero-section"]');

    if (await heroSection.count() > 0) {
      await expect(heroSection).toBeVisible();

      // CTAs should be visible
      const getStartedBtn = page.locator('a:has-text("Get Started"), button:has-text("Get Started")');
      const githubBtn = page.locator('a:has-text("GitHub")');

      await expect(getStartedBtn.first()).toBeVisible();
      await expect(githubBtn.first()).toBeVisible();
    }
  });
});

test.describe('Responsive Design - Large Desktop (1920px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(LARGE_DESKTOP_VIEWPORT);
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('content is centered with max-width constraint (not full-width)', async ({ page }) => {
    // Navigate to features section which has max-w-6xl constraint
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Get the inner container with max-width constraint
    const contentContainer = page.locator('#features .max-w-6xl, #features > div');
    const containerBox = await contentContainer.first().boundingBox();

    expect(containerBox).toBeTruthy();

    if (containerBox) {
      // Container should have max-width applied (not full viewport width)
      // max-w-6xl = 1152px, so container should be <= 1152px + some padding
      expect(containerBox.width).toBeLessThanOrEqual(MAX_CONTENT_WIDTH + 64);

      // Container should NOT span the full viewport width at 1920px
      expect(containerBox.width).toBeLessThan(LARGE_DESKTOP_VIEWPORT.width - 100);

      // Container should be centered (left margin should equal right margin approximately)
      const leftMargin = containerBox.x;
      const rightMargin = LARGE_DESKTOP_VIEWPORT.width - (containerBox.x + containerBox.width);

      // Left and right margins should be approximately equal (centered)
      const marginDifference = Math.abs(leftMargin - rightMargin);
      expect(marginDifference).toBeLessThan(50);
    }
  });

  test('hero section content is centered', async ({ page }) => {
    const heroSection = page.locator('#hero, [data-testid="hero-section"]');

    if (await heroSection.count() > 0) {
      const heroBox = await heroSection.boundingBox();

      if (heroBox) {
        // Hero should span full width but content inside should be centered
        // Check that main heading is centered
        const heading = heroSection.locator('h1');
        if (await heading.count() > 0) {
          const headingBox = await heading.boundingBox();
          if (headingBox) {
            // Heading should be roughly centered on the page
            const headingCenter = headingBox.x + headingBox.width / 2;
            const viewportCenter = LARGE_DESKTOP_VIEWPORT.width / 2;

            // Allow 100px tolerance for centering
            expect(Math.abs(headingCenter - viewportCenter)).toBeLessThan(100);
          }
        }
      }
    }
  });

  test('footer spans full width but content is constrained', async ({ page }) => {
    // Scroll to footer
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(500);

    const footer = page.locator('footer');

    if (await footer.count() > 0) {
      const footerBox = await footer.boundingBox();

      if (footerBox) {
        // Footer should span full width
        expect(footerBox.width).toBeGreaterThanOrEqual(LARGE_DESKTOP_VIEWPORT.width - 10);

        // Check that footer content (like links) is not stretched to edges
        const footerLinks = footer.locator('a').first();
        if (await footerLinks.count() > 0) {
          const linkBox = await footerLinks.boundingBox();
          if (linkBox) {
            // Links should have some margin from the edge
            expect(linkBox.x).toBeGreaterThan(50);
          }
        }
      }
    }
  });

  test('all sections maintain readable line lengths', async ({ page }) => {
    // Check that text paragraphs don't span full viewport width
    const paragraphs = page.locator('p');
    const paragraphCount = await paragraphs.count();

    // Check first few visible paragraphs
    for (let i = 0; i < Math.min(5, paragraphCount); i++) {
      const paragraph = paragraphs.nth(i);
      const isVisible = await paragraph.isVisible().catch(() => false);

      if (isVisible) {
        const box = await paragraph.boundingBox();
        if (box && box.width > 0) {
          // Paragraphs should not be wider than ~80 characters * ~10px = 800px typical
          // Or constrained by container (max-w-6xl = 1152px)
          expect(box.width).toBeLessThan(1200);
        }
      }
    }
  });
});

test.describe('Cross-Viewport Consistency', () => {
  test('navigation remains functional across viewport sizes', async ({ page }) => {
    const viewports = [TABLET_VIEWPORT, DESKTOP_VIEWPORT, LARGE_DESKTOP_VIEWPORT];

    for (const viewport of viewports) {
      await page.setViewportSize(viewport);
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Navigation should be accessible at all sizes
      const nav = page.locator('nav, [role="navigation"]');
      await expect(nav.first()).toBeVisible();

      // Features link should be clickable
      const featuresLink = page.locator('nav a[href="#features"]');
      if (await featuresLink.count() > 0) {
        const isVisible = await featuresLink.first().isVisible();
        expect(isVisible).toBe(true);
      }
    }
  });

  test('feature cards transition smoothly between layouts', async ({ page }) => {
    // Start at tablet
    await page.setViewportSize(TABLET_VIEWPORT);
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    const featureCards = page.locator('[data-testid^="feature-card"], .feature-card');

    // At tablet (768px), should be 3 columns (md:grid-cols-3)
    let cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(3);

    // Resize to desktop
    await page.setViewportSize(DESKTOP_VIEWPORT);
    await page.waitForTimeout(300);

    // Should still have 3 columns
    cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(3);

    // Resize to large desktop
    await page.setViewportSize(LARGE_DESKTOP_VIEWPORT);
    await page.waitForTimeout(300);

    // Should still have 3 columns with max-width constraint
    cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(3);

    // Verify cards are still in a row
    const firstBox = await featureCards.nth(0).boundingBox();
    const thirdBox = await featureCards.nth(2).boundingBox();

    if (firstBox && thirdBox) {
      expect(Math.abs(thirdBox.y - firstBox.y)).toBeLessThan(50);
    }
  });
});
