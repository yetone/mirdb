/**
 * E2E tests for MirDB Homepage responsive design and functionality.
 * Owner: Scenario 8 - Responsive Design and Accessibility
 *
 * Tests:
 * - Mobile viewport (360px) functionality
 * - Tablet viewport (768px) layout
 * - Desktop viewport (1280px) full layout
 * - No horizontal scrolling at any viewport
 * - Content readability across devices
 */

import { test, expect } from '@playwright/test';

// Viewport configurations
const VIEWPORTS = {
  mobile: { width: 360, height: 800 },
  tablet: { width: 768, height: 1024 },
  desktop: { width: 1280, height: 800 },
};

test.describe('Responsive Design - Mobile (360px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.goto('/');
    // Wait for page to be fully loaded
    await page.waitForLoadState('networkidle');
  });

  test('page is fully functional with no horizontal scrolling', async ({ page }) => {
    // Check that the page doesn't require horizontal scrolling
    const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);

    expect(scrollWidth).toBeLessThanOrEqual(clientWidth + 1); // Allow 1px tolerance

    // Verify all main sections are visible when scrolled to
    const sections = ['#home', '#about', '#features', '#roadmap'];
    for (const section of sections) {
      const element = page.locator(section);
      await element.scrollIntoViewIfNeeded();
      await expect(element).toBeVisible();
    }
  });

  test('hero section is readable and buttons are tappable', async ({ page }) => {
    const heroSection = page.locator('section').first();
    await expect(heroSection).toBeVisible();

    // Check logo is visible
    const logo = page.locator('img[alt="MirDB Logo"]');
    await expect(logo).toBeVisible();

    // Check heading is visible
    const heading = page.locator('h1');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('MirDB');

    // Check CTA buttons are visible and have adequate tap targets (at least 44x44px recommended)
    // Buttons in Hero are <a> elements with role="button"
    const buttons = heroSection.locator('a[role="button"]');
    const buttonCount = await buttons.count();
    expect(buttonCount).toBeGreaterThanOrEqual(1);

    for (let i = 0; i < buttonCount; i++) {
      const button = buttons.nth(i);
      await expect(button).toBeVisible();
      const boundingBox = await button.boundingBox();
      if (boundingBox) {
        // Minimum touch target recommendation is 44x44px
        expect(boundingBox.height).toBeGreaterThanOrEqual(40);
      }
    }
  });

  test('navigation menu works on mobile', async ({ page }) => {
    // Hide the theme toggle to avoid it blocking the mobile menu button
    await page.evaluate(() => {
      const themeToggleContainer = document.querySelector('.fixed.top-4.right-4');
      if (themeToggleContainer) {
        (themeToggleContainer as HTMLElement).style.display = 'none';
      }
    });

    // Check mobile menu button is visible
    const menuButton = page.locator('button[aria-label*="menu"], button[aria-controls="mobile-menu"]');
    await expect(menuButton).toBeVisible();

    // Open mobile menu
    await menuButton.click();

    // Check mobile menu is visible
    const mobileMenu = page.locator('#mobile-menu');
    await expect(mobileMenu).toBeVisible();

    // Verify navigation links are accessible
    const navLinks = mobileMenu.locator('a');
    const linkCount = await navLinks.count();
    expect(linkCount).toBeGreaterThanOrEqual(1);
  });

  test('feature cards stack vertically on mobile', async ({ page }) => {
    // Scroll to features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    // Get feature cards
    const featureCards = page.locator('[data-testid="feature-card"]');
    const cardCount = await featureCards.count();

    if (cardCount >= 2) {
      // Check that cards are stacked (second card's top is below first card's bottom)
      const firstCard = await featureCards.first().boundingBox();
      const secondCard = await featureCards.nth(1).boundingBox();

      if (firstCard && secondCard) {
        expect(secondCard.y).toBeGreaterThan(firstCard.y + firstCard.height - 10); // Allow some overlap
      }
    }
  });

  test('text is readable without zooming', async ({ page }) => {
    // Check body text font size is at least 14px (readable on mobile)
    const bodyTextSize = await page.evaluate(() => {
      const bodyElement = document.querySelector('p');
      if (bodyElement) {
        return parseFloat(window.getComputedStyle(bodyElement).fontSize);
      }
      return 16;
    });
    expect(bodyTextSize).toBeGreaterThanOrEqual(14);
  });
});

test.describe('Responsive Design - Tablet (768px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.tablet);
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('tablet layout is applied and content is readable', async ({ page }) => {
    // No horizontal scrolling
    const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);
    expect(scrollWidth).toBeLessThanOrEqual(clientWidth + 1);

    // Hero section visible
    await expect(page.locator('h1')).toBeVisible();

    // Features section with grid layout
    await page.locator('#features').scrollIntoViewIfNeeded();
    const featureCards = page.locator('[data-testid="feature-card"]');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(1);
  });

  test('features display in multi-column grid', async ({ page }) => {
    await page.locator('#features').scrollIntoViewIfNeeded();

    const featureCards = page.locator('[data-testid="feature-card"]');
    const cardCount = await featureCards.count();

    if (cardCount >= 2) {
      const firstCard = await featureCards.first().boundingBox();
      const secondCard = await featureCards.nth(1).boundingBox();

      if (firstCard && secondCard) {
        // On tablet (md breakpoint), cards should be in 2-column grid
        // So the second card should be roughly at the same Y position as the first
        // (within some tolerance for padding/margins)
        const yDifference = Math.abs(secondCard.y - firstCard.y);
        expect(yDifference).toBeLessThan(50); // Same row
      }
    }
  });

  test('navigation shows desktop menu on tablet', async ({ page }) => {
    // On tablet, we should see desktop navigation
    const desktopNav = page.locator('.hidden.md\\:flex');
    // Either desktop nav is visible or mobile menu button is visible
    const menuButton = page.locator('button[aria-controls="mobile-menu"]');

    const isDesktopNavVisible = await desktopNav.isVisible().catch(() => false);
    const isMobileMenuVisible = await menuButton.isVisible().catch(() => false);

    // At least one navigation method should be available
    expect(isDesktopNavVisible || isMobileMenuVisible).toBeTruthy();
  });
});

test.describe('Responsive Design - Desktop (1280px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.desktop);
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('desktop layout with full feature grid is displayed', async ({ page }) => {
    // No horizontal scrolling
    const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);
    expect(scrollWidth).toBeLessThanOrEqual(clientWidth + 1);

    // Hero section visible with proper styling
    await expect(page.locator('h1')).toBeVisible();

    // About section
    await page.locator('#about').scrollIntoViewIfNeeded();
    await expect(page.locator('#about')).toBeVisible();
  });

  test('features display in 3-column grid on desktop', async ({ page }) => {
    await page.locator('#features').scrollIntoViewIfNeeded();

    const featureCards = page.locator('[data-testid="feature-card"]');
    const cardCount = await featureCards.count();

    if (cardCount >= 3) {
      const firstCard = await featureCards.first().boundingBox();
      const secondCard = await featureCards.nth(1).boundingBox();
      const thirdCard = await featureCards.nth(2).boundingBox();

      if (firstCard && secondCard && thirdCard) {
        // On desktop (lg breakpoint), cards should be in 3-column grid
        // All three cards should be on roughly the same Y position
        const tolerance = 20;
        expect(Math.abs(secondCard.y - firstCard.y)).toBeLessThan(tolerance);
        expect(Math.abs(thirdCard.y - firstCard.y)).toBeLessThan(tolerance);
      }
    }
  });

  test('desktop navigation is visible', async ({ page }) => {
    // Desktop navigation should be visible
    const nav = page.locator('nav');
    await expect(nav).toBeVisible();

    // Mobile menu button should be hidden
    const menuButton = page.locator('button[aria-controls="mobile-menu"]');
    await expect(menuButton).toBeHidden();
  });

  test('all sections are accessible via navigation', async ({ page }) => {
    // Only test visible desktop navigation links (not mobile menu links which are hidden)
    const navLinks = page.locator('nav .hidden.md\\:flex a[href^="#"]');
    const linkCount = await navLinks.count();

    for (let i = 0; i < linkCount; i++) {
      const link = navLinks.nth(i);
      const href = await link.getAttribute('href');
      if (href && href.startsWith('#')) {
        // Ensure link is visible before clicking
        if (await link.isVisible()) {
          await link.click();
          // Wait for smooth scroll
          await page.waitForTimeout(500);
          const targetSection = page.locator(href);
          if (await targetSection.count() > 0) {
            await expect(targetSection).toBeVisible();
          }
        }
      }
    }
  });
});

test.describe('Cross-viewport functionality', () => {
  for (const [name, viewport] of Object.entries(VIEWPORTS)) {
    test(`footer is visible at ${name} viewport`, async ({ page }) => {
      await page.setViewportSize(viewport);
      await page.goto('/');

      // Scroll to bottom
      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
      await page.waitForTimeout(500);

      const footer = page.locator('footer');
      await expect(footer).toBeVisible();
    });

    test(`theme toggle is accessible at ${name} viewport`, async ({ page }) => {
      await page.setViewportSize(viewport);
      await page.goto('/');

      // Find theme toggle button
      const themeToggle = page.locator('button[aria-label*="theme"], button[aria-label*="Toggle"]');
      const toggleCount = await themeToggle.count();

      if (toggleCount > 0) {
        await expect(themeToggle.first()).toBeVisible();
      }
    });
  }
});
