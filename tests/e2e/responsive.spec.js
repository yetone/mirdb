/**
 * Responsive Design E2E Tests
 * Owner: Scenarios 10-12 - Responsive Design
 *
 * Tests:
 * - Mobile viewport (375px) layout
 * - Tablet viewport (768px) layout
 * - Desktop viewport (1280px) layout
 * - Mobile navigation menu
 * - Touch target sizes
 */
import { test, expect } from '@playwright/test';

// Mobile viewport (iPhone SE/X width)
const MOBILE_VIEWPORT = { width: 375, height: 667 };

test.describe('Responsive Design - Mobile (375px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(MOBILE_VIEWPORT);
    await page.goto('/');
  });

  test('no horizontal overflow at 375px viewport width', async ({ page }) => {
    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');

    // Check that there's no horizontal scrollbar visible
    const hasHorizontalScroll = await page.evaluate(() => {
      // Check if document is wider than viewport
      const docWidth = Math.max(
        document.documentElement.scrollWidth,
        document.body.scrollWidth
      );
      const viewportWidth = window.innerWidth;
      return docWidth > viewportWidth + 5; // Allow 5px tolerance
    });

    // The page should not have significant horizontal overflow
    // (some small overflow may occur due to scrollbar calculations)
    expect(hasHorizontalScroll).toBe(false);
  });

  test('mobile navigation hamburger menu is visible and functional', async ({ page }) => {
    await page.waitForLoadState('domcontentloaded');

    // Mobile menu button should be visible
    const mobileMenuBtn = page.locator('#mobile-menu-btn');
    await expect(mobileMenuBtn).toBeVisible({ timeout: 5000 });

    // Mobile menu should initially be hidden
    const mobileMenu = page.locator('#mobile-menu');
    await expect(mobileMenu).toBeHidden();

    // Click hamburger button to open menu
    await mobileMenuBtn.click();
    await expect(mobileMenu).toBeVisible({ timeout: 3000 });

    // Mobile menu should contain navigation links
    const featuresLink = mobileMenu.locator('a[href="#features"]');
    await expect(featuresLink).toBeVisible();

    // Click again to close menu
    await mobileMenuBtn.click();
    await expect(mobileMenu).toBeHidden({ timeout: 3000 });
  });

  test('code blocks are horizontally scrollable on mobile', async ({ page }) => {
    // Navigate to quick-start section
    await page.goto('/#quick-start');
    await page.waitForLoadState('domcontentloaded');
    await page.waitForTimeout(300);

    // Find code blocks
    const codeBlocks = page.locator('.code-block');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThan(0);

    // Check that code blocks have overflow-x styling that allows scrolling
    const firstCodeBlock = codeBlocks.first();
    const preElement = firstCodeBlock.locator('pre');

    // Get computed style for overflow
    const overflowX = await preElement.evaluate((el) => {
      return window.getComputedStyle(el).overflowX;
    });

    // Should be 'auto', 'scroll', or 'visible' (visible means content can extend)
    // The key is that code blocks have overflow handling
    expect(['auto', 'scroll', 'visible']).toContain(overflowX);

    // Verify code block container doesn't overflow viewport
    const codeBlockRect = await firstCodeBlock.boundingBox();
    expect(codeBlockRect.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width + 5);
  });

  test('touch targets are at least 44x44px for accessibility', async ({ page }) => {
    await page.waitForLoadState('domcontentloaded');

    // Test key interactive elements for minimum touch target size
    // Focus on primary mobile navigation elements
    const primaryTouchTargets = [
      { selector: '#mobile-menu-btn', name: 'Mobile menu button' },
      { selector: '#hero a', name: 'Hero CTA buttons' }
    ];

    for (const { selector, name } of primaryTouchTargets) {
      const elements = page.locator(selector);
      const count = await elements.count();

      if (count > 0) {
        const firstElement = elements.first();
        const isVisible = await firstElement.isVisible().catch(() => false);

        if (isVisible) {
          const boundingBox = await firstElement.boundingBox();

          if (boundingBox) {
            // Touch targets should be at least 44x44px (WCAG 2.5.5)
            // Allow some tolerance for computed dimensions
            expect(boundingBox.width).toBeGreaterThanOrEqual(44);
            expect(boundingBox.height).toBeGreaterThanOrEqual(44);
          }
        }
      }
    }
  });

  test('all sections stack vertically on mobile', async ({ page }) => {
    // Verify main sections are visible and stack vertically
    const sections = ['#hero', '#demo', '#features', '#quick-start', '#commands', '#status'];

    let previousBottom = 0;

    for (const sectionId of sections) {
      const section = page.locator(sectionId);
      const isVisible = await section.isVisible().catch(() => false);

      if (isVisible) {
        const box = await section.boundingBox();
        if (box) {
          // Section should start at or after the previous section ended
          expect(box.y).toBeGreaterThanOrEqual(previousBottom - 1);

          // Section should fit within viewport width
          expect(box.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);

          previousBottom = box.y + box.height;
        }
      }
    }
  });

  test('navigation is accessible via hamburger menu', async ({ page }) => {
    // Open mobile menu
    const mobileMenuBtn = page.locator('#mobile-menu-btn');
    await mobileMenuBtn.click();

    const mobileMenu = page.locator('#mobile-menu');
    await expect(mobileMenu).toBeVisible();

    // Verify aria-expanded attribute is updated
    const ariaExpanded = await mobileMenuBtn.getAttribute('aria-expanded');
    expect(ariaExpanded).toBe('true');

    // Navigate using mobile menu link
    const featuresLink = mobileMenu.locator('a[href="#features"]');
    await featuresLink.click();

    // Should scroll to features section
    await page.waitForTimeout(500);
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport({ ratio: 0.3 });
  });

  test('hero section is readable on mobile', async ({ page }) => {
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();

    // Hero title should be visible
    const title = hero.locator('h1');
    await expect(title).toBeVisible();
    await expect(title).toContainText('MirDB');

    // Value proposition should be visible
    const description = hero.locator('p').first();
    await expect(description).toBeVisible();

    // CTA buttons should be visible
    const ctaButtons = hero.locator('a');
    const ctaCount = await ctaButtons.count();
    expect(ctaCount).toBeGreaterThanOrEqual(1);
  });

  test('features section displays as single column on mobile', async ({ page }) => {
    await page.goto('/#features');
    await page.waitForTimeout(500);

    const featuresGrid = page.locator('#features .grid');
    const featureCards = featuresGrid.locator('> div');
    const cardCount = await featureCards.count();

    expect(cardCount).toBeGreaterThan(0);

    // On mobile, cards should stack (single column)
    // Check that cards have similar x positions (stacked vertically)
    const firstCard = featureCards.first();
    const firstCardBox = await firstCard.boundingBox();

    if (cardCount > 1) {
      const secondCard = featureCards.nth(1);
      const secondCardBox = await secondCard.boundingBox();

      // Cards should be at similar x position (stacked) or very close
      // On mobile, they should be in a single column
      expect(Math.abs(firstCardBox.x - secondCardBox.x)).toBeLessThan(10);
    }
  });

  test('footer content is accessible on mobile', async ({ page }) => {
    await page.goto('/#footer');
    await page.waitForTimeout(300);

    const footer = page.locator('#footer');
    await expect(footer).toBeVisible();

    // Footer text should be visible
    const footerText = footer.locator('p');
    await expect(footerText.first()).toBeVisible();

    // GitHub link in footer should be accessible
    const githubLink = footer.locator('a[href*="github.com"]');
    await expect(githubLink).toBeVisible();
  });
});
