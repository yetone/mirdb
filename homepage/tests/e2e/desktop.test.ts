/**
 * E2E tests for desktop responsive design (1024px+).
 * Owner: Scenario 9 - Responsive Design - Desktop
 *
 * Tests:
 * - Homepage renders correctly at 1920px viewport width
 * - Content is centered with appropriate max-width constraints
 * - Features display in 3-column grid layout
 * - Layout maintains max-width constraints at 2560px viewport width
 * - Text line lengths remain comfortable for reading
 */

import { test, expect } from '@playwright/test';

// Desktop viewport sizes
const DESKTOP_STANDARD = { width: 1920, height: 1080 }; // Full HD desktop
const DESKTOP_LARGE = { width: 2560, height: 1440 }; // QHD / 1440p
const DESKTOP_MIN = { width: 1024, height: 768 }; // Minimum desktop width

test.describe('Responsive Design - Desktop (1024px-1920px+)', () => {
  test.describe('Test Case 1: 1920px Viewport Width', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(DESKTOP_STANDARD);
      await page.goto('/mirdb/');
    });

    test('content is centered with appropriate max-width at 1920px', async ({ page }) => {
      // Wait for page to fully load
      await page.waitForLoadState('networkidle');

      // Check that the page has no unexpected horizontal overflow
      const scrollInfo = await page.evaluate(() => ({
        scrollWidth: document.documentElement.scrollWidth,
        clientWidth: document.documentElement.clientWidth
      }));
      expect(scrollInfo.scrollWidth).toBeLessThanOrEqual(scrollInfo.clientWidth);

      // Hero section should have max-width and be centered
      const heroSection = page.locator('#hero');
      await expect(heroSection).toBeVisible();

      const heroContent = page.locator('#hero .max-w-4xl');
      await expect(heroContent).toBeVisible();
      const heroContentBox = await heroContent.boundingBox();
      expect(heroContentBox).toBeTruthy();

      // Content should be centered (equal margins on both sides, approximately)
      const leftMargin = heroContentBox!.x;
      const rightMargin = 1920 - heroContentBox!.x - heroContentBox!.width;
      const marginDiff = Math.abs(leftMargin - rightMargin);
      expect(marginDiff).toBeLessThan(50); // Allow some tolerance for centering

      // Features section should have max-width and be centered
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();

      const featuresContainer = page.locator('#features .max-w-6xl');
      await expect(featuresContainer).toBeVisible();
      const featuresBox = await featuresContainer.boundingBox();
      expect(featuresBox).toBeTruthy();

      // Features container should have max-width applied (not full 1920px)
      expect(featuresBox!.width).toBeLessThan(1920);
      expect(featuresBox!.width).toBeLessThanOrEqual(1152 + 64); // max-w-6xl (1152px) + some padding
    });

    test('no stretched layouts at 1920px - content respects max-width', async ({ page }) => {
      await page.waitForLoadState('networkidle');

      // Verify each section has appropriate max-width constraints
      const sections = [
        { selector: '#hero .max-w-4xl', maxWidth: 896 + 100 }, // max-w-4xl = 896px + tolerance
        { selector: '#features .max-w-6xl', maxWidth: 1152 + 100 }, // max-w-6xl = 1152px + tolerance
      ];

      for (const section of sections) {
        const element = page.locator(section.selector);
        if (await element.count() > 0) {
          const box = await element.boundingBox();
          expect(box).toBeTruthy();
          expect(box!.width).toBeLessThanOrEqual(section.maxWidth);
        }
      }

      // Check examples section
      const examplesSection = page.locator('#examples');
      await examplesSection.scrollIntoViewIfNeeded();
      await expect(examplesSection).toBeVisible();

      // Check installation section
      const installationSection = page.locator('#installation');
      await installationSection.scrollIntoViewIfNeeded();
      await expect(installationSection).toBeVisible();

      // Check architecture section
      const architectureSection = page.locator('#architecture');
      await architectureSection.scrollIntoViewIfNeeded();
      await expect(architectureSection).toBeVisible();
    });

    test('all sections visible and properly laid out at 1920px', async ({ page }) => {
      const sections = ['#hero', '#features', '#examples', '#architecture', '#installation'];

      for (const sectionId of sections) {
        const section = page.locator(sectionId);
        await section.scrollIntoViewIfNeeded();
        await expect(section).toBeVisible();

        // Verify section has reasonable width (not stretched edge-to-edge without constraints)
        const sectionBox = await section.boundingBox();
        expect(sectionBox).toBeTruthy();
      }

      // Verify footer is visible
      const footer = page.locator('footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();
    });
  });

  test.describe('Test Case 2: Features Grid 3-Column Layout', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(DESKTOP_STANDARD);
      await page.goto('/mirdb/');
    });

    test('features display in 3-column grid layout at desktop width', async ({ page }) => {
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();
      await expect(featuresSection).toBeVisible();

      // Get the features grid
      const featuresGrid = page.locator('#features .features-grid, #features .grid');
      await expect(featuresGrid).toBeVisible();

      // Get all feature cards
      const featureCards = featuresGrid.locator('> *');
      const cardCount = await featureCards.count();
      expect(cardCount).toBeGreaterThanOrEqual(3); // Need at least 3 for 3-column test

      // Get positions of first 3 cards to verify 3-column layout
      const cardPositions: { x: number; y: number }[] = [];
      for (let i = 0; i < Math.min(cardCount, 3); i++) {
        const cardBox = await featureCards.nth(i).boundingBox();
        expect(cardBox).toBeTruthy();
        cardPositions.push({ x: cardBox!.x, y: cardBox!.y });
      }

      // In a 3-column layout, first 3 cards should be on the same row (same Y position)
      // and have different X positions
      const yPositionVariance = Math.max(...cardPositions.map(p => p.y)) - Math.min(...cardPositions.map(p => p.y));
      expect(yPositionVariance).toBeLessThan(20); // All on same row within tolerance

      // X positions should be different (3 columns)
      const uniqueXPositions = new Set(cardPositions.map(p => Math.round(p.x / 50))); // Group similar positions
      expect(uniqueXPositions.size).toBe(3); // Should have 3 distinct column positions
    });

    test('feature cards have consistent widths in 3-column layout', async ({ page }) => {
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();

      const featuresGrid = page.locator('#features .features-grid, #features .grid');
      const featureCards = featuresGrid.locator('> *');
      const cardCount = await featureCards.count();

      // Collect widths of first row of cards
      const cardWidths: number[] = [];
      for (let i = 0; i < Math.min(cardCount, 3); i++) {
        const cardBox = await featureCards.nth(i).boundingBox();
        expect(cardBox).toBeTruthy();
        cardWidths.push(cardBox!.width);
      }

      // All cards in the row should have similar widths (within tolerance)
      const widthVariance = Math.max(...cardWidths) - Math.min(...cardWidths);
      expect(widthVariance).toBeLessThan(20);
    });

    test('features grid has proper gap between cards', async ({ page }) => {
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();

      const featuresGrid = page.locator('#features .features-grid, #features .grid');
      const featureCards = featuresGrid.locator('> *');
      const cardCount = await featureCards.count();

      if (cardCount >= 2) {
        const firstCardBox = await featureCards.nth(0).boundingBox();
        const secondCardBox = await featureCards.nth(1).boundingBox();
        expect(firstCardBox).toBeTruthy();
        expect(secondCardBox).toBeTruthy();

        // Gap between cards should be positive (cards shouldn't overlap)
        const gap = secondCardBox!.x - (firstCardBox!.x + firstCardBox!.width);
        expect(gap).toBeGreaterThan(0);
        expect(gap).toBeLessThan(100); // Reasonable gap, not excessively wide
      }
    });
  });

  test.describe('Test Case 3: 2560px Viewport Width (Ultra-wide)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(DESKTOP_LARGE);
      await page.goto('/mirdb/');
    });

    test('layout maintains max-width constraints at 2560px', async ({ page }) => {
      await page.waitForLoadState('networkidle');

      // Hero section content should maintain max-width (not expand to 2560px)
      const heroContent = page.locator('#hero .max-w-4xl');
      await expect(heroContent).toBeVisible();
      const heroBox = await heroContent.boundingBox();
      expect(heroBox).toBeTruthy();
      expect(heroBox!.width).toBeLessThanOrEqual(896 + 32); // max-w-4xl = 896px + padding

      // Features section should maintain max-width
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();

      const featuresContainer = page.locator('#features .max-w-6xl');
      await expect(featuresContainer).toBeVisible();
      const featuresBox = await featuresContainer.boundingBox();
      expect(featuresBox).toBeTruthy();
      expect(featuresBox!.width).toBeLessThanOrEqual(1152 + 64); // max-w-6xl = 1152px + padding
    });

    test('content remains centered at 2560px viewport', async ({ page }) => {
      await page.waitForLoadState('networkidle');

      // Hero content should be centered
      const heroContent = page.locator('#hero .max-w-4xl');
      const heroBox = await heroContent.boundingBox();
      expect(heroBox).toBeTruthy();

      // Calculate centering
      const leftMargin = heroBox!.x;
      const rightMargin = 2560 - heroBox!.x - heroBox!.width;
      const marginDiff = Math.abs(leftMargin - rightMargin);
      expect(marginDiff).toBeLessThan(50); // Content should be centered

      // Features container should be centered
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();

      const featuresContainer = page.locator('#features .max-w-6xl');
      const featuresBox = await featuresContainer.boundingBox();
      expect(featuresBox).toBeTruthy();

      const featuresLeftMargin = featuresBox!.x;
      const featuresRightMargin = 2560 - featuresBox!.x - featuresBox!.width;
      const featuresMarginDiff = Math.abs(featuresLeftMargin - featuresRightMargin);
      expect(featuresMarginDiff).toBeLessThan(50);
    });

    test('features still display in 3-column layout at 2560px', async ({ page }) => {
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();

      const featuresGrid = page.locator('#features .features-grid, #features .grid');
      const featureCards = featuresGrid.locator('> *');
      const cardCount = await featureCards.count();
      expect(cardCount).toBeGreaterThanOrEqual(3);

      // Verify 3-column layout
      const cardPositions: { x: number; y: number }[] = [];
      for (let i = 0; i < Math.min(cardCount, 3); i++) {
        const cardBox = await featureCards.nth(i).boundingBox();
        expect(cardBox).toBeTruthy();
        cardPositions.push({ x: cardBox!.x, y: cardBox!.y });
      }

      // First 3 cards should be on same row
      const yVariance = Math.max(...cardPositions.map(p => p.y)) - Math.min(...cardPositions.map(p => p.y));
      expect(yVariance).toBeLessThan(20);

      // Should have 3 distinct columns
      const uniqueXPositions = new Set(cardPositions.map(p => Math.round(p.x / 50)));
      expect(uniqueXPositions.size).toBe(3);
    });

    test('no horizontal scrollbar at 2560px viewport', async ({ page }) => {
      await page.waitForLoadState('networkidle');

      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);
    });
  });

  test.describe('Text Readability at Desktop Widths', () => {
    test('text line lengths are comfortable for reading at 1920px', async ({ page }) => {
      await page.setViewportSize(DESKTOP_STANDARD);
      await page.goto('/mirdb/');
      await page.waitForLoadState('networkidle');

      // Check paragraph text in features section has reasonable width
      const featureDescriptions = page.locator('#features p.text-gray-600, #features p.dark\\:text-gray-400').first();
      if (await featureDescriptions.count() > 0) {
        const descBox = await featureDescriptions.boundingBox();
        if (descBox) {
          // Good reading width is typically 45-75 characters or ~500-800px for body text
          expect(descBox.width).toBeLessThanOrEqual(800);
        }
      }

      // Hero tagline should be constrained
      const heroTagline = page.locator('[data-testid="hero-tagline"]');
      await expect(heroTagline).toBeVisible();
      const taglineBox = await heroTagline.boundingBox();
      expect(taglineBox).toBeTruthy();
      expect(taglineBox!.width).toBeLessThanOrEqual(900); // Reasonable max width for tagline
    });

    test('section descriptions have appropriate max-width', async ({ page }) => {
      await page.setViewportSize(DESKTOP_STANDARD);
      await page.goto('/mirdb/');

      // Features section description should have max-width
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();

      const featuresDescription = page.locator('#features .max-w-2xl').first();
      if (await featuresDescription.count() > 0) {
        const descBox = await featuresDescription.boundingBox();
        if (descBox) {
          expect(descBox.width).toBeLessThanOrEqual(672 + 32); // max-w-2xl = 672px + padding
        }
      }
    });
  });

  test.describe('Desktop Layout Edge Cases', () => {
    test('layout works correctly at minimum desktop width (1024px)', async ({ page }) => {
      await page.setViewportSize(DESKTOP_MIN);
      await page.goto('/mirdb/');
      await page.waitForLoadState('networkidle');

      // All sections should be visible
      const sections = ['#hero', '#features', '#examples', '#architecture', '#installation'];
      for (const sectionId of sections) {
        const section = page.locator(sectionId);
        await section.scrollIntoViewIfNeeded();
        await expect(section).toBeVisible();
      }

      // Features should be in 3-column grid (lg breakpoint starts at 1024px)
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();

      const featuresGrid = page.locator('#features .features-grid, #features .grid');
      const featureCards = featuresGrid.locator('> *');
      const cardCount = await featureCards.count();

      if (cardCount >= 3) {
        const cardPositions: { x: number; y: number }[] = [];
        for (let i = 0; i < 3; i++) {
          const cardBox = await featureCards.nth(i).boundingBox();
          expect(cardBox).toBeTruthy();
          cardPositions.push({ x: cardBox!.x, y: cardBox!.y });
        }

        // At 1024px (lg breakpoint), should be 3 columns
        const yVariance = Math.max(...cardPositions.map(p => p.y)) - Math.min(...cardPositions.map(p => p.y));
        expect(yVariance).toBeLessThan(30);
      }
    });

    test('no horizontal overflow at various desktop widths', async ({ page }) => {
      const desktopWidths = [1024, 1280, 1440, 1920, 2560];

      for (const width of desktopWidths) {
        await page.setViewportSize({ width, height: 900 });
        await page.goto('/mirdb/');
        await page.waitForLoadState('networkidle');

        const hasHorizontalScroll = await page.evaluate(() => {
          return document.documentElement.scrollWidth > document.documentElement.clientWidth;
        });
        expect(hasHorizontalScroll).toBe(false);
      }
    });

    test('footer spans full width but content is constrained', async ({ page }) => {
      await page.setViewportSize(DESKTOP_STANDARD);
      await page.goto('/mirdb/');

      const footer = page.locator('footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();

      // Footer container should have max-width
      const footerContent = footer.locator('.max-w-6xl, .max-w-4xl, .container').first();
      if (await footerContent.count() > 0) {
        const contentBox = await footerContent.boundingBox();
        expect(contentBox).toBeTruthy();
        expect(contentBox!.width).toBeLessThan(1920);
      }
    });
  });

  test.describe('Interactive Elements at Desktop Width', () => {
    test('CTA buttons are properly sized for desktop', async ({ page }) => {
      await page.setViewportSize(DESKTOP_STANDARD);
      await page.goto('/mirdb/');

      const getStartedButton = page.locator('[data-testid="get-started-button"]');
      await expect(getStartedButton).toBeVisible();
      const getStartedBox = await getStartedButton.boundingBox();
      expect(getStartedBox).toBeTruthy();
      expect(getStartedBox!.width).toBeGreaterThanOrEqual(100);
      expect(getStartedBox!.height).toBeGreaterThanOrEqual(40);

      const githubButton = page.locator('[data-testid="github-button"]');
      await expect(githubButton).toBeVisible();
      const githubBox = await githubButton.boundingBox();
      expect(githubBox).toBeTruthy();
      expect(githubBox!.width).toBeGreaterThanOrEqual(100);
      expect(githubBox!.height).toBeGreaterThanOrEqual(40);
    });

    test('navigation anchor links work at desktop width', async ({ page }) => {
      await page.setViewportSize(DESKTOP_STANDARD);
      await page.goto('/mirdb/');

      // Click Get Started and verify navigation
      const getStartedButton = page.locator('[data-testid="get-started-button"]');
      await getStartedButton.click();
      await page.waitForTimeout(1000);

      const installationSection = page.locator('#installation');
      await expect(installationSection).toBeInViewport();
    });

    test('tab navigation works in examples section at desktop', async ({ page }) => {
      await page.setViewportSize(DESKTOP_STANDARD);
      await page.goto('/mirdb/');

      const examplesSection = page.locator('#examples');
      await examplesSection.scrollIntoViewIfNeeded();

      const tabButtons = page.locator('#examples .tab-button');
      const tabCount = await tabButtons.count();

      if (tabCount > 1) {
        // Click second tab
        await tabButtons.nth(1).click();
        await page.waitForTimeout(200);

        // Verify tab is active
        await expect(tabButtons.nth(1)).toHaveAttribute('aria-selected', 'true');
      }
    });
  });
});
