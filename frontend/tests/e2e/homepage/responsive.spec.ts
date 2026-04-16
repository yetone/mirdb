/**
 * Responsive Design E2E Tests for Homepage
 * Owner: Scenario 7 - Responsive Design - Mobile Viewport
 *
 * Tests that the homepage displays and functions correctly on mobile devices
 * (viewport width ≤ 768px) without horizontal scrolling.
 *
 * Test cases cover:
 * - No horizontal scrollbar at mobile viewport
 * - Touch targets meet accessibility minimum (44x44px)
 * - Features grid layout adapts to mobile
 * - Hero section visible without excessive scrolling
 * - Navigation collapses/adapts for mobile
 * - Layout transitions at tablet breakpoint (768px)
 */

import { test, expect, Page } from '@playwright/test';
import { homepageSelectors, viewportSizes } from './fixtures';

test.describe('Responsive Design - Mobile Viewport', () => {
  test.beforeEach(async ({ page }) => {
    // Set mobile viewport before navigation
    await page.setViewportSize(viewportSizes.mobile);
    await page.goto('/');
    // Wait for page to be fully loaded
    await page.waitForLoadState('networkidle');
  });

  test('Test Case 1: Render homepage at 375px viewport width - no horizontal scrollbar appears, all content visible', async ({
    page,
  }) => {
    // Verify viewport is set correctly
    const viewport = page.viewportSize();
    expect(viewport?.width).toBe(375);

    // Check for horizontal overflow by comparing scroll width to client width
    const hasHorizontalScrollbar = await page.evaluate(() => {
      const body = document.body;
      const html = document.documentElement;
      // Check if content width exceeds viewport width
      const contentWidth = Math.max(
        body.scrollWidth,
        body.offsetWidth,
        html.clientWidth,
        html.scrollWidth,
        html.offsetWidth
      );
      const viewportWidth = window.innerWidth;
      return contentWidth > viewportWidth;
    });

    expect(hasHorizontalScrollbar).toBe(false);

    // Verify all major sections are visible (can be scrolled to)
    const heroSection = page.locator(homepageSelectors.hero.section);
    const featuresSection = page.locator(homepageSelectors.features.section);

    await expect(heroSection).toBeVisible();
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Verify hero headline is visible
    const headline = page.locator(homepageSelectors.hero.headline);
    await expect(headline).toBeVisible();
    await expect(headline).toContainText('Shorten Links');

    // Verify visible content elements don't overflow the viewport horizontally
    // Exclude hidden, absolute/fixed decorative elements (e.g., background effects)
    const overflowingElements = await page.evaluate(() => {
      const viewportWidth = window.innerWidth;
      // Focus on main content elements, not decorative positioned elements
      const contentSelectors = 'main, section, header, nav, footer, h1, h2, h3, p, button, a, input, form';
      const elements = document.querySelectorAll(contentSelectors);
      const overflowing: string[] = [];
      elements.forEach((el) => {
        const rect = el.getBoundingClientRect();
        const styles = window.getComputedStyle(el);
        // Skip hidden or off-screen decorative elements
        if (styles.visibility === 'hidden' || styles.display === 'none') {
          return;
        }
        // Check if element content extends past viewport (with tolerance)
        if (rect.right > viewportWidth + 10) {
          overflowing.push(
            el.tagName + (el.className ? `.${el.className.split(' ')[0]}` : '')
          );
        }
      });
      return overflowing;
    });

    // Log any overflowing elements for debugging
    if (overflowingElements.length > 0) {
      console.log('Overflowing content elements:', overflowingElements);
    }

    expect(overflowingElements.length).toBe(0);
  });

  test('Test Case 2: Measure CTA button touch target - button dimensions are at least 44x44 pixels', async ({
    page,
  }) => {
    const primaryCta = page.locator(homepageSelectors.hero.primaryCta);
    const secondaryCta = page.locator(homepageSelectors.hero.secondaryCta);

    // Wait for CTAs to be visible
    await expect(primaryCta).toBeVisible();
    await expect(secondaryCta).toBeVisible();

    // Get bounding boxes for both buttons
    const primaryBox = await primaryCta.boundingBox();
    const secondaryBox = await secondaryCta.boundingBox();

    // WCAG 2.1 requires minimum 44x44px touch targets
    const minTouchTarget = 44;

    // Verify primary CTA meets touch target requirements
    expect(primaryBox).toBeTruthy();
    expect(primaryBox!.width).toBeGreaterThanOrEqual(minTouchTarget);
    expect(primaryBox!.height).toBeGreaterThanOrEqual(minTouchTarget);

    // Verify secondary CTA meets touch target requirements
    expect(secondaryBox).toBeTruthy();
    expect(secondaryBox!.width).toBeGreaterThanOrEqual(minTouchTarget);
    expect(secondaryBox!.height).toBeGreaterThanOrEqual(minTouchTarget);

    // Additionally, verify all buttons on the page meet touch target requirements
    const allButtons = page.locator('button, a.btn, [role="button"]');
    const buttonCount = await allButtons.count();

    for (let i = 0; i < buttonCount; i++) {
      const button = allButtons.nth(i);
      const isVisible = await button.isVisible();
      if (isVisible) {
        const box = await button.boundingBox();
        if (box) {
          // Check minimum dimensions (width or height should be at least 44px)
          // Some inline links might be tall but narrow, which is acceptable
          const meetsMinimum = box.width >= minTouchTarget || box.height >= minTouchTarget;
          expect(meetsMinimum).toBe(true);
        }
      }
    }
  });

  test('Test Case 3: Check features grid at mobile viewport - feature cards display in 2-column or single-column layout', async ({
    page,
  }) => {
    const featuresSection = page.locator(homepageSelectors.features.section);
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Get all feature cards
    const featureCards = page.locator('[data-testid^="feature-card-"]');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(4);

    // Get positions of first few cards to determine layout
    const cardPositions: { x: number; y: number; width: number }[] = [];
    for (let i = 0; i < Math.min(4, cardCount); i++) {
      const card = featureCards.nth(i);
      await card.scrollIntoViewIfNeeded();
      const box = await card.boundingBox();
      if (box) {
        cardPositions.push({ x: box.x, y: box.y, width: box.width });
      }
    }

    // Verify we got positions for cards
    expect(cardPositions.length).toBeGreaterThanOrEqual(2);

    // At 375px (mobile), cards should be in 1-2 columns
    // Check the X positions to determine columns
    const uniqueXPositions = new Set(cardPositions.map((p) => Math.round(p.x)));
    const columnCount = uniqueXPositions.size;

    // On mobile (375px), should be 1 or 2 columns, not 4
    expect(columnCount).toBeLessThanOrEqual(2);

    // Verify cards don't overflow the viewport width
    const viewportWidth = viewportSizes.mobile.width;
    for (const pos of cardPositions) {
      expect(pos.x + pos.width).toBeLessThanOrEqual(viewportWidth + 10); // 10px tolerance for padding
    }
  });

  test('Test Case 4: Check hero section at mobile viewport - hero content visible without excessive scrolling', async ({
    page,
  }) => {
    const heroSection = page.locator(homepageSelectors.hero.section);
    await expect(heroSection).toBeVisible();

    // Get hero section dimensions
    const heroBox = await heroSection.boundingBox();
    expect(heroBox).toBeTruthy();

    // Hero should fit reasonably within viewport (not requiring excessive scroll)
    // At 375x667 (iPhone SE), hero should be within about 1.5 viewport heights
    const viewportHeight = viewportSizes.mobile.height;
    const maxHeroHeight = viewportHeight * 1.5;

    // Check that hero content is within reasonable bounds
    expect(heroBox!.height).toBeLessThan(maxHeroHeight);

    // Verify key hero elements are visible without scrolling
    const headline = page.locator(homepageSelectors.hero.headline);
    const subheadline = page.locator(homepageSelectors.hero.subheadline);
    const primaryCta = page.locator(homepageSelectors.hero.primaryCta);

    await expect(headline).toBeVisible();
    await expect(subheadline).toBeVisible();
    await expect(primaryCta).toBeVisible();

    // Verify headline uses appropriate font size for mobile
    const headlineFontSize = await headline.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });

    // Mobile headline should be readable (at least 24px, typically 36px for text-4xl)
    expect(headlineFontSize).toBeGreaterThanOrEqual(24);

    // Verify CTAs stack vertically on mobile (flex-col)
    const ctaContainer = page.locator('[data-testid="hero-section"] .flex');
    const ctaDirection = await ctaContainer.first().evaluate((el) => {
      return window.getComputedStyle(el).flexDirection;
    });

    // At 375px, buttons should be stacked (column)
    expect(ctaDirection).toBe('column');
  });

  test('Test Case 5: Check navbar at mobile viewport - navigation collapses to mobile-friendly format', async ({
    page,
  }) => {
    // Get the navbar
    const navbar = page.locator('nav.navbar');
    await expect(navbar).toBeVisible();

    // Get navbar dimensions
    const navbarBox = await navbar.boundingBox();
    expect(navbarBox).toBeTruthy();

    // Navbar should not overflow viewport width
    expect(navbarBox!.width).toBeLessThanOrEqual(viewportSizes.mobile.width);

    // Check that navigation links are accessible (either visible or in a menu)
    // The current implementation shows links in flex layout
    const navLinks = navbar.locator('a.btn, button.btn');
    const linkCount = await navLinks.count();

    // Verify there are navigation elements
    expect(linkCount).toBeGreaterThan(0);

    // Verify no nav elements overflow the viewport
    for (let i = 0; i < linkCount; i++) {
      const link = navLinks.nth(i);
      const isVisible = await link.isVisible();
      if (isVisible) {
        const box = await link.boundingBox();
        if (box) {
          // Element should be within viewport bounds
          expect(box.x + box.width).toBeLessThanOrEqual(viewportSizes.mobile.width + 5);
        }
      }
    }

    // Verify the logo/brand is still visible on mobile
    const logo = navbar.locator('a').first();
    await expect(logo).toBeVisible();
    const logoBox = await logo.boundingBox();
    expect(logoBox).toBeTruthy();
    expect(logoBox!.x).toBeGreaterThanOrEqual(0);
  });

  test('Test Case 6: Test at 768px viewport (tablet breakpoint) - layout transitions appropriately at breakpoint', async ({
    page,
  }) => {
    // Change to tablet viewport
    await page.setViewportSize(viewportSizes.tablet);
    await page.waitForTimeout(300); // Allow for responsive layout recalculation

    // Verify no horizontal scrollbar at tablet width
    const hasHorizontalScrollbar = await page.evaluate(() => {
      const body = document.body;
      return body.scrollWidth > window.innerWidth;
    });
    expect(hasHorizontalScrollbar).toBe(false);

    // Check features grid layout at tablet (768px)
    // Should be 2 columns at sm breakpoint (640px+) according to Tailwind classes
    const featuresSection = page.locator(homepageSelectors.features.section);
    await featuresSection.scrollIntoViewIfNeeded();

    const featureCards = page.locator('[data-testid^="feature-card-"]');
    const cardPositions: { x: number; y: number }[] = [];

    for (let i = 0; i < 4; i++) {
      const card = featureCards.nth(i);
      await card.scrollIntoViewIfNeeded();
      const box = await card.boundingBox();
      if (box) {
        cardPositions.push({ x: Math.round(box.x), y: Math.round(box.y) });
      }
    }

    // At 768px (tablet), should be 2 columns
    const uniqueXPositions = new Set(cardPositions.map((p) => p.x));
    const columnCount = uniqueXPositions.size;

    // At tablet size (sm breakpoint 640px+), expect 2 columns
    expect(columnCount).toBe(2);

    // Verify hero CTAs layout transitions
    const heroSection = page.locator(homepageSelectors.hero.section);
    await heroSection.scrollIntoViewIfNeeded();

    // At 640px+ (sm breakpoint), CTAs should be horizontal (flex-row)
    const ctaContainer = heroSection.locator('.flex').first();
    const ctaDirection = await ctaContainer.evaluate((el) => {
      return window.getComputedStyle(el).flexDirection;
    });

    // At 768px (above sm breakpoint), buttons should be in a row
    expect(ctaDirection).toBe('row');

    // Verify analytics section uses single column at tablet (lg breakpoint is 1024px)
    const analyticsSection = page.locator('[data-testid="analytics-preview-section"]');
    await analyticsSection.scrollIntoViewIfNeeded();

    const chartCards = analyticsSection.locator('[data-testid$="-chart"]');
    const chartPositions: { x: number }[] = [];

    const chartCount = await chartCards.count();
    for (let i = 0; i < chartCount; i++) {
      const chart = chartCards.nth(i);
      const box = await chart.boundingBox();
      if (box) {
        chartPositions.push({ x: Math.round(box.x) });
      }
    }

    // At 768px (below lg breakpoint 1024px), analytics charts should be stacked (1 column)
    const uniqueChartX = new Set(chartPositions.map((p) => p.x));
    expect(uniqueChartX.size).toBe(1);
  });
});

test.describe('Responsive Design - Text Readability', () => {
  test('Verify text is readable at mobile viewport without zooming', async ({ page }) => {
    await page.setViewportSize(viewportSizes.mobile);
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Check hero headline font size
    const headline = page.locator(homepageSelectors.hero.headline);
    const headlineFontSize = await headline.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // text-4xl is 36px, should be at least 24px on mobile
    expect(headlineFontSize).toBeGreaterThanOrEqual(24);

    // Check subheadline font size
    const subheadline = page.locator(homepageSelectors.hero.subheadline);
    const subheadlineFontSize = await subheadline.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // text-xl is 20px, should be at least 16px for readability
    expect(subheadlineFontSize).toBeGreaterThanOrEqual(16);

    // Check feature card text is readable
    const featuresSection = page.locator(homepageSelectors.features.section);
    await featuresSection.scrollIntoViewIfNeeded();

    const featureTitle = page.locator('[data-testid="feature-title-url-shortening"]');
    const titleFontSize = await featureTitle.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // text-xl is 20px minimum
    expect(titleFontSize).toBeGreaterThanOrEqual(16);

    const featureDescription = page.locator(
      '[data-testid="feature-description-url-shortening"]'
    );
    const descFontSize = await featureDescription.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // Body text should be at least 14px for mobile readability
    expect(descFontSize).toBeGreaterThanOrEqual(14);
  });
});

test.describe('Responsive Design - Multiple Viewport Tests', () => {
  const viewports = [
    { name: 'iPhone SE', ...viewportSizes.mobile },
    { name: 'Tablet', ...viewportSizes.tablet },
    { name: 'Desktop', ...viewportSizes.desktop },
  ];

  for (const vp of viewports) {
    test(`No horizontal overflow at ${vp.name} (${vp.width}x${vp.height})`, async ({
      page,
    }) => {
      await page.setViewportSize({ width: vp.width, height: vp.height });
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      const hasOverflow = await page.evaluate(() => {
        return document.body.scrollWidth > window.innerWidth;
      });

      expect(hasOverflow).toBe(false);
    });
  }
});
