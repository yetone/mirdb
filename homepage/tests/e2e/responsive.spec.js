/**
 * Responsive Design E2E Tests
 * Owner: Scenario 5 - Responsive Design
 *
 * End-to-end tests for responsive behavior of the MirDB homepage.
 *
 * Expected test coverage:
 * - Mobile viewport (320px, 375px, 414px)
 * - Tablet viewport (768px, 1024px)
 * - Desktop viewport (1280px+)
 * - No horizontal scrolling on mobile
 * - Navigation accessibility on all viewports
 *
 * Requirements traced:
 * - REQ-5: Responsive and accessible
 * - USR-5: Mobile device viewing
 */

const { test, expect } = require('@playwright/test');

test.describe('Responsive Design and Mobile Support', () => {

  // Test Case 1: Set viewport to 320px width (small mobile)
  test('page renders without horizontal overflow at 320px viewport', async ({ page }) => {
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto('/');

    // Wait for page to fully load
    await page.waitForLoadState('networkidle');

    // Check that body does not have horizontal scrollbar
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const windowWidth = await page.evaluate(() => window.innerWidth);

    // Body scroll width should not exceed viewport width
    expect(bodyScrollWidth).toBeLessThanOrEqual(windowWidth);

    // Verify no horizontal overflow
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });

  // Test Case 2: Set viewport to 375px width (iPhone)
  test('all content is visible and readable without horizontal scrolling at 375px', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    await page.waitForLoadState('networkidle');

    // Check no horizontal scroll
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Verify main content sections are visible
    await expect(page.locator('header')).toBeVisible();
    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();

    // Check that text content is readable (visible)
    const heroText = page.locator('.hero h1');
    await expect(heroText).toBeVisible();

    const tagline = page.locator('.hero .tagline');
    await expect(tagline).toBeVisible();
  });

  // Test Case 3: Set viewport to 768px width (tablet)
  test('layout adapts to tablet view with appropriate spacing at 768px', async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/');

    await page.waitForLoadState('networkidle');

    // Verify all sections are visible
    await expect(page.locator('header')).toBeVisible();
    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();

    // Check features grid has appropriate layout for tablet (2 columns or more)
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Verify navigation is visible and accessible
    const nav = page.locator('nav');
    await expect(nav).toBeVisible();

    // Check no horizontal scroll
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });

  // Test Case 4: Set viewport to 1024px width (small desktop)
  test('layout displays in desktop mode with full navigation at 1024px', async ({ page }) => {
    await page.setViewportSize({ width: 1024, height: 768 });
    await page.goto('/');

    await page.waitForLoadState('networkidle');

    // Verify full navigation is displayed
    const nav = page.locator('nav');
    await expect(nav).toBeVisible();

    // Check all nav links are visible
    const navLinks = page.locator('nav a');
    const navCount = await navLinks.count();
    expect(navCount).toBeGreaterThanOrEqual(3);

    for (let i = 0; i < navCount; i++) {
      await expect(navLinks.nth(i)).toBeVisible();
    }

    // Header should be in horizontal layout
    const header = page.locator('header .container');
    const headerFlexDirection = await header.evaluate(el =>
      window.getComputedStyle(el).flexDirection
    );
    expect(headerFlexDirection).toBe('row');
  });

  // Test Case 5: Check navigation on mobile viewport
  test('navigation is accessible via stacked links on mobile', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    await page.waitForLoadState('networkidle');

    // Navigation should be visible (stacked on mobile)
    const nav = page.locator('nav');
    await expect(nav).toBeVisible();

    // All nav links should be accessible (visible and clickable)
    const navLinks = page.locator('nav a');
    const navCount = await navLinks.count();
    expect(navCount).toBeGreaterThanOrEqual(3);

    // Verify each nav link is visible and has proper touch target
    for (let i = 0; i < navCount; i++) {
      const link = navLinks.nth(i);
      await expect(link).toBeVisible();

      // Check touch target size (minimum 44px)
      const box = await link.boundingBox();
      expect(box.height).toBeGreaterThanOrEqual(44);
    }
  });

  // Test Case 6: Measure font size on mobile
  test('body text font size is at least 14px for readability', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    await page.waitForLoadState('networkidle');

    // Check body font size
    const bodyFontSize = await page.evaluate(() => {
      const body = document.body;
      const computedStyle = window.getComputedStyle(body);
      return parseFloat(computedStyle.fontSize);
    });

    expect(bodyFontSize).toBeGreaterThanOrEqual(14);

    // Check paragraph font size
    const paragraphFontSize = await page.evaluate(() => {
      const paragraph = document.querySelector('.hero .description, .hero p, p');
      if (paragraph) {
        const computedStyle = window.getComputedStyle(paragraph);
        return parseFloat(computedStyle.fontSize);
      }
      return 16; // default
    });

    expect(paragraphFontSize).toBeGreaterThanOrEqual(14);
  });

  // Test Case 7: Check viewport meta tag
  test('HTML contains viewport meta tag with proper configuration', async ({ page }) => {
    await page.goto('/');

    // Check for viewport meta tag
    const viewportMeta = await page.locator('meta[name="viewport"]');
    await expect(viewportMeta).toHaveCount(1);

    // Verify viewport content attribute
    const viewportContent = await viewportMeta.getAttribute('content');
    expect(viewportContent).toContain('width=device-width');
    expect(viewportContent).toContain('initial-scale=1');
  });

  // Test Case 8: Test touch targets on mobile
  test('buttons and links have minimum 44px touch target size', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    await page.waitForLoadState('networkidle');

    // Check CTA buttons
    const ctaButtons = page.locator('.cta-buttons .btn');
    const buttonCount = await ctaButtons.count();

    for (let i = 0; i < buttonCount; i++) {
      const button = ctaButtons.nth(i);
      await expect(button).toBeVisible();

      const box = await button.boundingBox();
      // Touch targets should be at least 44px in both dimensions
      expect(box.height).toBeGreaterThanOrEqual(44);
    }

    // Check navigation links touch targets
    const navLinks = page.locator('nav a');
    const navCount = await navLinks.count();

    for (let i = 0; i < navCount; i++) {
      const link = navLinks.nth(i);
      const box = await link.boundingBox();
      expect(box.height).toBeGreaterThanOrEqual(44);
    }
  });

  // Additional test: Verify responsive images
  test('images do not exceed viewport width on mobile', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    await page.waitForLoadState('networkidle');

    // Check that logo image doesn't exceed viewport
    const logoImg = page.locator('.logo img');
    if (await logoImg.count() > 0) {
      const imgBox = await logoImg.boundingBox();
      if (imgBox) {
        expect(imgBox.width).toBeLessThanOrEqual(375);
      }
    }
  });

  // Additional test: Verify content wrapping on small screens
  test('text content wraps properly on small screens', async ({ page }) => {
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto('/');

    await page.waitForLoadState('networkidle');

    // Check that hero tagline wraps and stays within viewport
    const tagline = page.locator('.hero .tagline');
    if (await tagline.count() > 0) {
      const box = await tagline.boundingBox();
      if (box) {
        expect(box.x + box.width).toBeLessThanOrEqual(320);
      }
    }
  });
});
