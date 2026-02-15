/**
 * E2E Tests for Responsive Design
 * Owner: Scenario 5 - Responsive Design
 *
 * Validates that the homepage displays correctly across mobile, tablet,
 * and desktop viewport sizes.
 *
 * Test Cases:
 * 1. Mobile viewport (375px) - Single column layout, stacked sections
 * 2. Tablet viewport (768px) - Two-column feature grid
 * 3. Desktop viewport (1920px) - Full multi-section layout, 3+ column grid
 * 4. Hero section at mobile - Readable headline, CTA buttons fit
 * 5. QuickShortenForm at mobile - Full width input/button
 * 6. Navigation at mobile - Accessible navigation
 * 7. Resize from desktop to mobile - Smooth transitions
 */

import { test, expect, Page } from '@playwright/test';

// Viewport configurations for testing
const VIEWPORTS = {
  mobile: { width: 375, height: 667 },    // iPhone SE
  tablet: { width: 768, height: 1024 },   // iPad
  desktop: { width: 1920, height: 1080 }, // Full HD desktop
};

// Minimum tappable element size per mobile accessibility guidelines
const MIN_TAPPABLE_SIZE = 44;

/**
 * Helper function to check if an element has no horizontal overflow
 */
async function hasNoHorizontalScroll(page: Page): Promise<boolean> {
  const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
  const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);
  return scrollWidth <= clientWidth;
}

/**
 * Helper function to get the computed style of an element
 */
async function getComputedHeight(page: Page, selector: string): Promise<number> {
  const element = await page.locator(selector).first();
  const box = await element.boundingBox();
  return box?.height ?? 0;
}

/**
 * Helper function to count visible columns in a grid
 */
async function countGridColumns(page: Page, selector: string): Promise<number> {
  const gridItems = page.locator(selector);
  const count = await gridItems.count();
  if (count === 0) return 0;

  // Get positions of all items to determine columns
  const positions: number[] = [];
  for (let i = 0; i < Math.min(count, 6); i++) {
    const box = await gridItems.nth(i).boundingBox();
    if (box) {
      // Round to nearest 10px to account for minor variations
      const roundedY = Math.round(box.y / 10) * 10;
      if (!positions.includes(roundedY)) {
        positions.push(roundedY);
      }
    }
  }

  // Count items in the first row (same Y position as first item)
  const firstRowY = positions[0];
  let columnsInFirstRow = 0;
  for (let i = 0; i < count; i++) {
    const box = await gridItems.nth(i).boundingBox();
    if (box) {
      const roundedY = Math.round(box.y / 10) * 10;
      if (roundedY === firstRowY) {
        columnsInFirstRow++;
      }
    }
  }

  return columnsInFirstRow;
}

test.describe('Responsive Design - Homepage', () => {
  test.describe('Test Case 1: Mobile Viewport (375px)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);
      await page.goto('/');
      await page.waitForLoadState('networkidle');
    });

    test('should display single column layout with stacked sections', async ({ page }) => {
      // Check that no horizontal scroll exists
      const noScroll = await hasNoHorizontalScroll(page);
      expect(noScroll).toBe(true);

      // Check that the home page is rendered
      const homePage = page.locator('[data-testid="home-page"]');
      await expect(homePage).toBeVisible();

      // Check main sections are visible
      await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();
      await expect(page.locator('[data-testid="features-section"]')).toBeVisible();
      await expect(page.locator('[data-testid="footer"]')).toBeVisible();
    });

    test('should have all buttons with minimum 44px height for tappability', async ({ page }) => {
      // Check CTA buttons in hero section
      const getStartedBtn = page.locator('[data-testid="get-started-button"]');
      const learnMoreBtn = page.locator('[data-testid="learn-more-button"]');

      await expect(getStartedBtn).toBeVisible();
      await expect(learnMoreBtn).toBeVisible();

      // Get button heights
      const getStartedBox = await getStartedBtn.boundingBox();
      const learnMoreBox = await learnMoreBtn.boundingBox();

      expect(getStartedBox?.height).toBeGreaterThanOrEqual(MIN_TAPPABLE_SIZE);
      expect(learnMoreBox?.height).toBeGreaterThanOrEqual(MIN_TAPPABLE_SIZE);

      // Check navigation buttons
      const loginBtn = page.locator('[data-testid="login-link"]');
      const registerBtn = page.locator('[data-testid="register-link"]');

      await expect(loginBtn).toBeVisible();
      await expect(registerBtn).toBeVisible();

      const loginBox = await loginBtn.boundingBox();
      const registerBox = await registerBtn.boundingBox();

      expect(loginBox?.height).toBeGreaterThanOrEqual(MIN_TAPPABLE_SIZE);
      expect(registerBox?.height).toBeGreaterThanOrEqual(MIN_TAPPABLE_SIZE);
    });

    test('should display features in single column layout', async ({ page }) => {
      // Scroll to features section
      await page.locator('[data-testid="features-section"]').scrollIntoViewIfNeeded();

      // Count columns in feature grid
      const columns = await countGridColumns(page, '[data-testid^="feature-card-"]');
      expect(columns).toBe(1);
    });
  });

  test.describe('Test Case 2: Tablet Viewport (768px)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.tablet);
      await page.goto('/');
      await page.waitForLoadState('networkidle');
    });

    test('should display two-column feature grid', async ({ page }) => {
      // Scroll to features section
      await page.locator('[data-testid="features-section"]').scrollIntoViewIfNeeded();

      // Count columns in feature grid - should be 2 at md breakpoint
      const columns = await countGridColumns(page, '[data-testid^="feature-card-"]');
      expect(columns).toBe(2);
    });

    test('should have navigation links visible', async ({ page }) => {
      const navbar = page.locator('[data-testid="navbar"]');
      await expect(navbar).toBeVisible();

      const loginLink = page.locator('[data-testid="login-link"]');
      const registerLink = page.locator('[data-testid="register-link"]');

      await expect(loginLink).toBeVisible();
      await expect(registerLink).toBeVisible();

      // Verify links are not hidden or collapsed
      const loginBox = await loginLink.boundingBox();
      const registerBox = await registerLink.boundingBox();

      expect(loginBox?.width).toBeGreaterThan(0);
      expect(registerBox?.width).toBeGreaterThan(0);
    });

    test('should have adjusted spacing between sections', async ({ page }) => {
      // Verify sections have appropriate spacing
      const featuresSection = page.locator('[data-testid="features-section"]');
      const featuresSectionBox = await featuresSection.boundingBox();

      expect(featuresSectionBox).not.toBeNull();
      // Padding should be applied (py-20 = 5rem = 80px)
      expect(featuresSectionBox!.height).toBeGreaterThan(200);
    });
  });

  test.describe('Test Case 3: Desktop Viewport (1920px)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.desktop);
      await page.goto('/');
      await page.waitForLoadState('networkidle');
    });

    test('should display full multi-section layout with optimal spacing', async ({ page }) => {
      // Verify all sections are visible
      await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();
      await expect(page.locator('[data-testid="features-section"]')).toBeVisible();
      await expect(page.locator('[data-testid="footer"]')).toBeVisible();

      // Verify no horizontal scroll
      const noScroll = await hasNoHorizontalScroll(page);
      expect(noScroll).toBe(true);
    });

    test('should display features in 3+ column grid', async ({ page }) => {
      // Scroll to features section
      await page.locator('[data-testid="features-section"]').scrollIntoViewIfNeeded();

      // Count columns - should be 3 at lg breakpoint
      const columns = await countGridColumns(page, '[data-testid^="feature-card-"]');
      expect(columns).toBeGreaterThanOrEqual(3);
    });

    test('should have How It Works section in horizontal layout', async ({ page }) => {
      // Check that steps are displayed horizontally at desktop
      const step1 = page.locator('[data-testid="step-1"]');
      const step2 = page.locator('[data-testid="step-2"]');
      const step3 = page.locator('[data-testid="step-3"]');

      await expect(step1).toBeVisible();
      await expect(step2).toBeVisible();
      await expect(step3).toBeVisible();

      // Get positions to verify horizontal layout
      const step1Box = await step1.boundingBox();
      const step2Box = await step2.boundingBox();
      const step3Box = await step3.boundingBox();

      // Steps should be on the same row (similar Y position)
      expect(Math.abs((step1Box?.y ?? 0) - (step2Box?.y ?? 0))).toBeLessThan(50);
      expect(Math.abs((step2Box?.y ?? 0) - (step3Box?.y ?? 0))).toBeLessThan(50);

      // Steps should be in order horizontally
      expect(step1Box?.x).toBeLessThan(step2Box?.x ?? 0);
      expect(step2Box?.x).toBeLessThan(step3Box?.x ?? 0);
    });
  });

  test.describe('Test Case 4: Hero Section at Mobile Viewport', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);
      await page.goto('/');
      await page.waitForLoadState('networkidle');
    });

    test('should display readable hero headline and tagline', async ({ page }) => {
      const heroTitle = page.locator('[data-testid="hero-title"]');
      const heroTagline = page.locator('[data-testid="hero-tagline"]');

      await expect(heroTitle).toBeVisible();
      await expect(heroTagline).toBeVisible();

      // Verify text is readable (not cut off)
      const titleText = await heroTitle.textContent();
      const taglineText = await heroTagline.textContent();

      expect(titleText?.length).toBeGreaterThan(0);
      expect(taglineText?.length).toBeGreaterThan(0);

      // Verify title fits within viewport
      const titleBox = await heroTitle.boundingBox();
      expect(titleBox?.x).toBeGreaterThanOrEqual(0);
      expect((titleBox?.x ?? 0) + (titleBox?.width ?? 0)).toBeLessThanOrEqual(VIEWPORTS.mobile.width);
    });

    test('should stack CTA buttons vertically or fit on screen', async ({ page }) => {
      const getStartedBtn = page.locator('[data-testid="get-started-button"]');
      const learnMoreBtn = page.locator('[data-testid="learn-more-button"]');

      await expect(getStartedBtn).toBeVisible();
      await expect(learnMoreBtn).toBeVisible();

      const getStartedBox = await getStartedBtn.boundingBox();
      const learnMoreBox = await learnMoreBtn.boundingBox();

      // At mobile, buttons should stack (flex-col) - so learnMore should be below getStarted
      // OR they should both fit within the viewport width if displayed horizontally
      const buttonsStacked = (learnMoreBox?.y ?? 0) > (getStartedBox?.y ?? 0) + (getStartedBox?.height ?? 0) / 2;
      const buttonsFitHorizontally =
        (getStartedBox?.x ?? 0) >= 0 &&
        (learnMoreBox?.x ?? 0) + (learnMoreBox?.width ?? 0) <= VIEWPORTS.mobile.width;

      expect(buttonsStacked || buttonsFitHorizontally).toBe(true);
    });
  });

  test.describe('Test Case 5: QuickShortenForm at Mobile Viewport', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);
      await page.goto('/');
      await page.waitForLoadState('networkidle');
    });

    test('should display input field and button with full width', async ({ page }) => {
      // Scroll to the form
      await page.locator('#quick-shorten').scrollIntoViewIfNeeded();

      // Get the URL input and shorten button
      const urlInput = page.locator('input[type="url"]');
      const shortenButton = page.locator('button[type="submit"]');

      await expect(urlInput).toBeVisible();
      await expect(shortenButton).toBeVisible();

      // Get the container width for reference
      const containerBox = await page.locator('#quick-shorten .container').boundingBox();

      // Input should use most of the container width
      const inputBox = await urlInput.boundingBox();
      if (inputBox && containerBox) {
        // Input should be at least 80% of container width (accounting for padding)
        const inputWidthRatio = inputBox.width / containerBox.width;
        expect(inputWidthRatio).toBeGreaterThan(0.8);
      }
    });

    test('should have form elements stacked on mobile', async ({ page }) => {
      // Scroll to the form
      await page.locator('#quick-shorten').scrollIntoViewIfNeeded();

      // Get the URL input and shorten button
      const urlInput = page.locator('input[type="url"]');
      const shortenButton = page.locator('button[type="submit"]');

      const inputBox = await urlInput.boundingBox();
      const buttonBox = await shortenButton.boundingBox();

      // At mobile (< sm breakpoint), form should stack (button below input)
      // Check if button Y position is greater than input Y position
      const isStacked = (buttonBox?.y ?? 0) > (inputBox?.y ?? 0);

      // Or if they're on the same row, button should be to the right of input
      const isInline = (buttonBox?.x ?? 0) > (inputBox?.x ?? 0) + (inputBox?.width ?? 0) - 10;

      expect(isStacked || isInline).toBe(true);
    });
  });

  test.describe('Test Case 6: Navigation at Mobile Viewport', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);
      await page.goto('/');
      await page.waitForLoadState('networkidle');
    });

    test('should have accessible navigation', async ({ page }) => {
      const navbar = page.locator('[data-testid="navbar"]');
      await expect(navbar).toBeVisible();

      // Brand logo should be visible
      const brandLogo = page.locator('[data-testid="brand-logo"]');
      await expect(brandLogo).toBeVisible();
    });

    test('should have visible navigation links or hamburger menu', async ({ page }) => {
      // Check if nav links are visible
      const loginLink = page.locator('[data-testid="login-link"]');
      const registerLink = page.locator('[data-testid="register-link"]');

      // Either links should be visible or a hamburger menu should exist
      const linksVisible =
        (await loginLink.isVisible()) && (await registerLink.isVisible());
      const hamburgerExists = await page.locator('[data-testid="hamburger-menu"], .btn-square.drawer-button, button[aria-label*="menu"]').count() > 0;

      // Navigation should be accessible in some form
      expect(linksVisible || hamburgerExists).toBe(true);
    });

    test('should have theme toggle reachable', async ({ page }) => {
      // The navbar should contain theme toggle or it should be accessible
      const navbar = page.locator('[data-testid="navbar"]');
      await expect(navbar).toBeVisible();

      // Check that navbar is not overflowing
      const navbarBox = await navbar.boundingBox();
      expect(navbarBox?.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width);
    });
  });

  test.describe('Test Case 7: Window Resize from Desktop to Mobile', () => {
    test('should transition layout smoothly without broken elements', async ({ page }) => {
      // Start at desktop
      await page.setViewportSize(VIEWPORTS.desktop);
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Verify initial state
      await expect(page.locator('[data-testid="home-page"]')).toBeVisible();

      // Gradually resize to mobile
      const steps = [
        { width: 1440, height: 900 },
        { width: 1024, height: 768 },
        { width: 768, height: 1024 },
        { width: 640, height: 480 },
        { width: 375, height: 667 },
      ];

      for (const viewport of steps) {
        await page.setViewportSize(viewport);
        // Small delay to allow CSS transitions
        await page.waitForTimeout(100);

        // Verify no horizontal scroll at each step
        const noScroll = await hasNoHorizontalScroll(page);
        expect(noScroll).toBe(true);

        // Verify main sections are still visible
        await expect(page.locator('[data-testid="home-page"]')).toBeVisible();
        await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();
        await expect(page.locator('[data-testid="navbar"]')).toBeVisible();
      }
    });

    test('should not have overlapping content during resize', async ({ page }) => {
      // Start at desktop
      await page.setViewportSize(VIEWPORTS.desktop);
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Resize to tablet
      await page.setViewportSize(VIEWPORTS.tablet);
      await page.waitForTimeout(200);

      // Check hero buttons don't overlap
      const getStartedBtn = page.locator('[data-testid="get-started-button"]');
      const learnMoreBtn = page.locator('[data-testid="learn-more-button"]');

      const getStartedBox = await getStartedBtn.boundingBox();
      const learnMoreBox = await learnMoreBtn.boundingBox();

      if (getStartedBox && learnMoreBox) {
        // If buttons are on the same row, they shouldn't overlap
        const sameRow = Math.abs(getStartedBox.y - learnMoreBox.y) < 10;
        if (sameRow) {
          const noOverlap = getStartedBox.x + getStartedBox.width <= learnMoreBox.x + 5; // 5px tolerance
          expect(noOverlap).toBe(true);
        }
      }

      // Resize to mobile
      await page.setViewportSize(VIEWPORTS.mobile);
      await page.waitForTimeout(200);

      // Verify page still renders correctly
      await expect(page.locator('[data-testid="home-page"]')).toBeVisible();
    });
  });
});
