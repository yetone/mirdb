/**
 * Responsive Design E2E Tests
 * Owner: Scenario 6 - Responsive Design
 *
 * Verifies the homepage displays correctly across mobile, tablet, and desktop viewports.
 * Tests responsive behavior for REQ-6, NFR-2, US-5.
 *
 * Test Cases:
 * 1. Mobile viewport (375px) - no horizontal scrolling
 * 2. CTA button touch targets - minimum 44x44px
 * 3. Mobile navigation menu presence
 * 4. Mobile navigation menu functionality
 * 5. Tablet viewport (768px) layout adaptation
 * 6. Desktop viewport (1280px) full layout
 * 7. Feature cards layout adaptation across viewports
 * 8. Text readability at all viewports
 */

import { test, expect, type Page } from '@playwright/test';

// Viewport configurations for responsive testing
const VIEWPORTS = {
  mobile: { width: 375, height: 812 },
  tablet: { width: 768, height: 1024 },
  desktop: { width: 1280, height: 800 },
};

// Minimum touch target size per WCAG guidelines (44x44px)
const MIN_TOUCH_TARGET = 44;

// Minimum readable font size (16px recommended, 12px minimum)
const MIN_READABLE_FONT_SIZE = 12;

/**
 * Helper to check if element has no horizontal overflow
 */
async function hasNoHorizontalScroll(page: Page): Promise<boolean> {
  return page.evaluate(() => {
    return document.documentElement.scrollWidth <= document.documentElement.clientWidth;
  });
}

/**
 * Helper to get element dimensions including touch target area
 */
async function getElementTouchTargetSize(
  page: Page,
  selector: string
): Promise<{ width: number; height: number }> {
  const element = page.locator(selector).first();
  const box = await element.boundingBox();
  if (!box) {
    throw new Error(`Element not found: ${selector}`);
  }
  return { width: box.width, height: box.height };
}

/**
 * Helper to get computed font size of an element
 */
async function getFontSize(page: Page, selector: string): Promise<number> {
  const element = page.locator(selector).first();
  const fontSize = await element.evaluate((el) => {
    return parseFloat(window.getComputedStyle(el).fontSize);
  });
  return fontSize;
}

test.describe('Responsive Design - Mobile Viewport (375px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.goto('/');
  });

  /**
   * Test Case 1: Render homepage at 375px width (mobile)
   * Expected: All content is visible without horizontal scrolling
   */
  test('TC1: should display all content without horizontal scrolling on mobile', async ({
    page,
  }) => {
    // Wait for page content to load
    await page.waitForSelector('h1');

    // Check no horizontal scrolling
    const noHorizontalScroll = await hasNoHorizontalScroll(page);
    expect(noHorizontalScroll).toBe(true);

    // Verify main sections are visible
    await expect(page.locator('nav')).toBeVisible();
    await expect(page.locator('h1')).toBeVisible();
    await expect(page.locator('footer')).toBeVisible();
  });

  /**
   * Test Case 2: Check CTA button touch targets at mobile viewport
   * Expected: Buttons have minimum 44x44px touch target area
   */
  test('TC2: should have adequate touch target sizes for CTA buttons', async ({ page }) => {
    // Wait for page content to load
    await page.waitForSelector('h1');

    // Check primary CTA button (Get Started Free) in the hero section
    // Use a visible locator to find the CTA buttons in the main content area
    const primaryCTA = page.locator('section a[href="/register"] button').first();
    await expect(primaryCTA).toBeVisible();
    const primaryBox = await primaryCTA.boundingBox();
    expect(primaryBox).not.toBeNull();
    // The clickable area should meet minimum touch target
    expect(primaryBox!.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
    expect(primaryBox!.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);

    // Check secondary CTA button (Sign In) in the hero section
    const secondaryCTA = page.locator('section a[href="/login"] button').first();
    await expect(secondaryCTA).toBeVisible();
    const secondaryBox = await secondaryCTA.boundingBox();
    expect(secondaryBox).not.toBeNull();
    expect(secondaryBox!.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
    expect(secondaryBox!.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
  });

  /**
   * Test Case 3: Check for mobile navigation menu at 375px width
   * Expected: Hamburger menu or mobile nav is present
   */
  test('TC3: should display hamburger menu on mobile viewport', async ({ page }) => {
    // Desktop nav should be hidden on mobile
    const desktopNav = page.locator('.hidden.md\\:flex');
    await expect(desktopNav).toBeHidden();

    // Mobile menu button should be visible
    const mobileMenuButton = page.locator('button[aria-label="Toggle menu"]');
    await expect(mobileMenuButton).toBeVisible();

    // Menu button should be tappable (adequate size)
    const menuBox = await mobileMenuButton.boundingBox();
    expect(menuBox).not.toBeNull();
    expect(menuBox!.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
    expect(menuBox!.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
  });

  /**
   * Test Case 4: Open and close mobile navigation menu
   * Expected: Menu opens and closes correctly, showing nav links
   */
  test('TC4: should open and close mobile navigation menu correctly', async ({ page }) => {
    const mobileMenuButton = page.locator('button[aria-label="Toggle menu"]');

    // Open menu
    await mobileMenuButton.click();
    await page.waitForTimeout(300); // Wait for animation

    // Menu should be open with nav links visible
    const mobileMenu = page.locator('.md\\:hidden.absolute');
    await expect(mobileMenu).toBeVisible();

    // Check for Login and Sign Up links
    const loginLink = page.locator('[data-testid="mobile-login-link"]');
    const signUpLink = page.locator('[data-testid="mobile-register-button"]');
    await expect(loginLink).toBeVisible();
    await expect(signUpLink).toBeVisible();

    // Close menu
    await mobileMenuButton.click();
    await page.waitForTimeout(300); // Wait for animation

    // Menu should be closed
    await expect(mobileMenu).toBeHidden();
  });
});

test.describe('Responsive Design - Tablet Viewport (768px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.tablet);
    await page.goto('/');
  });

  /**
   * Test Case 5: Render homepage at 768px width (tablet)
   * Expected: Layout adapts to tablet viewport appropriately
   */
  test('TC5: should adapt layout appropriately for tablet viewport', async ({ page }) => {
    // Wait for page content to load
    await page.waitForSelector('h1');

    // Check no horizontal scrolling
    const noHorizontalScroll = await hasNoHorizontalScroll(page);
    expect(noHorizontalScroll).toBe(true);

    // Verify main sections are visible and properly sized
    const hero = page.locator('section').first();
    await expect(hero).toBeVisible();

    // Feature cards should be in a grid layout (2 columns on tablet)
    const featuresGrid = page.locator('[data-testid="feature-cards-grid"]');
    await expect(featuresGrid).toBeVisible();

    // Check grid has appropriate styling for tablet
    const gridClass = await featuresGrid.getAttribute('class');
    expect(gridClass).toContain('md:grid-cols-2');

    // Verify hero section text is properly sized
    const headline = page.locator('h1');
    const headlineBox = await headline.boundingBox();
    expect(headlineBox).not.toBeNull();
    // Headline should fit within viewport
    expect(headlineBox!.width).toBeLessThanOrEqual(VIEWPORTS.tablet.width);
  });
});

test.describe('Responsive Design - Desktop Viewport (1280px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.desktop);
    await page.goto('/');
  });

  /**
   * Test Case 6: Render homepage at 1280px width (desktop)
   * Expected: Full desktop layout is displayed
   */
  test('TC6: should display full desktop layout at 1280px width', async ({ page }) => {
    // Wait for page content to load
    await page.waitForSelector('h1');

    // Check no horizontal scrolling
    const noHorizontalScroll = await hasNoHorizontalScroll(page);
    expect(noHorizontalScroll).toBe(true);

    // Desktop navigation should be visible
    const desktopNav = page.locator('.hidden.md\\:flex');
    await expect(desktopNav).toBeVisible();

    // Mobile menu button should be hidden
    const mobileMenuButton = page.locator('button[aria-label="Toggle menu"]');
    await expect(mobileMenuButton).toBeHidden();

    // Feature cards should be in a 4-column grid on desktop
    const featuresGrid = page.locator('[data-testid="feature-cards-grid"]');
    await expect(featuresGrid).toBeVisible();
    const gridClass = await featuresGrid.getAttribute('class');
    expect(gridClass).toContain('lg:grid-cols-4');

    // All sections should be visible
    await expect(page.locator('nav')).toBeVisible();
    await expect(page.locator('h1')).toBeVisible();
    await expect(page.locator('[data-testid="features-section"]')).toBeVisible();
    await expect(page.locator('footer')).toBeVisible();
  });

  /**
   * Test Case 7: Check feature cards layout at different viewports
   * Expected: Cards stack on mobile, grid on desktop
   */
  test('TC7: should display feature cards in 4-column grid on desktop', async ({ page }) => {
    const featuresGrid = page.locator('[data-testid="feature-cards-grid"]');
    await expect(featuresGrid).toBeVisible();

    // Count feature cards
    const featureCards = page.locator('[data-testid="feature-cards-grid"] > div');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(3); // At least 3 features

    // Verify grid is horizontal on desktop (cards side by side)
    const firstCard = featureCards.first();
    const secondCard = featureCards.nth(1);
    const firstBox = await firstCard.boundingBox();
    const secondBox = await secondCard.boundingBox();

    expect(firstBox).not.toBeNull();
    expect(secondBox).not.toBeNull();

    // Cards should be side by side on desktop (same or similar Y position)
    const yDifference = Math.abs(firstBox!.y - secondBox!.y);
    expect(yDifference).toBeLessThan(50); // Allow small variance for alignment
  });
});

test.describe('Responsive Design - Feature Cards Layout Comparison', () => {
  /**
   * Test Case 7 (continued): Verify cards stack on mobile vs grid on desktop
   */
  test('TC7-mobile: should stack feature cards vertically on mobile', async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.goto('/');

    const featuresGrid = page.locator('[data-testid="feature-cards-grid"]');
    await expect(featuresGrid).toBeVisible();

    // Verify grid is vertical on mobile (cards stacked)
    const featureCards = page.locator('[data-testid="feature-cards-grid"] > div');
    const firstCard = featureCards.first();
    const secondCard = featureCards.nth(1);
    const firstBox = await firstCard.boundingBox();
    const secondBox = await secondCard.boundingBox();

    expect(firstBox).not.toBeNull();
    expect(secondBox).not.toBeNull();

    // Cards should be stacked on mobile (second card below first)
    expect(secondBox!.y).toBeGreaterThan(firstBox!.y);
  });
});

test.describe('Responsive Design - Text Readability', () => {
  /**
   * Test Case 8: Verify text readability at all viewports
   * Expected: All text is readable without zooming
   */
  test('TC8-mobile: should have readable text sizes on mobile', async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.goto('/');

    // Check headline font size
    const headlineFontSize = await getFontSize(page, 'h1');
    expect(headlineFontSize).toBeGreaterThanOrEqual(24); // Headlines should be prominent

    // Check body text font size
    const bodyTextFontSize = await getFontSize(page, 'p');
    expect(bodyTextFontSize).toBeGreaterThanOrEqual(MIN_READABLE_FONT_SIZE);
  });

  test('TC8-tablet: should have readable text sizes on tablet', async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.tablet);
    await page.goto('/');

    // Check headline font size
    const headlineFontSize = await getFontSize(page, 'h1');
    expect(headlineFontSize).toBeGreaterThanOrEqual(24);

    // Check body text font size
    const bodyTextFontSize = await getFontSize(page, 'p');
    expect(bodyTextFontSize).toBeGreaterThanOrEqual(MIN_READABLE_FONT_SIZE);

    // Check feature section heading
    const sectionHeadingFontSize = await getFontSize(page, '#features-heading');
    expect(sectionHeadingFontSize).toBeGreaterThanOrEqual(20);
  });

  test('TC8-desktop: should have readable text sizes on desktop', async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.desktop);
    await page.goto('/');

    // Check headline font size (should be larger on desktop)
    const headlineFontSize = await getFontSize(page, 'h1');
    expect(headlineFontSize).toBeGreaterThanOrEqual(32);

    // Check body text font size
    const bodyTextFontSize = await getFontSize(page, 'p');
    expect(bodyTextFontSize).toBeGreaterThanOrEqual(MIN_READABLE_FONT_SIZE);
  });
});

test.describe('Responsive Design - Cross-Viewport Consistency', () => {
  test('should maintain consistent branding across all viewports', async ({ page }) => {
    const viewports = [VIEWPORTS.mobile, VIEWPORTS.tablet, VIEWPORTS.desktop];

    for (const viewport of viewports) {
      await page.setViewportSize(viewport);
      await page.goto('/');

      // Logo/Brand should always be visible
      const logo = page.locator('[data-testid="logo-link"]');
      await expect(logo).toBeVisible();

      // Theme toggle should always be accessible somewhere on the page
      // There are multiple theme toggles (desktop and mobile), check that at least one is visible
      const visibleThemeToggles = page.locator('[data-testid="theme-toggle"]:visible');
      const count = await visibleThemeToggles.count();
      expect(count, `Expected at least one visible theme toggle at ${viewport.width}px`).toBeGreaterThan(0);
    }
  });

  test('should have no content overflow at any viewport', async ({ page }) => {
    const viewports = [VIEWPORTS.mobile, VIEWPORTS.tablet, VIEWPORTS.desktop];

    for (const viewport of viewports) {
      await page.setViewportSize(viewport);
      await page.goto('/');
      await page.waitForSelector('h1');

      const noHorizontalScroll = await hasNoHorizontalScroll(page);
      expect(
        noHorizontalScroll,
        `Horizontal scroll detected at ${viewport.width}px width`
      ).toBe(true);
    }
  });
});
