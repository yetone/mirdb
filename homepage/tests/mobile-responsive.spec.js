// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for MirDB Homepage Mobile Responsive Design
 *
 * Scenario: Verify the homepage displays correctly on mobile devices
 *
 * Test Case 1: Page renders without horizontal scroll at 375px viewport
 * Test Case 2: Hamburger menu icon is visible and clickable
 * Test Case 3: Mobile navigation menu expands showing all links
 * Test Case 4: Hero content is readable and properly sized
 * Test Case 5: Feature cards stack vertically and are fully visible
 */

test.describe('Mobile Responsive Design - Mobile View', () => {
  // Use mobile viewport for all tests in this suite
  test.use({ viewport: { width: 375, height: 667 } });

  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/');
    // Wait for page to be fully loaded
    await page.waitForLoadState('networkidle');
  });

  test('Test Case 1: Page renders without horizontal scroll at 375px viewport', async ({ page }) => {
    // Get the document scroll width and viewport width
    const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // Verify no horizontal scrollbar (scroll width should equal viewport width)
    expect(scrollWidth).toBeLessThanOrEqual(viewportWidth);

    // Verify the body doesn't overflow horizontally
    const bodyOverflow = await page.evaluate(() => {
      const body = document.body;
      return body.scrollWidth <= body.clientWidth + 1; // Allow 1px tolerance
    });
    expect(bodyOverflow).toBe(true);

    // Verify no element extends beyond viewport
    const hasOverflow = await page.evaluate(() => {
      const elements = document.querySelectorAll('*');
      for (const el of elements) {
        const rect = el.getBoundingClientRect();
        if (rect.right > window.innerWidth + 1) { // Allow 1px tolerance
          return true;
        }
      }
      return false;
    });
    expect(hasOverflow).toBe(false);
  });

  test('Test Case 2: Hamburger menu icon is visible and clickable', async ({ page }) => {
    // Verify hamburger menu toggle button is visible on mobile
    const hamburgerButton = page.getByTestId('mobile-menu-toggle');
    await expect(hamburgerButton).toBeVisible();

    // Verify it is clickable (enabled)
    await expect(hamburgerButton).toBeEnabled();

    // Verify the hamburger icon (three lines) is visible
    const hamburgerIcon = page.locator('[data-testid="mobile-menu-toggle"] .hamburger-icon');
    await expect(hamburgerIcon).toBeVisible();

    // Verify the button has appropriate accessible name
    const ariaLabel = await hamburgerButton.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel?.toLowerCase()).toMatch(/menu|navigation|nav/);
  });

  test('Test Case 3: Mobile navigation menu expands showing all links', async ({ page }) => {
    // First verify mobile nav is initially hidden/collapsed
    const mobileNav = page.getByTestId('mobile-nav-menu');

    // The mobile nav should exist but be hidden initially
    await expect(mobileNav).toBeHidden();

    // Click the hamburger menu toggle
    const hamburgerButton = page.getByTestId('mobile-menu-toggle');
    await hamburgerButton.click();

    // Wait for animation
    await page.waitForTimeout(300);

    // Verify mobile nav is now visible
    await expect(mobileNav).toBeVisible();

    // Verify all navigation links are visible in the expanded menu
    const featuresLink = mobileNav.locator('a[href="#features"]');
    const docsLink = mobileNav.locator('a:has-text("Documentation")');
    const githubLink = mobileNav.locator('a:has-text("GitHub")');

    await expect(featuresLink).toBeVisible();
    await expect(docsLink).toBeVisible();
    await expect(githubLink).toBeVisible();

    // Verify the menu can be closed
    await hamburgerButton.click();
    await page.waitForTimeout(300);
    await expect(mobileNav).toBeHidden();
  });

  test('Test Case 4: Hero content is readable and properly sized', async ({ page }) => {
    // Verify hero section is visible
    const heroSection = page.getByTestId('hero-section');
    await expect(heroSection).toBeVisible();

    // Verify hero title is visible and readable
    const heroTitle = page.getByTestId('product-name');
    await expect(heroTitle).toBeVisible();

    // Verify the title has appropriate font size for mobile (not too small)
    const titleFontSize = await heroTitle.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    expect(titleFontSize).toBeGreaterThanOrEqual(24); // At least 24px for readability

    // Verify hero tagline is visible
    const heroTagline = page.getByTestId('hero-tagline');
    await expect(heroTagline).toBeVisible();

    // Verify tagline font size is readable
    const taglineFontSize = await heroTagline.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    expect(taglineFontSize).toBeGreaterThanOrEqual(14); // At least 14px for readability

    // Verify CTA buttons are visible and properly sized
    const ctaSection = page.getByTestId('hero-cta');
    await expect(ctaSection).toBeVisible();

    const getStartedBtn = page.getByTestId('cta-get-started');
    const docsBtn = page.getByTestId('cta-documentation');

    await expect(getStartedBtn).toBeVisible();
    await expect(docsBtn).toBeVisible();

    // Verify buttons are within viewport
    const getStartedBox = await getStartedBtn.boundingBox();
    const docsBox = await docsBtn.boundingBox();

    expect(getStartedBox).not.toBeNull();
    expect(docsBox).not.toBeNull();

    // Buttons should be within the 375px viewport
    if (getStartedBox && docsBox) {
      expect(getStartedBox.x + getStartedBox.width).toBeLessThanOrEqual(375);
      expect(docsBox.x + docsBox.width).toBeLessThanOrEqual(375);
    }
  });

  test('Test Case 5: Feature cards stack vertically and are fully visible', async ({ page }) => {
    // Scroll to features section
    const featuresSection = page.getByTestId('features-section');
    await featuresSection.scrollIntoViewIfNeeded();

    // Verify features section is visible
    await expect(featuresSection).toBeVisible();

    // Get all feature cards
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    // Verify we have feature cards
    expect(cardCount).toBeGreaterThan(0);

    // Verify cards are stacked vertically (each card should be on its own row)
    const cardPositions = [];
    for (let i = 0; i < Math.min(cardCount, 4); i++) {
      const card = featureCards.nth(i);
      const box = await card.boundingBox();
      if (box) {
        cardPositions.push({ top: box.y, left: box.x, width: box.width });
      }
    }

    // Cards should be stacked vertically - each subsequent card should have a larger Y position
    for (let i = 1; i < cardPositions.length; i++) {
      expect(cardPositions[i].top).toBeGreaterThan(cardPositions[i - 1].top);
    }

    // Verify each card is fully visible (width within viewport)
    for (let i = 0; i < Math.min(cardCount, 4); i++) {
      const card = featureCards.nth(i);
      await card.scrollIntoViewIfNeeded();

      const box = await card.boundingBox();
      expect(box).not.toBeNull();
      if (box) {
        // Card should fit within viewport width (375px minus some padding)
        expect(box.width).toBeLessThanOrEqual(375);
        expect(box.x).toBeGreaterThanOrEqual(0);
        expect(box.x + box.width).toBeLessThanOrEqual(376); // Allow 1px tolerance
      }

      // Verify card content is visible
      const title = card.locator('[data-testid="feature-title"]');
      const description = card.locator('[data-testid="feature-description"]');

      await expect(title).toBeVisible();
      await expect(description).toBeVisible();
    }
  });

  test('Desktop navigation is hidden on mobile', async ({ page }) => {
    // On mobile, the desktop nav links should be hidden
    const desktopNavLinks = page.locator('.nav-links');
    await expect(desktopNavLinks).toBeHidden();

    // But hamburger menu should be visible
    const hamburgerButton = page.getByTestId('mobile-menu-toggle');
    await expect(hamburgerButton).toBeVisible();
  });
});
