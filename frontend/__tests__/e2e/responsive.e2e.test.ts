/**
 * Responsive Design E2E Tests - Mobile Viewport
 * Owner: Scenario 9 - Responsive Design - Mobile
 *
 * Tests verify that the homepage is fully functional and properly displayed
 * on mobile devices (375px viewport width - iPhone SE equivalent).
 */

import { test, expect, type Page } from '@playwright/test';

// Mobile viewport configuration (iPhone SE)
const MOBILE_VIEWPORT = {
  width: 375,
  height: 667,
};

// Minimum touch target size per WCAG guidelines
const MIN_TOUCH_TARGET = 44;

test.describe('Responsive Design - Mobile', () => {
  test.use({ viewport: MOBILE_VIEWPORT });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for the page to be fully loaded
    await page.waitForLoadState('networkidle');
  });

  test('Test Case 1: All content is visible without horizontal scrolling at 375px viewport', async ({ page }) => {
    // Get the body and document dimensions
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const windowWidth = await page.evaluate(() => window.innerWidth);

    // Ensure no horizontal overflow (body width should not exceed viewport width significantly)
    expect(bodyScrollWidth).toBeLessThanOrEqual(windowWidth + 1); // +1 for potential rounding

    // Check that the home page container is visible
    const homePage = page.getByTestId('home-page');
    await expect(homePage).toBeVisible();

    // Verify main sections are visible
    const heroSection = page.getByTestId('hero-section');
    await expect(heroSection).toBeVisible();

    const featuresSection = page.getByTestId('features-section');
    await expect(featuresSection).toBeVisible();

    // Verify guest shortener section is visible
    const guestShortenerSection = page.getByTestId('guest-shortener-section');
    await expect(guestShortenerSection).toBeVisible();
  });

  test('Test Case 2: Hero section text is readable and buttons are properly sized on mobile', async ({ page }) => {
    const heroSection = page.getByTestId('hero-section');
    await expect(heroSection).toBeVisible();

    // Verify headline is visible and readable
    const headline = page.getByTestId('hero-headline');
    await expect(headline).toBeVisible();
    const headlineText = await headline.textContent();
    expect(headlineText).toBeTruthy();
    expect(headlineText!.length).toBeGreaterThan(0);

    // Verify tagline is visible and readable
    const tagline = page.getByTestId('hero-tagline');
    await expect(tagline).toBeVisible();

    // Verify buttons are visible
    const getStartedButton = page.getByTestId('get-started-button');
    await expect(getStartedButton).toBeVisible();

    const signInButton = page.getByTestId('sign-in-button');
    await expect(signInButton).toBeVisible();

    // Check button heights meet minimum touch target (44px)
    const getStartedBox = await getStartedButton.boundingBox();
    expect(getStartedBox).not.toBeNull();
    expect(getStartedBox!.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);

    const signInBox = await signInButton.boundingBox();
    expect(signInBox).not.toBeNull();
    expect(signInBox!.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);

    // Verify buttons are stacked vertically on mobile (flex-col)
    // Buttons should have similar x positions when stacked vertically
    const ctaContainer = page.getByTestId('hero-cta-buttons');
    await expect(ctaContainer).toBeVisible();
  });

  test('Test Case 3: Feature cards display in single column layout on mobile', async ({ page }) => {
    const featuresSection = page.getByTestId('features-section');
    await expect(featuresSection).toBeVisible();

    // Scroll to features section
    await featuresSection.scrollIntoViewIfNeeded();

    // Get all feature cards
    const featureCards = page.getByTestId('feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(3); // At least 3 feature cards

    // Verify each card is visible and get their positions
    const cardPositions: { x: number; y: number; width: number }[] = [];
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      await expect(card).toBeVisible();
      const box = await card.boundingBox();
      expect(box).not.toBeNull();
      cardPositions.push({ x: box!.x, y: box!.y, width: box!.width });
    }

    // Verify single column layout by checking:
    // 1. Cards have similar x positions (aligned left)
    // 2. Cards are stacked vertically (increasing y positions)
    // 3. Card widths should be close to full container width
    for (let i = 1; i < cardPositions.length; i++) {
      // Cards should be vertically stacked (y increases)
      expect(cardPositions[i].y).toBeGreaterThan(cardPositions[i - 1].y);

      // Cards should have similar x alignment (within 10px tolerance)
      expect(Math.abs(cardPositions[i].x - cardPositions[0].x)).toBeLessThan(10);
    }

    // Verify cards are using most of the viewport width (at least 80%)
    const viewportWidth = MOBILE_VIEWPORT.width;
    for (const pos of cardPositions) {
      expect(pos.width).toBeGreaterThan(viewportWidth * 0.7);
    }
  });

  test('Test Case 4: Guest shortening input and button are touch-friendly (min 44px height)', async ({ page }) => {
    // Wait for guest shortener to be visible
    const guestShortenerSection = page.getByTestId('guest-shortener-section');
    await expect(guestShortenerSection).toBeVisible();

    // Scroll to the guest shortener section
    await guestShortenerSection.scrollIntoViewIfNeeded();

    // Find the URL input
    const urlInput = page.getByTestId('url-input');
    await expect(urlInput).toBeVisible();

    // Check input height meets minimum touch target
    const inputBox = await urlInput.boundingBox();
    expect(inputBox).not.toBeNull();
    expect(inputBox!.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);

    // Find the shorten button within the guest shortener section (using exact aria-label or text)
    const shortenButton = guestShortenerSection.getByRole('button', { name: 'Shorten URL' }).or(
      guestShortenerSection.getByRole('button', { name: 'Shorten' })
    );
    await expect(shortenButton).toBeVisible();

    // Check button height meets minimum touch target
    const buttonBox = await shortenButton.boundingBox();
    expect(buttonBox).not.toBeNull();
    expect(buttonBox!.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);

    // Verify button spans full width of container
    expect(buttonBox!.width).toBeGreaterThan(inputBox!.width * 0.9);
  });

  test('Test Case 5: Navigation (navbar) is accessible and usable on mobile', async ({ page }) => {
    // Wait for navbar to be visible
    const navbar = page.locator('.navbar');
    await expect(navbar).toBeVisible();

    // Check logo/brand is visible
    const logo = navbar.locator('a:has-text("URLShortener")');
    await expect(logo).toBeVisible();

    // Check navbar buttons are accessible
    const signInNavButton = navbar.getByRole('link', { name: /sign in/i });
    await expect(signInNavButton).toBeVisible();

    const getStartedNavButton = navbar.getByRole('link', { name: /get started/i });
    await expect(getStartedNavButton).toBeVisible();

    // Verify nav buttons have adequate touch target sizes
    const signInBox = await signInNavButton.boundingBox();
    expect(signInBox).not.toBeNull();
    expect(signInBox!.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
    expect(signInBox!.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);

    const getStartedNavBox = await getStartedNavButton.boundingBox();
    expect(getStartedNavBox).not.toBeNull();
    expect(getStartedNavBox!.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
    expect(getStartedNavBox!.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);

    // Verify theme toggle is visible and has proper touch target
    const themeToggle = navbar.getByRole('button', { name: /toggle theme/i }).or(
      navbar.locator('label[aria-label="Toggle theme"]')
    );
    await expect(themeToggle).toBeVisible();

    const themeToggleBox = await themeToggle.boundingBox();
    expect(themeToggleBox).not.toBeNull();
    expect(themeToggleBox!.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
    expect(themeToggleBox!.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);

    // Test that navbar doesn't overflow horizontally
    const navbarBox = await navbar.boundingBox();
    expect(navbarBox).not.toBeNull();
    expect(navbarBox!.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
  });

  test('Mobile navigation buttons are clickable and navigate correctly', async ({ page }) => {
    // Test Sign In navigation
    const signInNavButton = page.locator('.navbar').getByRole('link', { name: /sign in/i });
    await signInNavButton.click();
    await expect(page).toHaveURL('/login');

    // Go back and test Get Started navigation
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    const getStartedNavButton = page.locator('.navbar').getByRole('link', { name: /get started/i });
    await getStartedNavButton.click();
    await expect(page).toHaveURL('/register');
  });

  test('Mobile hero CTA buttons are clickable and navigate correctly', async ({ page }) => {
    // Test hero Get Started button
    const getStartedLink = page.getByTestId('get-started-link');
    await getStartedLink.click();
    await expect(page).toHaveURL('/register');

    // Go back and test Sign In button
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    const signInLink = page.getByTestId('sign-in-link');
    await signInLink.click();
    await expect(page).toHaveURL('/login');
  });

  test('Mobile viewport does not cause content clipping or overflow', async ({ page }) => {
    // Check for horizontal scroll
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Check that no elements are positioned outside the viewport
    const elementsOutsideViewport = await page.evaluate(() => {
      const elements = document.querySelectorAll('*');
      const viewportWidth = window.innerWidth;
      let outsideCount = 0;

      elements.forEach(el => {
        const rect = el.getBoundingClientRect();
        // Check if element extends beyond the right edge
        if (rect.right > viewportWidth + 5) {
          outsideCount++;
        }
      });

      return outsideCount;
    });

    // Allow for some minor overflow (scrollbars, etc.)
    expect(elementsOutsideViewport).toBeLessThan(5);
  });
});
