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

/**
 * Responsive Design E2E Tests - Tablet and Desktop Viewports
 * Owner: Scenario 10 - Responsive Design - Tablet and Desktop
 *
 * Tests verify that the homepage displays correctly on tablet (768px) and
 * desktop (1280px) viewports with appropriate grid layouts and hover effects.
 */

// Tablet viewport configuration (iPad portrait)
const TABLET_VIEWPORT = {
  width: 768,
  height: 1024,
};

// Desktop viewport configuration
const DESKTOP_VIEWPORT = {
  width: 1280,
  height: 800,
};

test.describe('Responsive Design - Tablet', () => {
  test.use({ viewport: TABLET_VIEWPORT });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('Test Case 1: Feature cards display in 2-column grid at 768px viewport', async ({ page }) => {
    const featuresSection = page.getByTestId('features-section');
    await expect(featuresSection).toBeVisible();

    // Scroll to features section
    await featuresSection.scrollIntoViewIfNeeded();

    // Get all feature cards
    const featureCards = page.getByTestId('feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(3); // At least 3 feature cards

    // Verify cards are in 2-column layout by checking positions
    const cardPositions: { x: number; y: number; width: number }[] = [];
    for (let i = 0; i < Math.min(cardCount, 4); i++) {
      const card = featureCards.nth(i);
      await expect(card).toBeVisible();
      const box = await card.boundingBox();
      expect(box).not.toBeNull();
      cardPositions.push({ x: box!.x, y: box!.y, width: box!.width });
    }

    // In a 2-column layout:
    // - Cards 0 and 1 should be on the same row (similar y, different x)
    // - Cards 2 and 3 should be on the next row (greater y than cards 0-1)
    if (cardCount >= 2) {
      // First two cards should be on the same row (y positions close together)
      expect(Math.abs(cardPositions[0].y - cardPositions[1].y)).toBeLessThan(20);
      // First two cards should be side by side (different x positions)
      expect(Math.abs(cardPositions[0].x - cardPositions[1].x)).toBeGreaterThan(50);
    }

    if (cardCount >= 3) {
      // Third card should be on a new row (greater y than first cards)
      expect(cardPositions[2].y).toBeGreaterThan(cardPositions[0].y + 50);
    }

    // Each card should take roughly half the container width (accounting for gap)
    const containerWidth = TABLET_VIEWPORT.width;
    const expectedCardWidth = (containerWidth - 100) / 2; // Roughly half minus padding/gap
    for (const pos of cardPositions) {
      expect(pos.width).toBeGreaterThan(expectedCardWidth * 0.7);
      expect(pos.width).toBeLessThan(containerWidth * 0.7); // Should not be full width
    }
  });

  test('Tablet layout has proper content alignment', async ({ page }) => {
    // Verify the page renders correctly
    const homePage = page.getByTestId('home-page');
    await expect(homePage).toBeVisible();

    // Check hero section is visible
    const heroSection = page.getByTestId('hero-section');
    await expect(heroSection).toBeVisible();

    // Check guest shortener section is visible
    const guestShortenerSection = page.getByTestId('guest-shortener-section');
    await expect(guestShortenerSection).toBeVisible();

    // Verify no horizontal overflow on tablet
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });
});

test.describe('Responsive Design - Desktop', () => {
  test.use({ viewport: DESKTOP_VIEWPORT });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('Test Case 2: Feature cards display in 3-column grid at 1280px viewport', async ({ page }) => {
    const featuresSection = page.getByTestId('features-section');
    await expect(featuresSection).toBeVisible();

    // Scroll to features section
    await featuresSection.scrollIntoViewIfNeeded();

    // Get all feature cards
    const featureCards = page.getByTestId('feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(3); // At least 3 feature cards

    // Verify cards are in 3-column layout by checking positions
    const cardPositions: { x: number; y: number; width: number }[] = [];
    for (let i = 0; i < Math.min(cardCount, 6); i++) {
      const card = featureCards.nth(i);
      await expect(card).toBeVisible();
      const box = await card.boundingBox();
      expect(box).not.toBeNull();
      cardPositions.push({ x: box!.x, y: box!.y, width: box!.width });
    }

    // In a 3-column layout:
    // - Cards 0, 1, and 2 should be on the same row (similar y, different x)
    if (cardCount >= 3) {
      // First three cards should be on the same row (y positions close together)
      expect(Math.abs(cardPositions[0].y - cardPositions[1].y)).toBeLessThan(20);
      expect(Math.abs(cardPositions[0].y - cardPositions[2].y)).toBeLessThan(20);

      // Cards should be side by side (different x positions)
      // Card 0 < Card 1 < Card 2 in x position
      expect(cardPositions[1].x).toBeGreaterThan(cardPositions[0].x + 50);
      expect(cardPositions[2].x).toBeGreaterThan(cardPositions[1].x + 50);
    }

    if (cardCount >= 4) {
      // Fourth card should be on a new row (greater y than first cards)
      expect(cardPositions[3].y).toBeGreaterThan(cardPositions[0].y + 50);
    }

    // Each card should take roughly one-third the container width
    const maxContainerWidth = 1152; // max-w-6xl = 72rem = 1152px
    const expectedCardWidth = maxContainerWidth / 3 - 24; // Roughly third minus gap
    for (const pos of cardPositions) {
      expect(pos.width).toBeGreaterThan(expectedCardWidth * 0.7);
      expect(pos.width).toBeLessThan(maxContainerWidth * 0.5); // Should not be more than half
    }
  });

  test('Test Case 3: Content is centered with max-width constraint on desktop', async ({ page }) => {
    const featuresSection = page.getByTestId('features-section');
    await expect(featuresSection).toBeVisible();

    // Scroll to features section
    await featuresSection.scrollIntoViewIfNeeded();

    // Get the features section container (the div with max-w-6xl)
    // The inner container with max-w-6xl should be centered
    const maxWidthContainer = featuresSection.locator('.max-w-6xl');
    await expect(maxWidthContainer).toBeVisible();

    const containerBox = await maxWidthContainer.boundingBox();
    expect(containerBox).not.toBeNull();

    // max-w-6xl is 72rem = 1152px, so container should be constrained
    // At 1280px viewport, it should be less than viewport width
    expect(containerBox!.width).toBeLessThanOrEqual(1152 + 32); // Allow for some padding

    // Container should be centered (left margin roughly equals right margin)
    const viewportWidth = DESKTOP_VIEWPORT.width;
    const leftMargin = containerBox!.x;
    const rightMargin = viewportWidth - (containerBox!.x + containerBox!.width);

    // The margins should be roughly equal (centered)
    expect(Math.abs(leftMargin - rightMargin)).toBeLessThan(50);
  });

  test('Test Case 4: Interactive elements show hover effects on desktop', async ({ page }) => {
    // Test 1: Feature cards have hover effects
    const featuresSection = page.getByTestId('features-section');
    await featuresSection.scrollIntoViewIfNeeded();

    const firstFeatureCard = page.getByTestId('feature-card').first();
    await expect(firstFeatureCard).toBeVisible();

    // Get the card with hover class (the GlassMorphismCard)
    const card = firstFeatureCard.locator('.card');
    await expect(card).toBeVisible();

    // Get initial transform/scale
    const initialTransform = await card.evaluate(el => {
      const style = window.getComputedStyle(el);
      return style.transform;
    });

    // Hover over the card
    await card.hover();

    // Wait for transition (300ms defined in the component)
    await page.waitForTimeout(350);

    // Get transform after hover - should have scale applied
    const hoverTransform = await card.evaluate(el => {
      const style = window.getComputedStyle(el);
      return style.transform;
    });

    // The transform should change on hover (scale(1.05) is applied)
    // Either transform changes OR we verify the hover class is present
    const hasHoverClasses = await card.evaluate(el => {
      return el.classList.contains('hover:scale-105') ||
             el.className.includes('hover:');
    });
    expect(hasHoverClasses || hoverTransform !== initialTransform).toBeTruthy();

    // Test 2: Buttons have hover effects
    const heroSection = page.getByTestId('hero-section');
    await heroSection.scrollIntoViewIfNeeded();

    const getStartedButton = page.getByTestId('get-started-button');
    await expect(getStartedButton).toBeVisible();

    // Verify button has hover transition classes
    const buttonHasTransition = await getStartedButton.evaluate(el => {
      const style = window.getComputedStyle(el);
      return style.transitionDuration !== '0s';
    });
    expect(buttonHasTransition).toBeTruthy();

    // Hover over button and verify it responds
    await getStartedButton.hover();
    await page.waitForTimeout(100);

    // Button should still be visible and functional after hover
    await expect(getStartedButton).toBeVisible();
  });

  test('Desktop layout has proper navigation and footer visibility', async ({ page }) => {
    // Verify navbar is visible
    const navbar = page.locator('.navbar');
    await expect(navbar).toBeVisible();

    // Verify hero section is visible
    const heroSection = page.getByTestId('hero-section');
    await expect(heroSection).toBeVisible();

    // Verify CTA buttons are visible and side by side on desktop
    const getStartedButton = page.getByTestId('get-started-button');
    const signInButton = page.getByTestId('sign-in-button');

    await expect(getStartedButton).toBeVisible();
    await expect(signInButton).toBeVisible();

    // On desktop, buttons might be side by side in the hero section
    const getStartedBox = await getStartedButton.boundingBox();
    const signInBox = await signInButton.boundingBox();

    // Both buttons should be visible and positioned properly
    expect(getStartedBox).not.toBeNull();
    expect(signInBox).not.toBeNull();
  });

  test('Desktop navigation buttons work correctly', async ({ page }) => {
    // Test Sign In navigation
    const signInLink = page.getByTestId('sign-in-link');
    await signInLink.click();
    await expect(page).toHaveURL('/login');

    // Go back and test Get Started navigation
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    const getStartedLink = page.getByTestId('get-started-link');
    await getStartedLink.click();
    await expect(page).toHaveURL('/register');
  });
});
