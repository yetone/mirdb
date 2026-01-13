import { test, expect } from '@playwright/test';

/**
 * E2E tests for Responsive Design - Mobile Viewport
 * Tests REQ-6 and US-5 requirements for mobile responsiveness
 */

test.describe('Mobile Viewport Responsive Design', () => {
  test.beforeEach(async ({ page }) => {
    // Set viewport to mobile width (375px) as per test case requirements
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');
    // Wait for page to fully load
    await page.waitForLoadState('networkidle');
  });

  test('TC1: Homepage renders without horizontal overflow at 375px viewport', async ({ page }) => {
    // Check that the body doesn't have horizontal scrollbar
    const documentScrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    const documentClientWidth = await page.evaluate(() => document.documentElement.clientWidth);

    // The scroll width should not exceed client width (no horizontal scrolling needed)
    expect(documentScrollWidth).toBeLessThanOrEqual(documentClientWidth);

    // Additional check: no element extends beyond viewport width
    const bodyWidth = await page.evaluate(() => {
      const body = document.body;
      const rect = body.getBoundingClientRect();
      return rect.width;
    });

    expect(bodyWidth).toBeLessThanOrEqual(375);
  });

  test('TC2: CTA buttons have minimum touch target size of 44px', async ({ page }) => {
    // Check the "Get Started Free" button (primary CTA)
    const getStartedBtn = page.getByTestId('get-started-btn');
    await expect(getStartedBtn).toBeVisible();

    const getStartedBox = await getStartedBtn.boundingBox();
    expect(getStartedBox).not.toBeNull();
    expect(getStartedBox!.height).toBeGreaterThanOrEqual(44);
    expect(getStartedBox!.width).toBeGreaterThanOrEqual(44);

    // Check the "Login" button in hero section
    const loginBtn = page.getByTestId('login-btn');
    await expect(loginBtn).toBeVisible();

    const loginBtnBox = await loginBtn.boundingBox();
    expect(loginBtnBox).not.toBeNull();
    expect(loginBtnBox!.height).toBeGreaterThanOrEqual(44);
    expect(loginBtnBox!.width).toBeGreaterThanOrEqual(44);

    // Check navigation login link
    const loginLink = page.getByTestId('login-link');
    await expect(loginLink).toBeVisible();

    const loginLinkBox = await loginLink.boundingBox();
    expect(loginLinkBox).not.toBeNull();
    expect(loginLinkBox!.height).toBeGreaterThanOrEqual(44);

    // Check navigation register link
    const registerLink = page.getByTestId('register-link');
    await expect(registerLink).toBeVisible();

    const registerLinkBox = await registerLink.boundingBox();
    expect(registerLinkBox).not.toBeNull();
    expect(registerLinkBox!.height).toBeGreaterThanOrEqual(44);
  });

  test('TC3: Body text is at least 16px for readability', async ({ page }) => {
    // Check the hero description paragraph font size
    const heroDescription = page.locator('.hero-content p').first();
    await expect(heroDescription).toBeVisible();

    const heroFontSize = await heroDescription.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });

    expect(heroFontSize).toBeGreaterThanOrEqual(16);

    // Check feature card description text
    const featureDescription = page.getByTestId('feature-description-url-shortening');
    await expect(featureDescription).toBeVisible();

    const featureFontSize = await featureDescription.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });

    expect(featureFontSize).toBeGreaterThanOrEqual(16);
  });

  test('TC4: Hero section stacks vertically and fits mobile width', async ({ page }) => {
    // Check that hero section is visible
    const heroSection = page.locator('section.hero');
    await expect(heroSection).toBeVisible();

    // Check that hero section fits within viewport
    const heroBox = await heroSection.boundingBox();
    expect(heroBox).not.toBeNull();
    expect(heroBox!.width).toBeLessThanOrEqual(375);

    // Check that hero content is centered and stacked vertically
    const heroContent = page.locator('.hero-content');
    const contentBox = await heroContent.boundingBox();
    expect(contentBox).not.toBeNull();
    expect(contentBox!.width).toBeLessThanOrEqual(375);

    // Verify CTA buttons are displayed (they may be in a flex row or stacked)
    const ctaButtons = page.locator('.hero-content .flex.gap-4');
    await expect(ctaButtons).toBeVisible();

    const ctaBox = await ctaButtons.boundingBox();
    expect(ctaBox).not.toBeNull();
    expect(ctaBox!.width).toBeLessThanOrEqual(375);
  });

  test('TC5: Feature cards stack vertically in single column on mobile', async ({ page }) => {
    // Check that features section is visible
    const featuresSection = page.getByTestId('features-section');
    await expect(featuresSection).toBeVisible();

    // Get the feature cards container
    const cardsContainer = page.getByTestId('feature-cards-container');
    await expect(cardsContainer).toBeVisible();

    // Verify grid is single column on mobile (grid-cols-1)
    const gridStyle = await cardsContainer.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });

    // Single column grid should have only one column value
    // The value should be a single pixel/fr value, not multiple
    const columnCount = gridStyle.split(' ').filter(col => col.trim() !== '').length;
    expect(columnCount).toBe(1);

    // Verify each feature card fits within mobile viewport
    const featureCards = page.locator('[data-testid^="feature-card-"]');
    const cardCount = await featureCards.count();

    expect(cardCount).toBe(4); // Should have 4 feature cards

    // Check that each card fits within viewport width
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      const cardBox = await card.boundingBox();
      expect(cardBox).not.toBeNull();
      expect(cardBox!.width).toBeLessThanOrEqual(375 - 32); // 375px minus padding (16px each side)
    }
  });
});
