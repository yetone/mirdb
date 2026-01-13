import { test, expect } from '@playwright/test';

/**
 * E2E tests for Responsive Design - Tablet Viewport
 * Tests REQ-6 requirements for tablet responsiveness (768px width)
 */

test.describe('Tablet Viewport Responsive Design', () => {
  test.beforeEach(async ({ page }) => {
    // Set viewport to tablet width (768px) as per test case requirements
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/');
    // Wait for page to fully load
    await page.waitForLoadState('networkidle');
  });

  test('TC1: Homepage renders with tablet-optimized layout at 768px viewport', async ({ page }) => {
    // Check that the body doesn't have horizontal scrollbar
    const documentScrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    const documentClientWidth = await page.evaluate(() => document.documentElement.clientWidth);

    // The scroll width should not exceed client width (no horizontal scrolling needed)
    expect(documentScrollWidth).toBeLessThanOrEqual(documentClientWidth);

    // Verify the page renders correctly
    const heroSection = page.locator('section.hero');
    await expect(heroSection).toBeVisible();

    // Verify hero section fits within tablet viewport
    const heroBox = await heroSection.boundingBox();
    expect(heroBox).not.toBeNull();
    expect(heroBox!.width).toBeLessThanOrEqual(768);

    // Verify hero content is properly sized for tablet
    const heroContent = page.locator('.hero-content');
    await expect(heroContent).toBeVisible();

    const contentBox = await heroContent.boundingBox();
    expect(contentBox).not.toBeNull();
    expect(contentBox!.width).toBeLessThanOrEqual(768);

    // Verify all main sections are visible
    const featuresSection = page.getByTestId('features-section');
    await expect(featuresSection).toBeVisible();

    const demoSection = page.getByTestId('demo-section');
    await expect(demoSection).toBeVisible();

    // Verify footer is visible
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();
  });

  test('TC2: Feature cards display in 2-column grid on tablet', async ({ page }) => {
    // Check that features section is visible
    const featuresSection = page.getByTestId('features-section');
    await expect(featuresSection).toBeVisible();

    // Get the feature cards container
    const cardsContainer = page.getByTestId('feature-cards-container');
    await expect(cardsContainer).toBeVisible();

    // Verify grid has 2 columns on tablet (md:grid-cols-2)
    const gridStyle = await cardsContainer.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });

    // 2-column grid should have exactly 2 column values
    const columnCount = gridStyle.split(' ').filter(col => col.trim() !== '').length;
    expect(columnCount).toBe(2);

    // Verify all 4 feature cards are present
    const featureCards = page.locator('[data-testid^="feature-card-"]');
    const cardCount = await featureCards.count();
    expect(cardCount).toBe(4);

    // Verify each feature card fits within its column
    // On tablet (768px), each column should be roughly half the container width
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      await expect(card).toBeVisible();

      const cardBox = await card.boundingBox();
      expect(cardBox).not.toBeNull();
      // Each card should be less than half of viewport width + gap allowance
      expect(cardBox!.width).toBeLessThanOrEqual(400);
      expect(cardBox!.width).toBeGreaterThan(200); // Not too narrow
    }

    // Verify cards are arranged in 2 columns by checking vertical positions
    const card1Box = await featureCards.nth(0).boundingBox();
    const card2Box = await featureCards.nth(1).boundingBox();
    const card3Box = await featureCards.nth(2).boundingBox();

    // Cards 0 and 1 should be on the same row (similar Y position)
    expect(Math.abs(card1Box!.y - card2Box!.y)).toBeLessThan(10);

    // Cards 0 and 2 should be in different rows (different Y positions)
    expect(card3Box!.y).toBeGreaterThan(card1Box!.y + card1Box!.height / 2);
  });

  test('TC3: Navigation elements are accessible and appropriately sized on tablet', async ({ page }) => {
    // Check navigation bar is visible
    const navbar = page.locator('nav.navbar');
    await expect(navbar).toBeVisible();

    // Verify navbar spans full width appropriately
    const navbarBox = await navbar.boundingBox();
    expect(navbarBox).not.toBeNull();
    expect(navbarBox!.width).toBeLessThanOrEqual(768);

    // Check the logo/brand link is visible
    const brandLink = page.locator('nav .btn-ghost').first();
    await expect(brandLink).toBeVisible();

    // Check login link has appropriate touch target size (min 44px for tablets)
    const loginLink = page.getByTestId('login-link');
    await expect(loginLink).toBeVisible();

    const loginLinkBox = await loginLink.boundingBox();
    expect(loginLinkBox).not.toBeNull();
    expect(loginLinkBox!.height).toBeGreaterThanOrEqual(40);
    expect(loginLinkBox!.width).toBeGreaterThanOrEqual(44);

    // Check register/sign up link has appropriate size
    const registerLink = page.getByTestId('register-link');
    await expect(registerLink).toBeVisible();

    const registerLinkBox = await registerLink.boundingBox();
    expect(registerLinkBox).not.toBeNull();
    expect(registerLinkBox!.height).toBeGreaterThanOrEqual(40);
    expect(registerLinkBox!.width).toBeGreaterThanOrEqual(44);

    // Verify navigation links are horizontally aligned (not stacked)
    // Both login and register should be in the same row
    expect(Math.abs(loginLinkBox!.y - registerLinkBox!.y)).toBeLessThan(10);

    // Verify hero CTA buttons are accessible
    const getStartedBtn = page.getByTestId('get-started-btn');
    await expect(getStartedBtn).toBeVisible();

    const getStartedBox = await getStartedBtn.boundingBox();
    expect(getStartedBox).not.toBeNull();
    expect(getStartedBox!.height).toBeGreaterThanOrEqual(40);

    const loginBtn = page.getByTestId('login-btn');
    await expect(loginBtn).toBeVisible();

    const loginBtnBox = await loginBtn.boundingBox();
    expect(loginBtnBox).not.toBeNull();
    expect(loginBtnBox!.height).toBeGreaterThanOrEqual(40);

    // Verify CTA buttons are side by side on tablet
    expect(Math.abs(getStartedBox!.y - loginBtnBox!.y)).toBeLessThan(10);
  });

  test('Content is properly spaced and readable on tablet', async ({ page }) => {
    // Verify padding/margins are appropriate - content should not touch edges
    const heroContent = page.locator('.hero-content');
    const heroBox = await heroContent.boundingBox();
    expect(heroBox).not.toBeNull();

    // Hero content should have some margin from edges
    expect(heroBox!.x).toBeGreaterThan(10);

    // Verify text is readable - check headline font size
    const headline = page.locator('.hero-content h1');
    await expect(headline).toBeVisible();

    const headlineFontSize = await headline.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });

    // Headline should be appropriately sized for tablet (not too small)
    expect(headlineFontSize).toBeGreaterThanOrEqual(36);

    // Verify feature descriptions are readable
    const featureDescription = page.getByTestId('feature-description-url-shortening');
    await expect(featureDescription).toBeVisible();

    const descFontSize = await featureDescription.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });

    expect(descFontSize).toBeGreaterThanOrEqual(14);
  });

  test('Demo section layout adapts appropriately for tablet', async ({ page }) => {
    // Check demo section is visible
    const demoSection = page.getByTestId('demo-section');
    await expect(demoSection).toBeVisible();

    // Verify demo section fits within viewport
    const demoBox = await demoSection.boundingBox();
    expect(demoBox).not.toBeNull();
    expect(demoBox!.width).toBeLessThanOrEqual(768);

    // Verify form input and button are appropriately laid out
    const demoInput = page.getByTestId('demo-url-input');
    await expect(demoInput).toBeVisible();

    const inputBox = await demoInput.boundingBox();
    expect(inputBox).not.toBeNull();
    // Input should be reasonably wide on tablet
    expect(inputBox!.width).toBeGreaterThan(200);

    const shortenButton = page.getByTestId('demo-shorten-button');
    await expect(shortenButton).toBeVisible();

    const buttonBox = await shortenButton.boundingBox();
    expect(buttonBox).not.toBeNull();
    expect(buttonBox!.height).toBeGreaterThanOrEqual(40);
  });
});
