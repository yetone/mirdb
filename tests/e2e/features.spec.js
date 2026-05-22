/**
 * Features Section E2E Tests
 * Owner: Scenario 3 - Features Section
 *
 * Tests:
 * - Feature cards count and styling
 * - Feature card content (URL Shortening, Analytics, Dashboard)
 * - Accessibility (alt text, semantic markup)
 * - Responsive layout at 375px
 */

const { test, expect } = require('@playwright/test');

test.describe('Features Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for the features section to be visible
    await page.waitForSelector('#features', { state: 'visible' });
  });

  test('should render 4 feature cards with consistent styling', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const cards = featuresSection.locator('article.feature-card');
    await expect(cards).toHaveCount(4);

    // Verify each card has consistent styling
    for (let i = 0; i < 4; i++) {
      const card = cards.nth(i);
      await expect(card).toHaveClass(/feature-card/);
      await expect(card).toHaveClass(/card/);
      await expect(card).toHaveClass(/bg-base-200/);
      await expect(card).toHaveClass(/border/);
      await expect(card).toHaveClass(/p-6/);
    }
  });

  test('should include URL Shortening, Analytics, and Dashboard feature cards', async ({ page }) => {
    const cards = page.locator('#features article.feature-card');
    await expect(cards).toHaveCount(4);

    // Check for specific feature titles
    const titles = cards.locator('h3');
    const titleTexts = await titles.allTextContents();

    expect(titleTexts).toContain('URL Shortening');
    expect(titleTexts).toContain('Analytics');
    expect(titleTexts).toContain('Dashboard');
  });

  test('should have descriptive content for each feature', async ({ page }) => {
    const cards = page.locator('#features article.feature-card');
    const descriptions = cards.locator('p');

    const descTexts = await descriptions.allTextContents();

    // URL Shortening description
    const urlShorteningDesc = descTexts.find(d => d.includes('short links'));
    expect(urlShorteningDesc).toBeTruthy();

    // Analytics description
    const analyticsDesc = descTexts.find(d => d.includes('clicks') || d.includes('Track'));
    expect(analyticsDesc).toBeTruthy();

    // Dashboard description
    const dashboardDesc = descTexts.find(d => d.includes('dashboard'));
    expect(dashboardDesc).toBeTruthy();
  });

  test('should have feature icons with alt text', async ({ page }) => {
    const icons = page.locator('#features article.feature-card img');
    await expect(icons).toHaveCount(4);

    // Verify each icon has alt text
    for (let i = 0; i < 4; i++) {
      const icon = icons.nth(i);
      const altText = await icon.getAttribute('alt');
      expect(altText).toBeTruthy();
      expect(altText.length).toBeGreaterThan(0);
    }

    // Verify specific alt text content
    const allAltTexts = await icons.evaluateAll(imgs => imgs.map(img => img.alt));
    expect(allAltTexts.some(alt => alt.toLowerCase().includes('link'))).toBe(true);
    expect(allAltTexts.some(alt => alt.toLowerCase().includes('chart') || alt.toLowerCase().includes('analytics'))).toBe(true);
    expect(allAltTexts.some(alt => alt.toLowerCase().includes('dashboard'))).toBe(true);
  });

  test('should use semantic markup for accessibility', async ({ page }) => {
    const featuresSection = page.locator('#features');

    // Section has an aria-labelledby pointing to a heading
    const ariaLabelledBy = await featuresSection.getAttribute('aria-labelledby');
    expect(ariaLabelledBy).toBe('features-heading');

    // The heading exists and has the matching id
    const heading = page.locator('#features-heading');
    await expect(heading).toBeVisible();
    const headingId = await heading.getAttribute('id');
    expect(headingId).toBe('features-heading');

    // Feature cards use article elements (semantic)
    const articles = featuresSection.locator('article');
    await expect(articles).toHaveCount(4);

    // Each article has an h3 heading
    for (let i = 0; i < 4; i++) {
      const heading = articles.nth(i).locator('h3');
      await expect(heading).toBeVisible();
    }
  });

  test('should stack cards vertically on mobile (375px)', async ({ page }) => {
    // Set viewport to mobile size
    await page.setViewportSize({ width: 375, height: 812 });

    const cards = page.locator('#features article.feature-card');
    await expect(cards).toHaveCount(4);

    // On mobile, cards should be in a single column
    const firstCard = await cards.nth(0).boundingBox();
    const secondCard = await cards.nth(1).boundingBox();
    const thirdCard = await cards.nth(2).boundingBox();
    const fourthCard = await cards.nth(3).boundingBox();

    // Cards should be stacked vertically (each below the previous)
    expect(secondCard.y).toBeGreaterThan(firstCard.y);
    expect(thirdCard.y).toBeGreaterThan(secondCard.y);
    expect(fourthCard.y).toBeGreaterThan(thirdCard.y);

    // On mobile, each card should roughly span the full width
    const viewportWidth = 375;
    expect(firstCard.width).toBeGreaterThan(viewportWidth * 0.7);
  });

  test('should have proper spacing between cards on mobile', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 812 });

    const cards = page.locator('#features article.feature-card');
    const firstCard = await cards.nth(0).boundingBox();
    const secondCard = await cards.nth(1).boundingBox();

    // There should be gap between cards (gap-8 = 2rem = 32px)
    const gap = secondCard.y - (firstCard.y + firstCard.height);
    expect(gap).toBeGreaterThanOrEqual(20);
  });
});
