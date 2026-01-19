// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Mobile Responsive Design - iPhone SE (375px)', () => {
  test.use({ viewport: { width: 375, height: 667 } });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Page layout adapts properly at 375px viewport, no horizontal scrolling required', async ({ page }) => {
    // Get the viewport width and document width
    const viewportWidth = 375;

    // Check that the document doesn't exceed viewport width (no horizontal scroll)
    const documentWidth = await page.evaluate(() => {
      return document.documentElement.scrollWidth;
    });

    // Document width should not significantly exceed viewport width
    // Allow small tolerance for potential subpixel rendering
    expect(documentWidth).toBeLessThanOrEqual(viewportWidth + 1);

    // Verify all major sections are visible
    const heroSection = page.locator('[data-testid="hero"]');
    const featuresSection = page.locator('[data-testid="features"]');
    const usageSection = page.locator('[data-testid="usage"]');
    const roadmapSection = page.locator('[data-testid="roadmap"]');
    const footerSection = page.locator('[data-testid="footer"]');

    await expect(heroSection).toBeVisible();
    await expect(featuresSection).toBeVisible();
    await expect(usageSection).toBeVisible();
    await expect(roadmapSection).toBeVisible();
    await expect(footerSection).toBeVisible();

    // Ensure no element overflows horizontally
    const hasOverflow = await page.evaluate(() => {
      const body = document.body;
      return body.scrollWidth > body.clientWidth;
    });
    expect(hasOverflow).toBe(false);
  });

  test('TC2: Hero section - text is readable, CTA buttons are touch-friendly (min 44px tap target)', async ({ page }) => {
    const heroSection = page.locator('[data-testid="hero"]');
    await expect(heroSection).toBeVisible();

    // Check h1 is visible and readable
    const h1 = heroSection.locator('h1');
    await expect(h1).toBeVisible();
    await expect(h1).toContainText('MirDB');

    // Check tagline is visible
    const tagline = heroSection.locator('.tagline');
    await expect(tagline).toBeVisible();

    // Check CTA buttons are visible
    const primaryCta = heroSection.locator('.btn-primary');
    const secondaryCta = heroSection.locator('.btn-secondary');

    await expect(primaryCta).toBeVisible();
    await expect(secondaryCta).toBeVisible();

    // Verify buttons have minimum touch-friendly size (44px height)
    const primaryBox = await primaryCta.boundingBox();
    const secondaryBox = await secondaryCta.boundingBox();

    expect(primaryBox).toBeTruthy();
    expect(secondaryBox).toBeTruthy();

    // Apple's Human Interface Guidelines recommend minimum 44px tap targets
    expect(primaryBox.height).toBeGreaterThanOrEqual(44);
    expect(secondaryBox.height).toBeGreaterThanOrEqual(44);

    // Verify buttons don't overflow the viewport
    expect(primaryBox.x).toBeGreaterThanOrEqual(0);
    expect(primaryBox.x + primaryBox.width).toBeLessThanOrEqual(375);
    expect(secondaryBox.x).toBeGreaterThanOrEqual(0);
    expect(secondaryBox.x + secondaryBox.width).toBeLessThanOrEqual(375);
  });

  test('TC3: Feature cards stack vertically or adapt to single-column layout on mobile', async ({ page }) => {
    const featuresSection = page.locator('[data-testid="features"]');
    await expect(featuresSection).toBeVisible();

    // Get all feature cards
    const featureCards = page.locator('[data-testid="feature-card"]');
    const cardCount = await featureCards.count();

    // Should have feature cards
    expect(cardCount).toBeGreaterThanOrEqual(1);

    // Get bounding boxes of first two cards to verify stacking
    if (cardCount >= 2) {
      const card1Box = await featureCards.nth(0).boundingBox();
      const card2Box = await featureCards.nth(1).boundingBox();

      expect(card1Box).toBeTruthy();
      expect(card2Box).toBeTruthy();

      // In single-column layout, card2 should be below card1 (higher Y value)
      // and they should have similar X positions (stacked vertically)
      expect(card2Box.y).toBeGreaterThan(card1Box.y);

      // Cards should not be side by side (their x positions should be similar)
      // Allow some tolerance for padding differences
      expect(Math.abs(card1Box.x - card2Box.x)).toBeLessThan(20);
    }

    // Verify cards don't overflow viewport width
    for (let i = 0; i < cardCount; i++) {
      const cardBox = await featureCards.nth(i).boundingBox();
      expect(cardBox).toBeTruthy();
      expect(cardBox.x).toBeGreaterThanOrEqual(0);
      expect(cardBox.x + cardBox.width).toBeLessThanOrEqual(375 + 5); // Small tolerance
    }
  });

  test('TC4: Navigation is accessible on mobile', async ({ page }) => {
    // The current design uses in-page navigation with anchor links
    // Check that navigation/CTA links are accessible and functional

    // Check hero CTA buttons are accessible
    const primaryCta = page.locator('.hero .btn-primary');
    const secondaryCta = page.locator('.hero .btn-secondary');

    await expect(primaryCta).toBeVisible();
    await expect(secondaryCta).toBeVisible();

    // Verify the "Learn More" link points to features section
    const learnMoreHref = await secondaryCta.getAttribute('href');
    expect(learnMoreHref).toBe('#features');

    // Verify footer navigation links are accessible
    const footerGithubLink = page.locator('footer .github-link');
    await expect(footerGithubLink).toBeVisible();

    // Verify the link is clickable and has proper size for touch
    const footerLinkBox = await footerGithubLink.boundingBox();
    expect(footerLinkBox).toBeTruthy();
    expect(footerLinkBox.height).toBeGreaterThanOrEqual(20);

    // Test that "Learn More" button scrolls to features section
    await secondaryCta.click();
    await page.waitForTimeout(500); // Wait for smooth scroll

    // Features section should now be in view
    const featuresSection = page.locator('[data-testid="features"]');
    await expect(featuresSection).toBeInViewport();
  });
});

test.describe('Mobile Responsive Design - iPhone 14 (390px)', () => {
  test.use({ viewport: { width: 390, height: 844 } });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC5: Page displays correctly without layout issues at 390px viewport', async ({ page }) => {
    const viewportWidth = 390;

    // Check that the document doesn't exceed viewport width
    const documentWidth = await page.evaluate(() => {
      return document.documentElement.scrollWidth;
    });

    expect(documentWidth).toBeLessThanOrEqual(viewportWidth + 1);

    // Verify all major sections are visible and properly rendered
    const heroSection = page.locator('[data-testid="hero"]');
    const featuresSection = page.locator('[data-testid="features"]');
    const usageSection = page.locator('[data-testid="usage"]');
    const roadmapSection = page.locator('[data-testid="roadmap"]');
    const footerSection = page.locator('[data-testid="footer"]');

    await expect(heroSection).toBeVisible();
    await expect(featuresSection).toBeVisible();
    await expect(usageSection).toBeVisible();
    await expect(roadmapSection).toBeVisible();
    await expect(footerSection).toBeVisible();

    // Verify hero section elements
    const h1 = heroSection.locator('h1');
    await expect(h1).toBeVisible();
    await expect(h1).toContainText('MirDB');

    const logo = heroSection.locator('img.logo');
    await expect(logo).toBeVisible();

    // Verify logo doesn't overflow
    const logoBox = await logo.boundingBox();
    expect(logoBox).toBeTruthy();
    expect(logoBox.x + logoBox.width).toBeLessThanOrEqual(viewportWidth);

    // Verify CTA buttons are properly sized
    const primaryCta = heroSection.locator('.btn-primary');
    const primaryBox = await primaryCta.boundingBox();
    expect(primaryBox).toBeTruthy();
    expect(primaryBox.height).toBeGreaterThanOrEqual(44);

    // Verify feature cards are stacked (single column)
    const featureCards = page.locator('[data-testid="feature-card"]');
    const cardCount = await featureCards.count();

    if (cardCount >= 2) {
      const card1Box = await featureCards.nth(0).boundingBox();
      const card2Box = await featureCards.nth(1).boundingBox();

      // Cards should be stacked vertically
      expect(card2Box.y).toBeGreaterThan(card1Box.y);
    }

    // Verify roadmap items are visible
    const roadmapItems = page.locator('[data-testid="roadmap-item"]');
    const itemCount = await roadmapItems.count();
    expect(itemCount).toBeGreaterThanOrEqual(1);

    // Verify footer content is properly laid out
    const footerBrand = footerSection.locator('.footer-brand');
    const footerLinks = footerSection.locator('.footer-links');

    await expect(footerBrand).toBeVisible();
    await expect(footerLinks).toBeVisible();

    // In mobile, footer should have centered/stacked layout
    // Check that footer content doesn't overflow
    const footerBox = await footerSection.boundingBox();
    expect(footerBox).toBeTruthy();
    expect(footerBox.width).toBeLessThanOrEqual(viewportWidth);

    // Ensure no horizontal scrolling
    const hasOverflow = await page.evaluate(() => {
      return document.body.scrollWidth > document.body.clientWidth;
    });
    expect(hasOverflow).toBe(false);
  });
});
