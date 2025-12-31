import { test, expect } from '@playwright/test';

test.describe('Responsive Design - Desktop', () => {
  test('TC1: Page renders without horizontal scroll at 1920x1080', async ({ page }) => {
    // Set viewport to full HD resolution
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.goto('/');

    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');

    // Verify no horizontal scroll by checking document width equals viewport width
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Verify all major sections are visible
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeVisible();

    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    const projectStatusSection = page.locator('[data-testid="project-status-section"]');
    await expect(projectStatusSection).toBeVisible();

    const footerSection = page.locator('[data-testid="footer-section"]');
    await expect(footerSection).toBeVisible();
  });

  test('TC2: Page renders correctly at 1280x720 laptop resolution', async ({ page }) => {
    // Set viewport to standard laptop resolution
    await page.setViewportSize({ width: 1280, height: 720 });
    await page.goto('/');

    await page.waitForLoadState('domcontentloaded');

    // Verify no horizontal scroll
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Verify hero section is properly contained within viewport
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    const heroBox = await heroSection.boundingBox();
    expect(heroBox).not.toBeNull();
    if (heroBox) {
      // Hero should fit within the viewport width
      expect(heroBox.width).toBeLessThanOrEqual(1280);
    }

    // Verify features section is visible and properly laid out
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Feature cards should be in a grid layout
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(5);

    // Verify all sections are accessible by scrolling
    const sections = ['#features', '#architecture', '#quick-start', '#configuration', '#project-status'];
    for (const section of sections) {
      const sectionElement = page.locator(section);
      await sectionElement.scrollIntoViewIfNeeded();
      await expect(sectionElement).toBeVisible();
    }
  });

  test('TC3: Hero section is centered and properly spaced on desktop', async ({ page }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.goto('/');

    await page.waitForLoadState('domcontentloaded');

    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify hero content is centered
    const heroContent = page.locator('.hero-content');
    await expect(heroContent).toBeVisible();

    // Check that hero content is horizontally centered
    const heroContentBox = await heroContent.boundingBox();
    const heroSectionBox = await heroSection.boundingBox();

    expect(heroContentBox).not.toBeNull();
    expect(heroSectionBox).not.toBeNull();

    if (heroContentBox && heroSectionBox) {
      // Calculate center offset - content should be centered within the hero section
      const contentCenterX = heroContentBox.x + heroContentBox.width / 2;
      const sectionCenterX = heroSectionBox.x + heroSectionBox.width / 2;

      // Allow for a small margin of error (10px)
      expect(Math.abs(contentCenterX - sectionCenterX)).toBeLessThan(10);
    }

    // Verify text is centered (text-align: center in CSS)
    const textAlign = await heroSection.evaluate((el) => {
      return window.getComputedStyle(el).textAlign;
    });
    expect(textAlign).toBe('center');

    // Verify proper spacing - hero should have padding
    const heroPadding = await heroSection.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return parseFloat(style.padding) || parseFloat(style.paddingTop);
    });
    expect(heroPadding).toBeGreaterThan(0);

    // Verify all hero elements are visible and properly spaced
    const productName = heroSection.locator('h1');
    await expect(productName).toBeVisible();
    await expect(productName).toContainText('MirDB');

    const tagline = heroSection.locator('[data-testid="tagline"]');
    await expect(tagline).toBeVisible();

    const ctaButtons = heroSection.locator('.cta-buttons');
    await expect(ctaButtons).toBeVisible();

    // Verify CTA buttons are centered
    const ctaJustifyContent = await ctaButtons.evaluate((el) => {
      return window.getComputedStyle(el).justifyContent;
    });
    expect(ctaJustifyContent).toBe('center');
  });

  test('TC4: Features section displays in grid layout on desktop', async ({ page }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.goto('/');

    await page.waitForLoadState('domcontentloaded');

    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Verify feature grid exists
    const featureGrid = page.locator('.feature-grid');
    await expect(featureGrid).toBeVisible();

    // Verify grid display
    const gridDisplay = await featureGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(gridDisplay).toBe('grid');

    // Verify there are 5 feature cards
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(5);

    // Get all feature card positions to verify multi-column layout
    const cardPositions = await featureCards.evaluateAll((cards) => {
      return cards.map((card) => {
        const rect = card.getBoundingClientRect();
        return { x: rect.x, y: rect.y, width: rect.width, height: rect.height };
      });
    });

    // Verify cards are arranged in multiple columns (not all in same X position)
    const uniqueXPositions = new Set(cardPositions.map((pos) => Math.round(pos.x)));
    expect(uniqueXPositions.size).toBeGreaterThan(1);

    // Verify cards have reasonable width on desktop (not stretching full width)
    const maxCardWidth = Math.max(...cardPositions.map((pos) => pos.width));
    expect(maxCardWidth).toBeLessThan(600); // Cards should not be too wide

    // Verify first row has multiple cards (checking cards with same Y position)
    const firstRowY = cardPositions[0].y;
    const cardsInFirstRow = cardPositions.filter(
      (pos) => Math.abs(pos.y - firstRowY) < 10
    ).length;
    expect(cardsInFirstRow).toBeGreaterThanOrEqual(2); // At least 2 cards in first row for desktop

    // Verify max-width constraint on feature grid
    const featureGridBox = await featureGrid.boundingBox();
    expect(featureGridBox).not.toBeNull();
    if (featureGridBox) {
      expect(featureGridBox.width).toBeLessThanOrEqual(1200); // max-width: 1200px
    }
  });
});
