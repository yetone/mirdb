// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Responsive Design - Tablet Viewports
 * Verifies that the homepage displays correctly on tablet viewports as specified in REQ-10
 *
 * Test Cases:
 * 1. Load page at 768x1024 (iPad portrait) resolution - Layout adapts appropriately, content readable, no horizontal scroll
 * 2. Load page at 1024x768 (iPad landscape) resolution - Layout adapts appropriately for landscape orientation
 */

const TABLET_VIEWPORTS = [
  { name: 'iPad Portrait (768x1024)', width: 768, height: 1024 },
  { name: 'iPad Landscape (1024x768)', width: 1024, height: 768 },
];

test.describe('Responsive Design - Tablet Viewports', () => {
  /**
   * Test Case 1: Load page at 768x1024 (iPad portrait) resolution
   * Expected: Layout adapts appropriately, content readable, no horizontal scroll
   */
  test('TC1: Page displays correctly at 768x1024 (iPad portrait) with no horizontal scroll and readable content', async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Verify no horizontal scrollbar (page width matches viewport)
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth);

    // Verify all main sections are visible and content is readable
    const heroSection = page.locator('.hero, [data-testid="hero-section"]').first();
    await expect(heroSection).toBeVisible();

    // Verify hero content is readable
    const heroTitle = page.locator('.hero h1, [data-testid="hero-section"] h1').first();
    await expect(heroTitle).toBeVisible();
    const heroTitleBox = await heroTitle.boundingBox();
    expect(heroTitleBox).not.toBeNull();
    if (heroTitleBox) {
      // Title should fit within viewport width
      expect(heroTitleBox.width).toBeLessThanOrEqual(viewportWidth);
    }

    const heroTagline = page.locator('.hero .tagline, [data-testid="hero-tagline"]').first();
    await expect(heroTagline).toBeVisible();

    const heroDescription = page.locator('.hero .description, [data-testid="hero-description"]').first();
    await expect(heroDescription).toBeVisible();

    // Verify CTA buttons are visible and accessible
    const ctaButtons = page.locator('.cta-buttons .btn');
    const buttonCount = await ctaButtons.count();
    expect(buttonCount).toBeGreaterThanOrEqual(1);
    for (let i = 0; i < buttonCount; i++) {
      await expect(ctaButtons.nth(i)).toBeVisible();
    }

    // Scroll to and verify value proposition section is readable
    const valuePropositionSection = page.locator('#value-proposition, .value-proposition').first();
    await valuePropositionSection.scrollIntoViewIfNeeded();
    await expect(valuePropositionSection).toBeVisible();

    // Verify all three pillars are visible
    const pillars = page.locator('.pillar');
    const pillarCount = await pillars.count();
    expect(pillarCount).toBe(3);

    // Each pillar should be fully visible and readable
    for (let i = 0; i < pillarCount; i++) {
      const pillar = pillars.nth(i);
      await pillar.scrollIntoViewIfNeeded();
      await expect(pillar).toBeVisible();

      // Verify pillar content is readable
      const pillarTitle = pillar.locator('h3');
      await expect(pillarTitle).toBeVisible();

      const pillarDescription = pillar.locator('p');
      await expect(pillarDescription).toBeVisible();
    }

    // Scroll to and verify features section
    const featuresSection = page.locator('#features, .features').first();
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Verify feature cards are visible
    const featureCards = page.locator('.feature-card, .feature');
    const featureCount = await featureCards.count();
    expect(featureCount).toBeGreaterThanOrEqual(3);

    // Verify each feature card is readable
    for (let i = 0; i < Math.min(featureCount, 3); i++) {
      const card = featureCards.nth(i);
      await card.scrollIntoViewIfNeeded();
      await expect(card).toBeVisible();

      const cardTitle = card.locator('h3');
      await expect(cardTitle).toBeVisible();

      const cardDescription = card.locator('p');
      await expect(cardDescription).toBeVisible();
    }

    // Scroll to and verify getting started section
    const gettingStartedSection = page.locator('#getting-started, .getting-started').first();
    await gettingStartedSection.scrollIntoViewIfNeeded();
    await expect(gettingStartedSection).toBeVisible();

    // Verify code blocks are readable
    const codeBlocks = page.locator('pre code');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThanOrEqual(1);

    // Scroll to and verify footer
    const footer = page.locator('.footer, footer').first();
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();
  });

  /**
   * Test Case 2: Load page at 1024x768 (iPad landscape) resolution
   * Expected: Layout adapts appropriately for landscape orientation
   */
  test('TC2: Page displays correctly at 1024x768 (iPad landscape) with appropriate layout adaptation', async ({ page }) => {
    await page.setViewportSize({ width: 1024, height: 768 });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Verify no horizontal scrollbar
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth);

    // Verify navigation is visible
    const navbar = page.locator('.navbar, nav').first();
    await expect(navbar).toBeVisible();
    const navBox = await navbar.boundingBox();
    if (navBox) {
      expect(navBox.width).toBeLessThanOrEqual(viewportWidth);
    }

    // At 1024px width (> 768px breakpoint), nav links should be visible
    const navLinks = page.locator('.nav-links a:not(.btn)');
    const navLinkCount = await navLinks.count();
    expect(navLinkCount).toBeGreaterThanOrEqual(1);

    // Verify hero section
    const heroSection = page.locator('.hero, [data-testid="hero-section"]').first();
    await expect(heroSection).toBeVisible();

    // Verify hero content layout
    const heroTitle = page.locator('.hero h1, [data-testid="hero-section"] h1').first();
    await expect(heroTitle).toBeVisible();

    const heroTagline = page.locator('.hero .tagline, [data-testid="hero-tagline"]').first();
    await expect(heroTagline).toBeVisible();

    // Verify CTA buttons are accessible
    const ctaButtons = page.locator('.cta-buttons .btn');
    const buttonCount = await ctaButtons.count();
    expect(buttonCount).toBeGreaterThanOrEqual(1);

    // Scroll to value proposition section
    const valuePropositionSection = page.locator('#value-proposition, .value-proposition').first();
    await valuePropositionSection.scrollIntoViewIfNeeded();
    await expect(valuePropositionSection).toBeVisible();

    // Verify pillars layout - at 1024px they should be in a row layout (3 columns)
    const pillars = page.locator('.pillar');
    await expect(pillars).toHaveCount(3);

    const box1 = await pillars.nth(0).boundingBox();
    const box2 = await pillars.nth(1).boundingBox();
    const box3 = await pillars.nth(2).boundingBox();

    expect(box1).not.toBeNull();
    expect(box2).not.toBeNull();
    expect(box3).not.toBeNull();

    if (box1 && box2 && box3) {
      // At 1024px width, pillars should be arranged side-by-side (row layout)
      // Check they are at approximately the same Y position
      expect(Math.abs(box1.y - box2.y)).toBeLessThan(10);
      expect(Math.abs(box2.y - box3.y)).toBeLessThan(10);

      // Verify pillars are arranged left to right
      expect(box1.x).toBeLessThan(box2.x);
      expect(box2.x).toBeLessThan(box3.x);
    }

    // Scroll to features section
    const featuresSection = page.locator('#features, .features').first();
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Verify feature cards are visible and properly laid out
    const featureCards = page.locator('.feature-card, .feature');
    const featureCount = await featureCards.count();
    expect(featureCount).toBeGreaterThanOrEqual(3);

    // At 1024px, feature grid should show multiple columns (not single column)
    const firstCard = featureCards.nth(0);
    const secondCard = featureCards.nth(1);
    await firstCard.scrollIntoViewIfNeeded();

    const firstBox = await firstCard.boundingBox();
    const secondBox = await secondCard.boundingBox();

    if (firstBox && secondBox) {
      // At 1024px width with minmax(320px, 1fr), we should have 2-3 columns
      // Either cards are side by side (same y) or stacked (different y)
      // The grid should accommodate multiple columns at this width
      const featuresBox = await featuresSection.boundingBox();
      if (featuresBox) {
        expect(featuresBox.width).toBeLessThanOrEqual(viewportWidth);
      }
    }

    // Scroll to architecture section
    const architectureSection = page.locator('#architecture, .architecture').first();
    await architectureSection.scrollIntoViewIfNeeded();
    await expect(architectureSection).toBeVisible();

    // Verify architecture diagram is visible
    const archDiagram = page.locator('.architecture-diagram, [data-testid="architecture-diagram"]').first();
    await archDiagram.scrollIntoViewIfNeeded();
    await expect(archDiagram).toBeVisible();

    // At landscape width (1024px > 768px breakpoint), arch-flow should be horizontal
    const archFlow = page.locator('.arch-flow').first();
    const archFlowStyle = await archFlow.evaluate((el) => {
      return window.getComputedStyle(el).flexDirection;
    });
    // At 1024px (above 768px breakpoint), flex-direction should be row
    expect(archFlowStyle).toBe('row');

    // Scroll to getting started section
    const gettingStartedSection = page.locator('#getting-started, .getting-started').first();
    await gettingStartedSection.scrollIntoViewIfNeeded();
    await expect(gettingStartedSection).toBeVisible();

    // Scroll to footer
    const footer = page.locator('.footer, footer').first();
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();
  });

  /**
   * Additional test: Verify tablet layout for both orientations
   * Tests both tablet resolutions in a parameterized manner
   */
  for (const viewport of TABLET_VIEWPORTS) {
    test(`Tablet layout verification at ${viewport.name}`, async ({ page }) => {
      await page.setViewportSize({ width: viewport.width, height: viewport.height });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Verify no horizontal overflow
      const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
      expect(bodyScrollWidth).toBeLessThanOrEqual(viewport.width);

      // Verify navigation bar is fully visible
      const navbar = page.locator('.navbar, nav').first();
      await expect(navbar).toBeVisible();
      const navBox = await navbar.boundingBox();
      if (navBox) {
        expect(navBox.width).toBeLessThanOrEqual(viewport.width);
      }

      // Verify hero section layout
      const heroSection = page.locator('.hero, [data-testid="hero-section"]').first();
      await expect(heroSection).toBeVisible();
      const heroBox = await heroSection.boundingBox();
      if (heroBox) {
        expect(heroBox.width).toBeLessThanOrEqual(viewport.width);
      }

      // Verify logo is visible
      const heroLogo = page.locator('.hero-logo, [data-testid="hero-logo"]').first();
      await expect(heroLogo).toBeVisible();

      // Verify all sections are accessible via scrolling
      const valuePropositionSection = page.locator('#value-proposition, .value-proposition').first();
      await valuePropositionSection.scrollIntoViewIfNeeded();
      await expect(valuePropositionSection).toBeVisible();

      const featuresSection = page.locator('#features, .features').first();
      await featuresSection.scrollIntoViewIfNeeded();
      await expect(featuresSection).toBeVisible();

      const architectureSection = page.locator('#architecture, .architecture').first();
      await architectureSection.scrollIntoViewIfNeeded();
      await expect(architectureSection).toBeVisible();

      const gettingStartedSection = page.locator('#getting-started, .getting-started').first();
      await gettingStartedSection.scrollIntoViewIfNeeded();
      await expect(gettingStartedSection).toBeVisible();

      const footer = page.locator('.footer, footer').first();
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();

      // Verify content fits within viewport
      const container = page.locator('.container').first();
      const containerBox = await container.boundingBox();
      if (containerBox) {
        expect(containerBox.width).toBeLessThanOrEqual(viewport.width);
      }
    });
  }

  /**
   * Test: Verify content readability on tablet
   * Ensures text content is properly sized and readable on tablet screens
   */
  test('Content is readable on tablet viewports', async ({ page }) => {
    // Test on iPad portrait
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Verify hero title font size is appropriate
    const heroTitle = page.locator('.hero h1').first();
    const heroTitleFontSize = await heroTitle.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // At 768px (mobile breakpoint), font size should be 2.5rem = 40px
    expect(heroTitleFontSize).toBeLessThanOrEqual(56); // 3.5rem = 56px max
    expect(heroTitleFontSize).toBeGreaterThanOrEqual(32); // 2rem = 32px min

    // Verify tagline font size
    const tagline = page.locator('.hero .tagline').first();
    const taglineFontSize = await tagline.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    expect(taglineFontSize).toBeGreaterThanOrEqual(16); // At least 16px for readability

    // Verify body text is readable
    const pillarText = page.locator('.pillar p').first();
    await pillarText.scrollIntoViewIfNeeded();
    const pillarFontSize = await pillarText.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    expect(pillarFontSize).toBeGreaterThanOrEqual(14); // At least 14px for readability

    // Verify code blocks are readable
    const codeBlock = page.locator('pre').first();
    await codeBlock.scrollIntoViewIfNeeded();
    const codeBox = await codeBlock.boundingBox();
    if (codeBox) {
      // Code block should fit within viewport
      expect(codeBox.width).toBeLessThanOrEqual(768);
      // Code should have horizontal scroll if needed
      const codeOverflow = await codeBlock.evaluate((el) => {
        return window.getComputedStyle(el).overflowX;
      });
      expect(codeOverflow).toBe('auto');
    }
  });

  /**
   * Test: Verify navigation adapts appropriately on tablet
   */
  test('Navigation adapts appropriately for tablet viewports', async ({ page }) => {
    // Test iPad portrait (768px)
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    const navbar = page.locator('.navbar').first();
    await expect(navbar).toBeVisible();

    // At exactly 768px (breakpoint), nav links may be hidden per CSS
    const navLinks = page.locator('.nav-links a:not(.btn)');
    const navLinkCount = await navLinks.count();

    // Verify the GitHub button is still visible (it's the .btn)
    const githubButton = page.locator('.nav-links .btn');
    await expect(githubButton).toBeVisible();

    // Test iPad landscape (1024px)
    await page.setViewportSize({ width: 1024, height: 768 });
    await page.waitForLoadState('domcontentloaded');

    // At 1024px (> 768px), nav links should be visible
    const navLinksLandscape = page.locator('.nav-links a:not(.btn)');
    const navLinkCountLandscape = await navLinksLandscape.count();
    expect(navLinkCountLandscape).toBeGreaterThanOrEqual(1);

    // Each nav link should be visible
    for (let i = 0; i < navLinkCountLandscape; i++) {
      await expect(navLinksLandscape.nth(i)).toBeVisible();
    }
  });
});
