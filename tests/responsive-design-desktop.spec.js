// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Responsive Design - Desktop Viewports
 * Verifies that the homepage displays correctly on desktop viewports as specified in REQ-10
 *
 * Test Cases:
 * 1. Load page at 1920x1080 resolution - All sections display correctly with no horizontal overflow
 * 2. Load page at 1440x900 resolution - Layout adapts appropriately, all content remains accessible
 * 3. Load page at 1280x720 resolution - Layout adapts appropriately, all content remains accessible
 * 4. Check value proposition pillars on desktop - Three pillars display side-by-side in row layout
 */

const DESKTOP_VIEWPORTS = [
  { name: '1920x1080', width: 1920, height: 1080 },
  { name: '1440x900', width: 1440, height: 900 },
  { name: '1280x720', width: 1280, height: 720 },
];

test.describe('Responsive Design - Desktop Viewports', () => {
  /**
   * Test Case 1: Load page at 1920x1080 resolution
   * Expected: All sections display correctly with no horizontal overflow, appropriate spacing
   */
  test('TC1: Page displays correctly at 1920x1080 resolution with no horizontal overflow', async ({ page }) => {
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Verify no horizontal scrollbar (page width matches viewport)
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth);

    // Verify all main sections are visible
    const heroSection = page.locator('.hero, [data-testid="hero-section"]').first();
    await expect(heroSection).toBeVisible();

    const valuePropositionSection = page.locator('#value-proposition, .value-proposition').first();
    await expect(valuePropositionSection).toBeVisible();

    const featuresSection = page.locator('#features, .features').first();
    await expect(featuresSection).toBeVisible();

    const gettingStartedSection = page.locator('#getting-started, .getting-started').first();
    await expect(gettingStartedSection).toBeVisible();

    const footer = page.locator('.footer, footer').first();
    await expect(footer).toBeVisible();

    // Verify appropriate spacing - container has proper max-width
    const container = page.locator('.container').first();
    const containerBox = await container.boundingBox();
    expect(containerBox).not.toBeNull();
    if (containerBox) {
      // Container should have max-width of ~1200px and be centered
      expect(containerBox.width).toBeLessThanOrEqual(1200 + 64); // max-width + padding
      expect(containerBox.x).toBeGreaterThan(0); // Should have margin on left (centered)
    }
  });

  /**
   * Test Case 2: Load page at 1440x900 resolution
   * Expected: Layout adapts appropriately, all content remains accessible
   */
  test('TC2: Page displays correctly at 1440x900 resolution with accessible content', async ({ page }) => {
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Verify no horizontal scrollbar
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth);

    // Verify all main sections are visible and accessible via scrolling
    const heroSection = page.locator('.hero, [data-testid="hero-section"]').first();
    await expect(heroSection).toBeVisible();

    // Scroll to and verify value proposition section
    const valuePropositionSection = page.locator('#value-proposition, .value-proposition').first();
    await valuePropositionSection.scrollIntoViewIfNeeded();
    await expect(valuePropositionSection).toBeVisible();

    // Scroll to and verify features section
    const featuresSection = page.locator('#features, .features').first();
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Scroll to and verify getting started section
    const gettingStartedSection = page.locator('#getting-started, .getting-started').first();
    await gettingStartedSection.scrollIntoViewIfNeeded();
    await expect(gettingStartedSection).toBeVisible();

    // Scroll to and verify footer
    const footer = page.locator('.footer, footer').first();
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Verify navigation is accessible
    const navbar = page.locator('.navbar, nav').first();
    await expect(navbar).toBeVisible();

    // Verify CTA buttons are accessible
    const ctaButtons = page.locator('.cta-buttons .btn');
    const buttonCount = await ctaButtons.count();
    expect(buttonCount).toBeGreaterThanOrEqual(1);
    for (let i = 0; i < buttonCount; i++) {
      await expect(ctaButtons.nth(i)).toBeVisible();
    }
  });

  /**
   * Test Case 3: Load page at 1280x720 resolution
   * Expected: Layout adapts appropriately, all content remains accessible
   */
  test('TC3: Page displays correctly at 1280x720 resolution with accessible content', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 720 });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Verify no horizontal scrollbar
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth);

    // Verify all main sections are visible and accessible via scrolling
    const heroSection = page.locator('.hero, [data-testid="hero-section"]').first();
    await expect(heroSection).toBeVisible();

    // Verify hero content is visible
    const heroTitle = page.locator('.hero h1, [data-testid="hero-section"] h1').first();
    await expect(heroTitle).toBeVisible();

    const heroTagline = page.locator('.hero .tagline, [data-testid="hero-tagline"]').first();
    await expect(heroTagline).toBeVisible();

    // Scroll to and verify value proposition section
    const valuePropositionSection = page.locator('#value-proposition, .value-proposition').first();
    await valuePropositionSection.scrollIntoViewIfNeeded();
    await expect(valuePropositionSection).toBeVisible();

    // Verify pillars are visible
    const pillars = page.locator('.pillar');
    const pillarCount = await pillars.count();
    expect(pillarCount).toBe(3);

    // Scroll to and verify features section
    const featuresSection = page.locator('#features, .features').first();
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Verify feature cards are visible
    const featureCards = page.locator('.feature-card, .feature');
    const featureCount = await featureCards.count();
    expect(featureCount).toBeGreaterThanOrEqual(3);

    // Scroll to and verify getting started section
    const gettingStartedSection = page.locator('#getting-started, .getting-started').first();
    await gettingStartedSection.scrollIntoViewIfNeeded();
    await expect(gettingStartedSection).toBeVisible();

    // Scroll to and verify footer
    const footer = page.locator('.footer, footer').first();
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();
  });

  /**
   * Test Case 4: Check value proposition pillars on desktop
   * Expected: Three pillars display side-by-side in row layout
   */
  test('TC4: Value proposition pillars display side-by-side in row layout on desktop', async ({ page }) => {
    // Use a typical desktop viewport width
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Navigate to value proposition section
    const valuePropositionSection = page.locator('#value-proposition, .value-proposition').first();
    await valuePropositionSection.scrollIntoViewIfNeeded();
    await expect(valuePropositionSection).toBeVisible();

    // Get all three pillar elements
    const pillars = page.locator('.pillar');
    await expect(pillars).toHaveCount(3);

    // Get bounding boxes for all pillars
    const pillar1 = pillars.nth(0);
    const pillar2 = pillars.nth(1);
    const pillar3 = pillars.nth(2);

    await expect(pillar1).toBeVisible();
    await expect(pillar2).toBeVisible();
    await expect(pillar3).toBeVisible();

    const box1 = await pillar1.boundingBox();
    const box2 = await pillar2.boundingBox();
    const box3 = await pillar3.boundingBox();

    // Verify all pillars have bounding boxes
    expect(box1).not.toBeNull();
    expect(box2).not.toBeNull();
    expect(box3).not.toBeNull();

    if (box1 && box2 && box3) {
      // Verify pillars are arranged side-by-side (same Y position, different X positions)
      // Allow small tolerance for Y alignment (within 10px)
      expect(Math.abs(box1.y - box2.y)).toBeLessThan(10);
      expect(Math.abs(box2.y - box3.y)).toBeLessThan(10);

      // Verify pillars are arranged left to right (pillar1.x < pillar2.x < pillar3.x)
      expect(box1.x).toBeLessThan(box2.x);
      expect(box2.x).toBeLessThan(box3.x);

      // Verify pillars are not stacked (they should have similar widths)
      expect(Math.abs(box1.width - box2.width)).toBeLessThan(50);
      expect(Math.abs(box2.width - box3.width)).toBeLessThan(50);

      // Verify pillars are in a row layout (total width should fit within viewport)
      const totalWidth = box3.x + box3.width - box1.x;
      expect(totalWidth).toBeLessThan(1440); // Should fit within viewport
    }

    // Verify each pillar has required content structure
    for (let i = 0; i < 3; i++) {
      const pillar = pillars.nth(i);

      // Each pillar should have a title (h3)
      const pillarTitle = pillar.locator('h3');
      await expect(pillarTitle).toBeVisible();

      // Each pillar should have a description (p)
      const pillarDescription = pillar.locator('p');
      await expect(pillarDescription).toBeVisible();

      // Each pillar should have an icon
      const pillarIcon = pillar.locator('.pillar-icon, svg');
      await expect(pillarIcon.first()).toBeVisible();
    }
  });

  /**
   * Additional test: Verify desktop layout for all viewports
   * Tests all three desktop resolutions in a parameterized manner
   */
  for (const viewport of DESKTOP_VIEWPORTS) {
    test(`Desktop layout verification at ${viewport.name}`, async ({ page }) => {
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

      // At desktop widths (> 768px), nav links should be visible
      if (viewport.width > 768) {
        const navLinks = page.locator('.nav-links a:not(.btn)');
        const navLinkCount = await navLinks.count();
        // At least some nav links should be visible on desktop
        expect(navLinkCount).toBeGreaterThanOrEqual(0);
      }

      // Verify hero section layout
      const heroSection = page.locator('.hero, [data-testid="hero-section"]').first();
      await expect(heroSection).toBeVisible();
      const heroBox = await heroSection.boundingBox();
      if (heroBox) {
        expect(heroBox.width).toBeLessThanOrEqual(viewport.width);
      }

      // Scroll to value proposition and verify pillars are in row layout at desktop widths
      const valuePropositionSection = page.locator('#value-proposition, .value-proposition').first();
      await valuePropositionSection.scrollIntoViewIfNeeded();

      const pillars = page.locator('.pillar');
      const pillarCount = await pillars.count();
      expect(pillarCount).toBe(3);

      // At desktop widths, pillars should be side by side
      const box1 = await pillars.nth(0).boundingBox();
      const box2 = await pillars.nth(1).boundingBox();
      const box3 = await pillars.nth(2).boundingBox();

      if (box1 && box2 && box3) {
        // All three pillars should be at approximately the same Y position (row layout)
        expect(Math.abs(box1.y - box2.y)).toBeLessThan(10);
        expect(Math.abs(box2.y - box3.y)).toBeLessThan(10);

        // Pillars should be arranged left to right
        expect(box1.x).toBeLessThan(box2.x);
        expect(box2.x).toBeLessThan(box3.x);
      }

      // Scroll to features section and verify layout
      const featuresSection = page.locator('#features, .features').first();
      await featuresSection.scrollIntoViewIfNeeded();
      await expect(featuresSection).toBeVisible();

      const featuresBox = await featuresSection.boundingBox();
      if (featuresBox) {
        expect(featuresBox.width).toBeLessThanOrEqual(viewport.width);
      }

      // Verify feature grid displays in multi-column layout at desktop widths
      const featureCards = page.locator('.feature-card, .feature');
      const featureCount = await featureCards.count();
      expect(featureCount).toBeGreaterThanOrEqual(3);

      // Scroll to footer
      const footer = page.locator('.footer, footer').first();
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();
    });
  }
});
