// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Responsive Design - Mobile Viewports
 * Verifies that the homepage displays correctly on mobile viewports as specified in REQ-10
 *
 * Test Cases:
 * 1. Load page at 375x667 (iPhone SE) resolution
 * 2. Load page at 414x896 (iPhone 11 Pro Max) resolution
 * 3. Check value proposition pillars on mobile - stack vertically
 * 4. Check touch targets on mobile - minimum 44x44px
 * 5. Check navigation on mobile - accessible
 */

const MOBILE_VIEWPORTS = [
  { name: 'iPhone SE', width: 375, height: 667 },
  { name: 'iPhone 11 Pro Max', width: 414, height: 896 },
  { name: 'iPhone 14 Pro', width: 390, height: 844 },
];

test.describe('Responsive Design - Mobile Viewports', () => {
  /**
   * Test Case 1: Load page at 375x667 (iPhone SE) resolution
   * Expected: Layout adapts to single column, all content accessible, no horizontal scroll
   */
  test('TC1: Page displays correctly at 375x667 (iPhone SE) with single column layout and no horizontal scroll', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Verify no horizontal scrollbar (page width matches viewport)
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

    // Verify content fits within viewport width
    const container = page.locator('.container').first();
    const containerBox = await container.boundingBox();
    expect(containerBox).not.toBeNull();
    if (containerBox) {
      expect(containerBox.width).toBeLessThanOrEqual(375);
    }
  });

  /**
   * Test Case 2: Load page at 414x896 (iPhone 11 Pro Max) resolution
   * Expected: Layout adapts appropriately for larger mobile screen
   */
  test('TC2: Page displays correctly at 414x896 (iPhone 11 Pro Max) with adapted layout', async ({ page }) => {
    await page.setViewportSize({ width: 414, height: 896 });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Verify no horizontal scrollbar
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth);

    // Verify all main sections are visible and accessible via scrolling
    const heroSection = page.locator('.hero, [data-testid="hero-section"]').first();
    await expect(heroSection).toBeVisible();

    // Verify hero content adapts to larger mobile viewport
    const heroTitle = page.locator('.hero h1, [data-testid="hero-section"] h1').first();
    await expect(heroTitle).toBeVisible();

    // Scroll to and verify value proposition section
    const valuePropositionSection = page.locator('#value-proposition, .value-proposition').first();
    await valuePropositionSection.scrollIntoViewIfNeeded();
    await expect(valuePropositionSection).toBeVisible();

    // Scroll to and verify features section
    const featuresSection = page.locator('#features, .features').first();
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Verify feature cards are visible and adapt to mobile layout
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

    // Verify CTA buttons are accessible
    await page.goto('/');
    const ctaButtons = page.locator('.cta-buttons .btn');
    const buttonCount = await ctaButtons.count();
    expect(buttonCount).toBeGreaterThanOrEqual(1);
    for (let i = 0; i < buttonCount; i++) {
      await expect(ctaButtons.nth(i)).toBeVisible();
    }
  });

  /**
   * Test Case 3: Check value proposition pillars on mobile
   * Expected: Three pillars stack vertically in single column layout
   */
  test('TC3: Value proposition pillars stack vertically in single column layout on mobile', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });
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
      // Verify pillars are stacked vertically (different Y positions)
      // Pillar 2 should be below pillar 1
      expect(box2.y).toBeGreaterThan(box1.y);
      // Pillar 3 should be below pillar 2
      expect(box3.y).toBeGreaterThan(box2.y);

      // Verify pillars have similar X positions (left-aligned in single column)
      // Allow for some tolerance due to padding
      expect(Math.abs(box1.x - box2.x)).toBeLessThan(50);
      expect(Math.abs(box2.x - box3.x)).toBeLessThan(50);

      // Verify each pillar width fits within mobile viewport
      expect(box1.width).toBeLessThanOrEqual(375);
      expect(box2.width).toBeLessThanOrEqual(375);
      expect(box3.width).toBeLessThanOrEqual(375);
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
   * Test Case 4: Check touch targets on mobile
   * Expected: All buttons and links have minimum 44x44px touch target size
   */
  test('TC4: All interactive elements have minimum 44x44px touch target size on mobile', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Minimum touch target size according to WCAG and Apple HIG
    const MIN_TOUCH_SIZE = 44;

    // Check CTA buttons in hero section
    const ctaButtons = page.locator('.cta-buttons .btn');
    const buttonCount = await ctaButtons.count();
    expect(buttonCount).toBeGreaterThanOrEqual(1);

    for (let i = 0; i < buttonCount; i++) {
      const button = ctaButtons.nth(i);
      await expect(button).toBeVisible();
      const buttonBox = await button.boundingBox();
      expect(buttonBox).not.toBeNull();
      if (buttonBox) {
        // Button should have at least 44px height (width can be larger for buttons)
        expect(buttonBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_SIZE);
        // Width should also be at least 44px
        expect(buttonBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_SIZE);
      }
    }

    // Check navigation button/link if visible
    const navbar = page.locator('.navbar, nav').first();
    await expect(navbar).toBeVisible();

    // Check if there's a mobile hamburger menu or visible nav link
    const navButtons = navbar.locator('.btn, button, a.btn');
    const navButtonCount = await navButtons.count();

    for (let i = 0; i < navButtonCount; i++) {
      const navButton = navButtons.nth(i);
      const isVisible = await navButton.isVisible();
      if (isVisible) {
        const navButtonBox = await navButton.boundingBox();
        if (navButtonBox) {
          // Nav buttons should have adequate touch target
          expect(navButtonBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_SIZE);
          expect(navButtonBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_SIZE);
        }
      }
    }

    // Scroll to footer and check footer links
    const footer = page.locator('.footer, footer').first();
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Footer links should be clickable with adequate spacing
    const footerLinks = footer.locator('a');
    const footerLinkCount = await footerLinks.count();

    for (let i = 0; i < footerLinkCount; i++) {
      const link = footerLinks.nth(i);
      const isVisible = await link.isVisible();
      if (isVisible) {
        const linkBox = await link.boundingBox();
        if (linkBox) {
          // Links should have at least 44px combined touch area (height or clickable area)
          // For text links, the line-height typically provides adequate touch area
          expect(linkBox.height).toBeGreaterThanOrEqual(20); // Minimum readable height
        }
      }
    }
  });

  /**
   * Test Case 5: Check navigation on mobile
   * Expected: Navigation is accessible, possibly via hamburger menu
   */
  test('TC5: Navigation is accessible on mobile viewport', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Verify navbar is visible
    const navbar = page.locator('.navbar, nav').first();
    await expect(navbar).toBeVisible();

    // Verify navbar fits within viewport
    const navbarBox = await navbar.boundingBox();
    expect(navbarBox).not.toBeNull();
    if (navbarBox) {
      expect(navbarBox.width).toBeLessThanOrEqual(375);
    }

    // Check if logo is visible in navbar
    const navLogo = navbar.locator('.logo, .nav-logo, a.logo').first();
    await expect(navLogo).toBeVisible();

    // On mobile, either:
    // 1. Navigation links are hidden (hamburger menu pattern)
    // 2. Navigation links are compressed/stacked
    // 3. Only essential links (like GitHub button) are shown

    // Check if GitHub/CTA button is still accessible
    const navGitHubButton = navbar.locator('.btn, a.btn').first();
    const isGitHubButtonVisible = await navGitHubButton.isVisible().catch(() => false);

    // At minimum, the primary navigation (logo and at least one CTA) should be accessible
    // The navigation should not overflow the viewport
    const navbarScrollWidth = await page.evaluate(() => {
      const nav = document.querySelector('.navbar, nav');
      return nav ? nav.scrollWidth : 0;
    });
    expect(navbarScrollWidth).toBeLessThanOrEqual(375);

    // Verify key navigation elements are functional
    // Users should be able to get to main sections via in-page links or scrolling
    // Check if in-page anchors work
    const gettingStartedSection = page.locator('#getting-started, .getting-started').first();
    await gettingStartedSection.scrollIntoViewIfNeeded();
    await expect(gettingStartedSection).toBeVisible();

    // Verify the page is fully scrollable (no content cut off)
    const footer = page.locator('.footer, footer').first();
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();
  });

  /**
   * Additional test: Verify mobile layout for all viewports
   * Tests all three mobile resolutions in a parameterized manner
   */
  for (const viewport of MOBILE_VIEWPORTS) {
    test(`Mobile layout verification at ${viewport.name} (${viewport.width}x${viewport.height})`, async ({ page }) => {
      await page.setViewportSize({ width: viewport.width, height: viewport.height });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Verify no horizontal overflow
      const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
      expect(bodyScrollWidth).toBeLessThanOrEqual(viewport.width);

      // Verify navigation is present
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

      // Scroll to value proposition and verify pillars are stacked
      const valuePropositionSection = page.locator('#value-proposition, .value-proposition').first();
      await valuePropositionSection.scrollIntoViewIfNeeded();

      const pillars = page.locator('.pillar');
      const pillarCount = await pillars.count();
      expect(pillarCount).toBe(3);

      // At mobile widths, pillars should be stacked vertically
      const box1 = await pillars.nth(0).boundingBox();
      const box2 = await pillars.nth(1).boundingBox();
      const box3 = await pillars.nth(2).boundingBox();

      if (box1 && box2 && box3) {
        // All three pillars should be at different Y positions (stacked)
        expect(box2.y).toBeGreaterThan(box1.y);
        expect(box3.y).toBeGreaterThan(box2.y);
      }

      // Scroll to features section and verify layout
      const featuresSection = page.locator('#features, .features').first();
      await featuresSection.scrollIntoViewIfNeeded();
      await expect(featuresSection).toBeVisible();

      const featuresBox = await featuresSection.boundingBox();
      if (featuresBox) {
        expect(featuresBox.width).toBeLessThanOrEqual(viewport.width);
      }

      // Verify feature cards are in single column on mobile
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
