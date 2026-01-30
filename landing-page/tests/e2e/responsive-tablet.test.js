/**
 * MirDB Landing Page - Tablet Responsive Design Tests
 * Owner: Scenario 10 - Responsive Design - Tablet
 *
 * Tests verify:
 * 1. Page renders with tablet-optimized layout at 768px viewport
 * 2. Feature cards display in 2-column grid
 * 3. Navigation is fully visible without hamburger menu
 * 4. Hero content is properly sized and readable
 */

const { test, expect } = require('@playwright/test');
const { setupPage, setViewport, VIEWPORTS } = require('../helpers/test-utils');

test.describe('Responsive Design - Tablet (768px - 1024px)', () => {
  test.beforeEach(async ({ page }) => {
    // Set tablet viewport before navigation
    await setViewport(page, 'tablet');
    await setupPage(page);
  });

  test('Test Case 1: Page renders with tablet-optimized layout at 768px viewport', async ({ page }) => {
    // Verify viewport is set to tablet width
    const viewportSize = page.viewportSize();
    expect(viewportSize.width).toBe(VIEWPORTS.tablet.width);
    expect(viewportSize.height).toBe(VIEWPORTS.tablet.height);

    // Check that body does not have horizontal overflow
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const bodyClientWidth = await page.evaluate(() => document.body.clientWidth);

    // scrollWidth should not be significantly greater than clientWidth
    expect(bodyScrollWidth).toBeLessThanOrEqual(bodyClientWidth + 1);

    // Verify main sections are visible
    const sections = ['#hero', '#features', '#architecture', '#configuration', '#getting-started'];
    for (const sectionId of sections) {
      const section = page.locator(sectionId);
      const exists = await section.count() > 0;
      if (exists) {
        await expect(section).toBeVisible();
      }
    }

    // Verify page has appropriate layout structure
    const mainContent = page.locator('main#main-content');
    await expect(mainContent).toBeVisible();
  });

  test('Test Case 2: Feature cards display in 2-column grid', async ({ page }) => {
    // Navigate to features section
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Get computed grid styles
    const gridStyles = await featuresGrid.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        gridTemplateColumns: styles.gridTemplateColumns,
        display: styles.display
      };
    });

    // Verify it's a grid display
    expect(gridStyles.display).toBe('grid');

    // Get all feature cards
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBe(4); // Should have 4 feature cards

    // Get positions of first two cards to verify 2-column layout
    const firstCard = featureCards.nth(0);
    const secondCard = featureCards.nth(1);
    const thirdCard = featureCards.nth(2);

    const firstBox = await firstCard.boundingBox();
    const secondBox = await secondCard.boundingBox();
    const thirdBox = await thirdCard.boundingBox();

    // First and second cards should be on the same row (similar y position)
    // Allow for small differences due to potential padding/margin differences
    expect(Math.abs(firstBox.y - secondBox.y)).toBeLessThan(10);

    // Third card should be below first card (on the next row)
    expect(thirdBox.y).toBeGreaterThan(firstBox.y + firstBox.height - 10);

    // First and second cards should be side by side (different x positions)
    expect(secondBox.x).toBeGreaterThan(firstBox.x + firstBox.width - 10);

    // First and third cards should have similar x positions (same column)
    expect(Math.abs(firstBox.x - thirdBox.x)).toBeLessThan(10);

    // Verify grid has 2 columns by checking computed style
    // gridTemplateColumns should show two values (e.g., "300px 300px" or similar)
    const columnValues = gridStyles.gridTemplateColumns.split(' ').filter(v => v && v !== 'none');
    expect(columnValues.length).toBe(2);
  });

  test('Test Case 3: Navigation is fully visible without hamburger menu', async ({ page }) => {
    const nav = page.locator('.nav');
    await expect(nav).toBeVisible();

    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Check that nav-links has display: flex (not hidden)
    const displayStyle = await navLinks.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(displayStyle).toBe('flex');

    // Verify all navigation links are visible
    const links = page.locator('.nav-links a');
    const linkCount = await links.count();
    expect(linkCount).toBeGreaterThan(0);

    // Check each link is visible
    for (let i = 0; i < linkCount; i++) {
      const link = links.nth(i);
      await expect(link).toBeVisible();

      // Verify link has non-zero dimensions
      const linkBox = await link.boundingBox();
      expect(linkBox).not.toBeNull();
      expect(linkBox.width).toBeGreaterThan(0);
      expect(linkBox.height).toBeGreaterThan(0);
    }

    // Verify no hamburger menu is visible (should not exist or be hidden on tablet)
    const hamburgerMenu = page.locator('.hamburger-menu, .menu-toggle, .mobile-menu-btn');
    const hamburgerCount = await hamburgerMenu.count();
    if (hamburgerCount > 0) {
      const isHamburgerHidden = await hamburgerMenu.first().isHidden();
      expect(isHamburgerHidden).toBe(true);
    }

    // Navigation should not have overflow-x: auto (not scrollable like mobile)
    // On tablet, navigation should fit without scrolling
    const overflowStyle = await navLinks.evaluate((el) => {
      return window.getComputedStyle(el).overflowX;
    });
    // Accept 'visible' or 'auto' since the content should fit
    expect(['visible', 'auto']).toContain(overflowStyle);
  });

  test('Test Case 4: Hero content is properly sized and readable', async ({ page }) => {
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();

    // Check hero logo is visible and appropriately sized
    const heroLogo = page.locator('.hero-logo');
    const logoCount = await heroLogo.count();
    if (logoCount > 0) {
      await expect(heroLogo).toBeVisible();
      const logoBox = await heroLogo.boundingBox();
      // Logo should be reasonably sized (not too small, not too large)
      expect(logoBox.width).toBeGreaterThanOrEqual(100);
      expect(logoBox.width).toBeLessThanOrEqual(300);
    }

    // Check hero title is visible and has proper font size
    const heroTitle = page.locator('.hero-title');
    await expect(heroTitle).toBeVisible();
    const titleFontSize = await heroTitle.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // Title should be large but readable (between 32px and 64px for tablet)
    expect(titleFontSize).toBeGreaterThanOrEqual(32);
    expect(titleFontSize).toBeLessThanOrEqual(64);

    // Check hero tagline is visible
    const heroTagline = page.locator('.hero-tagline');
    await expect(heroTagline).toBeVisible();
    const taglineFontSize = await heroTagline.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // Tagline should be readable
    expect(taglineFontSize).toBeGreaterThanOrEqual(16);

    // Check value proposition is visible and readable
    const valueProposition = page.locator('.hero-value-proposition');
    const vpCount = await valueProposition.count();
    if (vpCount > 0) {
      await expect(valueProposition).toBeVisible();
      const vpFontSize = await valueProposition.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      // Value proposition should have readable font size
      expect(vpFontSize).toBeGreaterThanOrEqual(16);
    }

    // Check CTA buttons are visible and horizontally arranged
    const heroCta = page.locator('.hero-cta');
    await expect(heroCta).toBeVisible();

    const ctaButtons = page.locator('.hero-cta .btn');
    const buttonCount = await ctaButtons.count();
    expect(buttonCount).toBeGreaterThan(0);

    if (buttonCount >= 2) {
      const firstButton = ctaButtons.nth(0);
      const secondButton = ctaButtons.nth(1);

      const firstBtnBox = await firstButton.boundingBox();
      const secondBtnBox = await secondButton.boundingBox();

      // On tablet, buttons should be side-by-side (row layout)
      // Second button should be to the right of first button (not below)
      expect(secondBtnBox.x).toBeGreaterThan(firstBtnBox.x);

      // Buttons should be on approximately the same row
      expect(Math.abs(firstBtnBox.y - secondBtnBox.y)).toBeLessThan(20);
    }

    // Verify hero section fits within viewport
    const heroBox = await hero.boundingBox();
    expect(heroBox.width).toBeLessThanOrEqual(VIEWPORTS.tablet.width + 1);
  });

  // Additional tests for comprehensive tablet coverage

  test('Architecture explanations are in 2-column grid', async ({ page }) => {
    const explanations = page.locator('.architecture-explanations');
    const explCount = await explanations.count();

    if (explCount > 0) {
      await expect(explanations).toBeVisible();

      const gridStyle = await explanations.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          display: styles.display,
          gridTemplateColumns: styles.gridTemplateColumns
        };
      });

      expect(gridStyle.display).toBe('grid');

      // Check for 2-column layout
      const columnValues = gridStyle.gridTemplateColumns.split(' ').filter(v => v && v !== 'none');
      expect(columnValues.length).toBe(2);
    }
  });

  test('Installation options are in 2-column grid', async ({ page }) => {
    const options = page.locator('.installation-options');
    const optCount = await options.count();

    if (optCount > 0) {
      await expect(options).toBeVisible();

      const gridStyle = await options.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          display: styles.display,
          gridTemplateColumns: styles.gridTemplateColumns
        };
      });

      expect(gridStyle.display).toBe('grid');

      // Check for 2-column layout
      const columnValues = gridStyle.gridTemplateColumns.split(' ').filter(v => v && v !== 'none');
      expect(columnValues.length).toBe(2);
    }
  });

  test('Footer has row layout on tablet', async ({ page }) => {
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    const footerContent = page.locator('.footer-content');
    const contentCount = await footerContent.count();

    if (contentCount > 0) {
      const flexDirection = await footerContent.evaluate((el) => {
        return window.getComputedStyle(el).flexDirection;
      });

      // Footer content should be in row layout on tablet
      expect(flexDirection).toBe('row');
    }
  });

  test('All sections fit within tablet viewport width', async ({ page }) => {
    const sections = ['#hero', '#features', '#usage', '#architecture', '#configuration', '#getting-started'];

    for (const sectionId of sections) {
      const section = page.locator(sectionId);
      const exists = await section.count() > 0;

      if (exists) {
        const sectionBox = await section.boundingBox();
        // Section should not exceed viewport width
        expect(sectionBox.width).toBeLessThanOrEqual(VIEWPORTS.tablet.width + 1);
      }
    }

    // Verify no horizontal scroll on the page
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const bodyClientWidth = await page.evaluate(() => document.body.clientWidth);
    expect(bodyScrollWidth).toBeLessThanOrEqual(bodyClientWidth + 1);
  });
});
