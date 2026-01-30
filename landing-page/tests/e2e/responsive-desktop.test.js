/**
 * MirDB Landing Page - Desktop Responsive Design Tests
 * Owner: Scenario 11 - Responsive Design - Desktop
 *
 * Tests verify:
 * 1. Page renders with full desktop layout at 1280px viewport
 * 2. Feature cards display in 4-column grid
 * 3. Content has reasonable max-width and is centered
 * 4. Navigation shows all links horizontally
 */

const { test, expect } = require('@playwright/test');
const { setupPage, setViewport, VIEWPORTS } = require('../helpers/test-utils');

test.describe('Responsive Design - Desktop (> 1024px)', () => {
  test.beforeEach(async ({ page }) => {
    // Set desktop viewport before navigation
    await setViewport(page, 'desktop');
    await setupPage(page);
  });

  test('Test Case 1: Page renders with full desktop layout at 1280px viewport', async ({ page }) => {
    // Verify viewport is set to desktop width
    const viewportSize = page.viewportSize();
    expect(viewportSize.width).toBe(VIEWPORTS.desktop.width);
    expect(viewportSize.height).toBe(VIEWPORTS.desktop.height);

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

    // Verify no horizontal overflow at document level
    const htmlScrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    const htmlClientWidth = await page.evaluate(() => document.documentElement.clientWidth);
    expect(htmlScrollWidth).toBeLessThanOrEqual(htmlClientWidth + 1);
  });

  test('Test Case 2: Feature cards display in 4-column grid', async ({ page }) => {
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

    // Get positions of all four cards to verify 4-column layout
    const firstCard = featureCards.nth(0);
    const secondCard = featureCards.nth(1);
    const thirdCard = featureCards.nth(2);
    const fourthCard = featureCards.nth(3);

    const firstBox = await firstCard.boundingBox();
    const secondBox = await secondCard.boundingBox();
    const thirdBox = await thirdCard.boundingBox();
    const fourthBox = await fourthCard.boundingBox();

    // All four cards should be on the same row (similar y position)
    // Allow for small differences due to potential padding/margin differences
    expect(Math.abs(firstBox.y - secondBox.y)).toBeLessThan(10);
    expect(Math.abs(firstBox.y - thirdBox.y)).toBeLessThan(10);
    expect(Math.abs(firstBox.y - fourthBox.y)).toBeLessThan(10);

    // Cards should be side by side (increasing x positions)
    expect(secondBox.x).toBeGreaterThan(firstBox.x + firstBox.width - 20);
    expect(thirdBox.x).toBeGreaterThan(secondBox.x + secondBox.width - 20);
    expect(fourthBox.x).toBeGreaterThan(thirdBox.x + thirdBox.width - 20);

    // Verify grid has 4 columns by checking computed style
    // gridTemplateColumns should show four values (e.g., "250px 250px 250px 250px" or similar)
    const columnValues = gridStyles.gridTemplateColumns.split(' ').filter(v => v && v !== 'none');
    expect(columnValues.length).toBe(4);
  });

  test('Test Case 3: Content has reasonable max-width and is centered', async ({ page }) => {
    // Check container max-width
    const container = page.locator('.container').first();
    await expect(container).toBeVisible();

    const containerStyles = await container.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        maxWidth: styles.maxWidth,
        marginLeft: styles.marginLeft,
        marginRight: styles.marginRight,
        width: el.offsetWidth,
        rect: el.getBoundingClientRect()
      };
    });

    // Container should have a max-width set (not "none")
    expect(containerStyles.maxWidth).not.toBe('none');

    // Parse the max-width value
    const maxWidthValue = parseFloat(containerStyles.maxWidth);
    // Max-width should be a reasonable value (between 900px and 1400px)
    expect(maxWidthValue).toBeGreaterThanOrEqual(900);
    expect(maxWidthValue).toBeLessThanOrEqual(1400);

    // Check that container is centered (margins should be auto, resulting in equal left/right)
    // On desktop with viewport > max-width, we expect the container to be centered
    const viewportWidth = VIEWPORTS.desktop.width;
    const containerLeft = containerStyles.rect.left;
    const containerRight = viewportWidth - (containerStyles.rect.left + containerStyles.width);

    // Container should be approximately centered (left and right margins should be similar)
    // Allow for some tolerance due to padding and scrollbar
    expect(Math.abs(containerLeft - containerRight)).toBeLessThan(50);

    // Check that content sections are properly contained
    const featuresContainer = page.locator('#features .container');
    const featuresContainerExists = await featuresContainer.count() > 0;
    if (featuresContainerExists) {
      const featuresStyles = await featuresContainer.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          maxWidth: styles.maxWidth,
          width: el.offsetWidth
        };
      });
      expect(featuresStyles.maxWidth).not.toBe('none');
    }
  });

  test('Test Case 4: Navigation shows all links horizontally', async ({ page }) => {
    const nav = page.locator('.nav');
    await expect(nav).toBeVisible();

    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Check that nav-links has display: flex
    const displayStyle = await navLinks.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(displayStyle).toBe('flex');

    // Verify all navigation links are visible
    const links = page.locator('.nav-links a');
    const linkCount = await links.count();
    expect(linkCount).toBeGreaterThan(0);

    // Get all link bounding boxes to verify horizontal layout
    const linkBoxes = [];
    for (let i = 0; i < linkCount; i++) {
      const link = links.nth(i);
      await expect(link).toBeVisible();
      const linkBox = await link.boundingBox();
      expect(linkBox).not.toBeNull();
      expect(linkBox.width).toBeGreaterThan(0);
      expect(linkBox.height).toBeGreaterThan(0);
      linkBoxes.push(linkBox);
    }

    // Verify all links are on the same row (similar y positions)
    if (linkBoxes.length > 1) {
      for (let i = 1; i < linkBoxes.length; i++) {
        // Links should be on the same horizontal line
        expect(Math.abs(linkBoxes[i].y - linkBoxes[0].y)).toBeLessThan(20);
      }
    }

    // Verify links are arranged horizontally (each subsequent link has greater x)
    for (let i = 1; i < linkBoxes.length; i++) {
      expect(linkBoxes[i].x).toBeGreaterThan(linkBoxes[i - 1].x);
    }

    // Verify no hamburger menu is visible (desktop should show full nav)
    const hamburgerMenu = page.locator('.hamburger-menu, .menu-toggle, .mobile-menu-btn');
    const hamburgerCount = await hamburgerMenu.count();
    if (hamburgerCount > 0) {
      const isHamburgerHidden = await hamburgerMenu.first().isHidden();
      expect(isHamburgerHidden).toBe(true);
    }

    // Navigation should not have overflow on desktop
    const overflowStyle = await navLinks.evaluate((el) => {
      return window.getComputedStyle(el).overflowX;
    });
    // On desktop, nav should not need to scroll
    expect(['visible', 'auto']).toContain(overflowStyle);
  });

  // Additional tests for comprehensive desktop coverage

  test('Hero section has appropriate desktop sizing', async ({ page }) => {
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();

    // Check hero logo sizing
    const heroLogo = page.locator('.hero-logo');
    const logoCount = await heroLogo.count();
    if (logoCount > 0) {
      await expect(heroLogo).toBeVisible();
      const logoBox = await heroLogo.boundingBox();
      // Logo should be appropriately sized on desktop (larger than tablet)
      expect(logoBox.width).toBeGreaterThanOrEqual(150);
      expect(logoBox.width).toBeLessThanOrEqual(400);
    }

    // Check hero title font size
    const heroTitle = page.locator('.hero-title');
    await expect(heroTitle).toBeVisible();
    const titleFontSize = await heroTitle.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // Title should be larger on desktop (at least 36px)
    expect(titleFontSize).toBeGreaterThanOrEqual(36);

    // CTA buttons should be horizontal on desktop
    const heroCta = page.locator('.hero-cta');
    await expect(heroCta).toBeVisible();

    const ctaButtons = page.locator('.hero-cta .btn');
    const buttonCount = await ctaButtons.count();
    if (buttonCount >= 2) {
      const firstBtn = ctaButtons.nth(0);
      const secondBtn = ctaButtons.nth(1);

      const firstBtnBox = await firstBtn.boundingBox();
      const secondBtnBox = await secondBtn.boundingBox();

      // Buttons should be side by side (second button to the right)
      expect(secondBtnBox.x).toBeGreaterThan(firstBtnBox.x);

      // Buttons should be on the same row
      expect(Math.abs(firstBtnBox.y - secondBtnBox.y)).toBeLessThan(20);
    }
  });

  test('Architecture explanations display in multi-column grid', async ({ page }) => {
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

      // On desktop, should have 2 or more columns
      const columnValues = gridStyle.gridTemplateColumns.split(' ').filter(v => v && v !== 'none');
      expect(columnValues.length).toBeGreaterThanOrEqual(2);
    }
  });

  test('Installation options display in multi-column grid', async ({ page }) => {
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

      // On desktop, should have 2 columns for installation options
      const columnValues = gridStyle.gridTemplateColumns.split(' ').filter(v => v && v !== 'none');
      expect(columnValues.length).toBeGreaterThanOrEqual(2);
    }
  });

  test('Footer has horizontal row layout on desktop', async ({ page }) => {
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    const footerContent = page.locator('.footer-content');
    const contentCount = await footerContent.count();

    if (contentCount > 0) {
      const flexDirection = await footerContent.evaluate((el) => {
        return window.getComputedStyle(el).flexDirection;
      });

      // Footer content should be in row layout on desktop
      expect(flexDirection).toBe('row');
    }
  });

  test('All sections fit within desktop viewport without horizontal scroll', async ({ page }) => {
    const sections = ['#hero', '#features', '#usage', '#architecture', '#configuration', '#getting-started'];

    for (const sectionId of sections) {
      const section = page.locator(sectionId);
      const exists = await section.count() > 0;

      if (exists) {
        const sectionBox = await section.boundingBox();
        // Section should not exceed viewport width
        expect(sectionBox.width).toBeLessThanOrEqual(VIEWPORTS.desktop.width + 1);
      }
    }

    // Verify no horizontal scroll on the page
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const bodyClientWidth = await page.evaluate(() => document.body.clientWidth);
    expect(bodyScrollWidth).toBeLessThanOrEqual(bodyClientWidth + 1);
  });

  test('Section spacing is appropriate for desktop', async ({ page }) => {
    // Check that sections have adequate padding on desktop
    const section = page.locator('section').first();
    const padding = await section.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        top: parseFloat(styles.paddingTop),
        bottom: parseFloat(styles.paddingBottom)
      };
    });

    // Desktop should have generous section padding (at least 48px)
    expect(padding.top).toBeGreaterThanOrEqual(48);
    expect(padding.bottom).toBeGreaterThanOrEqual(48);
  });
});
