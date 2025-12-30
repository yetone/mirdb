// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: Responsive Design - Desktop
 * Scenario: Verify the page displays correctly on desktop viewport sizes
 * NFR-1: Page must be fully responsive across desktop, tablet, and mobile viewports
 */

test.describe('Responsive Design - Desktop', () => {
  // Set desktop viewport for all tests
  test.use({ viewport: { width: 1920, height: 1080 } });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Page renders correctly at 1920x1080 viewport
   * Input: Load page at 1920x1080 viewport
   * Expected: Page renders correctly with no horizontal scroll, all content visible
   */
  test('TC1: Page renders correctly at 1920x1080 with no horizontal scroll', async ({ page }) => {
    // Verify page has loaded
    const body = page.locator('body');
    await expect(body).toBeVisible();

    // Check there's no horizontal scrollbar (page width doesn't exceed viewport)
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Verify main sections are visible
    const header = page.locator('header.header');
    await expect(header).toBeVisible();

    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    const documentationSection = page.locator('#documentation');
    await expect(documentationSection).toBeVisible();

    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Verify no content is cut off horizontally
    const headerBox = await header.boundingBox();
    expect(headerBox).not.toBeNull();
    expect(headerBox.x).toBeGreaterThanOrEqual(0);

    // Verify content fits within viewport width
    const heroBox = await heroSection.boundingBox();
    expect(heroBox).not.toBeNull();
    expect(heroBox.width).toBeLessThanOrEqual(1920);
  });

  /**
   * Test Case 2: Full navigation links visible on desktop
   * Input: Check navigation on desktop
   * Expected: Full navigation links visible in header (not collapsed into hamburger menu)
   */
  test('TC2: Navigation links fully visible on desktop (no hamburger menu)', async ({ page }) => {
    // Verify navigation element exists and is visible
    const nav = page.locator('nav.main-nav');
    await expect(nav).toBeVisible();

    // Verify nav list is displayed as flex (not collapsed)
    const navList = page.locator('.nav-list');
    await expect(navList).toBeVisible();

    const navDisplay = await navList.evaluate(el => window.getComputedStyle(el).display);
    expect(navDisplay).toBe('flex');

    // Verify all navigation links are visible
    const featuresLink = page.locator('.nav-link', { hasText: 'Features' });
    await expect(featuresLink).toBeVisible();

    const gettingStartedLink = page.locator('.nav-link', { hasText: 'Getting Started' });
    await expect(gettingStartedLink).toBeVisible();

    const documentationLink = page.locator('.nav-link', { hasText: 'Documentation' });
    await expect(documentationLink).toBeVisible();

    const githubLink = page.locator('.nav-link', { hasText: 'GitHub' });
    await expect(githubLink).toBeVisible();

    // Verify there's no hamburger menu button (typically hidden on desktop)
    const hamburgerMenu = page.locator('.hamburger-menu, .mobile-menu-toggle, [class*="hamburger"], [class*="mobile-toggle"]');
    const hamburgerCount = await hamburgerMenu.count();
    if (hamburgerCount > 0) {
      await expect(hamburgerMenu.first()).not.toBeVisible();
    }

    // Verify navigation links are horizontally aligned (inline layout)
    const navItems = page.locator('.nav-list li');
    const itemCount = await navItems.count();
    expect(itemCount).toBe(4);

    // Check that nav items are displayed horizontally
    const firstItemBox = await navItems.first().boundingBox();
    const lastItemBox = await navItems.last().boundingBox();
    expect(firstItemBox).not.toBeNull();
    expect(lastItemBox).not.toBeNull();

    // Items should be on roughly the same vertical line (horizontal layout)
    expect(Math.abs(firstItemBox.y - lastItemBox.y)).toBeLessThan(50);

    // Items should be spaced horizontally
    expect(lastItemBox.x).toBeGreaterThan(firstItemBox.x);
  });

  /**
   * Test Case 3: Features display in multi-column grid layout
   * Input: Verify feature grid on desktop
   * Expected: Features display in multi-column grid layout
   */
  test('TC3: Features display in multi-column grid layout on desktop', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Verify features grid exists
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Verify grid is using CSS grid or flexbox for multi-column layout
    const gridDisplay = await featuresGrid.evaluate(el => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns
      };
    });

    // Should be using CSS grid
    expect(gridDisplay.display).toBe('grid');

    // Verify feature cards exist
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(3); // At least 3 feature cards

    // Verify multi-column layout by checking card positions
    if (cardCount >= 2) {
      const firstCardBox = await featureCards.nth(0).boundingBox();
      const secondCardBox = await featureCards.nth(1).boundingBox();

      expect(firstCardBox).not.toBeNull();
      expect(secondCardBox).not.toBeNull();

      // On desktop 1920px, cards should be side by side (same Y position approximately)
      // or at least in a grid pattern
      const isSameRow = Math.abs(firstCardBox.y - secondCardBox.y) < 50;
      const isDifferentColumn = secondCardBox.x > firstCardBox.x;

      // Either cards are in the same row but different columns (multi-column)
      // or cards are properly laid out in a grid
      expect(isSameRow && isDifferentColumn).toBe(true);
    }

    // Verify at least 2 columns on desktop by checking if cards are arranged in multiple columns
    if (cardCount >= 3) {
      const firstCardBox = await featureCards.nth(0).boundingBox();
      const thirdCardBox = await featureCards.nth(2).boundingBox();

      // Check the third card is either in the same row (3+ columns) or a new row
      // but should not be in a single column layout
      const firstRowY = firstCardBox.y;
      const thirdCardY = thirdCardBox.y;

      // If same row, cards are in 3+ columns
      // If different row, verify there's proper grid spacing
      if (Math.abs(firstRowY - thirdCardY) < 50) {
        // Same row - at least 3 columns
        expect(thirdCardBox.x).toBeGreaterThan(firstCardBox.x);
      } else {
        // Different row - should still have multiple columns
        // Third card should not be at the same X position as first
        // (which would indicate single column)
        const secondCardBox = await featureCards.nth(1).boundingBox();
        const isDifferentXFromFirst = Math.abs(secondCardBox.x - firstCardBox.x) > 100;
        expect(isDifferentXFromFirst).toBe(true);
      }
    }
  });

  /**
   * Test Case 4: Code blocks display correctly on desktop
   * Input: Check code blocks on desktop
   * Expected: Code blocks display full width with readable font size, no overflow
   */
  test('TC4: Code blocks display full width with readable font size, no overflow', async ({ page }) => {
    // Navigate to getting started section where code blocks are
    const gettingStartedSection = page.locator('#getting-started');
    await gettingStartedSection.scrollIntoViewIfNeeded();
    await expect(gettingStartedSection).toBeVisible();

    // Find all code blocks (pre elements)
    const codeBlocks = page.locator('pre');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Check each code block
    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      await expect(codeBlock).toBeVisible();

      // Get computed styles
      const styles = await codeBlock.evaluate(el => {
        const computed = window.getComputedStyle(el);
        return {
          fontSize: computed.fontSize,
          overflowX: computed.overflowX,
          width: el.offsetWidth,
          scrollWidth: el.scrollWidth
        };
      });

      // Verify readable font size (at least 12px, typically 14px for code)
      const fontSize = parseFloat(styles.fontSize);
      expect(fontSize).toBeGreaterThanOrEqual(12);
      expect(fontSize).toBeLessThanOrEqual(20); // Not too large

      // Verify overflow is handled properly (either auto/scroll or content fits)
      const hasOverflow = styles.scrollWidth > styles.width + 5; // 5px tolerance
      if (hasOverflow) {
        // If content overflows, overflow-x should be auto or scroll
        expect(['auto', 'scroll']).toContain(styles.overflowX);
      }

      // Verify code block has reasonable width on desktop
      expect(styles.width).toBeGreaterThan(300); // At least 300px wide
    }

    // Specifically check the getting started code blocks
    const stepCodeBlocks = page.locator('.step pre');
    const stepCodeCount = await stepCodeBlocks.count();
    expect(stepCodeCount).toBeGreaterThan(0);

    // Verify code content is readable (has content and proper formatting)
    const firstCodeBlock = stepCodeBlocks.first();
    const codeContent = await firstCodeBlock.locator('code').textContent();
    expect(codeContent).toBeTruthy();
    expect(codeContent.length).toBeGreaterThan(0);
  });

  /**
   * Additional test: Verify overall layout and spacing on desktop
   */
  test('Desktop layout has proper spacing and alignment', async ({ page }) => {
    // Check header container has proper max-width
    const headerContainer = page.locator('.header-container');
    await expect(headerContainer).toBeVisible();

    const headerContainerBox = await headerContainer.boundingBox();
    expect(headerContainerBox).not.toBeNull();
    expect(headerContainerBox.width).toBeLessThanOrEqual(1200 + 100); // max-width + padding

    // Check section containers have proper max-width
    const sectionContainers = page.locator('.section-container');
    const sectionCount = await sectionContainers.count();

    for (let i = 0; i < sectionCount; i++) {
      const container = sectionContainers.nth(i);
      const containerBox = await container.boundingBox();
      if (containerBox) {
        expect(containerBox.width).toBeLessThanOrEqual(1200 + 100); // max-width + padding
      }
    }

    // Verify footer container has proper max-width
    const footerContainer = page.locator('.footer-container');
    await expect(footerContainer).toBeVisible();

    const footerContainerBox = await footerContainer.boundingBox();
    expect(footerContainerBox).not.toBeNull();
    expect(footerContainerBox.width).toBeLessThanOrEqual(1200 + 100);

    // Verify header layout is horizontal (logo on left, nav on right)
    const logoSection = page.locator('.logo-section');
    const mainNav = page.locator('.main-nav');

    const logoBox = await logoSection.boundingBox();
    const navBox = await mainNav.boundingBox();

    expect(logoBox).not.toBeNull();
    expect(navBox).not.toBeNull();

    // Nav should be to the right of logo on desktop
    expect(navBox.x).toBeGreaterThan(logoBox.x);
  });

  /**
   * Additional test: Verify hero section displays correctly on desktop
   */
  test('Hero section displays properly on desktop', async ({ page }) => {
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Check hero heading font size is large on desktop
    const heroHeading = page.locator('.hero-section h1');
    await expect(heroHeading).toBeVisible();

    const headingFontSize = await heroHeading.evaluate(el =>
      parseFloat(window.getComputedStyle(el).fontSize)
    );
    expect(headingFontSize).toBeGreaterThanOrEqual(40); // Should be at least 40px on desktop (3rem = 48px)

    // Check CTA buttons are horizontally aligned
    const ctaContainer = page.locator('.hero-cta');
    await expect(ctaContainer).toBeVisible();

    const buttons = page.locator('.hero-cta .btn');
    const buttonCount = await buttons.count();
    expect(buttonCount).toBe(2);

    const firstButtonBox = await buttons.nth(0).boundingBox();
    const secondButtonBox = await buttons.nth(1).boundingBox();

    expect(firstButtonBox).not.toBeNull();
    expect(secondButtonBox).not.toBeNull();

    // Buttons should be horizontally aligned on desktop
    expect(Math.abs(firstButtonBox.y - secondButtonBox.y)).toBeLessThan(10);
    expect(secondButtonBox.x).toBeGreaterThan(firstButtonBox.x);
  });
});
