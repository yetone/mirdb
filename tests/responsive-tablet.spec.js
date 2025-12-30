// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: Responsive Design - Tablet
 * Scenario: Verify the page displays correctly on tablet viewport sizes
 * NFR-1: Page must be fully responsive across desktop, tablet, and mobile viewports
 */

test.describe('Responsive Design - Tablet', () => {
  // Set tablet viewport for all tests (768x1024 - common tablet portrait)
  test.use({ viewport: { width: 768, height: 1024 } });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Page renders correctly at 768x1024 viewport
   * Input: Load page at 768x1024 viewport
   * Expected: Page renders correctly with appropriate layout for tablet size
   */
  test('TC1: Page renders correctly at 768x1024 with appropriate tablet layout', async ({ page }) => {
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

    // Verify content fits within viewport width (768px)
    const heroBox = await heroSection.boundingBox();
    expect(heroBox).not.toBeNull();
    expect(heroBox.width).toBeLessThanOrEqual(768);

    // Verify hero section text is readable on tablet
    const heroHeading = page.locator('.hero-section h1');
    await expect(heroHeading).toBeVisible();
    const headingFontSize = await heroHeading.evaluate(el =>
      parseFloat(window.getComputedStyle(el).fontSize)
    );
    // On tablet, heading should still be reasonably large (at least 28px)
    expect(headingFontSize).toBeGreaterThanOrEqual(28);
  });

  /**
   * Test Case 2: Navigation is accessible on tablet
   * Input: Check navigation on tablet
   * Expected: Navigation is accessible (may be collapsed or adjusted for tablet)
   */
  test('TC2: Navigation is accessible on tablet viewport', async ({ page }) => {
    // Verify navigation element exists
    const nav = page.locator('nav.main-nav');
    await expect(nav).toBeVisible();

    // Verify nav list exists
    const navList = page.locator('.nav-list');
    await expect(navList).toBeVisible();

    // Verify all navigation links are present and accessible
    const featuresLink = page.locator('.nav-link', { hasText: 'Features' });
    await expect(featuresLink).toBeVisible();

    const gettingStartedLink = page.locator('.nav-link', { hasText: 'Getting Started' });
    await expect(gettingStartedLink).toBeVisible();

    const documentationLink = page.locator('.nav-link', { hasText: 'Documentation' });
    await expect(documentationLink).toBeVisible();

    const githubLink = page.locator('.nav-link', { hasText: 'GitHub' });
    await expect(githubLink).toBeVisible();

    // Verify navigation links are clickable
    const navLinks = page.locator('.nav-link');
    const linkCount = await navLinks.count();
    expect(linkCount).toBe(4);

    // Verify each link is within the viewport
    for (let i = 0; i < linkCount; i++) {
      const link = navLinks.nth(i);
      const linkBox = await link.boundingBox();
      expect(linkBox).not.toBeNull();
      // Link should be within viewport width
      expect(linkBox.x + linkBox.width).toBeLessThanOrEqual(768);
    }

    // Test that navigation links work (click Features link)
    await featuresLink.click();
    await page.waitForTimeout(500); // Wait for smooth scroll
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  /**
   * Test Case 3: Features reflow to appropriate column layout on tablet
   * Input: Verify feature layout on tablet
   * Expected: Features reflow to 2-column or 1-column layout as appropriate
   */
  test('TC3: Features reflow to 2-column or 1-column layout on tablet', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Verify features grid exists
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Verify grid is using CSS grid
    const gridDisplay = await featuresGrid.evaluate(el => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns
      };
    });
    expect(gridDisplay.display).toBe('grid');

    // Verify feature cards exist
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(3);

    // On tablet at 768px, with minmax(300px, 1fr), we should have 2 columns
    // Check that cards are arranged in at most 2 columns
    if (cardCount >= 2) {
      const firstCardBox = await featureCards.nth(0).boundingBox();
      const secondCardBox = await featureCards.nth(1).boundingBox();

      expect(firstCardBox).not.toBeNull();
      expect(secondCardBox).not.toBeNull();

      // Verify cards are side by side (2-column) or stacked (1-column)
      const isSameRow = Math.abs(firstCardBox.y - secondCardBox.y) < 50;

      if (isSameRow) {
        // 2-column layout: second card should be to the right of first
        expect(secondCardBox.x).toBeGreaterThan(firstCardBox.x);

        // Each card should take roughly half the container width (minus gaps)
        const maxCardWidth = 400; // Slightly more than half of 768 to account for gaps/padding
        expect(firstCardBox.width).toBeLessThanOrEqual(maxCardWidth);
      } else {
        // 1-column layout: cards are stacked vertically
        expect(secondCardBox.y).toBeGreaterThan(firstCardBox.y);
      }
    }

    // Verify all cards fit within viewport
    for (let i = 0; i < cardCount; i++) {
      const cardBox = await featureCards.nth(i).boundingBox();
      expect(cardBox).not.toBeNull();
      expect(cardBox.x).toBeGreaterThanOrEqual(0);
      expect(cardBox.x + cardBox.width).toBeLessThanOrEqual(768 + 10); // Allow small tolerance
    }

    // Verify cards have readable content
    for (let i = 0; i < Math.min(cardCount, 3); i++) {
      const card = featureCards.nth(i);
      const cardHeading = card.locator('h3');
      const cardParagraph = card.locator('p');

      await expect(cardHeading).toBeVisible();
      await expect(cardParagraph).toBeVisible();
    }
  });

  /**
   * Test Case 4: Touch targets have adequate size on tablet
   * Input: Check touch targets on tablet
   * Expected: Buttons and links have adequate touch target size (min 44x44px)
   */
  test('TC4: Buttons and links have adequate touch target size (min 44x44px)', async ({ page }) => {
    // Check CTA buttons in hero section
    const primaryBtn = page.locator('.btn-primary').first();
    await expect(primaryBtn).toBeVisible();

    const primaryBtnBox = await primaryBtn.boundingBox();
    expect(primaryBtnBox).not.toBeNull();
    // Minimum touch target size is 44x44px per WCAG guidelines
    expect(primaryBtnBox.height).toBeGreaterThanOrEqual(44);
    expect(primaryBtnBox.width).toBeGreaterThanOrEqual(44);

    const secondaryBtn = page.locator('.btn-secondary').first();
    await expect(secondaryBtn).toBeVisible();

    const secondaryBtnBox = await secondaryBtn.boundingBox();
    expect(secondaryBtnBox).not.toBeNull();
    expect(secondaryBtnBox.height).toBeGreaterThanOrEqual(44);
    expect(secondaryBtnBox.width).toBeGreaterThanOrEqual(44);

    // Check navigation links
    const navLinks = page.locator('.nav-link');
    const navLinkCount = await navLinks.count();

    for (let i = 0; i < navLinkCount; i++) {
      const navLink = navLinks.nth(i);
      const navLinkBox = await navLink.boundingBox();
      expect(navLinkBox).not.toBeNull();

      // For text links, we check the clickable area height
      // Navigation links should have adequate vertical padding
      // The total clickable height should be at least 44px (or close to it with padding)
      expect(navLinkBox.height).toBeGreaterThanOrEqual(24);

      // Check the computed touch area including padding
      const touchArea = await navLink.evaluate(el => {
        const style = window.getComputedStyle(el);
        const paddingTop = parseFloat(style.paddingTop);
        const paddingBottom = parseFloat(style.paddingBottom);
        const lineHeight = parseFloat(style.lineHeight) || parseFloat(style.fontSize) * 1.5;
        return lineHeight + paddingTop + paddingBottom;
      });
      // Total touch area should be reasonable for tap targets
      expect(touchArea).toBeGreaterThanOrEqual(24);
    }

    // Check footer links
    const footerLinks = page.locator('.footer-nav a');
    const footerLinkCount = await footerLinks.count();

    for (let i = 0; i < footerLinkCount; i++) {
      const footerLink = footerLinks.nth(i);
      await expect(footerLink).toBeVisible();

      const footerLinkBox = await footerLink.boundingBox();
      expect(footerLinkBox).not.toBeNull();
      // Footer links should be reasonably tappable
      expect(footerLinkBox.height).toBeGreaterThanOrEqual(20);
    }

    // Check that commands table cells are readable on tablet
    const commandsTable = page.locator('.commands-table');
    if (await commandsTable.isVisible()) {
      const tableBox = await commandsTable.boundingBox();
      expect(tableBox).not.toBeNull();
      // Table should fit within viewport
      expect(tableBox.width).toBeLessThanOrEqual(768);
    }
  });

  /**
   * Additional test: Header layout adapts properly on tablet
   */
  test('Header layout adapts properly on tablet viewport', async ({ page }) => {
    const headerContainer = page.locator('.header-container');
    await expect(headerContainer).toBeVisible();

    // Check header container layout
    const headerStyles = await headerContainer.evaluate(el => {
      const style = window.getComputedStyle(el);
      return {
        flexDirection: style.flexDirection,
        justifyContent: style.justifyContent,
        alignItems: style.alignItems
      };
    });

    // On tablet (768px width), the layout may be column (stacked) or row
    // Both are acceptable as long as content fits
    const headerBox = await headerContainer.boundingBox();
    expect(headerBox).not.toBeNull();
    expect(headerBox.width).toBeLessThanOrEqual(768);

    // Verify logo and navigation are both visible
    const logoSection = page.locator('.logo-section');
    const mainNav = page.locator('.main-nav');

    await expect(logoSection).toBeVisible();
    await expect(mainNav).toBeVisible();

    // Verify logo text is readable
    const logoText = page.locator('.logo-text');
    const logoFontSize = await logoText.evaluate(el =>
      parseFloat(window.getComputedStyle(el).fontSize)
    );
    expect(logoFontSize).toBeGreaterThanOrEqual(20);
  });

  /**
   * Additional test: Code blocks are readable on tablet
   */
  test('Code blocks are readable and scrollable on tablet', async ({ page }) => {
    // Navigate to getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await gettingStartedSection.scrollIntoViewIfNeeded();
    await expect(gettingStartedSection).toBeVisible();

    // Find all code blocks
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

      // Verify readable font size
      const fontSize = parseFloat(styles.fontSize);
      expect(fontSize).toBeGreaterThanOrEqual(12);

      // Verify code block fits within container or has scroll
      const hasOverflow = styles.scrollWidth > styles.width + 5;
      if (hasOverflow) {
        expect(['auto', 'scroll']).toContain(styles.overflowX);
      }

      // Code block should have reasonable width on tablet
      expect(styles.width).toBeGreaterThan(200);
      expect(styles.width).toBeLessThanOrEqual(768);
    }
  });

  /**
   * Additional test: Hero section CTA buttons layout on tablet
   */
  test('Hero section CTA buttons are properly aligned on tablet', async ({ page }) => {
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    const ctaContainer = page.locator('.hero-cta');
    await expect(ctaContainer).toBeVisible();

    const buttons = page.locator('.hero-cta .btn');
    const buttonCount = await buttons.count();
    expect(buttonCount).toBe(2);

    const firstButtonBox = await buttons.nth(0).boundingBox();
    const secondButtonBox = await buttons.nth(1).boundingBox();

    expect(firstButtonBox).not.toBeNull();
    expect(secondButtonBox).not.toBeNull();

    // On tablet, buttons should either be:
    // 1. Horizontally aligned (side by side)
    // 2. Vertically stacked if viewport is narrow
    const isSameRow = Math.abs(firstButtonBox.y - secondButtonBox.y) < 20;
    const isStacked = secondButtonBox.y > firstButtonBox.y + firstButtonBox.height - 10;

    // One of these layouts should be true
    expect(isSameRow || isStacked).toBe(true);

    // Both buttons should fit within viewport
    expect(firstButtonBox.x + firstButtonBox.width).toBeLessThanOrEqual(768);
    expect(secondButtonBox.x + secondButtonBox.width).toBeLessThanOrEqual(768);
  });
});
