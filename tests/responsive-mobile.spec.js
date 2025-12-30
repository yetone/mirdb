// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: Responsive Design - Mobile
 * Scenario: Verify the page displays correctly on mobile viewport sizes
 * NFR-1: Page must be fully responsive across desktop, tablet, and mobile viewports
 */

test.describe('Responsive Design - Mobile', () => {
  // Set mobile viewport for all tests (375x667 - iPhone SE/standard mobile portrait)
  test.use({ viewport: { width: 375, height: 667 } });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Page renders correctly at 375x667 viewport
   * Input: Load page at 375x667 viewport
   * Expected: Page renders correctly with single-column layout, no horizontal scroll
   */
  test('TC1: Page renders correctly at 375x667 with single-column layout, no horizontal scroll', async ({ page }) => {
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

    // Verify content fits within viewport width (375px)
    const heroBox = await heroSection.boundingBox();
    expect(heroBox).not.toBeNull();
    expect(heroBox.width).toBeLessThanOrEqual(375);

    // Verify feature cards stack in single-column layout on mobile
    const featuresGrid = page.locator('.features-grid');
    await featuresGrid.scrollIntoViewIfNeeded();
    await expect(featuresGrid).toBeVisible();

    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(2);

    // Check that cards are stacked vertically (single column)
    if (cardCount >= 2) {
      const firstCardBox = await featureCards.nth(0).boundingBox();
      const secondCardBox = await featureCards.nth(1).boundingBox();

      expect(firstCardBox).not.toBeNull();
      expect(secondCardBox).not.toBeNull();

      // Cards should be stacked vertically (second card below first)
      expect(secondCardBox.y).toBeGreaterThan(firstCardBox.y + firstCardBox.height - 20);

      // Cards should be full width or nearly full width (single column)
      expect(firstCardBox.width).toBeGreaterThan(300);
    }
  });

  /**
   * Test Case 2: Navigation is accessible on mobile
   * Input: Check navigation on mobile
   * Expected: Navigation is accessible via hamburger menu or similar mobile pattern
   */
  test('TC2: Navigation is accessible via hamburger menu or similar mobile pattern', async ({ page }) => {
    // Verify navigation element exists
    const nav = page.locator('nav.main-nav');
    await expect(nav).toBeVisible();

    // Verify nav list exists and navigation links are accessible
    const navList = page.locator('.nav-list');
    await expect(navList).toBeVisible();

    // On mobile, navigation may be wrapped or displayed as hamburger menu
    // Check if nav links are accessible (visible or can be made visible)
    const featuresLink = page.locator('.nav-link', { hasText: 'Features' });
    const gettingStartedLink = page.locator('.nav-link', { hasText: 'Getting Started' });
    const documentationLink = page.locator('.nav-link', { hasText: 'Documentation' });
    const githubLink = page.locator('.nav-link', { hasText: 'GitHub' });

    // Check for hamburger menu first
    const hamburgerMenu = page.locator('.hamburger-menu, .mobile-menu-toggle, [class*="hamburger"], [class*="mobile-toggle"], .menu-toggle');
    const hamburgerCount = await hamburgerMenu.count();

    if (hamburgerCount > 0 && await hamburgerMenu.first().isVisible()) {
      // If hamburger menu is present and visible, click it to reveal nav
      await hamburgerMenu.first().click();
      await page.waitForTimeout(300); // Wait for animation
    }

    // All navigation links should be accessible (visible after any menu expansion)
    // The current implementation uses flex-wrap to show all links on mobile
    await expect(featuresLink).toBeVisible();
    await expect(gettingStartedLink).toBeVisible();
    await expect(documentationLink).toBeVisible();
    await expect(githubLink).toBeVisible();

    // Verify navigation links work (click Features link)
    await featuresLink.click();
    await page.waitForTimeout(500); // Wait for smooth scroll
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();

    // Verify each link is within viewport width
    const navLinks = page.locator('.nav-link');
    const linkCount = await navLinks.count();
    expect(linkCount).toBe(4);

    for (let i = 0; i < linkCount; i++) {
      const link = navLinks.nth(i);
      const linkBox = await link.boundingBox();
      expect(linkBox).not.toBeNull();
      // Link should be within viewport width
      expect(linkBox.x + linkBox.width).toBeLessThanOrEqual(375 + 10); // Small tolerance
    }
  });

  /**
   * Test Case 3: Text is readable on mobile
   * Input: Verify text readability on mobile
   * Expected: Font sizes are appropriate for mobile reading (min 16px body text)
   */
  test('TC3: Font sizes are appropriate for mobile reading (min 16px body text)', async ({ page }) => {
    // Check body text font size
    const bodyFontSize = await page.evaluate(() => {
      return parseFloat(window.getComputedStyle(document.body).fontSize);
    });
    // Default body font should be at least 16px for mobile readability
    expect(bodyFontSize).toBeGreaterThanOrEqual(16);

    // Check hero description text
    const heroDescription = page.locator('.hero-description');
    await expect(heroDescription).toBeVisible();
    const heroDescFontSize = await heroDescription.evaluate(el =>
      parseFloat(window.getComputedStyle(el).fontSize)
    );
    // Hero description should be at least 16px for mobile
    expect(heroDescFontSize).toBeGreaterThanOrEqual(16);

    // Check feature card paragraph text
    const featureCardText = page.locator('.feature-card p').first();
    await featureCardText.scrollIntoViewIfNeeded();
    await expect(featureCardText).toBeVisible();
    const featureTextFontSize = await featureCardText.evaluate(el =>
      parseFloat(window.getComputedStyle(el).fontSize)
    );
    // Feature card text should be at least 14px (acceptable for secondary text)
    expect(featureTextFontSize).toBeGreaterThanOrEqual(14);

    // Check hero heading is appropriately sized for mobile
    const heroHeading = page.locator('.hero-section h1');
    await heroHeading.scrollIntoViewIfNeeded();
    await expect(heroHeading).toBeVisible();
    const headingFontSize = await heroHeading.evaluate(el =>
      parseFloat(window.getComputedStyle(el).fontSize)
    );
    // On mobile, heading should be at least 28px (responsive from 3rem to 2rem)
    expect(headingFontSize).toBeGreaterThanOrEqual(28);
    // But not too large for mobile screen
    expect(headingFontSize).toBeLessThanOrEqual(48);

    // Check section headings
    const sectionHeading = page.locator('section h2').first();
    await sectionHeading.scrollIntoViewIfNeeded();
    await expect(sectionHeading).toBeVisible();
    const sectionHeadingFontSize = await sectionHeading.evaluate(el =>
      parseFloat(window.getComputedStyle(el).fontSize)
    );
    // Section headings should be readable (responsive from 2.25rem to 1.75rem = 28px)
    expect(sectionHeadingFontSize).toBeGreaterThanOrEqual(24);

    // Check table text is readable
    const tableCell = page.locator('.commands-table td').first();
    await tableCell.scrollIntoViewIfNeeded();
    await expect(tableCell).toBeVisible();
    const tableFontSize = await tableCell.evaluate(el =>
      parseFloat(window.getComputedStyle(el).fontSize)
    );
    // Table text should be at least 14px
    expect(tableFontSize).toBeGreaterThanOrEqual(14);
  });

  /**
   * Test Case 4: Code blocks are readable on mobile
   * Input: Check code blocks on mobile
   * Expected: Code blocks are scrollable horizontally if needed, text is readable
   */
  test('TC4: Code blocks are scrollable horizontally if needed, text is readable', async ({ page }) => {
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
      await codeBlock.scrollIntoViewIfNeeded();
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

      // Verify readable font size (at least 12px for code)
      const fontSize = parseFloat(styles.fontSize);
      expect(fontSize).toBeGreaterThanOrEqual(12);
      expect(fontSize).toBeLessThanOrEqual(18); // Not too large for mobile

      // Verify overflow is handled properly with horizontal scroll
      const hasOverflow = styles.scrollWidth > styles.width + 5; // 5px tolerance
      if (hasOverflow) {
        // If content overflows, overflow-x should be auto or scroll
        expect(['auto', 'scroll']).toContain(styles.overflowX);
      }

      // Verify code block fits within viewport width
      const codeBlockBox = await codeBlock.boundingBox();
      expect(codeBlockBox).not.toBeNull();
      expect(codeBlockBox.width).toBeLessThanOrEqual(375);
    }

    // Verify code content is readable (has content and proper formatting)
    const firstCodeBlock = codeBlocks.first();
    const codeContent = await firstCodeBlock.locator('code').textContent();
    expect(codeContent).toBeTruthy();
    expect(codeContent.length).toBeGreaterThan(0);

    // Test that code block can be scrolled if content overflows
    const lastCodeBlock = codeBlocks.last();
    await lastCodeBlock.scrollIntoViewIfNeeded();
    const lastCodeStyles = await lastCodeBlock.evaluate(el => {
      return {
        scrollWidth: el.scrollWidth,
        clientWidth: el.clientWidth,
        overflowX: window.getComputedStyle(el).overflowX
      };
    });

    if (lastCodeStyles.scrollWidth > lastCodeStyles.clientWidth) {
      // Content overflows, verify scroll is possible
      expect(['auto', 'scroll']).toContain(lastCodeStyles.overflowX);
    }
  });

  /**
   * Test Case 5: CTA buttons are appropriately sized for mobile
   * Input: Verify CTA buttons on mobile
   * Expected: CTA buttons are full-width or appropriately sized for thumb tapping
   */
  test('TC5: CTA buttons are full-width or appropriately sized for thumb tapping', async ({ page }) => {
    // Check CTA buttons in hero section
    const primaryBtn = page.locator('.btn-primary').first();
    await expect(primaryBtn).toBeVisible();

    const primaryBtnBox = await primaryBtn.boundingBox();
    expect(primaryBtnBox).not.toBeNull();

    // Minimum touch target size is 44x44px per WCAG guidelines
    expect(primaryBtnBox.height).toBeGreaterThanOrEqual(44);
    expect(primaryBtnBox.width).toBeGreaterThanOrEqual(44);

    // On mobile, buttons should be reasonably wide for easy tapping
    // Either full-width (close to 375px minus padding) or at least 120px wide
    expect(primaryBtnBox.width).toBeGreaterThanOrEqual(100);

    const secondaryBtn = page.locator('.btn-secondary').first();
    await expect(secondaryBtn).toBeVisible();

    const secondaryBtnBox = await secondaryBtn.boundingBox();
    expect(secondaryBtnBox).not.toBeNull();
    expect(secondaryBtnBox.height).toBeGreaterThanOrEqual(44);
    expect(secondaryBtnBox.width).toBeGreaterThanOrEqual(100);

    // Check that buttons fit within viewport
    expect(primaryBtnBox.x + primaryBtnBox.width).toBeLessThanOrEqual(375);
    expect(secondaryBtnBox.x + secondaryBtnBox.width).toBeLessThanOrEqual(375);

    // Check hero CTA container layout
    const ctaContainer = page.locator('.hero-cta');
    await expect(ctaContainer).toBeVisible();

    const buttons = page.locator('.hero-cta .btn');
    const buttonCount = await buttons.count();
    expect(buttonCount).toBe(2);

    const firstButtonBox = await buttons.nth(0).boundingBox();
    const secondButtonBox = await buttons.nth(1).boundingBox();

    expect(firstButtonBox).not.toBeNull();
    expect(secondButtonBox).not.toBeNull();

    // On mobile, buttons may be either:
    // 1. Horizontally aligned (side by side with flex-wrap)
    // 2. Vertically stacked
    const isSameRow = Math.abs(firstButtonBox.y - secondButtonBox.y) < 20;
    const isStacked = secondButtonBox.y > firstButtonBox.y + firstButtonBox.height - 10;

    // One of these layouts should be true
    expect(isSameRow || isStacked).toBe(true);

    // Verify adequate spacing between buttons
    if (isStacked) {
      const gap = secondButtonBox.y - (firstButtonBox.y + firstButtonBox.height);
      expect(gap).toBeGreaterThanOrEqual(8); // At least 8px gap
    }
  });

  /**
   * Additional test: Header layout adapts properly on mobile
   */
  test('Header layout stacks vertically on mobile', async ({ page }) => {
    const headerContainer = page.locator('.header-container');
    await expect(headerContainer).toBeVisible();

    // On mobile (375px width), header should use column layout
    const headerStyles = await headerContainer.evaluate(el => {
      const style = window.getComputedStyle(el);
      return {
        flexDirection: style.flexDirection,
        textAlign: style.textAlign
      };
    });

    // Header should use column layout on mobile (center-aligned)
    expect(headerStyles.flexDirection).toBe('column');

    // Verify header fits within viewport
    const headerBox = await headerContainer.boundingBox();
    expect(headerBox).not.toBeNull();
    expect(headerBox.width).toBeLessThanOrEqual(375);

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

    // On mobile, logo section should be centered
    const logoSectionStyles = await logoSection.evaluate(el => {
      const style = window.getComputedStyle(el);
      return {
        alignItems: style.alignItems
      };
    });
    expect(logoSectionStyles.alignItems).toBe('center');
  });

  /**
   * Additional test: Footer adapts to mobile layout
   */
  test('Footer adapts to mobile layout', async ({ page }) => {
    const footer = page.locator('footer.footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    const footerContainer = page.locator('.footer-container');
    await expect(footerContainer).toBeVisible();

    // On mobile, footer container should stack vertically
    const footerStyles = await footerContainer.evaluate(el => {
      const style = window.getComputedStyle(el);
      return {
        flexDirection: style.flexDirection,
        textAlign: style.textAlign
      };
    });
    expect(footerStyles.flexDirection).toBe('column');

    // Verify footer links are accessible
    const footerLinks = page.locator('.footer-nav a');
    const footerLinkCount = await footerLinks.count();
    expect(footerLinkCount).toBeGreaterThan(0);

    for (let i = 0; i < footerLinkCount; i++) {
      const footerLink = footerLinks.nth(i);
      await expect(footerLink).toBeVisible();

      const footerLinkBox = await footerLink.boundingBox();
      expect(footerLinkBox).not.toBeNull();
      // Footer links should be tappable
      expect(footerLinkBox.height).toBeGreaterThanOrEqual(20);
    }

    // Verify footer fits within viewport
    const footerBox = await footerContainer.boundingBox();
    expect(footerBox).not.toBeNull();
    expect(footerBox.width).toBeLessThanOrEqual(375);
  });

  /**
   * Additional test: Tables are scrollable or fit on mobile
   */
  test('Commands table is readable and fits or scrolls on mobile', async ({ page }) => {
    // Navigate to documentation section where the table is
    const documentationSection = page.locator('#documentation');
    await documentationSection.scrollIntoViewIfNeeded();
    await expect(documentationSection).toBeVisible();

    const commandsTable = page.locator('.commands-table');
    await expect(commandsTable).toBeVisible();

    // Get table dimensions
    const tableStyles = await commandsTable.evaluate(el => {
      const parent = el.parentElement;
      return {
        tableWidth: el.offsetWidth,
        parentWidth: parent ? parent.offsetWidth : el.offsetWidth,
        overflowX: parent ? window.getComputedStyle(parent).overflowX : 'visible'
      };
    });

    // Table should either fit within viewport or have scroll capability
    if (tableStyles.tableWidth > 375) {
      // If table is wider than viewport, parent should allow scrolling
      expect(['auto', 'scroll', 'visible']).toContain(tableStyles.overflowX);
    }

    // Verify table headers are readable
    const tableHeaders = page.locator('.commands-table th');
    const headerCount = await tableHeaders.count();
    expect(headerCount).toBe(2);

    for (let i = 0; i < headerCount; i++) {
      const header = tableHeaders.nth(i);
      await expect(header).toBeVisible();
    }

    // Verify table cells have readable font size
    const tableCells = page.locator('.commands-table td');
    const cellCount = await tableCells.count();
    expect(cellCount).toBeGreaterThan(0);

    const firstCell = tableCells.first();
    const cellFontSize = await firstCell.evaluate(el =>
      parseFloat(window.getComputedStyle(el).fontSize)
    );
    expect(cellFontSize).toBeGreaterThanOrEqual(12);
  });
});
