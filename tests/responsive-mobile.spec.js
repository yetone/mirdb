// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = path.resolve(__dirname, '../index.html');

/**
 * Mobile Responsive Design Tests
 * Verify the landing page displays correctly on mobile devices (width < 768px)
 */
test.describe('Responsive Design - Mobile', () => {
  // Set viewport to mobile width for all tests in this suite
  test.use({ viewport: { width: 375, height: 667 } });

  test.beforeEach(async ({ page }) => {
    await page.goto(`file://${indexPath}`);
  });

  /**
   * Test Case 1: Load page at 375px width
   * Input: Load page at 375px width
   * Expected: All content visible without horizontal scroll
   */
  test('TC1: All content visible without horizontal scroll at 375px', async ({ page }) => {
    // Verify body does not have horizontal overflow
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // Body should not be wider than viewport (no horizontal scroll)
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth);

    // Verify hero section is visible
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify hero title is visible
    const heroTitle = page.locator('[data-testid="hero-title"]');
    await expect(heroTitle).toBeVisible();

    // Verify hero tagline is visible
    const heroTagline = page.locator('[data-testid="hero-tagline"]');
    await expect(heroTagline).toBeVisible();

    // Verify CTA buttons are visible
    const ctaGetStarted = page.locator('[data-testid="cta-get-started"]');
    const ctaGithub = page.locator('[data-testid="cta-github"]');
    await expect(ctaGetStarted).toBeVisible();
    await expect(ctaGithub).toBeVisible();

    // Verify features section is visible when scrolled to
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Verify quick start section is visible when scrolled to
    const quickstartSection = page.locator('#quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();
    await expect(quickstartSection).toBeVisible();
  });

  /**
   * Test Case 2: Check navigation at mobile width
   * Input: Check navigation at mobile width
   * Expected: Navigation adapts to mobile-friendly format (hamburger menu or collapsible)
   */
  test('TC2: Navigation adapts to mobile-friendly format', async ({ page }) => {
    // Verify navbar exists
    const navbar = page.locator('[data-testid="navbar"]');
    await expect(navbar).toBeVisible();

    // Verify brand logo is still visible on mobile
    const navBrand = page.locator('[data-testid="nav-brand"]');
    await expect(navBrand).toBeVisible();

    // Verify hamburger menu button is visible on mobile
    const hamburgerMenu = page.locator('[data-testid="hamburger-menu"]');
    await expect(hamburgerMenu).toBeVisible();

    // Verify hamburger button has correct aria attributes for accessibility
    await expect(hamburgerMenu).toHaveAttribute('aria-expanded', 'false');
    await expect(hamburgerMenu).toHaveAttribute('aria-label', 'Toggle navigation menu');

    // Verify nav links are hidden by default on mobile
    const navLinks = page.locator('[data-testid="nav-links"]');
    await expect(navLinks).not.toBeVisible();

    // Click hamburger menu to open navigation
    await hamburgerMenu.click();

    // Verify nav links are now visible
    await expect(navLinks).toBeVisible();

    // Verify hamburger button aria-expanded is now true
    await expect(hamburgerMenu).toHaveAttribute('aria-expanded', 'true');

    // Verify all navigation links are accessible
    await expect(page.locator('[data-testid="nav-features"]')).toBeVisible();
    await expect(page.locator('[data-testid="nav-quickstart"]')).toBeVisible();
    await expect(page.locator('[data-testid="nav-commands"]')).toBeVisible();
    await expect(page.locator('[data-testid="nav-configuration"]')).toBeVisible();
    await expect(page.locator('[data-testid="nav-github"]')).toBeVisible();

    // Click hamburger again to close
    await hamburgerMenu.click();
    await expect(navLinks).not.toBeVisible();
    await expect(hamburgerMenu).toHaveAttribute('aria-expanded', 'false');
  });

  /**
   * Test Case 3: Check feature cards at mobile width
   * Input: Check feature cards at mobile width
   * Expected: Feature cards stack vertically in single column
   */
  test('TC3: Feature cards stack vertically in single column', async ({ page }) => {
    // Scroll to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Get all feature cards
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBe(3);

    // Get the positions of each card
    const cardPositions = [];
    for (let i = 0; i < cardCount; i++) {
      const box = await featureCards.nth(i).boundingBox();
      expect(box).not.toBeNull();
      cardPositions.push(box);
    }

    // In single column layout, all cards should have similar x positions
    // and each card should be below the previous one (y increases)
    const tolerance = 10; // Allow small difference in x position

    // All cards should be roughly aligned horizontally (same x position)
    for (let i = 1; i < cardPositions.length; i++) {
      expect(Math.abs(cardPositions[i].x - cardPositions[0].x)).toBeLessThan(tolerance);
    }

    // Each subsequent card should be below the previous one
    for (let i = 1; i < cardPositions.length; i++) {
      expect(cardPositions[i].y).toBeGreaterThan(cardPositions[i - 1].y);
    }
  });

  /**
   * Test Case 4: Check code blocks at mobile width
   * Input: Check code blocks at mobile width
   * Expected: Code blocks are horizontally scrollable or wrap appropriately
   */
  test('TC4: Code blocks are horizontally scrollable', async ({ page }) => {
    // Scroll to quick start section
    const quickstartSection = page.locator('#quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    // Get code blocks
    const codeBlocks = page.locator('.code-block');
    const blockCount = await codeBlocks.count();
    expect(blockCount).toBeGreaterThan(0);

    // Check each code block for proper overflow handling
    for (let i = 0; i < blockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      await expect(codeBlock).toBeVisible();

      // Verify the code block container doesn't overflow the viewport
      const codeBlockBox = await codeBlock.boundingBox();
      const viewportWidth = await page.evaluate(() => window.innerWidth);

      // Code block should not extend beyond viewport
      expect(codeBlockBox.x + codeBlockBox.width).toBeLessThanOrEqual(viewportWidth + 20); // Small margin allowed

      // Verify pre element inside has overflow-x set for scrolling
      const preElement = codeBlock.locator('pre');
      const overflowX = await preElement.evaluate((el) => window.getComputedStyle(el).overflowX);
      expect(overflowX).toBe('auto');
    }
  });

  /**
   * Test Case 5: Verify CTA button touch targets
   * Input: Verify CTA button touch targets
   * Expected: CTA buttons have minimum 44px height for touch accessibility
   */
  test('TC5: CTA buttons have minimum 44px height for touch accessibility', async ({ page }) => {
    // Get all CTA buttons in hero section
    const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
    const githubBtn = page.locator('[data-testid="cta-github"]');

    // Check Get Started button dimensions
    const getStartedBox = await getStartedBtn.boundingBox();
    expect(getStartedBox).not.toBeNull();
    expect(getStartedBox.height).toBeGreaterThanOrEqual(44);

    // Check GitHub button dimensions
    const githubBox = await githubBtn.boundingBox();
    expect(githubBox).not.toBeNull();
    expect(githubBox.height).toBeGreaterThanOrEqual(44);

    // Also check copy buttons in code blocks
    const copyButtons = page.locator('.copy-btn');
    const copyBtnCount = await copyButtons.count();

    for (let i = 0; i < Math.min(copyBtnCount, 3); i++) {
      const copyBtn = copyButtons.nth(i);
      await copyBtn.scrollIntoViewIfNeeded();
      const copyBtnBox = await copyBtn.boundingBox();
      expect(copyBtnBox).not.toBeNull();
      // Copy buttons should have at least 44px touch target
      expect(copyBtnBox.height).toBeGreaterThanOrEqual(44);
    }
  });

  /**
   * Test Case 6: Test page at 320px width (minimum supported)
   * Input: Test page at 320px width (minimum supported)
   * Expected: Page remains functional at minimum 320px width
   */
  test('TC6: Page remains functional at minimum 320px width', async ({ page }) => {
    // Set viewport to 320px
    await page.setViewportSize({ width: 320, height: 568 });
    await page.reload();

    // Verify no horizontal scrollbar at 320px
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth);

    // Verify hero section renders correctly
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify hero title is visible and readable
    const heroTitle = page.locator('[data-testid="hero-title"]');
    await expect(heroTitle).toBeVisible();
    const titleText = await heroTitle.textContent();
    expect(titleText).toBe('MirDB');

    // Verify CTA buttons are accessible
    const ctaGetStarted = page.locator('[data-testid="cta-get-started"]');
    await expect(ctaGetStarted).toBeVisible();

    // Verify hamburger menu works at 320px
    const hamburgerMenu = page.locator('[data-testid="hamburger-menu"]');
    await expect(hamburgerMenu).toBeVisible();

    await hamburgerMenu.click();
    const navLinks = page.locator('[data-testid="nav-links"]');
    await expect(navLinks).toBeVisible();

    // Verify features section displays at 320px
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Verify feature cards are visible and stacked
    const featureCards = page.locator('.feature-card');
    const firstCard = featureCards.first();
    await expect(firstCard).toBeVisible();
  });

  /**
   * Additional test: Navigation link clicks work on mobile
   */
  test('Navigation links work correctly on mobile', async ({ page }) => {
    // Open hamburger menu
    const hamburgerMenu = page.locator('[data-testid="hamburger-menu"]');
    await hamburgerMenu.click();

    // Click Features link
    const featuresLink = page.locator('[data-testid="nav-features"]');
    await featuresLink.click();

    // Wait for scroll
    await page.waitForTimeout(500);

    // Verify features section is near top of viewport
    const featuresSection = page.locator('#features');
    const box = await featuresSection.boundingBox();
    expect(box).not.toBeNull();
    expect(box.y).toBeLessThan(150);

    // Verify menu closes after clicking link
    const navLinks = page.locator('[data-testid="nav-links"]');
    await expect(navLinks).not.toBeVisible();
  });

  /**
   * Additional test: Footer is responsive on mobile
   */
  test('Footer displays correctly on mobile', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Get footer content container
    const footerContent = page.locator('.footer-content');

    // Verify footer content is visible
    await expect(footerContent).toBeVisible();

    // On mobile, footer should be single column
    // Check that footer brand and links are stacked
    const footerBrand = page.locator('.footer-brand');
    const footerLinks = page.locator('.footer-links').first();

    const brandBox = await footerBrand.boundingBox();
    const linksBox = await footerLinks.boundingBox();

    expect(brandBox).not.toBeNull();
    expect(linksBox).not.toBeNull();

    // Links should be below brand (stacked layout)
    expect(linksBox.y).toBeGreaterThan(brandBox.y);
  });

  /**
   * Additional test: Tables are horizontally scrollable on mobile
   */
  test('Tables are horizontally scrollable on mobile', async ({ page }) => {
    // Scroll to commands section
    const commandsSection = page.locator('#commands');
    await commandsSection.scrollIntoViewIfNeeded();

    // Get table wrappers
    const tableWrappers = page.locator('.table-wrapper');
    const wrapperCount = await tableWrappers.count();
    expect(wrapperCount).toBeGreaterThan(0);

    // Check first table wrapper has overflow handling
    const firstWrapper = tableWrappers.first();
    const overflowX = await firstWrapper.evaluate((el) => window.getComputedStyle(el).overflowX);
    expect(overflowX).toBe('auto');
  });
});
