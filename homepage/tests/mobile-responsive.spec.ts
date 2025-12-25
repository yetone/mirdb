import { test, expect } from '@playwright/test';

// Mobile viewport sizes
const MOBILE_VIEWPORTS = {
  iPhoneSE: { width: 375, height: 667 },
  iPhone12: { width: 390, height: 844 },
};

test.describe('Mobile Responsive Design', () => {
  test.describe('iPhone SE (375x667)', () => {
    test.use({ viewport: MOBILE_VIEWPORTS.iPhoneSE });

    test.beforeEach(async ({ page }) => {
      await page.goto('/');
    });

    test('TC1: Page renders correctly with single-column layout and readable text at 375x667', async ({ page }) => {
      // Page should load without horizontal overflow
      const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);
      expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth + 5); // Allow small tolerance

      // Hero section should be visible
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Product name should be visible and readable
      const productName = page.locator('[data-testid="product-name"]');
      await expect(productName).toBeVisible();

      // Tagline should be visible
      const tagline = page.locator('[data-testid="tagline"]');
      await expect(tagline).toBeVisible();

      // CTAs should be visible
      const getStartedBtn = page.locator('[data-testid="get-started-btn"]');
      const githubLink = page.locator('[data-testid="github-link"]');
      await expect(getStartedBtn).toBeVisible();
      await expect(githubLink).toBeVisible();

      // Check that buttons stack vertically on mobile (flex-col)
      const heroButtonsContainer = page.locator('.flex.flex-col.sm\\:flex-row');
      await expect(heroButtonsContainer).toBeVisible();

      // Get button positions - they should stack vertically
      const getStartedBox = await getStartedBtn.boundingBox();
      const githubBox = await githubLink.boundingBox();
      expect(getStartedBox).not.toBeNull();
      expect(githubBox).not.toBeNull();
      // On mobile, the GitHub link should be below the Get Started button
      expect(githubBox!.y).toBeGreaterThan(getStartedBox!.y);
    });

    test('TC2: Navigation collapses to hamburger menu on mobile', async ({ page }) => {
      // Desktop nav links should be hidden on mobile
      const desktopNavLinks = page.locator('.nav-links');
      await expect(desktopNavLinks).toBeHidden();

      // Hamburger menu button should be visible
      const hamburgerBtn = page.locator('[data-testid="mobile-menu-btn"]');
      await expect(hamburgerBtn).toBeVisible();

      // Hamburger menu should have accessible label
      await expect(hamburgerBtn).toHaveAttribute('aria-label', /menu|navigation/i);
    });

    test('TC3: Mobile navigation menu opens with accessible links to all sections', async ({ page }) => {
      // Click the hamburger menu button
      const hamburgerBtn = page.locator('[data-testid="mobile-menu-btn"]');
      await hamburgerBtn.click();

      // Mobile menu should be visible
      const mobileMenu = page.locator('[data-testid="mobile-menu"]');
      await expect(mobileMenu).toBeVisible();

      // All navigation links should be present
      const featuresLink = mobileMenu.locator('a[href="#features"]');
      const gettingStartedLink = mobileMenu.locator('a[href="#getting-started"]');
      const configurationLink = mobileMenu.locator('a[href="#configuration"]');
      const githubLink = mobileMenu.locator('a[href*="github"]');

      await expect(featuresLink).toBeVisible();
      await expect(gettingStartedLink).toBeVisible();
      await expect(configurationLink).toBeVisible();
      await expect(githubLink).toBeVisible();

      // Verify link text
      await expect(featuresLink).toHaveText('Features');
      await expect(gettingStartedLink).toHaveText('Getting Started');
      await expect(configurationLink).toHaveText('Configuration');
    });

    test('TC4: Code blocks are scrollable horizontally without breaking page layout', async ({ page }) => {
      // Scroll to getting started section where code blocks exist
      await page.locator('#getting-started').scrollIntoViewIfNeeded();
      await page.waitForTimeout(300);

      // Check that page doesn't have horizontal overflow
      const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);
      expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth + 5);

      // Find code blocks
      const codeBlocks = page.locator('pre');
      const codeBlockCount = await codeBlocks.count();
      expect(codeBlockCount).toBeGreaterThan(0);

      // Check that code blocks have overflow-x-auto or similar scrollable styling
      const firstCodeBlock = codeBlocks.first();
      const overflowX = await firstCodeBlock.evaluate(el => getComputedStyle(el).overflowX);
      expect(['auto', 'scroll']).toContain(overflowX);
    });

    test('TC5: Hero section displays without horizontal overflow on mobile', async ({ page }) => {
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Check hero section doesn't overflow horizontally
      const heroBox = await heroSection.boundingBox();
      expect(heroBox).not.toBeNull();
      expect(heroBox!.width).toBeLessThanOrEqual(MOBILE_VIEWPORTS.iPhoneSE.width);

      // Check all main elements are within viewport
      const productName = page.locator('[data-testid="product-name"]');
      const tagline = page.locator('[data-testid="tagline"]');
      const getStartedBtn = page.locator('[data-testid="get-started-btn"]');

      // Verify elements are not overflowing
      const productNameBox = await productName.boundingBox();
      const taglineBox = await tagline.boundingBox();
      const getStartedBox = await getStartedBtn.boundingBox();

      expect(productNameBox).not.toBeNull();
      expect(taglineBox).not.toBeNull();
      expect(getStartedBox).not.toBeNull();

      // Elements should fit within viewport
      expect(productNameBox!.x).toBeGreaterThanOrEqual(0);
      expect(productNameBox!.x + productNameBox!.width).toBeLessThanOrEqual(MOBILE_VIEWPORTS.iPhoneSE.width + 5);
      expect(taglineBox!.x).toBeGreaterThanOrEqual(0);
      expect(taglineBox!.x + taglineBox!.width).toBeLessThanOrEqual(MOBILE_VIEWPORTS.iPhoneSE.width + 5);
    });

    test('Mobile menu closes when clicking a navigation link', async ({ page }) => {
      // Open mobile menu
      const hamburgerBtn = page.locator('[data-testid="mobile-menu-btn"]');
      await hamburgerBtn.click();

      const mobileMenu = page.locator('[data-testid="mobile-menu"]');
      await expect(mobileMenu).toBeVisible();

      // Click a navigation link
      const featuresLink = mobileMenu.locator('a[href="#features"]');
      await featuresLink.click();
      await page.waitForTimeout(500);

      // Menu should close after clicking a link
      await expect(mobileMenu).toBeHidden();
    });

    test('Mobile menu closes when clicking outside', async ({ page }) => {
      // Open mobile menu
      const hamburgerBtn = page.locator('[data-testid="mobile-menu-btn"]');
      await hamburgerBtn.click();

      const mobileMenu = page.locator('[data-testid="mobile-menu"]');
      await expect(mobileMenu).toBeVisible();

      // Click outside the menu (on the hero section)
      await page.locator('[data-testid="hero-section"]').click({ force: true });
      await page.waitForTimeout(300);

      // Menu should close
      await expect(mobileMenu).toBeHidden();
    });
  });

  test.describe('iPhone 12 (390x844)', () => {
    test.use({ viewport: MOBILE_VIEWPORTS.iPhone12 });

    test.beforeEach(async ({ page }) => {
      await page.goto('/');
    });

    test('TC1: Page renders correctly at 390x844 viewport', async ({ page }) => {
      // Page should load without horizontal overflow
      const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);
      expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth + 5);

      // Hero section should be visible
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Product name should be visible
      const productName = page.locator('[data-testid="product-name"]');
      await expect(productName).toBeVisible();
    });

    test('TC2: Navigation is mobile-friendly at 390x844', async ({ page }) => {
      // Desktop nav links should be hidden
      const desktopNavLinks = page.locator('.nav-links');
      await expect(desktopNavLinks).toBeHidden();

      // Hamburger menu button should be visible
      const hamburgerBtn = page.locator('[data-testid="mobile-menu-btn"]');
      await expect(hamburgerBtn).toBeVisible();
    });

    test('TC3: Mobile navigation links are accessible at 390x844', async ({ page }) => {
      // Open hamburger menu
      const hamburgerBtn = page.locator('[data-testid="mobile-menu-btn"]');
      await hamburgerBtn.click();

      const mobileMenu = page.locator('[data-testid="mobile-menu"]');
      await expect(mobileMenu).toBeVisible();

      // All links should be present and clickable
      const links = mobileMenu.locator('a');
      const linkCount = await links.count();
      expect(linkCount).toBeGreaterThanOrEqual(4); // Features, Getting Started, Configuration, GitHub
    });
  });

  test.describe('Desktop viewport (should show desktop navigation)', () => {
    test.use({ viewport: { width: 1024, height: 768 } });

    test.beforeEach(async ({ page }) => {
      await page.goto('/');
    });

    test('Desktop navigation is visible and mobile menu is hidden', async ({ page }) => {
      // Desktop nav links should be visible
      const desktopNavLinks = page.locator('.nav-links');
      await expect(desktopNavLinks).toBeVisible();

      // Hamburger menu button should be hidden
      const hamburgerBtn = page.locator('[data-testid="mobile-menu-btn"]');
      await expect(hamburgerBtn).toBeHidden();
    });
  });
});
