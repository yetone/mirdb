// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Navigation Functionality E2E Tests
 *
 * Scenario: Verify navigation provides access to documentation, GitHub, and other resources as specified in REQ-5
 * Tests for header navigation, footer links, and smooth scrolling behavior
 */

test.describe('Navigation Functionality', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Check for logo and product name in header
   * Input: Check for logo and product name in header
   * Expected: Logo and 'MirDB' product name are displayed in the header
   */
  test('should display logo and MirDB product name in header', async ({ page }) => {
    // Find the header navigation
    const header = page.locator('header.header');
    await expect(header).toBeVisible();

    // Find the logo/product name link
    const logo = page.locator('.logo');
    await expect(logo).toBeVisible();
    await expect(logo).toContainText('MirDB');

    // Verify it's in the navigation area
    const nav = page.locator('nav.nav');
    await expect(nav).toBeVisible();
  });

  /**
   * Test Case 2: Click Features navigation link
   * Input: Click Features navigation link
   * Expected: Page scrolls to the Features section
   */
  test('should scroll to Features section when clicking Features navigation link', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click on Features navigation link
    await page.click('a[href="#features"]');

    // Wait for scroll animation to complete
    await page.waitForTimeout(500);

    // Verify the Features section is in view
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify page has scrolled (scroll position changed or element is at top of viewport)
    const boundingBox = await featuresSection.boundingBox();
    expect(boundingBox).toBeTruthy();
    // The section should be near the top of the viewport (within reasonable range for navigation bar)
    expect(boundingBox.y).toBeLessThan(200);
  });

  /**
   * Test Case 3: Click Getting Started navigation link
   * Input: Click Getting Started navigation link
   * Expected: Page scrolls to the Getting Started section
   */
  test('should scroll to Getting Started section when clicking Getting Started navigation link', async ({ page }) => {
    // Click on Getting Started navigation link
    await page.click('a[href="#getting-started"]');

    // Wait for scroll animation to complete
    await page.waitForTimeout(500);

    // Verify the Getting Started section is in view
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Verify the section is near the top of the viewport
    const boundingBox = await gettingStartedSection.boundingBox();
    expect(boundingBox).toBeTruthy();
    expect(boundingBox.y).toBeLessThan(200);
  });

  /**
   * Test Case 4: Click Documentation navigation link
   * Input: Click Documentation navigation link
   * Expected: Navigation to documentation page or external documentation site
   */
  test('should have Documentation link in navigation', async ({ page }) => {
    // Find the Documentation link in the navigation
    const docsLink = page.locator('nav.nav a:has-text("Documentation")');
    await expect(docsLink).toBeVisible();

    // Verify the link has an href attribute (either internal or external)
    const href = await docsLink.getAttribute('href');
    expect(href).toBeTruthy();

    // If it's an external link, it should have target="_blank"
    if (href.startsWith('http')) {
      const target = await docsLink.getAttribute('target');
      expect(target).toBe('_blank');

      // Should also have rel="noopener" for security
      const rel = await docsLink.getAttribute('rel');
      expect(rel).toContain('noopener');
    }
  });

  /**
   * Test Case 5: Click GitHub navigation link
   * Input: Click GitHub navigation link
   * Expected: Opens GitHub repository in new tab or navigates to GitHub
   */
  test('should have GitHub link that opens in new tab', async ({ page }) => {
    // Find the GitHub link in the navigation
    const githubLink = page.locator('nav.nav a:has-text("GitHub")');
    await expect(githubLink).toBeVisible();

    // Verify the link points to GitHub
    const href = await githubLink.getAttribute('href');
    expect(href).toContain('github.com');
    expect(href).toContain('mirdb');

    // Verify it opens in a new tab
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify security attribute
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  /**
   * Test Case 6: Verify footer links
   * Input: Verify footer links
   * Expected: Footer contains links to Documentation, GitHub, and License
   */
  test('should display footer with Documentation, GitHub, and License links', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator('footer.footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Find the footer navigation
    const footerNav = footer.locator('.footer-nav, nav');

    // Verify Documentation link in footer
    const docsLink = footerNav.locator('a:has-text("Documentation")');
    await expect(docsLink).toBeVisible();
    const docsHref = await docsLink.getAttribute('href');
    expect(docsHref).toBeTruthy();

    // Verify GitHub link in footer
    const githubLink = footerNav.locator('a:has-text("GitHub")');
    await expect(githubLink).toBeVisible();
    const githubHref = await githubLink.getAttribute('href');
    expect(githubHref).toContain('github.com');

    // Verify License link in footer
    const licenseLink = footerNav.locator('a:has-text("License")');
    await expect(licenseLink).toBeVisible();
    const licenseHref = await licenseLink.getAttribute('href');
    expect(licenseHref).toBeTruthy();
    expect(licenseHref.toLowerCase()).toContain('license');
  });

  /**
   * Additional test: Verify all required navigation links are present
   */
  test('should have all required navigation links (Features, Getting Started, Documentation, GitHub)', async ({ page }) => {
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Check for Features link
    const featuresLink = navLinks.locator('a[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await expect(featuresLink).toContainText('Features');

    // Check for Getting Started link
    const gettingStartedLink = navLinks.locator('a[href="#getting-started"]');
    await expect(gettingStartedLink).toBeVisible();
    await expect(gettingStartedLink).toContainText('Getting Started');

    // Check for Documentation link
    const docsLink = navLinks.locator('a:has-text("Documentation")');
    await expect(docsLink).toBeVisible();

    // Check for GitHub link
    const githubLink = navLinks.locator('a:has-text("GitHub")');
    await expect(githubLink).toBeVisible();
  });

  /**
   * Additional test: Verify logo links to homepage
   */
  test('should have logo that links to homepage', async ({ page }) => {
    const logo = page.locator('.logo');
    await expect(logo).toBeVisible();

    const href = await logo.getAttribute('href');
    // Logo should link to homepage (either "#" or "/" or "#hero")
    expect(href === '#' || href === '/' || href === '#hero').toBeTruthy();
  });
});
