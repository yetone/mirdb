// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Navigation Links and Header Tests
 *
 * Scenario: Verify navigation provides clear access to documentation, GitHub, and getting started
 */

test.describe('Navigation Links and Header', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Check for logo in header
   * Expected: MirDB logo is displayed in the header navigation
   */
  test('should display MirDB logo in the header navigation', async ({ page }) => {
    // Check header is visible
    const header = page.locator('header, .header');
    await expect(header).toBeVisible();

    // Check for logo - either an image logo or a text logo with branding
    const navBrand = header.locator('.nav-brand, .logo, [data-testid="logo"]');
    await expect(navBrand).toBeVisible();

    // Verify it contains MirDB branding
    const brandText = await navBrand.textContent();
    expect(brandText.toLowerCase()).toContain('mirdb');

    // Check for logo image or SVG if present
    const logoImage = header.locator('.nav-brand img, .nav-brand svg, .logo-image, [data-testid="logo-image"]');
    const logoImageCount = await logoImage.count();

    if (logoImageCount > 0) {
      await expect(logoImage.first()).toBeVisible();
    } else {
      // Text logo is acceptable - verify the brand link is functional
      const brandLink = navBrand.locator('a');
      await expect(brandLink).toBeVisible();
    }
  });

  /**
   * Test Case 2: Check for Documentation link
   * Expected: Documentation link is present in navigation and opens documentation
   */
  test('should have Documentation link in navigation', async ({ page }) => {
    const header = page.locator('header, .header');
    await expect(header).toBeVisible();

    // Find Documentation link in navigation
    const docsLink = header.locator('a:has-text("Documentation"), a:has-text("Docs")');
    await expect(docsLink).toBeVisible();

    // Verify the link has a valid href
    const href = await docsLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href.length).toBeGreaterThan(0);
  });

  /**
   * Test Case 3: Check for GitHub link
   * Expected: GitHub link is present and opens MirDB repository
   */
  test('should have GitHub link in navigation that opens MirDB repository', async ({ page }) => {
    const header = page.locator('header, .header');
    await expect(header).toBeVisible();

    // Find GitHub link in navigation
    const githubLink = header.locator('.nav-links a:has-text("GitHub")');
    await expect(githubLink).toBeVisible();

    // Verify it links to GitHub
    const href = await githubLink.getAttribute('href');
    expect(href).toMatch(/github\.com/i);
    expect(href.toLowerCase()).toContain('mirdb');

    // Verify it opens in new tab (best practice for external links)
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify it has noopener for security
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  /**
   * Test Case 4: Check for Getting Started link
   * Expected: Getting Started link navigates to quick-start section or guide
   */
  test('should have Getting Started link that navigates to quick-start section', async ({ page }) => {
    const header = page.locator('header, .header');
    await expect(header).toBeVisible();

    // Find Getting Started or Quick Start link in navigation
    const getStartedLink = header.locator('a:has-text("Getting Started"), a:has-text("Quick Start"), a:has-text("Get Started")');
    await expect(getStartedLink).toBeVisible();

    // Verify the link has a valid href pointing to quickstart section
    const href = await getStartedLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toMatch(/(#quickstart|#getting-started|\/quickstart|\/docs)/i);

    // Click the link and verify navigation
    await getStartedLink.click();
    await page.waitForTimeout(500);

    // After clicking, should navigate to quickstart section
    const url = page.url();
    expect(url).toMatch(/(quickstart|getting-started)/i);
  });

  /**
   * Test Case 5: Scroll page and check header visibility
   * Expected: Header remains fixed/sticky at top of viewport while scrolling
   */
  test('should keep header fixed/sticky at top of viewport while scrolling', async ({ page }) => {
    // Set viewport to ensure we have scrollable content
    await page.setViewportSize({ width: 1280, height: 720 });

    const header = page.locator('header, .header');
    await expect(header).toBeVisible();

    // Get initial header position
    const initialBox = await header.boundingBox();
    expect(initialBox.y).toBe(0); // Header should start at top

    // Scroll down the page significantly
    await page.evaluate(() => {
      window.scrollTo(0, 500);
    });
    await page.waitForTimeout(300);

    // Get header position after scrolling
    const scrolledBox = await header.boundingBox();

    // Header should still be at the top of the viewport (y = 0 or very close)
    expect(scrolledBox.y).toBeLessThanOrEqual(5);

    // Scroll down even more
    await page.evaluate(() => {
      window.scrollTo(0, 1000);
    });
    await page.waitForTimeout(300);

    // Header should still be visible and at the top
    await expect(header).toBeVisible();
    const deepScrollBox = await header.boundingBox();
    expect(deepScrollBox.y).toBeLessThanOrEqual(5);

    // Verify header is in viewport
    await expect(header).toBeInViewport();
  });

  /**
   * Test Case 6: Check footer for license information
   * Expected: Footer displays license information and additional resource links
   */
  test('should display footer with license information and resource links', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator('footer, .footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Check for license information
    const footerText = await footer.textContent();
    expect(footerText.toLowerCase()).toMatch(/(license|mit|open source)/i);

    // Check for resource links
    const footerLinks = footer.locator('a');
    const linkCount = await footerLinks.count();
    expect(linkCount).toBeGreaterThanOrEqual(1);

    // Verify at least one link is a GitHub-related link
    const githubLink = footer.locator('a[href*="github.com"]');
    const githubLinkCount = await githubLink.count();
    expect(githubLinkCount).toBeGreaterThanOrEqual(1);
  });
});

test.describe('Navigation Accessibility', () => {
  test('should have keyboard-navigable header links', async ({ page }) => {
    await page.goto('/');

    const header = page.locator('header, .header');
    await expect(header).toBeVisible();

    // Focus on the first navigation link
    const navLinks = header.locator('.nav-links a, nav a');
    const linkCount = await navLinks.count();

    expect(linkCount).toBeGreaterThanOrEqual(3);

    // Verify each link can receive focus
    for (let i = 0; i < linkCount; i++) {
      const link = navLinks.nth(i);
      await link.focus();
      await expect(link).toBeFocused();
    }
  });
});
