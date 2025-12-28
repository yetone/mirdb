// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: Navigation and Header
 * Scenario: Verify header contains logo and navigation links to Features, Docs, and GitHub
 */

test.describe('Navigation and Header', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage before each test
    await page.goto('/');
  });

  /**
   * Test Case 1: Logo is displayed in header and links to homepage
   * Input: Check header for logo
   * Expected: Logo is displayed in header and links to homepage
   */
  test('TC1: Logo is displayed in header and links to homepage', async ({ page }) => {
    // Verify the header exists
    const header = page.locator('header');
    await expect(header).toBeVisible();

    // Verify the logo link exists in the header
    const logoLink = page.locator('header .logo-link');
    await expect(logoLink).toBeVisible();

    // Verify the logo image is visible within the link
    const logoImg = page.locator('header .logo-link .nav-logo');
    await expect(logoImg).toBeVisible();

    // Verify the logo image has appropriate alt text
    const altText = await logoImg.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText.toLowerCase()).toMatch(/mirdb|logo/i);

    // Verify the logo link points to homepage (# or /)
    const href = await logoLink.getAttribute('href');
    expect(href === '#' || href === '/' || href === './').toBeTruthy();

    // Click the logo and verify we stay on the homepage
    await logoLink.click();
    // The logo href="#" navigates to /# which is effectively the homepage
    const currentUrl = page.url();
    expect(currentUrl === 'http://localhost:3000/' || currentUrl === 'http://localhost:3000/#').toBeTruthy();
  });

  /**
   * Test Case 2: Features navigation link is present and scrolls to features section
   * Input: Check header for Features link
   * Expected: Features navigation link is present and scrolls to features section
   */
  test('TC2: Features navigation link is present and scrolls to features section', async ({ page }) => {
    // Verify the Features link exists in the navigation
    const featuresLink = page.locator('header nav a[href="#features"]');
    await expect(featuresLink).toBeVisible();

    // Verify the link text contains "Features"
    const linkText = await featuresLink.textContent();
    expect(linkText.toLowerCase()).toContain('features');

    // Click the Features link
    await featuresLink.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify the URL hash changes to #features
    await expect(page).toHaveURL(/#features$/);

    // Verify the features section is now in view
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify the features section is in the viewport
    const isInViewport = await featuresSection.evaluate((el) => {
      const rect = el.getBoundingClientRect();
      return rect.top >= -100 && rect.top <= window.innerHeight;
    });
    expect(isInViewport).toBe(true);
  });

  /**
   * Test Case 3: Docs navigation link is present and scrolls to documentation section
   * Input: Check header for Docs link
   * Expected: Docs navigation link is present and scrolls to documentation section
   */
  test('TC3: Docs navigation link is present and scrolls to documentation section', async ({ page }) => {
    // Verify the Docs link exists in the navigation
    // Docs may link to #quick-start or #commands or #configuration (documentation sections)
    const docsLink = page.locator('header nav a:has-text("Docs")');
    await expect(docsLink).toBeVisible();

    // Get the href to know which section it points to
    const href = await docsLink.getAttribute('href');
    expect(href).toBeTruthy();

    // The docs link should point to a documentation-related section
    // Could be #quick-start, #commands, #configuration, or #docs
    expect(href.startsWith('#')).toBeTruthy();

    // Click the Docs link
    await docsLink.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify the URL hash changes
    await expect(page).toHaveURL(new RegExp(`${href}$`));

    // Verify the target section is now in view
    const targetSection = page.locator(href);
    await expect(targetSection).toBeVisible();

    // Verify the section is in the viewport
    const isInViewport = await targetSection.evaluate((el) => {
      const rect = el.getBoundingClientRect();
      return rect.top >= -100 && rect.top <= window.innerHeight;
    });
    expect(isInViewport).toBe(true);
  });

  /**
   * Test Case 4: GitHub navigation link is present and opens repository in new tab
   * Input: Check header for GitHub link
   * Expected: GitHub navigation link is present and opens repository in new tab
   */
  test('TC4: GitHub navigation link is present and opens repository in new tab', async ({ page, context }) => {
    // Verify the GitHub link exists in the navigation
    const githubLink = page.locator('header nav a:has-text("GitHub")');
    await expect(githubLink).toBeVisible();

    // Verify the link text contains "GitHub"
    const linkText = await githubLink.textContent();
    expect(linkText.toLowerCase()).toContain('github');

    // Verify the href points to GitHub
    const href = await githubLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toContain('github.com');
    expect(href.toLowerCase()).toContain('mirdb');

    // Verify it opens in a new tab (target="_blank")
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify it has rel="noopener noreferrer" for security
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toBeTruthy();
    expect(rel).toContain('noopener');

    // Test that clicking opens a new page
    const [newPage] = await Promise.all([
      context.waitForEvent('page'),
      githubLink.click()
    ]);

    // Verify the new page URL contains github.com
    const newPageUrl = newPage.url();
    expect(newPageUrl).toContain('github.com');
  });

  /**
   * Additional test: Verify header is visible at top of page
   */
  test('Header is visible at top of page', async ({ page }) => {
    // Verify the header is visible
    const header = page.locator('header');
    await expect(header).toBeVisible();

    // Verify the header is at the top of the page
    const headerBox = await header.boundingBox();
    expect(headerBox).toBeTruthy();
    expect(headerBox.y).toBeLessThanOrEqual(10); // Header should be near the top

    // Verify all navigation links are within the header
    const navLinks = page.locator('header nav .nav-links li');
    const count = await navLinks.count();
    expect(count).toBeGreaterThanOrEqual(3); // At least Features, Docs, GitHub
  });
});
