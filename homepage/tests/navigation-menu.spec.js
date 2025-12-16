// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E and Unit Tests for MirDB Homepage Navigation Menu Functionality
 *
 * Test Case 1: Navigation menu is present in the header
 * Test Case 2: Logo/home link navigation returns to top of homepage
 * Test Case 3: Features link scrolls to or navigates to features section
 * Test Case 4: GitHub link opens in new tab
 * Test Case 5: External links have target='_blank' and rel='noopener noreferrer'
 */

test.describe('Navigation Menu Functionality', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/');
  });

  test('Test Case 1: Navigation menu is present in the header', async ({ page }) => {
    // Verify the header element exists
    const header = page.locator('header.header');
    await expect(header).toBeVisible();

    // Verify the navigation element exists within the header
    const nav = page.locator('nav.nav');
    await expect(nav).toBeVisible();

    // Verify the nav logo/home link exists
    const navLogo = page.locator('.nav-logo');
    await expect(navLogo).toBeVisible();

    // Verify the navigation links container exists
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Verify navigation links are present
    const navLinkItems = page.locator('.nav-links li');
    const linkCount = await navLinkItems.count();
    expect(linkCount).toBeGreaterThanOrEqual(1);
  });

  test('Test Case 2: Logo/home link navigation returns to top of homepage', async ({ page }) => {
    // Verify the logo/home link exists and is properly configured
    const navLogo = page.locator('.nav-logo');
    await expect(navLogo).toBeVisible();

    // Verify the logo links to the homepage root
    await expect(navLogo).toHaveAttribute('href', '/');

    // Verify the logo text contains MirDB
    const logoText = await navLogo.textContent();
    expect(logoText).toContain('MirDB');

    // Navigate to Features section first by clicking the features link
    const featuresLink = page.locator('.nav-links a[href="#features"]');
    await featuresLink.click();
    await expect(page).toHaveURL(/#features/);

    // Now click the logo/home link
    await navLogo.click();

    // Verify we're navigated back to the homepage (URL should be root without hash)
    await expect(page).toHaveURL(/\/$/);
  });

  test('Test Case 3: Features link scrolls to or navigates to features section', async ({ page }) => {
    // Find the Features link in the navigation
    const featuresLink = page.locator('.nav-links a[href="#features"]');
    await expect(featuresLink).toBeVisible();

    // Verify the Features link text
    const linkText = await featuresLink.textContent();
    expect(linkText?.toLowerCase()).toContain('features');

    // Click the Features link
    await featuresLink.click();

    // Verify the URL now includes #features anchor
    await expect(page).toHaveURL(/#features/);

    // Wait for scroll animation to complete
    await page.waitForTimeout(500);

    // Verify the features section is in view
    const featuresSection = page.getByTestId('features-section');
    await expect(featuresSection).toBeInViewport();
  });

  test('Test Case 4: GitHub link opens in new tab', async ({ page }) => {
    // Find the GitHub link in the navigation
    const githubLink = page.locator('.nav-links a:has-text("GitHub")');
    await expect(githubLink).toBeVisible();

    // Verify the GitHub link has target="_blank" to open in new tab
    await expect(githubLink).toHaveAttribute('target', '_blank');

    // Verify the GitHub link points to GitHub
    const href = await githubLink.getAttribute('href');
    expect(href).toContain('github.com');
  });

  test('Test Case 5: External links have target="_blank" and rel="noopener noreferrer"', async ({ page }) => {
    // Get all external links (links with target="_blank")
    const externalLinks = page.locator('.nav-links a[target="_blank"]');
    const count = await externalLinks.count();

    // Verify at least one external link exists
    expect(count).toBeGreaterThan(0);

    // Check each external link has proper rel attribute
    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const rel = await link.getAttribute('rel');

      // Verify rel contains both noopener and noreferrer for security
      expect(rel).toBeTruthy();
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    }

    // Specifically verify the GitHub link has proper attributes
    const githubLink = page.locator('.nav-links a:has-text("GitHub")');
    await expect(githubLink).toHaveAttribute('rel', /noopener.*noreferrer|noreferrer.*noopener/);
  });

  test('Navigation links are accessible via keyboard', async ({ page }) => {
    // Start by focusing on the nav logo
    const navLogo = page.locator('.nav-logo');
    await navLogo.focus();
    await expect(navLogo).toBeFocused();

    // Tab through navigation links
    await page.keyboard.press('Tab');

    // First nav link should be focused
    const firstNavLink = page.locator('.nav-links li:first-child a');
    await expect(firstNavLink).toBeFocused();
  });

  test('Navigation has proper ARIA labels for accessibility', async ({ page }) => {
    // Verify the nav logo has aria-label for screen readers
    const navLogo = page.locator('.nav-logo');
    const ariaLabel = await navLogo.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
  });
});
