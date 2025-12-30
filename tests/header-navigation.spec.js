const { test, expect } = require('@playwright/test');

/**
 * Header Navigation Tests
 * Scenario: Validate the header navigation provides clear access to key sections and external links
 */

test.describe('Navigation and Header', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Page contains a header element with navigation', async ({ page }) => {
    // Verify header element is present
    const header = page.locator('header');
    await expect(header).toBeVisible();

    // Verify header contains a nav element
    const nav = header.locator('nav');
    await expect(nav).toBeVisible();

    // Verify navigation contains list of links
    const navLinks = nav.locator('ul li a');
    const count = await navLinks.count();
    expect(count).toBeGreaterThanOrEqual(3);
  });

  test('Test Case 2: Header contains link to Features section (anchor or smooth scroll)', async ({ page }) => {
    // Find the Features navigation link in the header
    const header = page.locator('header');
    const featuresLink = header.locator('a[href="#features"], a:has-text("Features")');

    await expect(featuresLink.first()).toBeVisible();

    // Verify the href attribute points to the features section
    const href = await featuresLink.first().getAttribute('href');
    expect(href).toContain('features');

    // Click the link and verify the page scrolls to the features section
    await featuresLink.first().click();

    // Verify the features section is visible after clicking
    const featuresSection = page.locator('#features, [id*="features"]');
    await expect(featuresSection.first()).toBeVisible();
  });

  test('Test Case 3: Header contains link to Quick Start section', async ({ page }) => {
    // Find the Quick Start navigation link in the header
    const header = page.locator('header');
    const quickstartLink = header.locator('a[href="#quickstart"], a:has-text("Quick Start")');

    await expect(quickstartLink.first()).toBeVisible();

    // Verify the href attribute points to the quickstart section
    const href = await quickstartLink.first().getAttribute('href');
    expect(href).toContain('quickstart');

    // Click the link and verify the page scrolls to the quickstart section
    await quickstartLink.first().click();

    // Verify the quickstart section is visible after clicking
    const quickstartSection = page.locator('#quickstart, [id*="quickstart"]');
    await expect(quickstartSection.first()).toBeVisible();
  });

  test('Test Case 4: Header contains external link to GitHub repository', async ({ page }) => {
    // Find the GitHub navigation link in the header
    const header = page.locator('header');
    const githubLink = header.locator('a:has-text("GitHub")');

    await expect(githubLink.first()).toBeVisible();

    // Verify the href attribute points to the GitHub repository
    const href = await githubLink.first().getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Verify the link opens in a new tab (external link best practice)
    const target = await githubLink.first().getAttribute('target');
    expect(target).toBe('_blank');

    // Verify the link has rel="noopener" for security
    const rel = await githubLink.first().getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('Header logo links to homepage', async ({ page }) => {
    // Verify logo is present and links to homepage
    const header = page.locator('header');
    const logo = header.locator('a.logo-text, a:has-text("MirDB"):first-child');

    await expect(logo.first()).toBeVisible();

    // Verify the logo text is MirDB
    const logoText = await logo.first().textContent();
    expect(logoText).toContain('MirDB');

    // Verify it links to homepage
    const href = await logo.first().getAttribute('href');
    expect(href === '/' || href === '#' || href === '').toBeTruthy();
  });
});
