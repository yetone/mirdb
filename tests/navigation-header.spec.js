const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Navigation and Header
 * Scenario: Verify header navigation contains required elements and links
 * Tests the header component including logo, navigation links to Features, Docs, and GitHub
 */
test.describe('Navigation and Header', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Header contains MirDB logo or text brand', async ({ page }) => {
    // Verify header element exists
    const header = page.locator('header');
    await expect(header).toBeVisible();

    // Verify logo element exists in header
    const logo = header.locator('.logo');
    await expect(logo).toBeVisible();

    // Verify the logo contains the MirDB brand name
    const brandText = logo.locator('span');
    await expect(brandText).toBeVisible();
    await expect(brandText).toHaveText('MirDB');

    // Verify the logo image is present (logo.gif)
    const logoImage = logo.locator('img');
    await expect(logoImage).toBeVisible();
    const logoSrc = await logoImage.getAttribute('src');
    expect(logoSrc).toContain('logo');
    const logoAlt = await logoImage.getAttribute('alt');
    expect(logoAlt).toContain('MirDB');
  });

  test('TC2: Navigation contains link to Features section', async ({ page }) => {
    // Locate the navigation links in header
    const navLinks = page.locator('header .nav-links');
    await expect(navLinks).toBeVisible();

    // Find the Features link
    const featuresLink = navLinks.locator('a[href="#features"]');
    await expect(featuresLink).toBeVisible();

    // Verify the link text contains "Features"
    const linkText = await featuresLink.textContent();
    expect(linkText.toLowerCase()).toContain('features');

    // Verify the href points to the features section
    const href = await featuresLink.getAttribute('href');
    expect(href).toBe('#features');
  });

  test('TC3: Navigation contains link to Documentation', async ({ page }) => {
    // Locate the navigation links in header
    const navLinks = page.locator('header .nav-links');
    await expect(navLinks).toBeVisible();

    // Find the Docs link (case-insensitive)
    const docsLink = navLinks.locator('a').filter({
      hasText: /docs|documentation/i
    });
    await expect(docsLink).toBeVisible();

    // Verify the link has a valid href
    const href = await docsLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href.length).toBeGreaterThan(0);

    // Verify the link text contains "Docs" or "Documentation"
    const linkText = await docsLink.textContent();
    expect(linkText.toLowerCase()).toMatch(/docs|documentation/);
  });

  test('TC4: Navigation contains link to GitHub repository', async ({ page }) => {
    // Locate the navigation links in header
    const navLinks = page.locator('header .nav-links');
    await expect(navLinks).toBeVisible();

    // Find the GitHub link specifically (filter by text "GitHub" to avoid matching Docs link)
    const githubLink = navLinks.locator('a').filter({ hasText: 'GitHub' });
    await expect(githubLink).toBeVisible();

    // Verify the link text contains "GitHub"
    const linkText = await githubLink.textContent();
    expect(linkText).toContain('GitHub');

    // Verify the href points to the GitHub repository
    const href = await githubLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Verify it opens in a new tab
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');
  });

  test('TC5: Clicking Features navigation link scrolls to Features section', async ({ page }) => {
    // Verify the Features section exists
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the Features navigation link
    const featuresLink = page.locator('header .nav-links a[href="#features"]');
    await featuresLink.click();

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Verify the Features section is in view or scroll position changed
    const featuresBox = await featuresSection.boundingBox();
    const viewportHeight = await page.evaluate(() => window.innerHeight);

    // The features section should be at or near the top of the viewport after clicking
    // or the scroll position should have changed
    const finalScrollY = await page.evaluate(() => window.scrollY);

    // Either scroll position changed, or features section is in viewport
    const scrolledOrInView = finalScrollY !== initialScrollY ||
      (featuresBox.y >= 0 && featuresBox.y < viewportHeight);

    expect(scrolledOrInView).toBe(true);

    // Verify the URL hash changed to #features
    const currentUrl = page.url();
    expect(currentUrl).toContain('#features');
  });
});
