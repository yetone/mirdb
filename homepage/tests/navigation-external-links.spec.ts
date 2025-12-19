import { test, expect } from '@playwright/test';

test.describe('Navigation and External Links', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage
    await page.goto('file://' + process.cwd() + '/public/index.html');
  });

  test('TC1: Header contains navigation bar with logo and links', async ({ page }) => {
    // Verify header exists
    const header = page.locator('header');
    await expect(header).toBeVisible();

    // Verify navbar exists
    const navbar = page.locator('nav.navbar, nav, header nav');
    await expect(navbar).toBeVisible();

    // Verify logo exists in navigation
    const logo = page.locator('.logo').first();
    await expect(logo).toBeVisible();
    const logoText = await logo.textContent();
    expect(logoText?.toLowerCase()).toContain('mirdb');

    // Verify navigation links exist
    const navLinks = page.locator('.nav-links li a, nav ul li a');
    const linkCount = await navLinks.count();
    expect(linkCount).toBeGreaterThanOrEqual(3); // At least Features, Quick Start, and GitHub
  });

  test('TC2: Documentation link navigates to documentation page or section', async ({ page }) => {
    // Look for documentation link in header or any navigation area
    // Could be "Docs", "Documentation", "Quick Start" (which serves as inline docs), or similar
    const docsLink = page.locator('nav a[href*="doc"], nav a[href*="quickstart"], nav a[href="#quickstart"], a:has-text("Doc"), a:has-text("Quick Start")').first();

    // Verify the documentation link exists and is accessible
    await expect(docsLink).toBeVisible();

    // Get the href attribute
    const href = await docsLink.getAttribute('href');
    expect(href).toBeTruthy();

    // Click the link and verify navigation
    await docsLink.click();

    // If it's an internal anchor, verify the scroll happened
    if (href?.startsWith('#')) {
      // Wait for scroll to complete
      await page.waitForTimeout(500);

      // Verify the target section is visible
      const targetSection = page.locator(href);
      await expect(targetSection).toBeVisible();
    }
  });

  test('TC3: GitHub link opens repository in new tab', async ({ page }) => {
    // Find GitHub link in navigation or footer
    const githubLink = page.locator('a[href*="github.com/mirdb"]').first();
    await expect(githubLink).toBeVisible();

    // Verify it has target="_blank" attribute
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify the href points to a GitHub repository
    const href = await githubLink.getAttribute('href');
    expect(href).toContain('github.com');
    expect(href).toContain('mirdb');
  });

  test('TC4: External links include rel="noopener noreferrer" attribute', async ({ page }) => {
    // Find all external links (links with target="_blank")
    const externalLinks = page.locator('a[target="_blank"]');
    const count = await externalLinks.count();

    expect(count).toBeGreaterThan(0); // At least GitHub link should exist

    // Check each external link for security attributes
    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const rel = await link.getAttribute('rel');

      // Verify rel contains both noopener and noreferrer for security
      expect(rel).toBeTruthy();
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    }
  });

  test('TC5: Features navigation link scrolls to Features section', async ({ page }) => {
    // Find the Features navigation link
    const featuresLink = page.locator('nav a[href="#features"], .nav-links a[href="#features"]');
    await expect(featuresLink).toBeVisible();

    // Click the Features link
    await featuresLink.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500);

    // Verify the Features section is now visible and near the top of viewport
    const featuresSection = page.locator('#features, [data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Check that the section is within viewport
    const boundingBox = await featuresSection.boundingBox();
    expect(boundingBox).toBeTruthy();
    // Section should be near the top after scrolling (within 150px from top)
    expect(boundingBox!.y).toBeLessThan(150);
  });

  test('TC6: Quick Start navigation link scrolls to Quick Start section', async ({ page }) => {
    // Find the Quick Start navigation link
    const quickstartLink = page.locator('nav a[href="#quickstart"], .nav-links a[href="#quickstart"]');
    await expect(quickstartLink).toBeVisible();

    // Click the Quick Start link
    await quickstartLink.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500);

    // Verify the Quick Start section is now visible and near the top of viewport
    const quickstartSection = page.locator('#quickstart, [data-testid="quickstart-section"]');
    await expect(quickstartSection).toBeVisible();

    // Check that the section is within viewport
    const boundingBox = await quickstartSection.boundingBox();
    expect(boundingBox).toBeTruthy();
    // Section should be near the top after scrolling (within 150px from top)
    expect(boundingBox!.y).toBeLessThan(150);
  });

  test('TC7: Logo links to homepage or scrolls to top', async ({ page }) => {
    // First scroll down the page
    await page.evaluate(() => window.scrollTo(0, 1000));
    await page.waitForTimeout(300);

    // Find the logo link
    const logoLink = page.locator('.logo, nav a[href="/"], nav a[href="#"], header a.logo').first();
    await expect(logoLink).toBeVisible();

    // Get initial scroll position (should be > 0)
    const initialScrollY = await page.evaluate(() => window.scrollY);
    expect(initialScrollY).toBeGreaterThan(0);

    // Click the logo
    await logoLink.click();

    // Wait for any scroll animation
    await page.waitForTimeout(500);

    // Verify either:
    // 1. Page scrolled to top (scrollY is near 0)
    // 2. URL changed to homepage (for single-page navigation)
    const finalScrollY = await page.evaluate(() => window.scrollY);
    const currentURL = page.url();

    // Either scroll happened OR we navigated home
    const scrolledToTop = finalScrollY < 100;
    const navigatedHome = currentURL.endsWith('/') || currentURL.endsWith('index.html');

    expect(scrolledToTop || navigatedHome).toBe(true);
  });
});
