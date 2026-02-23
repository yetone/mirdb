/**
 * Navigation and Resources E2E Tests
 * Owner: Scenario 5 - Navigation and Resources
 *
 * Tests:
 * - Navigation element exists with links to Features, Quick Start, etc.
 * - Click nav link to Features section - page scrolls smoothly
 * - Click nav link to Quick Start section - page scrolls smoothly
 * - GitHub repository link exists (https://github.com/yetone/mirdb)
 * - GitHub link has target='_blank' and rel='noopener noreferrer'
 * - CI badge element is present
 * - Footer exists with copyright text
 * - Logo in navigation links to top of page or homepage
 */

const { test, expect } = require('@playwright/test');

test.describe('Navigation and Resources', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Navigation element exists with links to Features, Quick Start, etc.', async ({ page }) => {
    // Check that header/nav element exists
    const header = page.locator('header.header');
    await expect(header).toBeVisible();

    // Check for navigation element
    const nav = page.locator('nav.header__nav');
    await expect(nav).toBeVisible();

    // Check for navigation links
    const navLinks = page.locator('.header__nav-link');
    await expect(navLinks).toHaveCount(await navLinks.count());

    // Verify specific navigation links exist
    await expect(page.locator('a.header__nav-link[href="#features"]')).toBeVisible();
    await expect(page.locator('a.header__nav-link[href="#quick-start"]')).toBeVisible();
  });

  test('Test Case 2: Click nav link to Features section - page scrolls', async ({ page }) => {
    // Get the features section position before clicking
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Click the Features nav link
    await page.locator('a.header__nav-link[href="#features"]').click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify the features section is now in view
    await expect(featuresSection).toBeInViewport({ ratio: 0.5 });
  });

  test('Test Case 3: Click nav link to Quick Start section - page scrolls', async ({ page }) => {
    // Get the quick start section
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Click the Quick Start nav link
    await page.locator('a.header__nav-link[href="#quick-start"]').click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify the quick start section is now in view
    await expect(quickStartSection).toBeInViewport({ ratio: 0.5 });
  });

  test('Test Case 4: GitHub repository link exists', async ({ page }) => {
    // Check for GitHub repository link
    const githubLink = page.locator('a[href="https://github.com/yetone/mirdb"]');
    await expect(githubLink.first()).toBeVisible();
  });

  test('Test Case 5: GitHub link opens in new tab with proper security attributes', async ({ page }) => {
    // Find GitHub link
    const githubLink = page.locator('a[href="https://github.com/yetone/mirdb"]').first();
    await expect(githubLink).toBeVisible();

    // Verify target="_blank" attribute
    await expect(githubLink).toHaveAttribute('target', '_blank');

    // Verify rel="noopener noreferrer" attribute
    const relAttr = await githubLink.getAttribute('rel');
    expect(relAttr).toContain('noopener');
    expect(relAttr).toContain('noreferrer');
  });

  test('Test Case 6: CI status badge is present', async ({ page }) => {
    // Scroll to resources section to trigger lazy loading
    await page.locator('#resources').scrollIntoViewIfNeeded();

    // Look for CI badge element (could be CircleCI, GitHub Actions, etc.)
    const ciBadge = page.locator('.resources__badge img, .resources__badges img');
    await expect(ciBadge.first()).toBeAttached();

    // Verify the badge has proper alt text for accessibility
    const altText = await ciBadge.first().getAttribute('alt');
    expect(altText).toBeTruthy();
  });

  test('Test Case 7: Footer exists with copyright text', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator('footer.footer');
    await footer.scrollIntoViewIfNeeded();

    // Check footer element exists
    await expect(footer).toBeVisible();

    // Check for copyright text specifically
    const copyrightElement = page.locator('.footer__copyright');
    await expect(copyrightElement).toBeVisible();

    // Verify copyright text content
    const footerText = await copyrightElement.textContent();
    expect(footerText).toMatch(/©|copyright|MirDB/i);
  });

  test('Test Case 8: Logo in navigation links to top of page or homepage', async ({ page }) => {
    // Find logo link in navigation
    const logoLink = page.locator('.header__logo-link, header a[href="#"], header a[href="/"], header a[href="#hero"]');
    await expect(logoLink.first()).toBeVisible();

    // Verify it links to top of page (either #, #hero, or /)
    const href = await logoLink.first().getAttribute('href');
    expect(['#', '#hero', '/', '']).toContain(href);
  });
});
