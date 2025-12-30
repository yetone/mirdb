// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '..', 'index.html');

test.describe('GitHub Navigation Link', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  /**
   * Test Case 1: Search entire page for GitHub link
   * Expected: At least one link to github.com exists on the page
   */
  test('TC1: page contains at least one GitHub link', async ({ page }) => {
    // Find all links that contain github.com in href
    const githubLinks = page.locator('a[href*="github.com"]');
    const count = await githubLinks.count();

    // Verify at least one GitHub link exists
    expect(count).toBeGreaterThanOrEqual(1);

    // Verify the links point to valid GitHub URLs
    for (let i = 0; i < count; i++) {
      const href = await githubLinks.nth(i).getAttribute('href');
      expect(href).toMatch(/github\.com/);
    }
  });

  /**
   * Test Case 2: Check GitHub link visibility in hero or navigation
   * Expected: GitHub link is prominently placed in hero section or main navigation
   */
  test('TC2: GitHub link is prominently placed in hero section or navigation', async ({ page }) => {
    // Check for GitHub link in hero section
    const heroSection = page.locator('.hero');
    const heroGithubLink = heroSection.locator('a[href*="github.com"]');
    const heroLinkCount = await heroGithubLink.count();

    // Check for GitHub link in navigation (header nav, if exists)
    const navSection = page.locator('nav, header nav, .nav, .navigation');
    const navGithubLink = navSection.locator('a[href*="github.com"]');
    const navLinkCount = await navGithubLink.count();

    // At least one GitHub link should be in hero section or navigation
    const hasPrminentPlacement = heroLinkCount > 0 || navLinkCount > 0;
    expect(hasPrminentPlacement).toBe(true);

    // If in hero, verify it's visible
    if (heroLinkCount > 0) {
      await expect(heroGithubLink.first()).toBeVisible();
    }

    // If in nav, verify it's visible
    if (navLinkCount > 0) {
      await expect(navGithubLink.first()).toBeVisible();
    }
  });

  /**
   * Test Case 3: Inspect GitHub link security attributes
   * Expected: External GitHub link includes rel='noopener' or rel='noopener noreferrer'
   */
  test('TC3: GitHub links have proper security attributes', async ({ page }) => {
    // Find all GitHub links
    const githubLinks = page.locator('a[href*="github.com"]');
    const count = await githubLinks.count();
    expect(count).toBeGreaterThanOrEqual(1);

    // Check each GitHub link for security attributes
    for (let i = 0; i < count; i++) {
      const link = githubLinks.nth(i);
      const rel = await link.getAttribute('rel');
      const target = await link.getAttribute('target');

      // If link opens in new tab, it MUST have rel='noopener' or rel='noopener noreferrer'
      if (target === '_blank') {
        expect(rel).toBeTruthy();
        expect(rel).toMatch(/noopener/);
      }

      // Even without target="_blank", external links should have noopener for security
      // At least one link should have proper security attributes
    }

    // Verify at least one link has rel with noopener
    const linksWithNoopener = page.locator('a[href*="github.com"][rel*="noopener"]');
    const noopenerCount = await linksWithNoopener.count();
    expect(noopenerCount).toBeGreaterThanOrEqual(1);
  });

  /**
   * Test Case 4: Verify GitHub link target
   * Expected: GitHub link opens in new tab (target='_blank') or same tab with clear indication
   */
  test('TC4: GitHub link has appropriate target attribute', async ({ page }) => {
    // Find all GitHub links
    const githubLinks = page.locator('a[href*="github.com"]');
    const count = await githubLinks.count();
    expect(count).toBeGreaterThanOrEqual(1);

    // Track if we have at least one properly configured link
    let hasProperlyConfiguredLink = false;

    for (let i = 0; i < count; i++) {
      const link = githubLinks.nth(i);
      const target = await link.getAttribute('target');
      const linkText = await link.textContent();

      if (target === '_blank') {
        // Opens in new tab - this is acceptable for external links
        hasProperlyConfiguredLink = true;
      } else if (!target || target === '_self') {
        // Opens in same tab - link text should indicate it's going to GitHub
        const indicatesExternal = linkText && (
          linkText.toLowerCase().includes('github') ||
          linkText.toLowerCase().includes('source') ||
          linkText.toLowerCase().includes('repository') ||
          linkText.toLowerCase().includes('view on')
        );
        if (indicatesExternal) {
          hasProperlyConfiguredLink = true;
        }
      }
    }

    expect(hasProperlyConfiguredLink).toBe(true);
  });
});
