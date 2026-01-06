// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Footer Section Tests
 *
 * These tests verify that the footer section displays correctly with:
 * - GitHub link
 * - License information
 * - Status badges
 */

test.describe('Footer Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('footer element exists on the page', async ({ page }) => {
    // Test Case 1: Query for footer element
    const footer = page.locator('footer[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Footer should be at the bottom of the page
    const footerBox = await footer.boundingBox();
    const pageHeight = await page.evaluate(() => document.documentElement.scrollHeight);
    expect(footerBox).not.toBeNull();

    // Footer's bottom should be at or near the page bottom
    if (footerBox) {
      const footerBottom = footerBox.y + footerBox.height;
      // Allow some tolerance for padding/margins
      expect(footerBottom).toBeGreaterThanOrEqual(pageHeight - 10);
    }
  });

  test('GitHub repository link exists in footer', async ({ page }) => {
    // Test Case 2: Check for GitHub link in footer
    const footer = page.locator('footer[data-testid="footer"]');
    const githubLink = footer.locator('a[data-testid="footer-github-link"]');

    await expect(githubLink).toBeVisible();

    // Verify it points to the MirDB GitHub repository
    const href = await githubLink.getAttribute('href');
    expect(href).toContain('github.com/yetone/mirdb');

    // Verify it opens in a new tab
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify it has proper rel attribute for security
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('license information is displayed in footer', async ({ page }) => {
    // Test Case 3: Check for license information
    const footer = page.locator('footer[data-testid="footer"]');
    const licenseInfo = footer.locator('[data-testid="footer-license"]');

    await expect(licenseInfo).toBeVisible();

    // Check that license text is present (ISC license as per project)
    const licenseText = await licenseInfo.textContent();
    expect(licenseText).toBeTruthy();
    expect(licenseText?.toLowerCase()).toMatch(/isc|license/);
  });

  test('status badges are visible in footer area', async ({ page }) => {
    // Test Case 4: Check for status badges in footer
    const footer = page.locator('footer[data-testid="footer"]');
    const badgesContainer = footer.locator('[data-testid="footer-badges"]');

    await expect(badgesContainer).toBeVisible();

    // Check for at least one badge image
    const badges = badgesContainer.locator('img');
    const badgeCount = await badges.count();
    expect(badgeCount).toBeGreaterThanOrEqual(1);

    // Verify badges are properly loaded (have src attributes)
    const firstBadge = badges.first();
    const src = await firstBadge.getAttribute('src');
    expect(src).toBeTruthy();
    expect(src).toContain('img.shields.io');

    // Verify badges have alt text for accessibility
    const alt = await firstBadge.getAttribute('alt');
    expect(alt).toBeTruthy();
  });

  test('footer GitHub link navigates to MirDB GitHub repository', async ({ page }) => {
    // Test Case 5: Click footer GitHub link and verify navigation
    const footer = page.locator('footer[data-testid="footer"]');
    const githubLink = footer.locator('a[data-testid="footer-github-link"]');

    await expect(githubLink).toBeVisible();

    // Get the href to verify the link destination
    const href = await githubLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Since the link opens in a new tab, we verify the href is correct
    // rather than actually navigating (which would require handling new tabs)
    // This is the standard approach for external links in E2E tests

    // Alternatively, we can verify the link is clickable
    await expect(githubLink).toBeEnabled();

    // Verify the link text or content is meaningful
    const linkText = await githubLink.textContent();
    expect(linkText?.toLowerCase()).toMatch(/github|view source|source code|repository/);
  });

  test('footer has proper semantic HTML structure', async ({ page }) => {
    // Additional test for accessibility and semantic structure
    const footer = page.locator('footer[data-testid="footer"]');

    // Footer should be a semantic <footer> element
    const tagName = await footer.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('footer');

    // Footer content should be within a container for proper layout
    const container = footer.locator('.footer-container, .container');
    await expect(container).toBeVisible();
  });

  test('footer is responsive and visible on mobile viewport', async ({ page }) => {
    // Test responsiveness
    await page.setViewportSize({ width: 375, height: 667 }); // iPhone SE viewport

    const footer = page.locator('footer[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Check that GitHub link is still visible on mobile
    const githubLink = footer.locator('a[data-testid="footer-github-link"]');
    await expect(githubLink).toBeVisible();

    // Check that badges are still visible
    const badgesContainer = footer.locator('[data-testid="footer-badges"]');
    await expect(badgesContainer).toBeVisible();
  });
});
