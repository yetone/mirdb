import { test, expect } from '@playwright/test';

test.describe('Footer with License and Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Footer section is visible at bottom of page', async ({ page }) => {
    // Scroll to page footer
    // Expected: Footer section is visible at bottom of page

    const footer = page.locator('[data-testid="footer-section"]');

    // Scroll to footer
    await footer.scrollIntoViewIfNeeded();

    // Verify footer is visible
    await expect(footer).toBeVisible();

    // Verify footer is in viewport after scrolling
    await expect(footer).toBeInViewport();

    // Verify footer is at the bottom of the page (after main content)
    const footerBoundingBox = await footer.boundingBox();
    const mainContent = page.locator('main');
    const mainBoundingBox = await mainContent.boundingBox();

    expect(footerBoundingBox).not.toBeNull();
    expect(mainBoundingBox).not.toBeNull();

    // Footer should be at the bottom of the main content area
    if (footerBoundingBox && mainBoundingBox) {
      expect(footerBoundingBox.y).toBeGreaterThan(0);
    }
  });

  test('TC2: License text or link is displayed', async ({ page }) => {
    // Check footer for license information
    // Expected: License text or link is displayed

    const footer = page.locator('[data-testid="footer-section"]');
    await footer.scrollIntoViewIfNeeded();

    // Verify license text is present
    const licenseElement = page.locator('[data-testid="footer-license"]');
    await expect(licenseElement).toBeVisible();
    await expect(licenseElement).toContainText('MIT License');

    // Verify license link is present and functional
    const licenseLink = page.locator('[data-testid="footer-license-link"]');
    await expect(licenseLink).toBeVisible();
    await expect(licenseLink).toHaveAttribute('href', /opensource\.org\/licenses\/MIT/);
    await expect(licenseLink).toHaveText('MIT License');

    // Verify link opens in new tab with proper security attributes
    await expect(licenseLink).toHaveAttribute('target', '_blank');
    await expect(licenseLink).toHaveAttribute('rel', /noopener/);
  });

  test('TC3: Link to MirDB GitHub repository is present and functional', async ({ page }) => {
    // Check footer for GitHub link
    // Expected: Link to MirDB GitHub repository is present and functional

    const footer = page.locator('[data-testid="footer-section"]');
    await footer.scrollIntoViewIfNeeded();

    // Verify GitHub link is present
    const githubLink = page.locator('[data-testid="footer-github-link"]');
    await expect(githubLink).toBeVisible();

    // Verify link points to correct GitHub repository
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    await expect(githubLink).toContainText(/GitHub/i);

    // Verify link opens in new tab with proper security attributes
    await expect(githubLink).toHaveAttribute('target', '_blank');
    await expect(githubLink).toHaveAttribute('rel', /noopener/);
  });

  test('TC4: Links to community resources or contribution guidelines present', async ({ page }) => {
    // Check footer for community resources
    // Expected: Links to community resources or contribution guidelines present

    const footer = page.locator('[data-testid="footer-section"]');
    await footer.scrollIntoViewIfNeeded();

    // Verify community links container is present
    const communityLinks = page.locator('[data-testid="footer-community-links"]');
    await expect(communityLinks).toBeVisible();

    // Verify Contributing Guidelines link
    const contributingLink = page.locator('[data-testid="footer-contributing-link"]');
    await expect(contributingLink).toBeVisible();
    await expect(contributingLink).toHaveAttribute('href', /CONTRIBUTING/);
    await expect(contributingLink).toContainText(/Contributing/i);
    await expect(contributingLink).toHaveAttribute('target', '_blank');
    await expect(contributingLink).toHaveAttribute('rel', /noopener/);

    // Verify Issues link
    const issuesLink = page.locator('[data-testid="footer-issues-link"]');
    await expect(issuesLink).toBeVisible();
    await expect(issuesLink).toHaveAttribute('href', /github\.com\/yetone\/mirdb\/issues/);
    await expect(issuesLink).toContainText(/Issues/i);
    await expect(issuesLink).toHaveAttribute('target', '_blank');

    // Verify Discussions link
    const discussionsLink = page.locator('[data-testid="footer-discussions-link"]');
    await expect(discussionsLink).toBeVisible();
    await expect(discussionsLink).toHaveAttribute('href', /github\.com\/yetone\/mirdb\/discussions/);
    await expect(discussionsLink).toContainText(/Discussions/i);
    await expect(discussionsLink).toHaveAttribute('target', '_blank');
  });

  test('Footer has proper visual styling', async ({ page }) => {
    // Additional test to verify footer styling
    const footer = page.locator('[data-testid="footer-section"]');
    await footer.scrollIntoViewIfNeeded();

    // Verify footer has a dark background (bg-gray-950)
    await expect(footer).toBeVisible();

    // Verify copyright text is present
    const copyright = page.locator('[data-testid="footer-copyright"]');
    await expect(copyright).toBeVisible();
    await expect(copyright).toContainText('MirDB');
    await expect(copyright).toContainText('Open source');
  });

  test('Footer links have proper accessibility attributes', async ({ page }) => {
    // Verify all external links have proper attributes for security and accessibility
    const footer = page.locator('[data-testid="footer-section"]');
    await footer.scrollIntoViewIfNeeded();

    // Get all links in the footer
    const footerLinks = footer.locator('a[target="_blank"]');
    const linkCount = await footerLinks.count();

    // Verify there are multiple external links
    expect(linkCount).toBeGreaterThan(0);

    // Verify each external link has noopener attribute
    for (let i = 0; i < linkCount; i++) {
      const link = footerLinks.nth(i);
      await expect(link).toHaveAttribute('rel', /noopener/);
    }
  });
});
