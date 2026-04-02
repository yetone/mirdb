import { test, expect } from '@playwright/test';

test.describe('Footer Links and Information', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('footer section is present at bottom of page', async ({ page }) => {
    // Check footer exists
    const footer = page.getByTestId('footer');
    await expect(footer).toBeVisible();
    await expect(footer).toHaveAttribute('role', 'contentinfo');
  });

  test('footer contains MirDB branding', async ({ page }) => {
    const footer = page.getByTestId('footer');
    await expect(footer.getByText('MirDB', { exact: true })).toBeVisible();
  });

  test('footer displays copyright notice', async ({ page }) => {
    const footer = page.getByTestId('footer');
    const currentYear = new Date().getFullYear();
    await expect(footer.getByText(new RegExp(`${currentYear}.*MirDB.*All rights reserved`))).toBeVisible();
  });

  test('GitHub link in footer is present and opens in new tab', async ({ page }) => {
    const githubLink = page.getByTestId('footer-github-link');

    // Verify link is visible
    await expect(githubLink).toBeVisible();

    // Verify link attributes
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    await expect(githubLink).toHaveAttribute('target', '_blank');
    await expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');

    // Verify link text
    await expect(githubLink).toContainText('GitHub');
  });

  test('License link in footer is present and opens in new tab', async ({ page }) => {
    const licenseLink = page.getByTestId('footer-license-link');

    // Verify link is visible
    await expect(licenseLink).toBeVisible();

    // Verify link attributes
    await expect(licenseLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb/blob/master/LICENSE');
    await expect(licenseLink).toHaveAttribute('target', '_blank');
    await expect(licenseLink).toHaveAttribute('rel', 'noopener noreferrer');

    // Verify link text
    await expect(licenseLink).toContainText('License');
  });

  test('Contact/Issues link in footer is present and opens in new tab', async ({ page }) => {
    const issuesLink = page.getByTestId('footer-issues-link');

    // Verify link is visible
    await expect(issuesLink).toBeVisible();

    // Verify link attributes
    await expect(issuesLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb/issues');
    await expect(issuesLink).toHaveAttribute('target', '_blank');
    await expect(issuesLink).toHaveAttribute('rel', 'noopener noreferrer');

    // Verify link text
    await expect(issuesLink).toContainText('Contact');
    await expect(issuesLink).toContainText('Issues');
  });

  test('all footer external links have target="_blank"', async ({ page }) => {
    const githubLink = page.getByTestId('footer-github-link');
    const licenseLink = page.getByTestId('footer-license-link');
    const issuesLink = page.getByTestId('footer-issues-link');

    // All external links should open in new tab
    await expect(githubLink).toHaveAttribute('target', '_blank');
    await expect(licenseLink).toHaveAttribute('target', '_blank');
    await expect(issuesLink).toHaveAttribute('target', '_blank');
  });

  test('footer has proper accessibility attributes', async ({ page }) => {
    // Footer role
    const footer = page.locator('footer[role="contentinfo"]');
    await expect(footer).toBeVisible();

    // Navigation with aria-label
    const nav = page.getByRole('navigation', { name: 'Footer navigation' });
    await expect(nav).toBeVisible();
  });

  test('footer is visible when scrolled to bottom', async ({ page }) => {
    // Scroll to bottom of page
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(500);

    // Footer should be in viewport
    const footer = page.getByTestId('footer');
    await expect(footer).toBeInViewport();
  });
});
