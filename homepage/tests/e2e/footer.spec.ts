/**
 * Footer E2E tests.
 * Owner: Scenario 5 - Footer Section
 *
 * Tests for:
 * - Footer link navigation (Privacy, Terms)
 * - Footer presence on homepage
 */

import { test, expect } from '@playwright/test';

test.describe('Footer', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('footer is visible on homepage', async ({ page }) => {
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();
  });

  test('clicking Privacy Policy link navigates to Privacy page', async ({ page }) => {
    const privacyLink = page.getByRole('link', { name: /privacy policy/i });
    await expect(privacyLink).toBeVisible();
    await privacyLink.click();
    await expect(page).toHaveURL('/privacy');
    await expect(page.getByRole('heading', { name: /privacy policy/i })).toBeVisible();
  });

  test('clicking Terms of Service link navigates to Terms page', async ({ page }) => {
    const termsLink = page.getByRole('link', { name: /terms of service/i });
    await expect(termsLink).toBeVisible();
    await termsLink.click();
    await expect(page).toHaveURL('/terms');
    await expect(page.getByRole('heading', { name: /terms of service/i })).toBeVisible();
  });

  test('social links are present with correct attributes', async ({ page }) => {
    const twitterLink = page.getByRole('link', { name: /twitter/i });
    const githubLink = page.getByRole('link', { name: /github/i });
    const linkedinLink = page.getByRole('link', { name: /linkedin/i });

    await expect(twitterLink).toBeVisible();
    await expect(githubLink).toBeVisible();
    await expect(linkedinLink).toBeVisible();

    await expect(twitterLink).toHaveAttribute('target', '_blank');
    await expect(githubLink).toHaveAttribute('target', '_blank');
    await expect(linkedinLink).toHaveAttribute('target', '_blank');
  });

  test('copyright notice displays current year', async ({ page }) => {
    const currentYear = new Date().getFullYear();
    const copyrightText = page.getByText(new RegExp(`© ${currentYear}`));
    await expect(copyrightText).toBeVisible();
  });
});
