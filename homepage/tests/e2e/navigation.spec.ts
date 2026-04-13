/**
 * Navigation and Links E2E Tests
 * Owner: Scenario 4 - GitHub Repository Links
 *
 * Tests:
 * - GitHub links have correct href
 * - External links have security attributes
 * - Footer links work correctly
 */

import { test, expect } from '@playwright/test';

const GITHUB_URL = 'https://github.com/yetone/mirdb';

test.describe('GitHub Repository Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('GitHub link in hero section has correct href', async ({ page }) => {
    // Find the GitHub link in the hero section
    const heroSection = page.locator('#hero');
    const githubLink = heroSection.locator(`a[href="${GITHUB_URL}"]`);

    // Verify link exists with correct href
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toHaveAttribute('href', GITHUB_URL);
  });

  test('GitHub link has target="_blank" attribute', async ({ page }) => {
    // Find the GitHub link
    const githubLink = page.locator(`a[href="${GITHUB_URL}"]`).first();

    // Verify target attribute
    await expect(githubLink).toHaveAttribute('target', '_blank');
  });

  test('GitHub link has security attributes (noopener)', async ({ page }) => {
    // Find the GitHub link
    const githubLink = page.locator(`a[href="${GITHUB_URL}"]`).first();

    // Verify rel attribute contains 'noopener'
    const relValue = await githubLink.getAttribute('rel');
    expect(relValue).toBeTruthy();
    expect(relValue).toContain('noopener');
  });

  test('GitHub link has rel="noopener noreferrer" for complete security', async ({ page }) => {
    // Find the GitHub link
    const githubLink = page.locator(`a[href="${GITHUB_URL}"]`).first();

    // Verify full rel attribute value
    await expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  test('clicking GitHub link triggers navigation intent', async ({ page, context }) => {
    // Find the GitHub link
    const githubLink = page.locator(`a[href="${GITHUB_URL}"]`).first();

    // Listen for new page/tab opening
    const pagePromise = context.waitForEvent('page');

    // Click the link
    await githubLink.click();

    // Wait for new page and verify URL
    const newPage = await pagePromise;
    await newPage.waitForLoadState();

    // Verify the new page URL contains github.com/yetone/mirdb
    expect(newPage.url()).toContain('github.com/yetone/mirdb');
  });

  test('all external GitHub links have proper security attributes', async ({ page }) => {
    // Find all GitHub links on the page
    const githubLinks = page.locator(`a[href^="https://github.com/yetone/mirdb"]`);
    const count = await githubLinks.count();

    // Verify at least one GitHub link exists
    expect(count).toBeGreaterThan(0);

    // Check each link has security attributes
    for (let i = 0; i < count; i++) {
      const link = githubLinks.nth(i);
      await expect(link).toHaveAttribute('target', '_blank');

      const relValue = await link.getAttribute('rel');
      expect(relValue).toBeTruthy();
      expect(relValue).toContain('noopener');
    }
  });

  test('GitHub link text indicates repository destination', async ({ page }) => {
    // Find the primary GitHub CTA in hero
    const heroSection = page.locator('#hero');
    const githubLink = heroSection.locator(`a[href="${GITHUB_URL}"]`);

    // Verify link has text indicating GitHub/repository
    const text = await githubLink.textContent();
    expect(text?.toLowerCase()).toMatch(/github|repository|source|code/i);
  });

  test('GitHub link is accessible via keyboard', async ({ page }) => {
    // Navigate to the page
    await page.goto('/');

    // Tab through the page to reach the GitHub link
    await page.keyboard.press('Tab'); // Skip link
    await page.keyboard.press('Tab'); // GitHub link in hero

    // Get the focused element
    const focusedElement = page.locator(':focus');

    // Verify the focused element is or contains the GitHub link
    const href = await focusedElement.getAttribute('href');
    expect(href).toBe(GITHUB_URL);
  });
});

/**
 * Footer E2E Tests
 * Owner: Scenario 17 - Footer and Supplementary Content
 *
 * Tests:
 * - Footer element exists at page bottom
 * - Footer contains GitHub link
 * - Footer contains license information
 * - Footer contains copyright/attribution
 */
test.describe('Footer and Supplementary Content', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('footer element exists at page bottom', async ({ page }) => {
    // Find the footer element
    const footer = page.locator('footer#footer');
    await expect(footer).toBeVisible();

    // Verify it has the contentinfo role for accessibility
    await expect(footer).toHaveAttribute('role', 'contentinfo');
  });

  test('footer contains GitHub repository link', async ({ page }) => {
    const footer = page.locator('footer#footer');

    // Find GitHub link in footer
    const githubLink = footer.locator(`a[href="${GITHUB_URL}"]`);
    await expect(githubLink).toBeVisible();

    // Verify link text indicates GitHub
    const text = await githubLink.textContent();
    expect(text?.toLowerCase()).toMatch(/github/i);
  });

  test('footer GitHub link has security attributes', async ({ page }) => {
    const footer = page.locator('footer#footer');
    const githubLink = footer.locator(`a[href="${GITHUB_URL}"]`);

    await expect(githubLink).toHaveAttribute('target', '_blank');
    await expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  test('footer contains license information', async ({ page }) => {
    const footer = page.locator('footer#footer');

    // Check for license link or text
    const licenseLink = footer.locator('a:has-text("License")');
    await expect(licenseLink).toBeVisible();

    // Verify license link points to repository LICENSE file
    const href = await licenseLink.getAttribute('href');
    expect(href).toContain('LICENSE');
  });

  test('footer contains copyright or attribution', async ({ page }) => {
    const footer = page.locator('footer#footer');

    // Check for copyright symbol or text
    const footerText = await footer.textContent();
    expect(footerText).toMatch(/©|copyright|mirdb/i);
  });

  test('footer links are keyboard accessible', async ({ page }) => {
    // Scroll to footer first
    await page.locator('footer#footer').scrollIntoViewIfNeeded();

    // Find footer links
    const footer = page.locator('footer#footer');
    const links = footer.locator('a');
    const linkCount = await links.count();

    // Verify there are links in the footer
    expect(linkCount).toBeGreaterThan(0);

    // Check each link is focusable
    for (let i = 0; i < linkCount; i++) {
      const link = links.nth(i);
      await link.focus();
      await expect(link).toBeFocused();
    }
  });
});
