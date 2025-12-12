import { test, expect } from '@playwright/test';

/**
 * Footer Content and Links E2E Tests
 *
 * This test suite verifies that the footer contains appropriate links
 * and attribution according to the PRD requirements.
 *
 * REQ-8: Link to GitHub repository for source code access
 * PRD: Footer with links and attribution (Homepage Structure)
 */

test.describe('Footer Content and Links', () => {

  /**
   * Test Case 1: Footer is visible at bottom of page
   * Input: Scroll to page footer
   * Expected: Footer is visible at bottom of page
   */
  test('should display footer at the bottom of the page', async ({ page }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto('/');

    // Scroll to the bottom of the page
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(300);

    // Verify footer is visible
    const footer = page.locator('[data-testid="main-footer"]');
    await expect(footer).toBeVisible();
    await expect(footer).toBeInViewport();
  });

  /**
   * Test Case 2: Footer contains GitHub link
   * Input: Check footer for GitHub link
   * Expected: Footer contains link to MirDB GitHub repository
   */
  test('should contain a link to MirDB GitHub repository in footer', async ({ page }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto('/');

    // Scroll to footer
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(300);

    // Verify footer contains GitHub link
    const footer = page.locator('[data-testid="main-footer"]');
    const githubLink = footer.locator('[data-testid="footer-github-link"]');

    await expect(githubLink).toBeVisible();

    // Verify href points to GitHub MirDB repository
    const href = await githubLink.getAttribute('href');
    expect(href).toContain('github.com');
    expect(href?.toLowerCase()).toContain('mirdb');

    // Verify it opens in new tab for security
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify rel attribute for security
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  /**
   * Test Case 3: Footer contains attribution/copyright
   * Input: Check footer attribution
   * Expected: Footer contains appropriate copyright or project attribution
   */
  test('should contain appropriate project attribution in footer', async ({ page }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto('/');

    // Scroll to footer
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(300);

    // Verify footer contains attribution
    const footer = page.locator('[data-testid="main-footer"]');
    const attribution = footer.locator('[data-testid="footer-attribution"]');

    await expect(attribution).toBeVisible();

    // Verify attribution text contains project name or copyright info
    const attributionText = await attribution.textContent();
    expect(attributionText?.toLowerCase()).toMatch(/mirdb|copyright|©|\d{4}/);
  });

  /**
   * Additional Test: Footer is accessible on mobile viewport
   */
  test('should display footer correctly on mobile viewport', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    // Scroll to the bottom of the page
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(300);

    // Verify footer is visible on mobile
    const footer = page.locator('[data-testid="main-footer"]');
    await expect(footer).toBeVisible();

    // Verify GitHub link is accessible on mobile
    const githubLink = footer.locator('[data-testid="footer-github-link"]');
    await expect(githubLink).toBeVisible();

    // Verify attribution is visible on mobile
    const attribution = footer.locator('[data-testid="footer-attribution"]');
    await expect(attribution).toBeVisible();
  });

  /**
   * Additional Test: Footer has semantic HTML structure
   */
  test('should use semantic footer HTML element', async ({ page }) => {
    await page.goto('/');

    // Verify footer uses semantic <footer> element
    const footer = page.locator('footer[data-testid="main-footer"]');
    await expect(footer).toHaveCount(1);
  });

  /**
   * Additional Test: Footer links are keyboard accessible
   */
  test('should have keyboard accessible links in footer', async ({ page }) => {
    await page.goto('/');

    // Scroll to footer
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(300);

    // Verify GitHub link is focusable
    const githubLink = page.locator('[data-testid="footer-github-link"]');
    await githubLink.focus();
    await expect(githubLink).toBeFocused();
  });
});
