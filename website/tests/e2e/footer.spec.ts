/**
 * Footer Section E2E Tests
 * Owner: Scenario 8 - Footer with GitHub Links
 *
 * Tests for:
 * - Footer section visibility
 * - GitHub repository link
 * - Additional links (issues, contributing)
 * - Copyright/license information
 */
import { test, expect } from '@playwright/test';
import { SELECTORS, CONTENT } from '../fixtures/test-data';

test.describe('Footer with GitHub Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Footer section is present at bottom of page', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator(SELECTORS.footer);
    await footer.scrollIntoViewIfNeeded();

    // Verify footer is visible
    await expect(footer).toBeVisible();
    await expect(footer).toHaveClass(/footer/);

    // Verify footer is at the end of the document (after main content)
    const main = page.locator('main');
    await expect(main).toBeVisible();

    // Footer should be after main in DOM order
    const footerBoundingBox = await footer.boundingBox();
    const mainBoundingBox = await main.boundingBox();
    expect(footerBoundingBox).not.toBeNull();
    expect(mainBoundingBox).not.toBeNull();
    if (footerBoundingBox && mainBoundingBox) {
      expect(footerBoundingBox.y).toBeGreaterThan(mainBoundingBox.y);
    }
  });

  test('TC2: GitHub repository link is present', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator(SELECTORS.footer);
    await footer.scrollIntoViewIfNeeded();

    // Find GitHub link
    const githubLink = page.locator('[data-testid="github-link"]');
    await expect(githubLink).toBeVisible();

    // Verify link points to GitHub repository
    const href = await githubLink.getAttribute('href');
    expect(href).toContain('github.com');
    expect(href).toContain('mirdb');

    // Verify link text mentions GitHub
    await expect(githubLink).toContainText(/GitHub/i);
  });

  test('TC3: GitHub link opens in new tab', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator(SELECTORS.footer);
    await footer.scrollIntoViewIfNeeded();

    // Find GitHub link
    const githubLink = page.locator('[data-testid="github-link"]');
    await expect(githubLink).toBeVisible();

    // Verify target="_blank" attribute for new tab
    await expect(githubLink).toHaveAttribute('target', '_blank');

    // Verify rel="noopener noreferrer" for security
    await expect(githubLink).toHaveAttribute('rel', /noopener/);

    // Verify href points to GitHub
    await expect(githubLink).toHaveAttribute('href', CONTENT.githubUrl);
  });

  test('TC4: Footer includes copyright or license information', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator(SELECTORS.footer);
    await footer.scrollIntoViewIfNeeded();

    // Find copyright element
    const copyright = page.locator('[data-testid="footer-copyright"]');
    await expect(copyright).toBeVisible();

    // Verify it contains copyright or license text
    const copyrightText = await copyright.textContent();
    expect(copyrightText).not.toBeNull();

    // Should contain copyright symbol or "License" or "MIT"
    const hasCopyrightInfo =
      copyrightText?.includes('Copyright') ||
      copyrightText?.includes('\u00A9') ||
      copyrightText?.includes('License') ||
      copyrightText?.includes('MIT');
    expect(hasCopyrightInfo).toBe(true);
  });

  test('Footer contains additional links (issues, contributing)', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator(SELECTORS.footer);
    await footer.scrollIntoViewIfNeeded();

    // Check for issues link
    const issuesLink = page.locator('[data-testid="issues-link"]');
    await expect(issuesLink).toBeVisible();
    await expect(issuesLink).toHaveAttribute('href', /issues/);
    await expect(issuesLink).toHaveAttribute('target', '_blank');

    // Check for contributing link
    const contributingLink = page.locator('[data-testid="contributing-link"]');
    await expect(contributingLink).toBeVisible();
    await expect(contributingLink).toHaveAttribute('href', /CONTRIBUTING/i);
    await expect(contributingLink).toHaveAttribute('target', '_blank');
  });

  test('Footer links have proper accessibility attributes', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator(SELECTORS.footer);
    await footer.scrollIntoViewIfNeeded();

    // Check footer nav has aria-label
    const footerNav = page.locator('.footer-links');
    await expect(footerNav).toHaveAttribute('aria-label', /footer/i);

    // All external links should have rel="noopener noreferrer"
    const externalLinks = page.locator('.footer-link');
    const linkCount = await externalLinks.count();
    expect(linkCount).toBeGreaterThan(0);

    for (let i = 0; i < linkCount; i++) {
      const link = externalLinks.nth(i);
      await expect(link).toHaveAttribute('rel', /noopener/);
    }
  });
});
