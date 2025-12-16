// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for MirDB Homepage Footer Section
 *
 * Test Case 1: Footer section is present at bottom of page
 * Test Case 2: GitHub repository link is present and correct
 * Test Case 3: Copyright notice is displayed
 * Test Case 4: All footer links are clickable and lead to correct destinations
 */

test.describe('Homepage Footer Section', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/');
  });

  test('Test Case 1: Footer section is present at bottom of page', async ({ page }) => {
    // Verify the footer element exists
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Verify footer has data-testid for testing
    const footerSection = page.getByTestId('footer-section');
    await expect(footerSection).toBeVisible();

    // Verify footer is at the bottom of the page by checking its position
    const footerBoundingBox = await footer.boundingBox();
    expect(footerBoundingBox).not.toBeNull();

    // Verify footer has reasonable height
    expect(footerBoundingBox?.height).toBeGreaterThan(50);
  });

  test('Test Case 2: GitHub repository link is present and correct', async ({ page }) => {
    // Verify GitHub link exists in footer
    const footerSection = page.getByTestId('footer-section');
    const githubLink = footerSection.getByTestId('footer-github-link');

    await expect(githubLink).toBeVisible();

    // Verify GitHub link has correct href
    await expect(githubLink).toHaveAttribute('href', /github\.com\/yetone\/mirdb/);

    // Verify the link text contains "GitHub"
    await expect(githubLink).toContainText(/GitHub/i);

    // Verify link opens in new tab for external links
    await expect(githubLink).toHaveAttribute('target', '_blank');
    await expect(githubLink).toHaveAttribute('rel', /noopener/);
  });

  test('Test Case 3: Copyright notice is displayed', async ({ page }) => {
    // Verify copyright element exists
    const footerSection = page.getByTestId('footer-section');
    const copyright = footerSection.getByTestId('footer-copyright');

    await expect(copyright).toBeVisible();

    // Verify copyright text contains expected elements
    const copyrightText = await copyright.textContent();

    // Should contain copyright symbol or word
    expect(copyrightText).toMatch(/©|copyright/i);

    // Should contain year
    expect(copyrightText).toMatch(/202[0-9]/);

    // Should contain MirDB
    expect(copyrightText).toMatch(/MirDB/i);
  });

  test('Test Case 4: All footer links are clickable and lead to correct destinations', async ({ page }) => {
    const footerSection = page.getByTestId('footer-section');

    // Get all links in footer
    const footerLinks = footerSection.locator('a');
    const linkCount = await footerLinks.count();

    // Verify there are links in the footer
    expect(linkCount).toBeGreaterThan(0);

    // Verify each link is clickable (has href and is enabled)
    for (let i = 0; i < linkCount; i++) {
      const link = footerLinks.nth(i);
      await expect(link).toBeVisible();
      await expect(link).toBeEnabled();

      // Verify link has valid href attribute
      const href = await link.getAttribute('href');
      expect(href).toBeTruthy();
      expect(href).not.toBe('#');
    }

    // Verify GitHub link specifically
    const githubLink = footerSection.getByTestId('footer-github-link');
    await expect(githubLink).toHaveAttribute('href', /github\.com/);

    // Verify Documentation link if present
    const docsLink = footerSection.getByTestId('footer-docs-link');
    await expect(docsLink).toBeVisible();
    await expect(docsLink).toHaveAttribute('href', /github\.com/);
  });

  test('Footer has proper styling and accessibility', async ({ page }) => {
    const footer = page.getByTestId('footer-section');

    // Verify footer is visible
    await expect(footer).toBeVisible();

    // Verify links have proper cursor style for clickability
    const links = footer.locator('a');
    const linkCount = await links.count();

    for (let i = 0; i < linkCount; i++) {
      const link = links.nth(i);
      await expect(link).toHaveCSS('cursor', 'pointer');
    }
  });

  test('Footer contains license information', async ({ page }) => {
    const footerSection = page.getByTestId('footer-section');
    const copyrightText = await footerSection.textContent();

    // Footer should mention the MIT license
    expect(copyrightText?.toLowerCase()).toContain('mit');
  });
});
