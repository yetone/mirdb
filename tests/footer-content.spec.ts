import { test, expect } from '@playwright/test';

/**
 * Footer Content Tests - Scenario 18
 *
 * These tests verify the footer contains required links and license information
 * as specified in REQ-6 (GitHub repository links) and the PRD footer requirements.
 */

test.describe('Footer Content', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: footer contains clickable link to GitHub repository', async ({ page }) => {
    // Test Case 1: Check footer for GitHub link
    // Expected: Footer contains clickable link to GitHub repository

    // Navigate to footer (scroll to page footer)
    const footer = page.locator('footer[data-testid="footer"]');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Verify GitHub link exists in footer
    const githubLink = page.locator('[data-testid="footer-github-link"]');
    await expect(githubLink).toBeVisible();

    // Verify link is clickable (enabled)
    await expect(githubLink).toBeEnabled();

    // Verify link points to GitHub repository
    const href = await githubLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Verify link opens in new tab for external link
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify security attributes for external link
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');

    // Verify link contains meaningful text
    const linkText = await githubLink.textContent();
    expect(linkText).toContain('GitHub');
  });

  test('TC2: footer displays license information', async ({ page }) => {
    // Test Case 2: Check footer for license text
    // Expected: Footer displays license information

    // Navigate to footer
    const footer = page.locator('footer[data-testid="footer"]');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Verify license information is displayed in footer
    const footerContent = await footer.textContent();
    expect(footerContent).toBeTruthy();

    // Check for MIT License mention (as specified in PRD and open source project)
    expect(footerContent!.toLowerCase()).toContain('mit license');

    // Verify the license text is in a visible element
    const copyrightElement = page.locator('.footer-copyright');
    await expect(copyrightElement).toBeVisible();

    const copyrightText = await copyrightElement.textContent();
    expect(copyrightText).toContain('open source');
    expect(copyrightText).toContain('MIT License');
  });

  test('TC3: page has semantic footer element', async ({ page }) => {
    // Test Case 3: Verify footer element exists
    // Expected: Page has semantic <footer> element

    // Verify semantic footer element exists
    const footer = page.locator('footer');
    await expect(footer).toBeAttached();

    // Verify it's a proper footer element (not just a div with class)
    const tagName = await footer.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('footer');

    // Verify footer has proper test ID for accessibility/testing
    const footerWithTestId = page.locator('footer[data-testid="footer"]');
    await expect(footerWithTestId).toBeAttached();

    // Verify footer is at the bottom of the page (comes after main content)
    const main = page.locator('main');
    await expect(main).toBeAttached();

    // Get positions to verify footer is below main content
    const mainBox = await main.boundingBox();
    const footerBox = await footer.boundingBox();

    expect(mainBox).toBeTruthy();
    expect(footerBox).toBeTruthy();

    // Footer should be positioned below main content
    expect(footerBox!.y).toBeGreaterThanOrEqual(mainBox!.y + mainBox!.height - 1);
  });

  test('footer structure and accessibility', async ({ page }) => {
    // Additional test for footer structure and accessibility
    const footer = page.locator('footer[data-testid="footer"]');
    await footer.scrollIntoViewIfNeeded();

    // Verify footer container exists
    const footerContainer = footer.locator('.footer-container');
    await expect(footerContainer).toBeVisible();

    // Verify footer links section exists
    const footerLinks = footer.locator('.footer-links');
    await expect(footerLinks).toBeVisible();

    // Verify GitHub icon SVG is present
    const githubIcon = footer.locator('[data-testid="footer-github-link"] svg');
    await expect(githubIcon).toBeAttached();

    // Verify footer has proper styling (background should be visible)
    const footerVisible = await footer.isVisible();
    expect(footerVisible).toBe(true);
  });

  test('footer GitHub link contains repository text', async ({ page }) => {
    // Verify the GitHub link has appropriate label text
    const githubLink = page.locator('[data-testid="footer-github-link"]');
    await expect(githubLink).toBeVisible();

    const linkText = await githubLink.textContent();
    expect(linkText).toContain('Repository');
  });
});
