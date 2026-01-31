/**
 * Footer Section E2E Tests
 * Owner: Scenario 7 - Footer Resources Section
 *
 * Tests:
 * - GitHub link presence and target
 * - Documentation link
 * - License information
 * - Semantic footer structure
 *
 * Requirements: REQ-7
 */

const { test, expect } = require('@playwright/test');

test.describe('Footer Resources Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Scroll to footer
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();
  });

  test('Test Case 1: Footer contains GitHub repository link', async ({ page }) => {
    const footer = page.locator('footer');
    const githubLink = footer.locator('a[href="https://github.com/yetone/mirdb"]');

    await expect(githubLink).toBeVisible();

    const href = await githubLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');
  });

  test('Test Case 2: Footer contains documentation link', async ({ page }) => {
    const footer = page.locator('footer');

    // Look for documentation link - could be linked to GitHub README or docs page
    const docsLink = footer.locator('[data-testid="footer-docs-link"]');
    await expect(docsLink).toBeVisible();

    const href = await docsLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href.length).toBeGreaterThan(0);
  });

  test('Test Case 3: Footer displays license information', async ({ page }) => {
    const footer = page.locator('footer');
    const footerText = await footer.textContent();

    // Check for license type (MIT, Apache-2.0, or dual license)
    const hasLicenseInfo =
      footerText.includes('MIT') ||
      footerText.includes('Apache-2.0') ||
      footerText.includes('Apache 2.0') ||
      footerText.toLowerCase().includes('license');

    expect(hasLicenseInfo).toBeTruthy();
  });

  test('Test Case 4: GitHub link opens in new tab with security attributes', async ({ page }) => {
    const footer = page.locator('footer');
    const githubLink = footer.locator('[data-testid="footer-github-link"]');

    await expect(githubLink).toBeVisible();

    // Verify target="_blank" for new tab
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify rel="noopener noreferrer" for security
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');
  });

  test('Test Case 5: Footer uses semantic HTML structure', async ({ page }) => {
    const footer = page.locator('footer');

    // Verify it's a footer element
    const tagName = await footer.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('footer');

    // Verify role="contentinfo" for accessibility
    const role = await footer.getAttribute('role');
    expect(role).toBe('contentinfo');
  });

  test('Footer contains links section with proper structure', async ({ page }) => {
    const footer = page.locator('footer');
    const linksSection = footer.locator('.footer-links');

    await expect(linksSection).toBeVisible();

    // Check that links are organized in the footer
    const linkCount = await linksSection.locator('a').count();
    expect(linkCount).toBeGreaterThanOrEqual(2);
  });

  test('Footer contains copyright information', async ({ page }) => {
    const footer = page.locator('footer');
    const footerText = await footer.textContent();

    // Check for copyright notice
    const hasCopyright =
      footerText.includes('©') ||
      footerText.toLowerCase().includes('copyright') ||
      footerText.includes('MirDB');

    expect(hasCopyright).toBeTruthy();
  });

  test('Documentation link has proper accessibility attributes', async ({ page }) => {
    const footer = page.locator('footer');
    const docsLink = footer.locator('[data-testid="footer-docs-link"]');

    await expect(docsLink).toBeVisible();

    // External links should have target="_blank" and rel="noopener noreferrer"
    const target = await docsLink.getAttribute('target');
    expect(target).toBe('_blank');

    const rel = await docsLink.getAttribute('rel');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');
  });
});
