/**
 * MirDB Landing Page - Footer Section E2E Tests
 * Owner: Scenario 7 - Footer Section
 *
 * Tests verify:
 * - Footer displays GitHub repository link
 * - GitHub link has proper security attributes
 * - License information is displayed
 * - CircleCI status badge is present
 * - Author/project attribution is included
 */

const { test, expect } = require('@playwright/test');
const { setupPage } = require('../helpers/test-utils');

test.describe('Footer Section', () => {
  test.beforeEach(async ({ page }) => {
    await setupPage(page);
  });

  // Test Case 1: Check footer for GitHub repository link
  test('TC1: Footer contains GitHub repository link', async ({ page }) => {
    // Scroll to footer to ensure visibility
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Find the GitHub link
    const githubLink = footer.locator('a[href="https://github.com/yetone/mirdb"]');
    await expect(githubLink).toBeVisible();

    // Verify the link href is correct
    const href = await githubLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');
  });

  // Test Case 2: Verify GitHub link has proper security attributes
  test('TC2: GitHub link has rel="noopener noreferrer" for security', async ({ page }) => {
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();

    // Find the GitHub link
    const githubLink = footer.locator('a[href="https://github.com/yetone/mirdb"]');
    await expect(githubLink).toBeVisible();

    // Verify rel attribute contains noopener and noreferrer
    const relAttr = await githubLink.getAttribute('rel');
    expect(relAttr).toBeTruthy();
    expect(relAttr).toContain('noopener');
    expect(relAttr).toContain('noreferrer');

    // Verify target="_blank" for external link
    const targetAttr = await githubLink.getAttribute('target');
    expect(targetAttr).toBe('_blank');
  });

  // Test Case 3: Check for license information
  test('TC3: License information is displayed in footer', async ({ page }) => {
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Get footer text content
    const footerText = await footer.textContent();

    // Check for license mention (MIT or Apache-2.0)
    const hasLicenseInfo = footerText.includes('MIT') ||
                           footerText.includes('Apache') ||
                           footerText.toLowerCase().includes('license');
    expect(hasLicenseInfo).toBe(true);
  });

  // Test Case 4: Check for CircleCI badge
  test('TC4: CircleCI status badge is present and loads correctly', async ({ page }) => {
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Find the CI badge image
    const ciBadge = footer.locator('.ci-badge, img[alt*="CircleCI"], img[alt*="CI"], img[alt*="Build Status"]');
    await expect(ciBadge).toBeVisible();

    // Verify the badge is an image
    const tagName = await ciBadge.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('img');

    // Verify the badge has a source
    const src = await ciBadge.getAttribute('src');
    expect(src).toBeTruthy();

    // Verify the src contains CircleCI reference
    expect(src.toLowerCase()).toContain('circleci');
  });

  // Test Case 5: Check for author attribution
  test('TC5: Author or project maintainer is credited', async ({ page }) => {
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Get footer text content
    const footerText = await footer.textContent();

    // Check for author/maintainer credit (yetone, MirDB, or copyright notice)
    const hasAttribution = footerText.includes('yetone') ||
                          footerText.includes('MirDB') ||
                          footerText.includes('©') ||
                          footerText.toLowerCase().includes('author') ||
                          footerText.toLowerCase().includes('maintainer');
    expect(hasAttribution).toBe(true);
  });

  // Additional test: Footer accessibility
  test('Footer has proper semantic structure', async ({ page }) => {
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Verify footer exists and is the proper HTML element
    const tagName = await footer.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('footer');

    // Verify footer is not empty
    const footerContent = await footer.textContent();
    expect(footerContent.trim().length).toBeGreaterThan(0);
  });

  // Additional test: All external links in footer have proper attributes
  test('All external links in footer have proper security attributes', async ({ page }) => {
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();

    // Find all external links in footer
    const externalLinks = footer.locator('a[href^="http"]');
    const linkCount = await externalLinks.count();

    // Check each external link has proper attributes
    for (let i = 0; i < linkCount; i++) {
      const link = externalLinks.nth(i);
      const href = await link.getAttribute('href');

      // External links should have target="_blank" and rel="noopener noreferrer"
      await expect(link).toHaveAttribute('target', '_blank');
      const rel = await link.getAttribute('rel');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    }
  });
});
