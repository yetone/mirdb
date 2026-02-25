/**
 * Documentation and External Links E2E Tests
 * Owner: Scenario 7 - Documentation and External Links
 *
 * Test cases:
 * - GitHub repository link is present and valid
 * - Rust docs link is present
 * - Links open in new tab (target='_blank')
 * - License information is displayed
 */

const { test, expect } = require('@playwright/test');

test.describe('Documentation and External Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('GitHub repository link is present', async ({ page }) => {
    // Test Case 1: Check for GitHub repository link
    // Expected: Link to GitHub repository (github.com/yetone/mirdb) is present

    // Check in the links section
    const linksSection = page.locator('#links');
    await expect(linksSection).toBeVisible();

    // Find the GitHub link card
    const githubLink = linksSection.locator('a[data-link="github"]');
    await expect(githubLink).toBeVisible();

    // Verify the href contains the correct GitHub URL
    const href = await githubLink.getAttribute('href');
    expect(href).toContain('github.com/yetone/mirdb');

    // Verify the link title is correct
    const linkTitle = githubLink.locator('.link-card__title');
    await expect(linkTitle).toHaveText('GitHub Repository');
  });

  test('Rust docs link is present', async ({ page }) => {
    // Test Case 2: Check for Rust docs link
    // Expected: Link to docs.rs or Rust documentation is present

    const linksSection = page.locator('#links');
    await expect(linksSection).toBeVisible();

    // Find the docs link card
    const docsLink = linksSection.locator('a[data-link="docs"]');
    await expect(docsLink).toBeVisible();

    // Verify the href contains docs.rs
    const href = await docsLink.getAttribute('href');
    expect(href).toContain('docs.rs');

    // Verify the link title mentions Rust Documentation
    const linkTitle = docsLink.locator('.link-card__title');
    await expect(linkTitle).toHaveText('Rust Documentation');
  });

  test('GitHub link opens in new tab', async ({ page }) => {
    // Test Case 3: Click GitHub link
    // Expected: Link opens in new tab (target='_blank') and navigates to valid GitHub page

    const linksSection = page.locator('#links');
    const githubLink = linksSection.locator('a[data-link="github"]');

    // Verify target="_blank" attribute
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify rel="noopener noreferrer" for security
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');

    // Verify href is correct
    const href = await githubLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');
  });

  test('Rust docs link opens in new tab', async ({ page }) => {
    // Verify docs link also opens in new tab
    const linksSection = page.locator('#links');
    const docsLink = linksSection.locator('a[data-link="docs"]');

    const target = await docsLink.getAttribute('target');
    expect(target).toBe('_blank');

    const rel = await docsLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('License information is displayed', async ({ page }) => {
    // Test Case 4: Verify license information displayed
    // Expected: Open-source license type is mentioned (e.g., MIT, Apache)

    // Check in the links section license box
    const licenseBox = page.locator('.links-license');
    await expect(licenseBox).toBeVisible();

    const licenseText = await licenseBox.textContent();
    expect(licenseText.toLowerCase()).toContain('mit');

    // Also verify it mentions "open source"
    expect(licenseText.toLowerCase()).toContain('open source');
  });

  test('Footer contains GitHub and Docs links', async ({ page }) => {
    // Additional test: Verify footer also has documentation links
    const footer = page.locator('footer.site-footer');
    await expect(footer).toBeVisible();

    // GitHub link in footer
    const footerGithub = footer.locator('a[data-link="github"]');
    await expect(footerGithub).toBeVisible();

    const githubHref = await footerGithub.getAttribute('href');
    expect(githubHref).toContain('github.com/yetone/mirdb');
    expect(await footerGithub.getAttribute('target')).toBe('_blank');

    // Docs link in footer
    const footerDocs = footer.locator('a[data-link="docs"]');
    await expect(footerDocs).toBeVisible();

    const docsHref = await footerDocs.getAttribute('href');
    expect(docsHref).toContain('docs.rs');
    expect(await footerDocs.getAttribute('target')).toBe('_blank');
  });

  test('Footer displays MIT License information', async ({ page }) => {
    // Verify footer also mentions the license
    const footer = page.locator('footer.site-footer');
    const copyrightText = await footer.locator('.footer-copyright').textContent();

    expect(copyrightText.toLowerCase()).toContain('mit license');
  });

  test('Links section has proper accessibility', async ({ page }) => {
    // Check accessibility attributes
    const linksSection = page.locator('#links');

    // Section has proper aria-labelledby
    const ariaLabelledBy = await linksSection.getAttribute('aria-labelledby');
    expect(ariaLabelledBy).toBe('links-title');

    // Title exists and is visible
    const title = page.locator('#links-title');
    await expect(title).toBeVisible();
    await expect(title).toHaveText('Resources');
  });

  test('Link cards have all required elements', async ({ page }) => {
    const linksSection = page.locator('#links');
    const linkCards = linksSection.locator('.link-card');

    // Should have 3 link cards
    await expect(linkCards).toHaveCount(3);

    // Each card should have icon, title, description
    for (let i = 0; i < 3; i++) {
      const card = linkCards.nth(i);
      await expect(card.locator('.link-card__icon')).toBeVisible();
      await expect(card.locator('.link-card__title')).toBeVisible();
      await expect(card.locator('.link-card__description')).toBeVisible();
    }
  });
});
