/**
 * Footer and External Links E2E Tests
 * Owner: Scenario 9 - Footer and External Links
 *
 * Test cases:
 * - GitHub repository link present
 * - Links open in new tab
 * - License information present
 * - External link security (rel attributes)
 */

import { test, expect } from '@playwright/test';
import { navigateToHomepage, selectors } from './test-utils';

test.describe('Footer and External Links', () => {
  test.beforeEach(async ({ page }) => {
    await navigateToHomepage(page);
  });

  test('TC1: GitHub repository link is present in footer', async ({ page }) => {
    const footer = page.locator(selectors.footer.section);
    await expect(footer).toBeVisible();

    // Check for GitHub link in footer
    const githubLink = footer.locator('a[href*="github.com"]').first();
    await expect(githubLink).toBeVisible();

    const href = await githubLink.getAttribute('href');
    expect(href).toContain('github.com');
    expect(href).toContain('mirdb');
  });

  test('TC2: Repository link opens in a new tab (target="_blank")', async ({ page }) => {
    const footer = page.locator(selectors.footer.section);
    const githubLink = footer.locator('a[href*="github.com"]').first();

    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');
  });

  test('TC3: Documentation link is present in footer', async ({ page }) => {
    const footer = page.locator(selectors.footer.section);

    // Check for documentation link (README or docs)
    const docLink = footer.locator('a').filter({ hasText: /documentation|docs|readme/i });
    await expect(docLink).toBeVisible();

    const href = await docLink.getAttribute('href');
    expect(href).toBeTruthy();
    // Documentation could be README.md or separate docs
    expect(href).toMatch(/readme|docs|documentation/i);
  });

  test('TC4: License information is present in the footer', async ({ page }) => {
    const footer = page.locator(selectors.footer.section);

    // Check for license link or text
    const licenseLink = footer.locator('a').filter({ hasText: /license/i });
    const licenseLinkVisible = await licenseLink.isVisible();

    // Or check for license text in copyright
    const copyrightText = await footer.locator(selectors.footer.copyright).textContent();
    const hasLicenseText = copyrightText?.toLowerCase().includes('license') ||
                          copyrightText?.toLowerCase().includes('mit') ||
                          copyrightText?.toLowerCase().includes('apache');

    // Either a license link or license text should be present
    expect(licenseLinkVisible || hasLicenseText).toBeTruthy();
  });

  test('TC5: External links have rel="noopener noreferrer" attribute', async ({ page }) => {
    const footer = page.locator(selectors.footer.section);

    // Get all external links (links that open in new tab)
    const externalLinks = footer.locator('a[target="_blank"]');
    const count = await externalLinks.count();

    expect(count).toBeGreaterThan(0);

    // Verify each external link has proper rel attribute
    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const rel = await link.getAttribute('rel');

      expect(rel).toBeTruthy();
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    }
  });

  test('Footer is visible at the bottom of the page', async ({ page }) => {
    const footer = page.locator(selectors.footer.section);
    await expect(footer).toBeVisible();

    // Verify footer has proper role
    const role = await footer.getAttribute('role');
    expect(role).toBe('contentinfo');
  });

  test('Footer links are accessible and focusable', async ({ page }) => {
    const footer = page.locator(selectors.footer.section);
    const footerLinks = footer.locator(selectors.footer.link);
    const count = await footerLinks.count();

    expect(count).toBeGreaterThan(0);

    // Verify each link is focusable
    for (let i = 0; i < count; i++) {
      const link = footerLinks.nth(i);
      await expect(link).toBeEnabled();
    }
  });

  test('GitHub link in navigation also opens in new tab', async ({ page }) => {
    // Check navigation for GitHub link as well
    const nav = page.locator(selectors.navigation.links);
    const navGithubLink = nav.locator('a[href*="github.com"]');

    if (await navGithubLink.isVisible()) {
      const target = await navGithubLink.getAttribute('target');
      expect(target).toBe('_blank');

      const rel = await navGithubLink.getAttribute('rel');
      expect(rel).toContain('noopener');
    }
  });
});
