/**
 * External Links Navigation E2E Tests
 * Owner: Scenario 4 - External Links Navigation
 *
 * Tests for documentation and source code repository links
 * in header, footer, and body sections.
 */

import { test, expect } from '@playwright/test';

// Constants for external URLs
const GITHUB_URL = 'https://github.com/mirdb/mirdb';
const DOCS_URL = 'https://mirdb.dev/docs';

test.describe('External Links Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: documentation link exists on homepage', async ({ page }) => {
    // Find link element containing 'documentation' or 'docs' text/aria
    const docsLink = page.locator('a').filter({
      has: page.locator('text=/documentation|docs/i')
    }).or(
      page.locator('a[aria-label*="documentation" i], a[aria-label*="docs" i]')
    ).first();

    await expect(docsLink).toBeVisible();

    // Verify the href attribute exists and contains a valid URL
    const href = await docsLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toMatch(/^https?:\/\//);
  });

  test('TC2: clicking documentation link navigates correctly', async ({ page, context }) => {
    // Find documentation link
    const docsLink = page.locator('a:has-text("Documentation")').first();
    await expect(docsLink).toBeVisible();

    // Get the href to verify navigation target
    const href = await docsLink.getAttribute('href');
    expect(href).toBe(DOCS_URL);

    // Since it opens in new tab (target="_blank"), verify the attribute
    const target = await docsLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify rel attribute for security
    const rel = await docsLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('TC3: source code link exists in footer', async ({ page }) => {
    // Navigate to footer section
    const footer = page.locator('#footer, footer');
    await expect(footer).toBeVisible();

    // Find source code link in footer
    const sourceCodeLink = footer.locator('a').filter({
      has: page.locator('text=/source|code|github|repository/i')
    }).first();

    await expect(sourceCodeLink).toBeVisible();

    // Verify the link has a valid href
    const href = await sourceCodeLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toMatch(/^https?:\/\//);
  });

  test('TC4: clicking source code link navigates to repository', async ({ page }) => {
    // Find source code link in footer
    const footer = page.locator('#footer, footer');
    const sourceCodeLink = footer.locator('a:has-text("Source Code"), a:has-text("GitHub")').first();

    await expect(sourceCodeLink).toBeVisible();

    // Get the href to verify it points to a valid repository
    const href = await sourceCodeLink.getAttribute('href');
    expect(href).toBe(GITHUB_URL);

    // Verify it opens in new tab
    const target = await sourceCodeLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify rel attribute for security
    const rel = await sourceCodeLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('header contains GitHub navigation link', async ({ page }) => {
    // Find header navigation
    const header = page.locator('#header, header');
    await expect(header).toBeVisible();

    // Find GitHub link in header nav
    const githubLink = header.locator('a:has-text("GitHub")').first();
    await expect(githubLink).toBeVisible();

    // Verify it points to the repository
    const href = await githubLink.getAttribute('href');
    expect(href).toBe(GITHUB_URL);

    // Verify it opens in new tab
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');
  });

  test('all external links have secure attributes', async ({ page }) => {
    // Find all external links (starting with http/https)
    const externalLinks = page.locator('a[href^="http"]');
    const count = await externalLinks.count();

    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const href = await link.getAttribute('href');
      const target = await link.getAttribute('target');
      const rel = await link.getAttribute('rel');

      // External links should open in new tab and have noopener
      if (target === '_blank') {
        expect(rel).toContain('noopener');
      }

      // Verify href is a valid URL
      expect(href).toMatch(/^https?:\/\/.+/);
    }
  });

  test('footer contains both documentation and source code links', async ({ page }) => {
    const footer = page.locator('#footer, footer');
    await expect(footer).toBeVisible();

    // Verify documentation link exists
    const docsLink = footer.locator('a:has-text("Documentation")');
    await expect(docsLink).toBeVisible();

    // Verify source code link exists
    const sourceLink = footer.locator('a:has-text("Source Code"), a:has-text("GitHub")').first();
    await expect(sourceLink).toBeVisible();
  });
});
