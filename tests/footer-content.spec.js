// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

/**
 * Footer Content Test Suite
 *
 * Verifies footer contains required information:
 * - GitHub repository link
 * - License information
 * - Copyright notice with year
 *
 * Scenario: Footer Content (ID: 18)
 * UUID: f5b28fc7-d448-4559-98bc-66573d04185a
 */

// Helper to get file URL
const getFileUrl = () => {
  return 'file://' + path.resolve(__dirname, '..', 'index.html');
};

test.describe('Footer Content', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(getFileUrl());
    await page.waitForLoadState('domcontentloaded');
  });

  test.describe('Footer Element Presence', () => {
    test('Footer element should exist at bottom of page', async ({ page }) => {
      // Verify footer element exists
      const footer = page.locator('body > footer');
      await expect(footer).toHaveCount(1);
      await expect(footer).toBeVisible();
    });
  });

  test.describe('TC1: GitHub Link', () => {
    test('should have GitHub repository link in footer', async ({ page }) => {
      const footer = page.locator('footer');

      // Check for GitHub link presence
      const githubLink = footer.locator('a[href*="github.com"]');
      await expect(githubLink).toHaveCount(1);
      await expect(githubLink).toBeVisible();
    });

    test('GitHub link should point to correct repository', async ({ page }) => {
      const footer = page.locator('footer');
      const githubLink = footer.locator('a[href*="github.com"]');

      // Verify the href attribute points to the MirDB repository
      const href = await githubLink.getAttribute('href');
      expect(href).toContain('github.com/yetone/mirdb');
    });

    test('GitHub link should have proper security attributes', async ({ page }) => {
      const footer = page.locator('footer');
      const githubLink = footer.locator('a[href*="github.com"]');

      // Check for target="_blank" for external link
      const target = await githubLink.getAttribute('target');
      expect(target).toBe('_blank');

      // Check for rel="noopener noreferrer" for security
      const rel = await githubLink.getAttribute('rel');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    });

    test('GitHub link should contain recognizable text', async ({ page }) => {
      const footer = page.locator('footer');
      const githubLink = footer.locator('a[href*="github.com"]');

      const linkText = await githubLink.textContent();
      expect(linkText?.toLowerCase()).toMatch(/github/i);
    });
  });

  test.describe('TC2: License Information', () => {
    test('should display license type in footer', async ({ page }) => {
      const footer = page.locator('footer');
      const footerText = await footer.textContent();

      // Check for license information (MIT)
      expect(footerText?.toLowerCase()).toMatch(/mit|license/i);
    });

    test('should have MIT license specifically mentioned', async ({ page }) => {
      const footer = page.locator('footer');
      const footerText = await footer.textContent();

      // The footer should mention MIT license
      expect(footerText).toContain('MIT');
    });

    test('license text should be visible', async ({ page }) => {
      const footer = page.locator('footer');

      // Find element containing license info
      const licenseElement = footer.locator('p:has-text("MIT"), span:has-text("MIT")');
      const count = await licenseElement.count();
      expect(count).toBeGreaterThanOrEqual(1);
    });
  });

  test.describe('TC3: Copyright Notice', () => {
    test('should have copyright notice in footer', async ({ page }) => {
      const footer = page.locator('footer');
      const footerText = await footer.textContent();

      // Check for copyright symbol or word
      expect(footerText).toMatch(/©|copyright/i);
    });

    test('should include year in copyright notice', async ({ page }) => {
      const footer = page.locator('footer');
      const footerText = await footer.textContent();

      // Check for a valid year (2020-2029 range)
      expect(footerText).toMatch(/20[2-9][0-9]/);
    });

    test('should include product name in copyright notice', async ({ page }) => {
      const footer = page.locator('footer');
      const footerText = await footer.textContent();

      // Check for MirDB in copyright context
      expect(footerText).toContain('MirDB');
    });

    test('copyright notice should be visible', async ({ page }) => {
      const footer = page.locator('footer');

      // Find element containing copyright symbol
      const copyrightElement = footer.locator('p:has-text("©"), p:has-text("copyright")');
      const count = await copyrightElement.count();
      expect(count).toBeGreaterThanOrEqual(1);
    });
  });

  test.describe('Footer Content Integration', () => {
    test('all required footer elements should be present together', async ({ page }) => {
      const footer = page.locator('footer');
      const footerText = await footer.textContent();

      // All three required elements should be present
      expect(footerText).toMatch(/github/i);
      expect(footerText).toMatch(/mit|license/i);
      expect(footerText).toMatch(/©.*20[2-9][0-9]|20[2-9][0-9].*©/i);
    });

    test('footer content should be readable (non-empty)', async ({ page }) => {
      const footer = page.locator('footer');
      const footerContent = footer.locator('.footer-content, div, p');

      const count = await footerContent.count();
      expect(count).toBeGreaterThanOrEqual(1);

      // Footer should have meaningful content length
      const footerText = await footer.textContent();
      expect(footerText?.trim().length).toBeGreaterThan(10);
    });
  });
});
