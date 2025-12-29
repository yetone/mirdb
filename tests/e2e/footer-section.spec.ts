import { test, expect } from '@playwright/test';

/**
 * E2E Tests for MirDB Homepage Footer Section
 *
 * These tests verify that the footer section contains:
 * - Footer element presence at page bottom
 * - GitHub repository link
 * - License information
 * - Version/status information
 */

test.describe('Footer Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Footer element is present at bottom of page', async ({ page }) => {
    // Check that the footer element exists
    const footer = page.locator('footer, .footer, [data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Verify footer is at the bottom by checking it's the last major section
    const footerElement = page.locator('footer');
    await expect(footerElement).toBeVisible();

    // Check that footer has content
    const footerContent = await footerElement.textContent();
    expect(footerContent?.trim().length).toBeGreaterThan(0);
  });

  test('TC2: Footer contains link to GitHub repository', async ({ page }) => {
    // Find GitHub link in footer
    const footer = page.locator('footer, .footer');
    const githubLink = footer.locator('a').filter({ hasText: /GitHub/i });

    // Verify GitHub link exists and is visible
    await expect(githubLink).toBeVisible();

    // Verify the href points to GitHub
    const href = await githubLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href?.toLowerCase()).toContain('github');
  });

  test('TC3: Footer displays license information', async ({ page }) => {
    // Find footer element
    const footer = page.locator('footer, .footer');

    // Check for license information (could be MIT, Apache, or generic "License" text)
    const footerContent = await footer.textContent();

    // Look for common license patterns
    const hasLicenseInfo =
      footerContent?.toLowerCase().includes('license') ||
      footerContent?.toLowerCase().includes('mit') ||
      footerContent?.toLowerCase().includes('apache') ||
      footerContent?.toLowerCase().includes('bsd');

    expect(hasLicenseInfo).toBeTruthy();

    // Alternatively, check for a license link
    const licenseLink = footer.locator('a').filter({ hasText: /license/i });
    const licenseText = footer.locator('*').filter({ hasText: /license|mit|apache/i });

    // At least one of these should be present
    const hasLicenseLink = (await licenseLink.count()) > 0;
    const hasLicenseText = (await licenseText.count()) > 0;

    expect(hasLicenseLink || hasLicenseText).toBeTruthy();
  });

  test('TC4: Footer displays version or status information', async ({ page }) => {
    // Find footer element
    const footer = page.locator('footer, .footer');
    const footerContent = await footer.textContent();

    // Check for version/status patterns
    // Version could be: v1.0.0, version 1.0, Version: 1.0, etc.
    // Status could be: beta, alpha, stable, development, active, etc.
    const hasVersionInfo =
      /v\d+\.\d+/i.test(footerContent || '') ||
      /version/i.test(footerContent || '');

    const hasStatusInfo =
      /beta|alpha|stable|development|active|status/i.test(footerContent || '');

    // Either version or status should be present
    expect(hasVersionInfo || hasStatusInfo).toBeTruthy();

    // Also verify the element is visible
    const versionOrStatusElement = footer.locator('*').filter({
      hasText: /v\d|version|beta|alpha|stable|development|active|status/i
    }).first();

    await expect(versionOrStatusElement).toBeVisible();
  });
});
