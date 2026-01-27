/**
 * Footer Links and Information E2E tests.
 * Owner: Scenario 13 - Footer Links and Information
 *
 * Tests for REQ-9: Footer with links to license, contributing guidelines, and community
 */

import { test, expect } from '@playwright/test';

test.describe('Footer Links and Information', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Load footer section - Footer is visible with all required links
  test('should display footer with all required links', async ({ page }) => {
    // Scroll to footer to ensure it's in view
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();

    // Verify footer is visible
    await expect(footer).toBeVisible();

    // Verify footer contains MirDB branding
    const footerBrand = footer.locator('.footer-logo, .footer-brand');
    await expect(footerBrand.first()).toBeVisible();

    // Verify footer contains required links
    const licenseLink = footer.locator('a[href*="LICENSE"]');
    const contributingLink = footer.locator('a[href*="CONTRIBUTING"]');
    const githubLink = footer.locator('a[href*="github.com/yetone/mirdb"]').first();

    await expect(licenseLink).toBeVisible();
    await expect(contributingLink).toBeVisible();
    await expect(githubLink).toBeVisible();
  });

  // Test Case 2: Click license link - User is directed to license information
  test('should have functional license link directing to license information', async ({ page }) => {
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();

    // Find the license link
    const licenseLink = footer.locator('a[href*="LICENSE"]');
    await expect(licenseLink).toBeVisible();

    // Get the href attribute
    const href = await licenseLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toContain('LICENSE');

    // Verify the link opens in new tab (security best practice for external links)
    const target = await licenseLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify rel attribute for security
    const rel = await licenseLink.getAttribute('rel');
    expect(rel).toContain('noopener');

    // Verify link text indicates license
    const linkText = await licenseLink.textContent();
    expect(linkText?.toLowerCase()).toContain('license');
  });

  // Test Case 3: Click contributing link - User is directed to contributing guidelines
  test('should have functional contributing link directing to contributing guidelines', async ({ page }) => {
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();

    // Find the contributing link
    const contributingLink = footer.locator('a[href*="CONTRIBUTING"]');
    await expect(contributingLink).toBeVisible();

    // Get the href attribute
    const href = await contributingLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toContain('CONTRIBUTING');

    // Verify the link opens in new tab
    const target = await contributingLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify rel attribute for security
    const rel = await contributingLink.getAttribute('rel');
    expect(rel).toContain('noopener');

    // Verify link text indicates contributing
    const linkText = await contributingLink.textContent();
    expect(linkText?.toLowerCase()).toContain('contribut');
  });

  // Test Case 4: Verify GitHub link in footer - GitHub repository link is present and functional
  test('should have functional GitHub repository link in footer', async ({ page }) => {
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();

    // Find the GitHub link (first one that points to the repository)
    const githubLink = footer.locator('a[href*="github.com/yetone/mirdb"]').first();
    await expect(githubLink).toBeVisible();

    // Get the href attribute
    const href = await githubLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Verify the link opens in new tab
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify rel attribute for security
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');

    // Verify link text indicates GitHub
    const linkText = await githubLink.textContent();
    expect(linkText?.toLowerCase()).toContain('github');
  });

  // Additional test: Footer displays MIT license information
  test('should display MIT license information', async ({ page }) => {
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();

    // Check for MIT license text mention
    const footerText = await footer.textContent();
    expect(footerText?.toLowerCase()).toContain('mit');
  });

  // Additional test: Footer has proper semantic structure
  test('should have proper semantic footer structure', async ({ page }) => {
    // Verify footer element exists
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Footer should have the footer class
    const footerClass = await footer.getAttribute('class');
    expect(footerClass).toContain('footer');

    // Footer should contain a container for proper layout
    const container = footer.locator('.container');
    await expect(container).toBeVisible();
  });

  // Additional test: All footer links are accessible
  test('should have accessible footer links with discernible text', async ({ page }) => {
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();

    // Get all links in the footer
    const footerLinks = footer.locator('a[href]');
    const linkCount = await footerLinks.count();

    expect(linkCount).toBeGreaterThanOrEqual(3); // GitHub, License, Contributing

    // Check each link has discernible text
    for (let i = 0; i < linkCount; i++) {
      const link = footerLinks.nth(i);
      const text = await link.textContent();
      const ariaLabel = await link.getAttribute('aria-label');

      const hasDiscernibleText = (text && text.trim() !== '') || (ariaLabel && ariaLabel.trim() !== '');
      expect(hasDiscernibleText).toBe(true);
    }
  });
});
