/**
 * Navigation Links Tests
 * Owner: Scenario 4 - Navigation Links
 *
 * Test cases:
 * - GitHub link in header navigation
 * - View on GitHub button in hero
 * - Features navigation link
 * - Docs navigation link
 * - All links are valid and clickable
 * - Smooth scrolling for anchor links
 */

import { test, expect } from '@playwright/test';

test.describe('Navigation Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC1: GitHub link exists in header navigation', async ({ page }) => {
    // Find the navigation element
    const nav = page.locator('nav.site-nav');
    await expect(nav).toBeVisible();

    // Find GitHub link in navigation
    const githubLink = nav.locator('a[href*="github.com/yetone/mirdb"]').first();
    await expect(githubLink).toBeVisible();

    // Verify the href is correct
    const href = await githubLink.getAttribute('href');
    expect(href).toContain('github.com/yetone/mirdb');
  });

  test('TC2: View on GitHub button in hero section links correctly', async ({ page }) => {
    // Find the hero CTA section
    const heroCta = page.locator('.hero-cta');
    await expect(heroCta).toBeVisible();

    // Find the View on GitHub button
    const githubBtn = heroCta.locator('a:has-text("View on GitHub")');
    await expect(githubBtn).toBeVisible();

    // Verify the href is the exact GitHub URL
    const href = await githubBtn.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Verify it opens in a new tab
    const target = await githubBtn.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify rel attribute for security
    const rel = await githubBtn.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('TC3: Features navigation link exists', async ({ page }) => {
    // Find the navigation links
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Find Features link
    const featuresLink = navLinks.locator('a[href="#features"]');
    await expect(featuresLink).toBeVisible();

    // Verify the text
    await expect(featuresLink).toContainText('Features');
  });

  test('TC4: Docs navigation link exists', async ({ page }) => {
    // Find the navigation links
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Find Docs link (looking for link with "Docs" text)
    const docsLink = navLinks.locator('a.nav-link-docs, a:has-text("Docs")').first();
    await expect(docsLink).toBeVisible();

    // Verify it has an href attribute
    const href = await docsLink.getAttribute('href');
    expect(href).toBeTruthy();

    // For now, docs points to GitHub readme
    expect(href).toContain('github.com/yetone/mirdb');
  });

  test('TC5: All navigation links are clickable with valid href attributes', async ({ page }) => {
    // Get all navigation links
    const navLinks = page.locator('.nav-links a');
    const linkCount = await navLinks.count();

    expect(linkCount).toBeGreaterThan(0);

    for (let i = 0; i < linkCount; i++) {
      const link = navLinks.nth(i);
      await expect(link).toBeVisible();

      // Verify each link has a valid href
      const href = await link.getAttribute('href');
      expect(href).toBeTruthy();
      expect(href).not.toBe('');
      expect(href).not.toBe('#');

      // Verify link is enabled/clickable
      const isDisabled = await link.getAttribute('disabled');
      expect(isDisabled).toBeNull();
    }
  });

  test('TC6: Smooth scrolling works for anchor links', async ({ page }) => {
    // Get the initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Find the Features anchor link
    const featuresLink = page.locator('.nav-links a[href="#features"]');
    await expect(featuresLink).toBeVisible();

    // Click the features link
    await featuresLink.click();

    // Wait for scroll animation to complete
    await page.waitForTimeout(500);

    // Get the new scroll position
    const newScrollY = await page.evaluate(() => window.scrollY);

    // Verify that the page scrolled (the scroll position changed)
    expect(newScrollY).toBeGreaterThan(initialScrollY);

    // Verify the URL hash was updated
    const url = page.url();
    expect(url).toContain('#features');

    // Verify the features section is in view
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  test('Navigation brand logo links to home', async ({ page }) => {
    const navBrand = page.locator('.nav-brand');
    await expect(navBrand).toBeVisible();

    // Verify logo image is present
    const logo = navBrand.locator('img.nav-logo');
    await expect(logo).toBeVisible();

    // Verify brand name text
    const brandName = navBrand.locator('.nav-brand-name');
    await expect(brandName).toContainText('MirDB');

    // Verify link href
    const href = await navBrand.getAttribute('href');
    expect(href === '#' || href === '/' || href === '').toBeTruthy();
  });

  test('Getting Started navigation link exists and works', async ({ page }) => {
    // Find the Getting Started link
    const gettingStartedLink = page.locator('.nav-links a[href="#getting-started"]');
    await expect(gettingStartedLink).toBeVisible();

    // Click and verify scroll
    await gettingStartedLink.click();
    await page.waitForTimeout(500);

    // Verify the section is in view
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeInViewport();
  });

  test('External links open in new tab with proper security attributes', async ({ page }) => {
    // Find external links (links with target="_blank")
    const externalLinks = page.locator('.nav-links a[target="_blank"]');
    const linkCount = await externalLinks.count();

    expect(linkCount).toBeGreaterThan(0);

    for (let i = 0; i < linkCount; i++) {
      const link = externalLinks.nth(i);

      // Verify rel attribute contains noopener for security
      const rel = await link.getAttribute('rel');
      expect(rel).toContain('noopener');
    }
  });
});
