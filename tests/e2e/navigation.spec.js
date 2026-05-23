/**
 * E2E tests for Navigation and GitHub Links.
 * Owner: Scenario 7 - Navigation and GitHub Links
 *
 * Tests:
 * - Header nav element exists with role="navigation"
 * - Nav links exist for internal sections
 * - GitHub link exists in header, hero, and footer
 * - Clicking nav link updates URL hash and scrolls to section
 * - External links use target="_blank" and rel="noopener noreferrer"
 * - Hamburger menu works at 375px viewport
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

const homepagePath = 'file://' + path.resolve(__dirname, '../../index.html');

test.describe('Navigation and GitHub Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(homepagePath);
  });

  // Test Case 1: Query header nav element
  test('nav element exists with role="navigation" inside header', async ({ page }) => {
    const header = page.locator('header');
    await expect(header).toBeVisible();

    const nav = header.locator('nav[role="navigation"]');
    await expect(nav).toBeVisible();
  });

  // Test Case 2: Query nav links for internal sections
  test('nav links exist for #features, #quick-start, and #status', async ({ page }) => {
    const navLinks = page.locator('.nav-link[href^="#"]');
    const count = await navLinks.count();
    expect(count).toBeGreaterThanOrEqual(3);

    const hrefs = await navLinks.evaluateAll(links => links.map(l => l.getAttribute('href')));
    expect(hrefs).toContain('#features');
    expect(hrefs).toContain('#quick-start');
    expect(hrefs).toContain('#status');
  });

  // Test Case 3: Query GitHub link in header
  test('GitHub link exists in header/nav pointing to https://github.com/yetone/mirdb', async ({ page }) => {
    const githubLink = page.locator('[data-testid="github-link-header"]');
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
  });

  // Test Case 4: Query GitHub link in hero section
  test('GitHub link exists in hero section', async ({ page }) => {
    const githubLink = page.locator('[data-testid="github-link-hero"]');
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
  });

  // Test Case 5: Query GitHub link in footer
  test('GitHub link exists in footer', async ({ page }) => {
    const githubLink = page.locator('[data-testid="github-link-footer"]');
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
  });

  // Test Case 6: Click nav link and verify URL hash
  test('clicking nav link updates URL hash and scrolls to section', async ({ page }) => {
    // Click the Features link
    const featuresLink = page.locator('.nav-link[href="#features"]').first();
    await featuresLink.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500);

    // Verify URL hash
    const url = page.url();
    expect(url).toContain('#features');

    // Verify the features section is in view
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  // Test Case 7: Verify all external links use target='_blank' and rel='noopener noreferrer'
  test('all external GitHub links open in new tab with security attributes', async ({ page }) => {
    const githubLinks = page.locator('a[href="https://github.com/yetone/mirdb"]');
    const count = await githubLinks.count();
    expect(count).toBeGreaterThanOrEqual(3);

    for (let i = 0; i < count; i++) {
      const link = githubLinks.nth(i);
      await expect(link).toHaveAttribute('target', '_blank');
      await expect(link).toHaveAttribute('rel', 'noopener noreferrer');
    }
  });

  // Test Case 8: Test hamburger menu at 375px viewport
  test('hamburger menu toggles nav visibility at 375px viewport', async ({ page }) => {
    // Set viewport to mobile
    await page.setViewportSize({ width: 375, height: 667 });

    // Wait for layout to settle
    await page.waitForTimeout(200);

    // Hamburger button should be visible
    const hamburger = page.locator('.mobile-menu-toggle');
    await expect(hamburger).toBeVisible();

    // Nav menu should be hidden initially
    const navMenu = page.locator('#nav-menu');
    await expect(navMenu).not.toBeVisible();

    // Click hamburger to open
    await hamburger.click();
    await expect(navMenu).toBeVisible();

    // Click hamburger again to close
    await hamburger.click();
    await expect(navMenu).not.toBeVisible();
  });

  // Additional: Mobile menu closes when clicking a link
  test('mobile menu closes when clicking a nav link', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });
    await page.waitForTimeout(200);

    const hamburger = page.locator('.mobile-menu-toggle');
    const navMenu = page.locator('#nav-menu');

    // Open menu
    await hamburger.click();
    await expect(navMenu).toBeVisible();

    // Click a nav link
    const quickStartLink = page.locator('.nav-link[href="#quick-start"]').first();
    await quickStartLink.click();

    // Menu should close
    await expect(navMenu).not.toBeVisible();
  });

  // Additional: Site logo links to home
  test('site logo links to top of page', async ({ page }) => {
    const logo = page.locator('.site-logo');
    await expect(logo).toBeVisible();
    await expect(logo).toHaveAttribute('href', '#');
  });

  // Additional: Footer contains copyright
  test('footer contains copyright text', async ({ page }) => {
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    const copyright = footer.locator('.footer-copyright');
    await expect(copyright).toBeVisible();

    const text = await copyright.textContent();
    expect(text.toLowerCase()).toContain('mirdb');
  });
});
