/**
 * Navigation and GitHub Links Tests
 * Owner: Scenario 6 - Navigation and GitHub Links
 *
 * Test Cases:
 * 1. Navigation contains link to Features section
 * 2. Navigation contains link to Getting Started section
 * 3. Navigation contains GitHub link with recognizable icon
 * 4. Click Features navigation link scrolls to section
 * 5. Click Getting Started navigation link scrolls to section
 * 6. GitHub link URL is 'https://github.com/yetone/mirdb'
 * 7. GitHub links have target='_blank' and rel='noopener'
 * 8. At least 3 GitHub links exist: navbar, hero CTA, footer
 * 9. Footer contains GitHub link with star button or icon
 */

import { test, expect } from '@playwright/test';

test.describe('Navigation and GitHub Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Navigation contains link to Features section', async ({ page }) => {
    const nav = page.locator('nav');
    const featuresLink = nav.locator('a[href="#features"]');

    await expect(featuresLink).toBeVisible();
    await expect(featuresLink).toContainText('Features');
  });

  test('TC2: Navigation contains link to Getting Started section', async ({ page }) => {
    const nav = page.locator('nav');
    const gettingStartedLink = nav.locator('a[href="#getting-started"]');

    await expect(gettingStartedLink).toBeVisible();
    await expect(gettingStartedLink).toContainText('Getting Started');
  });

  test('TC3: Navigation contains GitHub link with recognizable icon', async ({ page }) => {
    const nav = page.locator('nav');
    const githubLink = nav.locator('a[href*="github.com"]');

    await expect(githubLink).toBeVisible();

    // Check for GitHub icon (SVG)
    const githubIcon = githubLink.locator('svg');
    await expect(githubIcon).toBeVisible();
  });

  test('TC4: Click Features navigation link scrolls to Features section', async ({ page }) => {
    const nav = page.locator('nav');
    const featuresLink = nav.locator('a[href="#features"]');
    const featuresSection = page.locator('#features');

    // Click the Features link
    await featuresLink.click();

    // Wait for smooth scroll animation
    await page.waitForTimeout(500);

    // Verify the features section is in viewport
    await expect(featuresSection).toBeInViewport();
  });

  test('TC5: Click Getting Started navigation link scrolls to Getting Started section', async ({ page }) => {
    const nav = page.locator('nav');
    const gettingStartedLink = nav.locator('a[href="#getting-started"]');
    const gettingStartedSection = page.locator('#getting-started');

    // Click the Getting Started link
    await gettingStartedLink.click();

    // Wait for smooth scroll animation
    await page.waitForTimeout(500);

    // Verify the getting started section is in viewport
    await expect(gettingStartedSection).toBeInViewport();
  });

  test('TC6: Verify GitHub link URL in navbar is correct', async ({ page }) => {
    const nav = page.locator('nav');
    const githubLink = nav.locator('a[href*="github.com"]');

    const href = await githubLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');
  });

  test('TC7: Verify GitHub links open in new tab with noopener', async ({ page }) => {
    // Get all GitHub links on the page
    const githubLinks = page.locator('a[href*="github.com"]');
    const count = await githubLinks.count();

    expect(count).toBeGreaterThanOrEqual(1);

    // Check each GitHub link for target="_blank" and rel containing "noopener"
    for (let i = 0; i < count; i++) {
      const link = githubLinks.nth(i);
      const target = await link.getAttribute('target');
      const rel = await link.getAttribute('rel');

      expect(target).toBe('_blank');
      expect(rel).toContain('noopener');
    }
  });

  test('TC8: At least 3 GitHub link entry points exist (navbar, hero CTA, footer)', async ({ page }) => {
    const githubLinks = page.locator('a[href*="github.com"]');
    const count = await githubLinks.count();

    // Should have at least 3 GitHub links
    expect(count).toBeGreaterThanOrEqual(3);

    // Verify specific locations
    const navGithubLink = page.locator('nav a[href*="github.com"]');
    const heroGithubLink = page.locator('.hero a[href*="github.com"]');
    const footerGithubLink = page.locator('footer a.footer__link--github');

    await expect(navGithubLink).toBeVisible();
    await expect(heroGithubLink).toBeVisible();
    await expect(footerGithubLink).toBeVisible();
  });

  test('TC9: Footer contains GitHub link with star button or icon', async ({ page }) => {
    const footer = page.locator('footer');
    const githubLink = footer.locator('a.footer__link--github');

    await expect(githubLink).toBeVisible();

    // Check for star icon or text
    const starIcon = githubLink.locator('svg');
    const starText = githubLink.locator(':text("Star")');

    // Should have either a star icon or star text
    const hasStarIcon = await starIcon.count() > 0;
    const hasStarText = await starText.count() > 0;

    expect(hasStarIcon || hasStarText).toBeTruthy();
  });
});

test.describe('Navigation Smooth Scroll Functionality', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Navigation links trigger smooth scroll behavior', async ({ page }) => {
    // Check that HTML has smooth scroll behavior
    const htmlElement = page.locator('html');
    const scrollBehavior = await htmlElement.evaluate((el) => {
      return window.getComputedStyle(el).scrollBehavior;
    });

    expect(scrollBehavior).toBe('smooth');
  });

  test('Internal anchor links work correctly', async ({ page }) => {
    // Get current scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click features link
    await page.click('nav a[href="#features"]');

    // Wait for scroll
    await page.waitForTimeout(600);

    // Check scroll position changed
    const newScrollY = await page.evaluate(() => window.scrollY);

    // Verify features section is in viewport after scroll
    const featuresSection = page.locator('#features');

    // The section should be in viewport after clicking the link
    await expect(featuresSection).toBeInViewport();
  });
});

/**
 * Footer Content and Links Tests
 * Owner: Scenario 15 - Footer Content and Links
 *
 * Test Cases:
 * 1. Footer element exists at bottom of page
 * 2. GitHub link present in footer
 * 3. License link exists in footer
 * 4. Copyright notice with year is present
 * 5. Copyright includes current year (2026)
 * 6. License link navigates to license information
 */
test.describe('Footer Content and Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Footer element exists at bottom of page', async ({ page }) => {
    const footer = page.locator('footer');

    // Footer should exist
    await expect(footer).toBeVisible();

    // Footer should have role="contentinfo"
    await expect(footer).toHaveAttribute('role', 'contentinfo');

    // Footer should have the footer class
    await expect(footer).toHaveClass(/footer/);
  });

  test('TC2: GitHub link present in footer', async ({ page }) => {
    const footer = page.locator('footer');
    // Use the specific class for the GitHub star link
    const githubLink = footer.locator('a.footer__link--github');

    await expect(githubLink).toBeVisible();

    // Verify the link points to the GitHub repo
    const href = await githubLink.getAttribute('href');
    expect(href).toContain('github.com/yetone/mirdb');

    // Check for target="_blank" and rel="noopener"
    await expect(githubLink).toHaveAttribute('target', '_blank');
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('TC3: License link exists in footer', async ({ page }) => {
    const footer = page.locator('footer');
    const licenseLink = footer.locator('a:has-text("License")');

    await expect(licenseLink).toBeVisible();

    // License link should point to LICENSE file
    const href = await licenseLink.getAttribute('href');
    expect(href).toContain('LICENSE');
  });

  test('TC4: Copyright notice with year is present', async ({ page }) => {
    const footer = page.locator('footer');
    const copyright = footer.locator('.footer__copyright');

    await expect(copyright).toBeVisible();

    // Copyright text should contain copyright symbol or word
    const text = await copyright.textContent();
    expect(text).toMatch(/©|copyright/i);

    // Copyright should contain a year
    expect(text).toMatch(/\d{4}/);
  });

  test('TC5: Copyright includes current year (2026)', async ({ page }) => {
    const footer = page.locator('footer');
    const copyright = footer.locator('.footer__copyright');

    const text = await copyright.textContent();

    // Copyright must include 2026
    expect(text).toContain('2026');
  });

  test('TC6: License link navigates to license information', async ({ page }) => {
    const footer = page.locator('footer');
    const licenseLink = footer.locator('a:has-text("License")');

    // Get the href attribute
    const href = await licenseLink.getAttribute('href');

    // License link should point to GitHub LICENSE file
    expect(href).toContain('github.com/yetone/mirdb');
    expect(href).toContain('LICENSE');

    // Verify it opens in new tab
    await expect(licenseLink).toHaveAttribute('target', '_blank');

    // Verify noopener for security
    const rel = await licenseLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });
});
