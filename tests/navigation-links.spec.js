// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Navigation Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: GitHub repository link is present in header with valid href', async ({ page }) => {
    // Find the GitHub link in header navigation (the one labeled "GitHub")
    const navLinks = page.locator('.nav-links');
    const githubLink = navLinks.locator('a').filter({ hasText: /^GitHub$/ });

    // Verify the link exists and is visible
    await expect(githubLink).toBeVisible();

    // Verify it has a valid href attribute pointing to GitHub
    const href = await githubLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toContain('github.com');
    expect(href).not.toBe('#');
    expect(href).not.toBe('');

    // Verify it opens in new tab (external link)
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify rel attribute for security
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('TC2: Documentation link is present in navigation', async ({ page }) => {
    // Find documentation link in header navigation
    const navLinks = page.locator('.nav-links');
    const docsLink = navLinks.locator('a').filter({ hasText: /docs|documentation/i });

    // Verify the documentation link exists and is visible
    await expect(docsLink).toBeVisible();

    // Verify it has a valid href attribute
    const href = await docsLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).not.toBe('#');
    expect(href).not.toBe('');
  });

  test('TC3: Footer contains links to GitHub, documentation, and license', async ({ page }) => {
    const footer = page.locator('footer.footer');
    const footerLinks = footer.locator('.footer-links');

    // Verify footer is visible
    await expect(footer).toBeVisible();
    await expect(footerLinks).toBeVisible();

    // Check GitHub link in footer
    const githubFooterLink = footerLinks.locator('a').filter({ hasText: /github/i });
    await expect(githubFooterLink).toBeVisible();
    const githubHref = await githubFooterLink.getAttribute('href');
    expect(githubHref).toContain('github.com');

    // Check License link in footer
    const licenseLink = footerLinks.locator('a').filter({ hasText: /license/i });
    await expect(licenseLink).toBeVisible();
    const licenseHref = await licenseLink.getAttribute('href');
    expect(licenseHref).toBeTruthy();
    expect(licenseHref).not.toBe('#');

    // Check Documentation link in footer
    const docsFooterLink = footerLinks.locator('a').filter({ hasText: /docs|documentation/i });
    await expect(docsFooterLink).toBeVisible();
    const docsHref = await docsFooterLink.getAttribute('href');
    expect(docsHref).toBeTruthy();
    expect(docsHref).not.toBe('#');
  });

  test('TC4: All navigation links have valid href attributes (no broken links)', async ({ page }) => {
    // Get all navigation links in header
    const headerNavLinks = page.locator('.nav-links a');
    const headerLinkCount = await headerNavLinks.count();

    // Verify each header navigation link has a valid href
    for (let i = 0; i < headerLinkCount; i++) {
      const link = headerNavLinks.nth(i);
      const href = await link.getAttribute('href');
      const text = await link.textContent();

      // Link should have a valid href (not empty or just '#')
      expect(href, `Header link "${text}" should have valid href`).toBeTruthy();
      expect(href, `Header link "${text}" href should not be empty`).not.toBe('');
      expect(href, `Header link "${text}" href should not be just '#'`).not.toBe('#');
    }

    // Get all navigation links in footer
    const footerLinks = page.locator('.footer-links a');
    const footerLinkCount = await footerLinks.count();

    // Verify each footer link has a valid href
    for (let i = 0; i < footerLinkCount; i++) {
      const link = footerLinks.nth(i);
      const href = await link.getAttribute('href');
      const text = await link.textContent();

      // Link should have a valid href (not empty or just '#')
      expect(href, `Footer link "${text}" should have valid href`).toBeTruthy();
      expect(href, `Footer link "${text}" href should not be empty`).not.toBe('');
      expect(href, `Footer link "${text}" href should not be just '#'`).not.toBe('#');
    }
  });

  test('Header navigation is visible and contains expected links', async ({ page }) => {
    // Verify header navigation structure
    const nav = page.locator('.nav');
    await expect(nav).toBeVisible();

    // Verify brand
    const brand = nav.locator('.nav-brand');
    await expect(brand).toBeVisible();
    await expect(brand).toContainText('MirDB');

    // Verify nav-links container
    const navLinks = nav.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Verify internal navigation links exist (Features, Quick Start)
    const featuresLink = navLinks.locator('a[href="#features"]');
    await expect(featuresLink).toBeVisible();

    const quickstartLink = navLinks.locator('a[href="#quickstart"]');
    await expect(quickstartLink).toBeVisible();
  });

  test('Internal navigation links scroll to correct sections', async ({ page }) => {
    // Click Features link and verify it scrolls to features section
    await page.click('.nav-links a[href="#features"]');
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();

    // Click Quick Start link and verify it scrolls to quickstart section
    await page.click('.nav-links a[href="#quickstart"]');
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();
  });

  test('Hero buttons have correct links', async ({ page }) => {
    const heroButtons = page.locator('.hero-buttons');

    // Check "Get Started" button
    const getStartedBtn = heroButtons.locator('a.btn-primary');
    await expect(getStartedBtn).toBeVisible();
    const getStartedHref = await getStartedBtn.getAttribute('href');
    expect(getStartedHref).toBe('#quickstart');

    // Check "View on GitHub" button
    const githubBtn = heroButtons.locator('a.btn-secondary');
    await expect(githubBtn).toBeVisible();
    const githubHref = await githubBtn.getAttribute('href');
    expect(githubHref).toContain('github.com');
  });
});
