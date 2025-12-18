// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const pageUrl = 'file://' + path.join(__dirname, '..', 'index.html');

test.describe('Navigation and Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(pageUrl);
  });

  // TC1: Check navigation contains Features link
  test('TC1: Features navigation link exists and scrolls to features section', async ({ page }) => {
    // Verify navigation contains Features link
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    const featuresLink = navLinks.locator('a[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await expect(featuresLink).toHaveText('Features');

    // Click the link and verify it scrolls to the features section
    await featuresLink.click();

    // Wait for the URL hash to update
    await expect(page).toHaveURL(/#features$/);

    // Verify the features section is in view
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();
    await expect(featuresSection).toBeInViewport();
  });

  // TC2: Check navigation contains Getting Started link
  test('TC2: Getting Started navigation link exists and scrolls to correct section', async ({ page }) => {
    // Verify navigation contains Getting Started link
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    const gettingStartedLink = navLinks.locator('a[href="#getting-started"]');
    await expect(gettingStartedLink).toBeVisible();
    await expect(gettingStartedLink).toHaveText('Getting Started');

    // Click the link and verify it scrolls to the getting started section
    await gettingStartedLink.click();

    // Wait for the URL hash to update
    await expect(page).toHaveURL(/#getting-started$/);

    // Verify the getting started section is in view
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();
    await expect(gettingStartedSection).toBeInViewport();
  });

  // TC3: Check GitHub link in navigation/header
  test('TC3: GitHub link in navigation points to valid repository URL', async ({ page }) => {
    // Find the GitHub link in the navigation
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    const githubLink = navLinks.locator('a[href^="https://github.com"]');
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toHaveText('GitHub');

    // Verify the URL points to the correct repository
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

    // Verify it opens in a new tab
    await expect(githubLink).toHaveAttribute('target', '_blank');
  });

  // TC4: Check Documentation link is present and functional
  test('TC4: Documentation link is present and functional', async ({ page }) => {
    // The Getting Started section serves as documentation entry point
    // Check that the navigation link to Getting Started exists
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    const gettingStartedLink = navLinks.locator('a[href="#getting-started"]');
    await expect(gettingStartedLink).toBeVisible();

    // Also check the CTA button in the hero section
    const ctaGetStarted = page.locator('.cta-buttons a[href="#getting-started"]');
    await expect(ctaGetStarted).toBeVisible();
    await expect(ctaGetStarted).toHaveText('Get Started');

    // Verify clicking navigates to the section
    await ctaGetStarted.click();
    await expect(page).toHaveURL(/#getting-started$/);

    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeInViewport();
  });

  // TC5: Verify external links have appropriate rel attributes (unit test)
  test('TC5: External links have rel="noopener" or "noreferrer" for security', async ({ page }) => {
    // Find all external links (links with target="_blank")
    const externalLinks = page.locator('a[target="_blank"]');
    const count = await externalLinks.count();

    // Ensure there are external links to test
    expect(count).toBeGreaterThan(0);

    // Check each external link has appropriate rel attribute
    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const rel = await link.getAttribute('rel');

      // rel should contain 'noopener' and/or 'noreferrer'
      expect(rel).not.toBeNull();
      const hasNoopener = rel?.includes('noopener');
      const hasNoreferrer = rel?.includes('noreferrer');

      // At least one of these should be present for security
      expect(hasNoopener || hasNoreferrer).toBeTruthy();
    }
  });

  // TC6: Check footer contains GitHub link
  test('TC6: Footer has link to GitHub repository', async ({ page }) => {
    // Find the footer section
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Find the GitHub link in the footer
    const footerGithubLink = footer.locator('a[href^="https://github.com"]');
    await expect(footerGithubLink).toBeVisible();
    await expect(footerGithubLink).toHaveText('GitHub');

    // Verify the URL points to the correct repository
    await expect(footerGithubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

    // Verify it has security attributes
    await expect(footerGithubLink).toHaveAttribute('target', '_blank');
    const rel = await footerGithubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  // TC7: Check footer contains license information
  test('TC7: Footer displays license information', async ({ page }) => {
    // Find the footer section
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // The footer should contain license text
    const footerContent = footer.locator('.footer-content');
    await expect(footerContent).toBeVisible();

    // Check for license text
    const licenseText = footerContent.locator('p', { hasText: /MIT|License/i });
    await expect(licenseText).toBeVisible();
    await expect(licenseText).toContainText('MIT');
  });

  // Additional test: Verify all internal navigation links point to valid sections
  test('All internal navigation links point to existing sections', async ({ page }) => {
    const navLinks = page.locator('.nav-links a[href^="#"]');
    const count = await navLinks.count();

    for (let i = 0; i < count; i++) {
      const link = navLinks.nth(i);
      const href = await link.getAttribute('href');

      if (href && href.startsWith('#')) {
        const sectionId = href.substring(1);
        const section = page.locator(`#${sectionId}`);
        await expect(section).toBeVisible();
      }
    }
  });

  // Additional test: Verify header navigation structure
  test('Header contains proper navigation structure', async ({ page }) => {
    // Check header exists
    const header = page.locator('header.header');
    await expect(header).toBeVisible();

    // Check nav exists within header
    const nav = header.locator('nav.nav');
    await expect(nav).toBeVisible();

    // Check logo is present
    const logo = nav.locator('.logo');
    await expect(logo).toBeVisible();
    await expect(logo).toHaveText('MirDB');

    // Check nav-links list exists
    const navLinksList = nav.locator('.nav-links');
    await expect(navLinksList).toBeVisible();

    // Verify we have the expected navigation items
    const navItems = navLinksList.locator('li');
    await expect(navItems).toHaveCount(3);
  });

  // Additional test: Mobile menu button exists and is accessible
  test('Mobile menu button is present for responsive design', async ({ page }) => {
    const mobileMenuBtn = page.locator('.mobile-menu-btn');

    // Mobile menu button exists in the DOM (hidden on desktop, visible on mobile)
    await expect(mobileMenuBtn).toBeAttached();
    await expect(mobileMenuBtn).toHaveAttribute('aria-label', 'Toggle menu');

    // Verify it becomes visible on mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await expect(mobileMenuBtn).toBeVisible();
  });
});
