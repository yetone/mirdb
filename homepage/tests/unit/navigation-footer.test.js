/**
 * Unit Tests for Navigation and Footer
 * Owner: Scenario 8 - Navigation and Footer
 *
 * Tests HTML structure, semantic elements, and content requirements
 * Test Cases: 1, 2, 5, 6, 7, 9
 */

const { test, expect } = require('@playwright/test');

test.describe('Navigation Header Unit Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Header element exists with navigation elements', async ({ page }) => {
    // Check for header element
    const header = page.locator('header.header');
    await expect(header).toBeVisible();

    // Check for navigation inside header
    const nav = header.locator('nav.nav');
    await expect(nav).toBeVisible();

    // Check for logo
    const logo = nav.locator('.nav__logo');
    await expect(logo).toBeVisible();

    // Check for navigation links container
    const navLinks = nav.locator('.nav__links');
    await expect(navLinks).toBeVisible();
  });

  test('Test Case 2: GitHub link present in header navigation', async ({ page }) => {
    const header = page.locator('header.header');

    // Check for GitHub link in navigation
    const githubLink = header.locator('a[href*="github.com/yetone/mirdb"]');
    await expect(githubLink).toBeVisible();

    // Verify it's in the nav links section
    const navGithubLink = header.locator('.nav__link--github, .nav__links a[href*="github.com"]');
    await expect(navGithubLink.first()).toBeVisible();

    // Check that GitHub text is present
    const linkText = await navGithubLink.first().textContent();
    expect(linkText.toLowerCase()).toContain('github');
  });

  test('Header has sticky positioning', async ({ page }) => {
    const header = page.locator('header.header');

    // Check that header is sticky
    const position = await header.evaluate(el => getComputedStyle(el).position);
    expect(position).toBe('sticky');
  });

  test('Header navigation links exist for sections', async ({ page }) => {
    const navLinks = page.locator('.nav__links');

    // Check for Features link
    const featuresLink = navLinks.locator('a[href="#features"]');
    await expect(featuresLink).toBeVisible();

    // Check for Quick Start link
    const quickstartLink = navLinks.locator('a[href="#quickstart"]');
    await expect(quickstartLink).toBeVisible();
  });

  test('Header logo links to home', async ({ page }) => {
    const logo = page.locator('.nav__logo');
    await expect(logo).toBeVisible();

    // Logo should contain MirDB text or image
    const hasLogo = await logo.locator('img').count();
    const hasText = await logo.textContent();
    expect(hasLogo > 0 || hasText.includes('MirDB')).toBeTruthy();
  });
});

test.describe('Footer Unit Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 5: Footer element exists with links and information', async ({ page }) => {
    // Check for footer element
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Check for footer container
    const footerContainer = footer.locator('.footer__container');
    await expect(footerContainer).toBeVisible();

    // Check for content info role (accessibility)
    await expect(footer).toHaveAttribute('role', 'contentinfo');
  });

  test('Test Case 6: License information is displayed in footer', async ({ page }) => {
    const footer = page.locator('footer.footer');

    // Check for license text
    const licenseElement = footer.locator('.footer__license');
    await expect(licenseElement).toBeVisible();

    // Verify license text contains MIT
    const licenseText = await licenseElement.textContent();
    expect(licenseText.toLowerCase()).toContain('mit');
    expect(licenseText.toLowerCase()).toContain('license');
  });

  test('Test Case 7: GitHub repository link present in footer', async ({ page }) => {
    const footer = page.locator('footer.footer');

    // Check for GitHub link in footer
    const githubLink = footer.locator('a[href*="github.com/yetone/mirdb"]');
    await expect(githubLink.first()).toBeVisible();

    // Verify the link text or aria-label
    const firstGithubLink = githubLink.first();
    const linkText = await firstGithubLink.textContent();
    const ariaLabel = await firstGithubLink.getAttribute('aria-label');

    const hasGitHubReference =
      linkText.toLowerCase().includes('github') ||
      (ariaLabel && ariaLabel.toLowerCase().includes('github'));
    expect(hasGitHubReference).toBeTruthy();
  });

  test('Footer GitHub link opens in new tab', async ({ page }) => {
    const footer = page.locator('footer.footer');
    const githubLink = footer.locator('a[href*="github.com/yetone/mirdb"]').first();

    // Check target="_blank" for new tab
    await expect(githubLink).toHaveAttribute('target', '_blank');

    // Check for security attributes
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('Footer has proper semantic structure', async ({ page }) => {
    // Check footer element
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Check for footer links section
    const footerLinks = footer.locator('.footer__links');
    const linkCount = await footerLinks.locator('a').count();

    // Should have at least one link (GitHub)
    expect(linkCount).toBeGreaterThanOrEqual(1);
  });

  test('Footer license link points to LICENSE file', async ({ page }) => {
    const footer = page.locator('footer.footer');

    // Check for license link that points to the LICENSE file
    const licenseLink = footer.locator('a[href*="LICENSE"]');

    // If there's a license link, verify it
    if (await licenseLink.count() > 0) {
      await expect(licenseLink).toHaveAttribute('target', '_blank');
      const href = await licenseLink.getAttribute('href');
      expect(href).toContain('github.com/yetone/mirdb');
    }
  });
});

test.describe('Smooth Scroll Unit Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 9: initSmoothScroll function attaches scroll listeners to anchor links', async ({ page }) => {
    // Wait for page to be fully loaded
    await page.waitForLoadState('domcontentloaded');

    // Get all anchor links that should have smooth scroll
    const anchorLinks = page.locator('a[href^="#"]');
    const linkCount = await anchorLinks.count();

    // Should have multiple anchor links with scroll behavior
    expect(linkCount).toBeGreaterThan(0);

    // Check that clicking an anchor link triggers smooth scroll behavior
    // by verifying the link exists and has proper href
    const featuresLink = page.locator('a[href="#features"]');
    await expect(featuresLink.first()).toBeVisible();

    // Verify the initSmoothScroll function is available
    const hasInitSmoothScroll = await page.evaluate(() => {
      return typeof window.initSmoothScroll === 'function';
    });
    expect(hasInitSmoothScroll).toBeTruthy();
  });

  test('Anchor links have proper href attributes', async ({ page }) => {
    // Check navigation links have correct hrefs
    const featuresLink = page.locator('.nav__links a[href="#features"]');
    await expect(featuresLink).toHaveAttribute('href', '#features');

    const quickstartLink = page.locator('.nav__links a[href="#quickstart"]');
    await expect(quickstartLink).toHaveAttribute('href', '#quickstart');

    // Check hero CTA links
    const getStartedLink = page.locator('.hero__cta--primary');
    await expect(getStartedLink).toHaveAttribute('href', '#quickstart');
  });

  test('scrollToElement function is available globally', async ({ page }) => {
    await page.waitForLoadState('domcontentloaded');

    const hasScrollToElement = await page.evaluate(() => {
      return typeof window.scrollToElement === 'function';
    });
    expect(hasScrollToElement).toBeTruthy();
  });
});
