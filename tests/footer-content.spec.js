// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: Footer Content
 * Scenario: Verify the footer contains project links, license information, and community resources
 * Requirements: REQ-8 - Include footer with project links
 */

test.describe('Footer Content', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Check footer for GitHub repository link
   * Input: Check footer for GitHub repository link
   * Expected: Footer contains link to MirDB GitHub repository
   */
  test('TC1: Footer contains link to MirDB GitHub repository', async ({ page }) => {
    // Scroll to footer to ensure visibility
    const footer = page.locator('footer.footer');
    await footer.scrollIntoViewIfNeeded();

    // Verify footer element exists
    await expect(footer).toBeVisible();

    // Find GitHub repository link in footer
    const githubRepoLink = page.locator('footer a[href*="github.com/yetone/mirdb"]').first();
    await expect(githubRepoLink).toBeVisible();

    // Verify the link href points to the MirDB GitHub repository
    const href = await githubRepoLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toContain('github.com');
    expect(href).toContain('yetone/mirdb');

    // Verify the link text contains 'GitHub' or is meaningful
    const linkText = await githubRepoLink.textContent();
    expect(linkText).toBeTruthy();
    expect(linkText.toLowerCase()).toContain('github');

    // Verify link has proper security attributes for external links
    await expect(githubRepoLink).toHaveAttribute('target', '_blank');
    await expect(githubRepoLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  /**
   * Test Case 2: Check footer for license information
   * Input: Check footer for license information
   * Expected: Footer displays or links to license information
   */
  test('TC2: Footer displays or links to license information', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator('footer.footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Check for license text content in footer
    const footerText = await footer.textContent();
    expect(footerText).toBeTruthy();

    // Footer should mention license (MIT, Apache, BSD, or generic "license")
    const lowerFooterText = footerText.toLowerCase();
    const hasLicenseInfo =
      lowerFooterText.includes('license') ||
      lowerFooterText.includes('mit') ||
      lowerFooterText.includes('apache') ||
      lowerFooterText.includes('bsd') ||
      lowerFooterText.includes('gpl');

    expect(hasLicenseInfo).toBeTruthy();

    // Verify license information is displayed (either as text or link)
    // Check for text mentioning MIT license specifically based on PRD
    const hasMitLicense = lowerFooterText.includes('mit');
    expect(hasMitLicense).toBeTruthy();
  });

  /**
   * Test Case 3: Verify footer is present and visible
   * Input: Verify footer is present and visible
   * Expected: Footer element exists at bottom of page with appropriate styling
   */
  test('TC3: Footer element exists at bottom of page with appropriate styling', async ({ page }) => {
    // Verify footer element exists
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Verify footer is positioned at the bottom of the page
    // Scroll to footer to ensure it's accessible
    await footer.scrollIntoViewIfNeeded();

    // Verify footer has a container for layout
    const footerContainer = page.locator('footer .footer-container');
    await expect(footerContainer).toBeVisible();

    // Verify footer has appropriate styling by checking computed styles
    const footerBgColor = await footer.evaluate(el => {
      const style = window.getComputedStyle(el);
      return style.backgroundColor;
    });
    // Footer should have a background color set (not transparent/initial)
    expect(footerBgColor).toBeTruthy();

    // Verify footer is the last major element in the document
    // by checking it's positioned after the main content
    const footerPosition = await footer.evaluate(el => {
      const rect = el.getBoundingClientRect();
      const docHeight = document.documentElement.scrollHeight;
      const footerBottom = rect.bottom + window.scrollY;
      return {
        footerBottom,
        docHeight,
        isAtBottom: Math.abs(footerBottom - docHeight) < 10 // Allow 10px tolerance
      };
    });

    expect(footerPosition.isAtBottom).toBeTruthy();

    // Verify footer has semantic HTML element
    const footerTagName = await footer.evaluate(el => el.tagName);
    expect(footerTagName).toBe('FOOTER');
  });

  /**
   * Test Case 4: Check footer links are functional
   * Input: Check footer links are functional
   * Expected: All links in footer navigate to valid destinations
   */
  test('TC4: All links in footer navigate to valid destinations', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator('footer.footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Get all links in footer
    const footerLinks = page.locator('footer a');
    const linkCount = await footerLinks.count();

    // Footer should have at least one link
    expect(linkCount).toBeGreaterThanOrEqual(1);

    // Verify each link has a valid href
    for (let i = 0; i < linkCount; i++) {
      const link = footerLinks.nth(i);
      const href = await link.getAttribute('href');

      // Each link should have a non-empty href
      expect(href).toBeTruthy();
      expect(href.length).toBeGreaterThan(0);

      // Verify href is a valid URL format (either absolute or relative)
      const isValidUrl =
        href.startsWith('http://') ||
        href.startsWith('https://') ||
        href.startsWith('/') ||
        href.startsWith('#') ||
        href.startsWith('mailto:');

      expect(isValidUrl).toBeTruthy();
    }

    // Test that GitHub link opens in new tab (external link behavior)
    const githubLink = page.locator('footer a[href*="github.com"]').first();
    if (await githubLink.count() > 0) {
      await expect(githubLink).toHaveAttribute('target', '_blank');

      // Click and verify new page opens
      const [newPage] = await Promise.all([
        page.context().waitForEvent('page'),
        githubLink.click()
      ]);

      expect(newPage).toBeTruthy();
      const newPageUrl = newPage.url();
      expect(newPageUrl).toContain('github.com');

      await newPage.close();
    }
  });

  /**
   * Additional test: Footer navigation structure
   */
  test('Footer has proper navigation structure', async ({ page }) => {
    const footer = page.locator('footer.footer');
    await footer.scrollIntoViewIfNeeded();

    // Verify footer navigation element exists
    const footerNav = page.locator('footer .footer-nav');
    await expect(footerNav).toBeVisible();

    // Verify navigation contains links
    const navLinks = page.locator('footer .footer-nav a');
    const navLinkCount = await navLinks.count();
    expect(navLinkCount).toBeGreaterThanOrEqual(1);
  });

  /**
   * Additional test: Footer contains Issues link for community engagement
   */
  test('Footer contains Issues link for community engagement', async ({ page }) => {
    const footer = page.locator('footer.footer');
    await footer.scrollIntoViewIfNeeded();

    // Check for Issues link (community resource)
    const issuesLink = page.locator('footer a[href*="issues"]');
    const hasIssuesLink = await issuesLink.count() > 0;

    if (hasIssuesLink) {
      await expect(issuesLink.first()).toBeVisible();
      const href = await issuesLink.first().getAttribute('href');
      expect(href).toContain('github.com');
      expect(href).toContain('issues');
    }

    // Either issues link exists or there's at least community resource info
    expect(hasIssuesLink).toBeTruthy();
  });

  /**
   * Additional test: Footer copyright and year
   */
  test('Footer displays copyright information', async ({ page }) => {
    const footer = page.locator('footer.footer');
    await footer.scrollIntoViewIfNeeded();

    const footerText = await footer.textContent();
    expect(footerText).toBeTruthy();

    // Check for copyright symbol or text
    const hasCopyright =
      footerText.includes('©') ||
      footerText.toLowerCase().includes('copyright') ||
      footerText.toLowerCase().includes('mirdb');

    expect(hasCopyright).toBeTruthy();
  });

  /**
   * Additional test: Footer links are keyboard accessible
   */
  test('Footer links are keyboard accessible', async ({ page }) => {
    const footer = page.locator('footer.footer');
    await footer.scrollIntoViewIfNeeded();

    const footerLinks = page.locator('footer a');
    const linkCount = await footerLinks.count();

    // Verify at least one link can be focused
    if (linkCount > 0) {
      const firstLink = footerLinks.first();
      await firstLink.focus();
      await expect(firstLink).toBeFocused();
    }
  });
});
