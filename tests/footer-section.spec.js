// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '../index.html');
const GITHUB_REPO_URL = 'https://github.com/yetone/mirdb';
const CIRCLECI_URL = 'https://circleci.com/gh/yetone/mirdb';

test.describe('Footer Section (REQ-9)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  // Test Case 1: Check for semantic footer element
  // Expected: Semantic footer element is present at page bottom
  test('TC1: Semantic footer element is present at page bottom', async ({ page }) => {
    // Check that a semantic <footer> element exists
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Verify it has the footer class
    const footerWithClass = page.locator('footer.footer');
    await expect(footerWithClass).toBeVisible();

    // Verify footer is positioned at the bottom of the page
    const footerBox = await footer.boundingBox();
    expect(footerBox).not.toBeNull();

    // Get the page height and verify footer is near the bottom
    const pageHeight = await page.evaluate(() => document.body.scrollHeight);
    expect(footerBox.y + footerBox.height).toBeLessThanOrEqual(pageHeight + 5); // Allow small tolerance
  });

  // Test Case 2: Verify license information
  // Expected: Footer contains license text or link to LICENSE file
  test('TC2: Footer contains license information', async ({ page }) => {
    // Look for license text in footer
    const licenseElement = page.locator('.footer .license');
    await expect(licenseElement).toBeVisible();

    // Verify license text contains "ISC License" or similar
    const licenseText = await licenseElement.textContent();
    expect(licenseText.toLowerCase()).toContain('license');

    // Verify it specifically mentions ISC (the project's license)
    expect(licenseText.toLowerCase()).toContain('isc');
  });

  // Test Case 3: Check for CircleCI build badge
  // Expected: CircleCI status badge image is present and loads
  test('TC3: CircleCI status badge image is present', async ({ page }) => {
    // Locate the CircleCI badge image
    const circleciBadge = page.locator('.footer img[alt*="CircleCI"]');
    await expect(circleciBadge).toBeVisible();

    // Verify the badge has the correct source URL format
    const src = await circleciBadge.getAttribute('src');
    expect(src).toContain('circleci.com');
    expect(src).toContain('yetone/mirdb');
    expect(src).toContain('.svg');

    // Verify the badge is wrapped in a link to CircleCI
    const parentLink = circleciBadge.locator('..');
    const parentHref = await parentLink.getAttribute('href');
    expect(parentHref).toBe(CIRCLECI_URL);
  });

  // Test Case 4: Verify footer links are functional (e2e)
  // Expected: All links in footer navigate to correct destinations
  test('TC4: Footer links have correct destinations', async ({ page }) => {
    // Get all links in the footer
    const footerLinks = page.locator('.footer a');
    const linkCount = await footerLinks.count();

    // Verify there are links in the footer
    expect(linkCount).toBeGreaterThan(0);

    // Check each link has a valid href
    for (let i = 0; i < linkCount; i++) {
      const link = footerLinks.nth(i);
      const href = await link.getAttribute('href');
      expect(href).toBeTruthy();
      expect(href.length).toBeGreaterThan(0);
    }

    // Specifically verify the GitHub link
    const githubLink = page.locator('.footer a[href*="github.com/yetone/mirdb"]');
    await expect(githubLink).toBeVisible();
    const githubHref = await githubLink.getAttribute('href');
    expect(githubHref).toBe(GITHUB_REPO_URL);

    // Specifically verify the CircleCI link
    const circleciLink = page.locator('.footer a[href*="circleci.com"]');
    await expect(circleciLink).toBeVisible();
    const circleciHref = await circleciLink.getAttribute('href');
    expect(circleciHref).toBe(CIRCLECI_URL);
  });

  // Additional test: Footer has proper structure with all required elements
  test('Footer has proper structure with all required elements', async ({ page }) => {
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Verify footer content wrapper exists
    const footerContent = page.locator('.footer-content');
    await expect(footerContent).toBeVisible();

    // Verify footer links section exists
    const footerLinks = page.locator('.footer-links');
    await expect(footerLinks).toBeVisible();

    // Verify license information exists
    const license = page.locator('.footer .license');
    await expect(license).toBeVisible();

    // Verify copyright information exists
    const copyright = page.locator('.footer .copyright');
    await expect(copyright).toBeVisible();

    // Verify copyright contains year and project name
    const copyrightText = await copyright.textContent();
    expect(copyrightText).toMatch(/©?\s*\d{4}/); // Contains year
    expect(copyrightText.toLowerCase()).toContain('mirdb');
  });

  // Additional test: Footer GitHub stars badge is properly formatted
  test('Footer GitHub stars badge is properly formatted', async ({ page }) => {
    const starsBadge = page.locator('.footer img[alt="GitHub Stars"]');
    await expect(starsBadge).toBeVisible();

    // Verify the badge uses shields.io
    const src = await starsBadge.getAttribute('src');
    expect(src).toContain('img.shields.io');
    expect(src).toContain('github/stars');
    expect(src).toContain('style=social');

    // Verify the badge is clickable (wrapped in a link)
    const parentLink = starsBadge.locator('..');
    const parentHref = await parentLink.getAttribute('href');
    expect(parentHref).toBe(GITHUB_REPO_URL);
  });

  // Additional test: Footer is accessible
  test('Footer is accessible with semantic HTML', async ({ page }) => {
    // Verify footer uses semantic <footer> tag
    const footer = page.locator('footer');
    const tagName = await footer.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('footer');

    // Verify all images have alt text
    const footerImages = page.locator('.footer img');
    const imageCount = await footerImages.count();

    for (let i = 0; i < imageCount; i++) {
      const img = footerImages.nth(i);
      const alt = await img.getAttribute('alt');
      expect(alt).toBeTruthy();
      expect(alt.length).toBeGreaterThan(0);
    }

    // Verify links are actual anchor elements
    const footerLinks = page.locator('.footer a');
    const linkCount = await footerLinks.count();

    for (let i = 0; i < linkCount; i++) {
      const link = footerLinks.nth(i);
      const linkTagName = await link.evaluate(el => el.tagName.toLowerCase());
      expect(linkTagName).toBe('a');
    }
  });
});
