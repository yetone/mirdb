// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '../index.html');
const GITHUB_REPO_URL = 'https://github.com/yetone/mirdb';

test.describe('GitHub Repository Link (REQ-5, US-4)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  // Test Case 1: Check for GitHub link in page
  // Expected: Anchor element with href pointing to GitHub repository exists
  test('TC1: GitHub link anchor element exists with correct href', async ({ page }) => {
    // Check for primary CTA button linking to GitHub
    const primaryCta = page.locator('#cta-primary');
    await expect(primaryCta).toBeVisible();

    // Verify href points to GitHub repository
    const href = await primaryCta.getAttribute('href');
    expect(href).toBe(GITHUB_REPO_URL);

    // Also check footer GitHub link
    const footerGithubLink = page.locator('.footer-links a[href*="github.com/yetone/mirdb"]').first();
    await expect(footerGithubLink).toBeVisible();

    const footerHref = await footerGithubLink.getAttribute('href');
    expect(footerHref).toBe(GITHUB_REPO_URL);
  });

  // Test Case 2: Verify GitHub link is clickable
  // Expected: Link navigates to correct GitHub repository URL
  test('TC2: GitHub link is clickable and navigates to correct URL', async ({ page }) => {
    // Test primary CTA button
    const primaryCta = page.locator('#cta-primary');
    await expect(primaryCta).toBeVisible();
    await expect(primaryCta).toBeEnabled();

    // Verify the link is an anchor element with valid href
    const tagName = await primaryCta.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('a');

    const href = await primaryCta.getAttribute('href');
    expect(href).toBe(GITHUB_REPO_URL);

    // Verify it's a valid URL format
    expect(href).toMatch(/^https:\/\/github\.com\/[a-zA-Z0-9_-]+\/[a-zA-Z0-9_-]+$/);

    // Test footer GitHub link is also clickable
    const footerGithubLink = page.locator('.footer-links a[href*="github.com/yetone/mirdb"]').first();
    await expect(footerGithubLink).toBeEnabled();
  });

  // Test Case 3: Check for GitHub star badge or count display
  // Expected: Star count badge or number is visible near GitHub link
  test('TC3: GitHub star badge is visible in footer', async ({ page }) => {
    // Locate the GitHub stars badge image
    const starsBadge = page.locator('.footer-links img[alt="GitHub Stars"]');
    await expect(starsBadge).toBeVisible();

    // Verify the badge uses shields.io with correct repository
    const src = await starsBadge.getAttribute('src');
    expect(src).toContain('img.shields.io/github/stars/yetone/mirdb');
    expect(src).toContain('style=social');

    // Verify the badge is wrapped in a link to the repository
    const parentLink = starsBadge.locator('..');
    const parentHref = await parentLink.getAttribute('href');
    expect(parentHref).toBe(GITHUB_REPO_URL);
  });

  // Test Case 4: Verify GitHub link visibility
  // Expected: GitHub link is visually prominent and easily discoverable
  test('TC4: GitHub link is visually prominent and easily discoverable', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 720 });

    // Check primary CTA is above the fold and prominent
    const primaryCta = page.locator('#cta-primary');
    await expect(primaryCta).toBeVisible();

    // Verify button text mentions GitHub
    const buttonText = await primaryCta.textContent();
    expect(buttonText.toLowerCase()).toContain('github');

    // Verify the CTA has btn-primary class (indicating visual prominence)
    const hasProminentClass = await primaryCta.evaluate(el =>
      el.classList.contains('btn-primary') || el.classList.contains('btn')
    );
    expect(hasProminentClass).toBeTruthy();

    // Check button is positioned above the fold
    const boundingBox = await primaryCta.boundingBox();
    expect(boundingBox).not.toBeNull();
    expect(boundingBox.y + boundingBox.height).toBeLessThan(720); // Fully visible in viewport

    // Verify the button has sufficient size for clickability (touch-friendly)
    expect(boundingBox.width).toBeGreaterThan(100);
    expect(boundingBox.height).toBeGreaterThan(30);

    // Check footer has visible GitHub section
    const footerLinks = page.locator('.footer-links');
    await expect(footerLinks).toBeVisible();
  });

  // Additional: Verify all GitHub-related links point to the same repository
  test('All GitHub links point to the same repository', async ({ page }) => {
    // Collect all GitHub links on the page
    const githubLinks = page.locator('a[href*="github.com/yetone/mirdb"]');
    const count = await githubLinks.count();

    // Should have at least 2 links (hero CTA and footer)
    expect(count).toBeGreaterThanOrEqual(2);

    // Verify all links point to the correct repository
    for (let i = 0; i < count; i++) {
      const href = await githubLinks.nth(i).getAttribute('href');
      expect(href).toBe(GITHUB_REPO_URL);
    }
  });

  // Additional: Verify footer badges section structure
  test('Footer contains properly structured badges section', async ({ page }) => {
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    const footerLinks = page.locator('.footer-links');
    await expect(footerLinks).toBeVisible();

    // Check for GitHub stars badge
    const starsBadge = footerLinks.locator('img[alt="GitHub Stars"]');
    await expect(starsBadge).toBeVisible();

    // Check for CircleCI badge (related to project)
    const cirlceciBadge = footerLinks.locator('img[alt*="CircleCI"]');
    await expect(cirlceciBadge).toBeVisible();
  });
});
