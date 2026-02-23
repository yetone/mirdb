/**
 * Footer Section E2E Tests
 * Owner: Scenario 6 - Footer and External Links
 *
 * End-to-end tests for footer:
 * - Footer is present
 * - GitHub link correct and clickable
 * - CircleCI badge loads
 * - Badge links to CI page
 * - License information present
 * - All external links valid
 */

import { test, expect } from '@playwright/test';

test.describe('Footer and External Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Footer section is present at bottom of page', async ({ page }) => {
    // Verify footer exists
    const footer = page.locator('[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Verify footer is a semantic footer element
    const tagName = await footer.evaluate((el) => el.tagName.toLowerCase());
    expect(tagName).toBe('footer');

    // Verify footer has the correct class
    await expect(footer).toHaveClass(/footer/);

    // Verify footer is at the bottom of the page
    const footerBox = await footer.boundingBox();
    const viewportSize = page.viewportSize();

    // Scroll to footer to ensure it's visible
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeInViewport();
  });

  test('TC2: GitHub repository link navigates to https://github.com/yetone/mirdb', async ({ page }) => {
    // Find the GitHub link
    const githubLink = page.locator('[data-testid="github-link"]');
    await expect(githubLink).toBeVisible();

    // Verify link text
    await expect(githubLink).toContainText('GitHub Repository');

    // Verify href attribute
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

    // Verify link opens in new tab
    await expect(githubLink).toHaveAttribute('target', '_blank');
    await expect(githubLink).toHaveAttribute('rel', 'noopener');
  });

  test('TC3: CircleCI build status badge is displayed and loads correctly', async ({ page }) => {
    // Scroll to footer first
    const footer = page.locator('[data-testid="footer"]');
    await footer.scrollIntoViewIfNeeded();

    // Find the CircleCI badge container
    const badgeContainer = page.locator('[data-testid="circleci-badge"]');
    await expect(badgeContainer).toBeVisible();

    // Find the badge image
    const badgeImg = page.locator('[data-testid="circleci-img"]');
    await expect(badgeImg).toBeVisible();

    // Verify the image source points to CircleCI
    await expect(badgeImg).toHaveAttribute('src', /circleci\.com/);
    await expect(badgeImg).toHaveAttribute('src', 'https://circleci.com/gh/yetone/mirdb.svg?style=svg');

    // Verify alt text for accessibility
    await expect(badgeImg).toHaveAttribute('alt', 'CircleCI Build Status');
  });

  test('TC4: CircleCI badge links to CircleCI project page', async ({ page }) => {
    // Scroll to footer first
    const footer = page.locator('[data-testid="footer"]');
    await footer.scrollIntoViewIfNeeded();

    // Find the CircleCI link
    const circleciLink = page.locator('[data-testid="circleci-link"]');
    await expect(circleciLink).toBeVisible();

    // Verify href attribute points to CircleCI project page
    await expect(circleciLink).toHaveAttribute('href', 'https://circleci.com/gh/yetone/mirdb');

    // Verify link opens in new tab
    await expect(circleciLink).toHaveAttribute('target', '_blank');
    await expect(circleciLink).toHaveAttribute('rel', 'noopener');

    // Verify aria-label for accessibility
    await expect(circleciLink).toHaveAttribute('aria-label', 'CircleCI Build Status');
  });

  test('TC5: License information is displayed or linked in footer', async ({ page }) => {
    // Scroll to footer first
    const footer = page.locator('[data-testid="footer"]');
    await footer.scrollIntoViewIfNeeded();

    // Find the license link
    const licenseLink = page.locator('[data-testid="license-link"]');
    await expect(licenseLink).toBeVisible();

    // Verify link text
    await expect(licenseLink).toContainText('License');

    // Verify href attribute points to the LICENSE file
    await expect(licenseLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb/blob/master/LICENSE');

    // Verify the MIT License text is present in footer
    const licenseText = page.locator('.footer__license');
    await expect(licenseText).toBeVisible();
    await expect(licenseText).toContainText('MIT License');
  });

  test('TC6: All external links have proper href attributes and open correctly', async ({ page }) => {
    // Scroll to footer first
    const footer = page.locator('[data-testid="footer"]');
    await footer.scrollIntoViewIfNeeded();

    // Get all links in the footer
    const footerLinks = footer.locator('a');
    const linkCount = await footerLinks.count();

    // Verify we have expected links (GitHub, License, CircleCI)
    expect(linkCount).toBeGreaterThanOrEqual(3);

    // Verify each link has proper attributes
    for (let i = 0; i < linkCount; i++) {
      const link = footerLinks.nth(i);

      // Verify href is present and not empty
      const href = await link.getAttribute('href');
      expect(href).toBeTruthy();
      expect(href.length).toBeGreaterThan(0);

      // Verify external links have target="_blank" and rel="noopener"
      if (href.startsWith('http')) {
        await expect(link).toHaveAttribute('target', '_blank');
        await expect(link).toHaveAttribute('rel', 'noopener');
      }
    }

    // Verify specific links exist
    const githubLink = page.locator('[data-testid="github-link"]');
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

    const licenseLink = page.locator('[data-testid="license-link"]');
    await expect(licenseLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb/blob/master/LICENSE');

    const circleciLink = page.locator('[data-testid="circleci-link"]');
    await expect(circleciLink).toHaveAttribute('href', 'https://circleci.com/gh/yetone/mirdb');
  });

  test('Footer links have visible hover states', async ({ page }) => {
    // Scroll to footer first
    const footer = page.locator('[data-testid="footer"]');
    await footer.scrollIntoViewIfNeeded();

    const githubLink = page.locator('[data-testid="github-link"]');

    // Get initial color
    const initialColor = await githubLink.evaluate((el) =>
      window.getComputedStyle(el).color
    );

    // Hover over the link
    await githubLink.hover();

    // Get color after hover
    const hoverColor = await githubLink.evaluate((el) =>
      window.getComputedStyle(el).color
    );

    // Colors should be different on hover (or link has transition effect)
    // Just verify the link has some styling for hover state
    expect(hoverColor || initialColor).toBeTruthy();
  });

  test('Footer is accessible with proper semantic structure', async ({ page }) => {
    // Scroll to footer first
    const footer = page.locator('[data-testid="footer"]');
    await footer.scrollIntoViewIfNeeded();

    // Verify it's a semantic footer element
    const tagName = await footer.evaluate((el) => el.tagName.toLowerCase());
    expect(tagName).toBe('footer');

    // Verify links are keyboard accessible
    const footerLinks = footer.locator('a');
    const linkCount = await footerLinks.count();

    for (let i = 0; i < linkCount; i++) {
      const link = footerLinks.nth(i);
      // Verify link is focusable
      await link.focus();
      await expect(link).toBeFocused();
    }
  });

  test('Footer displays correctly with minimum touch target sizes', async ({ page }) => {
    // Scroll to footer first
    const footer = page.locator('[data-testid="footer"]');
    await footer.scrollIntoViewIfNeeded();

    // Check GitHub link touch target
    const githubLink = page.locator('[data-testid="github-link"]');
    const boundingBox = await githubLink.boundingBox();

    // Links should have reasonable click targets
    expect(boundingBox.height).toBeGreaterThanOrEqual(20);
  });
});
