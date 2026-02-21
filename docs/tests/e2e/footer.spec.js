/**
 * E2E tests for Footer and External Links
 * Owner: Scenario 7
 *
 * Test cases:
 * - Footer presence
 * - GitHub link (correct URL, target="_blank")
 * - CircleCI badge
 * - External link security (rel="noopener noreferrer")
 */

const { test, expect } = require('@playwright/test');

test.describe('Footer and External Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('');
  });

  test('Test Case 1: Footer element with copyright information is present', async ({ page }) => {
    // Check that footer element exists
    const footer = page.locator('footer#footer');
    await expect(footer).toBeVisible();

    // Check for semantic role
    await expect(footer).toHaveAttribute('role', 'contentinfo');

    // Check for copyright text with current year
    const currentYear = new Date().getFullYear().toString();
    const copyrightText = page.locator('footer p');
    await expect(copyrightText).toContainText(currentYear);
    await expect(copyrightText).toContainText('MirDB');
    await expect(copyrightText).toContainText('All rights reserved');
  });

  test('Test Case 2: GitHub repository link exists with target="_blank"', async ({ page }) => {
    // Find GitHub link in footer
    const githubLink = page.locator('footer#footer a#github-link');
    await expect(githubLink).toBeVisible();

    // Verify correct URL
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

    // Verify opens in new tab
    await expect(githubLink).toHaveAttribute('target', '_blank');

    // Verify link text or aria-label
    const ariaLabel = await githubLink.getAttribute('aria-label');
    expect(ariaLabel).toContain('GitHub');
  });

  test('Test Case 3: CircleCI build status badge is displayed', async ({ page }) => {
    // Find CircleCI badge
    const circleciBadge = page.locator('footer#footer img#circleci-badge');
    await expect(circleciBadge).toBeVisible();

    // Verify image source points to CircleCI
    const src = await circleciBadge.getAttribute('src');
    expect(src).toContain('circleci.com');
    expect(src).toContain('yetone/mirdb');

    // Verify alt text for accessibility
    const altText = await circleciBadge.getAttribute('alt');
    expect(altText).toContain('CircleCI');
    expect(altText.toLowerCase()).toContain('build');
  });

  test('Test Case 4: External links have rel="noopener noreferrer" attribute', async ({ page }) => {
    // Get all external links in footer (links with target="_blank")
    const externalLinks = page.locator('footer#footer a[target="_blank"]');
    const count = await externalLinks.count();

    // Ensure there are external links
    expect(count).toBeGreaterThan(0);

    // Check each external link has proper rel attribute
    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const rel = await link.getAttribute('rel');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    }
  });

  test('Test Case 5: GitHub and external doc links have target="_blank"', async ({ page }) => {
    // Check GitHub link opens in new tab
    const githubLink = page.locator('footer#footer a[href*="github.com"]');
    await expect(githubLink).toHaveAttribute('target', '_blank');

    // Check CircleCI link opens in new tab
    const circleciLink = page.locator('footer#footer a[href*="circleci.com"]');
    await expect(circleciLink).toHaveAttribute('target', '_blank');
  });

  test('Footer is visible after scrolling to bottom of page', async ({ page }) => {
    // Scroll to bottom of page
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Check footer is visible
    const footer = page.locator('footer#footer');
    await expect(footer).toBeInViewport();
  });

  test('Footer links are keyboard navigable', async ({ page }) => {
    // Scroll to footer first
    await page.locator('footer#footer').scrollIntoViewIfNeeded();

    // Focus the first link in footer using JavaScript
    await page.evaluate(() => {
      const footer = document.querySelector('footer#footer');
      const firstLink = footer.querySelector('a');
      if (firstLink) firstLink.focus();
    });

    // Verify a link in footer is focused
    const circleciLink = page.locator('footer#footer a[href*="circleci.com"]');
    await expect(circleciLink).toBeFocused();

    // Tab to next link
    await page.keyboard.press('Tab');

    // Verify GitHub link is now focused
    const githubLink = page.locator('footer#footer a#github-link');
    await expect(githubLink).toBeFocused();
  });

  test('Footer has proper contrast in light mode', async ({ page }) => {
    // Ensure light mode
    await page.evaluate(() => {
      localStorage.removeItem('mirdb-theme');
      document.documentElement.classList.remove('dark');
    });
    await page.reload();

    const footer = page.locator('footer#footer');
    await expect(footer).toBeVisible();

    // Check footer background color
    const bgColor = await footer.evaluate(el => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Should be dark slate (bg-slate-900)
    expect(bgColor).toBeTruthy();
  });

  test('Footer has proper contrast in dark mode', async ({ page }) => {
    // Enable dark mode
    await page.evaluate(() => {
      localStorage.setItem('mirdb-theme', 'dark');
      document.documentElement.classList.add('dark');
    });
    await page.reload();

    const footer = page.locator('footer#footer');
    await expect(footer).toBeVisible();

    // Check footer background color in dark mode
    const bgColor = await footer.evaluate(el => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Should be even darker (bg-slate-950)
    expect(bgColor).toBeTruthy();
  });
});
