// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('GitHub Navigation Link', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Click View on GitHub button in hero section navigates to https://github.com/yetone/mirdb', async ({ page }) => {
    // Find the View on GitHub button in the hero CTAs
    const githubButton = page.locator('.hero-ctas a[href="https://github.com/yetone/mirdb"]');
    await expect(githubButton).toBeVisible();

    // Verify the button text
    await expect(githubButton).toHaveText('View on GitHub');

    // Verify the button links to the correct GitHub URL
    await expect(githubButton).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

    // Set up listener for popup (new tab)
    const popupPromise = page.context().waitForEvent('page');

    // Click the GitHub button
    await githubButton.click();

    // Verify the new tab opens with the correct URL
    const popup = await popupPromise;
    await popup.waitForLoadState('domcontentloaded');
    expect(popup.url()).toBe('https://github.com/yetone/mirdb');
  });

  test('TC2: GitHub link has target=_blank and rel=noopener noreferrer attributes', async ({ page }) => {
    // Find all GitHub links on the page
    const heroGithubLink = page.locator('.hero-ctas a[href="https://github.com/yetone/mirdb"]');
    const navGithubLink = page.locator('.nav-links a[href="https://github.com/yetone/mirdb"]');
    const footerGithubLink = page.locator('.footer-links a[href="https://github.com/yetone/mirdb"]');

    // Verify hero GitHub link has correct attributes
    await expect(heroGithubLink).toHaveAttribute('target', '_blank');
    await expect(heroGithubLink).toHaveAttribute('rel', 'noopener noreferrer');

    // Verify navigation GitHub link has correct attributes
    await expect(navGithubLink).toHaveAttribute('target', '_blank');
    await expect(navGithubLink).toHaveAttribute('rel', 'noopener noreferrer');

    // Verify footer GitHub link has correct attributes
    await expect(footerGithubLink).toHaveAttribute('target', '_blank');
    await expect(footerGithubLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  test('TC3: GitHub link is visible without scrolling (above the fold)', async ({ page }) => {
    // Get viewport dimensions
    const viewportSize = page.viewportSize();

    // Find the View on GitHub button in hero section
    const heroGithubButton = page.locator('.hero-ctas a[href="https://github.com/yetone/mirdb"]');
    await expect(heroGithubButton).toBeVisible();

    // Verify the button is within the viewport (above the fold)
    const boundingBox = await heroGithubButton.boundingBox();
    expect(boundingBox).not.toBeNull();

    // The button should be visible within the initial viewport height
    expect(boundingBox.y).toBeLessThan(viewportSize.height);
    expect(boundingBox.y + boundingBox.height).toBeLessThan(viewportSize.height);

    // Also verify navigation GitHub link is visible
    const navGithubLink = page.locator('.nav-links a[href="https://github.com/yetone/mirdb"]');
    await expect(navGithubLink).toBeVisible();

    const navBoundingBox = await navGithubLink.boundingBox();
    expect(navBoundingBox).not.toBeNull();
    expect(navBoundingBox.y).toBeLessThan(viewportSize.height);
  });

  test('GitHub link in navigation bar is functional', async ({ page }) => {
    // Find the GitHub link in navigation
    const navGithubLink = page.locator('.nav-links a[href="https://github.com/yetone/mirdb"]');
    await expect(navGithubLink).toBeVisible();

    // Verify it has correct href
    await expect(navGithubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

    // Set up listener for popup (new tab)
    const popupPromise = page.context().waitForEvent('page');

    // Click the navigation GitHub link
    await navGithubLink.click();

    // Verify the new tab opens with the correct URL
    const popup = await popupPromise;
    await popup.waitForLoadState('domcontentloaded');
    expect(popup.url()).toBe('https://github.com/yetone/mirdb');
  });

  test('Footer GitHub link is present and functional', async ({ page }) => {
    // Scroll to footer to make it visible
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));

    // Find the GitHub link in footer
    const footerGithubLink = page.locator('.footer-links a[href="https://github.com/yetone/mirdb"]');
    await expect(footerGithubLink).toBeVisible();

    // Verify it has correct href and security attributes
    await expect(footerGithubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    await expect(footerGithubLink).toHaveAttribute('target', '_blank');
    await expect(footerGithubLink).toHaveAttribute('rel', 'noopener noreferrer');
  });
});
