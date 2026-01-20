/**
 * GitHub Navigation and Project Status E2E Tests
 * Browser-based tests for verifying GitHub repository links and CI status badges (REQ-5, REQ-6)
 */

const { test, expect } = require('@playwright/test');

test.describe('GitHub Navigation and Project Status', () => {
    test.beforeEach(async ({ page }) => {
        // Navigate to the homepage
        await page.goto('file://' + process.cwd() + '/index.html');
    });

    // Test Case 3: Check for CI status badge (E2E)
    // Type: e2e
    // Expected: CircleCI or similar CI status badge image is displayed
    test('TC3-E2E: CI status badge is visible on the page', async ({ page }) => {
        // Locate the badges container
        const badgesContainer = page.locator('.badges');
        await expect(badgesContainer).toBeVisible();

        // Locate the badge image
        const badgeImg = badgesContainer.locator('img');
        await expect(badgeImg).toBeVisible();

        // Verify the badge has proper alt text
        const altText = await badgeImg.getAttribute('alt');
        expect(altText).toBeTruthy();
        expect(altText.toLowerCase()).toMatch(/circleci|build|status/);

        // Check that the badge image source is set
        const imgSrc = await badgeImg.getAttribute('src');
        expect(imgSrc).toBeTruthy();
    });

    // Test Case 4: Verify badge links to CI dashboard (E2E)
    // Type: e2e
    // Expected: Clicking CI badge navigates to CircleCI build status page
    test('TC4-E2E: Clicking CI badge navigates to CircleCI build status page', async ({ page, context }) => {
        // Locate the badge link
        const badgeLink = page.locator('.badges a');
        await expect(badgeLink).toBeVisible();

        // Verify the href points to CircleCI
        const href = await badgeLink.getAttribute('href');
        expect(href).toContain('circleci.com');
        expect(href).toContain('yetone/mirdb');

        // Verify target="_blank" is set
        const target = await badgeLink.getAttribute('target');
        expect(target).toBe('_blank');

        // Verify rel="noopener noreferrer" for security
        const rel = await badgeLink.getAttribute('rel');
        expect(rel).toContain('noopener');
    });

    // Test for View on GitHub button navigation
    test('TC-E2E: View on GitHub button is visible and has correct href', async ({ page }) => {
        // Locate the "View on GitHub" button
        const githubBtn = page.locator('a.btn-secondary:has-text("View on GitHub")');
        await expect(githubBtn).toBeVisible();

        // Verify the href
        const href = await githubBtn.getAttribute('href');
        expect(href).toBe('https://github.com/yetone/mirdb');

        // Verify external link attributes
        const target = await githubBtn.getAttribute('target');
        expect(target).toBe('_blank');

        const rel = await githubBtn.getAttribute('rel');
        expect(rel).toContain('noopener');
    });

    // Test footer GitHub links
    test('TC-E2E: Footer contains GitHub links', async ({ page }) => {
        // Locate the footer
        const footer = page.locator('footer');
        await expect(footer).toBeVisible();

        // Check for GitHub link in footer
        const footerGithubLink = footer.locator('a:has-text("GitHub")');
        await expect(footerGithubLink).toBeVisible();

        const href = await footerGithubLink.getAttribute('href');
        expect(href).toBe('https://github.com/yetone/mirdb');
    });

    // Test for multiple GitHub links on page
    test('TC-E2E: Page contains multiple GitHub links', async ({ page }) => {
        // Count all GitHub links
        const githubLinks = page.locator('a[href*="github.com"]');
        const count = await githubLinks.count();

        // Should have at least 3 GitHub links (hero button, footer GitHub, Documentation, License)
        expect(count).toBeGreaterThanOrEqual(3);
    });
});
