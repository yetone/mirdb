// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * GitHub Repository Link Tests (REQ-8)
 * Verifies that GitHub repository links are functional and secure
 */
test.describe('GitHub Repository Links', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test('TC1: GitHub link in hero section opens repository in new tab', async ({ page }) => {
        // Locate the GitHub button in hero section
        const githubBtn = page.locator('#github-btn');

        // Verify button is visible
        await expect(githubBtn).toBeVisible();

        // Verify button text
        await expect(githubBtn).toHaveText('View on GitHub');

        // Verify button has correct href pointing to GitHub repository
        const href = await githubBtn.getAttribute('href');
        expect(href).toBe('https://github.com/yetone/mirdb');

        // Verify target="_blank" for new tab behavior
        const target = await githubBtn.getAttribute('target');
        expect(target).toBe('_blank');
    });

    test('TC2: GitHub link in footer opens repository in new tab', async ({ page }) => {
        // Locate the footer GitHub link
        const footerGithubLink = page.locator('footer a[href*="github.com"]');

        // Verify link is visible
        await expect(footerGithubLink).toBeVisible();

        // Verify link text contains GitHub reference
        const linkText = await footerGithubLink.textContent();
        expect(linkText).toContain('GitHub');

        // Verify link has correct href pointing to GitHub repository
        const href = await footerGithubLink.getAttribute('href');
        expect(href).toBe('https://github.com/yetone/mirdb');

        // Verify target="_blank" for new tab behavior
        const target = await footerGithubLink.getAttribute('target');
        expect(target).toBe('_blank');
    });

    test('TC3: GitHub links have proper security attributes', async ({ page }) => {
        // Test hero GitHub button security attributes
        const heroGithubBtn = page.locator('#github-btn');

        // Verify rel attribute contains noopener for security
        const heroRel = await heroGithubBtn.getAttribute('rel');
        expect(heroRel).toContain('noopener');
        expect(heroRel).toContain('noreferrer');

        // Test footer GitHub link security attributes
        const footerGithubLink = page.locator('footer a[href*="github.com"]');

        // Verify rel attribute contains noopener for security
        const footerRel = await footerGithubLink.getAttribute('rel');
        expect(footerRel).toContain('noopener');
        expect(footerRel).toContain('noreferrer');
    });

    test('GitHub links point to correct repository URL', async ({ page }) => {
        // Get all GitHub links on the page
        const githubLinks = page.locator('a[href*="github.com/yetone/mirdb"]');

        // Should have at least 2 links (hero and footer)
        const count = await githubLinks.count();
        expect(count).toBeGreaterThanOrEqual(2);

        // Verify all links point to the correct repository
        for (let i = 0; i < count; i++) {
            const href = await githubLinks.nth(i).getAttribute('href');
            expect(href).toBe('https://github.com/yetone/mirdb');
        }
    });
});
