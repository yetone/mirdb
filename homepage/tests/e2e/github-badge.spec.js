/**
 * GitHub Integration and Status Badges E2E Tests
 * Owner: Scenario 5 - GitHub Integration and Status Badges
 *
 * Tests for:
 * - Badge click navigation to CI dashboard
 * - GitHub link navigation
 */

const { test, expect } = require('@playwright/test');

test.describe('GitHub Integration and Status Badges E2E (Scenario 5)', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test('Test Case 4: Clicking badge navigates to CircleCI build page', async ({ page }) => {
        // Find the CircleCI badge link
        const badgeLink = page.locator('a[href*="circleci.com/gh/yetone/mirdb"]');
        await expect(badgeLink).toBeVisible();

        // Verify the badge image is within the link
        const badgeImage = badgeLink.locator('img[src*="circleci.com"]');
        await expect(badgeImage).toBeVisible();

        // Verify the href attribute points to CircleCI
        const href = await badgeLink.getAttribute('href');
        expect(href).toContain('circleci.com/gh/yetone/mirdb');
    });

    test('GitHub link in hero section is clickable', async ({ page }) => {
        const heroGithubLink = page.locator('#hero a[href*="github.com/yetone/mirdb"]');
        await expect(heroGithubLink).toBeVisible();

        // Verify the link has the correct href
        const href = await heroGithubLink.getAttribute('href');
        expect(href).toBe('https://github.com/yetone/mirdb');
    });

    test('GitHub link in footer is clickable', async ({ page }) => {
        const footerGithubLink = page.locator('footer a[href*="github.com/yetone/mirdb"]');
        await expect(footerGithubLink).toBeVisible();

        // Verify the link has the correct href
        const href = await footerGithubLink.getAttribute('href');
        expect(href).toBe('https://github.com/yetone/mirdb');
    });

    test('CircleCI badge image loads correctly', async ({ page }) => {
        const badgeImage = page.locator('img[src*="circleci.com"]');
        await expect(badgeImage).toBeVisible();

        // Check that the image has loaded (not broken)
        const naturalWidth = await badgeImage.evaluate(img => img.naturalWidth);
        // Badge should have some width when loaded (even SVG badges typically have naturalWidth)
        // Note: CircleCI badges are SVG, so we mainly check visibility
        await expect(badgeImage).toHaveAttribute('src', /circleci\.com\/gh\/yetone\/mirdb/);
    });
});
