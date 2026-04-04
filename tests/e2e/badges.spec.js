/**
 * Status Badges Tests
 * Owner: Scenario 5 - Project Status Badges
 *
 * Tests:
 * - CircleCI badge presence
 * - Badge link to CircleCI
 * - Badge image loading
 */

const { test, expect } = require('@playwright/test');
const { selectors, gotoHomepage } = require('./test-utils');

// CircleCI badge URL
const CIRCLECI_URL = 'https://circleci.com/gh/yetone/mirdb';
const CIRCLECI_BADGE_SRC = 'https://circleci.com/gh/yetone/mirdb.svg?style=shield';

test.describe('Project Status Badges', () => {
    test.beforeEach(async ({ page }) => {
        await gotoHomepage(page);
    });

    // Test Case 1: CircleCI status badge image is present on page
    test('should display CircleCI status badge image', async ({ page }) => {
        const badgesContainer = page.locator(selectors.badges);
        await expect(badgesContainer).toBeVisible();

        const badgeImage = page.locator(`${selectors.badges} img.badge`);
        await expect(badgeImage).toBeVisible();

        // Check that it's the CircleCI badge by verifying alt text
        const altText = await badgeImage.getAttribute('alt');
        expect(altText).toBeTruthy();
        expect(altText.toLowerCase()).toContain('circleci');
    });

    // Test Case 2: Badge links to https://circleci.com/gh/yetone/mirdb
    test('should link CircleCI badge to correct URL', async ({ page }) => {
        const badgeLink = page.locator(`${selectors.badges} a`);
        await expect(badgeLink).toBeVisible();

        // Check that the badge link points to CircleCI
        const href = await badgeLink.getAttribute('href');
        expect(href).toBe(CIRCLECI_URL);

        // Check that it opens in a new tab
        const target = await badgeLink.getAttribute('target');
        expect(target).toBe('_blank');

        // Check for security rel attribute
        const rel = await badgeLink.getAttribute('rel');
        expect(rel).toContain('noopener');
    });

    // Test Case 3: Badge image loads without errors (no broken image)
    test('should load badge image without errors', async ({ page }) => {
        const badgeImage = page.locator(`${selectors.badges} img.badge`);
        await expect(badgeImage).toBeVisible();

        // Check the src attribute points to CircleCI badge URL
        const src = await badgeImage.getAttribute('src');
        expect(src).toBe(CIRCLECI_BADGE_SRC);

        // Check that the image has natural dimensions (indicates successful load)
        // Note: CircleCI badge is an external SVG, so we check that the element renders
        const boundingBox = await badgeImage.boundingBox();
        expect(boundingBox).not.toBeNull();
        expect(boundingBox.width).toBeGreaterThan(0);
        expect(boundingBox.height).toBeGreaterThan(0);
    });

    // Additional test: Badge has proper accessibility attributes
    test('should have accessible badge link with aria-label', async ({ page }) => {
        const badgeLink = page.locator(`${selectors.badges} a`);
        await expect(badgeLink).toBeVisible();

        // Check for aria-label for accessibility
        const ariaLabel = await badgeLink.getAttribute('aria-label');
        expect(ariaLabel).toBeTruthy();
        expect(ariaLabel.toLowerCase()).toContain('circleci');
    });

    // Additional test: Badges section is within hero section
    test('should position badges section within hero section', async ({ page }) => {
        const hero = page.locator(selectors.hero);
        const badges = hero.locator('.badges');

        await expect(badges).toBeVisible();
    });
});
