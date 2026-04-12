/**
 * External Links and Status Badges E2E Tests
 * Owner: Scenario 5 - External Links and Status Badges
 *
 * Tests for GitHub link and CircleCI badge display and functionality.
 * Validates REQ-6, REQ-7, and User Story 5.
 */

import { test, expect } from '@playwright/test';

const GITHUB_URL = 'https://github.com/yetone/mirdb';
const CIRCLECI_URL = 'https://circleci.com/gh/yetone/mirdb';
const CIRCLECI_BADGE_URL = 'atompunk.yetone.fun';

test.describe('External Links and Status Badges', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test('TC1: GitHub link with correct href exists', async ({ page }) => {
        // Query for GitHub link element anywhere on the page
        const githubLinks = page.locator(`a[href="${GITHUB_URL}"]`);

        // At least one GitHub link should exist
        const count = await githubLinks.count();
        expect(count).toBeGreaterThan(0);

        // Check first GitHub link is visible
        await expect(githubLinks.first()).toBeVisible();

        // Verify href attribute
        const href = await githubLinks.first().getAttribute('href');
        expect(href).toBe(GITHUB_URL);
    });

    test('TC2: CircleCI badge image exists with correct src', async ({ page }) => {
        // Query for CircleCI badge image - check for badge URL in src
        const badgeImage = page.locator(`img[src*="${CIRCLECI_BADGE_URL}"], img[src*="circleci"]`);

        // Badge image should exist
        const count = await badgeImage.count();
        expect(count).toBeGreaterThan(0);

        // Badge should be visible
        await expect(badgeImage.first()).toBeVisible();

        // Verify the src contains the badge URL
        const src = await badgeImage.first().getAttribute('src');
        expect(src).toBeTruthy();
        expect(src!.includes(CIRCLECI_BADGE_URL) || src!.includes('circleci')).toBe(true);
    });

    test('TC3: CircleCI badge links to correct CircleCI page', async ({ page }) => {
        // Find the CircleCI link that wraps the badge
        const circleciLink = page.locator(`a[href="${CIRCLECI_URL}"]`);

        // At least one CircleCI link should exist
        const count = await circleciLink.count();
        expect(count).toBeGreaterThan(0);

        // Verify href attribute
        const href = await circleciLink.first().getAttribute('href');
        expect(href).toBe(CIRCLECI_URL);

        // Check the badge image is inside the link (in hero section)
        const badgeInLink = circleciLink.first().locator('img');
        const imgCount = await badgeInLink.count();
        if (imgCount > 0) {
            // If there's an image inside, verify it's the badge
            await expect(badgeInLink).toBeVisible();
        }
    });

    test('TC4: GitHub link has icon or visual indicator', async ({ page }) => {
        // Check for GitHub links with associated icons or visual styling
        const githubLinks = page.locator(`a[href="${GITHUB_URL}"]`);

        // Check each GitHub link for visual indicators
        const count = await githubLinks.count();
        expect(count).toBeGreaterThan(0);

        let hasVisualIndicator = false;

        for (let i = 0; i < count; i++) {
            const link = githubLinks.nth(i);
            const isVisible = await link.isVisible();

            if (isVisible) {
                // Check for icon (emoji, SVG, or image) within or near the link
                const icon = link.locator('.resource-icon, svg, img, [class*="icon"]');
                const iconCount = await icon.count();

                // Check for recognizable text (GitHub)
                const text = await link.textContent();

                // Check if link has styling (class) that makes it recognizable
                const className = await link.getAttribute('class');

                if (iconCount > 0 || text?.includes('GitHub') || className?.includes('resource')) {
                    hasVisualIndicator = true;
                    break;
                }
            }
        }

        expect(hasVisualIndicator).toBe(true);
    });

    test('TC5: GitHub link opens correct destination (verify href)', async ({ page }) => {
        // Find GitHub link and verify it will open the correct URL
        const githubLinks = page.locator(`a[href="${GITHUB_URL}"]`);

        // At least one link should exist
        const count = await githubLinks.count();
        expect(count).toBeGreaterThan(0);

        // Verify all GitHub links have the correct href
        for (let i = 0; i < count; i++) {
            const link = githubLinks.nth(i);
            const isVisible = await link.isVisible();

            if (isVisible) {
                const href = await link.getAttribute('href');
                expect(href).toBe(GITHUB_URL);
            }
        }

        // Verify the navigation link in header specifically
        const navGithubLink = page.locator('nav a[href*="github"]');
        const navCount = await navGithubLink.count();
        if (navCount > 0) {
            const href = await navGithubLink.first().getAttribute('href');
            expect(href).toBe(GITHUB_URL);
        }
    });

    test('TC6: External links have target="_blank" and rel="noopener"', async ({ page }) => {
        // Get all external links (GitHub and CircleCI)
        const externalUrls = [GITHUB_URL, CIRCLECI_URL];

        for (const url of externalUrls) {
            const links = page.locator(`a[href="${url}"]`);
            const count = await links.count();

            for (let i = 0; i < count; i++) {
                const link = links.nth(i);
                const isVisible = await link.isVisible();

                if (isVisible) {
                    // Check target attribute
                    const target = await link.getAttribute('target');
                    expect(target).toBe('_blank');

                    // Check rel attribute contains 'noopener'
                    const rel = await link.getAttribute('rel');
                    expect(rel).toBeTruthy();
                    expect(rel).toContain('noopener');
                }
            }
        }
    });

    test('Resources section contains external links', async ({ page }) => {
        // Verify the resources section exists
        const resourcesSection = page.locator('#resources, .resources');
        await expect(resourcesSection).toBeVisible();

        // Check for GitHub link in resources
        const githubInResources = resourcesSection.locator(`a[href="${GITHUB_URL}"]`);
        await expect(githubInResources).toBeVisible();

        // Check for CircleCI link in resources
        const circleciInResources = resourcesSection.locator(`a[href="${CIRCLECI_URL}"]`);
        await expect(circleciInResources).toBeVisible();
    });

    test('Navigation contains GitHub link', async ({ page }) => {
        // Verify GitHub link in header navigation (not footer)
        const headerNav = page.locator('header nav');
        const githubInNav = headerNav.locator(`a[href="${GITHUB_URL}"]`);

        await expect(githubInNav).toBeVisible();

        // Verify text says GitHub
        const text = await githubInNav.textContent();
        expect(text).toContain('GitHub');
    });

    test('Hero section contains CircleCI badge', async ({ page }) => {
        // Verify hero section has the badge
        const heroSection = page.locator('#hero, .hero');
        await expect(heroSection).toBeVisible();

        // Check for badges container
        const badgesContainer = heroSection.locator('.hero-badges');
        await expect(badgesContainer).toBeVisible();

        // Badge image should be in hero
        const badgeImage = badgesContainer.locator('img');
        await expect(badgeImage).toBeVisible();

        // Badge should link to CircleCI
        const badgeLink = badgesContainer.locator(`a[href="${CIRCLECI_URL}"]`);
        await expect(badgeLink).toBeVisible();
    });

    test('CircleCI badge has meaningful alt text', async ({ page }) => {
        // Find the badge image
        const badgeImage = page.locator(`img[src*="${CIRCLECI_BADGE_URL}"], img[src*="circleci"]`);
        await expect(badgeImage.first()).toBeVisible();

        // Check alt text exists and is meaningful
        const alt = await badgeImage.first().getAttribute('alt');
        expect(alt).toBeTruthy();
        expect(alt!.length).toBeGreaterThan(0);

        // Alt text should describe build status
        const lowerAlt = alt!.toLowerCase();
        expect(lowerAlt.includes('build') || lowerAlt.includes('status') || lowerAlt.includes('circleci')).toBe(true);
    });
});
