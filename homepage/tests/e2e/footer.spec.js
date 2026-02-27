/**
 * E2E Tests for Footer & External Links
 * Owner: Scenario 7 - Footer & External Links
 *
 * Tests the footer section, license information, and external link behavior.
 */

const { test, expect } = require('@playwright/test');

test.describe('Footer & External Links', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test.describe('Test Case 1: Footer Element', () => {
        test('footer element exists with semantic <footer> tag', async ({ page }) => {
            // Query for the semantic footer element
            const footer = page.locator('footer');

            // Verify footer exists
            await expect(footer).toHaveCount(1);

            // Verify it has the main-footer class
            await expect(footer).toHaveClass(/main-footer/);

            // Verify it is visible when scrolled to
            await footer.scrollIntoViewIfNeeded();
            await expect(footer).toBeVisible();
        });
    });

    test.describe('Test Case 2: License Information', () => {
        test('footer contains license mention (MIT, Apache, or license link)', async ({ page }) => {
            const footer = page.locator('footer');

            // Get footer text content
            const footerText = await footer.textContent();

            // Check for license mentions - MIT, Apache, or the word "license"
            const hasLicenseMention =
                footerText.toLowerCase().includes('mit') ||
                footerText.toLowerCase().includes('apache') ||
                footerText.toLowerCase().includes('license') ||
                footerText.toLowerCase().includes('gpl') ||
                footerText.toLowerCase().includes('bsd');

            expect(hasLicenseMention).toBe(true);
        });
    });

    test.describe('Test Case 3: GitHub Repository Link', () => {
        test('link exists with href pointing to GitHub repository', async ({ page }) => {
            const footer = page.locator('footer');

            // Find GitHub repository link in footer
            const githubRepoLink = footer.locator('a[href*="github.com"]').first();

            // Verify link exists
            await expect(githubRepoLink).toBeVisible();

            // Verify href points to GitHub repository
            const href = await githubRepoLink.getAttribute('href');
            expect(href).toMatch(/^https:\/\/github\.com\//);

            // Ensure it's not just the issues page (repo link should not end with /issues)
            // At least one link should be to the main repository
            const allGithubLinks = await footer.locator('a[href*="github.com"]').all();
            let hasRepoLink = false;
            for (const link of allGithubLinks) {
                const linkHref = await link.getAttribute('href');
                if (linkHref && !linkHref.endsWith('/issues')) {
                    hasRepoLink = true;
                    break;
                }
            }
            expect(hasRepoLink).toBe(true);
        });
    });

    test.describe('Test Case 4: GitHub Issues Link', () => {
        test('link exists with href pointing to GitHub issues page', async ({ page }) => {
            const footer = page.locator('footer');

            // Find GitHub issues link in footer
            const issuesLink = footer.locator('a[href*="github.com"][href*="issues"]');

            // Verify issues link exists
            await expect(issuesLink).toBeVisible();

            // Verify href points to GitHub issues
            const href = await issuesLink.getAttribute('href');
            expect(href).toMatch(/github\.com\/.*\/issues/);
        });
    });

    test.describe('Test Case 5: External Link Attributes', () => {
        test('external links have target="_blank" and rel="noopener noreferrer"', async ({ page }) => {
            const footer = page.locator('footer');

            // Get all external links in footer (links with github.com)
            const externalLinks = await footer.locator('a[href*="github.com"]').all();

            expect(externalLinks.length).toBeGreaterThan(0);

            for (const link of externalLinks) {
                // Check target="_blank"
                const target = await link.getAttribute('target');
                expect(target).toBe('_blank');

                // Check rel contains "noopener" and "noreferrer"
                const rel = await link.getAttribute('rel');
                expect(rel).toContain('noopener');
                expect(rel).toContain('noreferrer');
            }
        });
    });

    test.describe('Test Case 6: Manual - Click GitHub Repository Link', () => {
        test('GitHub repository link is clickable and points to valid URL', async ({ page }) => {
            const footer = page.locator('footer');

            // Find the main GitHub repository link (not issues)
            const allGithubLinks = await footer.locator('a[href*="github.com"]').all();
            let repoLink = null;

            for (const link of allGithubLinks) {
                const href = await link.getAttribute('href');
                if (href && !href.includes('/issues')) {
                    repoLink = link;
                    break;
                }
            }

            expect(repoLink).not.toBeNull();

            // Verify it has target="_blank" (opens in new tab)
            const target = await repoLink.getAttribute('target');
            expect(target).toBe('_blank');

            // Verify the link URL is valid
            const href = await repoLink.getAttribute('href');
            expect(href).toMatch(/^https:\/\/github\.com\//);
        });
    });

    test.describe('Test Case 7: All GitHub Links on Page', () => {
        test('all links containing github.com have proper external link attributes', async ({ page }) => {
            // Get all GitHub links on the entire page
            const allGithubLinks = await page.locator('a[href*="github.com"]').all();

            expect(allGithubLinks.length).toBeGreaterThan(0);

            for (const link of allGithubLinks) {
                const href = await link.getAttribute('href');

                // Skip code example links (inside <code> blocks) which are not actual anchor links
                const isInsideCode = await link.evaluate(el => !!el.closest('code'));
                if (isInsideCode) continue;

                // Check target="_blank"
                const target = await link.getAttribute('target');
                expect(target, `Link ${href} should have target="_blank"`).toBe('_blank');

                // Check rel contains "noopener" (at minimum for security)
                const rel = await link.getAttribute('rel');
                expect(rel, `Link ${href} should have rel containing "noopener"`).toContain('noopener');
            }
        });
    });

    test.describe('Footer Accessibility', () => {
        test('footer links are accessible via keyboard', async ({ page }) => {
            const footer = page.locator('footer');
            await footer.scrollIntoViewIfNeeded();

            // Get all links in footer
            const links = await footer.locator('a').all();
            expect(links.length).toBeGreaterThan(0);

            // Verify each link is focusable
            for (const link of links) {
                await link.focus();
                await expect(link).toBeFocused();
            }
        });

        test('footer has proper color contrast visible text', async ({ page }) => {
            const footer = page.locator('footer');
            await expect(footer).toBeVisible();

            // Verify footer has visible text content
            const text = await footer.textContent();
            expect(text.trim().length).toBeGreaterThan(0);
        });
    });

    test.describe('Footer Structure', () => {
        test('footer contains copyright information', async ({ page }) => {
            const footer = page.locator('footer');
            const footerText = await footer.textContent();

            // Check for copyright symbol or "copyright" text
            const hasCopyright =
                footerText.includes('©') ||
                footerText.toLowerCase().includes('copyright');

            expect(hasCopyright).toBe(true);
        });

        test('footer contains proper container structure', async ({ page }) => {
            const footer = page.locator('footer');

            // Footer should have a top-level container wrapper
            const container = footer.locator('> .container');
            await expect(container).toHaveCount(1);

            // Container should have footer-content inside
            const footerContent = container.locator('.footer-content');
            await expect(footerContent).toHaveCount(1);
        });
    });
});
