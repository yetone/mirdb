// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Footer Content Tests (REQ-8, PRD Footer Requirements)
 * Verifies that footer contains all required information:
 * - GitHub repository link
 * - License information
 * - Version/release information
 */
test.describe('Footer Content', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    // Test Case 1: Footer contains GitHub repository link
    test('TC1: Footer should contain GitHub repository link', async ({ page }) => {
        // Locate the footer element
        const footer = page.locator('footer.footer');
        await expect(footer).toBeVisible();

        // Find GitHub link in footer
        const githubLink = footer.locator('a[href*="github.com"]');
        await expect(githubLink).toBeVisible();

        // Verify link points to correct repository
        const href = await githubLink.getAttribute('href');
        expect(href).toBe('https://github.com/yetone/mirdb');

        // Verify link has proper security attributes
        const rel = await githubLink.getAttribute('rel');
        expect(rel).toContain('noopener');
        expect(rel).toContain('noreferrer');

        // Verify link opens in new tab
        const target = await githubLink.getAttribute('target');
        expect(target).toBe('_blank');

        // Verify link text mentions GitHub
        const linkText = await githubLink.textContent();
        expect(linkText?.toLowerCase()).toContain('github');
    });

    // Test Case 2: Footer contains license information
    test('TC2: Footer should display license information', async ({ page }) => {
        // Locate the footer element
        const footer = page.locator('footer.footer');
        await expect(footer).toBeVisible();

        // Get footer text content
        const footerText = await footer.textContent();
        expect(footerText).not.toBeNull();

        // Check for license information (MIT License as specified in package.json)
        const hasLicenseInfo = footerText?.toLowerCase().includes('license') ||
                              footerText?.toLowerCase().includes('mit') ||
                              footerText?.toLowerCase().includes('open source');

        expect(hasLicenseInfo).toBe(true);

        // Verify specific license mention exists
        const licenseElement = footer.locator('text=/license/i');
        await expect(licenseElement).toBeVisible();
    });

    // Test Case 3: Footer contains version/release information
    test('TC3: Footer should display version or release information', async ({ page }) => {
        // Locate the footer element
        const footer = page.locator('footer.footer');
        await expect(footer).toBeVisible();

        // Get footer text content
        const footerText = await footer.textContent();
        expect(footerText).not.toBeNull();

        // Check for version information
        // Version can be displayed as "v1.0.0", "Version 1.0.0", "1.0.0", or similar
        const hasVersionInfo = footerText?.toLowerCase().includes('version') ||
                              footerText?.match(/v?\d+\.\d+\.\d+/) !== null ||
                              footerText?.toLowerCase().includes('release');

        expect(hasVersionInfo).toBe(true);

        // Verify version element exists with semantic versioning format
        const versionElement = footer.locator('[data-testid="version"]');
        await expect(versionElement).toBeVisible();

        // Verify version follows semantic versioning pattern (e.g., 1.0.0)
        const versionText = await versionElement.textContent();
        expect(versionText).toMatch(/v?\d+\.\d+\.\d+/);
    });

    // Additional test: Footer has all required elements together
    test('TC4: Footer should have all required content elements', async ({ page }) => {
        const footer = page.locator('footer.footer');
        await expect(footer).toBeVisible();

        // Verify footer container exists
        const footerContainer = footer.locator('.container');
        await expect(footerContainer).toBeVisible();

        // Count the paragraph elements - should have content organized properly
        const paragraphs = footer.locator('p');
        const paragraphCount = await paragraphs.count();
        expect(paragraphCount).toBeGreaterThanOrEqual(2);

        // Verify copyright year exists (should be a 4-digit year)
        const footerText = await footer.textContent();
        const hasValidYear = footerText?.match(/©\s*\d{4}/) !== null ||
                            footerText?.match(/\d{4}\s*MirDB/) !== null;
        expect(hasValidYear).toBe(true);
    });

    // Test: Footer is visible when scrolling to bottom
    test('TC5: Footer should be visible when scrolling to bottom of page', async ({ page }) => {
        // Scroll to the bottom of the page
        await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));

        // Wait for any scroll animations to complete
        await page.waitForTimeout(500);

        // Verify footer is visible in viewport
        const footer = page.locator('footer.footer');
        await expect(footer).toBeInViewport();
    });
});
