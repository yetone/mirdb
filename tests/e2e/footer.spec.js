/**
 * Footer E2E Tests
 * Owner: Scenario 7 - Footer Content and Information
 *
 * Tests for:
 * - Footer presence
 * - Version information
 * - License information
 * - GitHub link in footer
 */

const { test, expect } = require('@playwright/test');

test.describe('Footer', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test('footer element is present at bottom of page', async ({ page }) => {
        // Locate the footer element
        const footer = page.locator('footer');

        // Verify footer exists
        await expect(footer).toBeVisible();

        // Verify footer has the correct class
        await expect(footer).toHaveClass(/site-footer/);

        // Verify footer is positioned at the bottom (after main content)
        const main = page.locator('main');
        await expect(main).toBeVisible();

        // Get bounding boxes to verify footer is below main content
        const mainBox = await main.boundingBox();
        const footerBox = await footer.boundingBox();

        expect(footerBox.y).toBeGreaterThanOrEqual(mainBox.y + mainBox.height);
    });

    test('footer displays version information', async ({ page }) => {
        const footer = page.locator('footer');

        // Check for version text - could be "Version X.X.X" or "v1.0.0" format
        const versionPattern = /(version|v)\s*\d+\.\d+(\.\d+)?/i;
        const footerText = await footer.textContent();

        expect(footerText).toMatch(versionPattern);

        // Also verify there's a visible version element
        const versionElement = footer.locator('.footer-version');
        await expect(versionElement).toBeVisible();
    });

    test('footer displays license information', async ({ page }) => {
        const footer = page.locator('footer');

        // Check for common license types mentioned
        const licensePattern = /(MIT|Apache|GPL|BSD|ISC|License)/i;
        const footerText = await footer.textContent();

        expect(footerText).toMatch(licensePattern);

        // Verify there's a visible license element
        const licenseElement = footer.locator('.footer-license');
        await expect(licenseElement).toBeVisible();
    });

    test('footer contains GitHub link', async ({ page }) => {
        const footer = page.locator('footer');

        // Find GitHub link within the footer
        const githubLink = footer.locator('a[href*="github"]');

        // Verify GitHub link exists and is visible
        await expect(githubLink).toBeVisible();

        // Verify link has correct attributes
        const href = await githubLink.getAttribute('href');
        expect(href).toContain('github.com');

        // Verify link opens in new tab for security
        await expect(githubLink).toHaveAttribute('target', '_blank');
        await expect(githubLink).toHaveAttribute('rel', /noopener/);
    });

    test('footer has proper semantic structure', async ({ page }) => {
        const footer = page.locator('footer');

        // Footer should contain copyright or project info
        const footerContent = footer.locator('.footer-content');
        await expect(footerContent).toBeVisible();

        // Verify footer has small text styling (informational content)
        const footerText = footer.locator('.footer-info');
        await expect(footerText).toBeVisible();
    });

    test('footer GitHub link contains proper icon or text', async ({ page }) => {
        const footer = page.locator('footer');
        const githubLink = footer.locator('a[href*="github"]');

        // Check that GitHub link has either an icon or descriptive text
        const linkText = await githubLink.textContent();
        const hasIcon = await githubLink.locator('svg').count() > 0;

        // Either has "GitHub" text or an SVG icon
        expect(linkText.toLowerCase().includes('github') || hasIcon).toBe(true);
    });
});
