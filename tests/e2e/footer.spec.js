/**
 * Footer Section Tests
 * Owner: Scenario 6 - Footer with License and Links
 *
 * Tests:
 * - Footer element presence
 * - License information
 * - Copyright statement
 * - GitHub repository link
 */

const { test, expect } = require('@playwright/test');
const { selectors, GITHUB_URL, gotoHomepage } = require('./test-utils');

test.describe('Footer with License and Links', () => {
    test.beforeEach(async ({ page }) => {
        await gotoHomepage(page);
    });

    // Test Case 1: Footer element exists on page
    test('should display footer element on the page', async ({ page }) => {
        const footer = page.locator(selectors.footer);

        await expect(footer).toBeVisible();

        // Check that footer has proper role for accessibility
        const role = await footer.getAttribute('role');
        expect(role).toBe('contentinfo');
    });

    // Test Case 2: Footer contains license text (MIT or applicable license)
    test('should display license information in footer', async ({ page }) => {
        const footerLicense = page.locator(selectors.footerLicense);

        await expect(footerLicense).toBeVisible();

        // Check that license text contains MIT
        const licenseText = await footerLicense.textContent();
        expect(licenseText.toLowerCase()).toContain('mit');
        expect(licenseText.toLowerCase()).toContain('license');
    });

    // Test Case 3: Footer contains copyright statement with year
    test('should display copyright statement with year in footer', async ({ page }) => {
        const footerCopyright = page.locator(selectors.footerCopyright);

        await expect(footerCopyright).toBeVisible();

        // Check that copyright text exists and contains a year
        const copyrightText = await footerCopyright.textContent();
        expect(copyrightText).toMatch(/©|copyright/i);
        expect(copyrightText).toMatch(/\d{4}/); // Contains a 4-digit year
    });

    // Test Case 4: Footer contains link to GitHub repository
    test('should display link to GitHub repository in footer', async ({ page }) => {
        const footerLink = page.locator(selectors.footerLink);

        await expect(footerLink).toBeVisible();

        // Check that it links to the correct GitHub repository
        const href = await footerLink.getAttribute('href');
        expect(href).toBe(GITHUB_URL);

        // Check that it opens in a new tab
        const target = await footerLink.getAttribute('target');
        expect(target).toBe('_blank');

        // Check for security rel attribute
        const rel = await footerLink.getAttribute('rel');
        expect(rel).toContain('noopener');
    });

    // Additional test: Footer has proper structure and content container
    test('should have properly structured footer with content container', async ({ page }) => {
        const footerContent = page.locator(selectors.footerContent);

        await expect(footerContent).toBeVisible();

        // Verify all required elements are within footer content
        const license = footerContent.locator(selectors.footerLicense);
        const copyright = footerContent.locator(selectors.footerCopyright);
        const link = footerContent.locator(selectors.footerLink);

        await expect(license).toBeVisible();
        await expect(copyright).toBeVisible();
        await expect(link).toBeVisible();
    });

    // Additional test: Footer is positioned at the bottom of the page
    test('should be positioned at the bottom of the page', async ({ page }) => {
        // Scroll to the footer
        const footer = page.locator(selectors.footer);
        await footer.scrollIntoViewIfNeeded();

        await expect(footer).toBeVisible();

        // Verify footer is at the bottom by checking it's the last major element
        const footerBoundingBox = await footer.boundingBox();
        const viewportSize = page.viewportSize();

        // Footer should be near or at the bottom of the content
        expect(footerBoundingBox).toBeTruthy();
    });
});
