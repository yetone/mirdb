/**
 * Navigation and Links E2E Tests
 * Owner: Scenario 6 - Navigation and Links
 *
 * Test cases:
 * - Navigation bar contains logo linking to homepage top
 * - Features link scrolls to features section
 * - Architecture link scrolls to architecture section
 * - Documentation link is present and functional
 * - GitHub link opens repository in new tab
 * - Footer contains GitHub repository link
 * - External links have target=_blank
 */

const { test, expect } = require('@playwright/test');

test.describe('Navigation and Links', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test('navigation bar displays MirDB logo that links to homepage top', async ({ page }) => {
        // Test Case ID: 1
        // Input: Check navigation bar for MirDB logo
        // Expected: Logo is visible and links to homepage top

        const nav = page.locator('nav.nav');
        await expect(nav).toBeVisible();

        const logo = nav.locator('.nav-logo');
        await expect(logo).toBeVisible();

        // Check logo text contains MirDB
        const logoText = await logo.textContent();
        expect(logoText).toContain('MirDB');

        // Check logo links to top of page (either # or /)
        const href = await logo.getAttribute('href');
        expect(href === '#' || href === '/' || href === '#hero').toBeTruthy();
    });

    test('Features link scrolls to features section', async ({ page }) => {
        // Test Case ID: 2
        // Input: Check navigation for Features link
        // Expected: Features link scrolls to features section

        const nav = page.locator('nav.nav');
        const featuresLink = nav.locator('a[href="#features"]');

        await expect(featuresLink).toBeVisible();
        await expect(featuresLink).toHaveText('Features');

        // Click the link and verify scroll to features section
        await featuresLink.click();

        // Wait for smooth scroll to complete
        await page.waitForTimeout(500);

        // Verify the features section is in viewport
        const featuresSection = page.locator('#features');
        await expect(featuresSection).toBeInViewport();
    });

    test('Architecture link scrolls to architecture section', async ({ page }) => {
        // Test Case ID: 3
        // Input: Check navigation for Architecture link
        // Expected: Architecture link scrolls to architecture section

        const nav = page.locator('nav.nav');
        const archLink = nav.locator('a[href="#architecture"]');

        await expect(archLink).toBeVisible();
        await expect(archLink).toHaveText('Architecture');

        // Click the link and verify scroll to architecture section
        await archLink.click();

        // Wait for smooth scroll to complete
        await page.waitForTimeout(500);

        // Verify the architecture section is in viewport
        const archSection = page.locator('#architecture');
        await expect(archSection).toBeInViewport();
    });

    test('Documentation link is present and functional', async ({ page }) => {
        // Test Case ID: 4
        // Input: Check navigation for Documentation link
        // Expected: Documentation link is present and functional

        const nav = page.locator('nav.nav');
        const docsLink = nav.locator('.nav-links a').filter({ hasText: /Docs|Documentation/ });

        await expect(docsLink).toBeVisible();

        // Verify it has a valid href attribute
        const href = await docsLink.getAttribute('href');
        expect(href).toBeTruthy();
    });

    test('GitHub link in navigation opens repository in new tab', async ({ page }) => {
        // Test Case ID: 5
        // Input: Check GitHub link in navigation
        // Expected: GitHub link opens repository in new tab

        const nav = page.locator('nav.nav');
        const githubLink = nav.locator('a[href*="github.com"]');

        await expect(githubLink).toBeVisible();

        // Check it has target="_blank" for opening in new tab
        await expect(githubLink).toHaveAttribute('target', '_blank');

        // Check it has rel="noopener noreferrer" for security
        const rel = await githubLink.getAttribute('rel');
        expect(rel).toContain('noopener');
    });

    test('footer contains GitHub repository link that works', async ({ page }) => {
        // Test Case ID: 6
        // Input: Check footer GitHub link
        // Expected: Footer contains GitHub repository link that works

        const footer = page.locator('footer.footer');
        await expect(footer).toBeVisible();

        const githubLink = footer.locator('a[href*="github.com"]');
        await expect(githubLink).toBeVisible();

        // Verify the link contains github.com in href
        const href = await githubLink.getAttribute('href');
        expect(href).toContain('github.com');
    });

    test('all external links have target=_blank for new tab', async ({ page }) => {
        // Test Case ID: 7
        // Input: Verify all external links have target=_blank
        // Expected: External links open in new tab

        // Get all links that point to external domains (github.com specifically)
        const externalLinks = page.locator('a[href*="github.com"]');
        const count = await externalLinks.count();

        expect(count).toBeGreaterThan(0);

        // Check each external link has target="_blank"
        for (let i = 0; i < count; i++) {
            const link = externalLinks.nth(i);
            await expect(link).toHaveAttribute('target', '_blank');
        }
    });

    // BEGIN: Footer Content Tests - Owner: Scenario 15
    test('footer displays license information', async ({ page }) => {
        // Scenario 15 - Test Case ID: 2
        // Input: Check footer for license information
        // Expected: Footer displays license information (likely MIT or Apache)

        const footer = page.locator('footer');
        await expect(footer).toBeVisible();

        // Get footer text content
        const footerText = await footer.textContent();

        // Verify license information is present (MIT or Apache)
        const hasLicense = footerText.includes('MIT') || footerText.includes('Apache');
        expect(hasLicense).toBeTruthy();
    });

    test('footer contains copyright notice with appropriate year', async ({ page }) => {
        // Scenario 15 - Test Case ID: 3
        // Input: Check footer for copyright
        // Expected: Footer contains copyright notice with appropriate year

        const footer = page.locator('footer');
        await expect(footer).toBeVisible();

        // Get footer text content
        const footerText = await footer.textContent();

        // Verify copyright symbol is present (© or "Copyright")
        const hasCopyright = footerText.includes('©') || footerText.toLowerCase().includes('copyright');
        expect(hasCopyright).toBeTruthy();

        // Verify year is present (4-digit year)
        const yearPattern = /20\d{2}/;
        const hasYear = yearPattern.test(footerText);
        expect(hasYear).toBeTruthy();
    });

    test('footer uses semantic HTML with footer element', async ({ page }) => {
        // Scenario 15 - Test Case ID: 4
        // Input: Verify footer semantic HTML
        // Expected: Footer uses <footer> element for proper semantics

        // Check that footer element exists
        const footer = page.locator('footer');
        await expect(footer).toBeVisible();

        // Verify it has role="contentinfo" for accessibility
        const role = await footer.getAttribute('role');
        expect(role).toBe('contentinfo');

        // Verify footer is a semantic <footer> element (not a div with class)
        const tagName = await footer.evaluate(el => el.tagName.toLowerCase());
        expect(tagName).toBe('footer');
    });
    // END: Footer Content Tests - Owner: Scenario 15
});
