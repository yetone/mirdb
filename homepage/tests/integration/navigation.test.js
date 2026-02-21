/**
 * Navigation Integration Tests
 * Owner: Scenario 5 - Navigation and External Links
 *
 * Tests:
 * - Navigation bar presence and links
 * - Smooth scroll to sections
 * - External links open in new tab
 * - Footer links functionality
 * - rel="noopener noreferrer" on external links
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

const homepageUrl = 'file://' + path.resolve(__dirname, '../../index.html');

/* ========================================
   Navigation Bar Tests
   ======================================== */
test.describe('Navigation Bar', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto(homepageUrl);
    });

    test('TC1: navigation bar (nav) is present at top of page', async ({ page }) => {
        // Test Case 1: Navigation bar (<nav>) is present at top of page
        const nav = page.locator('nav');
        await expect(nav).toBeVisible();

        // Check that nav has correct role
        const role = await nav.getAttribute('role');
        expect(role).toBe('navigation');

        // Check that nav has aria-label
        const ariaLabel = await nav.getAttribute('aria-label');
        expect(ariaLabel).toBe('Main navigation');
    });

    test('TC2: navigation contains Features link', async ({ page }) => {
        // Test Case 2: Link to '#features' or Features section exists in navigation
        const nav = page.locator('nav');
        const featuresLink = nav.locator('a[href="#features"]');

        await expect(featuresLink).toBeVisible();
        await expect(featuresLink).toContainText('Features');
    });

    test('TC3: navigation contains Quick Start link', async ({ page }) => {
        // Test Case 3: Link to '#quickstart' or Quick Start section exists in navigation
        const nav = page.locator('nav');
        const quickStartLink = nav.locator('a[href="#quickstart"]');

        await expect(quickStartLink).toBeVisible();
        await expect(quickStartLink).toContainText('Quick Start');
    });

    test('TC10: logo or text logo is present in navigation', async ({ page }) => {
        // Test Case 10: MirDB logo or text logo is present in navigation
        const nav = page.locator('nav');
        const logo = nav.locator('.nav-logo');

        await expect(logo).toBeVisible();
        await expect(logo).toContainText('MirDB');

        // Logo should link to homepage
        const href = await logo.getAttribute('href');
        expect(href).toBe('/');
    });
});

/* ========================================
   Internal Navigation Tests
   ======================================== */
test.describe('Internal Navigation', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto(homepageUrl);
    });

    test('TC4: clicking Features link scrolls to Features section', async ({ page }) => {
        // Test Case 4: Clicking Features link scrolls to or navigates to Features section
        const featuresLink = page.locator('nav a[href="#features"]');

        // Click the Features link
        await featuresLink.click();

        // Wait for scroll
        await page.waitForTimeout(500);

        // Check that features section is in viewport
        const featuresSection = page.locator('#features');
        await expect(featuresSection).toBeInViewport();
    });

    test('clicking Quick Start link scrolls to Quick Start section', async ({ page }) => {
        const quickStartLink = page.locator('nav a[href="#quickstart"]');

        // Click the Quick Start link
        await quickStartLink.click();

        // Wait for scroll
        await page.waitForTimeout(500);

        // Check that quickstart section is in viewport
        const quickstartSection = page.locator('#quickstart');
        await expect(quickstartSection).toBeInViewport();
    });
});

/* ========================================
   External Links Tests
   ======================================== */
test.describe('External Links', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto(homepageUrl);
    });

    test('TC5: GitHub repository link exists in navigation or footer', async ({ page }) => {
        // Test Case 5: Link to https://github.com/yetone/mirdb exists
        const githubLinks = page.locator('a[href="https://github.com/yetone/mirdb"]');
        const count = await githubLinks.count();

        // Should exist at least once (in nav and/or footer)
        expect(count).toBeGreaterThanOrEqual(1);
    });

    test('TC6: external links open in new tab (target=_blank)', async ({ page }) => {
        // Test Case 6: GitHub and external links have target='_blank'
        const navGithubLink = page.locator('nav a[href="https://github.com/yetone/mirdb"]');
        const footerGithubLink = page.locator('footer a[href="https://github.com/yetone/mirdb"]');

        // Check nav GitHub link
        const navTarget = await navGithubLink.getAttribute('target');
        expect(navTarget).toBe('_blank');

        // Check footer GitHub link
        const footerTarget = await footerGithubLink.getAttribute('target');
        expect(footerTarget).toBe('_blank');
    });

    test('TC7: external links have security attributes (rel=noopener noreferrer)', async ({ page }) => {
        // Test Case 7: External links have rel='noopener noreferrer'
        const externalLinks = page.locator('a[target="_blank"]');
        const count = await externalLinks.count();

        for (let i = 0; i < count; i++) {
            const link = externalLinks.nth(i);
            const rel = await link.getAttribute('rel');

            expect(rel).toContain('noopener');
            expect(rel).toContain('noreferrer');
        }
    });
});

/* ========================================
   Footer Tests
   ======================================== */
test.describe('Footer', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto(homepageUrl);
    });

    test('TC8: footer element is present at bottom of page', async ({ page }) => {
        // Test Case 8: Footer element (<footer>) is present at bottom of page
        const footer = page.locator('footer');
        await expect(footer).toBeVisible();

        // Check that footer has correct role
        const role = await footer.getAttribute('role');
        expect(role).toBe('contentinfo');
    });

    test('TC9: footer contains license information or link', async ({ page }) => {
        // Test Case 9: Footer displays license information or link
        const footer = page.locator('footer');

        // Check for license link
        const licenseLink = footer.locator('a:has-text("License")');
        await expect(licenseLink).toBeVisible();

        // Check that license link points to correct URL
        const href = await licenseLink.getAttribute('href');
        expect(href).toContain('LICENSE');
    });

    test('footer contains GitHub link', async ({ page }) => {
        const footer = page.locator('footer');
        const githubLink = footer.locator('a[href="https://github.com/yetone/mirdb"]');

        await expect(githubLink).toBeVisible();
    });

    test('footer contains Documentation link', async ({ page }) => {
        const footer = page.locator('footer');
        const docsLink = footer.locator('a:has-text("Documentation")');

        await expect(docsLink).toBeVisible();

        // Check that docs link opens in new tab
        const target = await docsLink.getAttribute('target');
        expect(target).toBe('_blank');
    });

    test('footer links have security attributes', async ({ page }) => {
        const footer = page.locator('footer');
        const externalLinks = footer.locator('a[target="_blank"]');
        const count = await externalLinks.count();

        expect(count).toBeGreaterThanOrEqual(1);

        for (let i = 0; i < count; i++) {
            const link = externalLinks.nth(i);
            const rel = await link.getAttribute('rel');

            expect(rel).toContain('noopener');
            expect(rel).toContain('noreferrer');
        }
    });
});
