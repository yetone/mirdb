/**
 * Accessibility E2E Tests - Screen Reader Compatibility
 * Owner: Scenario 9 - Accessibility - Screen Reader Compatibility
 *
 * Tests for semantic HTML structure, heading hierarchy, alt text,
 * and ARIA landmarks as specified in NFR-3 and User Story 7.
 */

import { test, expect } from '@playwright/test';

test.describe('Accessibility - Screen Reader Compatibility', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test('TC1: Page has semantic header element with appropriate content', async ({ page }) => {
        // Query for semantic header element
        const header = page.locator('header');

        // Check header exists and is visible
        await expect(header).toBeVisible();

        // Header should contain navigation
        const nav = header.locator('nav');
        await expect(nav).toBeVisible();

        // Header should contain the logo/brand link
        const logoLink = header.locator('a.nav-logo, a[href="#"]');
        await expect(logoLink).toBeVisible();

        // Header should contain navigation links
        const navLinks = header.locator('ul.nav-links li');
        const linkCount = await navLinks.count();
        expect(linkCount).toBeGreaterThan(0);
    });

    test('TC2: Page has main element wrapping primary content', async ({ page }) => {
        // Query for semantic main element
        const main = page.locator('main');

        // Check main exists and is visible
        await expect(main).toBeVisible();

        // Main should contain the primary content sections
        const sections = main.locator('section');
        const sectionCount = await sections.count();

        // Expect multiple sections inside main
        expect(sectionCount).toBeGreaterThan(0);

        // Verify main contains key sections
        await expect(main.locator('#hero')).toBeVisible();
        await expect(main.locator('#features')).toBeVisible();
        await expect(main.locator('#quickstart')).toBeVisible();
        await expect(main.locator('#usage')).toBeVisible();
    });

    test('TC3: Page has footer element with appropriate content', async ({ page }) => {
        // Query for semantic footer element
        const footer = page.locator('footer');

        // Check footer exists and is visible
        await expect(footer).toBeVisible();

        // Footer should contain copyright text
        const footerText = await footer.textContent();
        expect(footerText).toContain('MirDB');

        // Footer should contain navigation/links
        const footerLinks = footer.locator('a');
        const linkCount = await footerLinks.count();
        expect(linkCount).toBeGreaterThan(0);
    });

    test('TC4: Page has exactly one h1 element', async ({ page }) => {
        // Check h1 count - should be exactly one for proper accessibility
        const h1Elements = page.locator('h1');
        const h1Count = await h1Elements.count();

        expect(h1Count).toBe(1);

        // Verify the h1 is visible and contains meaningful content
        const h1 = h1Elements.first();
        await expect(h1).toBeVisible();

        const h1Text = await h1.textContent();
        expect(h1Text!.length).toBeGreaterThan(0);
        expect(h1Text).toContain('MirDB');
    });

    test('TC5: Headings follow sequential order without skipping levels', async ({ page }) => {
        // Get all heading elements in document order
        const headings = page.locator('h1, h2, h3, h4, h5, h6');
        const headingCount = await headings.count();

        expect(headingCount).toBeGreaterThan(0);

        // Extract heading levels in order
        const headingLevels: number[] = [];
        for (let i = 0; i < headingCount; i++) {
            const heading = headings.nth(i);
            const tagName = await heading.evaluate((el) => el.tagName.toLowerCase());
            const level = parseInt(tagName.replace('h', ''), 10);
            headingLevels.push(level);
        }

        // First heading should be h1
        expect(headingLevels[0]).toBe(1);

        // Check that no heading skips more than one level
        for (let i = 1; i < headingLevels.length; i++) {
            const currentLevel = headingLevels[i];
            const previousLevel = headingLevels[i - 1];

            // When going deeper, should not skip levels (e.g., h1 to h3)
            // Going from h1 to h3 would mean skipping h2, which is bad
            if (currentLevel > previousLevel) {
                const levelDiff = currentLevel - previousLevel;
                expect(levelDiff).toBeLessThanOrEqual(1);
            }
            // Going up levels (e.g., h3 back to h2) is always allowed
        }
    });

    test('TC6: All img elements have non-empty alt attributes', async ({ page }) => {
        // Get all img elements
        const images = page.locator('img');
        const imageCount = await images.count();

        expect(imageCount).toBeGreaterThan(0);

        // Check each image has a non-empty alt attribute
        for (let i = 0; i < imageCount; i++) {
            const img = images.nth(i);
            const alt = await img.getAttribute('alt');

            // Alt should exist and be non-empty
            expect(alt).not.toBeNull();
            expect(alt!.trim().length).toBeGreaterThan(0);
        }
    });

    test('TC7: No critical semantic structure violations', async ({ page }) => {
        // Verify proper semantic landmark structure

        // 1. Check for exactly one header landmark
        const headers = page.locator('header');
        const headerCount = await headers.count();
        expect(headerCount).toBe(1);

        // 2. Check for exactly one main landmark
        const mains = page.locator('main');
        const mainCount = await mains.count();
        expect(mainCount).toBe(1);

        // 3. Check for exactly one footer landmark
        const footers = page.locator('footer');
        const footerCount = await footers.count();
        expect(footerCount).toBe(1);

        // 4. Check nav element exists
        const navs = page.locator('nav');
        const navCount = await navs.count();
        expect(navCount).toBeGreaterThan(0);

        // 5. Verify document has lang attribute
        const htmlLang = await page.locator('html').getAttribute('lang');
        expect(htmlLang).toBeTruthy();
        expect(htmlLang!.length).toBeGreaterThan(0);

        // 6. Verify page has a title
        const title = await page.title();
        expect(title.length).toBeGreaterThan(0);

        // 7. Check for proper figure/figcaption structure if figures exist
        const figures = page.locator('figure');
        const figureCount = await figures.count();
        if (figureCount > 0) {
            for (let i = 0; i < figureCount; i++) {
                const figure = figures.nth(i);
                const figcaption = figure.locator('figcaption');
                const hasFigcaption = (await figcaption.count()) > 0;
                // Figures should have figcaption for accessibility
                expect(hasFigcaption).toBe(true);
            }
        }

        // 8. Check main is not inside header or footer
        const mainInHeader = await page.locator('header main').count();
        const mainInFooter = await page.locator('footer main').count();
        expect(mainInHeader).toBe(0);
        expect(mainInFooter).toBe(0);

        // 9. Verify article elements have headings (feature cards)
        const articles = page.locator('article');
        const articleCount = await articles.count();
        for (let i = 0; i < articleCount; i++) {
            const article = articles.nth(i);
            const articleHeading = article.locator('h1, h2, h3, h4, h5, h6');
            const hasHeading = (await articleHeading.count()) > 0;
            expect(hasHeading).toBe(true);
        }

        // 10. Verify buttons have accessible labels
        const buttons = page.locator('button');
        const buttonCount = await buttons.count();
        for (let i = 0; i < buttonCount; i++) {
            const button = buttons.nth(i);
            const ariaLabel = await button.getAttribute('aria-label');
            const textContent = await button.textContent();

            // Button should have either aria-label or text content
            const hasAccessibleName = (ariaLabel && ariaLabel.trim().length > 0) ||
                                      (textContent && textContent.trim().length > 0);
            expect(hasAccessibleName).toBe(true);
        }
    });
});
