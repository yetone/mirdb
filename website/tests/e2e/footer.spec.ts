/**
 * Footer Section E2E Tests
 * Owner: Scenario 6 - Footer Section
 *
 * Tests for footer section display including semantic element, project name, year, and links.
 */

import { test, expect } from '@playwright/test';

test.describe('Footer Section', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test('TC1: Semantic footer element exists', async ({ page }) => {
        // Query for semantic footer element
        const footer = page.locator('footer');

        // Check footer exists and is visible
        await expect(footer).toBeVisible();

        // Verify it's an actual HTML footer element (semantic)
        const tagName = await footer.evaluate((el) => el.tagName.toLowerCase());
        expect(tagName).toBe('footer');
    });

    test('TC2: Footer contains MirDB text', async ({ page }) => {
        // Query for footer element
        const footer = page.locator('footer');
        await expect(footer).toBeVisible();

        // Get footer text content
        const footerText = await footer.textContent();

        // Check footer contains 'MirDB' text
        expect(footerText).toContain('MirDB');
    });

    test('TC3: Footer contains a 4-digit year', async ({ page }) => {
        // Query for footer element
        const footer = page.locator('footer');
        await expect(footer).toBeVisible();

        // Get footer text content
        const footerText = await footer.textContent();

        // Check footer contains a 4-digit year (2020-2029 range covers typical use)
        const yearRegex = /\b(20\d{2})\b/;
        const match = footerText?.match(yearRegex);
        expect(match).not.toBeNull();
        expect(match![1]).toMatch(/^20\d{2}$/);
    });

    test('TC4: Footer contains at least one link', async ({ page }) => {
        // Query for footer element
        const footer = page.locator('footer');
        await expect(footer).toBeVisible();

        // Query for links within footer
        const footerLinks = footer.locator('a');

        // Check that at least one link exists
        const linkCount = await footerLinks.count();
        expect(linkCount).toBeGreaterThanOrEqual(1);

        // Verify the first link has an href attribute
        const firstLink = footerLinks.first();
        await expect(firstLink).toBeVisible();
        const href = await firstLink.getAttribute('href');
        expect(href).toBeTruthy();
        expect(href!.length).toBeGreaterThan(0);
    });
});
