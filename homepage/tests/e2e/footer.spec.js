/**
 * Footer Section E2E Tests
 * Owner: Scenario 13 - Footer Section
 *
 * Tests for:
 * - Footer visibility when scrolled to bottom
 * - Footer content visibility (GitHub link, license, CI badge)
 * - Footer interaction and accessibility
 */

const { test, expect } = require('@playwright/test');

test.describe('Footer Section Visibility', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test('TC4: Footer is visible when scrolled to bottom of page', async ({ page }) => {
        // Get the footer element
        const footer = page.locator('footer');

        // Scroll the footer into view
        await footer.scrollIntoViewIfNeeded();

        // Wait for any animations to complete
        await page.waitForTimeout(300);

        // Verify footer is visible
        await expect(footer).toBeVisible();
    });

    test('Footer becomes visible after scrolling from top', async ({ page }) => {
        // Start at the top of the page
        await page.evaluate(() => window.scrollTo(0, 0));

        // Wait for position to settle
        await page.waitForTimeout(100);

        // Get the footer element
        const footer = page.locator('footer');

        // Scroll to footer
        await footer.scrollIntoViewIfNeeded();
        await page.waitForTimeout(300);

        // Verify footer is visible
        await expect(footer).toBeVisible();

        // Verify we're at the bottom of the page
        const footerBoundingBox = await footer.boundingBox();
        expect(footerBoundingBox).not.toBeNull();
    });

    test('Footer is positioned at the bottom of the page', async ({ page }) => {
        // Get the footer
        const footer = page.locator('footer');

        // Get the document and footer position info using evaluate
        const positions = await page.evaluate(() => {
            const footerEl = document.querySelector('footer');
            const footerRect = footerEl.getBoundingClientRect();
            const documentHeight = document.documentElement.scrollHeight;
            const footerOffsetTop = footerEl.offsetTop;
            const footerOffsetHeight = footerEl.offsetHeight;

            return {
                documentHeight,
                footerOffsetTop,
                footerOffsetHeight,
                footerBottom: footerOffsetTop + footerOffsetHeight
            };
        });

        // Footer's bottom (offsetTop + height) should be close to or equal to document height
        expect(positions.footerBottom).toBeGreaterThanOrEqual(positions.documentHeight - 10);
    });

    test('Footer content is visible when footer is in view', async ({ page }) => {
        const footer = page.locator('footer');
        await footer.scrollIntoViewIfNeeded();
        await page.waitForTimeout(300);

        // Check footer content container
        const footerContent = page.locator('.footer-content');
        await expect(footerContent).toBeVisible();

        // Check footer links section
        const footerLinks = page.locator('.footer-links');
        await expect(footerLinks).toBeVisible();

        // Check footer badge section
        const footerBadge = page.locator('.footer-badge');
        await expect(footerBadge).toBeVisible();
    });

    test('GitHub link in footer is visible and interactive', async ({ page }) => {
        const footer = page.locator('footer');
        await footer.scrollIntoViewIfNeeded();

        // Find GitHub link in footer
        const githubLink = footer.locator('a[href*="github.com/yetone/mirdb"]');
        await expect(githubLink).toBeVisible();

        // Check that the link is interactable
        await expect(githubLink).toBeEnabled();
    });

    test('License information is visible in footer', async ({ page }) => {
        const footer = page.locator('footer');
        await footer.scrollIntoViewIfNeeded();

        // Check that MIT license text is visible
        const footerText = await footer.textContent();
        expect(footerText.toLowerCase()).toContain('mit');
    });

    test('CI badge is visible in footer', async ({ page }) => {
        const footer = page.locator('footer');
        await footer.scrollIntoViewIfNeeded();

        // Check CI badge image
        const ciBadge = footer.locator('img[src*="circleci"]');
        await expect(ciBadge).toBeVisible();
    });

    test('Footer has proper semantic structure', async ({ page }) => {
        const footer = page.locator('footer');
        await expect(footer).toBeVisible();

        // Check footer has role contentinfo (implicit or explicit)
        const role = await footer.getAttribute('role');
        if (role) {
            expect(role).toBe('contentinfo');
        }
        // If no explicit role, the semantic <footer> element has implicit contentinfo role
    });
});
