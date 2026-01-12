// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Hero Section Display', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test('TC1: MirDB logo is displayed with proper dimensions and alt text', async ({ page }) => {
        // Locate the hero logo
        const logo = page.locator('#mirdb-logo');

        // Verify logo is visible
        await expect(logo).toBeVisible();

        // Verify alt text is present
        const altText = await logo.getAttribute('alt');
        expect(altText).toBe('MirDB Logo');

        // Verify logo has proper dimensions (should be rendered with width)
        const boundingBox = await logo.boundingBox();
        expect(boundingBox).not.toBeNull();
        expect(boundingBox.width).toBeGreaterThan(0);
        expect(boundingBox.height).toBeGreaterThan(0);

        // Verify logo src points to the correct file
        const src = await logo.getAttribute('src');
        expect(src).toContain('logo.gif');
    });

    test('TC2: Tagline contains Persistent Key-Value Store and Memcached Protocol', async ({ page }) => {
        // Locate the tagline element
        const tagline = page.locator('#tagline');

        // Verify tagline is visible
        await expect(tagline).toBeVisible();

        // Get the text content
        const taglineText = await tagline.textContent();

        // Verify tagline contains required phrases
        expect(taglineText).toContain('Persistent Key-Value Store');
        expect(taglineText).toContain('Memcached Protocol');
    });

    test('TC3: Get Started button scrolls to installation/quick-start section', async ({ page }) => {
        // Locate the Get Started button
        const getStartedBtn = page.locator('#get-started-btn');

        // Verify button is visible
        await expect(getStartedBtn).toBeVisible();

        // Verify button text
        await expect(getStartedBtn).toHaveText('Get Started');

        // Click the button
        await getStartedBtn.click();

        // Wait for smooth scroll to complete
        await page.waitForTimeout(1000);

        // Verify the quick-start section is now in view
        const quickStartSection = page.locator('#quick-start');
        await expect(quickStartSection).toBeInViewport();
    });

    test('TC4: View on GitHub button opens GitHub repository in new tab', async ({ page, context }) => {
        // Locate the GitHub button
        const githubBtn = page.locator('#github-btn');

        // Verify button is visible
        await expect(githubBtn).toBeVisible();

        // Verify button text
        await expect(githubBtn).toHaveText('View on GitHub');

        // Verify button has correct href
        const href = await githubBtn.getAttribute('href');
        expect(href).toBe('https://github.com/yetone/mirdb');

        // Verify target="_blank" for new tab
        const target = await githubBtn.getAttribute('target');
        expect(target).toBe('_blank');

        // Verify rel attribute for security
        const rel = await githubBtn.getAttribute('rel');
        expect(rel).toContain('noopener');
    });
});
