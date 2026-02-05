/**
 * Hero Section E2E Tests
 * Owner: Scenario 1 - Hero Section and Product Introduction
 *
 * Tests for:
 * - Product name display (MirDB)
 * - Tagline visibility (Persistent Key-Value Store)
 * - Memcached compatibility mention
 * - CTA buttons (Get Started, GitHub)
 */

const { test, expect } = require('@playwright/test');

test.describe('Hero Section', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test('TC1: Page loads successfully with HTTP 200 status', async ({ page }) => {
        // Navigate and verify response status
        const response = await page.goto('/');
        expect(response.status()).toBe(200);
    });

    test('TC2: Product name MirDB is visible in the hero section', async ({ page }) => {
        // Locate the hero section
        const heroSection = page.locator('#hero');
        await expect(heroSection).toBeVisible();

        // Check for MirDB text in the hero title
        const heroTitle = heroSection.locator('.hero-title');
        await expect(heroTitle).toBeVisible();
        await expect(heroTitle).toHaveText('MirDB');
    });

    test('TC3: Tagline containing Persistent Key-Value Store is visible', async ({ page }) => {
        const heroSection = page.locator('#hero');

        // Check for tagline text
        const tagline = heroSection.locator('.hero-tagline');
        await expect(tagline).toBeVisible();
        await expect(tagline).toContainText('Persistent Key-Value Store');
    });

    test('TC4: Memcached compatibility mention is visible above the fold', async ({ page }) => {
        const heroSection = page.locator('#hero');

        // Check for Memcached mention in description
        const description = heroSection.locator('.hero-description');
        await expect(description).toBeVisible();
        await expect(description).toContainText('Memcached');

        // Verify it mentions compatibility
        await expect(description).toContainText(/Memcached.*compatib/i);
    });

    test('TC5: Get Started CTA button is present and clickable', async ({ page }) => {
        const heroSection = page.locator('#hero');

        // Find Get Started button
        const getStartedBtn = heroSection.locator('a.btn', { hasText: 'Get Started' });
        await expect(getStartedBtn).toBeVisible();

        // Verify it's clickable (has href attribute)
        await expect(getStartedBtn).toHaveAttribute('href', '#quickstart');

        // Verify button is enabled and can be clicked
        await expect(getStartedBtn).toBeEnabled();
    });

    test('TC6: View on GitHub CTA button is present and clickable', async ({ page }) => {
        const heroSection = page.locator('#hero');

        // Find GitHub button - contains 'GitHub' text
        const githubBtn = heroSection.locator('a.btn', { hasText: /GitHub/i });
        await expect(githubBtn).toBeVisible();

        // Verify it links to GitHub (external link)
        const href = await githubBtn.getAttribute('href');
        expect(href).toContain('github.com');

        // Verify it opens in new tab with proper security attributes
        await expect(githubBtn).toHaveAttribute('target', '_blank');
        await expect(githubBtn).toHaveAttribute('rel', /noopener/);

        // Verify button is enabled and can be clicked
        await expect(githubBtn).toBeEnabled();
    });
});
