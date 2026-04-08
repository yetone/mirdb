/**
 * Hero Section E2E Tests
 * Owner: Scenario 1 - Hero Section Display
 *
 * Test cases:
 * - Hero displays 'Persistent Key-Value Store' and 'Memcached Protocol'
 * - Get Started CTA is visible and links to installation
 * - View Code CTA is visible and links to GitHub
 * - Rust implementation is mentioned
 */

const { test, expect } = require('@playwright/test');

test.describe('Hero Section Display', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test('hero section contains tagline mentioning Persistent Key-Value Store and Memcached Protocol', async ({ page }) => {
        // Test Case ID: 1
        // Input: Load index.html and inspect hero section
        // Expected: Hero section contains tagline mentioning 'Persistent Key-Value Store' and 'Memcached Protocol'

        const heroSection = page.locator('#hero');
        await expect(heroSection).toBeVisible();

        const heroTitle = heroSection.locator('.hero-title');
        await expect(heroTitle).toBeVisible();

        const titleText = await heroTitle.textContent();
        expect(titleText).toContain('Persistent Key-Value Store');
        expect(titleText).toContain('Memcached Protocol');
    });

    test('Get Started button is visible with href linking to installation section', async ({ page }) => {
        // Test Case ID: 2
        // Input: Check for primary CTA button
        // Expected: Get Started button is visible with href linking to installation section

        const heroSection = page.locator('#hero');
        const getStartedButton = heroSection.locator('.hero-cta-primary');

        await expect(getStartedButton).toBeVisible();
        await expect(getStartedButton).toHaveText('Get Started');
        await expect(getStartedButton).toHaveAttribute('href', '#installation');
    });

    test('View Code button is visible with href linking to GitHub repository', async ({ page }) => {
        // Test Case ID: 3
        // Input: Check for secondary CTA button
        // Expected: View Code button is visible with href linking to GitHub repository

        const heroSection = page.locator('#hero');
        const viewCodeButton = heroSection.locator('.hero-cta-secondary');

        await expect(viewCodeButton).toBeVisible();
        await expect(viewCodeButton).toHaveText('View Code');

        const href = await viewCodeButton.getAttribute('href');
        expect(href).toContain('github.com');
    });

    test('hero section mentions Rust as the implementation language', async ({ page }) => {
        // Test Case ID: 4
        // Input: Verify Rust implementation is mentioned
        // Expected: Hero section mentions Rust as the implementation language

        const heroSection = page.locator('#hero');
        const heroContent = await heroSection.textContent();

        expect(heroContent).toContain('Rust');

        // Additionally check for the highlighted Rust text
        const rustHighlight = heroSection.locator('.hero-rust-highlight');
        await expect(rustHighlight).toBeVisible();
        await expect(rustHighlight).toHaveText('Rust');
    });
});
