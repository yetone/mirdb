// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for MirDB Landing Page Hero Section
 * These tests verify the hero section displays product name, tagline,
 * value proposition, and call-to-action buttons correctly.
 */

test.describe('Hero Section Display', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test('TC1: Hero section contains h1 with MirDB product name', async ({ page }) => {
        // Test Case 1: Load landing page and inspect hero section DOM
        // Expected: Hero section contains h1 with 'MirDB' product name

        const hero = page.locator('.hero');
        await expect(hero).toBeVisible();

        const productName = page.locator('.hero h1.product-name');
        await expect(productName).toBeVisible();
        await expect(productName).toHaveText('MirDB');
    });

    test('TC2: Tagline contains Persistent Key-Value Store and Memcached Protocol text', async ({ page }) => {
        // Test Case 2: Check tagline text content
        // Expected: Tagline contains 'Persistent Key-Value Store' and 'Memcached Protocol' text

        const tagline = page.locator('.hero .tagline');
        await expect(tagline).toBeVisible();

        const taglineText = await tagline.textContent();
        expect(taglineText).toContain('Persistent Key-Value Store');
        expect(taglineText).toContain('Memcached Protocol');
    });

    test('TC3: Primary CTA button Get Started is visible and interactive', async ({ page }) => {
        // Test Case 3: Verify Get Started button presence
        // Expected: Primary CTA button with text 'Get Started' is visible and interactive

        const getStartedButton = page.locator('.cta-primary');
        await expect(getStartedButton).toBeVisible();
        await expect(getStartedButton).toHaveText('Get Started');

        // Verify it's interactive (has href attribute and is clickable)
        await expect(getStartedButton).toHaveAttribute('href', '#get-started');
        await expect(getStartedButton).toBeEnabled();

        // Test that clicking navigates to the quick-start section
        await getStartedButton.click();
        await expect(page).toHaveURL(/#get-started/);
    });

    test('TC4: Secondary CTA link View on GitHub is visible and links to GitHub repository', async ({ page }) => {
        // Test Case 4: Verify GitHub link presence
        // Expected: Secondary CTA link 'View on GitHub' is visible and links to GitHub repository

        const githubLink = page.locator('.cta-secondary');
        await expect(githubLink).toBeVisible();
        await expect(githubLink).toHaveText('View on GitHub');

        // Verify it links to a GitHub repository
        const href = await githubLink.getAttribute('href');
        expect(href).toMatch(/github\.com/);

        // Verify it opens in new tab for external link
        await expect(githubLink).toHaveAttribute('target', '_blank');
        await expect(githubLink).toHaveAttribute('rel', /noopener/);
    });

    test('Hero section displays value proposition', async ({ page }) => {
        // Additional test for value proposition text
        const valueProp = page.locator('.hero .value-proposition');
        await expect(valueProp).toBeVisible();

        const text = await valueProp.textContent();
        expect(text).toContain('persistence');
        expect(text).toContain('memcached');
    });

    test('CTA buttons container is visible with both buttons', async ({ page }) => {
        // Verify both CTA buttons are present in the container
        const ctaContainer = page.locator('.cta-buttons');
        await expect(ctaContainer).toBeVisible();

        const buttons = ctaContainer.locator('a');
        await expect(buttons).toHaveCount(2);
    });
});
