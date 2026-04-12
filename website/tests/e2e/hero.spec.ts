/**
 * Hero Section E2E Tests
 * Owner: Scenario 1 - Hero Section Display
 *
 * Tests for hero section display including logo, headline, subheadline, and CTA button.
 */

import { test, expect } from '@playwright/test';

test.describe('Hero Section Display', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test('TC1: Logo image is visible with correct src and alt text', async ({ page }) => {
        // Navigate to homepage and check for logo element
        const logo = page.locator('.hero-logo');

        // Check logo is visible
        await expect(logo).toBeVisible();

        // Check src contains 'logo.gif'
        const src = await logo.getAttribute('src');
        expect(src).toContain('logo.gif');

        // Check alt text exists and is meaningful
        const alt = await logo.getAttribute('alt');
        expect(alt).toBeTruthy();
        expect(alt!.length).toBeGreaterThan(0);
    });

    test('TC2: H1 headline contains required keywords', async ({ page }) => {
        // Query for main headline h1 element
        const h1 = page.locator('h1');

        // Check H1 is visible
        await expect(h1).toBeVisible();

        // Get the text content
        const text = await h1.textContent();

        // Check all required keywords are present
        expect(text).toContain('MirDB');
        expect(text).toContain('Persistent Key-Value Store');
        expect(text).toContain('Memcached Protocol');
    });

    test('TC3: Subheadline contains required text', async ({ page }) => {
        // Query for subheadline element
        const subheadline = page.locator('.hero-subtitle');

        // Check subheadline is visible
        await expect(subheadline).toBeVisible();

        // Get the text content (case insensitive check)
        const text = await subheadline.textContent();
        const lowerText = text!.toLowerCase();

        // Check required keywords
        expect(lowerText).toContain('painless');
        expect(lowerText).toContain('memcached');
    });

    test('TC4: Get Started button scrolls to quickstart section', async ({ page }) => {
        // Find the CTA button - look for the "Get Started" link
        const ctaButton = page.locator('a.btn-primary, a.btn.btn-primary').filter({ hasText: 'Get Started' });
        await expect(ctaButton).toBeVisible();

        // Check button text
        const buttonText = await ctaButton.textContent();
        expect(buttonText).toContain('Get Started');

        // Check href attribute points to quickstart section
        const href = await ctaButton.getAttribute('href');
        expect(href).toBe('#quickstart');

        // Get initial scroll position
        const initialScrollY = await page.evaluate(() => window.scrollY);

        // Click the button
        await ctaButton.click();

        // Wait for smooth scroll animation
        await page.waitForTimeout(500);

        // Check that page has scrolled (scroll position changed)
        const newScrollY = await page.evaluate(() => window.scrollY);
        expect(newScrollY).toBeGreaterThan(initialScrollY);

        // Check quickstart section is in viewport
        const quickstartSection = page.locator('#quickstart');
        await expect(quickstartSection).toBeInViewport();
    });

    test('TC5: Hero section is fully visible above the fold on 1920x1080', async ({ page }) => {
        // Set viewport to 1920x1080 (desktop)
        await page.setViewportSize({ width: 1920, height: 1080 });
        await page.goto('/');

        // Get the hero section
        const heroSection = page.locator('#hero');
        await expect(heroSection).toBeVisible();

        // Check hero is in viewport (above the fold)
        await expect(heroSection).toBeInViewport();

        // Get hero bounding box
        const heroBox = await heroSection.boundingBox();
        expect(heroBox).not.toBeNull();

        // Verify hero section starts near top of page (accounting for header ~64px)
        // The hero section should start within reasonable distance from top
        expect(heroBox!.y).toBeLessThanOrEqual(150);

        // Verify all key hero elements are visible
        await expect(page.locator('.hero-logo')).toBeVisible();
        await expect(page.locator('.hero h1')).toBeVisible();
        await expect(page.locator('.hero-subtitle')).toBeVisible();
        await expect(page.locator('.hero .btn-primary')).toBeVisible();

        // All elements should be in viewport without scrolling
        await expect(page.locator('.hero-logo')).toBeInViewport();
        await expect(page.locator('.hero h1')).toBeInViewport();
        await expect(page.locator('.hero-subtitle')).toBeInViewport();
        await expect(page.locator('.hero .btn-primary')).toBeInViewport();
    });
});
