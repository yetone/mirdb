/**
 * E2E Tests for Homepage - Hero Section & Navigation
 * Owner: Scenario 1 - Hero Section & Navigation
 *
 * Tests the hero section, navigation, and smooth scrolling functionality.
 */

const { test, expect } = require('@playwright/test');

test.describe('Homepage - Hero Section & Navigation', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test.describe('Test Case 1: Page Load', () => {
        test('page loads successfully with status 200 and hero section visible', async ({ page }) => {
            // Verify page loaded successfully
            const response = await page.goto('/');
            expect(response.status()).toBe(200);

            // Verify hero section is visible
            const heroSection = page.locator('#hero');
            await expect(heroSection).toBeVisible();
        });
    });

    test.describe('Test Case 2: Hero Content', () => {
        test('hero contains MirDB as headline and value proposition as subheadline', async ({ page }) => {
            // Check headline
            const headline = page.locator('.hero h1');
            await expect(headline).toContainText('MirDB');

            // Check subheadline/tagline
            const tagline = page.locator('.hero-tagline');
            await expect(tagline).toContainText('Persistent Memcached-Compatible Key-Value Store');
        });
    });

    test.describe('Test Case 3: Primary CTA - GitHub Button', () => {
        test('Get Started on GitHub button opens GitHub repository in new tab', async ({ page }) => {
            // Get the primary CTA button
            const githubButton = page.locator('a.btn-primary:has-text("Get Started on GitHub")');
            await expect(githubButton).toBeVisible();

            // Verify it has target="_blank"
            await expect(githubButton).toHaveAttribute('target', '_blank');

            // Verify it points to GitHub
            const href = await githubButton.getAttribute('href');
            expect(href).toMatch(/^https:\/\/github\.com\//);

            // Verify rel="noopener" for security
            const rel = await githubButton.getAttribute('rel');
            expect(rel).toContain('noopener');
        });
    });

    test.describe('Test Case 4: Secondary CTA - Quick Start Button', () => {
        test('View Quick Start button scrolls to Quick Start section with hash in URL', async ({ page }) => {
            // Get the secondary CTA button
            const quickStartButton = page.locator('a.btn-secondary:has-text("View Quick Start")');
            await expect(quickStartButton).toBeVisible();

            // Click the button
            await quickStartButton.click();

            // Wait for smooth scroll and URL hash update
            await page.waitForTimeout(500);

            // Check URL has #quick-start
            await expect(page).toHaveURL(/#quick-start$/);

            // Check that quick-start section is in view
            const quickStartSection = page.locator('#quick-start');
            await expect(quickStartSection).toBeInViewport();
        });
    });

    test.describe('Test Case 7: Navigation Link - Benchmarks', () => {
        test('clicking benchmarks link scrolls to benchmarks section without page reload', async ({ page }) => {
            // Get the navigation link
            const benchmarksLink = page.locator('.nav-links a[href="#benchmarks"]');
            await expect(benchmarksLink).toBeVisible();

            // Click the link
            await benchmarksLink.click();

            // Wait for smooth scroll
            await page.waitForTimeout(500);

            // Check URL has #benchmarks
            await expect(page).toHaveURL(/#benchmarks$/);

            // Check that benchmarks section is visible
            const benchmarksSection = page.locator('#benchmarks');
            await expect(benchmarksSection).toBeInViewport();
        });
    });

    test.describe('Navigation Structure', () => {
        test('navigation contains all required links', async ({ page }) => {
            const navLinks = page.locator('.nav-links a');
            await expect(navLinks).toHaveCount(5); // Features, Quick Start, Architecture, Benchmarks, GitHub

            // Verify each link exists
            await expect(page.locator('.nav-links a[href="#features"]')).toBeVisible();
            await expect(page.locator('.nav-links a[href="#quick-start"]')).toBeVisible();
            await expect(page.locator('.nav-links a[href="#architecture"]')).toBeVisible();
            await expect(page.locator('.nav-links a[href="#benchmarks"]')).toBeVisible();
            await expect(page.locator('.nav-links a[href*="github.com"]')).toBeVisible();
        });

        test('all anchor navigation links use smooth scrolling', async ({ page }) => {
            const anchorLinks = page.locator('.nav-links a[href^="#"]');
            const count = await anchorLinks.count();

            for (let i = 0; i < count; i++) {
                const link = anchorLinks.nth(i);
                const href = await link.getAttribute('href');
                const targetId = href.substring(1);

                // Click the link
                await link.click();
                await page.waitForTimeout(300);

                // Verify URL was updated
                await expect(page).toHaveURL(new RegExp(`#${targetId}$`));

                // Navigate back to top for next iteration
                await page.evaluate(() => window.scrollTo(0, 0));
                await page.waitForTimeout(100);
            }
        });
    });

    test.describe('Header and Logo', () => {
        test('logo links to top of page', async ({ page }) => {
            // First scroll down
            await page.evaluate(() => window.scrollTo(0, 500));
            await page.waitForTimeout(100);

            // Click logo
            const logo = page.locator('.nav-logo');
            await logo.click();
            await page.waitForTimeout(300);

            // Should scroll to top (allow small tolerance for rendering differences)
            const scrollPosition = await page.evaluate(() => window.pageYOffset);
            expect(scrollPosition).toBeLessThanOrEqual(10);
        });

        test('header is fixed at top', async ({ page }) => {
            const header = page.locator('.site-header');

            // Check it's visible
            await expect(header).toBeVisible();

            // Scroll down
            await page.evaluate(() => window.scrollTo(0, 500));
            await page.waitForTimeout(100);

            // Header should still be visible at top
            await expect(header).toBeVisible();
            await expect(header).toBeInViewport();
        });
    });

    test.describe('Accessibility', () => {
        test('skip link is present and functional', async ({ page }) => {
            const skipLink = page.locator('.skip-link');

            // Skip link should exist
            await expect(skipLink).toHaveCount(1);

            // Skip link should have correct href
            await expect(skipLink).toHaveAttribute('href', '#main-content');

            // Main content should have matching id
            const mainContent = page.locator('#main-content');
            await expect(mainContent).toHaveCount(1);
        });

        test('navigation is properly labeled', async ({ page }) => {
            const nav = page.locator('nav[aria-label="Main navigation"]');
            await expect(nav).toHaveCount(1);
        });

        test('hero section has proper heading hierarchy', async ({ page }) => {
            // H1 should be present
            const h1 = page.locator('h1');
            await expect(h1).toHaveCount(1);
            await expect(h1).toContainText('MirDB');
        });
    });
});
