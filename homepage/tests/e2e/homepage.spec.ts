/**
 * Homepage E2E Tests
 * Owner: Scenario 1 - Hero Section & Branding (visibility test)
 * Owner: Scenario 9 - Performance (Lighthouse metrics)
 *
 * Test Case 5: Hero section is visible within first 100vh of viewport
 */

import { test, expect } from '@playwright/test';

test.describe('Hero Section E2E', () => {
	test.beforeEach(async ({ page }) => {
		await page.goto('/');
	});

	// Test Case 5: Hero section is visible within first 100vh of viewport
	test('hero section should be visible above the fold', async ({ page }) => {
		// Get viewport height
		const viewportHeight = await page.evaluate(() => window.innerHeight);

		// Get hero section
		const heroSection = page.locator('#hero');
		await expect(heroSection).toBeVisible();

		// Get hero section bounding box
		const boundingBox = await heroSection.boundingBox();
		expect(boundingBox).not.toBeNull();

		// Verify hero section starts within the first viewport height
		expect(boundingBox!.y).toBeLessThan(viewportHeight);
	});

	test('hero section should display MirDB branding', async ({ page }) => {
		// Check for MirDB title
		const title = page.locator('h1');
		await expect(title).toBeVisible();
		await expect(title).toHaveText('MirDB');
	});

	test('hero section should display ASCII logo', async ({ page }) => {
		const asciiLogo = page.locator('.ascii-logo');
		await expect(asciiLogo).toBeVisible();
		const logoText = await asciiLogo.textContent();
		expect(logoText).toContain('███');
	});

	test('hero section should display tagline', async ({ page }) => {
		const tagline = page.locator('.tagline');
		await expect(tagline).toBeVisible();
		await expect(tagline).toContainText('Persistent Key-Value Store');
		await expect(tagline).toContainText('Memcached');
	});

	test('hero section should have Get Started CTA button', async ({ page }) => {
		const ctaButton = page.locator('button:has-text("Get Started")');
		await expect(ctaButton).toBeVisible();
	});

	test('CTA button should scroll to Quick Start section on click', async ({ page }) => {
		// Get initial scroll position
		const initialScrollY = await page.evaluate(() => window.scrollY);
		expect(initialScrollY).toBe(0);

		// Click the Get Started button
		const ctaButton = page.locator('button:has-text("Get Started")');
		await ctaButton.click();

		// Wait for scroll animation
		await page.waitForTimeout(500);

		// Check that we scrolled
		const quickStartSection = page.locator('#quickstart');
		await expect(quickStartSection).toBeInViewport();
	});

	test('GitHub link should have correct href and open in new tab', async ({ page }) => {
		const githubLink = page.locator('a:has-text("View on GitHub")');
		await expect(githubLink).toBeVisible();
		await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
		await expect(githubLink).toHaveAttribute('target', '_blank');
	});
});
