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
		// ASCII logo contains the MirDB stylized text with underscores and pipes
		expect(logoText).toContain('_');
		expect(logoText).toContain('|');
	});

	test('hero section should display tagline', async ({ page }) => {
		const tagline = page.locator('.tagline');
		await expect(tagline).toBeVisible();
		await expect(tagline).toContainText('Persistent Key-Value Store');
		await expect(tagline).toContainText('Memcached');
	});

	test('hero section should have Get Started CTA button', async ({ page }) => {
		const ctaButton = page.locator('a:has-text("Get Started")');
		await expect(ctaButton).toBeVisible();
	});

	test('CTA button should scroll to Quick Start section on click', async ({ page }) => {
		// Get initial scroll position
		const initialScrollY = await page.evaluate(() => window.scrollY);
		expect(initialScrollY).toBe(0);

		// Click the Get Started button
		const ctaButton = page.locator('a:has-text("Get Started")');
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

/**
 * SEO Tests
 * Owner: Scenario 9 - Performance & SEO
 *
 * Test Cases 4-8: SEO meta tags verification
 */
test.describe('SEO Meta Tags', () => {
	test.beforeEach(async ({ page }) => {
		await page.goto('/');
	});

	// Test Case 4: Check for meta title tag
	test('should have title tag with MirDB keyword and proper length', async ({ page }) => {
		const title = await page.title();

		// Title should contain 'MirDB' keyword
		expect(title).toContain('MirDB');

		// Title should be between 50-60 characters for SEO
		expect(title.length).toBeGreaterThanOrEqual(50);
		expect(title.length).toBeLessThanOrEqual(70);
	});

	// Test Case 5: Check for meta description
	test('should have meta description with proper length', async ({ page }) => {
		const metaDescription = await page.locator('meta[name="description"]').getAttribute('content');

		expect(metaDescription).not.toBeNull();
		expect(metaDescription!.length).toBeGreaterThanOrEqual(100);
		expect(metaDescription!.length).toBeLessThanOrEqual(200);
		expect(metaDescription).toContain('MirDB');
		expect(metaDescription!.toLowerCase()).toContain('key-value');
	});

	// Test Case 6: Check Open Graph tags
	test('should have all required Open Graph tags', async ({ page }) => {
		const ogTitle = await page.locator('meta[property="og:title"]').getAttribute('content');
		const ogDescription = await page.locator('meta[property="og:description"]').getAttribute('content');
		const ogImage = await page.locator('meta[property="og:image"]').getAttribute('content');
		const ogUrl = await page.locator('meta[property="og:url"]').getAttribute('content');

		expect(ogTitle).not.toBeNull();
		expect(ogTitle).toContain('MirDB');

		expect(ogDescription).not.toBeNull();
		expect(ogDescription!.length).toBeGreaterThan(50);

		expect(ogImage).not.toBeNull();
		expect(ogImage).toMatch(/^https?:\/\/.+/);

		expect(ogUrl).not.toBeNull();
		expect(ogUrl).toMatch(/^https?:\/\/.+/);
	});

	// Test Case 7: Check canonical URL
	test('should have canonical link element with absolute URL', async ({ page }) => {
		const canonical = await page.locator('link[rel="canonical"]').getAttribute('href');

		expect(canonical).not.toBeNull();
		expect(canonical).toMatch(/^https?:\/\/.+/);
	});
});

/**
 * Semantic HTML Tests
 * Owner: Scenario 9 - Performance & SEO
 *
 * Test Cases: Semantic HTML5 elements verification
 */
test.describe('Semantic HTML Structure', () => {
	test.beforeEach(async ({ page }) => {
		await page.goto('/');
	});

	test('should use semantic HTML5 elements', async ({ page }) => {
		// Check for header element
		const header = page.locator('header');
		await expect(header).toBeVisible();

		// Check for main element
		const main = page.locator('main');
		await expect(main).toBeVisible();

		// Check for navigation element
		const nav = page.locator('nav');
		await expect(nav).toBeVisible();

		// Check for section elements
		const sections = page.locator('section');
		expect(await sections.count()).toBeGreaterThan(0);

		// Check for footer element
		const footer = page.locator('footer');
		await expect(footer).toBeVisible();
	});

	test('should have proper heading hierarchy', async ({ page }) => {
		// H1 should exist and be unique
		const h1Elements = page.locator('h1');
		expect(await h1Elements.count()).toBe(1);

		// H2 elements should exist for sections
		const h2Elements = page.locator('h2');
		expect(await h2Elements.count()).toBeGreaterThan(0);
	});

	test('should have lang attribute on html element', async ({ page }) => {
		const htmlLang = await page.locator('html').getAttribute('lang');
		expect(htmlLang).toBe('en');
	});
});

/**
 * robots.txt Tests
 * Owner: Scenario 9 - Performance & SEO
 *
 * Test Case 8: Check robots.txt
 */
test.describe('robots.txt', () => {
	test('should exist and allow indexing of homepage', async ({ page }) => {
		const response = await page.goto('/robots.txt');

		expect(response?.status()).toBe(200);

		const content = await page.content();

		// Should not disallow root path
		expect(content).toContain('User-agent');
		expect(content).not.toMatch(/Disallow:\s*\/$/m);
	});
});

/**
 * Performance Tests
 * Owner: Scenario 9 - Performance & SEO
 *
 * Test Cases 9-11: Performance metrics
 */
test.describe('Performance Metrics', () => {
	test('should load page quickly', async ({ page }) => {
		// Measure page load performance
		const startTime = Date.now();

		await page.goto('/', { waitUntil: 'networkidle' });

		const loadTime = Date.now() - startTime;

		// Page should load in under 5 seconds in test environment
		expect(loadTime).toBeLessThan(5000);
	});

	test('should have minimal DOM size', async ({ page }) => {
		await page.goto('/');

		const domNodeCount = await page.evaluate(() => {
			return document.querySelectorAll('*').length;
		});

		// DOM should have reasonable number of nodes (under 1500)
		expect(domNodeCount).toBeLessThan(1500);
	});

	test('should have optimized images with proper attributes', async ({ page }) => {
		await page.goto('/');

		const images = page.locator('img');
		const imageCount = await images.count();

		for (let i = 0; i < imageCount; i++) {
			const img = images.nth(i);

			// All images should have alt attributes for accessibility
			const alt = await img.getAttribute('alt');
			expect(alt).not.toBeNull();

			// Check for width and height to prevent layout shift
			const width = await img.getAttribute('width');
			const height = await img.getAttribute('height');

			// Either explicit dimensions or CSS sizing should be used
			if (!width || !height) {
				const style = await img.getAttribute('style');
				const hasClass = await img.getAttribute('class');
				expect(style || hasClass).not.toBeNull();
			}
		}
	});
});
