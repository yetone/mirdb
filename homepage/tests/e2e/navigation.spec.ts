import { test, expect } from '@playwright/test';

test.describe('Navigation & Layout E2E Tests', () => {
	test.beforeEach(async ({ page }) => {
		await page.goto('/');
	});

	test('homepage loads with header and footer visible', async ({ page }) => {
		await expect(page.locator('header')).toBeVisible();
		await expect(page.locator('footer')).toBeVisible();
	});

	test('header contains logo with MirDB text', async ({ page }) => {
		const logo = page.locator('.logo-text');
		await expect(logo).toBeVisible();
		await expect(logo).toHaveText('MirDB');
	});

	test('navigation contains Features, Quick Start, Docs, and GitHub links', async ({ page }) => {
		const nav = page.locator('nav[aria-label="Main navigation"]');
		await expect(nav).toBeVisible();

		await expect(nav.getByRole('link', { name: /features/i })).toBeVisible();
		await expect(nav.getByRole('link', { name: /quick start/i })).toBeVisible();
		await expect(nav.getByRole('link', { name: /docs/i })).toBeVisible();
		await expect(nav.getByRole('link', { name: /github/i })).toBeVisible();
	});

	test('GitHub link has correct href and target', async ({ page }) => {
		const githubLink = page.locator('nav[aria-label="Main navigation"]').getByRole('link', { name: /github/i });
		await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
		await expect(githubLink).toHaveAttribute('target', '_blank');
	});

	test('clicking Features link scrolls to Features section', async ({ page }) => {
		const featuresLink = page.locator('nav[aria-label="Main navigation"]').getByRole('link', { name: /features/i });
		await featuresLink.click();

		// Wait for smooth scroll animation
		await page.waitForTimeout(500);

		// Check URL hash updated
		await expect(page).toHaveURL(/#features$/);

		// Check Features section is in viewport
		const featuresSection = page.locator('#features');
		await expect(featuresSection).toBeInViewport();
	});

	test('clicking Quick Start link scrolls to Quick Start section', async ({ page }) => {
		const quickStartLink = page.locator('nav[aria-label="Main navigation"]').getByRole('link', { name: /quick start/i });
		await quickStartLink.click();

		await page.waitForTimeout(500);
		await expect(page).toHaveURL(/#quick-start$/);

		const quickStartSection = page.locator('#quick-start');
		await expect(quickStartSection).toBeInViewport();
	});

	test('clicking Docs link scrolls to Docs section', async ({ page }) => {
		const docsLink = page.locator('nav[aria-label="Main navigation"]').getByRole('link', { name: /docs/i });
		await docsLink.click();

		await page.waitForTimeout(500);
		await expect(page).toHaveURL(/#docs$/);

		const docsSection = page.locator('#docs');
		await expect(docsSection).toBeInViewport();
	});

	test('navigate through all sections without page reload', async ({ page }) => {
		const initialUrl = page.url();

		// Click Features
		await page.locator('nav[aria-label="Main navigation"]').getByRole('link', { name: /features/i }).click();
		await page.waitForTimeout(300);

		// Click Quick Start
		await page.locator('nav[aria-label="Main navigation"]').getByRole('link', { name: /quick start/i }).click();
		await page.waitForTimeout(300);

		// Click Docs
		await page.locator('nav[aria-label="Main navigation"]').getByRole('link', { name: /docs/i }).click();
		await page.waitForTimeout(300);

		// Verify we're still on the same page (no reload)
		const finalUrl = page.url();
		expect(finalUrl.replace(/#.*$/, '')).toBe(initialUrl.replace(/#.*$/, ''));
	});

	test('footer contains copyright text', async ({ page }) => {
		const footer = page.locator('footer');
		await expect(footer).toBeVisible();

		const currentYear = new Date().getFullYear();
		await expect(footer).toContainText(`© ${currentYear} MirDB`);
	});

	test('footer contains GitHub link with correct attributes', async ({ page }) => {
		const footerGithubLink = page.locator('footer').getByRole('link', { name: /github/i });
		await expect(footerGithubLink).toBeVisible();
		await expect(footerGithubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
		await expect(footerGithubLink).toHaveAttribute('target', '_blank');
	});

	test('header is fixed/sticky at top of page', async ({ page }) => {
		const header = page.locator('header');

		// Check that header has fixed positioning
		const position = await header.evaluate((el) => {
			return window.getComputedStyle(el).position;
		});
		expect(position).toBe('fixed');
	});

	test('scrolling page keeps header visible', async ({ page }) => {
		// Scroll down
		await page.evaluate(() => window.scrollTo(0, 500));
		await page.waitForTimeout(100);

		// Header should still be visible
		const header = page.locator('header');
		await expect(header).toBeVisible();
	});
});

test.describe('Mobile Navigation', () => {
	test.use({ viewport: { width: 375, height: 667 } });

	test('mobile menu button is visible on small viewport', async ({ page }) => {
		await page.goto('/');
		const menuButton = page.locator('.mobile-menu-toggle');
		await expect(menuButton).toBeVisible();
	});

	test('clicking mobile menu button opens navigation', async ({ page }) => {
		await page.goto('/');
		const menuButton = page.locator('.mobile-menu-toggle');
		await menuButton.click();

		const mobileMenu = page.locator('#mobile-menu');
		await expect(mobileMenu).toBeVisible();
	});

	test('mobile menu contains all navigation links', async ({ page }) => {
		await page.goto('/');
		await page.locator('.mobile-menu-toggle').click();

		const mobileMenu = page.locator('#mobile-menu');
		await expect(mobileMenu.getByRole('link', { name: /features/i })).toBeVisible();
		await expect(mobileMenu.getByRole('link', { name: /quick start/i })).toBeVisible();
		await expect(mobileMenu.getByRole('link', { name: /docs/i })).toBeVisible();
		await expect(mobileMenu.getByRole('link', { name: /github/i })).toBeVisible();
	});
});
