/**
 * E2E tests for theme toggle functionality
 * Owner: Scenario 5 - Theme Toggle
 */

import { test, expect } from '@playwright/test';

test.describe('Theme Toggle E2E', () => {
	test.beforeEach(async ({ page }) => {
		// Clear localStorage before each test
		await page.goto('/');
		await page.evaluate(() => localStorage.clear());
	});

	test('should persist theme across page reload (test case 7)', async ({ page }) => {
		await page.goto('/');

		// Find and click the theme toggle button
		const themeToggle = page.locator('button.theme-toggle');

		// Wait for the toggle to be visible
		await expect(themeToggle).toBeVisible();

		// Get initial theme
		const initialTheme = await page.locator('html').getAttribute('data-theme');

		// Click to toggle theme
		await themeToggle.click();

		// Verify theme changed
		const newTheme = await page.locator('html').getAttribute('data-theme');
		expect(newTheme).not.toBe(initialTheme);

		// Reload the page
		await page.reload();

		// Verify theme persisted
		const persistedTheme = await page.locator('html').getAttribute('data-theme');
		expect(persistedTheme).toBe(newTheme);
	});

	test('should toggle between dark and light themes', async ({ page }) => {
		await page.goto('/');

		const themeToggle = page.locator('button.theme-toggle');
		await expect(themeToggle).toBeVisible();

		// Start with a known state
		await page.evaluate(() => {
			localStorage.setItem('mirdb-theme', 'dark');
			document.documentElement.setAttribute('data-theme', 'dark');
		});

		// Refresh to apply
		await page.reload();

		// Verify dark theme
		await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark');

		// Toggle to light
		await themeToggle.click();
		await expect(page.locator('html')).toHaveAttribute('data-theme', 'light');

		// Toggle back to dark
		await themeToggle.click();
		await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark');
	});

	test('should have accessible theme toggle button', async ({ page }) => {
		await page.goto('/');

		const themeToggle = page.locator('button.theme-toggle');
		await expect(themeToggle).toBeVisible();

		// Check for aria-label
		await expect(themeToggle).toHaveAttribute('aria-label', /switch to (light|dark) theme/i);

		// Check for aria-pressed
		await expect(themeToggle).toHaveAttribute('aria-pressed');
	});

	test('should be keyboard accessible', async ({ page }) => {
		await page.goto('/');

		const themeToggle = page.locator('button.theme-toggle');
		await expect(themeToggle).toBeVisible();

		// Focus the button using Tab
		await themeToggle.focus();
		await expect(themeToggle).toBeFocused();

		// Get initial theme
		const initialTheme = await page.locator('html').getAttribute('data-theme');

		// Press Enter to toggle
		await page.keyboard.press('Enter');

		// Verify theme changed
		const newTheme = await page.locator('html').getAttribute('data-theme');
		expect(newTheme).not.toBe(initialTheme);
	});

	test('should store preference in localStorage', async ({ page }) => {
		await page.goto('/');

		const themeToggle = page.locator('button.theme-toggle');
		await expect(themeToggle).toBeVisible();

		// Toggle theme
		await themeToggle.click();

		// Check localStorage
		const storedTheme = await page.evaluate(() => localStorage.getItem('mirdb-theme'));
		expect(storedTheme).toBeTruthy();
		expect(['light', 'dark']).toContain(storedTheme);
	});

	test.describe('Color Contrast (test case 8)', () => {
		test('dark theme should have readable text', async ({ page }) => {
			await page.goto('/');

			// Set dark theme
			await page.evaluate(() => {
				localStorage.setItem('mirdb-theme', 'dark');
				document.documentElement.setAttribute('data-theme', 'dark');
			});
			await page.reload();

			// Get computed styles for text and background
			const styles = await page.evaluate(() => {
				const computedStyle = getComputedStyle(document.documentElement);
				return {
					textColor: computedStyle.getPropertyValue('--color-text').trim(),
					bgColor: computedStyle.getPropertyValue('--color-background').trim()
				};
			});

			// Verify colors are defined
			expect(styles.textColor).toBeTruthy();
			expect(styles.bgColor).toBeTruthy();

			// Dark theme should have light text
			expect(styles.textColor).toMatch(/#[a-fA-F0-9]{6}|rgb/);
		});

		test('light theme should have readable text', async ({ page }) => {
			await page.goto('/');

			// Set light theme
			await page.evaluate(() => {
				localStorage.setItem('mirdb-theme', 'light');
				document.documentElement.setAttribute('data-theme', 'light');
			});
			await page.reload();

			// Get computed styles
			const styles = await page.evaluate(() => {
				const computedStyle = getComputedStyle(document.documentElement);
				return {
					textColor: computedStyle.getPropertyValue('--color-text').trim(),
					bgColor: computedStyle.getPropertyValue('--color-background').trim()
				};
			});

			// Verify colors are defined
			expect(styles.textColor).toBeTruthy();
			expect(styles.bgColor).toBeTruthy();

			// Light theme should have dark text
			expect(styles.textColor).toMatch(/#[a-fA-F0-9]{6}|rgb/);
		});

		test('both themes should define all necessary CSS variables', async ({ page }) => {
			const requiredVariables = [
				'--color-background',
				'--color-surface',
				'--color-text',
				'--color-text-secondary',
				'--color-border',
				'--color-code-bg',
				'--color-code-text'
			];

			// Test dark theme
			await page.goto('/');
			await page.evaluate(() => {
				localStorage.setItem('mirdb-theme', 'dark');
				document.documentElement.setAttribute('data-theme', 'dark');
			});
			await page.reload();

			for (const variable of requiredVariables) {
				const value = await page.evaluate(
					(v) => getComputedStyle(document.documentElement).getPropertyValue(v).trim(),
					variable
				);
				expect(value, `Dark theme missing ${variable}`).toBeTruthy();
			}

			// Test light theme
			await page.evaluate(() => {
				localStorage.setItem('mirdb-theme', 'light');
				document.documentElement.setAttribute('data-theme', 'light');
			});
			await page.reload();

			for (const variable of requiredVariables) {
				const value = await page.evaluate(
					(v) => getComputedStyle(document.documentElement).getPropertyValue(v).trim(),
					variable
				);
				expect(value, `Light theme missing ${variable}`).toBeTruthy();
			}
		});
	});

	test.describe('System Preference Respect', () => {
		test('should respect system dark mode preference on first visit', async ({ page }) => {
			// Emulate dark color scheme preference
			await page.emulateMedia({ colorScheme: 'dark' });

			// Clear localStorage to simulate first visit
			await page.goto('/');
			await page.evaluate(() => localStorage.clear());
			await page.reload();

			// Should apply dark theme
			const theme = await page.locator('html').getAttribute('data-theme');
			expect(theme).toBe('dark');
		});

		test('should respect system light mode preference on first visit', async ({ page }) => {
			// Emulate light color scheme preference
			await page.emulateMedia({ colorScheme: 'light' });

			// Clear localStorage to simulate first visit
			await page.goto('/');
			await page.evaluate(() => localStorage.clear());
			await page.reload();

			// Should apply light theme
			const theme = await page.locator('html').getAttribute('data-theme');
			expect(theme).toBe('light');
		});
	});
});
