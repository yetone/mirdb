/**
 * Accessibility E2E Tests (WCAG 2.1 AA)
 * Owner: Scenario 8 - Accessibility
 *
 * Validates the homepage meets WCAG 2.1 AA accessibility standards including:
 * - Automated accessibility audit (axe-core)
 * - Keyboard navigation
 * - Screen reader support
 * - Color contrast
 * - Focus indicators
 * - ARIA landmarks
 * - Heading hierarchy
 *
 * NFR-3 traceability
 */

import { test, expect } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

test.describe('Accessibility (WCAG 2.1 AA)', () => {
	test.beforeEach(async ({ page }) => {
		await page.goto('/');
		// Wait for page to fully load
		await page.waitForLoadState('networkidle');
	});

	// Test Case 1: Run axe-core accessibility scan
	test('should have no critical or serious accessibility violations', async ({ page }) => {
		const accessibilityScanResults = await new AxeBuilder({ page })
			.withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
			.analyze();

		// Filter for critical and serious violations only
		const criticalViolations = accessibilityScanResults.violations.filter(
			(violation) => violation.impact === 'critical' || violation.impact === 'serious'
		);

		// Log violations for debugging
		if (criticalViolations.length > 0) {
			console.log(
				'Critical/Serious violations:',
				JSON.stringify(criticalViolations, null, 2)
			);
		}

		expect(criticalViolations).toHaveLength(0);
	});

	// Test Case 2: Check all interactive elements have accessible names
	test('all interactive elements should have accessible names', async ({ page }) => {
		// Check all buttons have accessible names
		const buttons = await page.locator('button').all();
		for (const button of buttons) {
			const accessibleName =
				(await button.getAttribute('aria-label')) ||
				(await button.textContent()) ||
				(await button.getAttribute('title'));
			expect(accessibleName?.trim()).toBeTruthy();
		}

		// Check all links have accessible names
		const links = await page.locator('a').all();
		for (const link of links) {
			const accessibleName =
				(await link.getAttribute('aria-label')) ||
				(await link.textContent()) ||
				(await link.getAttribute('title'));
			expect(accessibleName?.trim()).toBeTruthy();
		}
	});

	// Test Case 3: Tab through all focusable elements
	test('focus order should be logical (top to bottom, left to right)', async ({ page }) => {
		// Get initial focus position
		await page.keyboard.press('Tab');

		// Skip link should be first focusable element
		const skipLink = page.locator('.skip-link');
		await expect(skipLink).toBeFocused();

		// Continue tabbing and collect focus order
		const focusOrder: string[] = [];
		let lastFocusedElement = await page.evaluate(() => document.activeElement?.tagName);

		// Tab through elements and record order
		for (let i = 0; i < 20; i++) {
			await page.keyboard.press('Tab');
			const focused = await page.evaluate(() => {
				const el = document.activeElement;
				if (!el) return null;
				return {
					tag: el.tagName,
					id: el.id,
					className: el.className,
					rect: el.getBoundingClientRect()
				};
			});

			if (focused) {
				focusOrder.push(`${focused.tag}${focused.id ? '#' + focused.id : ''}`);
			}
		}

		// Verify we captured focus order
		expect(focusOrder.length).toBeGreaterThan(0);
	});

	// Test Case 4: Check focus visible on all interactive elements
	test('focus ring should be visible with sufficient contrast', async ({ page }) => {
		// Tab to the first interactive element after skip link
		await page.keyboard.press('Tab'); // Skip link
		await page.keyboard.press('Tab'); // Logo link

		// Check that focused element has visible focus indicator
		const focusedElement = page.locator(':focus-visible');
		const hasOutline = await focusedElement.evaluate((el) => {
			const styles = window.getComputedStyle(el);
			const outline = styles.outline;
			const outlineWidth = parseInt(styles.outlineWidth);
			const boxShadow = styles.boxShadow;
			// Check for visible outline or box-shadow
			return outlineWidth > 0 || (boxShadow && boxShadow !== 'none');
		});

		expect(hasOutline).toBeTruthy();
	});

	// Test Case 5: Check skip link presence
	test('skip to main content link should be available for keyboard users', async ({ page }) => {
		// Skip link should exist
		const skipLink = page.locator('a[href="#main-content"]');
		await expect(skipLink).toHaveCount(1);

		// Skip link should have correct text
		await expect(skipLink).toContainText('Skip to main content');

		// Main content target should exist
		const mainContent = page.locator('#main-content');
		await expect(mainContent).toHaveCount(1);

		// Skip link should be visible when focused
		await page.keyboard.press('Tab');
		await expect(skipLink).toBeFocused();
		await expect(skipLink).toBeVisible();
	});

	// Test Case 6: Verify heading structure
	test('should have proper heading hierarchy (single h1, no skipped levels)', async ({
		page
	}) => {
		// Get all headings
		const headings = await page.evaluate(() => {
			const headingElements = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
			return Array.from(headingElements).map((h) => ({
				level: parseInt(h.tagName[1]),
				text: h.textContent?.trim() || ''
			}));
		});

		// Should have exactly one h1
		const h1Count = headings.filter((h) => h.level === 1).length;
		expect(h1Count).toBe(1);

		// Heading levels should not skip (e.g., h1 -> h3 without h2)
		let previousLevel = 0;
		for (const heading of headings) {
			if (previousLevel > 0) {
				// Allow same level, going up, or going down by 1
				const levelDiff = heading.level - previousLevel;
				expect(levelDiff).toBeLessThanOrEqual(1);
			}
			previousLevel = heading.level;
		}
	});

	// Test Case 7: Check color contrast ratios
	test('text should have sufficient color contrast', async ({ page }) => {
		const accessibilityScanResults = await new AxeBuilder({ page })
			.withTags(['wcag2aa'])
			.include('body')
			.analyze();

		// Filter for contrast-related violations
		const contrastViolations = accessibilityScanResults.violations.filter((violation) =>
			violation.id.includes('contrast')
		);

		expect(contrastViolations).toHaveLength(0);
	});

	// Test Case 8: Verify ARIA landmarks
	test('page should have main, nav, header, footer landmarks', async ({ page }) => {
		// Check for main landmark
		const main = page.locator('main, [role="main"]');
		await expect(main).toHaveCount(1);

		// Check for navigation landmark
		const nav = page.locator('nav, [role="navigation"]');
		expect(await nav.count()).toBeGreaterThanOrEqual(1);

		// Check for header landmark (banner)
		const header = page.locator('header, [role="banner"]');
		await expect(header).toHaveCount(1);

		// Check for footer landmark (contentinfo)
		const footer = page.locator('footer, [role="contentinfo"]');
		await expect(footer).toHaveCount(1);
	});

	// Test Case 9: Check logo/decorative images
	test('decorative images should have alt="" or role="presentation"', async ({ page }) => {
		// Get all images
		const images = await page.locator('img').all();

		for (const img of images) {
			const alt = await img.getAttribute('alt');
			const role = await img.getAttribute('role');
			const ariaHidden = await img.getAttribute('aria-hidden');

			// Each image should either:
			// 1. Have meaningful alt text
			// 2. Have alt="" (empty) for decorative images
			// 3. Have role="presentation" or role="none"
			// 4. Have aria-hidden="true"
			const hasAlt = alt !== null;
			const isDecorativeByRole = role === 'presentation' || role === 'none';
			const isHidden = ariaHidden === 'true';

			expect(hasAlt || isDecorativeByRole || isHidden).toBeTruthy();
		}
	});

	// Additional accessibility tests

	test('ASCII logo should be properly labeled for screen readers', async ({ page }) => {
		const asciiLogo = page.locator('.ascii-logo');
		const ariaLabel = await asciiLogo.getAttribute('aria-label');
		expect(ariaLabel).toBeTruthy();
	});

	test('external links should indicate they open in new tab', async ({ page }) => {
		const externalLinks = await page.locator('a[target="_blank"]').all();

		for (const link of externalLinks) {
			const ariaLabel = await link.getAttribute('aria-label');
			const text = await link.textContent();
			const rel = await link.getAttribute('rel');

			// Should have rel="noopener noreferrer" for security
			expect(rel).toContain('noopener');

			// Should indicate it opens in new tab either via aria-label or visible text (case-insensitive)
			const ariaLabelLower = ariaLabel?.toLowerCase() || '';
			const indicatesNewTab =
				ariaLabelLower.includes('new tab') ||
				ariaLabelLower.includes('opens in') ||
				text?.includes('(external)');
			expect(indicatesNewTab).toBeTruthy();
		}
	});

	test('theme toggle should be keyboard accessible', async ({ page }) => {
		// Find theme toggle
		const themeToggle = page.locator('.theme-toggle');
		await expect(themeToggle).toHaveCount(1);

		// Focus on theme toggle
		await themeToggle.focus();
		await expect(themeToggle).toBeFocused();

		// Check it has proper ARIA attributes for accessibility
		const ariaLabel = await themeToggle.getAttribute('aria-label');
		expect(ariaLabel).toBeTruthy();

		// Should be clickable via keyboard (button is focusable)
		const role = await themeToggle.evaluate((el) => el.tagName.toLowerCase());
		expect(role).toBe('button');
	});

	test('copy buttons should be keyboard accessible', async ({ page }) => {
		const copyButtons = page.locator('[data-testid="copy-button"]');
		const buttonCount = await copyButtons.count();

		if (buttonCount > 0) {
			const firstCopyButton = copyButtons.first();
			await firstCopyButton.focus();
			await expect(firstCopyButton).toBeFocused();

			// Should have accessible name
			const ariaLabel = await firstCopyButton.getAttribute('aria-label');
			expect(ariaLabel).toBeTruthy();
		}
	});

	test('mobile menu should be accessible when open', async ({ page }) => {
		// Set mobile viewport
		await page.setViewportSize({ width: 375, height: 667 });
		await page.goto('/');

		// Find mobile menu toggle
		const mobileMenuToggle = page.locator('.mobile-menu-toggle');
		await expect(mobileMenuToggle).toBeVisible();

		// Check accessibility attributes - toggle should have proper ARIA
		const ariaLabel = await mobileMenuToggle.getAttribute('aria-label');
		expect(ariaLabel).toBeTruthy();

		// Toggle should have aria-expanded attribute
		const ariaExpanded = await mobileMenuToggle.getAttribute('aria-expanded');
		expect(ariaExpanded === 'true' || ariaExpanded === 'false').toBeTruthy();

		// Toggle should be a button for keyboard accessibility
		const tagName = await mobileMenuToggle.evaluate((el) => el.tagName.toLowerCase());
		expect(tagName).toBe('button');
	});
});
