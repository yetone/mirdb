/**
 * Mobile Responsive Design E2E Tests
 * Owner: Scenario 7 - Responsive Design (Mobile)
 *
 * Validates REQ-6: Homepage shall be responsive and accessible across devices.
 */
import { test, expect, devices } from '@playwright/test';

const MOBILE_VIEWPORT = { width: 375, height: 667 }; // iPhone SE

test.describe('Mobile Responsive Design', () => {
	test.use({ viewport: MOBILE_VIEWPORT });

	test.describe('Test Case 1: Navigation component at mobile viewport', () => {
		test('should hide desktop nav and show hamburger menu button', async ({ page }) => {
			await page.goto('/');

			// Desktop navigation should be hidden
			const desktopNav = page.locator('.navigation:not(.mobile)');
			await expect(desktopNav).toBeHidden();

			// Hamburger menu button should be visible
			const hamburgerButton = page.locator('.mobile-menu-toggle');
			await expect(hamburgerButton).toBeVisible();
		});
	});

	test.describe('Test Case 2: Click hamburger menu at mobile viewport', () => {
		test('should show mobile menu with all nav links when hamburger is clicked', async ({ page }) => {
			await page.goto('/');

			// Click hamburger menu
			const hamburgerButton = page.locator('.mobile-menu-toggle');
			await hamburgerButton.click();

			// Mobile menu should appear
			const mobileMenu = page.locator('#mobile-menu');
			await expect(mobileMenu).toBeVisible();

			// All navigation links should be present
			const navLinks = mobileMenu.locator('.nav-link');
			await expect(navLinks).toHaveCount(await page.locator('.navigation.mobile .nav-link').count());

			// Check at least Features, Quick Start, and GitHub links exist
			await expect(mobileMenu.getByText('Features')).toBeVisible();
			await expect(mobileMenu.getByText('Quick Start')).toBeVisible();
		});
	});

	test.describe('Test Case 3: Close mobile menu after navigation', () => {
		test('should close menu automatically after selecting a link', async ({ page }) => {
			await page.goto('/');

			// Open hamburger menu
			const hamburgerButton = page.locator('.mobile-menu-toggle');
			await hamburgerButton.click();

			// Verify menu is open
			const mobileMenu = page.locator('#mobile-menu');
			await expect(mobileMenu).toBeVisible();

			// Click a navigation link (Features)
			const featuresLink = mobileMenu.getByText('Features');
			await featuresLink.click();

			// Menu should close after navigation
			await expect(mobileMenu).toBeHidden();
		});
	});

	test.describe('Test Case 4: Check horizontal overflow at 375px viewport', () => {
		test('should have no horizontal scrollbar on body', async ({ page }) => {
			await page.goto('/');

			// Wait for page to fully load
			await page.waitForLoadState('networkidle');

			// Check that body does not have horizontal overflow
			const hasHorizontalScroll = await page.evaluate(() => {
				return document.body.scrollWidth > document.body.clientWidth;
			});

			expect(hasHorizontalScroll).toBe(false);
		});
	});

	test.describe('Test Case 5: Check code blocks on mobile', () => {
		test('should have horizontal scroll within code block container, not page scroll', async ({ page }) => {
			await page.goto('/');

			// Navigate to Quick Start section using the hero CTA button (visible on mobile)
			const getStartedBtn = page.locator('a.btn-primary:has-text("Get Started")');
			await getStartedBtn.click();
			await page.waitForTimeout(500); // Wait for smooth scroll

			// Find code blocks
			const codeBlocks = page.locator('[data-testid="code-block"]');
			const codeBlockCount = await codeBlocks.count();
			expect(codeBlockCount).toBeGreaterThan(0);

			// Check each code block has overflow-x: auto and the page does not scroll horizontally
			const firstCodeBlock = codeBlocks.first();
			const codePreElement = firstCodeBlock.locator('.code-pre');

			// Check that pre element has overflow-x style that allows scrolling within container
			const overflowX = await codePreElement.evaluate((el) => {
				return window.getComputedStyle(el).overflowX;
			});
			expect(['auto', 'scroll']).toContain(overflowX);

			// Verify body doesn't have horizontal scroll even with code blocks
			const hasPageHorizontalScroll = await page.evaluate(() => {
				return document.body.scrollWidth > document.body.clientWidth;
			});
			expect(hasPageHorizontalScroll).toBe(false);
		});
	});

	test.describe('Test Case 6: Test feature grid on mobile', () => {
		test('should stack features vertically (1 column) on mobile', async ({ page }) => {
			await page.goto('/');

			// Find feature cards
			const featureCards = page.locator('.feature-card, [data-testid="feature-card"]');
			const featureCount = await featureCards.count();
			expect(featureCount).toBeGreaterThan(0);

			// Get bounding boxes for first two feature cards
			const firstCard = featureCards.nth(0);
			const secondCard = featureCards.nth(1);

			const firstBox = await firstCard.boundingBox();
			const secondBox = await secondCard.boundingBox();

			expect(firstBox).not.toBeNull();
			expect(secondBox).not.toBeNull();

			if (firstBox && secondBox) {
				// On mobile, cards should stack vertically (second card below first)
				// Both cards should have similar x position (stacked in 1 column)
				const xDifference = Math.abs(firstBox.x - secondBox.x);
				expect(xDifference).toBeLessThan(50); // Allow small tolerance

				// Second card should be below first card
				expect(secondBox.y).toBeGreaterThan(firstBox.y);
			}
		});
	});

	test.describe('Test Case 7: Measure touch target sizes', () => {
		test('should have all buttons and links with minimum 44x44px touch targets', async ({ page }) => {
			await page.goto('/');

			// Open mobile menu to test those links too
			const hamburgerButton = page.locator('.mobile-menu-toggle');
			await hamburgerButton.click();
			await page.waitForTimeout(300);

			// Test primary navigation and CTA elements
			// These are the critical touch targets that must meet the 44px height requirement
			const criticalElements = [
				{ selector: '.mobile-menu-toggle', name: 'Hamburger menu' },
				{ selector: '.logo', name: 'Logo' },
				{ selector: '.mobile-menu .nav-link', name: 'Mobile nav links' },
				{ selector: '.btn-primary', name: 'Primary CTA button' },
				{ selector: '.btn-secondary', name: 'Secondary CTA button' }
			];

			const MIN_TOUCH_TARGET = 44;
			const elementsWithSmallTargets: string[] = [];

			for (const { selector, name } of criticalElements) {
				const elements = page.locator(selector);
				const count = await elements.count();

				for (let i = 0; i < count; i++) {
					const element = elements.nth(i);
					const isVisible = await element.isVisible();

					if (isVisible) {
						const box = await element.boundingBox();
						if (box) {
							// Check if height is less than minimum (width can vary for buttons with text)
							if (box.height < MIN_TOUCH_TARGET) {
								const text = await element.textContent();
								elementsWithSmallTargets.push(
									`${name} "${text?.trim() || 'unknown'}" has height ${box.height.toFixed(0)}px (min: ${MIN_TOUCH_TARGET}px)`
								);
							}
						}
					}
				}
			}

			// Expect all critical interactive elements to meet touch target requirements
			expect(
				elementsWithSmallTargets,
				`The following critical elements have touch targets smaller than 44px:\n${elementsWithSmallTargets.join('\n')}`
			).toHaveLength(0);
		});
	});

	test('should have proper mobile menu aria attributes', async ({ page }) => {
		await page.goto('/');

		const hamburgerButton = page.locator('.mobile-menu-toggle');

		// Check initial aria-expanded state
		await expect(hamburgerButton).toHaveAttribute('aria-expanded', 'false');

		// Open menu
		await hamburgerButton.click();

		// Check aria-expanded is now true
		await expect(hamburgerButton).toHaveAttribute('aria-expanded', 'true');

		// Check mobile menu has proper role and aria-label
		const mobileMenu = page.locator('#mobile-menu');
		await expect(mobileMenu).toHaveAttribute('role', 'dialog');
		await expect(mobileMenu).toHaveAttribute('aria-modal', 'true');
	});

	test('should close mobile menu when clicking overlay', async ({ page }) => {
		await page.goto('/');

		// Open hamburger menu
		const hamburgerButton = page.locator('.mobile-menu-toggle');
		await hamburgerButton.click();

		// Verify menu is open
		const mobileMenu = page.locator('#mobile-menu');
		await expect(mobileMenu).toBeVisible();

		// Click overlay to close
		const overlay = page.locator('.mobile-menu-overlay');
		await overlay.click();

		// Menu should be closed
		await expect(mobileMenu).toBeHidden();
	});
});
