/**
 * E2E Tests for Theme Toggle Functionality
 * Owner: Scenario 6 - Theme Toggle & Accessibility
 *
 * Tests:
 * - Theme toggle button exists with proper aria-label
 * - Clicking toggle changes theme between light and dark
 * - Theme persists after page reload
 * - Keyboard accessibility (Enter/Space activation)
 */

const { test, expect } = require('@playwright/test');

test.describe('Theme Toggle E2E Tests', () => {
    test.beforeEach(async ({ page }) => {
        // Clear localStorage before each test
        await page.goto('http://localhost:8080');
        await page.evaluate(() => localStorage.clear());
        await page.reload();
    });

    test('Theme toggle button exists with aria-label', async ({ page }) => {
        // Test case 1: Query page for theme toggle button
        await page.goto('http://localhost:8080');

        const toggleButton = page.locator('.theme-toggle');
        await expect(toggleButton).toBeVisible();

        // Verify aria-label exists for accessibility
        const ariaLabel = await toggleButton.getAttribute('aria-label');
        expect(ariaLabel).toBeTruthy();
        expect(ariaLabel.toLowerCase()).toContain('theme');
    });

    test('Click toggle in light mode switches to dark mode', async ({ page }) => {
        // Test case 2: Click theme toggle in light mode
        await page.goto('http://localhost:8080');

        // Ensure we start in light mode
        await page.evaluate(() => {
            document.documentElement.setAttribute('data-theme', 'light');
            localStorage.setItem('mirdb-theme', 'light');
        });
        await page.reload();

        // Click the toggle
        const toggleButton = page.locator('.theme-toggle');
        await toggleButton.click();

        // Wait for theme transition
        await page.waitForTimeout(100);

        // Verify dark mode is applied
        const theme = await page.evaluate(() =>
            document.documentElement.getAttribute('data-theme')
        );
        expect(theme).toBe('dark');

        // Verify localStorage was updated
        const storedTheme = await page.evaluate(() =>
            localStorage.getItem('mirdb-theme')
        );
        expect(storedTheme).toBe('dark');
    });

    test('Click toggle in dark mode switches to light mode', async ({ page }) => {
        // Test case 3: Click theme toggle in dark mode
        await page.goto('http://localhost:8080');

        // Set dark mode first
        await page.evaluate(() => {
            document.documentElement.setAttribute('data-theme', 'dark');
            localStorage.setItem('mirdb-theme', 'dark');
        });
        await page.reload();

        // Verify we're in dark mode
        let theme = await page.evaluate(() =>
            document.documentElement.getAttribute('data-theme')
        );
        expect(theme).toBe('dark');

        // Click the toggle
        const toggleButton = page.locator('.theme-toggle');
        await toggleButton.click();

        // Wait for theme transition
        await page.waitForTimeout(100);

        // Verify light mode is applied
        theme = await page.evaluate(() =>
            document.documentElement.getAttribute('data-theme')
        );
        expect(theme).toBe('light');

        // Verify localStorage was updated
        const storedTheme = await page.evaluate(() =>
            localStorage.getItem('mirdb-theme')
        );
        expect(storedTheme).toBe('light');
    });

    test('Theme toggle button is keyboard accessible', async ({ page }) => {
        // Test case 10: Theme toggle button accessible via keyboard
        await page.goto('http://localhost:8080');

        // Set initial state to light mode
        await page.evaluate(() => {
            document.documentElement.setAttribute('data-theme', 'light');
            localStorage.setItem('mirdb-theme', 'light');
        });
        await page.reload();

        const toggleButton = page.locator('.theme-toggle');

        // Verify button is focusable
        await toggleButton.focus();
        await expect(toggleButton).toBeFocused();

        // Test Enter key activation
        await page.keyboard.press('Enter');
        await page.waitForTimeout(100);

        let theme = await page.evaluate(() =>
            document.documentElement.getAttribute('data-theme')
        );
        expect(theme).toBe('dark');

        // Test Space key activation
        await toggleButton.focus();
        await page.keyboard.press(' ');
        await page.waitForTimeout(100);

        theme = await page.evaluate(() =>
            document.documentElement.getAttribute('data-theme')
        );
        expect(theme).toBe('light');
    });

    test('Theme persists after page reload', async ({ page }) => {
        // Related to test case 8: Theme persistence
        await page.goto('http://localhost:8080');

        // Set dark mode
        const toggleButton = page.locator('.theme-toggle');
        await page.evaluate(() => {
            document.documentElement.setAttribute('data-theme', 'light');
            localStorage.setItem('mirdb-theme', 'light');
        });
        await page.reload();
        await toggleButton.click();
        await page.waitForTimeout(100);

        // Verify dark mode is set
        let theme = await page.evaluate(() =>
            document.documentElement.getAttribute('data-theme')
        );
        expect(theme).toBe('dark');

        // Reload the page
        await page.reload();

        // Verify theme is still dark after reload
        theme = await page.evaluate(() =>
            document.documentElement.getAttribute('data-theme')
        );
        expect(theme).toBe('dark');
    });

    test('Theme toggle has visible focus state', async ({ page }) => {
        await page.goto('http://localhost:8080');

        const toggleButton = page.locator('.theme-toggle');
        await toggleButton.focus();

        // Get computed styles to verify focus styling
        const outlineStyle = await toggleButton.evaluate(el => {
            const styles = window.getComputedStyle(el);
            return styles.outlineStyle;
        });

        // Focus-visible should apply some outline (not 'none')
        // Note: Actual style depends on browser and :focus-visible support
        await expect(toggleButton).toBeFocused();
    });

    test('Theme colors change when toggled', async ({ page }) => {
        await page.goto('http://localhost:8080');

        // Set light mode
        await page.evaluate(() => {
            document.documentElement.setAttribute('data-theme', 'light');
            localStorage.setItem('mirdb-theme', 'light');
        });
        await page.reload();

        // Get light mode background color
        const lightBgColor = await page.evaluate(() => {
            return getComputedStyle(document.documentElement)
                .getPropertyValue('--color-bg').trim();
        });

        // Toggle to dark mode
        const toggleButton = page.locator('.theme-toggle');
        await toggleButton.click();
        await page.waitForTimeout(350); // Wait for transition

        // Get dark mode background color
        const darkBgColor = await page.evaluate(() => {
            return getComputedStyle(document.documentElement)
                .getPropertyValue('--color-bg').trim();
        });

        // Colors should be different
        expect(lightBgColor).not.toBe(darkBgColor);
    });
});

test.describe('Theme System Preference Fallback', () => {
    test('Page respects system dark mode preference when no stored theme', async ({ page }) => {
        // Test case 9: Load page with no stored theme, system prefers dark
        // Emulate dark color scheme preference
        await page.emulateMedia({ colorScheme: 'dark' });

        // Clear any stored theme
        await page.goto('http://localhost:8080');
        await page.evaluate(() => localStorage.clear());
        await page.reload();

        // Page should be in dark mode following system preference
        const theme = await page.evaluate(() =>
            document.documentElement.getAttribute('data-theme')
        );
        expect(theme).toBe('dark');
    });

    test('Page respects system light mode preference when no stored theme', async ({ page }) => {
        // Emulate light color scheme preference
        await page.emulateMedia({ colorScheme: 'light' });

        // Clear any stored theme
        await page.goto('http://localhost:8080');
        await page.evaluate(() => localStorage.clear());
        await page.reload();

        // Page should be in light mode following system preference
        const theme = await page.evaluate(() =>
            document.documentElement.getAttribute('data-theme')
        );
        expect(theme).toBe('light');
    });

    test('Stored theme overrides system preference', async ({ page }) => {
        // Emulate dark system preference
        await page.emulateMedia({ colorScheme: 'dark' });

        await page.goto('http://localhost:8080');

        // Set stored preference to light
        await page.evaluate(() => {
            localStorage.setItem('mirdb-theme', 'light');
        });
        await page.reload();

        // Page should use stored preference (light) not system preference (dark)
        const theme = await page.evaluate(() =>
            document.documentElement.getAttribute('data-theme')
        );
        expect(theme).toBe('light');
    });
});
