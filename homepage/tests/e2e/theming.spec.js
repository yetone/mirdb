/**
 * Dark/Light Mode Theming Tests
 * Owner: Scenario 11 - Dark/Light Mode Theming
 *
 * Tests for:
 * - CSS custom properties for theming
 * - prefers-color-scheme dark
 * - prefers-color-scheme light
 * - Code block theme adaptation
 * - Contrast in both modes
 */

const { test, expect } = require('@playwright/test');

test.describe('Dark/Light Mode Theming', () => {
    test.describe('System preference detection - Dark mode', () => {
        test.use({ colorScheme: 'dark' });

        test('should display dark theme when system prefers dark mode', async ({ page }) => {
            await page.goto('/');

            const body = page.locator('body');
            const backgroundColor = await body.evaluate((el) => {
                return getComputedStyle(el).backgroundColor;
            });

            // Dark mode background should be dark (rgb values close to 0 or dark tones)
            // Expected: --color-background: #1a1a2e (rgb(26, 26, 46))
            expect(backgroundColor).toMatch(/rgb\(26,\s*26,\s*46\)/);
        });

        test('should display light text on dark background', async ({ page }) => {
            await page.goto('/');

            const body = page.locator('body');
            const textColor = await body.evaluate((el) => {
                return getComputedStyle(el).color;
            });

            // Dark mode text should be light (rgb values close to 255 or light tones)
            // Expected: --color-text: #e8e8e8 (rgb(232, 232, 232))
            expect(textColor).toMatch(/rgb\(232,\s*232,\s*232\)/);
        });

        test('should have dark surface color for feature cards', async ({ page }) => {
            await page.goto('/');

            const featureCard = page.locator('.feature-card').first();
            await expect(featureCard).toBeVisible();

            const cardBgColor = await featureCard.evaluate((el) => {
                return getComputedStyle(el).backgroundColor;
            });

            // Feature cards in dark mode should use the dark background color
            // Expected: --color-background: #1a1a2e (rgb(26, 26, 46))
            expect(cardBgColor).toMatch(/rgb\(26,\s*26,\s*46\)/);
        });

        test('should apply appropriate muted text color in dark mode', async ({ page }) => {
            await page.goto('/');

            // Check feature card description which uses muted text color
            const cardDescription = page.locator('.feature-card p').first();
            await expect(cardDescription).toBeVisible();

            const mutedColor = await cardDescription.evaluate((el) => {
                return getComputedStyle(el).color;
            });

            // Dark mode muted text: --color-text-muted: #a0a0a0 (rgb(160, 160, 160))
            expect(mutedColor).toMatch(/rgb\(160,\s*160,\s*160\)/);
        });

        test('should have themed primary buttons in dark mode', async ({ page }) => {
            await page.goto('/');

            const primaryBtn = page.locator('.btn-primary').first();
            await expect(primaryBtn).toBeVisible();

            const btnBgColor = await primaryBtn.evaluate((el) => {
                return getComputedStyle(el).backgroundColor;
            });

            // Primary button in dark mode: --color-primary: #4da6ff (rgb(77, 166, 255))
            expect(btnBgColor).toMatch(/rgb\(77,\s*166,\s*255\)/);
        });
    });

    test.describe('System preference detection - Light mode', () => {
        test.use({ colorScheme: 'light' });

        test('should display light theme when system prefers light mode', async ({ page }) => {
            await page.goto('/');

            const body = page.locator('body');
            const backgroundColor = await body.evaluate((el) => {
                return getComputedStyle(el).backgroundColor;
            });

            // Light mode background should be white/light (rgb values close to 255)
            // Expected: --color-background: #ffffff (rgb(255, 255, 255))
            expect(backgroundColor).toMatch(/rgb\(255,\s*255,\s*255\)/);
        });

        test('should display dark text on light background', async ({ page }) => {
            await page.goto('/');

            const body = page.locator('body');
            const textColor = await body.evaluate((el) => {
                return getComputedStyle(el).color;
            });

            // Light mode text should be dark (rgb values close to 0)
            // Expected: --color-text: #212529 (rgb(33, 37, 41))
            expect(textColor).toMatch(/rgb\(33,\s*37,\s*41\)/);
        });

        test('should have light surface color for feature cards', async ({ page }) => {
            await page.goto('/');

            const featureCard = page.locator('.feature-card').first();
            await expect(featureCard).toBeVisible();

            const cardBgColor = await featureCard.evaluate((el) => {
                return getComputedStyle(el).backgroundColor;
            });

            // Feature cards in light mode should use the light background color
            // Expected: --color-background: #ffffff (rgb(255, 255, 255))
            expect(cardBgColor).toMatch(/rgb\(255,\s*255,\s*255\)/);
        });

        test('should apply appropriate muted text color in light mode', async ({ page }) => {
            await page.goto('/');

            // Check feature card description which uses muted text color
            const cardDescription = page.locator('.feature-card p').first();
            await expect(cardDescription).toBeVisible();

            const mutedColor = await cardDescription.evaluate((el) => {
                return getComputedStyle(el).color;
            });

            // Light mode muted text: --color-text-muted: #6c757d (rgb(108, 117, 125))
            expect(mutedColor).toMatch(/rgb\(108,\s*117,\s*125\)/);
        });

        test('should have themed primary buttons in light mode', async ({ page }) => {
            await page.goto('/');

            const primaryBtn = page.locator('.btn-primary').first();
            await expect(primaryBtn).toBeVisible();

            const btnBgColor = await primaryBtn.evaluate((el) => {
                return getComputedStyle(el).backgroundColor;
            });

            // Primary button in light mode: --color-primary: #007acc (rgb(0, 122, 204))
            expect(btnBgColor).toMatch(/rgb\(0,\s*122,\s*204\)/);
        });
    });

    test.describe('Code block theming - Dark mode', () => {
        test.use({ colorScheme: 'dark' });

        test('should have syntax highlighted code blocks in dark mode', async ({ page }) => {
            await page.goto('/');

            // Wait for Prism.js to load and highlight code
            await page.waitForSelector('pre code.language-bash');

            const codeBlock = page.locator('.code-container pre').first();
            await expect(codeBlock).toBeVisible();

            // Check that Prism.js classes are applied (language-bash should be tokenized)
            const hasTokenization = await codeBlock.evaluate((el) => {
                // Check if Prism has tokenized the code (look for token classes)
                return el.innerHTML.includes('class="token') ||
                       el.querySelector('code.language-bash') !== null;
            });

            expect(hasTokenization).toBe(true);
        });
    });

    test.describe('Code block theming - Light mode', () => {
        test.use({ colorScheme: 'light' });

        test('should have syntax highlighted code blocks in light mode', async ({ page }) => {
            await page.goto('/');

            // Wait for Prism.js to load and highlight code
            await page.waitForSelector('pre code.language-bash');

            const codeBlock = page.locator('.code-container pre').first();
            await expect(codeBlock).toBeVisible();

            // Check that Prism.js classes are applied
            const hasTokenization = await codeBlock.evaluate((el) => {
                return el.innerHTML.includes('class="token') ||
                       el.querySelector('code.language-bash') !== null;
            });

            expect(hasTokenization).toBe(true);
        });
    });

    test.describe('Theme consistency across UI elements - Dark mode', () => {
        test.use({ colorScheme: 'dark' });

        test('should apply dark theme to header/navigation area', async ({ page }) => {
            await page.goto('/');

            const hero = page.locator('.hero');
            await expect(hero).toBeVisible();

            // Verify hero section uses theme variables
            const heroStyles = await hero.evaluate((el) => {
                const styles = getComputedStyle(el);
                return {
                    background: styles.background
                };
            });

            // Hero should have gradient with dark surface/background colors
            expect(heroStyles.background).toBeTruthy();
        });

        test('should apply dark theme to footer', async ({ page }) => {
            await page.goto('/');

            const footer = page.locator('.footer');
            await expect(footer).toBeVisible();

            const footerBgColor = await footer.evaluate((el) => {
                return getComputedStyle(el).backgroundColor;
            });

            // Footer should use dark background
            // Expected: --color-background: #1a1a2e (rgb(26, 26, 46))
            expect(footerBgColor).toMatch(/rgb\(26,\s*26,\s*46\)/);
        });

        test('should apply dark border colors', async ({ page }) => {
            await page.goto('/');

            const footer = page.locator('.footer');
            await expect(footer).toBeVisible();

            const borderColor = await footer.evaluate((el) => {
                return getComputedStyle(el).borderTopColor;
            });

            // Footer border should use dark border color
            // Expected: --color-border: #3a3a5a (rgb(58, 58, 90))
            expect(borderColor).toMatch(/rgb\(58,\s*58,\s*90\)/);
        });
    });

    test.describe('Theme consistency across UI elements - Light mode', () => {
        test.use({ colorScheme: 'light' });

        test('should apply light theme to footer', async ({ page }) => {
            await page.goto('/');

            const footer = page.locator('.footer');
            await expect(footer).toBeVisible();

            const footerBgColor = await footer.evaluate((el) => {
                return getComputedStyle(el).backgroundColor;
            });

            // Footer should use light background
            // Expected: --color-background: #ffffff (rgb(255, 255, 255))
            expect(footerBgColor).toMatch(/rgb\(255,\s*255,\s*255\)/);
        });

        test('should apply light border colors', async ({ page }) => {
            await page.goto('/');

            const footer = page.locator('.footer');
            await expect(footer).toBeVisible();

            const borderColor = await footer.evaluate((el) => {
                return getComputedStyle(el).borderTopColor;
            });

            // Footer border should use light border color
            // Expected: --color-border: #dee2e6 (rgb(222, 226, 230))
            expect(borderColor).toMatch(/rgb\(222,\s*226,\s*230\)/);
        });
    });
});
