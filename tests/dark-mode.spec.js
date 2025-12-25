// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Dark Mode Support', () => {
    test.describe('Test Case 1: Page switches to dark color scheme', () => {
        test('should display dark color scheme when prefers-color-scheme is dark', async ({ page }) => {
            // Set dark mode preference before navigating
            await page.emulateMedia({ colorScheme: 'dark' });
            await page.goto('/');

            // Wait for page to load
            await page.waitForLoadState('domcontentloaded');

            // Check that the body has dark background color
            const body = page.locator('body');
            const bgColor = await body.evaluate((el) => {
                return window.getComputedStyle(el).backgroundColor;
            });

            // Dark mode background should be dark (#0f172a = rgb(15, 23, 42))
            expect(bgColor).toBe('rgb(15, 23, 42)');
        });

        test('should display light color scheme by default (prefers-color-scheme: light)', async ({ page }) => {
            // Set light mode preference
            await page.emulateMedia({ colorScheme: 'light' });
            await page.goto('/');

            await page.waitForLoadState('domcontentloaded');

            const body = page.locator('body');
            const bgColor = await body.evaluate((el) => {
                return window.getComputedStyle(el).backgroundColor;
            });

            // Light mode background should be white (#ffffff = rgb(255, 255, 255))
            expect(bgColor).toBe('rgb(255, 255, 255)');
        });

        test('should update header background in dark mode', async ({ page }) => {
            await page.emulateMedia({ colorScheme: 'dark' });
            await page.goto('/');

            const header = page.locator('.header');
            const headerBgColor = await header.evaluate((el) => {
                return window.getComputedStyle(el).backgroundColor;
            });

            // Dark mode header background should be dark
            expect(headerBgColor).toBe('rgb(15, 23, 42)');
        });

        test('should update text colors in dark mode', async ({ page }) => {
            await page.emulateMedia({ colorScheme: 'dark' });
            await page.goto('/');

            // Check hero title color
            const heroTitle = page.locator('[data-testid="product-name"]');
            const titleColor = await heroTitle.evaluate((el) => {
                return window.getComputedStyle(el).color;
            });

            // Dark mode text should be light (#f1f5f9 = rgb(241, 245, 249))
            expect(titleColor).toBe('rgb(241, 245, 249)');
        });

        test('should update feature cards background in dark mode', async ({ page }) => {
            await page.emulateMedia({ colorScheme: 'dark' });
            await page.goto('/');

            const featureCard = page.locator('[data-testid="feature-card-memcached"]');
            const cardBgColor = await featureCard.evaluate((el) => {
                return window.getComputedStyle(el).backgroundColor;
            });

            // Dark mode surface color should be dark (#1e293b = rgb(30, 41, 59))
            expect(cardBgColor).toBe('rgb(30, 41, 59)');
        });
    });

    test.describe('Test Case 2: WCAG AA contrast requirements in dark mode', () => {
        test('should maintain sufficient contrast for body text in dark mode', async ({ page }) => {
            await page.emulateMedia({ colorScheme: 'dark' });
            await page.goto('/');

            // Get text color and background color
            const description = page.locator('[data-testid="hero-description"]');
            const textColor = await description.evaluate((el) => {
                return window.getComputedStyle(el).color;
            });

            // Text light color in dark mode: #cbd5e1 = rgb(203, 213, 225)
            // This provides good contrast against dark backgrounds
            expect(textColor).toBe('rgb(203, 213, 225)');
        });

        test('should maintain sufficient contrast for heading text in dark mode', async ({ page }) => {
            await page.emulateMedia({ colorScheme: 'dark' });
            await page.goto('/');

            const heading = page.locator('#features-title');
            const textColor = await heading.evaluate((el) => {
                return window.getComputedStyle(el).color;
            });

            // Main text in dark mode: #f1f5f9 = rgb(241, 245, 249)
            expect(textColor).toBe('rgb(241, 245, 249)');
        });

        test('should maintain sufficient contrast for navigation links in dark mode', async ({ page }) => {
            await page.emulateMedia({ colorScheme: 'dark' });
            await page.goto('/');

            const navLink = page.locator('.nav-links a').first();
            const linkColor = await navLink.evaluate((el) => {
                return window.getComputedStyle(el).color;
            });

            // Text light color: #cbd5e1 = rgb(203, 213, 225)
            expect(linkColor).toBe('rgb(203, 213, 225)');
        });

        test('should have visible focus indicators in dark mode', async ({ page }) => {
            await page.emulateMedia({ colorScheme: 'dark' });
            await page.goto('/');

            // Focus on a link
            const link = page.locator('.nav-links a').first();
            await link.focus();

            // Check that focus outline is visible (using primary color)
            const outlineColor = await link.evaluate((el) => {
                return window.getComputedStyle(el).outlineColor;
            });

            // Should have a visible outline color (primary blue in dark mode #60a5fa = rgb(96, 165, 250))
            expect(outlineColor).toBe('rgb(96, 165, 250)');
        });

        test('should maintain contrast for CTA buttons in dark mode', async ({ page }) => {
            await page.emulateMedia({ colorScheme: 'dark' });
            await page.goto('/');

            const primaryBtn = page.locator('[data-testid="primary-cta"]');
            const btnBgColor = await primaryBtn.evaluate((el) => {
                return window.getComputedStyle(el).backgroundColor;
            });
            const btnTextColor = await primaryBtn.evaluate((el) => {
                return window.getComputedStyle(el).color;
            });

            // Primary button should have good contrast
            // Background: #60a5fa = rgb(96, 165, 250)
            expect(btnBgColor).toBe('rgb(96, 165, 250)');
            // Text: white = rgb(255, 255, 255)
            expect(btnTextColor).toBe('rgb(255, 255, 255)');
        });
    });

    test.describe('Test Case 3: Code snippets visibility in dark mode', () => {
        test('should display code blocks with dark background in dark mode', async ({ page }) => {
            await page.emulateMedia({ colorScheme: 'dark' });
            await page.goto('/');

            // Scroll to quick start section
            await page.locator('#quick-start').scrollIntoViewIfNeeded();

            const codeBlock = page.locator('[data-testid="installation-code"]');
            const codeBgColor = await codeBlock.evaluate((el) => {
                return window.getComputedStyle(el).backgroundColor;
            });

            // Code block should have a dark background
            // The Prism.js tomorrow theme may override with #2d2d2d = rgb(45, 45, 45)
            // Or our custom dark background: #0f172a = rgb(15, 23, 42)
            // Accept either dark background - the key is it's dark (low luminosity)
            const bgMatch = codeBgColor.match(/rgb\((\d+), (\d+), (\d+)\)/);
            expect(bgMatch).toBeTruthy();
            const [, r, g, b] = bgMatch.map(Number);
            // Verify it's a dark color (average RGB < 100)
            const avgBrightness = (r + g + b) / 3;
            expect(avgBrightness).toBeLessThan(100);
        });

        test('should have visible code text in dark mode', async ({ page }) => {
            await page.emulateMedia({ colorScheme: 'dark' });
            await page.goto('/');

            await page.locator('#quick-start').scrollIntoViewIfNeeded();

            const codeElement = page.locator('[data-testid="installation-code"] code');
            const codeTextColor = await codeElement.evaluate((el) => {
                return window.getComputedStyle(el).color;
            });

            // Code text should be light colored for readability
            // Prism.js tomorrow theme uses #ccc = rgb(204, 204, 204) or similar
            // Our custom: #e2e8f0 = rgb(226, 232, 240)
            const textMatch = codeTextColor.match(/rgb\((\d+), (\d+), (\d+)\)/);
            expect(textMatch).toBeTruthy();
            const [, r, g, b] = textMatch.map(Number);
            // Verify it's a light color (average RGB > 150 for good contrast)
            const avgBrightness = (r + g + b) / 3;
            expect(avgBrightness).toBeGreaterThan(150);
        });

        test('should have border on code blocks in dark mode', async ({ page }) => {
            await page.emulateMedia({ colorScheme: 'dark' });
            await page.goto('/');

            await page.locator('#quick-start').scrollIntoViewIfNeeded();

            const codeBlock = page.locator('[data-testid="installation-code"]');
            const borderColor = await codeBlock.evaluate((el) => {
                return window.getComputedStyle(el).borderColor;
            });

            // Border should be visible in dark mode (#334155 = rgb(51, 65, 85))
            expect(borderColor).toBe('rgb(51, 65, 85)');
        });

        test('should render syntax highlighting colors in dark mode', async ({ page }) => {
            await page.emulateMedia({ colorScheme: 'dark' });
            await page.goto('/');

            await page.locator('#quick-start').scrollIntoViewIfNeeded();

            // Wait for Prism.js to load and apply highlighting
            await page.waitForTimeout(500);

            // Check that code block has syntax highlighting applied
            const codeBlock = page.locator('[data-testid="installation-code"] code');
            const hasLanguageClass = await codeBlock.evaluate((el) => {
                return el.classList.contains('language-bash');
            });

            expect(hasLanguageClass).toBe(true);
        });
    });

    test.describe('Test Case 4: Smooth transitions between color schemes', () => {
        test('should have transition property set on body', async ({ page }) => {
            await page.goto('/');

            const body = page.locator('body');
            const transition = await body.evaluate((el) => {
                return window.getComputedStyle(el).transition;
            });

            // Should include background-color transition
            expect(transition).toContain('background-color');
        });

        test('should have transition property set on header', async ({ page }) => {
            await page.goto('/');

            const header = page.locator('.header');
            const transition = await header.evaluate((el) => {
                return window.getComputedStyle(el).transition;
            });

            // Should include background-color transition
            expect(transition).toContain('background-color');
        });

        test('should have transition property set on feature cards', async ({ page }) => {
            await page.goto('/');

            const card = page.locator('.feature-card').first();
            const transition = await card.evaluate((el) => {
                return window.getComputedStyle(el).transition;
            });

            // Should have transition for smooth theme switching
            expect(transition).toContain('0.3s');
        });

        test('should apply smooth transition when switching from light to dark mode', async ({ page }) => {
            // Start in light mode
            await page.emulateMedia({ colorScheme: 'light' });
            await page.goto('/');

            // Verify initial light mode
            let bgColor = await page.locator('body').evaluate((el) => {
                return window.getComputedStyle(el).backgroundColor;
            });
            expect(bgColor).toBe('rgb(255, 255, 255)');

            // Switch to dark mode
            await page.emulateMedia({ colorScheme: 'dark' });

            // Wait for transition
            await page.waitForTimeout(350);

            // Verify dark mode is applied
            bgColor = await page.locator('body').evaluate((el) => {
                return window.getComputedStyle(el).backgroundColor;
            });
            expect(bgColor).toBe('rgb(15, 23, 42)');
        });

        test('should apply smooth transition when switching from dark to light mode', async ({ page }) => {
            // Start in dark mode
            await page.emulateMedia({ colorScheme: 'dark' });
            await page.goto('/');

            // Verify initial dark mode
            let bgColor = await page.locator('body').evaluate((el) => {
                return window.getComputedStyle(el).backgroundColor;
            });
            expect(bgColor).toBe('rgb(15, 23, 42)');

            // Switch to light mode
            await page.emulateMedia({ colorScheme: 'light' });

            // Wait for transition
            await page.waitForTimeout(350);

            // Verify light mode is applied
            bgColor = await page.locator('body').evaluate((el) => {
                return window.getComputedStyle(el).backgroundColor;
            });
            expect(bgColor).toBe('rgb(255, 255, 255)');
        });
    });

    test.describe('Additional Dark Mode Tests', () => {
        test('should update footer colors in dark mode', async ({ page }) => {
            await page.emulateMedia({ colorScheme: 'dark' });
            await page.goto('/');

            const footer = page.locator('.footer');
            const footerBgColor = await footer.evaluate((el) => {
                return window.getComputedStyle(el).backgroundColor;
            });

            // Footer background in dark mode: #020617 = rgb(2, 6, 23)
            expect(footerBgColor).toBe('rgb(2, 6, 23)');
        });

        test('should update architecture section background in dark mode', async ({ page }) => {
            await page.emulateMedia({ colorScheme: 'dark' });
            await page.goto('/');

            const architecture = page.locator('.architecture');
            const bgColor = await architecture.evaluate((el) => {
                return window.getComputedStyle(el).backgroundColor;
            });

            // Architecture section uses surface color in dark mode: #1e293b = rgb(30, 41, 59)
            expect(bgColor).toBe('rgb(30, 41, 59)');
        });

        test('should update architecture diagram background in dark mode', async ({ page }) => {
            await page.emulateMedia({ colorScheme: 'dark' });
            await page.goto('/');

            const diagramContainer = page.locator('.architecture-diagram');
            const bgColor = await diagramContainer.evaluate((el) => {
                return window.getComputedStyle(el).backgroundColor;
            });

            // Diagram container uses background color: #0f172a = rgb(15, 23, 42)
            expect(bgColor).toBe('rgb(15, 23, 42)');
        });

        test('should update primary color (links/accents) in dark mode', async ({ page }) => {
            await page.emulateMedia({ colorScheme: 'dark' });
            await page.goto('/');

            const tagline = page.locator('[data-testid="hero-tagline"]');
            const taglineColor = await tagline.evaluate((el) => {
                return window.getComputedStyle(el).color;
            });

            // Primary color in dark mode: #60a5fa = rgb(96, 165, 250)
            expect(taglineColor).toBe('rgb(96, 165, 250)');
        });

        test('should update border colors in dark mode', async ({ page }) => {
            await page.emulateMedia({ colorScheme: 'dark' });
            await page.goto('/');

            const header = page.locator('.header');
            const borderColor = await header.evaluate((el) => {
                return window.getComputedStyle(el).borderBottomColor;
            });

            // Border color in dark mode: #334155 = rgb(51, 65, 85)
            expect(borderColor).toBe('rgb(51, 65, 85)');
        });

        test('respects prefers-reduced-motion in dark mode', async ({ page }) => {
            await page.emulateMedia({ colorScheme: 'dark', reducedMotion: 'reduce' });
            await page.goto('/');

            // Verify that the reduced motion media query rule exists in the stylesheet
            // by checking if the CSS contains the prefers-reduced-motion rule
            const hasReducedMotionRule = await page.evaluate(() => {
                const stylesheets = Array.from(document.styleSheets);
                for (const sheet of stylesheets) {
                    try {
                        const rules = Array.from(sheet.cssRules || []);
                        for (const rule of rules) {
                            if (rule instanceof CSSMediaRule &&
                                rule.conditionText &&
                                rule.conditionText.includes('prefers-reduced-motion')) {
                                return true;
                            }
                        }
                    } catch (e) {
                        // Cross-origin stylesheet, skip
                    }
                }
                return false;
            });

            // The page should have the prefers-reduced-motion CSS rule defined
            expect(hasReducedMotionRule).toBe(true);
        });
    });
});
