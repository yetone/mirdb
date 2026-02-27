// @ts-check
const { test, expect } = require('@playwright/test');
const AxeBuilder = require('@axe-core/playwright').default;

/**
 * Accessibility Compliance Tests (Scenario 10)
 *
 * Validates WCAG 2.1 AA compliance including:
 * - Color contrast (4.5:1 ratio minimum)
 * - Keyboard navigation
 * - Screen reader support
 * - Semantic HTML
 * - Focus visibility
 */

test.describe('Accessibility Compliance', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
        await page.waitForLoadState('networkidle');
    });

    // Test Case 1: Lighthouse accessibility audit (axe-core based)
    test('should achieve accessibility score >= 95 with axe-core', async ({ page }) => {
        const accessibilityScanResults = await new AxeBuilder({ page })
            .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
            .analyze();

        // Calculate pass rate
        const totalChecks = accessibilityScanResults.passes.length + accessibilityScanResults.violations.length;
        const passRate = (accessibilityScanResults.passes.length / totalChecks) * 100;

        // Log any violations for debugging
        if (accessibilityScanResults.violations.length > 0) {
            console.log('Accessibility violations:', JSON.stringify(accessibilityScanResults.violations, null, 2));
        }

        // Should have no critical violations
        const criticalViolations = accessibilityScanResults.violations.filter(
            v => v.impact === 'critical' || v.impact === 'serious'
        );

        expect(criticalViolations).toHaveLength(0);
        expect(passRate).toBeGreaterThanOrEqual(95);
    });

    // Test Case 2: Skip navigation link
    test('should have skip to main content link as first focusable element', async ({ page }) => {
        // Press Tab to focus first element
        await page.keyboard.press('Tab');

        // Get the focused element
        const focusedElement = page.locator(':focus');

        // Check if it's the skip link
        await expect(focusedElement).toHaveAttribute('href', '#main-content');
        await expect(focusedElement).toHaveText(/skip to main content/i);
        await expect(focusedElement).toHaveClass(/skip-link/);
    });

    // Test Case 3: Tab through all interactive elements
    test('should be able to tab through all interactive elements', async ({ page }) => {
        const interactiveElements = await page.locator('a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])').all();

        // Filter out hidden elements
        const visibleInteractiveElements = [];
        for (const el of interactiveElements) {
            if (await el.isVisible()) {
                visibleInteractiveElements.push(el);
            }
        }

        // Tab through elements and track which ones receive focus
        const focusedElements = new Set();
        let tabCount = 0;
        const maxTabs = visibleInteractiveElements.length + 10; // Extra tabs for safety

        while (tabCount < maxTabs) {
            await page.keyboard.press('Tab');
            tabCount++;

            const focusedEl = await page.evaluate(() => {
                const el = document.activeElement;
                if (el && el !== document.body) {
                    return el.tagName + (el.id ? '#' + el.id : '') + (el.className ? '.' + el.className.split(' ')[0] : '');
                }
                return null;
            });

            if (focusedEl) {
                focusedElements.add(focusedEl);
            }

            // Break if we've cycled back to the start
            if (tabCount > visibleInteractiveElements.length && focusedElements.size > 0) {
                break;
            }
        }

        // Should be able to reach multiple interactive elements via Tab
        expect(focusedElements.size).toBeGreaterThan(5);
    });

    // Test Case 4: Focus visibility on buttons
    test('should have visible focus outline on buttons', async ({ page }) => {
        // Verify focus styles are defined in CSS for buttons
        // Check that buttons have :focus or :focus-visible rules in the stylesheet
        const hasFocusStyles = await page.evaluate(() => {
            // Check all stylesheets for button focus rules
            for (const sheet of document.styleSheets) {
                try {
                    for (const rule of sheet.cssRules) {
                        const selector = rule.selectorText || '';
                        // Check for button focus rules
                        if ((selector.includes('button:focus') ||
                             selector.includes('.btn:focus') ||
                             selector.includes('.copy-btn:focus') ||
                             selector.includes(':focus-visible')) &&
                            rule.style) {
                            // Check if it has outline or box-shadow
                            const outline = rule.style.outline || rule.style.outlineWidth;
                            const boxShadow = rule.style.boxShadow;
                            if (outline || boxShadow) {
                                return true;
                            }
                        }
                    }
                } catch (e) {
                    // Skip cross-origin stylesheets
                }
            }
            return false;
        });

        expect(hasFocusStyles).toBeTruthy();

        // Additionally verify buttons are keyboard accessible (can receive focus)
        const buttons = await page.locator('button').all();
        let focusableButtons = 0;

        for (const button of buttons) {
            if (await button.isVisible()) {
                // Check if button is focusable (not disabled)
                const isDisabled = await button.getAttribute('disabled');
                if (!isDisabled) {
                    focusableButtons++;
                }
            }
        }

        // Should have at least one focusable button (copy buttons, theme toggle)
        expect(focusableButtons).toBeGreaterThan(0);
    });

    // Test Case 5: Focus visibility on links
    test('should have visible focus outline on links', async ({ page }) => {
        // Verify focus styles are defined in CSS for links
        const hasFocusStyles = await page.evaluate(() => {
            // Check all stylesheets for link focus rules
            for (const sheet of document.styleSheets) {
                try {
                    for (const rule of sheet.cssRules) {
                        const selector = rule.selectorText || '';
                        // Check for link focus rules
                        if ((selector.includes('a:focus') ||
                             selector.includes(':focus-visible')) &&
                            rule.style) {
                            // Check if it has outline or box-shadow
                            const outline = rule.style.outline || rule.style.outlineWidth;
                            const boxShadow = rule.style.boxShadow;
                            if (outline || boxShadow) {
                                return true;
                            }
                        }
                    }
                } catch (e) {
                    // Skip cross-origin stylesheets
                }
            }
            return false;
        });

        expect(hasFocusStyles).toBeTruthy();

        // Additionally verify links are keyboard accessible (can receive focus)
        const links = await page.locator('a[href]').all();
        let focusableLinks = 0;

        for (const link of links) {
            if (await link.isVisible()) {
                focusableLinks++;
            }
        }

        // Should have multiple focusable links
        expect(focusableLinks).toBeGreaterThan(5);
    });

    // Test Case 6: Text contrast ratio in light mode
    test('should have text contrast ratio >= 4.5:1 in light mode', async ({ page }) => {
        // Ensure light mode
        await page.evaluate(() => {
            document.documentElement.setAttribute('data-theme', 'light');
        });

        const accessibilityScanResults = await new AxeBuilder({ page })
            .withTags(['wcag2aa'])
            .include('body')
            .analyze();

        // Check for color contrast violations
        const contrastViolations = accessibilityScanResults.violations.filter(
            v => v.id === 'color-contrast'
        );

        expect(contrastViolations).toHaveLength(0);
    });

    // Test Case 7: Text contrast ratio in dark mode
    test('should have text contrast ratio >= 4.5:1 in dark mode', async ({ page }) => {
        // Switch to dark mode
        await page.evaluate(() => {
            document.documentElement.setAttribute('data-theme', 'dark');
        });

        // Wait for theme transition
        await page.waitForTimeout(400);

        const accessibilityScanResults = await new AxeBuilder({ page })
            .withTags(['wcag2aa'])
            .include('body')
            .analyze();

        // Check for color contrast violations
        const contrastViolations = accessibilityScanResults.violations.filter(
            v => v.id === 'color-contrast'
        );

        expect(contrastViolations).toHaveLength(0);
    });

    // Test Case 8: Heading hierarchy
    test('should have proper heading hierarchy with single h1 and no skipped levels', async ({ page }) => {
        // Check for single h1
        const h1Count = await page.locator('h1').count();
        expect(h1Count).toBe(1);

        // Get all headings in order
        const headings = await page.evaluate(() => {
            const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
            return Array.from(allHeadings).map(h => ({
                tag: h.tagName.toLowerCase(),
                level: parseInt(h.tagName.charAt(1)),
                text: h.textContent?.trim().substring(0, 50)
            }));
        });

        // Check for skipped levels
        let previousLevel = 0;
        for (const heading of headings) {
            // Should not skip more than one level
            expect(heading.level - previousLevel).toBeLessThanOrEqual(1);
            previousLevel = heading.level;
        }

        // Should have h2s for sections
        const h2Count = await page.locator('h2').count();
        expect(h2Count).toBeGreaterThanOrEqual(4); // Features, Quick Start, Architecture, Benchmarks
    });

    // Test Case 9: Semantic landmark elements
    test('should have semantic landmark elements (header, main, footer, nav)', async ({ page }) => {
        // Check for header element
        const header = page.locator('header');
        await expect(header).toHaveCount(1);

        // Check for main element
        const main = page.locator('main');
        await expect(main).toHaveCount(1);
        await expect(main).toHaveAttribute('id', 'main-content');

        // Check for footer element
        const footer = page.locator('footer');
        await expect(footer).toHaveCount(1);

        // Check for nav element
        const nav = page.locator('nav');
        const navCount = await nav.count();
        expect(navCount).toBeGreaterThanOrEqual(1);

        // Check that nav has aria-label
        const mainNav = page.locator('nav[aria-label]');
        const mainNavCount = await mainNav.count();
        expect(mainNavCount).toBeGreaterThanOrEqual(1);
    });

    // Test Case 10: Images have alt text
    test('should have alt attribute on all images', async ({ page }) => {
        const images = await page.locator('img').all();

        for (const img of images) {
            // All images should have alt attribute (can be empty for decorative)
            const altAttr = await img.getAttribute('alt');
            expect(altAttr).not.toBeNull();
        }

        // Check SVGs used as images
        const svgsWithRole = await page.locator('svg[role="img"]').all();
        for (const svg of svgsWithRole) {
            // SVGs with role="img" should have aria-label or title
            const ariaLabel = await svg.getAttribute('aria-label');
            const title = await svg.locator('title').count();

            expect(ariaLabel !== null || title > 0).toBeTruthy();
        }
    });

    // Test Case 11: Buttons have accessible names
    test('should have accessible names on all buttons', async ({ page }) => {
        const buttons = await page.locator('button').all();

        for (const button of buttons) {
            // Get accessible name
            const accessibleName = await button.evaluate((el) => {
                // Check for aria-label
                if (el.hasAttribute('aria-label')) {
                    return el.getAttribute('aria-label');
                }
                // Check for aria-labelledby
                if (el.hasAttribute('aria-labelledby')) {
                    const labelId = el.getAttribute('aria-labelledby');
                    const labelEl = document.getElementById(labelId || '');
                    if (labelEl) return labelEl.textContent;
                }
                // Check for text content
                return el.textContent?.trim() || '';
            });

            // Should have some accessible name
            expect(accessibleName?.length).toBeGreaterThan(0);
        }
    });

    // Test Case 12: Links have accessible names
    test('should have accessible names on all links', async ({ page }) => {
        const links = await page.locator('a[href]').all();

        for (const link of links) {
            // Skip skip-link when it's visually hidden
            const isSkipLink = await link.getAttribute('class');
            if (isSkipLink?.includes('skip-link')) continue;

            // Get accessible name
            const accessibleName = await link.evaluate((el) => {
                // Check for aria-label
                if (el.hasAttribute('aria-label')) {
                    return el.getAttribute('aria-label');
                }
                // Check for aria-labelledby
                if (el.hasAttribute('aria-labelledby')) {
                    const labelId = el.getAttribute('aria-labelledby');
                    const labelEl = document.getElementById(labelId || '');
                    if (labelEl) return labelEl.textContent;
                }
                // Check for text content (including img alt text)
                const text = el.textContent?.trim() || '';
                if (text) return text;

                // Check for img with alt inside link
                const img = el.querySelector('img');
                if (img) return img.alt;

                return '';
            });

            // Should have some accessible name
            expect(accessibleName?.length, `Link ${await link.getAttribute('href')} should have accessible name`).toBeGreaterThan(0);
        }
    });

    // Test Case 13: Manual test (screen reader) - skipped for automation
    test.skip('should announce content correctly with screen reader', async () => {
        // This is a manual test case
        // Instructions: Navigate through the page with NVDA/VoiceOver
        // Verify all content is announced correctly in logical order
    });

    // Test Case 14: Color is not sole indicator
    test('should use icons or text in addition to color for status/state', async ({ page }) => {
        // Check copy buttons - they should have text label not just color
        const copyButtons = page.locator('.copy-btn');
        const copyButtonCount = await copyButtons.count();

        for (let i = 0; i < copyButtonCount; i++) {
            const button = copyButtons.nth(i);

            // Should have text label OR aria-label
            const textContent = await button.textContent();
            const ariaLabel = await button.getAttribute('aria-label');

            expect(textContent?.trim() || ariaLabel).toBeTruthy();
        }

        // Check theme toggle - should have aria-label describing current state
        const themeToggle = page.locator('.theme-toggle');
        if (await themeToggle.count() > 0) {
            const ariaLabel = await themeToggle.getAttribute('aria-label');
            expect(ariaLabel).toBeTruthy();
        }

        // Check that MirDB highlight row uses more than just color
        const highlightRow = page.locator('.benchmark-row-highlight');
        if (await highlightRow.count() > 0) {
            // The row should still be identifiable without color (it has th scope="row")
            const th = highlightRow.locator('th[scope="row"]');
            await expect(th).toHaveCount(1);
        }
    });

    // Additional: Verify axe-core passes with no violations
    test('should pass axe-core accessibility audit with no serious violations', async ({ page }) => {
        const accessibilityScanResults = await new AxeBuilder({ page }).analyze();

        // Filter to only serious and critical
        const seriousViolations = accessibilityScanResults.violations.filter(
            v => v.impact === 'critical' || v.impact === 'serious'
        );

        if (seriousViolations.length > 0) {
            console.log('Serious violations:', JSON.stringify(seriousViolations, null, 2));
        }

        expect(seriousViolations).toHaveLength(0);
    });

    // Additional: ARIA attributes are valid
    test('should have valid ARIA attributes', async ({ page }) => {
        const accessibilityScanResults = await new AxeBuilder({ page })
            .withTags(['cat.aria'])
            .analyze();

        const ariaViolations = accessibilityScanResults.violations;

        expect(ariaViolations).toHaveLength(0);
    });
});
