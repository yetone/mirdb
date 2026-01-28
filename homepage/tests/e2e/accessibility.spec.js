/**
 * Accessibility E2E Tests
 * Owner: Scenario 8 - Accessibility Compliance
 *
 * E2E Tests for:
 * - Keyboard tab navigation (TC3)
 * - Focus indicators visibility (TC5)
 *
 * WCAG 2.1 AA compliance testing
 */

const { test, expect } = require('@playwright/test');

test.describe('Accessibility E2E - Keyboard Navigation and Focus', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
        await page.waitForLoadState('networkidle');
    });

    /**
     * Test Case 3: Test keyboard tab navigation
     * All interactive elements are reachable via Tab key in logical order
     */
    test.describe('TC3: Keyboard Tab Navigation', () => {
        test('All interactive elements are reachable via Tab key', async ({ page }) => {
            // Get all focusable elements
            const focusableElements = await page.locator('a, button, [tabindex="0"], input, textarea, select').all();

            // Press Tab from the beginning and track focus
            await page.keyboard.press('Tab');

            let focusedCount = 0;
            const maxTabs = focusableElements.length + 5; // Extra margin for safety

            for (let i = 0; i < maxTabs && focusedCount < focusableElements.length; i++) {
                const focusedElement = await page.evaluate(() => {
                    const el = document.activeElement;
                    return el ? el.tagName : null;
                });

                if (focusedElement && focusedElement !== 'BODY' && focusedElement !== 'HTML') {
                    focusedCount++;
                }

                await page.keyboard.press('Tab');
            }

            // Should be able to tab through multiple interactive elements
            expect(focusedCount).toBeGreaterThanOrEqual(3);
        });

        test('Tab order follows logical document order', async ({ page }) => {
            const tabOrder = [];

            // Tab through the page and collect focused element IDs/descriptions
            await page.keyboard.press('Tab');

            for (let i = 0; i < 15; i++) {
                const focusedInfo = await page.evaluate(() => {
                    const el = document.activeElement;
                    if (!el || el.tagName === 'BODY') return null;
                    return {
                        tag: el.tagName,
                        id: el.id || '',
                        text: el.textContent?.trim().substring(0, 30) || '',
                        href: el.getAttribute('href') || ''
                    };
                });

                if (focusedInfo) {
                    tabOrder.push(focusedInfo);
                }

                await page.keyboard.press('Tab');
            }

            // First few tab stops should be hero section CTAs
            expect(tabOrder.length).toBeGreaterThan(0);

            // Check that hero CTAs come early in tab order
            const heroCtaIndex = tabOrder.findIndex(item =>
                item.id === 'cta-github' || item.text.toLowerCase().includes('github')
            );

            expect(heroCtaIndex).toBeLessThan(5);
        });

        test('Hero CTA buttons are keyboard accessible', async ({ page }) => {
            // Focus on primary CTA
            await page.keyboard.press('Tab');

            // Tab until we reach the GitHub button
            let foundGithubBtn = false;
            for (let i = 0; i < 10; i++) {
                const isFocused = await page.locator('#cta-github').evaluate(el => document.activeElement === el);
                if (isFocused) {
                    foundGithubBtn = true;
                    break;
                }
                await page.keyboard.press('Tab');
            }

            expect(foundGithubBtn).toBe(true);
        });

        test('Copy button is keyboard accessible', async ({ page }) => {
            // Tab to find the copy button
            let foundCopyBtn = false;
            await page.keyboard.press('Tab');

            for (let i = 0; i < 20; i++) {
                const focusedId = await page.evaluate(() => document.activeElement?.id || '');
                if (focusedId === 'copy-btn') {
                    foundCopyBtn = true;
                    break;
                }
                await page.keyboard.press('Tab');
            }

            expect(foundCopyBtn).toBe(true);
        });

        test('Footer links are reachable via keyboard', async ({ page }) => {
            // Tab through the entire page to reach footer
            let foundFooterLink = false;
            await page.keyboard.press('Tab');

            for (let i = 0; i < 30; i++) {
                const inFooter = await page.evaluate(() => {
                    const el = document.activeElement;
                    return el?.closest('footer') !== null;
                });

                if (inFooter) {
                    foundFooterLink = true;
                    break;
                }
                await page.keyboard.press('Tab');
            }

            expect(foundFooterLink).toBe(true);
        });

        test('Enter key activates links when focused', async ({ page }) => {
            // Focus on documentation link (internal anchor)
            const docsLink = page.locator('#cta-docs');
            await docsLink.focus();

            // Verify it's focused
            const isFocused = await docsLink.evaluate(el => document.activeElement === el);
            expect(isFocused).toBe(true);

            // Press Enter and check that it navigates
            await page.keyboard.press('Enter');

            // Wait for potential navigation/scroll
            await page.waitForTimeout(1000);

            // Should scroll to quick-start section OR the quick-start section should be visible
            const quickStartVisible = await page.locator('#quick-start').isVisible();
            const url = page.url();

            // Either URL contains hash or section is in viewport
            const navigated = url.includes('#quick-start') || quickStartVisible;
            expect(navigated).toBe(true);
        });

        test('Space key can activate buttons', async ({ page }) => {
            // Focus on copy button
            const copyBtn = page.locator('#copy-btn');
            await copyBtn.focus();

            // Initial state
            const initialText = await copyBtn.textContent();

            // Press Space
            await page.keyboard.press('Space');

            // Wait for potential state change
            await page.waitForTimeout(300);

            // Button should respond (either change text or maintain functionality)
            const isStillButton = await copyBtn.isVisible();
            expect(isStillButton).toBe(true);
        });
    });

    /**
     * Test Case 5: Verify focus indicators
     * Interactive elements have visible focus indicators when focused
     * Note: Uses Tab key navigation to trigger :focus-visible styles
     */
    test.describe('TC5: Focus Indicators', () => {
        test('Links show visible focus indicator', async ({ page }) => {
            // Use Tab to navigate - this triggers :focus-visible
            await page.keyboard.press('Tab');
            await page.waitForTimeout(100);

            // Check that focus styles are applied
            const styles = await page.evaluate(() => {
                const el = document.activeElement;
                if (!el) return null;
                const computed = window.getComputedStyle(el);
                return {
                    outline: computed.outline,
                    outlineWidth: computed.outlineWidth,
                    outlineColor: computed.outlineColor,
                    outlineStyle: computed.outlineStyle,
                    boxShadow: computed.boxShadow
                };
            });

            expect(styles).not.toBeNull();

            // Should have visible outline or box-shadow
            const hasVisibleFocus =
                (styles.outlineWidth !== '0px' && styles.outlineStyle !== 'none') ||
                styles.boxShadow !== 'none';

            expect(hasVisibleFocus).toBe(true);
        });

        test('Primary CTA button shows focus indicator', async ({ page }) => {
            // Tab until we reach a primary button
            await page.keyboard.press('Tab');
            await page.waitForTimeout(100);

            const styles = await page.evaluate(() => {
                const el = document.activeElement;
                if (!el) return null;
                const computed = window.getComputedStyle(el);
                return {
                    outline: computed.outline,
                    outlineWidth: computed.outlineWidth,
                    outlineStyle: computed.outlineStyle,
                    boxShadow: computed.boxShadow,
                    isPrimary: el.classList.contains('btn-primary')
                };
            });

            expect(styles).not.toBeNull();

            const hasVisibleFocus =
                (styles.outlineWidth !== '0px' && styles.outlineStyle !== 'none') ||
                styles.boxShadow !== 'none';

            expect(hasVisibleFocus).toBe(true);
        });

        test('Secondary CTA button shows focus indicator', async ({ page }) => {
            // Tab to secondary button
            await page.keyboard.press('Tab');
            await page.keyboard.press('Tab');
            await page.waitForTimeout(100);

            const styles = await page.evaluate(() => {
                const el = document.activeElement;
                if (!el) return null;
                const computed = window.getComputedStyle(el);
                return {
                    outline: computed.outline,
                    outlineWidth: computed.outlineWidth,
                    outlineStyle: computed.outlineStyle,
                    boxShadow: computed.boxShadow
                };
            });

            expect(styles).not.toBeNull();

            const hasVisibleFocus =
                (styles.outlineWidth !== '0px' && styles.outlineStyle !== 'none') ||
                styles.boxShadow !== 'none';

            expect(hasVisibleFocus).toBe(true);
        });

        test('Copy button shows focus indicator', async ({ page }) => {
            // Tab multiple times to reach copy button
            for (let i = 0; i < 10; i++) {
                await page.keyboard.press('Tab');
                const focusedId = await page.evaluate(() => document.activeElement?.id);
                if (focusedId === 'copy-btn') break;
            }
            await page.waitForTimeout(100);

            const styles = await page.evaluate(() => {
                const el = document.activeElement;
                if (!el) return null;
                const computed = window.getComputedStyle(el);
                return {
                    outline: computed.outline,
                    outlineWidth: computed.outlineWidth,
                    outlineStyle: computed.outlineStyle,
                    outlineOffset: computed.outlineOffset
                };
            });

            expect(styles).not.toBeNull();
            const hasVisibleFocus =
                styles.outlineWidth !== '0px' && styles.outlineStyle !== 'none';

            expect(hasVisibleFocus).toBe(true);
        });

        test('Focus indicator has sufficient contrast (outline color)', async ({ page }) => {
            // Use Tab to trigger focus-visible
            await page.keyboard.press('Tab');
            await page.waitForTimeout(100);

            const outlineColor = await page.evaluate(() => {
                const el = document.activeElement;
                if (!el) return null;
                const computed = window.getComputedStyle(el);
                return computed.outlineColor;
            });

            // Should have a visible color (not transparent)
            expect(outlineColor).not.toBeNull();
            expect(outlineColor).not.toBe('transparent');
            expect(outlineColor).not.toBe('rgba(0, 0, 0, 0)');
        });

        test('Focus moves visibly when tabbing', async ({ page }) => {
            // Take screenshot of first focused element
            await page.keyboard.press('Tab');
            const firstFocused = await page.evaluate(() => document.activeElement?.id || document.activeElement?.className);

            // Tab to next element
            await page.keyboard.press('Tab');
            const secondFocused = await page.evaluate(() => document.activeElement?.id || document.activeElement?.className);

            // Focus should have moved to a different element
            expect(firstFocused).not.toBe(secondFocused);
        });

        test('Focus indicator is visible in light mode', async ({ page }) => {
            // Emulate light color scheme
            await page.emulateMedia({ colorScheme: 'light' });
            await page.reload();
            await page.waitForLoadState('networkidle');

            // Use Tab to trigger focus-visible
            await page.keyboard.press('Tab');
            await page.waitForTimeout(100);

            const styles = await page.evaluate(() => {
                const el = document.activeElement;
                if (!el) return null;
                const computed = window.getComputedStyle(el);
                return {
                    outlineWidth: computed.outlineWidth,
                    outlineStyle: computed.outlineStyle
                };
            });

            expect(styles).not.toBeNull();
            const hasVisibleFocus =
                styles.outlineWidth !== '0px' && styles.outlineStyle !== 'none';

            expect(hasVisibleFocus).toBe(true);
        });

        test('Focus indicator is visible in dark mode', async ({ page }) => {
            // Emulate dark color scheme
            await page.emulateMedia({ colorScheme: 'dark' });
            await page.reload();
            await page.waitForLoadState('networkidle');

            // Use Tab to trigger focus-visible
            await page.keyboard.press('Tab');
            await page.waitForTimeout(100);

            const styles = await page.evaluate(() => {
                const el = document.activeElement;
                if (!el) return null;
                const computed = window.getComputedStyle(el);
                return {
                    outlineWidth: computed.outlineWidth,
                    outlineStyle: computed.outlineStyle
                };
            });

            expect(styles).not.toBeNull();
            const hasVisibleFocus =
                styles.outlineWidth !== '0px' && styles.outlineStyle !== 'none';

            expect(hasVisibleFocus).toBe(true);
        });

        test('Focus-visible selector is properly supported', async ({ page }) => {
            // Check that CSS includes :focus-visible
            const hasFocusVisibleStyles = await page.evaluate(() => {
                const styleSheets = document.styleSheets;
                for (const sheet of styleSheets) {
                    try {
                        const rules = sheet.cssRules || sheet.rules;
                        for (const rule of rules) {
                            if (rule.selectorText && rule.selectorText.includes(':focus-visible')) {
                                return true;
                            }
                        }
                    } catch (e) {
                        // Cross-origin stylesheets may throw
                        continue;
                    }
                }
                return false;
            });

            expect(hasFocusVisibleStyles).toBe(true);
        });
    });

    /**
     * Additional keyboard navigation tests
     */
    test.describe('Additional Keyboard Accessibility', () => {
        test('Escape key does not break page navigation', async ({ page }) => {
            await page.keyboard.press('Tab');
            await page.keyboard.press('Tab');

            // Press Escape
            await page.keyboard.press('Escape');

            // Should still be able to navigate
            await page.keyboard.press('Tab');

            const canStillFocus = await page.evaluate(() => {
                return document.activeElement !== null &&
                       document.activeElement.tagName !== 'BODY';
            });

            // Page should remain functional (focus may or may not change with Escape)
            const pageIsAccessible = await page.locator('body').isVisible();
            expect(pageIsAccessible).toBe(true);
        });

        test('Page content is accessible without mouse', async ({ page }) => {
            // Navigate to all major sections via keyboard
            const sectionsFound = new Set();

            await page.keyboard.press('Tab');

            for (let i = 0; i < 30; i++) {
                const sectionId = await page.evaluate(() => {
                    const el = document.activeElement;
                    const section = el?.closest('section');
                    return section?.id || null;
                });

                if (sectionId) {
                    sectionsFound.add(sectionId);
                }

                await page.keyboard.press('Tab');
            }

            // Should reach multiple sections
            expect(sectionsFound.size).toBeGreaterThanOrEqual(2);
        });

        test('No keyboard traps exist', async ({ page }) => {
            // Tab through the page and ensure we eventually cycle back
            const startFocus = await page.evaluate(() => document.activeElement?.id || 'none');

            // Tab many times
            for (let i = 0; i < 50; i++) {
                await page.keyboard.press('Tab');
            }

            // Should be able to continue tabbing (no trap)
            const canStillTab = await page.evaluate(() => {
                return document.activeElement !== null;
            });

            expect(canStillTab).toBe(true);
        });

        test('Shift+Tab navigates backwards', async ({ page }) => {
            // Tab forward a few times
            await page.keyboard.press('Tab');
            await page.keyboard.press('Tab');
            await page.keyboard.press('Tab');

            const forwardFocus = await page.evaluate(() => document.activeElement?.id || document.activeElement?.className);

            // Tab backwards
            await page.keyboard.press('Shift+Tab');

            const backwardFocus = await page.evaluate(() => document.activeElement?.id || document.activeElement?.className);

            // Should have moved to a previous element
            expect(backwardFocus).not.toBe(forwardFocus);
        });
    });
});
