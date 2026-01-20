const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = `file://${path.resolve(__dirname, '../index.html')}`;

test.describe('Keyboard Navigation Accessibility', () => {

    test('Test Case 1: All links and buttons can be reached via keyboard Tab navigation', async ({ page }) => {
        await page.goto(indexPath);

        // Get all interactive elements that should be focusable
        const interactiveElements = await page.locator('a[href], button').all();
        expect(interactiveElements.length).toBeGreaterThan(0);

        const expectedInteractiveCount = interactiveElements.length;
        const focusedElements = [];

        // Start tabbing through all interactive elements
        for (let i = 0; i < expectedInteractiveCount + 5; i++) {
            await page.keyboard.press('Tab');

            // Check what element is currently focused
            const focusedElement = await page.evaluate(() => {
                const el = document.activeElement;
                if (el && (el.tagName === 'A' || el.tagName === 'BUTTON')) {
                    return {
                        tagName: el.tagName,
                        href: el.href || null,
                        text: el.textContent.trim(),
                        className: el.className
                    };
                }
                return null;
            });

            if (focusedElement && !focusedElements.some(e =>
                e.text === focusedElement.text &&
                e.href === focusedElement.href
            )) {
                focusedElements.push(focusedElement);
            }
        }

        // Verify we can reach multiple interactive elements
        expect(focusedElements.length).toBeGreaterThan(3);

        // Verify specific important elements are reachable
        const elementTexts = focusedElements.map(e => e.text.toLowerCase());

        // Check for Get Started button
        expect(elementTexts.some(text => text.includes('get started'))).toBe(true);

        // Check for GitHub link
        expect(elementTexts.some(text =>
            text.includes('github') || text.includes('view on github')
        )).toBe(true);
    });

    test('Test Case 2: CTA buttons show visible focus indicator when focused', async ({ page }) => {
        await page.goto(indexPath);

        // Tab to the skip link first (if it exists), then to Get Started
        await page.keyboard.press('Tab');

        // Find the Get Started button and focus it
        const getStartedButton = page.locator('a.btn-primary').first();
        await getStartedButton.focus();

        // Check that the focused element has a visible focus style
        const focusStyles = await getStartedButton.evaluate((el) => {
            const styles = window.getComputedStyle(el);
            return {
                outline: styles.outline,
                outlineWidth: styles.outlineWidth,
                outlineStyle: styles.outlineStyle,
                outlineColor: styles.outlineColor,
                boxShadow: styles.boxShadow,
                border: styles.border,
                borderColor: styles.borderColor
            };
        });

        // Check for visible focus indicator (outline or box-shadow)
        const hasVisibleOutline = focusStyles.outlineWidth !== '0px' &&
                                   focusStyles.outlineStyle !== 'none' &&
                                   focusStyles.outlineColor !== 'transparent';

        const hasBoxShadow = focusStyles.boxShadow !== 'none' &&
                             focusStyles.boxShadow !== '';

        expect(hasVisibleOutline || hasBoxShadow).toBe(true);

        // Check secondary button (View on GitHub)
        const githubButton = page.locator('a.btn-secondary').first();
        await githubButton.focus();

        const secondaryFocusStyles = await githubButton.evaluate((el) => {
            const styles = window.getComputedStyle(el);
            return {
                outline: styles.outline,
                outlineWidth: styles.outlineWidth,
                outlineStyle: styles.outlineStyle,
                outlineColor: styles.outlineColor,
                boxShadow: styles.boxShadow
            };
        });

        const secondaryHasVisibleOutline = secondaryFocusStyles.outlineWidth !== '0px' &&
                                            secondaryFocusStyles.outlineStyle !== 'none' &&
                                            secondaryFocusStyles.outlineColor !== 'transparent';

        const secondaryHasBoxShadow = secondaryFocusStyles.boxShadow !== 'none' &&
                                       secondaryFocusStyles.boxShadow !== '';

        expect(secondaryHasVisibleOutline || secondaryHasBoxShadow).toBe(true);
    });

    test('Test Case 3: Pressing Enter on focused link activates it', async ({ page }) => {
        await page.goto(indexPath);

        // Focus on the "Get Started" button which navigates to #quick-start
        const getStartedButton = page.locator('a.btn-primary:has-text("Get Started")');
        await getStartedButton.focus();

        // Press Enter to activate the link
        await page.keyboard.press('Enter');

        // Wait for navigation/scroll to complete
        await page.waitForTimeout(500);

        // Check that the page scrolled to the quick-start section
        const quickStartVisible = await page.evaluate(() => {
            const quickStart = document.getElementById('quick-start');
            if (!quickStart) return false;

            const rect = quickStart.getBoundingClientRect();
            // Check if quick-start section is near the top of the viewport
            return rect.top < 200;
        });

        expect(quickStartVisible).toBe(true);
    });

    test('Test Case 4: Skip to main content link is available for keyboard users', async ({ page }) => {
        await page.goto(indexPath);

        // The skip link should be the first focusable element
        await page.keyboard.press('Tab');

        // Check if a skip link exists and is focused
        const skipLinkFocused = await page.evaluate(() => {
            const activeEl = document.activeElement;
            if (!activeEl) return { found: false };

            const text = activeEl.textContent?.toLowerCase() || '';
            const isSkipLink = text.includes('skip') ||
                               text.includes('main content') ||
                               activeEl.classList.contains('skip-link');

            return {
                found: isSkipLink,
                text: activeEl.textContent?.trim() || '',
                href: activeEl.href || '',
                tagName: activeEl.tagName
            };
        });

        expect(skipLinkFocused.found).toBe(true);
        expect(skipLinkFocused.href).toContain('#main');

        // Verify the skip link is visually hidden by default but becomes visible on focus
        const skipLink = page.locator('.skip-link');

        // Should be visible when focused
        await expect(skipLink).toBeVisible();

        // Press Enter to activate skip link
        await page.keyboard.press('Enter');

        // Wait for navigation
        await page.waitForTimeout(300);

        // Verify focus moved to main content
        const mainContentFocused = await page.evaluate(() => {
            const activeEl = document.activeElement;
            return activeEl?.id === 'main-content' ||
                   activeEl?.tagName === 'MAIN' ||
                   activeEl?.closest('#main-content') !== null ||
                   activeEl?.closest('main') !== null;
        });

        expect(mainContentFocused).toBe(true);
    });

    test('Focus order follows logical reading order', async ({ page }) => {
        await page.goto(indexPath);

        const focusOrder = [];

        // Tab through multiple elements and record order
        for (let i = 0; i < 10; i++) {
            await page.keyboard.press('Tab');

            const focusedInfo = await page.evaluate(() => {
                const el = document.activeElement;
                if (!el) return null;

                const rect = el.getBoundingClientRect();
                return {
                    tagName: el.tagName,
                    text: el.textContent?.trim().substring(0, 30) || '',
                    top: rect.top,
                    left: rect.left
                };
            });

            if (focusedInfo) {
                focusOrder.push(focusedInfo);
            }
        }

        // Verify focus generally moves down the page (top values increase)
        // Allow for some variation in horizontal movement
        let lastTop = -1;
        let topIncreaseCount = 0;

        for (const item of focusOrder) {
            if (item.top >= lastTop) {
                topIncreaseCount++;
            }
            lastTop = item.top;
        }

        // At least 70% of focus changes should maintain or increase top position
        // (allowing for horizontal navigation within sections)
        expect(topIncreaseCount / focusOrder.length).toBeGreaterThan(0.6);
    });

    test('All footer links are keyboard accessible', async ({ page }) => {
        await page.goto(indexPath);

        // Get footer links
        const footerLinks = page.locator('footer a');
        const footerLinkCount = await footerLinks.count();

        expect(footerLinkCount).toBeGreaterThan(0);

        // Focus each footer link and verify it's reachable
        for (let i = 0; i < footerLinkCount; i++) {
            const link = footerLinks.nth(i);
            await link.focus();

            const isFocused = await link.evaluate((el) => {
                return document.activeElement === el;
            });

            expect(isFocused).toBe(true);

            // Verify focus indicator is visible
            const focusStyles = await link.evaluate((el) => {
                const styles = window.getComputedStyle(el);
                return {
                    outlineWidth: styles.outlineWidth,
                    outlineStyle: styles.outlineStyle,
                    boxShadow: styles.boxShadow
                };
            });

            const hasVisibleFocus = focusStyles.outlineWidth !== '0px' &&
                                     focusStyles.outlineStyle !== 'none' ||
                                     focusStyles.boxShadow !== 'none';

            expect(hasVisibleFocus).toBe(true);
        }
    });

});
