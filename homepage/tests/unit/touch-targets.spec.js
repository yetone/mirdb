// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Touch Target Size Tests
 * Verifies accessibility requirement for mobile touch interactions
 * All interactive elements should have minimum 44x44px touch target (WCAG 2.5.5)
 *
 * Related to REQ-8 and US-5
 */

test.describe('Touch Target Sizes', () => {
    // Use mobile viewport for touch target tests
    test.use({ viewport: { width: 375, height: 667 } });

    test.beforeEach(async ({ page }) => {
        await page.goto('/');
        await page.waitForLoadState('networkidle');
    });

    test('TC3: All interactive elements have minimum 44x44px touch target', async ({ page }) => {
        // WCAG 2.5.5 Level AAA recommends 44x44px minimum touch target
        const MIN_TOUCH_TARGET = 44;

        // Collect all interactive elements to test
        const interactiveSelectors = [
            'a',           // Links
            'button',      // Buttons
            '[role="button"]',
            'input',
            '[tabindex="0"]',
            '.btn',
            '.copy-btn',
            '.badge-link'
        ];

        const failingElements = [];

        for (const selector of interactiveSelectors) {
            const elements = page.locator(selector);
            const count = await elements.count();

            for (let i = 0; i < count; i++) {
                const element = elements.nth(i);

                // Skip if element is not visible
                if (!(await element.isVisible())) continue;

                const box = await element.boundingBox();
                if (!box) continue;

                // Get computed styles for padding (which contributes to touch target)
                const styles = await element.evaluate(el => {
                    const computed = window.getComputedStyle(el);
                    return {
                        paddingTop: parseFloat(computed.paddingTop) || 0,
                        paddingBottom: parseFloat(computed.paddingBottom) || 0,
                        paddingLeft: parseFloat(computed.paddingLeft) || 0,
                        paddingRight: parseFloat(computed.paddingRight) || 0,
                        minHeight: computed.minHeight,
                        minWidth: computed.minWidth
                    };
                });

                // Calculate effective touch target size
                // The bounding box already includes padding
                const effectiveWidth = box.width;
                const effectiveHeight = box.height;

                // Check if element meets minimum touch target size
                // Allow a small tolerance for rounding
                if (effectiveWidth < MIN_TOUCH_TARGET - 2 || effectiveHeight < MIN_TOUCH_TARGET - 2) {
                    // Get element identifier for debugging
                    const identifier = await element.evaluate(el => {
                        return {
                            tag: el.tagName,
                            text: el.textContent?.slice(0, 30) || '',
                            testId: el.getAttribute('data-testid') || '',
                            class: el.className || ''
                        };
                    });

                    failingElements.push({
                        selector,
                        identifier,
                        width: effectiveWidth,
                        height: effectiveHeight
                    });
                }
            }
        }

        // Report failing elements if any
        if (failingElements.length > 0) {
            console.log('Elements with insufficient touch targets:', failingElements);
        }

        // All interactive elements should meet minimum touch target
        expect(failingElements.length).toBe(0);
    });

    test('Primary CTA buttons meet touch target requirements', async ({ page }) => {
        const MIN_TOUCH_TARGET = 44;

        // Test specific buttons that users frequently interact with
        const buttonSelectors = [
            '[data-testid="get-started-btn"]',
            '[data-testid="github-btn"]'
        ];

        for (const selector of buttonSelectors) {
            const button = page.locator(selector);

            if (await button.count() > 0 && await button.isVisible()) {
                const box = await button.boundingBox();
                expect(box).not.toBeNull();

                if (box) {
                    expect(box.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
                    expect(box.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
                }
            }
        }
    });

    test('Copy buttons meet touch target requirements', async ({ page }) => {
        const MIN_TOUCH_TARGET = 44;

        const codeExamplesSection = page.locator('[data-testid="code-examples-section"]');
        await codeExamplesSection.scrollIntoViewIfNeeded();

        const copyButtons = page.locator('.copy-btn');
        const count = await copyButtons.count();

        for (let i = 0; i < count; i++) {
            const btn = copyButtons.nth(i);

            if (await btn.isVisible()) {
                const box = await btn.boundingBox();
                expect(box).not.toBeNull();

                if (box) {
                    expect(box.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
                    expect(box.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
                }
            }
        }
    });

    test('Footer navigation links meet touch target requirements', async ({ page }) => {
        const MIN_TOUCH_TARGET = 44;

        const footerNav = page.locator('[data-testid="footer-nav"]');
        await footerNav.scrollIntoViewIfNeeded();

        const navLinks = footerNav.locator('a');
        const count = await navLinks.count();

        for (let i = 0; i < count; i++) {
            const link = navLinks.nth(i);

            if (await link.isVisible()) {
                const box = await link.boundingBox();
                expect(box).not.toBeNull();

                if (box) {
                    expect(box.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
                    expect(box.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
                }
            }
        }
    });

    test('Documentation links meet touch target requirements', async ({ page }) => {
        const MIN_TOUCH_TARGET = 44;

        const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
        await gettingStartedSection.scrollIntoViewIfNeeded();

        const docLinks = page.locator('[data-testid="getting-started-links"] a, [data-testid="docs-link"], [data-testid="wiki-link"]');
        const count = await docLinks.count();

        for (let i = 0; i < count; i++) {
            const link = docLinks.nth(i);

            if (await link.isVisible()) {
                const box = await link.boundingBox();
                expect(box).not.toBeNull();

                if (box) {
                    expect(box.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
                    expect(box.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
                }
            }
        }
    });

    test('Badge links meet touch target requirements', async ({ page }) => {
        const MIN_TOUCH_TARGET = 44;

        const footer = page.locator('[data-testid="footer-section"]');
        await footer.scrollIntoViewIfNeeded();

        const badgeLinks = page.locator('.badge-link');
        const count = await badgeLinks.count();

        for (let i = 0; i < count; i++) {
            const link = badgeLinks.nth(i);

            if (await link.isVisible()) {
                const box = await link.boundingBox();
                expect(box).not.toBeNull();

                if (box) {
                    expect(box.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
                    expect(box.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
                }
            }
        }
    });
});
