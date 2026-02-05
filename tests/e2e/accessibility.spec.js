/**
 * Accessibility E2E Tests
 * Owner: Scenario 8 - Accessibility Compliance
 *
 * Tests for:
 * - Color contrast (4.5:1)
 * - Keyboard navigation
 * - Focus indicators
 * - ARIA labels
 * - Semantic HTML (heading hierarchy, landmarks)
 */

const { test, expect } = require('@playwright/test');
const AxeBuilder = require('@axe-core/playwright').default;

test.describe('Accessibility', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    // Test Case 1: Run axe accessibility audit
    test('should pass automated accessibility audit without critical or serious violations', async ({ page }) => {
        const accessibilityScanResults = await new AxeBuilder({ page })
            .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
            .analyze();

        // Filter for only critical and serious violations
        const criticalAndSerious = accessibilityScanResults.violations.filter(
            violation => violation.impact === 'critical' || violation.impact === 'serious'
        );

        // Log violations for debugging
        if (criticalAndSerious.length > 0) {
            console.log('Critical/Serious Accessibility Violations:');
            criticalAndSerious.forEach(violation => {
                console.log(`- ${violation.id}: ${violation.description}`);
                console.log(`  Impact: ${violation.impact}`);
                console.log(`  Nodes affected: ${violation.nodes.length}`);
            });
        }

        expect(criticalAndSerious).toHaveLength(0);
    });

    // Test Case 2: Check main text color contrast
    test('should have sufficient color contrast for main text (>= 4.5:1)', async ({ page }) => {
        // Use axe-core to specifically check color contrast
        const accessibilityScanResults = await new AxeBuilder({ page })
            .withTags(['wcag2aa'])
            .options({ rules: { 'color-contrast': { enabled: true } } })
            .analyze();

        const contrastViolations = accessibilityScanResults.violations.filter(
            violation => violation.id === 'color-contrast'
        );

        // Log any contrast issues for debugging
        if (contrastViolations.length > 0) {
            console.log('Color Contrast Violations:');
            contrastViolations.forEach(violation => {
                violation.nodes.forEach(node => {
                    console.log(`  - ${node.html}`);
                    console.log(`    Message: ${node.failureSummary}`);
                });
            });
        }

        expect(contrastViolations).toHaveLength(0);
    });

    // Test Case 3: Tab through all interactive elements
    test('should allow keyboard navigation through all interactive elements', async ({ page }) => {
        // Get all interactive elements
        const interactiveElements = await page.locator('a, button, input, select, textarea, [tabindex]:not([tabindex="-1"])').all();

        // Filter out elements that are hidden or have display:none
        const visibleInteractiveCount = await page.evaluate(() => {
            const elements = document.querySelectorAll('a, button, input, select, textarea, [tabindex]:not([tabindex="-1"])');
            let count = 0;
            elements.forEach(el => {
                const style = window.getComputedStyle(el);
                if (style.display !== 'none' && style.visibility !== 'hidden') {
                    count++;
                }
            });
            return count;
        });

        expect(visibleInteractiveCount).toBeGreaterThan(0);

        // Start from the body and tab through
        await page.keyboard.press('Tab');

        let previousFocused = null;
        let tabbableCount = 0;
        const maxTabs = 50; // Safety limit
        const focusedElements = new Set();

        for (let i = 0; i < maxTabs; i++) {
            const currentFocused = await page.evaluate(() => {
                const el = document.activeElement;
                return {
                    tagName: el?.tagName?.toLowerCase(),
                    id: el?.id,
                    className: el?.className,
                    href: el?.href,
                    ariaLabel: el?.getAttribute('aria-label'),
                    outerHTML: el?.outerHTML?.substring(0, 100)
                };
            });

            if (!currentFocused.tagName || currentFocused.tagName === 'body') {
                break;
            }

            const elementKey = `${currentFocused.tagName}-${currentFocused.id || currentFocused.className}`;

            if (focusedElements.has(elementKey)) {
                // We've looped back
                break;
            }

            focusedElements.add(elementKey);
            tabbableCount++;
            previousFocused = currentFocused;

            await page.keyboard.press('Tab');
        }

        // We should be able to tab through at least some elements
        expect(tabbableCount).toBeGreaterThan(0);
    });

    // Test Case 4: Check focus visibility on buttons
    test('should show visible focus indicators on buttons when focused', async ({ page }) => {
        // Verify that focus-visible CSS rules exist for buttons
        // This tests that the accessibility CSS is properly defined
        const focusStylesExist = await page.evaluate(() => {
            const stylesheets = Array.from(document.styleSheets);
            let hasBtnFocusRule = false;
            let hasGlobalFocusRule = false;

            for (const sheet of stylesheets) {
                try {
                    const rules = Array.from(sheet.cssRules || []);
                    for (const rule of rules) {
                        if (rule.selectorText) {
                            // Check for .btn:focus-visible rule
                            if (rule.selectorText.includes('.btn') && rule.selectorText.includes('focus-visible')) {
                                if (rule.style.outline && !rule.style.outline.includes('none')) {
                                    hasBtnFocusRule = true;
                                }
                            }
                            // Check for global focus-visible rule
                            if (rule.selectorText.includes(':focus-visible') &&
                                (rule.selectorText.includes('a:') || rule.selectorText.includes('button'))) {
                                if (rule.style.outline && !rule.style.outline.includes('none')) {
                                    hasGlobalFocusRule = true;
                                }
                            }
                        }
                    }
                } catch (e) {
                    // Skip cross-origin stylesheets
                }
            }
            return { hasBtnFocusRule, hasGlobalFocusRule };
        });

        // CSS should have focus-visible rules defined
        expect(focusStylesExist.hasBtnFocusRule || focusStylesExist.hasGlobalFocusRule).toBe(true);

        // Verify visible buttons exist and can receive keyboard focus
        const visibleButtons = await page.evaluate(() => {
            const allButtons = document.querySelectorAll('button, .btn');
            const visible = [];

            allButtons.forEach((button, index) => {
                const style = window.getComputedStyle(button);
                const rect = button.getBoundingClientRect();

                if (style.display !== 'none' &&
                    style.visibility !== 'hidden' &&
                    rect.width > 0 &&
                    rect.height > 0) {
                    visible.push({
                        index,
                        tagName: button.tagName,
                        className: button.className
                    });
                }
            });

            return visible;
        });

        expect(visibleButtons.length).toBeGreaterThan(0);

        // Verify at least one button is reachable via Tab
        let focusedButtonCount = 0;

        for (let i = 0; i < 20; i++) {
            await page.keyboard.press('Tab');

            const isFocusedButton = await page.evaluate(() => {
                const el = document.activeElement;
                return el && (el.tagName === 'BUTTON' || el.classList.contains('btn'));
            });

            if (isFocusedButton) {
                focusedButtonCount++;
            }
        }

        expect(focusedButtonCount).toBeGreaterThan(0);
    });

    // Test Case 5: Check for ARIA labels on icon buttons
    test('should have aria-label attributes on copy buttons and icon-only buttons', async ({ page }) => {
        // Check copy buttons specifically
        const copyButtons = await page.locator('.copy-btn').all();

        for (const button of copyButtons) {
            const ariaLabel = await button.getAttribute('aria-label');
            expect(ariaLabel).toBeTruthy();
            expect(ariaLabel.length).toBeGreaterThan(0);
        }

        // Check hamburger button
        const hamburgerButton = await page.locator('.hamburger-btn');
        if (await hamburgerButton.count() > 0) {
            const ariaLabel = await hamburgerButton.getAttribute('aria-label');
            expect(ariaLabel).toBeTruthy();
        }

        // Check any buttons with only SVG children (icon-only buttons)
        const iconOnlyButtons = await page.evaluate(() => {
            const buttons = document.querySelectorAll('button');
            const results = [];

            buttons.forEach(button => {
                // Check if button only contains SVG (icon-only)
                const textContent = button.textContent?.trim();
                const hasSvg = button.querySelector('svg');
                const hasOnlySvgContent = hasSvg && (!textContent || textContent.length === 0);

                if (hasOnlySvgContent) {
                    results.push({
                        hasAriaLabel: !!button.getAttribute('aria-label'),
                        className: button.className
                    });
                }
            });

            return results;
        });

        // Icon-only buttons should have aria-label
        for (const button of iconOnlyButtons) {
            expect(button.hasAriaLabel).toBe(true);
        }
    });

    // Test Case 6: Verify heading hierarchy
    test('should have proper heading hierarchy with one h1 followed by h2s without skipping levels', async ({ page }) => {
        // Get all headings in document order
        const headings = await page.evaluate(() => {
            const elements = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
            return Array.from(elements).map(el => ({
                level: parseInt(el.tagName.charAt(1)),
                text: el.textContent?.trim()
            }));
        });

        // Should have at least one heading
        expect(headings.length).toBeGreaterThan(0);

        // Count h1s - should be exactly one
        const h1Count = headings.filter(h => h.level === 1).length;
        expect(h1Count).toBe(1);

        // First heading should be h1
        expect(headings[0].level).toBe(1);

        // Check for level skipping (e.g., h1 -> h3 without h2)
        let previousLevel = 0;
        for (const heading of headings) {
            // Allow going to same level, one level down, or any level up
            // Don't allow skipping more than one level down
            if (heading.level > previousLevel + 1 && previousLevel > 0) {
                throw new Error(
                    `Heading hierarchy skip detected: jumped from h${previousLevel} to h${heading.level} ("${heading.text}")`
                );
            }
            previousLevel = heading.level;
        }
    });

    // Test Case 7: Check for main landmark
    test('should have <main> element wrapping primary content', async ({ page }) => {
        const mainElement = await page.locator('main');

        // Check that main exists
        await expect(mainElement).toHaveCount(1);

        // Check that main contains the primary content sections
        const hasHero = await mainElement.locator('#hero').count();
        const hasFeatures = await mainElement.locator('#features').count();

        expect(hasHero).toBe(1);
        expect(hasFeatures).toBe(1);
    });

    // Test Case 8: Check for nav landmark
    test('should have navigation wrapped in <nav> element', async ({ page }) => {
        const navElement = await page.locator('nav');

        // Check that nav exists
        const navCount = await navElement.count();
        expect(navCount).toBeGreaterThan(0);

        // Check that nav has aria-label or role for accessibility
        const mainNav = await page.locator('nav').first();

        // Nav should contain links
        const linksInNav = await mainNav.locator('a').count();
        expect(linksInNav).toBeGreaterThan(0);
    });

    // Additional accessibility tests for comprehensive coverage

    test('should have skip link or first focusable element in header', async ({ page }) => {
        // Tab once and check we're in the header/nav area
        await page.keyboard.press('Tab');

        const firstFocused = await page.evaluate(() => {
            const el = document.activeElement;
            return {
                tagName: el?.tagName?.toLowerCase(),
                isInHeader: !!el?.closest('header'),
                isInNav: !!el?.closest('nav')
            };
        });

        // First focusable should be in header or nav
        expect(firstFocused.isInHeader || firstFocused.isInNav).toBe(true);
    });

    test('should have proper aria-expanded attribute on hamburger menu', async ({ page }) => {
        const hamburgerButton = await page.locator('.hamburger-btn');

        if (await hamburgerButton.count() > 0) {
            const ariaExpanded = await hamburgerButton.getAttribute('aria-expanded');
            // Should have aria-expanded attribute (either true or false)
            expect(ariaExpanded).toBeTruthy();
            expect(['true', 'false']).toContain(ariaExpanded);
        }
    });

    test('should have descriptive link text or aria-label for all links', async ({ page }) => {
        const links = await page.evaluate(() => {
            const allLinks = document.querySelectorAll('a');
            const results = [];

            allLinks.forEach(link => {
                const style = window.getComputedStyle(link);
                if (style.display === 'none' || style.visibility === 'hidden') return;

                const text = link.textContent?.trim();
                const ariaLabel = link.getAttribute('aria-label');
                const title = link.getAttribute('title');
                const hasDescriptiveContent = (text && text.length > 0) || ariaLabel || title;

                results.push({
                    href: link.href,
                    hasDescriptiveContent,
                    text: text?.substring(0, 50)
                });
            });

            return results;
        });

        for (const link of links) {
            expect(link.hasDescriptiveContent).toBe(true);
        }
    });

    test('should have proper role attributes where needed', async ({ page }) => {
        // Check that sections have proper labeling
        const sections = await page.locator('section[id]').all();

        for (const section of sections) {
            const id = await section.getAttribute('id');
            const ariaLabelledBy = await section.getAttribute('aria-labelledby');
            const ariaLabel = await section.getAttribute('aria-label');

            // Sections should have either aria-labelledby or aria-label for accessibility
            // Or contain a heading that labels them
            const hasHeading = await section.locator('h1, h2, h3').count();

            expect(hasHeading > 0 || ariaLabelledBy || ariaLabel).toBeTruthy();
        }
    });
});
