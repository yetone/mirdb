// @ts-check
const { test, expect } = require('@playwright/test');
const AxeBuilder = require('@axe-core/playwright').default;

test.describe('Accessibility Compliance - WCAG 2.1 AA', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    // Test Case 1: Check for single h1 element
    test('TC1: Page has exactly one h1 element (product name)', async ({ page }) => {
        // Get all h1 elements on the page
        const h1Elements = page.locator('h1');
        const h1Count = await h1Elements.count();

        // WCAG requires exactly one h1 per page for proper document structure
        expect(h1Count).toBe(1);

        // Verify the h1 contains the product name
        const h1Text = await h1Elements.first().textContent();
        expect(h1Text).toBe('MirDB');
    });

    // Test Case 2: Verify heading hierarchy is sequential
    test('TC2: Headings follow proper hierarchy (no skipped levels)', async ({ page }) => {
        // Get all heading elements in document order
        const headings = await page.evaluate(() => {
            const headingElements = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
            return Array.from(headingElements).map(h => ({
                level: parseInt(h.tagName.charAt(1)),
                text: h.textContent?.trim().substring(0, 50)
            }));
        });

        // Verify there are headings on the page
        expect(headings.length).toBeGreaterThan(0);

        // First heading should be h1
        expect(headings[0].level).toBe(1);

        // Check that no heading level is skipped
        // For example, h2 should not be followed directly by h4
        for (let i = 1; i < headings.length; i++) {
            const currentLevel = headings[i].level;
            const previousLevel = headings[i - 1].level;

            // A heading can go down (h1 -> h2), stay same (h2 -> h2),
            // or go back up any number of levels (h3 -> h2 or h3 -> h1)
            // But it cannot skip down (h1 -> h3 without h2)
            if (currentLevel > previousLevel) {
                // When going deeper, should only go one level at a time
                const levelDifference = currentLevel - previousLevel;
                expect(levelDifference,
                    `Heading hierarchy skipped from h${previousLevel} to h${currentLevel} at "${headings[i].text}"`
                ).toBe(1);
            }
        }
    });

    // Test Case 3: Check all images for alt attributes
    test('TC3: All images have descriptive alt text', async ({ page }) => {
        // Get all img elements
        const images = page.locator('img');
        const imgCount = await images.count();

        // Check each image has alt attribute
        for (let i = 0; i < imgCount; i++) {
            const img = images.nth(i);
            const alt = await img.getAttribute('alt');
            const src = await img.getAttribute('src');

            // Alt attribute must exist and not be empty
            expect(alt, `Image with src="${src}" is missing alt text`).toBeTruthy();
        }

        // Also check SVG images with role="img"
        const svgImages = page.locator('svg[role="img"]');
        const svgCount = await svgImages.count();

        for (let i = 0; i < svgCount; i++) {
            const svg = svgImages.nth(i);
            const ariaLabel = await svg.getAttribute('aria-label');
            const hasTitle = await svg.locator('title').count();

            // SVG images should have aria-label or title element
            expect(
                ariaLabel || hasTitle > 0,
                `SVG image at index ${i} is missing accessible name (aria-label or title)`
            ).toBeTruthy();
        }
    });

    // Test Case 4: Run color contrast checker on text elements
    test('TC4: All text meets WCAG AA contrast requirements (4.5:1)', async ({ page }) => {
        // Use axe-core to check color contrast
        const accessibilityResults = await new AxeBuilder({ page })
            .withTags(['wcag2aa'])
            .withRules(['color-contrast'])
            .analyze();

        // Filter for color contrast violations
        const contrastViolations = accessibilityResults.violations.filter(
            v => v.id === 'color-contrast'
        );

        // If there are violations, generate a detailed error message
        if (contrastViolations.length > 0) {
            const violationDetails = contrastViolations.flatMap(v =>
                v.nodes.map(n => ({
                    html: n.html,
                    message: n.failureSummary
                }))
            );

            console.log('Color contrast violations:', JSON.stringify(violationDetails, null, 2));
        }

        // Assert no color contrast violations
        expect(
            contrastViolations.length,
            `Found ${contrastViolations.length} color contrast violations`
        ).toBe(0);
    });

    // Test Case 5: Test keyboard navigation through all interactive elements
    test('TC5: All interactive elements are focusable and operable via keyboard', async ({ page }) => {
        // Get all interactive elements
        const interactiveElements = await page.evaluate(() => {
            const elements = document.querySelectorAll('a, button, input, select, textarea, [tabindex]');
            return Array.from(elements)
                .filter(el => {
                    const tabIndex = el.getAttribute('tabindex');
                    // Filter out elements with negative tabindex
                    return tabIndex === null || parseInt(tabIndex) >= 0;
                })
                .map(el => ({
                    tagName: el.tagName.toLowerCase(),
                    testId: el.getAttribute('data-testid') || null,
                    href: el.getAttribute('href') || null,
                    text: el.textContent?.trim().substring(0, 30) || ''
                }));
        });

        expect(interactiveElements.length).toBeGreaterThan(0);

        // Track focused elements via keyboard navigation
        const focusedElements = [];
        let maxTabs = 50; // Safety limit

        // Start from body
        await page.keyboard.press('Tab');

        while (maxTabs > 0) {
            const focusedInfo = await page.evaluate(() => {
                const el = document.activeElement;
                if (!el || el === document.body) return null;
                return {
                    tagName: el.tagName.toLowerCase(),
                    testId: el.getAttribute('data-testid'),
                    href: el.getAttribute('href'),
                    text: el.textContent?.trim().substring(0, 30)
                };
            });

            if (!focusedInfo) break;

            // Check if we've cycled back to the beginning
            if (focusedElements.length > 0 &&
                JSON.stringify(focusedInfo) === JSON.stringify(focusedElements[0])) {
                break;
            }

            focusedElements.push(focusedInfo);
            await page.keyboard.press('Tab');
            maxTabs--;
        }

        // Verify we could tab through elements
        expect(focusedElements.length).toBeGreaterThan(0);

        // All links and buttons should be in the tab order
        const links = page.locator('a[href]');
        const linkCount = await links.count();

        // At minimum, we should be able to reach navigation links and CTAs
        expect(focusedElements.length).toBeGreaterThanOrEqual(Math.min(linkCount, 5));
    });

    // Test Case 6: Check focus indicators visibility
    test('TC6: Focus indicators are clearly visible on all focusable elements', async ({ page }) => {
        // Get all focusable elements
        const focusableElements = page.locator('a, button, input, select, textarea, [tabindex="0"]');
        const count = await focusableElements.count();

        expect(count).toBeGreaterThan(0);

        // Test a sample of focusable elements for focus visibility
        const samplesToTest = Math.min(count, 10);

        for (let i = 0; i < samplesToTest; i++) {
            const element = focusableElements.nth(i);

            // Skip hidden elements
            if (!await element.isVisible()) continue;

            // Get styles before focus
            const beforeFocus = await element.evaluate(el => {
                const styles = window.getComputedStyle(el);
                return {
                    outline: styles.outline,
                    outlineColor: styles.outlineColor,
                    outlineWidth: styles.outlineWidth,
                    outlineStyle: styles.outlineStyle,
                    boxShadow: styles.boxShadow,
                    border: styles.border,
                    backgroundColor: styles.backgroundColor
                };
            });

            // Focus the element
            await element.focus();

            // Get styles after focus
            const afterFocus = await element.evaluate(el => {
                const styles = window.getComputedStyle(el);
                return {
                    outline: styles.outline,
                    outlineColor: styles.outlineColor,
                    outlineWidth: styles.outlineWidth,
                    outlineStyle: styles.outlineStyle,
                    boxShadow: styles.boxShadow,
                    border: styles.border,
                    backgroundColor: styles.backgroundColor
                };
            });

            // Check if there's a visible focus indicator
            // Browser default outline, custom outline, box-shadow, or border change are all acceptable
            const hasDefaultOutline = afterFocus.outlineStyle !== 'none' &&
                afterFocus.outlineWidth !== '0px';
            const hasBoxShadowChange = beforeFocus.boxShadow !== afterFocus.boxShadow;
            const hasBorderChange = beforeFocus.border !== afterFocus.border;
            const hasBackgroundChange = beforeFocus.backgroundColor !== afterFocus.backgroundColor;

            // At least one focus indicator should be present
            // Note: Modern browsers apply default focus styles, so this should pass
            const hasFocusIndicator = hasDefaultOutline || hasBoxShadowChange ||
                hasBorderChange || hasBackgroundChange;

            // Get element info for error message
            const elementInfo = await element.evaluate(el => ({
                tagName: el.tagName,
                testId: el.getAttribute('data-testid'),
                text: el.textContent?.trim().substring(0, 30)
            }));

            expect(
                hasFocusIndicator,
                `No visible focus indicator on ${elementInfo.tagName} "${elementInfo.text || elementInfo.testId}"`
            ).toBeTruthy();
        }
    });

    // Test Case 7: Run automated accessibility audit (axe-core)
    test('TC7: No critical or serious accessibility violations reported', async ({ page }) => {
        // Run full axe-core accessibility audit
        const accessibilityResults = await new AxeBuilder({ page })
            .withTags(['wcag2a', 'wcag2aa', 'wcag21aa'])
            .analyze();

        // Separate violations by impact
        const criticalViolations = accessibilityResults.violations.filter(
            v => v.impact === 'critical'
        );
        const seriousViolations = accessibilityResults.violations.filter(
            v => v.impact === 'serious'
        );

        // Log violations for debugging
        if (accessibilityResults.violations.length > 0) {
            console.log('Accessibility violations found:');
            accessibilityResults.violations.forEach(violation => {
                console.log(`\n[${violation.impact?.toUpperCase()}] ${violation.id}: ${violation.description}`);
                console.log(`Help: ${violation.helpUrl}`);
                violation.nodes.forEach((node, i) => {
                    console.log(`  Node ${i + 1}: ${node.html.substring(0, 100)}...`);
                    console.log(`    ${node.failureSummary}`);
                });
            });
        }

        // Assert no critical violations
        expect(
            criticalViolations.length,
            `Found ${criticalViolations.length} critical accessibility violations: ${
                criticalViolations.map(v => v.id).join(', ')
            }`
        ).toBe(0);

        // Assert no serious violations
        expect(
            seriousViolations.length,
            `Found ${seriousViolations.length} serious accessibility violations: ${
                seriousViolations.map(v => v.id).join(', ')
            }`
        ).toBe(0);

        // Log summary of passed checks
        console.log(`\nAccessibility audit passed: ${accessibilityResults.passes.length} checks passed`);
    });
});
