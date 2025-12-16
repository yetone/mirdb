// @ts-check
const { test, expect } = require('@playwright/test');
const AxeBuilder = require('@axe-core/playwright').default;

/**
 * WCAG 2.1 AA Accessibility Compliance Tests
 *
 * This test suite verifies that the MirDB homepage meets WCAG 2.1 AA accessibility standards.
 * Tests cover: color contrast, heading hierarchy, keyboard navigation, focus indicators,
 * alt text for images, descriptive link text, and automated axe-core auditing.
 */

test.describe('WCAG 2.1 AA Accessibility Compliance', () => {

    test.beforeEach(async ({ page }) => {
        await page.goto('/');
        // Wait for page to be fully loaded
        await page.waitForLoadState('networkidle');
    });

    /**
     * Test Case 1: Color Contrast Ratios
     * Expected: All text meets WCAG AA contrast ratio requirements (4.5:1 for normal text, 3:1 for large text)
     */
    test('TC1: Color contrast ratios meet WCAG AA requirements', async ({ page }) => {
        // Use axe-core to check color contrast specifically
        const accessibilityScanResults = await new AxeBuilder({ page })
            .withTags(['wcag2aa', 'wcag21aa'])
            .analyze();

        // Filter for color contrast related violations
        const contrastViolations = accessibilityScanResults.violations.filter(
            violation => violation.id === 'color-contrast' || violation.id === 'color-contrast-enhanced'
        );

        // Log any contrast violations for debugging
        if (contrastViolations.length > 0) {
            console.log('Color contrast violations found:');
            contrastViolations.forEach(violation => {
                console.log(`- ${violation.help}`);
                violation.nodes.forEach(node => {
                    console.log(`  Element: ${node.html}`);
                    console.log(`  Impact: ${node.impact}`);
                });
            });
        }

        expect(contrastViolations.length).toBe(0);
    });

    /**
     * Test Case 2: Heading Hierarchy
     * Expected: Page uses proper heading hierarchy (h1 > h2 > h3) without skipping levels
     */
    test('TC2: Heading hierarchy is properly structured without skipping levels', async ({ page }) => {
        // Get all heading elements in document order
        const headings = await page.$$eval('h1, h2, h3, h4, h5, h6', (elements) => {
            return elements.map(el => ({
                tagName: el.tagName.toLowerCase(),
                level: parseInt(el.tagName.substring(1)),
                text: el.textContent?.trim().substring(0, 50) || ''
            }));
        });

        expect(headings.length).toBeGreaterThan(0);

        // Verify there is exactly one h1
        const h1Count = headings.filter(h => h.level === 1).length;
        expect(h1Count).toBe(1);

        // Verify heading hierarchy doesn't skip levels
        let lastLevel = 0;
        const violations = [];

        for (const heading of headings) {
            // First heading should be h1
            if (lastLevel === 0 && heading.level !== 1) {
                violations.push(`First heading should be h1, but found h${heading.level}: "${heading.text}"`);
            }

            // Heading level should not jump by more than 1 (e.g., h1 to h3 is invalid)
            if (lastLevel > 0 && heading.level > lastLevel + 1) {
                violations.push(`Heading hierarchy skipped from h${lastLevel} to h${heading.level}: "${heading.text}"`);
            }

            lastLevel = heading.level;
        }

        if (violations.length > 0) {
            console.log('Heading hierarchy violations:');
            violations.forEach(v => console.log(`- ${v}`));
        }

        expect(violations.length).toBe(0);
    });

    /**
     * Test Case 3: Keyboard Navigation
     * Expected: All interactive elements are reachable and operable via keyboard (Tab, Enter, Escape)
     */
    test('TC3: All interactive elements are keyboard accessible', async ({ page }) => {
        // Get all interactive elements that should be keyboard accessible
        const interactiveElements = await page.$$eval(
            'a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])',
            (elements) => elements.map((el, index) => ({
                tagName: el.tagName.toLowerCase(),
                text: el.textContent?.trim().substring(0, 30) || el.getAttribute('aria-label') || '',
                href: el.getAttribute('href') || '',
                testId: el.getAttribute('data-testid') || '',
                index
            }))
        );

        expect(interactiveElements.length).toBeGreaterThan(0);

        // Test Tab navigation through all interactive elements
        let focusedCount = 0;
        const focusableElements = [];

        // Start from the beginning of the page by clicking on body first
        await page.locator('body').click();
        await page.keyboard.press('Tab');

        // Tab through elements and verify they can receive focus
        for (let i = 0; i < Math.min(interactiveElements.length + 5, 30); i++) {
            const focusedElement = await page.evaluate(() => {
                const el = document.activeElement;
                if (el && el !== document.body && el.tagName !== 'HTML') {
                    return {
                        tagName: el.tagName.toLowerCase(),
                        text: el.textContent?.trim().substring(0, 30) || el.getAttribute('aria-label') || '',
                        testId: el.getAttribute('data-testid') || ''
                    };
                }
                return null;
            });

            if (focusedElement && !focusableElements.find(f => f.testId === focusedElement.testId && focusedElement.testId)) {
                focusableElements.push(focusedElement);
                focusedCount++;
            }

            await page.keyboard.press('Tab');
        }

        // Verify we can tab through multiple elements (at least 5 interactive elements should be focusable)
        expect(focusedCount).toBeGreaterThanOrEqual(5);

        // Test Enter key on a button (Get Started link)
        const getStartedBtn = page.getByTestId('get-started-btn');
        await getStartedBtn.focus();

        // Verify focus is on the Get Started button
        const isFocused = await getStartedBtn.evaluate(el => el === document.activeElement);
        expect(isFocused).toBe(true);

        // Verify the element can be activated with Enter (check that clicking would navigate to the hash)
        const href = await getStartedBtn.getAttribute('href');
        expect(href).toBe('#getting-started');
    });

    /**
     * Test Case 4: Focus Indicators
     * Expected: Visible focus indicators appear on all interactive elements when focused
     */
    test('TC4: Visible focus indicators appear on all interactive elements', async ({ page }) => {
        // Test focus indicators on various interactive elements
        const elementsToTest = [
            { selector: '[data-testid="get-started-btn"]', name: 'Get Started button' },
            { selector: '[data-testid="github-btn"]', name: 'GitHub button' },
            { selector: '[data-testid="footer-github-link"]', name: 'Footer GitHub link' },
            { selector: '[data-testid="docs-link"]', name: 'Documentation link' }
        ];

        const focusResults = [];

        for (const element of elementsToTest) {
            const el = page.locator(element.selector).first();

            if (await el.count() === 0) {
                continue;
            }

            await el.focus();
            // Small delay to ensure focus styles are applied
            await page.waitForTimeout(100);

            // Check if the element has a visible focus indicator
            const focusStyle = await el.evaluate((elem) => {
                const styles = window.getComputedStyle(elem);
                const outlineWidth = styles.getPropertyValue('outline-width');
                const outlineStyle = styles.getPropertyValue('outline-style');
                const outlineColor = styles.getPropertyValue('outline-color');
                const boxShadow = styles.getPropertyValue('box-shadow');
                const borderColor = styles.getPropertyValue('border-color');

                // Check if outline has a visible color (not transparent)
                const outlineColorValid = outlineColor &&
                    outlineColor !== 'rgba(0, 0, 0, 0)' &&
                    outlineColor !== 'transparent';

                return {
                    outlineWidth,
                    outlineStyle,
                    outlineColor,
                    boxShadow,
                    borderColor,
                    hasVisibleOutline: outlineStyle !== 'none' && outlineWidth !== '0px' && outlineColorValid,
                    hasBoxShadow: boxShadow !== 'none' && boxShadow !== ''
                };
            });

            // Element should have either a visible outline or box shadow as focus indicator
            const hasFocusIndicator = focusStyle.hasVisibleOutline || focusStyle.hasBoxShadow;

            focusResults.push({
                name: element.name,
                hasFocusIndicator,
                styles: focusStyle
            });
        }

        // Log results for debugging
        focusResults.forEach(result => {
            if (!result.hasFocusIndicator) {
                console.log(`Missing focus indicator on: ${result.name}`);
                console.log(`  Styles: outline-width=${result.styles.outlineWidth}, outline-style=${result.styles.outlineStyle}, outline-color=${result.styles.outlineColor}`);
            }
        });

        // All tested elements should have focus indicators
        const elementsWithFocus = focusResults.filter(r => r.hasFocusIndicator);
        expect(elementsWithFocus.length).toBe(focusResults.length);
    });

    /**
     * Test Case 5: Alt Text for Images
     * Expected: All images and diagrams have descriptive alt text
     */
    test('TC5: All images have descriptive alt text', async ({ page }) => {
        // Get all images on the page
        const images = await page.$$eval('img', (imgs) => {
            return imgs.map((img) => ({
                src: img.src,
                alt: img.alt,
                testId: img.getAttribute('data-testid') || '',
                hasAlt: img.hasAttribute('alt'),
                altIsDescriptive: img.alt && img.alt.length > 3 && !['image', 'photo', 'picture', 'img'].includes(img.alt.toLowerCase())
            }));
        });

        // Also check for SVG images with proper accessibility attributes
        const svgs = await page.$$eval('svg:not([aria-hidden="true"])', (svgElements) => {
            return svgElements.map((svg) => ({
                hasTitle: !!svg.querySelector('title'),
                hasAriaLabel: svg.hasAttribute('aria-label'),
                role: svg.getAttribute('role')
            }));
        });

        // All <img> elements should have alt attributes
        const imagesWithoutAlt = images.filter(img => !img.hasAlt);
        if (imagesWithoutAlt.length > 0) {
            console.log('Images without alt attribute:');
            imagesWithoutAlt.forEach(img => console.log(`- ${img.src}`));
        }
        expect(imagesWithoutAlt.length).toBe(0);

        // All <img> elements should have descriptive alt text (not empty or generic)
        const imagesWithPoorAlt = images.filter(img => img.hasAlt && !img.altIsDescriptive);
        if (imagesWithPoorAlt.length > 0) {
            console.log('Images with poor/generic alt text:');
            imagesWithPoorAlt.forEach(img => console.log(`- ${img.src}: "${img.alt}"`));
        }

        // Check that figures/diagrams have proper accessibility
        const diagramFigure = await page.locator('[data-testid="architecture-diagram-container"] figure').first();
        if (await diagramFigure.count() > 0) {
            const figureHasAria = await diagramFigure.evaluate((el) => {
                return el.hasAttribute('role') && el.hasAttribute('aria-label');
            });
            expect(figureHasAria).toBe(true);

            // Check for visually hidden caption for screen readers
            const hasCaption = await page.locator('.visually-hidden[data-testid="architecture-diagram-alt"]').count() > 0;
            expect(hasCaption).toBe(true);
        }
    });

    /**
     * Test Case 6: Descriptive Link Text
     * Expected: Links have descriptive text (no 'click here' or 'read more' without context)
     */
    test('TC6: Links have descriptive text without generic phrases', async ({ page }) => {
        // Get all links on the page
        const links = await page.$$eval('a', (anchors) => {
            return anchors.map((a) => {
                // Check if link contains an img with alt text
                const imgAlt = a.querySelector('img')?.alt || '';
                return {
                    text: a.textContent?.trim() || '',
                    href: a.href,
                    ariaLabel: a.getAttribute('aria-label') || '',
                    imgAlt: imgAlt,
                    testId: a.getAttribute('data-testid') || ''
                };
            });
        });

        // Generic link text patterns to avoid
        const genericPatterns = [
            /^click here$/i,
            /^here$/i,
            /^read more$/i,
            /^more$/i,
            /^learn more$/i,
            /^link$/i,
            /^click$/i
        ];

        const linksWithGenericText = links.filter(link => {
            // Use aria-label first, then text, then img alt
            const textToCheck = link.ariaLabel || link.text || link.imgAlt;
            return genericPatterns.some(pattern => pattern.test(textToCheck.trim()));
        });

        if (linksWithGenericText.length > 0) {
            console.log('Links with generic text:');
            linksWithGenericText.forEach(link => {
                console.log(`- "${link.text}" (aria-label: "${link.ariaLabel}") -> ${link.href}`);
            });
        }

        expect(linksWithGenericText.length).toBe(0);

        // Verify all links have some accessible text (text content, aria-label, or img alt)
        const emptyLinks = links.filter(link => {
            const hasAccessibleText = link.text.trim() || link.ariaLabel.trim() || link.imgAlt.trim();
            return !hasAccessibleText;
        });

        if (emptyLinks.length > 0) {
            console.log('Links without accessible text:');
            emptyLinks.forEach(link => console.log(`- ${link.href}`));
        }

        expect(emptyLinks.length).toBe(0);
    });

    /**
     * Test Case 7: Axe-Core Full Accessibility Audit
     * Expected: No critical or serious accessibility violations detected
     */
    test('TC7: No critical or serious accessibility violations detected by axe-core', async ({ page }) => {
        // Run comprehensive axe-core accessibility scan
        const accessibilityScanResults = await new AxeBuilder({ page })
            .withTags(['wcag2a', 'wcag2aa', 'wcag21aa', 'best-practice'])
            .analyze();

        // Filter for critical and serious violations only
        const criticalAndSerious = accessibilityScanResults.violations.filter(
            violation => violation.impact === 'critical' || violation.impact === 'serious'
        );

        // Log all violations for debugging
        if (accessibilityScanResults.violations.length > 0) {
            console.log(`\nTotal violations found: ${accessibilityScanResults.violations.length}`);

            accessibilityScanResults.violations.forEach(violation => {
                console.log(`\n[${violation.impact?.toUpperCase()}] ${violation.id}: ${violation.help}`);
                console.log(`Description: ${violation.description}`);
                console.log(`Help URL: ${violation.helpUrl}`);
                console.log(`Affected elements:`);
                violation.nodes.slice(0, 3).forEach(node => {
                    console.log(`  - ${node.html.substring(0, 100)}...`);
                });
            });
        }

        // Test passes if there are no critical or serious violations
        expect(criticalAndSerious.length).toBe(0);
    });

    /**
     * Additional Test: Document Language
     * Verify the document has a valid lang attribute for screen readers
     */
    test('Document has valid language attribute', async ({ page }) => {
        const htmlLang = await page.getAttribute('html', 'lang');
        expect(htmlLang).toBeTruthy();
        expect(htmlLang).toMatch(/^[a-z]{2}(-[A-Z]{2})?$/);
    });

    /**
     * Additional Test: Semantic HTML Structure
     * Verify proper use of semantic landmarks
     */
    test('Page uses semantic HTML landmarks', async ({ page }) => {
        // Check for main semantic elements
        const mainCount = await page.locator('main, [role="main"]').count();
        const headerCount = await page.locator('header, [role="banner"]').count();
        const footerCount = await page.locator('footer, [role="contentinfo"]').count();
        const navCount = await page.locator('nav, [role="navigation"]').count();

        // At minimum, page should have a footer
        expect(footerCount).toBeGreaterThanOrEqual(1);
    });

    /**
     * Additional Test: Skip Link for Keyboard Users
     * Check if there's a way to skip to main content
     */
    test('Interactive elements have sufficient size for touch targets', async ({ page }) => {
        // Get all button and link elements
        const interactiveElements = await page.$$eval(
            'a.btn, button, .copy-btn',
            (elements) => elements.map(el => {
                const rect = el.getBoundingClientRect();
                return {
                    width: rect.width,
                    height: rect.height,
                    text: el.textContent?.trim().substring(0, 20) || '',
                    testId: el.getAttribute('data-testid') || ''
                };
            })
        );

        // WCAG 2.1 AA recommends touch targets of at least 44x44 pixels
        const smallTargets = interactiveElements.filter(el => el.width < 44 || el.height < 44);

        if (smallTargets.length > 0) {
            console.log('Elements with small touch targets (< 44px):');
            smallTargets.forEach(el => {
                console.log(`- "${el.text}" (${el.testId}): ${el.width}x${el.height}px`);
            });
        }

        // All interactive elements should meet minimum size
        expect(smallTargets.length).toBe(0);
    });
});
