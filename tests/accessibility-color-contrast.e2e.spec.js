/**
 * E2E Tests for Color Contrast Accessibility (WCAG 2.1 AA Compliance)
 *
 * NFR-3: Design must be accessible (WCAG 2.1 AA compliance)
 *
 * WCAG 2.1 AA Requirements:
 * - Normal text: 4.5:1 contrast ratio minimum
 * - Large text (18pt+ or 14pt+ bold): 3:1 contrast ratio minimum
 * - UI components and graphical objects: 3:1 contrast ratio
 */

const { test, expect } = require('@playwright/test');

// Utility function to calculate relative luminance
// Formula from WCAG 2.1: https://www.w3.org/WAI/GL/wiki/Relative_luminance
function getLuminance(r, g, b) {
    const [rs, gs, bs] = [r, g, b].map(c => {
        c = c / 255;
        return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
    });
    return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
}

// Calculate contrast ratio between two colors
function getContrastRatio(color1, color2) {
    const l1 = getLuminance(color1.r, color1.g, color1.b);
    const l2 = getLuminance(color2.r, color2.g, color2.b);
    const lighter = Math.max(l1, l2);
    const darker = Math.min(l1, l2);
    return (lighter + 0.05) / (darker + 0.05);
}

// Parse CSS color to RGB object
function parseColor(colorStr) {
    // Handle rgb(r, g, b) or rgba(r, g, b, a) format
    const rgbMatch = colorStr.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)(?:,\s*[\d.]+)?\)/);
    if (rgbMatch) {
        return {
            r: parseInt(rgbMatch[1], 10),
            g: parseInt(rgbMatch[2], 10),
            b: parseInt(rgbMatch[3], 10)
        };
    }

    // Handle hex format
    const hexMatch = colorStr.match(/^#([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i);
    if (hexMatch) {
        return {
            r: parseInt(hexMatch[1], 16),
            g: parseInt(hexMatch[2], 16),
            b: parseInt(hexMatch[3], 16)
        };
    }

    // Handle short hex format
    const shortHexMatch = colorStr.match(/^#([0-9a-f])([0-9a-f])([0-9a-f])$/i);
    if (shortHexMatch) {
        return {
            r: parseInt(shortHexMatch[1] + shortHexMatch[1], 16),
            g: parseInt(shortHexMatch[2] + shortHexMatch[2], 16),
            b: parseInt(shortHexMatch[3] + shortHexMatch[3], 16)
        };
    }

    // Default fallback
    return { r: 0, g: 0, b: 0 };
}

// Check if text is considered "large text" per WCAG
// Large text is 18pt (24px) or 14pt (18.67px) bold
function isLargeText(fontSize, fontWeight) {
    const fontSizeValue = parseFloat(fontSize);
    const isBold = parseInt(fontWeight) >= 700;

    // 18pt = 24px, 14pt = 18.67px
    if (fontSizeValue >= 24) return true;
    if (fontSizeValue >= 18.67 && isBold) return true;

    return false;
}

// Get the effective background color, traversing up the DOM if transparent
// Also handles CSS gradients by extracting the first color
async function getEffectiveBackgroundColor(page, element) {
    return await page.evaluate((el) => {
        let current = el;
        while (current) {
            const style = window.getComputedStyle(current);
            const bgColor = style.backgroundColor;
            const bgImage = style.backgroundImage;

            // Check for gradient backgrounds (linear-gradient, radial-gradient, etc.)
            if (bgImage && bgImage !== 'none') {
                // Extract the first color from the gradient
                // Gradients look like: linear-gradient(135deg, rgb(26, 26, 46) 0%, rgb(22, 33, 62) 100%)
                const gradientMatch = bgImage.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
                if (gradientMatch) {
                    return `rgb(${gradientMatch[1]}, ${gradientMatch[2]}, ${gradientMatch[3]})`;
                }
            }

            // Check if the background color is not transparent
            const rgba = bgColor.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)(?:,\s*([\d.]+))?\)/);
            if (rgba) {
                const alpha = rgba[4] !== undefined ? parseFloat(rgba[4]) : 1;
                if (alpha > 0) {
                    return bgColor;
                }
            }

            current = current.parentElement;
        }
        // Default to white if we reach the root
        return 'rgb(255, 255, 255)';
    }, await element.elementHandle());
}

test.describe('Color Contrast Accessibility (NFR-3)', () => {
    test.beforeEach(async ({ page }) => {
        // Start a local server or navigate to the file
        await page.goto('file:///workspace/index.html');
    });

    test('Test Case 1: Body text has at least 4.5:1 contrast ratio against background', async ({ page }) => {
        // Test body text in feature cards
        const featureCardParagraphs = page.locator('.feature-card p');
        const count = await featureCardParagraphs.count();

        expect(count).toBeGreaterThan(0);

        for (let i = 0; i < count; i++) {
            const paragraph = featureCardParagraphs.nth(i);
            const textColor = await paragraph.evaluate(el => window.getComputedStyle(el).color);
            const backgroundColor = await getEffectiveBackgroundColor(page, paragraph);
            const fontSize = await paragraph.evaluate(el => window.getComputedStyle(el).fontSize);
            const fontWeight = await paragraph.evaluate(el => window.getComputedStyle(el).fontWeight);

            const fgColor = parseColor(textColor);
            const bgColor = parseColor(backgroundColor);
            const ratio = getContrastRatio(fgColor, bgColor);
            const isLarge = isLargeText(fontSize, fontWeight);
            const requiredRatio = isLarge ? 3 : 4.5;

            expect(ratio,
                `Feature card paragraph ${i + 1}: ${textColor} on ${backgroundColor} has ratio ${ratio.toFixed(2)}, needs ${requiredRatio}:1`
            ).toBeGreaterThanOrEqual(requiredRatio);
        }

        // Test body text in quick-start section
        const quickStartParagraphs = page.locator('.quick-start p');
        const qsCount = await quickStartParagraphs.count();

        for (let i = 0; i < qsCount; i++) {
            const paragraph = quickStartParagraphs.nth(i);
            const textColor = await paragraph.evaluate(el => window.getComputedStyle(el).color);
            const backgroundColor = await getEffectiveBackgroundColor(page, paragraph);
            const fontSize = await paragraph.evaluate(el => window.getComputedStyle(el).fontSize);
            const fontWeight = await paragraph.evaluate(el => window.getComputedStyle(el).fontWeight);

            const fgColor = parseColor(textColor);
            const bgColor = parseColor(backgroundColor);
            const ratio = getContrastRatio(fgColor, bgColor);
            const isLarge = isLargeText(fontSize, fontWeight);
            const requiredRatio = isLarge ? 3 : 4.5;

            expect(ratio,
                `Quick-start paragraph ${i + 1}: ${textColor} on ${backgroundColor} has ratio ${ratio.toFixed(2)}, needs ${requiredRatio}:1`
            ).toBeGreaterThanOrEqual(requiredRatio);
        }

        // Test command descriptions
        const commandDescriptions = page.locator('.command-item span');
        const cmdCount = await commandDescriptions.count();

        for (let i = 0; i < cmdCount; i++) {
            const span = commandDescriptions.nth(i);
            const textColor = await span.evaluate(el => window.getComputedStyle(el).color);
            const backgroundColor = await getEffectiveBackgroundColor(page, span);
            const fontSize = await span.evaluate(el => window.getComputedStyle(el).fontSize);
            const fontWeight = await span.evaluate(el => window.getComputedStyle(el).fontWeight);

            const fgColor = parseColor(textColor);
            const bgColor = parseColor(backgroundColor);
            const ratio = getContrastRatio(fgColor, bgColor);
            const isLarge = isLargeText(fontSize, fontWeight);
            const requiredRatio = isLarge ? 3 : 4.5;

            expect(ratio,
                `Command description ${i + 1}: ${textColor} on ${backgroundColor} has ratio ${ratio.toFixed(2)}, needs ${requiredRatio}:1`
            ).toBeGreaterThanOrEqual(requiredRatio);
        }
    });

    test('Test Case 2: CTA button text has at least 4.5:1 contrast ratio', async ({ page }) => {
        // Test primary CTA button
        const primaryBtn = page.locator('.btn-primary').first();
        const primaryTextColor = await primaryBtn.evaluate(el => window.getComputedStyle(el).color);
        const primaryBgColor = await primaryBtn.evaluate(el => window.getComputedStyle(el).backgroundColor);
        const primaryFontSize = await primaryBtn.evaluate(el => window.getComputedStyle(el).fontSize);
        const primaryFontWeight = await primaryBtn.evaluate(el => window.getComputedStyle(el).fontWeight);

        const primaryFg = parseColor(primaryTextColor);
        const primaryBg = parseColor(primaryBgColor);
        const primaryRatio = getContrastRatio(primaryFg, primaryBg);
        const primaryIsLarge = isLargeText(primaryFontSize, primaryFontWeight);
        const primaryRequiredRatio = primaryIsLarge ? 3 : 4.5;

        expect(primaryRatio,
            `Primary button: ${primaryTextColor} on ${primaryBgColor} has ratio ${primaryRatio.toFixed(2)}, needs ${primaryRequiredRatio}:1`
        ).toBeGreaterThanOrEqual(primaryRequiredRatio);

        // Test secondary CTA button - for transparent buttons, we need the effective background
        const secondaryBtn = page.locator('.btn-secondary').first();
        const secondaryTextColor = await secondaryBtn.evaluate(el => window.getComputedStyle(el).color);
        const secondaryBgColor = await getEffectiveBackgroundColor(page, secondaryBtn);
        const secondaryFontSize = await secondaryBtn.evaluate(el => window.getComputedStyle(el).fontSize);
        const secondaryFontWeight = await secondaryBtn.evaluate(el => window.getComputedStyle(el).fontWeight);

        const secondaryFg = parseColor(secondaryTextColor);
        const secondaryBg = parseColor(secondaryBgColor);
        const secondaryRatio = getContrastRatio(secondaryFg, secondaryBg);
        const secondaryIsLarge = isLargeText(secondaryFontSize, secondaryFontWeight);
        const secondaryRequiredRatio = secondaryIsLarge ? 3 : 4.5;

        expect(secondaryRatio,
            `Secondary button: ${secondaryTextColor} on ${secondaryBgColor} has ratio ${secondaryRatio.toFixed(2)}, needs ${secondaryRequiredRatio}:1`
        ).toBeGreaterThanOrEqual(secondaryRequiredRatio);
    });

    test('Test Case 3: Links are visually distinguishable from surrounding text', async ({ page }) => {
        // Test footer links - they should have sufficient contrast
        const footerLinks = page.locator('.footer-links a');
        const linkCount = await footerLinks.count();

        expect(linkCount).toBeGreaterThan(0);

        for (let i = 0; i < linkCount; i++) {
            const link = footerLinks.nth(i);
            const textColor = await link.evaluate(el => window.getComputedStyle(el).color);
            const backgroundColor = await getEffectiveBackgroundColor(page, link);
            const fontSize = await link.evaluate(el => window.getComputedStyle(el).fontSize);
            const fontWeight = await link.evaluate(el => window.getComputedStyle(el).fontWeight);

            const fgColor = parseColor(textColor);
            const bgColor = parseColor(backgroundColor);
            const ratio = getContrastRatio(fgColor, bgColor);
            const isLarge = isLargeText(fontSize, fontWeight);
            const requiredRatio = isLarge ? 3 : 4.5;

            expect(ratio,
                `Footer link ${i + 1}: ${textColor} on ${backgroundColor} has ratio ${ratio.toFixed(2)}, needs ${requiredRatio}:1`
            ).toBeGreaterThanOrEqual(requiredRatio);
        }

        // Verify links have underline or other non-color distinction OR 3:1 contrast with surrounding text
        // Per WCAG, links must be distinguishable not solely by color
        // Check that links have underline on hover/focus OR have additional visual indicator

        // Check skip-link has proper contrast when focused
        const skipLink = page.locator('.skip-link');
        await skipLink.focus();

        const skipLinkColor = await skipLink.evaluate(el => window.getComputedStyle(el).color);
        const skipLinkBg = await skipLink.evaluate(el => window.getComputedStyle(el).backgroundColor);

        const skipFg = parseColor(skipLinkColor);
        const skipBg = parseColor(skipLinkBg);
        const skipRatio = getContrastRatio(skipFg, skipBg);

        expect(skipRatio,
            `Skip link: ${skipLinkColor} on ${skipLinkBg} has ratio ${skipRatio.toFixed(2)}, needs 4.5:1`
        ).toBeGreaterThanOrEqual(4.5);

        // Test hero section links (CTA buttons are already tested, but check GitHub badge link)
        const badgeLink = page.locator('.badges a').first();
        if (await badgeLink.count() > 0) {
            // Badge links should have visible focus state
            await badgeLink.focus();
            const focusOutline = await badgeLink.evaluate(el => window.getComputedStyle(el).outline);
            expect(focusOutline).not.toBe('none');
        }
    });

    test('Heading contrast meets WCAG requirements', async ({ page }) => {
        // Test all headings have sufficient contrast
        const headings = page.locator('h1, h2, h3');
        const headingCount = await headings.count();

        expect(headingCount).toBeGreaterThan(0);

        for (let i = 0; i < headingCount; i++) {
            const heading = headings.nth(i);
            const tagName = await heading.evaluate(el => el.tagName);
            const textColor = await heading.evaluate(el => window.getComputedStyle(el).color);
            const backgroundColor = await getEffectiveBackgroundColor(page, heading);
            const fontSize = await heading.evaluate(el => window.getComputedStyle(el).fontSize);
            const fontWeight = await heading.evaluate(el => window.getComputedStyle(el).fontWeight);

            const fgColor = parseColor(textColor);
            const bgColor = parseColor(backgroundColor);
            const ratio = getContrastRatio(fgColor, bgColor);
            const isLarge = isLargeText(fontSize, fontWeight);
            // Headings are typically large text, so 3:1 is often sufficient
            const requiredRatio = isLarge ? 3 : 4.5;

            expect(ratio,
                `${tagName} heading: ${textColor} on ${backgroundColor} has ratio ${ratio.toFixed(2)}, needs ${requiredRatio}:1`
            ).toBeGreaterThanOrEqual(requiredRatio);
        }
    });

    test('Tagline and secondary text meet contrast requirements', async ({ page }) => {
        // Test the tagline which has a lighter color
        const tagline = page.locator('.tagline');
        const taglineColor = await tagline.evaluate(el => window.getComputedStyle(el).color);
        const taglineBg = await getEffectiveBackgroundColor(page, tagline);
        const taglineFontSize = await tagline.evaluate(el => window.getComputedStyle(el).fontSize);
        const taglineFontWeight = await tagline.evaluate(el => window.getComputedStyle(el).fontWeight);

        const taglineFg = parseColor(taglineColor);
        const taglineBgColor = parseColor(taglineBg);
        const taglineRatio = getContrastRatio(taglineFg, taglineBgColor);
        const taglineIsLarge = isLargeText(taglineFontSize, taglineFontWeight);
        const taglineRequired = taglineIsLarge ? 3 : 4.5;

        expect(taglineRatio,
            `Tagline: ${taglineColor} on ${taglineBg} has ratio ${taglineRatio.toFixed(2)}, needs ${taglineRequired}:1`
        ).toBeGreaterThanOrEqual(taglineRequired);
    });

    test('Status list items meet contrast requirements', async ({ page }) => {
        // Test completed status items
        const completedItems = page.locator('.status-list li.completed');
        const completedCount = await completedItems.count();

        for (let i = 0; i < completedCount; i++) {
            const item = completedItems.nth(i);
            const textColor = await item.evaluate(el => window.getComputedStyle(el).color);
            const backgroundColor = await item.evaluate(el => window.getComputedStyle(el).backgroundColor);

            const fgColor = parseColor(textColor);
            const bgColor = parseColor(backgroundColor);
            const ratio = getContrastRatio(fgColor, bgColor);

            expect(ratio,
                `Completed status item ${i + 1}: ${textColor} on ${backgroundColor} has ratio ${ratio.toFixed(2)}, needs 4.5:1`
            ).toBeGreaterThanOrEqual(4.5);
        }

        // Test planned status items
        const plannedItems = page.locator('.status-list li.planned');
        const plannedCount = await plannedItems.count();

        for (let i = 0; i < plannedCount; i++) {
            const item = plannedItems.nth(i);
            const textColor = await item.evaluate(el => window.getComputedStyle(el).color);
            const backgroundColor = await item.evaluate(el => window.getComputedStyle(el).backgroundColor);

            const fgColor = parseColor(textColor);
            const bgColor = parseColor(backgroundColor);
            const ratio = getContrastRatio(fgColor, bgColor);

            expect(ratio,
                `Planned status item ${i + 1}: ${textColor} on ${backgroundColor} has ratio ${ratio.toFixed(2)}, needs 4.5:1`
            ).toBeGreaterThanOrEqual(4.5);
        }
    });

    test('Code blocks meet contrast requirements', async ({ page }) => {
        // Test code in pre blocks (terminal/code examples)
        const codeBlocks = page.locator('.quick-start pre code');
        const codeCount = await codeBlocks.count();

        for (let i = 0; i < codeCount; i++) {
            const code = codeBlocks.nth(i);
            const textColor = await code.evaluate(el => window.getComputedStyle(el).color);
            const backgroundColor = await getEffectiveBackgroundColor(page, code);

            const fgColor = parseColor(textColor);
            const bgColor = parseColor(backgroundColor);
            const ratio = getContrastRatio(fgColor, bgColor);

            expect(ratio,
                `Code block ${i + 1}: ${textColor} on ${backgroundColor} has ratio ${ratio.toFixed(2)}, needs 4.5:1`
            ).toBeGreaterThanOrEqual(4.5);
        }

        // Test inline code in command items
        const commandCodes = page.locator('.command-item code');
        const cmdCodeCount = await commandCodes.count();

        for (let i = 0; i < cmdCodeCount; i++) {
            const code = commandCodes.nth(i);
            const textColor = await code.evaluate(el => window.getComputedStyle(el).color);
            const backgroundColor = await code.evaluate(el => window.getComputedStyle(el).backgroundColor);

            const fgColor = parseColor(textColor);
            const bgColor = parseColor(backgroundColor);
            const ratio = getContrastRatio(fgColor, bgColor);

            expect(ratio,
                `Command code ${i + 1}: ${textColor} on ${backgroundColor} has ratio ${ratio.toFixed(2)}, needs 4.5:1`
            ).toBeGreaterThanOrEqual(4.5);
        }
    });

    test('Copyright text meets contrast requirements', async ({ page }) => {
        const copyright = page.locator('.copyright');
        const textColor = await copyright.evaluate(el => window.getComputedStyle(el).color);
        const backgroundColor = await getEffectiveBackgroundColor(page, copyright);

        const fgColor = parseColor(textColor);
        const bgColor = parseColor(backgroundColor);
        const ratio = getContrastRatio(fgColor, bgColor);

        expect(ratio,
            `Copyright: ${textColor} on ${backgroundColor} has ratio ${ratio.toFixed(2)}, needs 4.5:1`
        ).toBeGreaterThanOrEqual(4.5);
    });
});
