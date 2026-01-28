/**
 * Dark/Light Mode Theming Unit Tests
 * Owner: Scenario 11 - Dark/Light Mode Theming
 *
 * Tests for:
 * - CSS custom properties for theming (test case 1)
 * - Contrast in dark mode (test case 5)
 * - Theme variable definitions and values
 */

const fs = require('fs');
const path = require('path');

/**
 * Helper function to parse a hex color to RGB values
 * @param {string} hex - Hex color string (e.g., '#ffffff' or '#fff')
 * @returns {{r: number, g: number, b: number}} RGB values
 */
function hexToRgb(hex) {
    // Remove # if present
    hex = hex.replace(/^#/, '');

    // Handle shorthand hex (e.g., #fff -> #ffffff)
    if (hex.length === 3) {
        hex = hex.split('').map(char => char + char).join('');
    }

    const r = parseInt(hex.substring(0, 2), 16);
    const g = parseInt(hex.substring(2, 4), 16);
    const b = parseInt(hex.substring(4, 6), 16);

    return { r, g, b };
}

/**
 * Calculate relative luminance according to WCAG 2.1
 * @param {number} r - Red value (0-255)
 * @param {number} g - Green value (0-255)
 * @param {number} b - Blue value (0-255)
 * @returns {number} Relative luminance value (0-1)
 */
function calculateLuminance(r, g, b) {
    const sRGB = [r, g, b].map(value => {
        value /= 255;
        return value <= 0.03928 ? value / 12.92 : Math.pow((value + 0.055) / 1.055, 2.4);
    });
    return 0.2126 * sRGB[0] + 0.7152 * sRGB[1] + 0.0722 * sRGB[2];
}

/**
 * Calculate contrast ratio between two colors
 * @param {string} color1 - First hex color
 * @param {string} color2 - Second hex color
 * @returns {number} Contrast ratio (1-21)
 */
function calculateContrastRatio(color1, color2) {
    const rgb1 = hexToRgb(color1);
    const rgb2 = hexToRgb(color2);

    const lum1 = calculateLuminance(rgb1.r, rgb1.g, rgb1.b);
    const lum2 = calculateLuminance(rgb2.r, rgb2.g, rgb2.b);

    const lighter = Math.max(lum1, lum2);
    const darker = Math.min(lum1, lum2);

    return (lighter + 0.05) / (darker + 0.05);
}

/**
 * Extract CSS variable value from CSS content
 * @param {string} cssContent - Full CSS content
 * @param {string} variableName - Variable name without --
 * @param {boolean} inDarkMode - Whether to extract from dark mode section
 * @returns {string|null} Variable value or null if not found
 */
function extractCssVariableValue(cssContent, variableName, inDarkMode = false) {
    if (inDarkMode) {
        // Extract from dark mode media query
        const darkModeMatch = cssContent.match(
            /@media\s*\(\s*prefers-color-scheme\s*:\s*dark\s*\)\s*\{[\s\S]*?:root\s*\{([\s\S]*?)\}/
        );
        if (darkModeMatch) {
            const darkModeContent = darkModeMatch[1];
            const varMatch = darkModeContent.match(new RegExp(`--${variableName}\\s*:\\s*([^;]+);`));
            if (varMatch) return varMatch[1].trim();
        }
        return null;
    }

    // Extract from root (light mode)
    const rootMatch = cssContent.match(/:root\s*\{([\s\S]*?)\}/);
    if (rootMatch) {
        const rootContent = rootMatch[1];
        const varMatch = rootContent.match(new RegExp(`--${variableName}\\s*:\\s*([^;]+);`));
        if (varMatch) return varMatch[1].trim();
    }
    return null;
}

describe('Theming CSS Custom Properties', () => {
    let cssContent;

    beforeAll(() => {
        const cssPath = path.join(__dirname, '../../styles/main.css');
        cssContent = fs.readFileSync(cssPath, 'utf8');
    });

    describe('TC1: CSS custom properties for theming', () => {
        describe('Light mode (default) theme variables', () => {
            test('defines color-primary variable', () => {
                const value = extractCssVariableValue(cssContent, 'color-primary');
                expect(value).not.toBeNull();
                expect(value).toMatch(/^#[0-9a-fA-F]{3,6}$/);
            });

            test('defines color-background variable', () => {
                const value = extractCssVariableValue(cssContent, 'color-background');
                expect(value).not.toBeNull();
                expect(value).toMatch(/^#[0-9a-fA-F]{3,6}$/);
            });

            test('defines color-surface variable', () => {
                const value = extractCssVariableValue(cssContent, 'color-surface');
                expect(value).not.toBeNull();
                expect(value).toMatch(/^#[0-9a-fA-F]{3,6}$/);
            });

            test('defines color-text variable', () => {
                const value = extractCssVariableValue(cssContent, 'color-text');
                expect(value).not.toBeNull();
                expect(value).toMatch(/^#[0-9a-fA-F]{3,6}$/);
            });

            test('defines color-text-muted variable', () => {
                const value = extractCssVariableValue(cssContent, 'color-text-muted');
                expect(value).not.toBeNull();
                expect(value).toMatch(/^#[0-9a-fA-F]{3,6}$/);
            });

            test('defines color-border variable', () => {
                const value = extractCssVariableValue(cssContent, 'color-border');
                expect(value).not.toBeNull();
                expect(value).toMatch(/^#[0-9a-fA-F]{3,6}$/);
            });

            test('light mode background is light (high luminance)', () => {
                const bgValue = extractCssVariableValue(cssContent, 'color-background');
                expect(bgValue).not.toBeNull();

                const rgb = hexToRgb(bgValue);
                const luminance = calculateLuminance(rgb.r, rgb.g, rgb.b);

                // Light background should have high luminance (> 0.5)
                expect(luminance).toBeGreaterThan(0.5);
            });

            test('light mode text is dark (low luminance)', () => {
                const textValue = extractCssVariableValue(cssContent, 'color-text');
                expect(textValue).not.toBeNull();

                const rgb = hexToRgb(textValue);
                const luminance = calculateLuminance(rgb.r, rgb.g, rgb.b);

                // Dark text should have low luminance (< 0.2)
                expect(luminance).toBeLessThan(0.2);
            });
        });

        describe('Dark mode theme variables', () => {
            test('dark mode media query is defined', () => {
                expect(cssContent).toMatch(/@media\s*\(\s*prefers-color-scheme\s*:\s*dark\s*\)/);
            });

            test('dark mode defines color-primary variable', () => {
                const value = extractCssVariableValue(cssContent, 'color-primary', true);
                expect(value).not.toBeNull();
                expect(value).toMatch(/^#[0-9a-fA-F]{3,6}$/);
            });

            test('dark mode defines color-background variable', () => {
                const value = extractCssVariableValue(cssContent, 'color-background', true);
                expect(value).not.toBeNull();
                expect(value).toMatch(/^#[0-9a-fA-F]{3,6}$/);
            });

            test('dark mode defines color-surface variable', () => {
                const value = extractCssVariableValue(cssContent, 'color-surface', true);
                expect(value).not.toBeNull();
                expect(value).toMatch(/^#[0-9a-fA-F]{3,6}$/);
            });

            test('dark mode defines color-text variable', () => {
                const value = extractCssVariableValue(cssContent, 'color-text', true);
                expect(value).not.toBeNull();
                expect(value).toMatch(/^#[0-9a-fA-F]{3,6}$/);
            });

            test('dark mode defines color-text-muted variable', () => {
                const value = extractCssVariableValue(cssContent, 'color-text-muted', true);
                expect(value).not.toBeNull();
                expect(value).toMatch(/^#[0-9a-fA-F]{3,6}$/);
            });

            test('dark mode defines color-border variable', () => {
                const value = extractCssVariableValue(cssContent, 'color-border', true);
                expect(value).not.toBeNull();
                expect(value).toMatch(/^#[0-9a-fA-F]{3,6}$/);
            });

            test('dark mode background is dark (low luminance)', () => {
                const bgValue = extractCssVariableValue(cssContent, 'color-background', true);
                expect(bgValue).not.toBeNull();

                const rgb = hexToRgb(bgValue);
                const luminance = calculateLuminance(rgb.r, rgb.g, rgb.b);

                // Dark background should have low luminance (< 0.1)
                expect(luminance).toBeLessThan(0.1);
            });

            test('dark mode text is light (high luminance)', () => {
                const textValue = extractCssVariableValue(cssContent, 'color-text', true);
                expect(textValue).not.toBeNull();

                const rgb = hexToRgb(textValue);
                const luminance = calculateLuminance(rgb.r, rgb.g, rgb.b);

                // Light text should have high luminance (> 0.7)
                expect(luminance).toBeGreaterThan(0.7);
            });
        });

        describe('Theme variables are used in CSS', () => {
            test('body uses color-background variable', () => {
                expect(cssContent).toMatch(/body[\s\S]*?background-color:\s*var\(--color-background\)/);
            });

            test('body uses color-text variable', () => {
                expect(cssContent).toMatch(/body[\s\S]*?color:\s*var\(--color-text\)/);
            });

            test('surface color is used for sections', () => {
                expect(cssContent).toMatch(/background-color:\s*var\(--color-surface\)/);
            });

            test('border color is used', () => {
                expect(cssContent).toMatch(/border[\s\S]*?var\(--color-border\)/);
            });

            test('primary color is used for interactive elements', () => {
                expect(cssContent).toMatch(/background-color:\s*var\(--color-primary\)/);
            });

            test('text muted color is used for secondary text', () => {
                expect(cssContent).toMatch(/color:\s*var\(--color-text-muted\)/);
            });
        });
    });

    describe('TC5: Contrast in dark mode', () => {
        describe('WCAG AA compliance (4.5:1 for normal text)', () => {
            test('dark mode text on background has sufficient contrast ratio', () => {
                const bgColor = extractCssVariableValue(cssContent, 'color-background', true);
                const textColor = extractCssVariableValue(cssContent, 'color-text', true);

                expect(bgColor).not.toBeNull();
                expect(textColor).not.toBeNull();

                const contrastRatio = calculateContrastRatio(bgColor, textColor);

                // WCAG AA requires at least 4.5:1 for normal text
                expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
            });

            test('dark mode muted text on background has sufficient contrast ratio', () => {
                const bgColor = extractCssVariableValue(cssContent, 'color-background', true);
                const mutedTextColor = extractCssVariableValue(cssContent, 'color-text-muted', true);

                expect(bgColor).not.toBeNull();
                expect(mutedTextColor).not.toBeNull();

                const contrastRatio = calculateContrastRatio(bgColor, mutedTextColor);

                // WCAG AA requires at least 4.5:1 for normal text
                // Muted text should still meet this threshold
                expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
            });

            test('dark mode text on surface has sufficient contrast ratio', () => {
                const surfaceColor = extractCssVariableValue(cssContent, 'color-surface', true);
                const textColor = extractCssVariableValue(cssContent, 'color-text', true);

                expect(surfaceColor).not.toBeNull();
                expect(textColor).not.toBeNull();

                const contrastRatio = calculateContrastRatio(surfaceColor, textColor);

                // WCAG AA requires at least 4.5:1 for normal text
                expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
            });
        });

        describe('WCAG AAA compliance (7:1 for enhanced)', () => {
            test('dark mode main text contrast exceeds AAA threshold', () => {
                const bgColor = extractCssVariableValue(cssContent, 'color-background', true);
                const textColor = extractCssVariableValue(cssContent, 'color-text', true);

                expect(bgColor).not.toBeNull();
                expect(textColor).not.toBeNull();

                const contrastRatio = calculateContrastRatio(bgColor, textColor);

                // WCAG AAA requires at least 7:1 for normal text (enhanced)
                expect(contrastRatio).toBeGreaterThanOrEqual(7);
            });
        });

        describe('Light mode contrast verification', () => {
            test('light mode text on background has sufficient contrast ratio', () => {
                const bgColor = extractCssVariableValue(cssContent, 'color-background');
                const textColor = extractCssVariableValue(cssContent, 'color-text');

                expect(bgColor).not.toBeNull();
                expect(textColor).not.toBeNull();

                const contrastRatio = calculateContrastRatio(bgColor, textColor);

                // WCAG AA requires at least 4.5:1 for normal text
                expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
            });

            test('light mode muted text on background has sufficient contrast ratio', () => {
                const bgColor = extractCssVariableValue(cssContent, 'color-background');
                const mutedTextColor = extractCssVariableValue(cssContent, 'color-text-muted');

                expect(bgColor).not.toBeNull();
                expect(mutedTextColor).not.toBeNull();

                const contrastRatio = calculateContrastRatio(bgColor, mutedTextColor);

                // WCAG AA requires at least 4.5:1 for normal text
                expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
            });
        });
    });
});
