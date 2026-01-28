/**
 * CSS Custom Properties (Variables) Unit Tests
 * Owner: Scenario 10 - Cross-Browser Compatibility
 *
 * Tests for:
 * - CSS custom properties are defined in the stylesheet
 * - All theme variables are properly structured
 * - CSS variables follow correct syntax
 *
 * This unit test validates the CSS file structure itself,
 * while E2E tests verify browser rendering.
 */

const fs = require('fs');
const path = require('path');

describe('CSS Custom Properties Support', () => {
    let cssContent;

    beforeAll(() => {
        const cssPath = path.join(__dirname, '../../styles/main.css');
        cssContent = fs.readFileSync(cssPath, 'utf8');
    });

    describe('TC5: CSS Variables Definition', () => {
        test('CSS file contains :root selector with CSS variables', () => {
            expect(cssContent).toMatch(/:root\s*\{/);
        });

        test('Primary color variable is defined', () => {
            expect(cssContent).toMatch(/--color-primary\s*:/);
        });

        test('Primary dark color variable is defined', () => {
            expect(cssContent).toMatch(/--color-primary-dark\s*:/);
        });

        test('Background color variable is defined', () => {
            expect(cssContent).toMatch(/--color-background\s*:/);
        });

        test('Surface color variable is defined', () => {
            expect(cssContent).toMatch(/--color-surface\s*:/);
        });

        test('Text color variable is defined', () => {
            expect(cssContent).toMatch(/--color-text\s*:/);
        });

        test('Text muted color variable is defined', () => {
            expect(cssContent).toMatch(/--color-text-muted\s*:/);
        });

        test('Border color variable is defined', () => {
            expect(cssContent).toMatch(/--color-border\s*:/);
        });

        test('Border radius variables are defined', () => {
            expect(cssContent).toMatch(/--radius-sm\s*:/);
            expect(cssContent).toMatch(/--radius-md\s*:/);
            expect(cssContent).toMatch(/--radius-lg\s*:/);
        });

        test('Shadow variables are defined', () => {
            expect(cssContent).toMatch(/--shadow-sm\s*:/);
            expect(cssContent).toMatch(/--shadow-md\s*:/);
        });

        test('Font family variable is defined', () => {
            expect(cssContent).toMatch(/--font-family\s*:/);
        });

        test('Font mono variable is defined', () => {
            expect(cssContent).toMatch(/--font-mono\s*:/);
        });

        test('Max width variable is defined', () => {
            expect(cssContent).toMatch(/--max-width\s*:/);
        });
    });

    describe('TC5: CSS Variables Usage', () => {
        test('CSS variables are used in body styles', () => {
            // Body should use CSS variable for background
            expect(cssContent).toMatch(/background-color:\s*var\(--color-background\)/);
        });

        test('CSS variables are used for text colors', () => {
            expect(cssContent).toMatch(/color:\s*var\(--color-text\)/);
        });

        test('CSS variables are used for border radius', () => {
            expect(cssContent).toMatch(/border-radius:\s*var\(--radius/);
        });

        test('CSS variables are used for links', () => {
            expect(cssContent).toMatch(/color:\s*var\(--color-primary\)/);
        });
    });

    describe('TC5: Dark Mode Support', () => {
        test('prefers-color-scheme dark media query is defined', () => {
            expect(cssContent).toMatch(/@media\s*\(\s*prefers-color-scheme\s*:\s*dark\s*\)/);
        });

        test('Dark mode overrides CSS variables', () => {
            // Check that dark mode section contains variable overrides
            const darkModeMatch = cssContent.match(/@media\s*\(\s*prefers-color-scheme\s*:\s*dark\s*\)\s*\{[^}]+:root\s*\{([^}]+)\}/s);
            expect(darkModeMatch).not.toBeNull();

            if (darkModeMatch) {
                const darkModeVars = darkModeMatch[1];
                expect(darkModeVars).toMatch(/--color-primary\s*:/);
                expect(darkModeVars).toMatch(/--color-background\s*:/);
                expect(darkModeVars).toMatch(/--color-text\s*:/);
            }
        });
    });

    describe('TC5: CSS Variables Syntax Validation', () => {
        test('All CSS variable declarations have valid syntax', () => {
            // Extract all CSS variable declarations
            const variablePattern = /--[\w-]+\s*:\s*[^;]+;/g;
            const variables = cssContent.match(variablePattern);

            expect(variables).not.toBeNull();
            expect(variables.length).toBeGreaterThan(10);

            // Each variable should have a name and value
            variables.forEach(variable => {
                expect(variable).toMatch(/^--[\w-]+\s*:\s*.+;$/);
            });
        });

        test('CSS variable usage has valid var() syntax', () => {
            // Check that var() usages are valid
            const varUsagePattern = /var\(--[\w-]+\)/g;
            const usages = cssContent.match(varUsagePattern);

            expect(usages).not.toBeNull();
            expect(usages.length).toBeGreaterThan(20);

            // Each usage should reference a variable name
            usages.forEach(usage => {
                expect(usage).toMatch(/^var\(--[\w-]+\)$/);
            });
        });
    });

    describe('TC5: Cross-Browser Compatible CSS', () => {
        test('Uses standard CSS custom properties syntax', () => {
            // CSS custom properties should use double-dash prefix
            expect(cssContent).toMatch(/--[\w-]+\s*:/);
        });

        test('Uses var() function for variable references', () => {
            expect(cssContent).toMatch(/var\(--[\w-]+\)/);
        });

        test('Does not use vendor-prefixed custom properties', () => {
            // Should not use -webkit-, -moz-, etc. for CSS variables
            // as they are not needed (CSS custom properties are standardized)
            expect(cssContent).not.toMatch(/-webkit-\s*--/);
            expect(cssContent).not.toMatch(/-moz-\s*--/);
            expect(cssContent).not.toMatch(/-ms-\s*--/);
        });

        test('Uses box-sizing border-box for consistent layout', () => {
            expect(cssContent).toMatch(/box-sizing:\s*border-box/);
        });

        test('Uses standard flexbox properties', () => {
            expect(cssContent).toMatch(/display:\s*flex/);
        });

        test('Uses standard grid properties', () => {
            expect(cssContent).toMatch(/display:\s*grid/);
        });
    });
});
