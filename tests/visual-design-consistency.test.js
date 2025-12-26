/**
 * Visual Design Consistency Tests (NFR-6)
 *
 * Verifies design is consistent with developer tool aesthetics.
 * Tests for:
 * 1. Monospace fonts for code elements
 * 2. Consistent spacing scale
 * 3. Limited and consistent color palette
 * 4. Dark/muted color scheme for professional developer aesthetic
 */

describe('Visual Design Consistency (NFR-6)', () => {
    let cssContent;
    let htmlContent;

    beforeAll(() => {
        const fs = require('fs');
        const path = require('path');
        const cssPath = path.join(__dirname, '..', 'css', 'styles.css');
        const htmlPath = path.join(__dirname, '..', 'index.html');
        cssContent = fs.readFileSync(cssPath, 'utf-8');
        htmlContent = fs.readFileSync(htmlPath, 'utf-8');
    });

    describe('Test Case 1: Code blocks use monospace font', () => {
        test('monospace font variable is defined', () => {
            expect(cssContent).toMatch(/--font-mono:/);
        });

        test('monospace font variable contains "monospace" generic fallback', () => {
            const fontMonoMatch = cssContent.match(/--font-mono:\s*([^;]+);/);
            expect(fontMonoMatch).not.toBeNull();
            expect(fontMonoMatch[1].toLowerCase()).toContain('monospace');
        });

        test('code elements use monospace font-family variable', () => {
            // Check that code elements reference the monospace font variable
            expect(cssContent).toMatch(/code\s*\{[^}]*font-family:\s*var\(--font-mono\)/s);
        });

        test('step code blocks have monospace font', () => {
            // .step code should use monospace
            expect(cssContent).toMatch(/\.step\s+code\s*\{[^}]*font-family:\s*var\(--font-mono\)/s);
        });

        test('command card code elements use monospace font', () => {
            // .command-card code should use monospace
            expect(cssContent).toMatch(/\.command-card\s+code\s*\{[^}]*font-family:\s*var\(--font-mono\)/s);
        });

        test('specs table code elements use monospace font', () => {
            // .specs-table code should use monospace
            expect(cssContent).toMatch(/\.specs-table\s+code\s*\{[^}]*font-family:\s*var\(--font-mono\)/s);
        });

        test('HTML code elements exist for commands and configuration', () => {
            // Verify code tags are used in the HTML
            expect(htmlContent).toMatch(/<code[^>]*>/);
            expect(htmlContent).toMatch(/<pre[^>]*>/);
        });

        test('monospace font stack includes professional code fonts', () => {
            const fontMonoMatch = cssContent.match(/--font-mono:\s*([^;]+);/);
            expect(fontMonoMatch).not.toBeNull();
            const fontStack = fontMonoMatch[1];
            // Should include at least one well-known code font
            const hasCodeFont = /Fira Code|Consolas|SF Mono|Monaco|Menlo/i.test(fontStack);
            expect(hasCodeFont).toBe(true);
        });
    });

    describe('Test Case 2: Consistent spacing scale is used', () => {
        test('spacing variables are defined', () => {
            expect(cssContent).toMatch(/--spacing-xs:/);
            expect(cssContent).toMatch(/--spacing-sm:/);
            expect(cssContent).toMatch(/--spacing-md:/);
            expect(cssContent).toMatch(/--spacing-lg:/);
            expect(cssContent).toMatch(/--spacing-xl:/);
        });

        test('spacing values follow a consistent scale (using rem units)', () => {
            // Extract spacing values
            const spacingXs = cssContent.match(/--spacing-xs:\s*([^;]+);/);
            const spacingSm = cssContent.match(/--spacing-sm:\s*([^;]+);/);
            const spacingMd = cssContent.match(/--spacing-md:\s*([^;]+);/);
            const spacingLg = cssContent.match(/--spacing-lg:\s*([^;]+);/);
            const spacingXl = cssContent.match(/--spacing-xl:\s*([^;]+);/);

            expect(spacingXs).not.toBeNull();
            expect(spacingSm).not.toBeNull();
            expect(spacingMd).not.toBeNull();
            expect(spacingLg).not.toBeNull();
            expect(spacingXl).not.toBeNull();

            // All should use rem units
            expect(spacingXs[1]).toMatch(/rem/);
            expect(spacingSm[1]).toMatch(/rem/);
            expect(spacingMd[1]).toMatch(/rem/);
            expect(spacingLg[1]).toMatch(/rem/);
            expect(spacingXl[1]).toMatch(/rem/);
        });

        test('spacing values form a progressive scale (each value larger than previous)', () => {
            // Extract numeric values
            const parseRem = (match) => {
                if (!match) return 0;
                const numMatch = match[1].match(/([\d.]+)rem/);
                return numMatch ? parseFloat(numMatch[1]) : 0;
            };

            const xs = parseRem(cssContent.match(/--spacing-xs:\s*([^;]+);/));
            const sm = parseRem(cssContent.match(/--spacing-sm:\s*([^;]+);/));
            const md = parseRem(cssContent.match(/--spacing-md:\s*([^;]+);/));
            const lg = parseRem(cssContent.match(/--spacing-lg:\s*([^;]+);/));
            const xl = parseRem(cssContent.match(/--spacing-xl:\s*([^;]+);/));

            expect(xs).toBeGreaterThan(0);
            expect(sm).toBeGreaterThan(xs);
            expect(md).toBeGreaterThan(sm);
            expect(lg).toBeGreaterThan(md);
            expect(xl).toBeGreaterThan(lg);
        });

        test('components use spacing variables consistently', () => {
            // Check that components use the spacing variables instead of arbitrary values
            const varUsageCount = (cssContent.match(/var\(--spacing-/g) || []).length;
            // Should have many uses of spacing variables
            expect(varUsageCount).toBeGreaterThanOrEqual(20);
        });

        test('padding uses spacing variables', () => {
            // Multiple padding declarations should use spacing variables
            expect(cssContent).toMatch(/padding:\s*var\(--spacing-/);
            expect(cssContent).toMatch(/padding-top:\s*var\(--spacing-/);
        });

        test('gap uses spacing variables', () => {
            // Grid/flex gaps should use spacing variables
            expect(cssContent).toMatch(/gap:\s*var\(--spacing-/);
        });

        test('margins use spacing variables', () => {
            // Margins should use spacing variables
            expect(cssContent).toMatch(/margin-bottom:\s*var\(--spacing-/);
        });
    });

    describe('Test Case 3: Limited color palette is used consistently', () => {
        test('color variables are defined in :root', () => {
            expect(cssContent).toMatch(/--color-primary:/);
            expect(cssContent).toMatch(/--color-text:/);
            expect(cssContent).toMatch(/--color-bg:/);
            expect(cssContent).toMatch(/--color-border:/);
        });

        test('limited set of base colors defined (not excessive palette)', () => {
            // Count unique --color- variables (should be reasonable, not excessive)
            const colorVars = cssContent.match(/--color-[a-z-]+:/g) || [];
            const uniqueVars = [...new Set(colorVars)];
            // Should have between 5 and 15 color variables (focused palette)
            expect(uniqueVars.length).toBeGreaterThanOrEqual(5);
            expect(uniqueVars.length).toBeLessThanOrEqual(15);
        });

        test('components use color variables instead of hardcoded values', () => {
            // Count usage of color variables
            const varUsageCount = (cssContent.match(/var\(--color-/g) || []).length;
            // Should have many uses of color variables
            expect(varUsageCount).toBeGreaterThanOrEqual(30);
        });

        test('text elements use color variables', () => {
            expect(cssContent).toMatch(/color:\s*var\(--color-text/);
        });

        test('background elements use color variables', () => {
            expect(cssContent).toMatch(/background-color:\s*var\(--color-/);
        });

        test('border elements use color variables', () => {
            expect(cssContent).toMatch(/border[^:]*:\s*[^;]*var\(--color-border\)/);
        });

        test('primary color is consistently used for accents', () => {
            // Primary color should be used for interactive elements
            expect(cssContent).toMatch(/color:\s*var\(--color-primary\)/);
            expect(cssContent).toMatch(/background-color:\s*var\(--color-primary\)/);
        });
    });

    describe('Test Case 4: Dark/muted color scheme for developer aesthetic', () => {
        test('primary color is a professional blue tone', () => {
            const primaryMatch = cssContent.match(/--color-primary:\s*#([0-9a-fA-F]{6})/);
            expect(primaryMatch).not.toBeNull();
            // Verify it's in the blue family (high blue component)
            const hex = primaryMatch[1];
            const r = parseInt(hex.substring(0, 2), 16);
            const g = parseInt(hex.substring(2, 4), 16);
            const b = parseInt(hex.substring(4, 6), 16);
            // Blue should be the dominant or one of the dominant components
            expect(b).toBeGreaterThanOrEqual(Math.min(r, g));
        });

        test('text color is dark and readable', () => {
            const textMatch = cssContent.match(/--color-text:\s*#([0-9a-fA-F]{6})/);
            expect(textMatch).not.toBeNull();
            const hex = textMatch[1];
            const r = parseInt(hex.substring(0, 2), 16);
            const g = parseInt(hex.substring(2, 4), 16);
            const b = parseInt(hex.substring(4, 6), 16);
            // Text should be dark (low values for light mode)
            const luminance = (0.299 * r + 0.587 * g + 0.114 * b);
            expect(luminance).toBeLessThan(128); // Dark text for contrast
        });

        test('code block background uses dark color for terminal aesthetic', () => {
            const codeBgMatch = cssContent.match(/--color-code-bg:\s*#([0-9a-fA-F]{6})/);
            expect(codeBgMatch).not.toBeNull();
            const hex = codeBgMatch[1];
            const r = parseInt(hex.substring(0, 2), 16);
            const g = parseInt(hex.substring(2, 4), 16);
            const b = parseInt(hex.substring(4, 6), 16);
            // Code background should be dark
            const luminance = (0.299 * r + 0.587 * g + 0.114 * b);
            expect(luminance).toBeLessThan(100); // Dark background
        });

        test('code text color is light for contrast against dark background', () => {
            const codeTextMatch = cssContent.match(/--color-code-text:\s*#([0-9a-fA-F]{6})/);
            expect(codeTextMatch).not.toBeNull();
            const hex = codeTextMatch[1];
            const r = parseInt(hex.substring(0, 2), 16);
            const g = parseInt(hex.substring(2, 4), 16);
            const b = parseInt(hex.substring(4, 6), 16);
            // Code text should be light
            const luminance = (0.299 * r + 0.587 * g + 0.114 * b);
            expect(luminance).toBeGreaterThan(150); // Light text
        });

        test('background color uses neutral/white tones', () => {
            const bgMatch = cssContent.match(/--color-bg:\s*#([0-9a-fA-F]{6})/);
            expect(bgMatch).not.toBeNull();
            const hex = bgMatch[1];
            const r = parseInt(hex.substring(0, 2), 16);
            const g = parseInt(hex.substring(2, 4), 16);
            const b = parseInt(hex.substring(4, 6), 16);
            // Background should be light/white
            const luminance = (0.299 * r + 0.587 * g + 0.114 * b);
            expect(luminance).toBeGreaterThan(200); // Light background
        });

        test('alternate background has subtle difference', () => {
            const bgAltMatch = cssContent.match(/--color-bg-alt:\s*#([0-9a-fA-F]{6})/);
            expect(bgAltMatch).not.toBeNull();
            const hex = bgAltMatch[1];
            const r = parseInt(hex.substring(0, 2), 16);
            const g = parseInt(hex.substring(2, 4), 16);
            const b = parseInt(hex.substring(4, 6), 16);
            // Alt background should also be light
            const luminance = (0.299 * r + 0.587 * g + 0.114 * b);
            expect(luminance).toBeGreaterThan(200);
        });

        test('footer uses dark background for professional look', () => {
            // Footer should use dark background (code-bg color)
            expect(cssContent).toMatch(/\.footer\s*\{[^}]*background-color:\s*var\(--color-code-bg\)/s);
        });

        test('footer text uses light color for readability', () => {
            // Footer text should be light against dark background
            expect(cssContent).toMatch(/\.footer\s*\{[^}]*color:\s*var\(--color-code-text\)/s);
        });

        test('muted secondary color for less prominent text', () => {
            const secondaryMatch = cssContent.match(/--color-secondary:\s*#([0-9a-fA-F]{6})/);
            expect(secondaryMatch).not.toBeNull();
            // Secondary color should exist for muted text
        });

        test('text-light variant exists for less prominent text', () => {
            expect(cssContent).toMatch(/--color-text-light:/);
        });
    });

    describe('Typography Consistency', () => {
        test('sans-serif font variable is defined for body text', () => {
            expect(cssContent).toMatch(/--font-sans:/);
        });

        test('body uses sans-serif font variable', () => {
            expect(cssContent).toMatch(/body\s*\{[^}]*font-family:\s*var\(--font-sans\)/s);
        });

        test('font size is set on body element', () => {
            expect(cssContent).toMatch(/body\s*\{[^}]*font-size:/s);
        });

        test('line-height is set for readability', () => {
            expect(cssContent).toMatch(/line-height:\s*1\.[56]/);
        });

        test('headings have appropriate font weights', () => {
            // h1 should be bold
            expect(cssContent).toMatch(/font-weight:\s*[78]00/);
        });
    });

    describe('Border and Shadow Consistency', () => {
        test('border color variable is used consistently', () => {
            const borderUsage = (cssContent.match(/var\(--color-border\)/g) || []).length;
            expect(borderUsage).toBeGreaterThanOrEqual(5);
        });

        test('border-radius is consistent (8px or 12px pattern)', () => {
            const radiusMatches = cssContent.match(/border-radius:\s*\d+px/g) || [];
            const values = radiusMatches.map(m => parseInt(m.match(/\d+/)[0]));
            // Should use a limited set of border-radius values
            const uniqueValues = [...new Set(values)];
            expect(uniqueValues.length).toBeLessThanOrEqual(5);
        });

        test('cards use consistent border styling', () => {
            // Feature cards, specs cards, etc. should have consistent borders
            expect(cssContent).toMatch(/\.feature-card\s*\{[^}]*border:\s*1px\s+solid\s+var\(--color-border\)/s);
            expect(cssContent).toMatch(/\.specs-card\s*\{[^}]*border:\s*1px\s+solid\s+var\(--color-border\)/s);
        });
    });
});
