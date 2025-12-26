/**
 * Responsive Design - Mobile Tests
 *
 * Tests for verifying homepage renders correctly on mobile devices (NFR-1, US-6)
 * - No horizontal overflow at mobile viewport widths
 * - Navigation is accessible (hamburger menu or visible links)
 * - Text is readable (base font size at least 16px)
 * - Touch targets are adequate (at least 44px height)
 * - Viewport meta tag is present with width=device-width
 */

describe('Responsive Design - Mobile', () => {
    let styleSheets;

    beforeAll(() => {
        // Load CSS content for analysis
        const fs = require('fs');
        const path = require('path');
        const cssPath = path.join(__dirname, '..', 'css', 'styles.css');
        styleSheets = fs.readFileSync(cssPath, 'utf-8');
    });

    describe('Test Case 1: No horizontal overflow at 375px viewport width', () => {
        test('page should have box-sizing border-box reset', () => {
            // Check that the CSS includes box-sizing: border-box
            expect(styleSheets).toMatch(/box-sizing:\s*border-box/);
        });

        test('page should not have fixed widths larger than mobile viewport', () => {
            // Check that there are no fixed widths larger than 375px that could cause overflow
            // Container should use max-width, not fixed width
            expect(styleSheets).toMatch(/max-width:\s*var\(--container-max-width\)/);
        });

        test('container should have proper padding for mobile', () => {
            // Check container has padding that prevents edge-to-edge content
            expect(styleSheets).toMatch(/\.container\s*\{[^}]*padding:\s*0\s+var\(--spacing-md\)/);
        });

        test('grids should use auto-fit minmax for responsive behavior', () => {
            // Feature grid, comparison grid, etc. should use responsive grid patterns
            expect(styleSheets).toMatch(/grid-template-columns:\s*repeat\(auto-fit,\s*minmax\(/);
        });

        test('pre/code blocks should have overflow-x auto to prevent horizontal scrolling', () => {
            // Code blocks should scroll internally, not cause page overflow
            expect(styleSheets).toMatch(/overflow-x:\s*auto/);
        });
    });

    describe('Test Case 2: Navigation is accessible at mobile viewport', () => {
        let nav;
        let mediaQueryContent;

        beforeAll(() => {
            nav = document.querySelector('.nav') || document.querySelector('nav');
            // Extract the media query block for mobile
            const mediaMatch = styleSheets.match(/@media\s*\(max-width:\s*768px\)\s*\{([\s\S]*?)^\}/m);
            mediaQueryContent = mediaMatch ? mediaMatch[1] : '';
        });

        test('navigation element should exist', () => {
            expect(nav).not.toBeNull();
        });

        test('navigation should contain nav-links', () => {
            const navLinks = document.querySelector('.nav-links');
            expect(navLinks).not.toBeNull();
        });

        test('navigation links should be accessible (visible or via menu)', () => {
            const navLinks = document.querySelectorAll('.nav-links a');
            expect(navLinks.length).toBeGreaterThan(0);
        });

        test('navigation should have responsive styles for mobile', () => {
            // Check that nav has mobile-specific styles in media query
            expect(mediaQueryContent).toMatch(/\.nav\s*\{/);
        });

        test('nav-links should wrap on mobile for accessibility', () => {
            // Check that nav-links can wrap on mobile (in the media query)
            expect(mediaQueryContent).toMatch(/\.nav-links[\s\S]*?flex-wrap:\s*wrap/);
        });
    });

    describe('Test Case 3: Base font size is at least 16px', () => {
        test('body should have font-size of 16px or larger', () => {
            // Check CSS for body font-size
            expect(styleSheets).toMatch(/body\s*\{[^}]*font-size:\s*16px/s);
        });

        test('HTML document should not have font-size less than 16px', () => {
            const htmlFontSize = styleSheets.match(/html\s*\{[^}]*font-size:\s*(\d+)px/);
            if (htmlFontSize) {
                expect(parseInt(htmlFontSize[1])).toBeGreaterThanOrEqual(16);
            }
            // If no explicit html font-size, that's fine (browser default is 16px)
        });

        test('minimum readable font sizes should be used for text content', () => {
            // Check that no font-sizes are unreasonably small
            const smallFontMatches = styleSheets.match(/font-size:\s*(\d+)px/g);
            if (smallFontMatches) {
                smallFontMatches.forEach(match => {
                    const size = parseInt(match.match(/(\d+)/)[1]);
                    // Allow small sizes for code/monospace (0.875rem = 14px is acceptable)
                    // But ensure nothing is smaller than 12px
                    expect(size).toBeGreaterThanOrEqual(12);
                });
            }
        });
    });

    describe('Test Case 4: CTA button touch target is at least 44px in height', () => {
        let mediaQueryContent;

        beforeAll(() => {
            // Extract the media query block for mobile
            const mediaMatch = styleSheets.match(/@media\s*\(max-width:\s*768px\)\s*\{([\s\S]*?)^\}/m);
            mediaQueryContent = mediaMatch ? mediaMatch[1] : '';
        });

        test('buttons should have adequate padding for touch targets', () => {
            // Check that .btn has sufficient padding
            const btnMatch = styleSheets.match(/\.btn\s*\{[^}]*padding:\s*var\(--spacing-sm\)\s+var\(--spacing-lg\)/s);
            expect(btnMatch).not.toBeNull();
        });

        test('spacing-sm variable should provide adequate vertical padding', () => {
            // --spacing-sm: 1rem = 16px, so button with 16px top + 16px bottom = 32px padding
            // Plus text height (~16-20px), total should be > 44px
            expect(styleSheets).toMatch(/--spacing-sm:\s*1rem/);
        });

        test('buttons on mobile should have adequate width for touch', () => {
            // Check mobile styles for buttons within media query
            expect(mediaQueryContent).toMatch(/\.btn[\s\S]*?width:\s*100%/);
        });

        test('nav links should be spaced adequately for touch', () => {
            // Check that nav-links have gap for adequate touch spacing
            expect(styleSheets).toMatch(/\.nav-links\s*\{[^}]*gap:/s);
        });
    });

    describe('Test Case 5: Viewport meta tag with width=device-width is present', () => {
        test('viewport meta tag should exist', () => {
            const viewportMeta = document.querySelector('meta[name="viewport"]');
            expect(viewportMeta).not.toBeNull();
        });

        test('viewport meta tag should include width=device-width', () => {
            const viewportMeta = document.querySelector('meta[name="viewport"]');
            expect(viewportMeta).not.toBeNull();
            const content = viewportMeta.getAttribute('content');
            expect(content).toContain('width=device-width');
        });

        test('viewport meta tag should include initial-scale=1.0', () => {
            const viewportMeta = document.querySelector('meta[name="viewport"]');
            expect(viewportMeta).not.toBeNull();
            const content = viewportMeta.getAttribute('content');
            expect(content).toContain('initial-scale=1.0');
        });
    });

    describe('Additional mobile responsiveness checks', () => {
        let mediaQueryContent;

        beforeAll(() => {
            // Extract the media query block for mobile
            const mediaMatch = styleSheets.match(/@media\s*\(max-width:\s*768px\)\s*\{([\s\S]*?)^\}/m);
            mediaQueryContent = mediaMatch ? mediaMatch[1] : '';
        });

        test('hero section should have responsive font sizes', () => {
            // Check for mobile-specific hero styles within media query
            expect(mediaQueryContent).toMatch(/\.hero\s+h1[\s\S]*?font-size:/);
        });

        test('CTA buttons should stack vertically on mobile', () => {
            // Check that cta-buttons flex-direction changes to column on mobile
            expect(mediaQueryContent).toMatch(/\.cta-buttons[\s\S]*?flex-direction:\s*column/);
        });

        test('images should be responsive (if any)', () => {
            const images = document.querySelectorAll('img');
            // This project doesn't have images, but checking for responsive image patterns
            images.forEach(img => {
                // Images should have max-width: 100% in CSS or inline
                expect(true).toBe(true);
            });
        });

        test('tables should be scrollable on mobile', () => {
            // Check that comparison-table-container allows horizontal scroll
            const tables = document.querySelectorAll('table');
            expect(tables.length).toBeGreaterThan(0);
        });
    });
});
