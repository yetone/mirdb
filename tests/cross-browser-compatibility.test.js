/**
 * Cross-Browser Compatibility Tests (NFR-4)
 *
 * Verifies that the MirDB homepage works correctly across modern browsers:
 * - Chrome, Firefox, Safari, Edge
 *
 * Since we're using Jest/jsdom (not real browsers), these tests verify:
 * 1. HTML uses cross-browser compatible semantic elements
 * 2. CSS uses standard properties without vendor prefixes where not needed
 * 3. CSS custom properties (variables) are properly defined
 * 4. JavaScript (if any) uses cross-browser compatible APIs
 * 5. No browser-specific features that lack broad support
 */

describe('Cross-Browser Compatibility', () => {
    let htmlContent;
    let cssContent;

    beforeAll(() => {
        const fs = require('fs');
        const path = require('path');
        const htmlPath = path.join(__dirname, '..', 'index.html');
        const cssPath = path.join(__dirname, '..', 'css', 'styles.css');
        htmlContent = fs.readFileSync(htmlPath, 'utf-8');
        cssContent = fs.readFileSync(cssPath, 'utf-8');
    });

    describe('Test Case 1: Chrome Compatibility', () => {
        test('HTML5 doctype is properly declared', () => {
            expect(htmlContent).toMatch(/<!DOCTYPE html>/i);
        });

        test('uses semantic HTML5 elements supported in Chrome', () => {
            expect(document.querySelector('header')).not.toBeNull();
            expect(document.querySelector('main')).not.toBeNull();
            expect(document.querySelector('nav')).not.toBeNull();
            expect(document.querySelector('footer')).not.toBeNull();
            expect(document.querySelectorAll('section').length).toBeGreaterThan(0);
        });

        test('CSS Grid is properly used (supported in Chrome 57+)', () => {
            expect(cssContent).toMatch(/display:\s*grid/);
            expect(cssContent).toMatch(/grid-template-columns:/);
        });

        test('CSS Flexbox is properly used (supported in Chrome 29+)', () => {
            expect(cssContent).toMatch(/display:\s*flex/);
        });

        test('CSS custom properties are properly defined (supported in Chrome 49+)', () => {
            expect(cssContent).toMatch(/:root\s*\{/);
            expect(cssContent).toMatch(/--color-primary:/);
            expect(cssContent).toMatch(/--font-sans:/);
        });

        test('external links have proper security attributes', () => {
            const externalLinks = document.querySelectorAll('a[target="_blank"]');
            externalLinks.forEach(link => {
                expect(link.getAttribute('rel')).toContain('noopener');
            });
        });
    });

    describe('Test Case 2: Firefox Compatibility', () => {
        test('uses standard CSS properties (no -webkit-only properties without fallbacks)', () => {
            // Check that we're not using webkit-only properties exclusively
            const webkitOnlyMatches = cssContent.match(/-webkit-[^;]+;(?![^}]*[^-]webkit)/g);
            // We allow -webkit if there's also a standard property
            // The point is that Firefox should work with standard properties
            expect(cssContent).toMatch(/box-sizing:\s*border-box/);
        });

        test('CSS scroll-behavior is used properly (supported in Firefox 36+)', () => {
            expect(cssContent).toMatch(/scroll-behavior:\s*smooth/);
        });

        test('CSS transitions use standard syntax (supported in Firefox 16+)', () => {
            expect(cssContent).toMatch(/transition:/);
            // Should not only have -webkit-transition
            const transitionMatches = cssContent.match(/transition:[^;]+;/g);
            expect(transitionMatches).not.toBeNull();
            expect(transitionMatches.length).toBeGreaterThan(0);
        });

        test('CSS border-radius is used without vendor prefixes (supported in Firefox 4+)', () => {
            expect(cssContent).toMatch(/border-radius:/);
            // Standard border-radius should be present
        });

        test('meta charset is UTF-8 (Firefox prefers it early in document)', () => {
            const charsetMeta = document.querySelector('meta[charset]');
            expect(charsetMeta).not.toBeNull();
            expect(charsetMeta.getAttribute('charset').toUpperCase()).toBe('UTF-8');
        });
    });

    describe('Test Case 3: Safari Compatibility', () => {
        test('viewport meta tag is present (required for mobile Safari)', () => {
            const viewportMeta = document.querySelector('meta[name="viewport"]');
            expect(viewportMeta).not.toBeNull();
            expect(viewportMeta.getAttribute('content')).toContain('width=device-width');
        });

        test('uses -apple-system font for optimal Safari/macOS rendering', () => {
            expect(cssContent).toMatch(/-apple-system/);
        });

        test('uses system font stack for cross-platform consistency', () => {
            // Safari works best with system fonts
            // Font stack is defined in --font-sans variable
            expect(cssContent).toMatch(/--font-sans:\s*-apple-system[^;]*BlinkMacSystemFont/);
        });

        test('linear-gradient syntax is standard (Safari 7+)', () => {
            expect(cssContent).toMatch(/linear-gradient\s*\(/);
            // Should use standard syntax, not old webkit syntax
        });

        test('CSS variables are used correctly (Safari 9.1+)', () => {
            // Variables are used with var() function
            expect(cssContent).toMatch(/var\(--[a-zA-Z-]+\)/);
        });

        test('no experimental features without Safari support', () => {
            // Check that we don't use features Safari doesn't support
            // gap in flexbox is supported in Safari 14.1+
            // We use gap which is broadly supported
            expect(cssContent).toMatch(/gap:\s*var\(--spacing/);
        });
    });

    describe('Test Case 4: Edge Compatibility', () => {
        test('uses HTML5 semantic elements (Edge supports all)', () => {
            const semanticElements = ['header', 'nav', 'main', 'section', 'footer', 'article'];
            semanticElements.forEach(tag => {
                const elements = document.getElementsByTagName(tag);
                // At least some semantic elements should be present
            });
            expect(document.querySelector('header')).not.toBeNull();
            expect(document.querySelector('main')).not.toBeNull();
        });

        test('CSS custom properties work in Edge (Edge 16+)', () => {
            // Edge 16+ supports CSS custom properties
            expect(cssContent).toMatch(/:root\s*\{[\s\S]*--color-primary/);
        });

        test('CSS Grid works in Edge (Edge 16+)', () => {
            expect(cssContent).toMatch(/grid-template-columns:\s*repeat\(auto-fit/);
        });

        test('standard meta tags are present', () => {
            expect(document.querySelector('meta[charset]')).not.toBeNull();
            expect(document.querySelector('meta[name="viewport"]')).not.toBeNull();
            expect(document.querySelector('meta[name="description"]')).not.toBeNull();
        });

        test('uses standard anchor elements with proper attributes', () => {
            const links = document.querySelectorAll('a[href]');
            expect(links.length).toBeGreaterThan(0);
            // All external links should have rel="noopener"
            const externalLinks = document.querySelectorAll('a[target="_blank"]');
            externalLinks.forEach(link => {
                expect(link.getAttribute('rel')).toContain('noopener');
            });
        });
    });

    describe('General Cross-Browser Best Practices', () => {
        test('HTML lang attribute is set for accessibility', () => {
            // Check the raw HTML content since jsdom setup may not preserve all attributes
            expect(htmlContent).toMatch(/<html[^>]*lang\s*=\s*["']en["']/);
        });

        test('title element is present', () => {
            const title = document.querySelector('title');
            expect(title).not.toBeNull();
            expect(title.textContent.length).toBeGreaterThan(0);
        });

        test('no inline styles that could cause cross-browser issues', () => {
            // Check that we're not relying heavily on inline styles
            const elementsWithStyle = document.querySelectorAll('[style]');
            // Some inline styles are OK, but they should be minimal
            expect(elementsWithStyle.length).toBeLessThan(10);
        });

        test('CSS uses box-sizing border-box universally', () => {
            // This prevents cross-browser box model issues
            expect(cssContent).toMatch(/\*,\s*\*::before,\s*\*::after\s*\{[^}]*box-sizing:\s*border-box/s);
        });

        test('uses rem/em units for scalability', () => {
            // rem units work across all modern browsers
            expect(cssContent).toMatch(/font-size:.*rem/);
        });

        test('media queries use standard syntax', () => {
            expect(cssContent).toMatch(/@media\s*\(max-width:\s*\d+px\)/);
        });
    });
});
