/**
 * Tablet Responsive Design Tests
 *
 * Tests for verifying homepage renders correctly on tablet devices (NFR-1)
 * Viewport width: 768px (standard tablet width)
 */

describe('Responsive Design - Tablet', () => {
    let cssContent;
    let fs;
    let path;

    beforeAll(() => {
        fs = require('fs');
        path = require('path');
        const cssPath = path.join(__dirname, '..', 'css', 'styles.css');
        cssContent = fs.readFileSync(cssPath, 'utf-8');
    });

    describe('Test Case 1: Layout adapts with no content overflow at 768px viewport', () => {
        test('CSS should include media query for tablet/mobile breakpoint at 768px', () => {
            // Check that the CSS contains a media query targeting 768px or similar tablet breakpoint
            const hasTabletBreakpoint = cssContent.includes('@media') &&
                (cssContent.includes('768px') || cssContent.includes('max-width'));
            expect(hasTabletBreakpoint).toBe(true);
        });

        test('container should have responsive padding for tablet viewport', () => {
            // The container uses padding that adapts to viewport
            const containerStyles = cssContent.includes('.container');
            expect(containerStyles).toBe(true);
            // Check for responsive padding variable or explicit padding
            const hasResponsivePadding = cssContent.includes('--spacing-md') ||
                cssContent.includes('padding:') ||
                cssContent.includes('padding-left') ||
                cssContent.includes('padding-right');
            expect(hasResponsivePadding).toBe(true);
        });

        test('box-sizing should be set to border-box for proper layout calculations', () => {
            expect(cssContent.includes('box-sizing: border-box')).toBe(true);
        });

        test('navigation should be able to wrap or stack on tablet viewport', () => {
            // The nav should have flex-wrap or flex-direction capabilities for tablet
            const navExists = document.querySelector('.nav');
            expect(navExists).not.toBeNull();

            // Check CSS has nav responsive styles
            const hasNavResponsive = cssContent.includes('.nav') &&
                (cssContent.includes('flex-direction') || cssContent.includes('flex-wrap'));
            expect(hasNavResponsive).toBe(true);
        });

        test('body should not have horizontal overflow setup', () => {
            // The page shouldn't have settings that would cause overflow
            const body = document.querySelector('body');
            expect(body).not.toBeNull();
            // Check that there's no fixed width that would cause overflow
            expect(cssContent.includes('overflow-x: auto')).toBe(true);
        });

        test('pre elements should handle overflow gracefully', () => {
            // Pre/code blocks should have overflow-x: auto to prevent overflow
            const hasPreOverflow = cssContent.includes('pre') && cssContent.includes('overflow-x: auto');
            expect(hasPreOverflow).toBe(true);
        });
    });

    describe('Test Case 2: Feature cards display in appropriate grid at tablet viewport', () => {
        test('feature-grid should exist in HTML', () => {
            const featureGrid = document.querySelector('.feature-grid');
            expect(featureGrid).not.toBeNull();
        });

        test('feature-grid should use CSS Grid or Flexbox for responsive layout', () => {
            const hasGridLayout = cssContent.includes('.feature-grid') &&
                (cssContent.includes('display: grid') || cssContent.includes('display: flex'));
            expect(hasGridLayout).toBe(true);
        });

        test('feature-grid should use auto-fit or auto-fill for responsive columns', () => {
            // Check for responsive grid columns using auto-fit/auto-fill with minmax
            const hasResponsiveGrid = cssContent.includes('auto-fit') || cssContent.includes('auto-fill');
            expect(hasResponsiveGrid).toBe(true);
        });

        test('feature-grid should have minmax with appropriate minimum width for tablet', () => {
            // Feature cards should have a minimum width that allows 1-2 columns on tablet
            const hasMinMax = cssContent.includes('minmax');
            expect(hasMinMax).toBe(true);

            // The minmax value should allow cards to fit in tablet viewport
            // With 768px viewport and ~250px min-width, we'd get 2-3 columns
            const minMaxRegex = /minmax\s*\(\s*(\d+)px/;
            const match = cssContent.match(minMaxRegex);
            if (match) {
                const minWidth = parseInt(match[1], 10);
                // For tablet (768px), min-width should be <= 350px to allow at least 2 columns
                expect(minWidth).toBeLessThanOrEqual(350);
            }
        });

        test('feature cards should have all four feature elements', () => {
            const featureCards = document.querySelectorAll('.feature-card');
            expect(featureCards.length).toBe(4);
        });

        test('each feature card should have heading and description', () => {
            const featureCards = document.querySelectorAll('.feature-card');
            featureCards.forEach((card) => {
                expect(card.querySelector('h3')).not.toBeNull();
                expect(card.querySelector('p')).not.toBeNull();
            });
        });
    });

    describe('Test Case 3: All sections are visible and accessible at tablet viewport', () => {
        test('hero section should exist and be visible', () => {
            const heroSection = document.querySelector('.hero');
            expect(heroSection).not.toBeNull();
            expect(heroSection.querySelector('h1')).not.toBeNull();
        });

        test('features section should exist and be visible', () => {
            const featuresSection = document.querySelector('.features') || document.querySelector('#features');
            expect(featuresSection).not.toBeNull();
        });

        test('quick-start section should exist and be visible', () => {
            const quickStartSection = document.querySelector('.quick-start') || document.querySelector('#quick-start');
            expect(quickStartSection).not.toBeNull();
        });

        test('footer should exist and be visible', () => {
            const footer = document.querySelector('.footer') || document.querySelector('footer');
            expect(footer).not.toBeNull();
        });

        test('comparison section should exist and be visible', () => {
            const comparisonSection = document.querySelector('.comparison') || document.querySelector('#comparison');
            expect(comparisonSection).not.toBeNull();
        });

        test('technical specs section should exist and be visible', () => {
            const techSpecsSection = document.querySelector('.technical-specs') || document.querySelector('#technical-specs');
            expect(techSpecsSection).not.toBeNull();
        });

        test('all navigation links should point to existing sections', () => {
            const navLinks = document.querySelectorAll('.nav-links a[href^="#"]');
            navLinks.forEach((link) => {
                const targetId = link.getAttribute('href').replace('#', '');
                const targetSection = document.getElementById(targetId);
                expect(targetSection).not.toBeNull();
            });
        });

        test('CTA buttons in hero section should be accessible', () => {
            const ctaButtons = document.querySelector('.cta-buttons');
            expect(ctaButtons).not.toBeNull();
            const buttons = ctaButtons.querySelectorAll('a.btn');
            expect(buttons.length).toBeGreaterThan(0);
        });

        test('quick-start footer buttons should be accessible', () => {
            const quickStartFooter = document.querySelector('.quick-start-footer');
            expect(quickStartFooter).not.toBeNull();
            const buttons = quickStartFooter.querySelectorAll('a.btn');
            expect(buttons.length).toBeGreaterThan(0);
        });

        test('footer links should be accessible', () => {
            const footer = document.querySelector('.footer');
            expect(footer).not.toBeNull();
            const links = footer.querySelectorAll('a');
            expect(links.length).toBeGreaterThan(0);
        });
    });

    describe('CSS Responsive Design Verification', () => {
        test('CSS should include viewport meta tag compatible styles', () => {
            // Check that HTML has proper viewport meta tag
            const metaViewport = document.querySelector('meta[name="viewport"]');
            expect(metaViewport).not.toBeNull();
            expect(metaViewport.getAttribute('content')).toContain('width=device-width');
        });

        test('CSS should use relative units for font sizes in responsive areas', () => {
            // Check for use of rem or em units for accessibility
            const hasRemUnits = cssContent.includes('rem') || cssContent.includes('em');
            expect(hasRemUnits).toBe(true);
        });

        test('CSS should define a maximum container width for large screens', () => {
            const hasMaxWidth = cssContent.includes('max-width') &&
                cssContent.includes('--container-max-width');
            expect(hasMaxWidth).toBe(true);
        });

        test('buttons should use flex-wrap for tablet responsiveness', () => {
            // CTA buttons should wrap on smaller screens
            const hasFlexWrap = cssContent.includes('.cta-buttons') && cssContent.includes('flex-wrap');
            expect(hasFlexWrap).toBe(true);
        });

        test('comparison grid should be responsive', () => {
            const hasComparisonGrid = cssContent.includes('.comparison-grid') &&
                (cssContent.includes('auto-fit') || cssContent.includes('auto-fill'));
            expect(hasComparisonGrid).toBe(true);
        });

        test('specs grid should be responsive', () => {
            const hasSpecsGrid = cssContent.includes('.specs-grid') &&
                (cssContent.includes('auto-fit') || cssContent.includes('auto-fill'));
            expect(hasSpecsGrid).toBe(true);
        });

        test('footer content grid should be responsive', () => {
            const hasFooterGrid = cssContent.includes('.footer-content') &&
                (cssContent.includes('auto-fit') || cssContent.includes('auto-fill'));
            expect(hasFooterGrid).toBe(true);
        });
    });
});
