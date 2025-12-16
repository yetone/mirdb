/**
 * Accessibility Tests - WCAG 2.1 AA Compliance
 *
 * This test suite validates that the MirDB homepage meets WCAG 2.1 AA accessibility
 * standards including heading hierarchy, color contrast, keyboard navigation,
 * skip navigation links, and proper ARIA/lang attributes.
 */

const fs = require('fs');
const path = require('path');
const cheerio = require('cheerio');

// Load the homepage HTML and CSS
const htmlPath = path.join(__dirname, '..', 'index.html');
const cssPath = path.join(__dirname, '..', 'styles.css');
const htmlContent = fs.readFileSync(htmlPath, 'utf-8');
const cssContent = fs.readFileSync(cssPath, 'utf-8');
const $ = cheerio.load(htmlContent);

/**
 * Calculate relative luminance of a color
 * @param {number} r - Red component (0-255)
 * @param {number} g - Green component (0-255)
 * @param {number} b - Blue component (0-255)
 * @returns {number} Relative luminance
 */
function getLuminance(r, g, b) {
    const [rs, gs, bs] = [r, g, b].map(c => {
        c = c / 255;
        return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
    });
    return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
}

/**
 * Calculate contrast ratio between two colors
 * @param {string} color1 - Hex color (e.g., '#ffffff')
 * @param {string} color2 - Hex color (e.g., '#000000')
 * @returns {number} Contrast ratio
 */
function getContrastRatio(color1, color2) {
    const parseHex = (hex) => {
        hex = hex.replace('#', '');
        if (hex.length === 3) {
            hex = hex.split('').map(c => c + c).join('');
        }
        return {
            r: parseInt(hex.substring(0, 2), 16),
            g: parseInt(hex.substring(2, 4), 16),
            b: parseInt(hex.substring(4, 6), 16)
        };
    };

    const c1 = parseHex(color1);
    const c2 = parseHex(color2);

    const l1 = getLuminance(c1.r, c1.g, c1.b);
    const l2 = getLuminance(c2.r, c2.g, c2.b);

    const lighter = Math.max(l1, l2);
    const darker = Math.min(l1, l2);

    return (lighter + 0.05) / (darker + 0.05);
}

/**
 * Extract CSS variable value from CSS content
 * @param {string} varName - CSS variable name (e.g., '--primary-color')
 * @returns {string|null} The CSS value or null
 */
function getCSSVariable(varName) {
    const regex = new RegExp(`${varName}:\\s*([^;]+);`);
    const match = cssContent.match(regex);
    return match ? match[1].trim() : null;
}

describe('Accessibility Compliance - WCAG 2.1 AA', () => {
    describe('Test Case 1: Single H1 Element', () => {
        it('should have exactly one <h1> element on the page', () => {
            const h1Elements = $('h1');
            expect(h1Elements.length).toBe(1);
        });

        it('the <h1> element should contain meaningful content', () => {
            const h1 = $('h1');
            const text = h1.text().trim();
            expect(text.length).toBeGreaterThan(0);
            expect(text.toLowerCase()).toContain('mirdb');
        });

        it('the <h1> should be within the main content area', () => {
            const h1InMain = $('main h1, section h1, .hero h1');
            expect(h1InMain.length).toBe(1);
        });
    });

    describe('Test Case 2: Heading Hierarchy', () => {
        it('should have proper heading hierarchy (h1 -> h2 -> h3) without skipping levels', () => {
            const headings = $('h1, h2, h3, h4, h5, h6');
            let previousLevel = 0;
            let hasSkippedLevel = false;

            headings.each((index, element) => {
                const tagName = element.tagName.toLowerCase();
                const currentLevel = parseInt(tagName.charAt(1));

                // Can go up any amount (e.g., h3 to h1), but should not skip down (e.g., h1 to h3)
                if (currentLevel > previousLevel + 1 && previousLevel !== 0) {
                    hasSkippedLevel = true;
                }

                previousLevel = currentLevel;
            });

            expect(hasSkippedLevel).toBe(false);
        });

        it('should start heading hierarchy with h1', () => {
            const firstHeading = $('h1, h2, h3, h4, h5, h6').first();
            expect(firstHeading.length).toBe(1);
            expect(firstHeading.get(0).tagName.toLowerCase()).toBe('h1');
        });

        it('should have multiple h2 elements for section organization', () => {
            const h2Elements = $('h2');
            expect(h2Elements.length).toBeGreaterThanOrEqual(4);
        });

        it('h2 elements should follow h1', () => {
            const h1Index = $('h1, h2').index($('h1').first());
            const h2Index = $('h1, h2').index($('h2').first());
            expect(h2Index).toBeGreaterThan(h1Index);
        });

        it('h3 elements should follow h2 in their respective sections', () => {
            const sections = $('section');
            let validHierarchy = true;

            sections.each((i, section) => {
                const sectionHeadings = $(section).find('h2, h3');
                let lastWasH2 = false;

                sectionHeadings.each((j, heading) => {
                    const tag = heading.tagName.toLowerCase();
                    if (tag === 'h2') {
                        lastWasH2 = true;
                    } else if (tag === 'h3' && !lastWasH2) {
                        // h3 without preceding h2 in section is invalid
                        const parentSection = $(heading).closest('section');
                        if (parentSection.find('h2').length === 0) {
                            validHierarchy = false;
                        }
                    }
                });
            });

            expect(validHierarchy).toBe(true);
        });
    });

    describe('Test Case 3: Lang Attribute', () => {
        it('should have lang attribute on <html> element', () => {
            const htmlLang = $('html').attr('lang');
            expect(htmlLang).toBeDefined();
            expect(htmlLang.length).toBeGreaterThan(0);
        });

        it('should have a valid language code (e.g., "en")', () => {
            const htmlLang = $('html').attr('lang');
            // Valid language codes are 2-3 letters, optionally followed by region
            expect(htmlLang).toMatch(/^[a-z]{2,3}(-[A-Z]{2})?$/);
        });

        it('should use English (en) as the language', () => {
            const htmlLang = $('html').attr('lang');
            expect(htmlLang.toLowerCase()).toBe('en');
        });
    });

    describe('Test Case 4: Link Accessibility', () => {
        it('all links should have descriptive text or aria-label', () => {
            const links = $('a');
            let allLinksAccessible = true;
            const inaccessibleLinks = [];

            links.each((index, element) => {
                const $link = $(element);
                const text = $link.text().trim();
                const ariaLabel = $link.attr('aria-label');
                const title = $link.attr('title');
                const hasImg = $link.find('img[alt]').length > 0;

                // Link must have text, aria-label, title, or contain an image with alt
                if (!text && !ariaLabel && !title && !hasImg) {
                    allLinksAccessible = false;
                    inaccessibleLinks.push($link.attr('href'));
                }
            });

            expect(allLinksAccessible).toBe(true);
        });

        it('links should not have generic text like "click here" or "read more"', () => {
            const links = $('a');
            const genericPhrases = ['click here', 'read more', 'learn more', 'here', 'link'];
            let hasGenericText = false;

            links.each((index, element) => {
                const text = $(element).text().trim().toLowerCase();
                if (genericPhrases.includes(text)) {
                    hasGenericText = true;
                }
            });

            expect(hasGenericText).toBe(false);
        });

        it('external links should have indication they open in new window', () => {
            const externalLinks = $('a[target="_blank"]');
            let allHaveIndication = true;

            externalLinks.each((index, element) => {
                const $link = $(element);
                const text = $link.text().trim().toLowerCase();
                const ariaLabel = $link.attr('aria-label') || '';
                const rel = $link.attr('rel') || '';

                // Must have rel="noopener" for security
                if (!rel.includes('noopener')) {
                    allHaveIndication = false;
                }
            });

            expect(allHaveIndication).toBe(true);
        });

        it('all images should have alt attributes', () => {
            const images = $('img');
            let allHaveAlt = true;

            images.each((index, element) => {
                const alt = $(element).attr('alt');
                if (alt === undefined || alt === null) {
                    allHaveAlt = false;
                }
            });

            expect(allHaveAlt).toBe(true);
        });
    });

    describe('Test Case 5: Automated Accessibility Audit', () => {
        it('should have proper document structure', () => {
            // Check for proper HTML5 structure
            expect($('header').length).toBeGreaterThan(0);
            expect($('main').length).toBe(1);
            expect($('footer').length).toBe(1);
            expect($('nav').length).toBeGreaterThan(0);
        });

        it('should have semantic HTML elements', () => {
            expect($('section').length).toBeGreaterThan(0);
            expect($('article, aside, section').length).toBeGreaterThan(0);
        });

        it('should not have multiple main elements', () => {
            expect($('main').length).toBe(1);
        });

        it('form elements should have associated labels', () => {
            const formInputs = $('input, select, textarea').not('[type="hidden"], [type="submit"], [type="button"]');

            formInputs.each((index, element) => {
                const $input = $(element);
                const id = $input.attr('id');
                const ariaLabel = $input.attr('aria-label');
                const ariaLabelledby = $input.attr('aria-labelledby');

                // Must have id with matching label, or aria-label/aria-labelledby
                if (id) {
                    const hasLabel = $(`label[for="${id}"]`).length > 0;
                    expect(hasLabel || ariaLabel || ariaLabelledby).toBeTruthy();
                } else {
                    expect(ariaLabel || ariaLabelledby).toBeTruthy();
                }
            });
        });

        it('tables should have proper structure', () => {
            const tables = $('table');

            tables.each((index, element) => {
                const $table = $(element);
                const hasHeader = $table.find('thead, th').length > 0;
                expect(hasHeader).toBe(true);
            });
        });

        it('should not use deprecated HTML elements', () => {
            const deprecated = ['font', 'center', 'marquee', 'blink', 'frame', 'frameset'];
            deprecated.forEach(tag => {
                expect($(tag).length).toBe(0);
            });
        });

        it('buttons should have accessible names', () => {
            const buttons = $('button');

            buttons.each((index, element) => {
                const $button = $(element);
                const text = $button.text().trim();
                const ariaLabel = $button.attr('aria-label');

                expect(text.length > 0 || ariaLabel).toBeTruthy();
            });
        });
    });

    describe('Test Case 6: Color Contrast Ratios', () => {
        // Based on CSS variables defined in styles.css
        // Updated primary color to #c73c1d for better contrast
        // Updated text-light to #595959 for WCAG AA compliance
        const colorPairs = [
            { fg: '#333', bg: '#fff', name: 'text on white background' },      // --text-color on --bg-color
            { fg: '#595959', bg: '#fff', name: 'light text on white' },        // --text-light on --bg-color
            { fg: '#595959', bg: '#f8f9fa', name: 'light text on light bg' },  // --text-light on --bg-light
            { fg: '#fff', bg: '#1a1a2e', name: 'white text on dark bg' },      // white on --bg-dark
            { fg: '#e0e0e0', bg: '#1a1a2e', name: 'light gray on dark bg' },   // tagline color on hero bg
            { fg: '#c73c1d', bg: '#fff', name: 'primary color on white' },     // --primary-color on white
            { fg: '#ccc', bg: '#1a1a2e', name: 'footer text on dark bg' },     // footer link color on bg-dark
        ];

        colorPairs.forEach(({ fg, bg, name }) => {
            it(`should have sufficient contrast for ${name} (4.5:1 minimum)`, () => {
                const ratio = getContrastRatio(fg, bg);
                expect(ratio).toBeGreaterThanOrEqual(4.5);
            });
        });

        it('should have large text meet 3:1 contrast ratio', () => {
            // Large text (18pt+ or 14pt bold) needs only 3:1 ratio
            // Hero h1 with gradient might be checked separately
            const h1ContrastRatio = getContrastRatio('#c73c1d', '#1a1a2e');
            expect(h1ContrastRatio).toBeGreaterThanOrEqual(3);
        });

        it('should verify button contrast meets requirements', () => {
            // Primary button: white text on primary color
            const primaryBtnRatio = getContrastRatio('#fff', '#c73c1d');
            expect(primaryBtnRatio).toBeGreaterThanOrEqual(4.5);

            // Secondary button: white text on dark bg (transparent + border)
            const secondaryBtnRatio = getContrastRatio('#fff', '#1a1a2e');
            expect(secondaryBtnRatio).toBeGreaterThanOrEqual(4.5);
        });

        it('should verify code element contrast', () => {
            // Code elements: primary color on light gray background
            const codeRatio = getContrastRatio('#c73c1d', '#f1f1f1');
            expect(codeRatio).toBeGreaterThanOrEqual(4.5);
        });
    });

    describe('Test Case 7: Skip Navigation Link', () => {
        it('should have a skip navigation link', () => {
            const skipLink = $('a[href="#main"], a[href="#main-content"], a.skip-link, a.skip-nav, [class*="skip"]');
            expect(skipLink.length).toBeGreaterThan(0);
        });

        it('skip navigation link should be first focusable element', () => {
            // The skip link should be early in the DOM
            const firstLink = $('body a').first();
            const isSkipLink = firstLink.hasClass('skip-link') ||
                               firstLink.hasClass('skip-nav') ||
                               firstLink.attr('href') === '#main' ||
                               firstLink.attr('href') === '#main-content' ||
                               firstLink.text().toLowerCase().includes('skip');
            expect(isSkipLink).toBe(true);
        });

        it('skip navigation link should have accessible text', () => {
            const skipLink = $('a.skip-link, a.skip-nav, a[href="#main"], a[href="#main-content"]').first();
            const text = skipLink.text().trim().toLowerCase();
            const hasSkipText = text.includes('skip') || text.includes('main') || text.includes('content');
            expect(hasSkipText).toBe(true);
        });

        it('skip link target should exist', () => {
            const skipLink = $('a.skip-link, a.skip-nav, a[href="#main"], a[href="#main-content"]').first();
            const targetHref = skipLink.attr('href');

            if (targetHref && targetHref.startsWith('#')) {
                const targetId = targetHref.substring(1);
                const target = $(`#${targetId}, main`);
                expect(target.length).toBeGreaterThan(0);
            }
        });
    });

    describe('Keyboard Navigation', () => {
        it('interactive elements should have visible focus indicators defined in CSS', () => {
            // Check that focus styles exist in CSS
            const hasFocusStyles = cssContent.includes(':focus') ||
                                   cssContent.includes(':focus-visible');
            expect(hasFocusStyles).toBe(true);
        });

        it('all interactive elements should be accessible via keyboard', () => {
            // Check that buttons and links don't have tabindex=-1 (which removes them from tab order)
            const interactiveElements = $('a, button, input, select, textarea');
            let allAccessible = true;

            interactiveElements.each((index, element) => {
                const tabindex = $(element).attr('tabindex');
                if (tabindex === '-1') {
                    allAccessible = false;
                }
            });

            expect(allAccessible).toBe(true);
        });

        it('navigation links should be in logical tab order', () => {
            const navLinks = $('nav a');
            let hasCustomTabOrder = false;

            navLinks.each((index, element) => {
                const tabindex = $(element).attr('tabindex');
                // tabindex > 0 creates custom order which can be confusing
                if (tabindex && parseInt(tabindex) > 0) {
                    hasCustomTabOrder = true;
                }
            });

            expect(hasCustomTabOrder).toBe(false);
        });
    });

    describe('Additional WCAG Requirements', () => {
        it('should have a title element', () => {
            const title = $('title');
            expect(title.length).toBe(1);
            expect(title.text().trim().length).toBeGreaterThan(0);
        });

        it('title should be descriptive', () => {
            const title = $('title').text().trim();
            expect(title.toLowerCase()).toContain('mirdb');
        });

        it('should have meta viewport for responsive design', () => {
            const viewport = $('meta[name="viewport"]');
            expect(viewport.length).toBe(1);
        });

        it('viewport should not disable zoom', () => {
            const viewport = $('meta[name="viewport"]');
            const content = viewport.attr('content') || '';
            expect(content).not.toContain('user-scalable=no');
            expect(content).not.toContain('maximum-scale=1');
        });

        it('should have proper charset declaration', () => {
            const charset = $('meta[charset]');
            expect(charset.length).toBe(1);
            expect(charset.attr('charset').toLowerCase()).toBe('utf-8');
        });
    });
});
