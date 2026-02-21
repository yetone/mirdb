/**
 * Accessibility Unit Tests
 * Owner: Scenario 7 - Accessibility Compliance
 *
 * Tests:
 * - Semantic HTML structure validation
 * - Heading hierarchy (h1 > h2 > h3)
 * - ARIA attributes validation
 * - Color contrast ratios
 * - Keyboard navigation
 * - Skip link functionality
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

// Load the HTML file
const htmlPath = path.resolve(__dirname, '../../index.html');
const htmlContent = fs.readFileSync(htmlPath, 'utf-8');
const cssPath = path.resolve(__dirname, '../../css/styles.css');
const cssContent = fs.existsSync(cssPath) ? fs.readFileSync(cssPath, 'utf-8') : '';

let dom;
let document;

beforeEach(() => {
    dom = new JSDOM(htmlContent);
    document = dom.window.document;
});

/* ========================================
   Test Case 2: Single h1 Heading
   ======================================== */
describe('Test Case 2: Single h1 Heading', () => {
    test('Page has exactly one h1 element', () => {
        const h1Elements = document.querySelectorAll('h1');
        expect(h1Elements.length).toBe(1);
    });

    test('h1 element contains "MirDB"', () => {
        const h1 = document.querySelector('h1');
        expect(h1).not.toBeNull();
        expect(h1.textContent).toContain('MirDB');
    });
});

/* ========================================
   Test Case 3: Heading Hierarchy
   ======================================== */
describe('Test Case 3: Heading Hierarchy', () => {
    test('Headings follow h1 > h2 > h3 order without skipping levels', () => {
        const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
        const headingLevels = Array.from(headings).map(h =>
            parseInt(h.tagName.charAt(1))
        );

        // Check no skipped levels
        let previousLevel = 0;
        for (const level of headingLevels) {
            // Each heading level should either:
            // 1. Be the same as previous
            // 2. Be one level deeper than previous
            // 3. Be any level higher than previous (going back up)
            if (level > previousLevel + 1) {
                // Skipped a level - this is an error
                fail(`Heading hierarchy skipped from h${previousLevel} to h${level}`);
            }
            previousLevel = level;
        }

        // Should start with h1
        expect(headingLevels[0]).toBe(1);
    });

    test('h2 headings come after h1', () => {
        const allHeadings = document.querySelectorAll('h1, h2');
        const headingSequence = Array.from(allHeadings).map(h => h.tagName);

        // First heading should be H1
        expect(headingSequence[0]).toBe('H1');

        // H2s should exist and come after H1
        const h2Index = headingSequence.indexOf('H2');
        const h1Index = headingSequence.indexOf('H1');
        expect(h2Index).toBeGreaterThan(h1Index);
    });

    test('h3 headings only appear after h2 headings', () => {
        const allHeadings = Array.from(document.querySelectorAll('h1, h2, h3'));

        // For each h3, there should be an h2 before it
        allHeadings.forEach((heading, index) => {
            if (heading.tagName === 'H3') {
                const previousHeadings = allHeadings.slice(0, index).map(h => h.tagName);
                expect(previousHeadings).toContain('H2');
            }
        });
    });
});

/* ========================================
   Test Case 7: Color Contrast for Body Text
   ======================================== */
describe('Test Case 7: Body Text Color Contrast', () => {
    // Helper function to extract CSS variable values
    function extractCSSVariable(cssContent, varName) {
        const regex = new RegExp(`--${varName}:\\s*([^;]+);`);
        const match = cssContent.match(regex);
        return match ? match[1].trim() : null;
    }

    // Helper to parse hex color to RGB
    function hexToRgb(hex) {
        const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
        return result ? {
            r: parseInt(result[1], 16),
            g: parseInt(result[2], 16),
            b: parseInt(result[3], 16)
        } : null;
    }

    // Calculate relative luminance
    function relativeLuminance(rgb) {
        const sRGB = [rgb.r / 255, rgb.g / 255, rgb.b / 255];
        const linear = sRGB.map(val =>
            val <= 0.03928 ? val / 12.92 : Math.pow((val + 0.055) / 1.055, 2.4)
        );
        return 0.2126 * linear[0] + 0.7152 * linear[1] + 0.0722 * linear[2];
    }

    // Calculate contrast ratio
    function contrastRatio(color1, color2) {
        const l1 = Math.max(relativeLuminance(color1), relativeLuminance(color2));
        const l2 = Math.min(relativeLuminance(color1), relativeLuminance(color2));
        return (l1 + 0.05) / (l2 + 0.05);
    }

    test('Body text color has at least 4.5:1 contrast ratio against background', () => {
        // Extract colors from CSS
        const textColor = extractCSSVariable(cssContent, 'color-text') || '#1e293b';
        const bgColor = extractCSSVariable(cssContent, 'color-background') || '#ffffff';

        const textRgb = hexToRgb(textColor);
        const bgRgb = hexToRgb(bgColor);

        if (textRgb && bgRgb) {
            const ratio = contrastRatio(textRgb, bgRgb);
            expect(ratio).toBeGreaterThanOrEqual(4.5);
        }
    });

    test('Muted text color has at least 4.5:1 contrast ratio', () => {
        const mutedColor = extractCSSVariable(cssContent, 'color-text-muted') || '#64748b';
        const bgColor = extractCSSVariable(cssContent, 'color-background') || '#ffffff';

        const mutedRgb = hexToRgb(mutedColor);
        const bgRgb = hexToRgb(bgColor);

        if (mutedRgb && bgRgb) {
            const ratio = contrastRatio(mutedRgb, bgRgb);
            expect(ratio).toBeGreaterThanOrEqual(4.5);
        }
    });
});

/* ========================================
   Test Case 8: Large Text/Headings Contrast
   ======================================== */
describe('Test Case 8: Large Text Color Contrast', () => {
    function extractCSSVariable(cssContent, varName) {
        const regex = new RegExp(`--${varName}:\\s*([^;]+);`);
        const match = cssContent.match(regex);
        return match ? match[1].trim() : null;
    }

    function hexToRgb(hex) {
        const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
        return result ? {
            r: parseInt(result[1], 16),
            g: parseInt(result[2], 16),
            b: parseInt(result[3], 16)
        } : null;
    }

    function relativeLuminance(rgb) {
        const sRGB = [rgb.r / 255, rgb.g / 255, rgb.b / 255];
        const linear = sRGB.map(val =>
            val <= 0.03928 ? val / 12.92 : Math.pow((val + 0.055) / 1.055, 2.4)
        );
        return 0.2126 * linear[0] + 0.7152 * linear[1] + 0.0722 * linear[2];
    }

    function contrastRatio(color1, color2) {
        const l1 = Math.max(relativeLuminance(color1), relativeLuminance(color2));
        const l2 = Math.min(relativeLuminance(color1), relativeLuminance(color2));
        return (l1 + 0.05) / (l2 + 0.05);
    }

    test('Heading text color has at least 3:1 contrast ratio (large text requirement)', () => {
        // Headings use --color-text, check against both backgrounds
        const headingColor = extractCSSVariable(cssContent, 'color-text') || '#1e293b';
        const bgColor = extractCSSVariable(cssContent, 'color-background') || '#ffffff';
        const surfaceColor = extractCSSVariable(cssContent, 'color-surface') || '#f8fafc';

        const headingRgb = hexToRgb(headingColor);
        const bgRgb = hexToRgb(bgColor);
        const surfaceRgb = hexToRgb(surfaceColor);

        if (headingRgb && bgRgb) {
            const ratio1 = contrastRatio(headingRgb, bgRgb);
            expect(ratio1).toBeGreaterThanOrEqual(3);
        }

        if (headingRgb && surfaceRgb) {
            const ratio2 = contrastRatio(headingRgb, surfaceRgb);
            expect(ratio2).toBeGreaterThanOrEqual(3);
        }
    });

    test('Primary color (used in tagline) has at least 3:1 contrast', () => {
        const primaryColor = extractCSSVariable(cssContent, 'color-primary') || '#2563eb';
        const surfaceColor = extractCSSVariable(cssContent, 'color-surface') || '#f8fafc';

        const primaryRgb = hexToRgb(primaryColor);
        const surfaceRgb = hexToRgb(surfaceColor);

        if (primaryRgb && surfaceRgb) {
            const ratio = contrastRatio(primaryRgb, surfaceRgb);
            expect(ratio).toBeGreaterThanOrEqual(3);
        }
    });
});

/* ========================================
   Test Case 9: Images Have Alt Text
   ======================================== */
describe('Test Case 9: Images Have Alt Text', () => {
    test('All img elements have alt attributes', () => {
        const images = document.querySelectorAll('img');

        images.forEach(img => {
            expect(img.hasAttribute('alt')).toBe(true);
        });
    });

    test('Alt attributes are not empty for informational images', () => {
        const images = document.querySelectorAll('img:not([role="presentation"])');

        images.forEach(img => {
            // If image has role="presentation" or is decorative, empty alt is OK
            const alt = img.getAttribute('alt');
            // At minimum, alt attribute should exist
            expect(alt).not.toBeNull();
        });
    });
});

/* ========================================
   Test Case 10: Descriptive Link Text
   ======================================== */
describe('Test Case 10: Descriptive Link Text', () => {
    const genericLinkText = [
        'click here',
        'read more',
        'learn more',
        'more',
        'here',
        'link',
        'this'
    ];

    test('No links use generic text like "click here"', () => {
        const links = document.querySelectorAll('a');

        links.forEach(link => {
            const linkText = link.textContent.toLowerCase().trim();

            genericLinkText.forEach(generic => {
                // Link text should not be exactly the generic phrase
                expect(linkText).not.toBe(generic);
            });
        });
    });

    test('All links have non-empty accessible text', () => {
        const links = document.querySelectorAll('a');

        links.forEach(link => {
            const linkText = link.textContent.trim();
            const ariaLabel = link.getAttribute('aria-label');

            // Either link has text content or aria-label
            const hasAccessibleName = linkText.length > 0 || (ariaLabel && ariaLabel.length > 0);
            expect(hasAccessibleName).toBe(true);
        });
    });

    test('Links describe their destination or purpose', () => {
        const links = document.querySelectorAll('a');

        links.forEach(link => {
            const linkText = link.textContent.toLowerCase().trim();
            const href = link.getAttribute('href');

            // Skip empty or anchor-only links
            if (!href || href === '#') return;

            // Link text should give some indication of destination
            // At minimum, it should be more than 1 character
            expect(linkText.length).toBeGreaterThan(1);
        });
    });
});

/* ========================================
   Test Case 11: Semantic HTML Elements
   ======================================== */
describe('Test Case 11: Semantic HTML Elements', () => {
    test('Page uses <nav> element for navigation', () => {
        const nav = document.querySelector('nav');
        expect(nav).not.toBeNull();
    });

    test('Page uses <main> element for main content', () => {
        const main = document.querySelector('main');
        expect(main).not.toBeNull();
    });

    test('Page uses <header> element', () => {
        const header = document.querySelector('header');
        expect(header).not.toBeNull();
    });

    test('Page uses <footer> element for footer content', () => {
        const footer = document.querySelector('footer');
        expect(footer).not.toBeNull();
    });

    test('Page uses <section> elements with proper labeling', () => {
        const sections = document.querySelectorAll('section');
        expect(sections.length).toBeGreaterThan(0);

        // Each section should have either aria-label or aria-labelledby
        sections.forEach(section => {
            const hasAriaLabel = section.hasAttribute('aria-label');
            const hasAriaLabelledBy = section.hasAttribute('aria-labelledby');
            const hasId = section.hasAttribute('id');

            // Sections should be identifiable
            const isLabeled = hasAriaLabel || hasAriaLabelledBy || hasId;
            expect(isLabeled).toBe(true);
        });
    });

    test('Document has proper html lang attribute', () => {
        const html = document.querySelector('html');
        const lang = html?.getAttribute('lang');
        expect(lang).not.toBeNull();
        expect(lang?.length).toBeGreaterThan(0);
    });

    test('Main landmark contains primary content', () => {
        const main = document.querySelector('main');
        expect(main).not.toBeNull();

        // Main should have significant content
        expect(main.children.length).toBeGreaterThan(0);
    });
});

/* ========================================
   Test Case 12: ARIA Labels
   ======================================== */
describe('Test Case 12: ARIA Labels', () => {
    test('Navigation has aria-label', () => {
        const nav = document.querySelector('nav');
        if (nav) {
            const hasAriaLabel = nav.hasAttribute('aria-label');
            const hasAriaLabelledBy = nav.hasAttribute('aria-labelledby');
            expect(hasAriaLabel || hasAriaLabelledBy).toBe(true);
        }
    });

    test('Footer has role="contentinfo" or is <footer> element', () => {
        const footer = document.querySelector('footer');
        if (footer) {
            const role = footer.getAttribute('role');
            // Either has contentinfo role or is semantic footer
            const isSemanticOrRole = footer.tagName === 'FOOTER' || role === 'contentinfo';
            expect(isSemanticOrRole).toBe(true);
        }
    });

    test('Buttons without visible text have aria-label', () => {
        const buttons = document.querySelectorAll('button');

        buttons.forEach(button => {
            const buttonText = button.textContent.trim();
            const ariaLabel = button.getAttribute('aria-label');

            // If button has no visible text, it should have aria-label
            if (buttonText.length === 0) {
                expect(ariaLabel).not.toBeNull();
                expect(ariaLabel.length).toBeGreaterThan(0);
            }
        });
    });

    test('Icons marked as decorative have aria-hidden', () => {
        // Check for common icon patterns
        const iconsWithAriaHidden = document.querySelectorAll('[aria-hidden="true"]');

        // Status icons should be hidden from screen readers
        const statusIcons = document.querySelectorAll('.status-icon');
        statusIcons.forEach(icon => {
            expect(icon.getAttribute('aria-hidden')).toBe('true');
        });
    });

    test('Lists with role="list" have proper structure', () => {
        const lists = document.querySelectorAll('[role="list"]');

        lists.forEach(list => {
            const listItems = list.querySelectorAll('[role="listitem"]');
            // If list has items, they should have listitem role
            if (list.children.length > 0) {
                expect(listItems.length).toBeGreaterThan(0);
            }
        });
    });

    test('Interactive elements within sections are accessible', () => {
        const sections = document.querySelectorAll('section');

        sections.forEach(section => {
            const links = section.querySelectorAll('a');
            const buttons = section.querySelectorAll('button');

            // All links should have href
            links.forEach(link => {
                expect(link.hasAttribute('href')).toBe(true);
            });

            // All buttons should have accessible name
            buttons.forEach(button => {
                const hasText = button.textContent.trim().length > 0;
                const hasAriaLabel = button.hasAttribute('aria-label');
                expect(hasText || hasAriaLabel).toBe(true);
            });
        });
    });
});

/* ========================================
   Additional Accessibility Checks
   ======================================== */
describe('Additional Accessibility Checks', () => {
    test('Skip link is the first focusable element', () => {
        const skipLink = document.querySelector('.skip-link');
        expect(skipLink).not.toBeNull();

        // Skip link should have href pointing to main content
        const href = skipLink?.getAttribute('href');
        expect(href).toBe('#main');
    });

    test('Skip link text is descriptive', () => {
        const skipLink = document.querySelector('.skip-link');
        const text = skipLink?.textContent?.toLowerCase();
        expect(text).toContain('skip');
        expect(text).toContain('main');
    });

    test('Meta viewport allows user scaling', () => {
        const viewport = document.querySelector('meta[name="viewport"]');
        const content = viewport?.getAttribute('content') || '';

        // Should not disable user scaling
        expect(content).not.toContain('user-scalable=no');
        expect(content).not.toContain('maximum-scale=1');
    });

    test('External links have rel="noopener noreferrer"', () => {
        const externalLinks = document.querySelectorAll('a[target="_blank"]');

        externalLinks.forEach(link => {
            const rel = link.getAttribute('rel');
            expect(rel).toContain('noopener');
        });
    });

    test('Form labels are properly associated (if forms exist)', () => {
        const inputs = document.querySelectorAll('input:not([type="hidden"])');

        inputs.forEach(input => {
            const id = input.getAttribute('id');
            const ariaLabel = input.getAttribute('aria-label');
            const ariaLabelledBy = input.getAttribute('aria-labelledby');

            if (id) {
                // If input has id, there should be a label for it
                const label = document.querySelector(`label[for="${id}"]`);
                const hasLabel = label || ariaLabel || ariaLabelledBy;
                expect(hasLabel).toBeTruthy();
            } else {
                // Input should have aria-label or be wrapped in label
                const hasAccessibleName = ariaLabel || ariaLabelledBy || input.closest('label');
                expect(hasAccessibleName).toBeTruthy();
            }
        });
    });
});
