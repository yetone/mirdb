/**
 * Accessibility Compliance Tests
 * Owner: Scenario 8 - Accessibility Compliance
 *
 * Tests for:
 * - Heading hierarchy (h1-h6)
 * - Image alt text
 * - Color contrast ratios
 * - Link text descriptiveness
 * - Semantic HTML elements
 * - ARIA labels where needed
 */

const fs = require('fs');
const path = require('path');
const { loadHomepageHTML } = require('../setup/test-utils');

describe('Accessibility Compliance Tests', () => {
    beforeEach(() => {
        const html = loadHomepageHTML();
        document.body.innerHTML = html;
    });

    describe('Test Case 1: Heading Hierarchy', () => {
        test('Document has a single h1 element', () => {
            const h1Elements = document.querySelectorAll('h1');
            expect(h1Elements.length).toBe(1);
        });

        test('H1 contains the product name "MirDB"', () => {
            const h1 = document.querySelector('h1');
            expect(h1.textContent).toContain('MirDB');
        });

        test('Document has proper heading hierarchy without skipping levels', () => {
            const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
            const headingLevels = Array.from(headings).map(h => parseInt(h.tagName.charAt(1)));

            // First heading should be h1
            expect(headingLevels[0]).toBe(1);

            // Check that no level is skipped
            for (let i = 1; i < headingLevels.length; i++) {
                const currentLevel = headingLevels[i];
                const previousLevel = headingLevels[i - 1];
                // Can go deeper (at most one level at a time) or back up
                const levelDifference = currentLevel - previousLevel;
                expect(levelDifference).toBeLessThanOrEqual(1);
            }
        });

        test('All major sections have h2 headings', () => {
            const h2Elements = document.querySelectorAll('h2');
            expect(h2Elements.length).toBeGreaterThanOrEqual(3);

            // Check expected sections exist
            const h2Texts = Array.from(h2Elements).map(h2 => h2.textContent.toLowerCase());
            expect(h2Texts.some(text => text.includes('feature'))).toBe(true);
            expect(h2Texts.some(text => text.includes('quick') || text.includes('start') || text.includes('example'))).toBe(true);
        });
    });

    describe('Test Case 2: Image Alt Text', () => {
        test('All img elements have alt attributes', () => {
            const images = document.querySelectorAll('img');
            images.forEach(img => {
                expect(img.hasAttribute('alt')).toBe(true);
            });
        });

        test('All img alt attributes are non-empty and meaningful', () => {
            const images = document.querySelectorAll('img');
            images.forEach(img => {
                const alt = img.getAttribute('alt');
                expect(alt).toBeTruthy();
                expect(alt.length).toBeGreaterThan(0);
                // Alt text should not be just the filename
                expect(alt).not.toMatch(/\.(gif|jpg|jpeg|png|svg|webp)$/i);
            });
        });

        test('Logo image has descriptive alt text', () => {
            const logo = document.querySelector('.hero-logo, img[src*="logo"]');
            if (logo) {
                const alt = logo.getAttribute('alt');
                expect(alt).toBeTruthy();
                expect(alt.toLowerCase()).toContain('logo');
            }
        });

        test('Decorative icons are marked with aria-hidden', () => {
            const featureIcons = document.querySelectorAll('.feature-icon');
            featureIcons.forEach(icon => {
                expect(icon.getAttribute('aria-hidden')).toBe('true');
            });
        });
    });

    describe('Test Case 4: Color Contrast', () => {
        test('CSS custom properties define proper contrast colors', () => {
            // Read the CSS file to check color definitions
            const cssPath = path.join(__dirname, '../../styles/main.css');
            const css = fs.readFileSync(cssPath, 'utf-8');

            // Check that color variables are defined
            expect(css).toContain('--color-text');
            expect(css).toContain('--color-background');
            expect(css).toContain('--color-primary');
        });

        test('Text colors use high contrast values', () => {
            const cssPath = path.join(__dirname, '../../styles/main.css');
            const css = fs.readFileSync(cssPath, 'utf-8');

            // Light mode: dark text (#212529) on light background (#ffffff)
            // This provides a contrast ratio of approximately 14.5:1, well above 4.5:1
            expect(css).toMatch(/--color-text:\s*#212529/);
            expect(css).toMatch(/--color-background:\s*#ffffff/);
        });

        test('Primary color meets contrast requirements', () => {
            const cssPath = path.join(__dirname, '../../styles/main.css');
            const css = fs.readFileSync(cssPath, 'utf-8');

            // Primary blue #007acc on white has good contrast for large text
            // And white text on #007acc has contrast ratio of ~4.55:1
            expect(css).toMatch(/--color-primary:\s*#007acc/);
        });

        test('Button text has sufficient contrast', () => {
            const cssPath = path.join(__dirname, '../../styles/main.css');
            const css = fs.readFileSync(cssPath, 'utf-8');

            // Primary buttons use white text on colored background
            expect(css).toContain('.btn-primary');
            expect(css).toMatch(/\.btn-primary\s*\{[^}]*color:\s*white/);
        });
    });

    describe('Test Case 6: Link Text Descriptiveness', () => {
        test('No links use generic text like "click here"', () => {
            const links = document.querySelectorAll('a');
            const genericTexts = ['click here', 'here', 'read more', 'more', 'link'];

            links.forEach(link => {
                const text = link.textContent.trim().toLowerCase();
                genericTexts.forEach(generic => {
                    expect(text).not.toBe(generic);
                });
            });
        });

        test('Links with generic text have aria-labels for context', () => {
            const links = document.querySelectorAll('a');
            links.forEach(link => {
                const text = link.textContent.trim().toLowerCase();
                // If text might be ambiguous, check for aria-label
                if (text === 'github' || text.length < 10) {
                    const ariaLabel = link.getAttribute('aria-label');
                    // Either the link text should be descriptive or it has an aria-label
                    expect(text.length > 3 || ariaLabel).toBeTruthy();
                }
            });
        });

        test('CTA buttons have descriptive text or aria-labels', () => {
            const ctaGithub = document.querySelector('#cta-github, [href*="github"]');
            const ctaDocs = document.querySelector('#cta-docs, [href*="docs"], [href*="start"]');

            if (ctaGithub) {
                const hasDescriptiveContent =
                    ctaGithub.textContent.trim().length > 5 ||
                    ctaGithub.getAttribute('aria-label');
                expect(hasDescriptiveContent).toBeTruthy();
            }

            if (ctaDocs) {
                const hasDescriptiveContent =
                    ctaDocs.textContent.trim().length > 5 ||
                    ctaDocs.getAttribute('aria-label');
                expect(hasDescriptiveContent).toBeTruthy();
            }
        });

        test('Footer links have descriptive text or aria-labels', () => {
            const footer = document.querySelector('footer, .footer');
            if (footer) {
                const footerLinks = footer.querySelectorAll('a');
                footerLinks.forEach(link => {
                    const text = link.textContent.trim();
                    const ariaLabel = link.getAttribute('aria-label');
                    expect(text.length > 2 || ariaLabel).toBeTruthy();
                });
            }
        });
    });

    describe('Test Case 7: Semantic HTML Elements', () => {
        test('Page uses semantic section elements', () => {
            const sections = document.querySelectorAll('section');
            expect(sections.length).toBeGreaterThanOrEqual(1);
        });

        test('Page has a main content area or sections serve as main', () => {
            const main = document.querySelector('main');
            const sections = document.querySelectorAll('section');
            // Either has a <main> element or uses sections appropriately
            expect(main || sections.length > 0).toBeTruthy();
        });

        test('Page has a footer element', () => {
            const footer = document.querySelector('footer');
            expect(footer).toBeTruthy();
        });

        test('Footer has appropriate role', () => {
            const footer = document.querySelector('footer');
            expect(footer).toBeTruthy();
            // Footer element has implicit contentinfo role, or explicit role
            const role = footer.getAttribute('role');
            expect(!role || role === 'contentinfo').toBe(true);
        });

        test('Sections have aria-labelledby attributes linking to headings', () => {
            const sections = document.querySelectorAll('section[aria-labelledby]');
            // At least some sections should be properly labeled
            expect(sections.length).toBeGreaterThanOrEqual(1);

            sections.forEach(section => {
                const labelId = section.getAttribute('aria-labelledby');
                const labelElement = document.getElementById(labelId);
                expect(labelElement).toBeTruthy();
            });
        });

        test('Document uses semantic HTML5 elements appropriately', () => {
            // Check for semantic elements
            const hasSection = document.querySelectorAll('section').length > 0;
            const hasFooter = document.querySelector('footer') !== null;

            expect(hasSection).toBe(true);
            expect(hasFooter).toBe(true);
        });

        test('Code blocks use semantic pre and code elements', () => {
            const codeBlocks = document.querySelectorAll('pre code');
            expect(codeBlocks.length).toBeGreaterThanOrEqual(1);
        });
    });

    describe('Additional Accessibility Requirements', () => {
        test('Document has lang attribute', () => {
            // Since we're loading just the body content, we need to check the original HTML
            const htmlPath = path.join(__dirname, '../../index.html');
            const htmlContent = fs.readFileSync(htmlPath, 'utf-8');
            expect(htmlContent).toMatch(/<html[^>]*lang="en"/);
        });

        test('Buttons have accessible names', () => {
            const buttons = document.querySelectorAll('button');
            buttons.forEach(button => {
                const text = button.textContent.trim();
                const ariaLabel = button.getAttribute('aria-label');
                expect(text.length > 0 || ariaLabel).toBeTruthy();
            });
        });

        test('Interactive elements are not within non-interactive elements', () => {
            // Check that links are not nested (invalid HTML)
            const nestedLinks = document.querySelectorAll('a a');
            expect(nestedLinks.length).toBe(0);

            // Check that buttons are not inside links
            const buttonsInLinks = document.querySelectorAll('a button');
            expect(buttonsInLinks.length).toBe(0);
        });

        test('Form elements have labels or aria-labels if present', () => {
            const formElements = document.querySelectorAll('input, textarea, select');
            formElements.forEach(element => {
                const id = element.getAttribute('id');
                const ariaLabel = element.getAttribute('aria-label');
                const ariaLabelledBy = element.getAttribute('aria-labelledby');
                const label = id ? document.querySelector(`label[for="${id}"]`) : null;

                // Element should have either a label, aria-label, or aria-labelledby
                expect(label || ariaLabel || ariaLabelledBy).toBeTruthy();
            });
        });

        test('Skip links or proper document structure for navigation', () => {
            // The document should either have skip links or use proper landmark regions
            const skipLink = document.querySelector('a[href="#main"], a[href="#content"], .skip-link');
            const landmarkSections = document.querySelectorAll('section[aria-labelledby], footer[role="contentinfo"]');

            // Either has skip link or proper landmark structure
            expect(skipLink || landmarkSections.length > 0).toBeTruthy();
        });
    });
});
