/**
 * Primary CTA Button Functionality Tests
 *
 * Test cases for verifying the 'Get Started Free' button is prominently displayed
 * and correctly links to the registration page (REQ-2)
 */

const { JSDOM } = require('jsdom');
const fs = require('fs');
const path = require('path');

// Read the HTML file
const html = fs.readFileSync(path.resolve(__dirname, '../index.html'), 'utf8');

let dom;
let document;
let window;

describe('Primary CTA Button Functionality', () => {
    beforeEach(() => {
        dom = new JSDOM(html, {
            resources: 'usable',
            runScripts: 'dangerously'
        });
        document = dom.window.document;
        window = dom.window;
    });

    afterEach(() => {
        dom.window.close();
    });

    /**
     * Test Case 1: Render HeroSection component
     * Input: Render HeroSection component
     * Expected: Primary CTA button with text 'Get Started Free' is rendered
     */
    describe('Test Case 1: Primary CTA Button Rendering', () => {
        test('should render primary CTA button with text "Get Started Free"', () => {
            const ctaButton = document.querySelector('[data-testid="primary-cta-button"]');

            expect(ctaButton).not.toBeNull();
            expect(ctaButton.textContent.trim()).toBe('Get Started Free');
        });

        test('should render CTA button within hero section', () => {
            const heroSection = document.querySelector('[data-testid="hero-section"]');
            const ctaButton = document.querySelector('[data-testid="primary-cta-button"]');

            expect(heroSection).not.toBeNull();
            expect(ctaButton).not.toBeNull();
            expect(heroSection.contains(ctaButton)).toBe(true);
        });

        test('CTA button should be an anchor element for navigation', () => {
            const ctaButton = document.querySelector('[data-testid="primary-cta-button"]');

            expect(ctaButton.tagName).toBe('A');
        });

        test('CTA button should have role="button" for accessibility', () => {
            const ctaButton = document.querySelector('[data-testid="primary-cta-button"]');

            expect(ctaButton.getAttribute('role')).toBe('button');
        });
    });

    /**
     * Test Case 2: Click 'Get Started Free' button
     * Input: Click 'Get Started Free' button
     * Expected: Navigation to /register route occurs
     */
    describe('Test Case 2: CTA Button Navigation', () => {
        test('should have href pointing to /register', () => {
            const ctaButton = document.querySelector('[data-testid="primary-cta-button"]');

            expect(ctaButton.getAttribute('href')).toBe('/register');
        });

        test('clicking CTA button should trigger navigation to registration page', () => {
            const ctaButton = document.querySelector('[data-testid="primary-cta-button"]');

            // Verify the button is clickable (has href)
            const href = ctaButton.getAttribute('href');
            expect(href).toBe('/register');

            // Simulate click behavior verification
            let navigationTriggered = false;
            ctaButton.addEventListener('click', (e) => {
                navigationTriggered = true;
                e.preventDefault(); // Prevent actual navigation in test
            });

            ctaButton.click();
            expect(navigationTriggered).toBe(true);
        });

        test('CTA button href should not be empty or null', () => {
            const ctaButton = document.querySelector('[data-testid="primary-cta-button"]');
            const href = ctaButton.getAttribute('href');

            expect(href).not.toBeNull();
            expect(href).not.toBe('');
            expect(href.startsWith('/')).toBe(true);
        });
    });

    /**
     * Test Case 3: Check CTA button styling
     * Input: Check CTA button styling
     * Expected: Button has prominent styling (primary color, appropriate size)
     */
    describe('Test Case 3: CTA Button Styling', () => {
        test('should have btn-primary-cta class for prominent styling', () => {
            const ctaButton = document.querySelector('[data-testid="primary-cta-button"]');

            expect(ctaButton.classList.contains('btn-primary-cta')).toBe(true);
        });

        test('should have appropriate padding for prominent appearance', () => {
            const ctaButton = document.querySelector('[data-testid="primary-cta-button"]');
            const style = window.getComputedStyle(ctaButton);

            // Check that button has padding (not zero)
            const paddingTop = parseFloat(style.paddingTop);
            const paddingBottom = parseFloat(style.paddingBottom);
            const paddingLeft = parseFloat(style.paddingLeft);
            const paddingRight = parseFloat(style.paddingRight);

            expect(paddingTop).toBeGreaterThan(10);
            expect(paddingBottom).toBeGreaterThan(10);
            expect(paddingLeft).toBeGreaterThan(20);
            expect(paddingRight).toBeGreaterThan(20);
        });

        test('should have visible background color (not transparent)', () => {
            const ctaButton = document.querySelector('[data-testid="primary-cta-button"]');
            const style = window.getComputedStyle(ctaButton);

            // Background should not be transparent
            const bgColor = style.backgroundColor;
            expect(bgColor).not.toBe('transparent');
            expect(bgColor).not.toBe('rgba(0, 0, 0, 0)');
        });

        test('should have appropriate font size for visibility', () => {
            const ctaButton = document.querySelector('[data-testid="primary-cta-button"]');
            const style = window.getComputedStyle(ctaButton);

            const fontSizeValue = style.fontSize;
            // Check if font size is specified in rem (relative) or px (absolute)
            // 1.25rem = 20px at default base (16px), which is >= 16px
            if (fontSizeValue.includes('rem')) {
                const remValue = parseFloat(fontSizeValue);
                // 1.25rem is larger than 1rem (16px base) - acceptable
                expect(remValue).toBeGreaterThanOrEqual(1);
            } else {
                const fontSize = parseFloat(fontSizeValue);
                // Should be at least 16px for readability
                expect(fontSize).toBeGreaterThanOrEqual(16);
            }
        });

        test('should have bold font weight for emphasis', () => {
            const ctaButton = document.querySelector('[data-testid="primary-cta-button"]');
            const style = window.getComputedStyle(ctaButton);

            const fontWeight = parseInt(style.fontWeight, 10);
            // Font weight >= 600 is considered semi-bold or bold
            expect(fontWeight).toBeGreaterThanOrEqual(600);
        });

        test('should have border-radius for modern appearance', () => {
            const ctaButton = document.querySelector('[data-testid="primary-cta-button"]');
            const style = window.getComputedStyle(ctaButton);

            const borderRadius = parseFloat(style.borderRadius);
            expect(borderRadius).toBeGreaterThan(0);
        });

        test('should have cursor pointer for interactivity indication', () => {
            const ctaButton = document.querySelector('[data-testid="primary-cta-button"]');
            const style = window.getComputedStyle(ctaButton);

            expect(style.cursor).toBe('pointer');
        });

        test('should have minimum width for touch targets', () => {
            const ctaButton = document.querySelector('[data-testid="primary-cta-button"]');
            const style = window.getComputedStyle(ctaButton);

            const minWidth = parseFloat(style.minWidth);
            // Should have minimum width of at least 200px for good UX
            expect(minWidth).toBeGreaterThanOrEqual(200);
        });
    });

    /**
     * Test Case 4: Hover over CTA button
     * Input: Hover over CTA button
     * Expected: Button displays hover effect indicating interactivity
     */
    describe('Test Case 4: CTA Button Hover Effects', () => {
        test('should have transition property for smooth hover effects', () => {
            const ctaButton = document.querySelector('[data-testid="primary-cta-button"]');
            const style = window.getComputedStyle(ctaButton);

            // Check that transition is defined
            const transition = style.transition;
            expect(transition).not.toBe('none');
            expect(transition).not.toBe('');
        });

        test('should have box-shadow for depth and hover effect potential', () => {
            const ctaButton = document.querySelector('[data-testid="primary-cta-button"]');
            const style = window.getComputedStyle(ctaButton);

            const boxShadow = style.boxShadow;
            expect(boxShadow).not.toBe('none');
        });

        test('button styles should include hover state definitions', () => {
            // Verify CSS contains hover styles by checking the stylesheet
            const styleElement = document.querySelector('style');
            const cssText = styleElement.textContent;

            expect(cssText).toContain('.btn-primary-cta:hover');
            expect(cssText).toContain('transform');
        });

        test('button should have focus styles for accessibility', () => {
            const styleElement = document.querySelector('style');
            const cssText = styleElement.textContent;

            expect(cssText).toContain('.btn-primary-cta:focus');
            expect(cssText).toContain('outline');
        });

        test('button should have active state for click feedback', () => {
            const styleElement = document.querySelector('style');
            const cssText = styleElement.textContent;

            expect(cssText).toContain('.btn-primary-cta:active');
        });
    });

    /**
     * Additional tests for CTA button prominence
     */
    describe('CTA Button Prominence and Visibility', () => {
        test('CTA button should be visible without scrolling (in hero section)', () => {
            const heroSection = document.querySelector('[data-testid="hero-section"]');
            const ctaButton = document.querySelector('[data-testid="primary-cta-button"]');
            const style = window.getComputedStyle(heroSection);

            // Hero section should span full viewport height
            expect(style.minHeight).toBe('100vh');

            // CTA should be within hero
            expect(heroSection.contains(ctaButton)).toBe(true);
        });

        test('CTA button text should be clearly readable (white on colored background)', () => {
            const ctaButton = document.querySelector('[data-testid="primary-cta-button"]');
            const style = window.getComputedStyle(ctaButton);

            const textColor = style.color;
            // Should be white or near-white for contrast
            expect(textColor).toMatch(/rgb\(255,\s*255,\s*255\)|white/i);
        });

        test('hero section should have centered content layout', () => {
            const heroSection = document.querySelector('[data-testid="hero-section"]');
            const style = window.getComputedStyle(heroSection);

            expect(style.display).toBe('flex');
            expect(style.alignItems).toBe('center');
            expect(style.justifyContent).toBe('center');
        });
    });
});
