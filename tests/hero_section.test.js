/**
 * Hero Section Display and Branding Tests
 * Test cases for verifying the hero section displays project name, logo, and tagline
 */

// Mock DOM environment for testing
const { JSDOM } = require('jsdom');
const fs = require('fs');
const path = require('path');

// Read the HTML file
const html = fs.readFileSync(path.resolve(__dirname, '../index.html'), 'utf8');

let dom;
let document;
let window;

describe('Hero Section Display and Branding', () => {
    beforeEach(() => {
        dom = new JSDOM(html, { resources: 'usable' });
        document = dom.window.document;
        window = dom.window;
    });

    describe('Test Case 1: Project Name Display', () => {
        test('should prominently display project name "MirDB"', () => {
            const projectName = document.querySelector('[data-testid="hero-title"]');

            expect(projectName).not.toBeNull();
            expect(projectName.textContent.trim()).toBe('MirDB');
            expect(projectName.tagName).toBe('H1');

            // Verify it's in the hero section
            const heroSection = document.querySelector('[data-testid="hero-section"]');
            expect(heroSection.contains(projectName)).toBe(true);
        });
    });

    describe('Test Case 2: Logo Display', () => {
        test('should load and display project logo', () => {
            const logo = document.querySelector('[data-testid="hero-logo"]');

            expect(logo).not.toBeNull();
            expect(logo.tagName).toBe('IMG');
            expect(logo.getAttribute('src')).toContain('assets/logo.gif');
            expect(logo.getAttribute('alt')).toBe('MirDB Logo');

            // Verify logo is visible in hero section
            const heroSection = document.querySelector('[data-testid="hero-section"]');
            expect(heroSection.contains(logo)).toBe(true);

            // Verify logo container exists
            const logoContainer = logo.parentElement;
            expect(logoContainer.classList.contains('hero-logo')).toBe(true);
        });
    });

    describe('Test Case 3: Tagline Display', () => {
        test('should display clear tagline explaining project purpose', () => {
            const tagline = document.querySelector('[data-testid="hero-tagline"]');

            expect(tagline).not.toBeNull();
            expect(tagline.textContent).toContain('persistent key-value store');
            expect(tagline.textContent).toContain('Memcached protocol');
            expect(tagline.tagName).toBe('P');

            // Verify tagline is in hero section
            const heroSection = document.querySelector('[data-testid="hero-section"]');
            expect(heroSection.contains(tagline)).toBe(true);
        });

        test('tagline should clearly explain MirDB purpose within first viewport', () => {
            const tagline = document.querySelector('[data-testid="hero-tagline"]');
            const text = tagline.textContent.toLowerCase();

            // Check for key concepts
            expect(text).toMatch(/persistent|key-value|store|database/);
            expect(text).toMatch(/memcached|protocol/);
            expect(text).toMatch(/support|compatible|based/);
        });
    });

    describe('Hero Section Layout', () => {
        test('all hero elements should be in first viewport without scrolling', () => {
            const heroSection = document.querySelector('[data-testid="hero-section"]');

            expect(heroSection).not.toBeNull();
            expect(heroSection.classList.contains('hero-section')).toBe(true);

            // Verify min-height is viewport height
            const style = window.getComputedStyle(heroSection);
            expect(style.minHeight).toBe('100vh');
        });

        test('hero section should be centered and properly styled', () => {
            const heroSection = document.querySelector('[data-testid="hero-section"]');
            const style = window.getComputedStyle(heroSection);

            expect(style.display).toBe('flex');
            expect(style.flexDirection).toBe('column');
            expect(style.alignItems).toBe('center');
            expect(style.justifyContent).toBe('center');
            expect(style.textAlign).toBe('center');
        });
    });

    describe('Branding Elements', () => {
        test('project name should be prominent with H1 tag', () => {
            const projectName = document.querySelector('[data-testid="hero-title"]');

            expect(projectName.tagName).toBe('H1');
        });

        test('logo should be appropriately sized in CSS classes', () => {
            const logoContainer = document.querySelector('.hero-logo');

            expect(logoContainer).not.toBeNull();
            expect(logoContainer.classList.contains('hero-logo')).toBe(true);
        });
    });
});

module.exports = { html };
