/**
 * Hero Section Display Tests
 *
 * Tests for verifying the hero section displays product name, tagline,
 * and value proposition prominently (REQ-1, REQ-2, US-1)
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

describe('Hero Section Display', () => {
    let document;
    let heroSection;

    beforeAll(() => {
        const htmlPath = path.join(__dirname, '..', 'index.html');
        const htmlContent = fs.readFileSync(htmlPath, 'utf-8');
        const dom = new JSDOM(htmlContent);
        document = dom.window.document;
        heroSection = document.querySelector('#hero');
    });

    describe('Test Case 1: Hero section contains h1 element with MirDB text', () => {
        test('hero section should exist', () => {
            expect(heroSection).not.toBeNull();
        });

        test('hero section should contain h1 element', () => {
            const h1 = heroSection.querySelector('h1');
            expect(h1).not.toBeNull();
        });

        test('h1 element should contain "MirDB" text', () => {
            const h1 = heroSection.querySelector('h1');
            expect(h1.textContent).toContain('MirDB');
        });
    });

    describe('Test Case 2: Tagline mentions persistent key-value store and memcached', () => {
        test('hero section should contain tagline element', () => {
            const tagline = heroSection.querySelector('.tagline');
            expect(tagline).not.toBeNull();
        });

        test('tagline should mention "persistent key-value store"', () => {
            const tagline = heroSection.querySelector('.tagline');
            expect(tagline.textContent.toLowerCase()).toContain('persistent key-value store');
        });

        test('tagline should mention "memcached"', () => {
            const tagline = heroSection.querySelector('.tagline');
            expect(tagline.textContent.toLowerCase()).toContain('memcached');
        });
    });

    describe('Test Case 3: CTA button exists with proper text and href', () => {
        test('CTA buttons container should exist', () => {
            const ctaButtons = heroSection.querySelector('.cta-buttons');
            expect(ctaButtons).not.toBeNull();
        });

        test('primary CTA button should exist', () => {
            const primaryBtn = heroSection.querySelector('.btn-primary');
            expect(primaryBtn).not.toBeNull();
        });

        test('CTA button should have "Get Started" or "View on GitHub" text', () => {
            const buttons = heroSection.querySelectorAll('.cta-buttons a');
            const buttonTexts = Array.from(buttons).map(btn => btn.textContent.trim());
            const hasValidCTA = buttonTexts.some(text =>
                text === 'Get Started' || text === 'View on GitHub'
            );
            expect(hasValidCTA).toBe(true);
        });

        test('CTA buttons should have href attribute', () => {
            const buttons = heroSection.querySelectorAll('.cta-buttons a');
            buttons.forEach(btn => {
                expect(btn.hasAttribute('href')).toBe(true);
                expect(btn.getAttribute('href')).not.toBe('');
            });
        });

        test('GitHub button should link to GitHub repository', () => {
            const githubBtn = heroSection.querySelector('a[href*="github"]');
            expect(githubBtn).not.toBeNull();
            expect(githubBtn.getAttribute('href')).toContain('github.com');
        });
    });

    describe('Test Case 4: Hero section uses semantic HTML with appropriate heading hierarchy', () => {
        test('hero section should use section element', () => {
            expect(heroSection.tagName.toLowerCase()).toBe('section');
        });

        test('hero section should have an id attribute', () => {
            expect(heroSection.hasAttribute('id')).toBe(true);
        });

        test('h1 should be the primary heading in hero section', () => {
            const h1 = heroSection.querySelector('h1');
            expect(h1).not.toBeNull();
            // Verify there's only one h1 in the hero section
            const allH1s = heroSection.querySelectorAll('h1');
            expect(allH1s.length).toBe(1);
        });

        test('hero section should be inside main element', () => {
            const main = document.querySelector('main');
            expect(main).not.toBeNull();
            expect(main.contains(heroSection)).toBe(true);
        });

        test('page should have proper semantic structure (header, main, footer)', () => {
            expect(document.querySelector('header')).not.toBeNull();
            expect(document.querySelector('main')).not.toBeNull();
            expect(document.querySelector('footer')).not.toBeNull();
        });

        test('hero section should contain paragraph elements for content', () => {
            const paragraphs = heroSection.querySelectorAll('p');
            expect(paragraphs.length).toBeGreaterThanOrEqual(1);
        });
    });
});
