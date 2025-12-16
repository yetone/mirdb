/**
 * Footer Section Tests
 *
 * Verifies that the footer contains required elements as specified in PRD Interface Requirements:
 * - Footer element exists
 * - GitHub repository link
 * - License information (MIT)
 * - Technology credits (Rust and Tokio)
 */

const fs = require('fs');
const path = require('path');
const cheerio = require('cheerio');

describe('Footer Section', () => {
    let $;
    let html;

    beforeAll(() => {
        const htmlPath = path.join(__dirname, '..', 'index.html');
        html = fs.readFileSync(htmlPath, 'utf8');
        $ = cheerio.load(html);
    });

    describe('Test Case 1: Footer Element Exists', () => {
        test('Page should contain a <footer> element', () => {
            const footerElement = $('footer');
            expect(footerElement.length).toBe(1);
        });

        test('Footer should be at the bottom of the document (after main)', () => {
            const body = $('body');
            const children = body.children();
            const lastChild = children.last();
            expect(lastChild.is('footer')).toBe(true);
        });
    });

    describe('Test Case 2: GitHub Repository Link', () => {
        test('Footer should contain a link to GitHub repository', () => {
            const footerGithubLinks = $('footer a[href*="github.com"]');
            expect(footerGithubLinks.length).toBeGreaterThan(0);
        });

        test('GitHub link should point to the correct repository', () => {
            const githubRepoLink = $('footer a[href*="github.com/yetone/mirdb"]');
            expect(githubRepoLink.length).toBeGreaterThan(0);
        });

        test('GitHub link should open in a new tab (target="_blank")', () => {
            const githubRepoLink = $('footer a[href*="github.com/yetone/mirdb"]').first();
            expect(githubRepoLink.attr('target')).toBe('_blank');
        });

        test('GitHub link should have rel="noopener noreferrer" for security', () => {
            const githubRepoLink = $('footer a[href*="github.com/yetone/mirdb"]').first();
            expect(githubRepoLink.attr('rel')).toContain('noopener');
        });
    });

    describe('Test Case 3: License Information', () => {
        test('Footer should display MIT license information', () => {
            const footerText = $('footer').text().toLowerCase();
            expect(footerText).toContain('mit');
        });

        test('MIT license should be clearly visible in footer bottom section', () => {
            const footerBottom = $('.footer-bottom');
            expect(footerBottom.length).toBe(1);
            expect(footerBottom.text().toLowerCase()).toContain('mit');
        });
    });

    describe('Test Case 4: Rust Technology Credit', () => {
        test('Footer should mention Rust', () => {
            const footerText = $('footer').text().toLowerCase();
            expect(footerText).toContain('rust');
        });

        test('Footer should mention "Built with Rust" or similar', () => {
            const footerText = $('footer').text().toLowerCase();
            expect(footerText).toMatch(/built\s+with.*rust|rust/);
        });
    });

    describe('Test Case 5: Tokio Runtime Credit', () => {
        test('Footer should mention Tokio runtime', () => {
            const footerText = $('footer').text().toLowerCase();
            expect(footerText).toContain('tokio');
        });

        test('Technology section should credit both Rust and Tokio together', () => {
            const techSection = $('footer .footer-section').filter((_, el) => {
                return $(el).find('h3').text().toLowerCase() === 'technology';
            });
            expect(techSection.length).toBe(1);
            const techText = techSection.text().toLowerCase();
            expect(techText).toContain('rust');
            expect(techText).toContain('tokio');
        });
    });

    describe('Footer Structure', () => {
        test('Footer should have a container for proper layout', () => {
            const footerContainer = $('footer .container');
            expect(footerContainer.length).toBe(1);
        });

        test('Footer should have multiple sections', () => {
            const footerSections = $('footer .footer-section');
            expect(footerSections.length).toBeGreaterThanOrEqual(2);
        });

        test('Footer should contain a logo', () => {
            const footerLogo = $('footer .footer-logo');
            expect(footerLogo.length).toBe(1);
        });
    });
});
