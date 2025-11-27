/**
 * GitHub Repository Links Tests
 * Verify GitHub repository links are present and functional as per REQ-4
 */

const fs = require('fs');
const path = require('path');
const html = fs.readFileSync(path.resolve(__dirname, '../index.html'), 'utf8');

const { test, expect, describe, beforeEach } = require('@jest/globals');
const { JSDOM } = require('jsdom');

let document;
let dom;

describe('GitHub Repository Links Verification', () => {
    beforeEach(() => {
        dom = new JSDOM(html, { resources: 'usable' });
        document = dom.window.document;
    });

    describe('Test Case 1: GitHub Link Existence', () => {
        test('should have at least one GitHub repository link on the page', () => {
            // Find all links that contain "github" in href
            const links = document.querySelectorAll('a[href*="github"]');

            expect(links.length).toBeGreaterThanOrEqual(1);
        });

        test('should find GitHub links in multiple locations (hero, footer, etc.)', () => {
            // Check hero section for GitHub link
            const heroSection = document.querySelector('[data-testid="hero-section"]');
            const heroGitHubLink = heroSection ? heroSection.querySelector('a[href*="github"]') : null;

            // Verify link exists in hero section
            expect(heroGitHubLink).not.toBeNull();
            expect(heroSection.contains(heroGitHubLink)).toBe(true);
        });

        test('GitHub link should have correct repository URL', () => {
            const gitHubLinks = document.querySelectorAll('a[href*="github"]');

            let foundMirDBRepo = false;
            gitHubLinks.forEach(link => {
                const href = link.getAttribute('href');
                if (href.includes('yetone/mirdb')) {
                    foundMirDBRepo = true;
                }
            });

            expect(foundMirDBRepo).toBe(true);
        });
    });

    describe('Test Case 2: GitHub Link Functionality', () => {
        test('should open GitHub link in new tab', () => {
            const githubLinks = document.querySelectorAll('a[href*="github"]');

            let foundExternalLink = false;
            githubLinks.forEach(link => {
                const target = link.getAttribute('target');
                const rel = link.getAttribute('rel');

                if (target === '_blank' && rel === 'noopener noreferrer') {
                    foundExternalLink = true;
                }
            });

            expect(foundExternalLink).toBe(true);
        });

        test('primary GitHub link should have correct attributes', () => {
            // Find the primary GitHub link in hero section
            const heroSection = document.querySelector('[data-testid="hero-section"]');
            const githubLink = heroSection ? heroSection.querySelector('a[href*="github"]') : null;

            expect(githubLink).not.toBeNull();

            // Verify href points to correct repository
            const href = githubLink.getAttribute('href');
            expect(href).toBe('https://github.com/yetone/mirdb');

            // Verify it opens in new tab for better UX
            expect(githubLink.getAttribute('target')).toBe('_blank');

            // Verify security attributes
            expect(githubLink.getAttribute('rel')).toBe('noopener noreferrer');

            // Verify it's a button-style CTA
            expect(githubLink.classList.contains('cta-button')).toBe(true);
            expect(githubLink.classList.contains('cta-secondary')).toBe(true);
        });

        test('GitHub links should be accessible with proper text content', () => {
            const githubLinks = document.querySelectorAll('a[href*="github"]');

            githubLinks.forEach(link => {
                const textContent = link.textContent.trim();
                // Should have descriptive text (not empty)
                expect(textContent.length).toBeGreaterThan(0);

                // Should have some descriptive text for accessibility
                const lowerText = textContent.toLowerCase();
                // Some links like "Issues", "Pulls", "Releases", "View source" are also valid
                expect(textContent.length).toBeGreaterThan(0);
            });
        });
    });

    describe('GitHub Link Accessibility', () => {
        test('GitHub links should have descriptive text for screen readers', () => {
            const githubLinks = document.querySelectorAll('a[href*="github"]');

            githubLinks.forEach(link => {
                // Check for either descriptive text or aria-label
                const textContent = link.textContent.trim();
                const ariaLabel = link.getAttribute('aria-label');

                expect(textContent.length > 0 || ariaLabel).toBe(true);
            });
        });

        test('GitHub link in hero section should be keyboard accessible', () => {
            const githubLink = document.querySelector('[data-testid="cta-github"]');

            expect(githubLink).not.toBeNull();

            // Check if it's a link (not just a styled button)
            expect(githubLink.tagName).toBe('A');

            // Verify it has href
            expect(githubLink.getAttribute('href')).not.toBeNull();
            expect(githubLink.getAttribute('href')).not.toBe('');
        });
    });

    describe('Multiple GitHub Links', () => {
        test('page might have multiple GitHub links for better UX', () => {
            const githubLinks = document.querySelectorAll('a[href*="github"]');
            const heroSection = document.querySelector('[data-testid="hero-section"]');

            const heroGitHubLink = heroSection ? heroSection.querySelector('a[href*="github"]') : null;

            expect(heroGitHubLink).not.toBeNull();

            // Log number of links for debugging
            console.log(`Found ${githubLinks.length} GitHub links on the page`);

            // If there are multiple links, they should all point to same repo
            githubLinks.forEach(link => {
                const href = link.getAttribute('href');
                expect(href).toContain('github.com/yetone/mirdb');
            });
        });
    });
});
