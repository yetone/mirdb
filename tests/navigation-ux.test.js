/**
 * Navigation and User Experience Tests
 *
 * This test suite validates that the MirDB homepage has intuitive navigation
 * and good user experience, including navigation elements, links, and CTAs.
 */

const fs = require('fs');
const path = require('path');
const cheerio = require('cheerio');

// Load the homepage HTML
const htmlPath = path.join(__dirname, '..', 'index.html');
const htmlContent = fs.readFileSync(htmlPath, 'utf-8');
const $ = cheerio.load(htmlContent);

describe('Navigation and User Experience', () => {
    describe('Test Case 1: Navigation Element Presence', () => {
        it('should have a <nav> element for navigation', () => {
            const navElement = $('nav');
            expect(navElement.length).toBeGreaterThan(0);
        });

        it('should have the navbar class on navigation', () => {
            const navbar = $('nav.navbar');
            expect(navbar.length).toBe(1);
        });

        it('should have navigation links list', () => {
            const navLinks = $('nav ul.nav-links');
            expect(navLinks.length).toBe(1);
        });

        it('should have multiple navigation links', () => {
            const navItems = $('nav ul.nav-links li');
            expect(navItems.length).toBeGreaterThanOrEqual(3);
        });

        it('should have a logo or brand link', () => {
            const logo = $('nav .logo');
            expect(logo.length).toBe(1);
        });

        it('should have navigation within header element', () => {
            const navInHeader = $('header nav');
            expect(navInHeader.length).toBe(1);
        });
    });

    describe('Test Case 2: GitHub Link Presence', () => {
        it('should contain a link to GitHub repository', () => {
            const githubLinks = $('a[href*="github.com/yetone/mirdb"]');
            expect(githubLinks.length).toBeGreaterThan(0);
        });

        it('should have GitHub link in navigation', () => {
            const navGithubLink = $('nav a[href*="github.com/yetone/mirdb"]');
            expect(navGithubLink.length).toBeGreaterThan(0);
        });

        it('should open GitHub link in new tab', () => {
            const githubLinks = $('a[href*="github.com/yetone/mirdb"]');
            githubLinks.each((index, element) => {
                const target = $(element).attr('target');
                expect(target).toBe('_blank');
            });
        });

        it('should have noopener noreferrer for security on external links', () => {
            const githubLinks = $('a[href*="github.com/yetone/mirdb"]');
            githubLinks.each((index, element) => {
                const rel = $(element).attr('rel');
                expect(rel).toContain('noopener');
            });
        });

        it('should have GitHub text visible in navigation', () => {
            const navGithubLink = $('nav a[href*="github.com/yetone/mirdb"]');
            const linkText = navGithubLink.text().toLowerCase();
            expect(linkText).toContain('github');
        });
    });

    describe('Test Case 3: Call-to-Action Buttons', () => {
        it('should have a Get Started button', () => {
            const pageText = $('body').text().toLowerCase();
            expect(pageText).toContain('get started');
        });

        it('should have CTA buttons container', () => {
            const ctaButtons = $('.cta-buttons');
            expect(ctaButtons.length).toBe(1);
        });

        it('should have primary CTA button (Get Started)', () => {
            const primaryBtn = $('.cta-buttons .btn-primary');
            expect(primaryBtn.length).toBe(1);
        });

        it('should have secondary CTA button (View on GitHub)', () => {
            const secondaryBtn = $('.cta-buttons .btn-secondary');
            expect(secondaryBtn.length).toBe(1);
        });

        it('Get Started button should link to getting-started section', () => {
            const getStartedBtn = $('a:contains("Get Started")');
            expect(getStartedBtn.length).toBeGreaterThan(0);
            const href = getStartedBtn.first().attr('href');
            expect(href).toBe('#getting-started');
        });

        it('View on GitHub button should link to repository', () => {
            const githubBtn = $('a:contains("View on GitHub")');
            expect(githubBtn.length).toBeGreaterThan(0);
            const href = githubBtn.first().attr('href');
            expect(href).toBe('https://github.com/yetone/mirdb');
        });
    });

    describe('Test Case 4: Internal Links Validation', () => {
        it('should have Features link pointing to #features section', () => {
            const featuresLink = $('a[href="#features"]');
            expect(featuresLink.length).toBeGreaterThan(0);
        });

        it('should have Getting Started link pointing to #getting-started section', () => {
            const gettingStartedLink = $('a[href="#getting-started"]');
            expect(gettingStartedLink.length).toBeGreaterThan(0);
        });

        it('should have Architecture link pointing to #architecture section', () => {
            const archLink = $('a[href="#architecture"]');
            expect(archLink.length).toBeGreaterThan(0);
        });

        it('should have features section with matching id', () => {
            const featuresSection = $('#features');
            expect(featuresSection.length).toBe(1);
        });

        it('should have getting-started section with matching id', () => {
            const gettingStartedSection = $('#getting-started');
            expect(gettingStartedSection.length).toBe(1);
        });

        it('should have architecture section with matching id', () => {
            const archSection = $('#architecture');
            expect(archSection.length).toBe(1);
        });

        it('should have hero section with id', () => {
            const heroSection = $('#hero');
            expect(heroSection.length).toBe(1);
        });

        it('home logo link should point to root', () => {
            const logoLink = $('nav .logo a');
            expect(logoLink.length).toBe(1);
            const href = logoLink.attr('href');
            expect(href).toBe('/');
        });

        it('all internal anchor links should have corresponding sections', () => {
            const internalLinks = $('a[href^="#"]');
            internalLinks.each((index, element) => {
                const href = $(element).attr('href');
                if (href && href.length > 1) {
                    const targetId = href.substring(1);
                    const targetSection = $(`#${targetId}`);
                    expect(targetSection.length).toBeGreaterThan(0);
                }
            });
        });
    });

    describe('Test Case 5: External Links Validation', () => {
        it('should have GitHub repository link', () => {
            const githubRepoLink = $('a[href="https://github.com/yetone/mirdb"]');
            expect(githubRepoLink.length).toBeGreaterThan(0);
        });

        it('should have GitHub issues link in footer', () => {
            const issuesLink = $('footer a[href*="github.com/yetone/mirdb/issues"]');
            expect(issuesLink.length).toBe(1);
        });

        it('all external links should have target="_blank"', () => {
            const externalLinks = $('a[href^="https://"], a[href^="http://"]');
            externalLinks.each((index, element) => {
                const target = $(element).attr('target');
                expect(target).toBe('_blank');
            });
        });

        it('all external links should have rel="noopener noreferrer"', () => {
            const externalLinks = $('a[href^="https://"], a[href^="http://"]');
            externalLinks.each((index, element) => {
                const rel = $(element).attr('rel');
                expect(rel).toContain('noopener');
                expect(rel).toContain('noreferrer');
            });
        });

        it('GitHub links should point to valid repository URL format', () => {
            const githubLinks = $('a[href*="github.com"]');
            githubLinks.each((index, element) => {
                const href = $(element).attr('href');
                expect(href).toMatch(/^https:\/\/github\.com\/yetone\/mirdb/);
            });
        });

        it('should not have any broken href attributes', () => {
            const links = $('a');
            links.each((index, element) => {
                const href = $(element).attr('href');
                // href should exist and not be empty
                expect(href).toBeDefined();
                expect(href.length).toBeGreaterThan(0);
            });
        });
    });

    describe('Navigation Structure and Accessibility', () => {
        it('navigation links should have text content', () => {
            const navLinks = $('nav ul.nav-links li a');
            navLinks.each((index, element) => {
                const text = $(element).text().trim();
                expect(text.length).toBeGreaterThan(0);
            });
        });

        it('should have logical navigation order', () => {
            const navLinks = $('nav ul.nav-links li a');
            const expectedOrder = ['Features', 'Getting Started', 'Architecture', 'GitHub'];
            const actualOrder = [];
            navLinks.each((index, element) => {
                actualOrder.push($(element).text().trim());
            });
            expect(actualOrder).toEqual(expectedOrder);
        });

        it('should have footer with useful links', () => {
            const footerLinks = $('footer a');
            expect(footerLinks.length).toBeGreaterThan(0);
        });

        it('footer should contain repository link', () => {
            const footerRepoLink = $('footer a[href*="github.com/yetone/mirdb"]');
            expect(footerRepoLink.length).toBeGreaterThan(0);
        });
    });

    describe('User Experience Quality', () => {
        it('should have clear section headings', () => {
            const h2Elements = $('main h2');
            expect(h2Elements.length).toBeGreaterThanOrEqual(4);
        });

        it('should have consistent button styling classes', () => {
            const buttons = $('.btn');
            expect(buttons.length).toBeGreaterThan(0);
        });

        it('should have badge elements for visual interest', () => {
            const badges = $('.badges .badge');
            expect(badges.length).toBeGreaterThan(0);
        });

        it('should have proper container structure', () => {
            const containers = $('.container');
            expect(containers.length).toBeGreaterThan(0);
        });
    });
});
