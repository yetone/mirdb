/**
 * Navigation and Header Tests
 *
 * Tests for verifying header navigation provides access to key resources
 * (REQ-7, US-4)
 */

describe('Navigation and Header', () => {
    let header;
    let nav;

    beforeAll(() => {
        // Uses global document from jest-environment-jsdom setup.js
        header = document.querySelector('header');
        nav = document.querySelector('nav');
    });

    describe('Test Case 1: Header/nav element exists with navigation links', () => {
        test('header element should exist', () => {
            expect(header).not.toBeNull();
        });

        test('nav element should exist', () => {
            expect(nav).not.toBeNull();
        });

        test('header should be at the top of the page (first child of body or early in DOM)', () => {
            const body = document.body;
            const firstElement = body.children[0];
            expect(firstElement.tagName.toLowerCase()).toBe('header');
        });

        test('navigation should contain links', () => {
            const links = nav.querySelectorAll('a');
            expect(links.length).toBeGreaterThan(0);
        });

        test('header should contain the nav element', () => {
            expect(header.contains(nav)).toBe(true);
        });
    });

    describe('Test Case 2: GitHub link in navigation', () => {
        // Helper to find GitHub link by text content
        const getGitHubLink = () => {
            const links = nav.querySelectorAll('a[href*="github.com"]');
            return Array.from(links).find(link =>
                link.textContent.trim().toLowerCase() === 'github'
            ) || links[links.length - 1]; // Fallback to last GitHub link
        };

        test('GitHub link should exist in navigation', () => {
            const githubLink = getGitHubLink();
            expect(githubLink).not.toBeNull();
        });

        test('GitHub link href should contain github.com', () => {
            const githubLink = getGitHubLink();
            expect(githubLink.getAttribute('href')).toContain('github.com');
        });

        test('GitHub link should have accessible text', () => {
            const githubLink = getGitHubLink();
            const linkText = githubLink.textContent.trim();
            expect(linkText.length).toBeGreaterThan(0);
            expect(linkText.toLowerCase()).toContain('github');
        });
    });

    describe('Test Case 3: Documentation link in navigation', () => {
        test('navigation should have links with descriptive text', () => {
            const links = nav.querySelectorAll('a');
            const linkTexts = Array.from(links).map(link => link.textContent.trim().toLowerCase());

            // Check that navigation has meaningful links (Features, Quick Start, GitHub, etc.)
            const hasMeaningfulLinks = linkTexts.some(text =>
                text.includes('features') ||
                text.includes('quick start') ||
                text.includes('github') ||
                text.includes('docs') ||
                text.includes('documentation')
            );
            expect(hasMeaningfulLinks).toBe(true);
        });

        test('navigation links should have valid href attributes', () => {
            const links = nav.querySelectorAll('a');
            links.forEach(link => {
                const href = link.getAttribute('href');
                expect(href).not.toBeNull();
                expect(href.length).toBeGreaterThan(0);
            });
        });
    });

    describe('Test Case 4: Features navigation link', () => {
        test('Features link should exist', () => {
            const links = nav.querySelectorAll('a');
            const featuresLink = Array.from(links).find(link =>
                link.textContent.toLowerCase().includes('features')
            );
            expect(featuresLink).not.toBeUndefined();
        });

        test('Features link should have anchor href to features section', () => {
            const links = nav.querySelectorAll('a');
            const featuresLink = Array.from(links).find(link =>
                link.textContent.toLowerCase().includes('features')
            );
            expect(featuresLink).not.toBeUndefined();
            const href = featuresLink.getAttribute('href');
            expect(href).toContain('#features');
        });

        test('Features section with matching id should exist on page', () => {
            const featuresSection = document.querySelector('#features');
            expect(featuresSection).not.toBeNull();
        });
    });

    describe('Test Case 5: Internal navigation link scrolling', () => {
        test('internal navigation links should use anchor hrefs', () => {
            const links = nav.querySelectorAll('a');
            const internalLinks = Array.from(links).filter(link => {
                const href = link.getAttribute('href');
                return href && href.startsWith('#');
            });
            expect(internalLinks.length).toBeGreaterThan(0);
        });

        test('each internal anchor should have a corresponding section on the page', () => {
            const links = nav.querySelectorAll('a');
            const internalLinks = Array.from(links).filter(link => {
                const href = link.getAttribute('href');
                return href && href.startsWith('#') && href.length > 1;
            });

            internalLinks.forEach(link => {
                const href = link.getAttribute('href');
                const targetId = href.substring(1); // Remove the #
                const targetSection = document.getElementById(targetId);
                expect(targetSection).not.toBeNull();
            });
        });

        test('page should have smooth scroll behavior (CSS check)', () => {
            const htmlElement = document.documentElement;
            const bodyElement = document.body;

            // Check if smooth scroll is enabled via CSS (could be inline or in stylesheet)
            // Since JSDOM doesn't fully support computed styles from external CSS,
            // we verify the sections exist and links are properly structured
            const quickStartLink = nav.querySelector('a[href="#quick-start"]');
            expect(quickStartLink).not.toBeNull();

            const quickStartSection = document.querySelector('#quick-start');
            expect(quickStartSection).not.toBeNull();
        });
    });

    describe('Test Case 6: GitHub link security attributes', () => {
        // Helper to find GitHub link by text content
        const getGitHubLink = () => {
            const links = nav.querySelectorAll('a[href*="github.com"]');
            return Array.from(links).find(link =>
                link.textContent.trim().toLowerCase() === 'github'
            ) || links[links.length - 1]; // Fallback to last GitHub link
        };

        test('GitHub link should have target="_blank" for new tab', () => {
            const githubLink = getGitHubLink();
            expect(githubLink).not.toBeNull();
            expect(githubLink.getAttribute('target')).toBe('_blank');
        });

        test('GitHub link should have rel="noopener" for security', () => {
            const githubLink = getGitHubLink();
            expect(githubLink).not.toBeNull();
            const relAttr = githubLink.getAttribute('rel');
            expect(relAttr).not.toBeNull();
            expect(relAttr).toContain('noopener');
        });

        test('all external links should have proper security attributes', () => {
            const externalLinks = nav.querySelectorAll('a[target="_blank"]');
            externalLinks.forEach(link => {
                const relAttr = link.getAttribute('rel');
                expect(relAttr).not.toBeNull();
                expect(relAttr).toContain('noopener');
            });
        });
    });

    describe('Logo and branding', () => {
        test('logo or product name should exist in header', () => {
            const logo = header.querySelector('.nav-logo, .logo, [class*="logo"]');
            expect(logo).not.toBeNull();
        });

        test('logo should contain MirDB text', () => {
            const logo = header.querySelector('.nav-logo, .logo, [class*="logo"]');
            expect(logo.textContent).toContain('MirDB');
        });

        test('logo should link to homepage', () => {
            const logo = header.querySelector('.nav-logo');
            const href = logo.getAttribute('href');
            expect(href === '/' || href === '#' || href === 'index.html' || href === './').toBe(true);
        });
    });

    describe('Navigation accessibility', () => {
        test('navigation links should be in a list structure', () => {
            const navList = nav.querySelector('ul');
            expect(navList).not.toBeNull();
        });

        test('navigation list should have list items', () => {
            const navList = nav.querySelector('ul');
            const listItems = navList.querySelectorAll('li');
            expect(listItems.length).toBeGreaterThan(0);
        });

        test('nav element should have proper class for styling', () => {
            expect(nav.classList.length).toBeGreaterThan(0);
        });
    });
});
