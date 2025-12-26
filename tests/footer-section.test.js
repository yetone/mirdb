/**
 * Footer Section Tests
 *
 * Tests for verifying the footer contains essential links and information
 * (REQ-7, REQ-10)
 */

describe('Footer Section', () => {
    let footer;

    beforeAll(() => {
        footer = document.querySelector('footer');
    });

    describe('Test Case 1: Footer element exists with semantic HTML <footer> tag', () => {
        test('footer element should exist', () => {
            expect(footer).not.toBeNull();
        });

        test('footer should use semantic <footer> tag', () => {
            expect(footer.tagName.toLowerCase()).toBe('footer');
        });

        test('footer should have footer class', () => {
            expect(footer.classList.contains('footer')).toBe(true);
        });

        test('footer should contain a container div', () => {
            const container = footer.querySelector('.container');
            expect(container).not.toBeNull();
        });

        test('footer should have footer-content section', () => {
            const footerContent = footer.querySelector('.footer-content');
            expect(footerContent).not.toBeNull();
        });
    });

    describe('Test Case 2: Footer contains link to GitHub repository', () => {
        test('footer should contain a GitHub link', () => {
            const githubLink = footer.querySelector('a[href*="github"]');
            expect(githubLink).not.toBeNull();
        });

        test('GitHub link should point to correct repository', () => {
            const githubLink = footer.querySelector('a[href*="github.com/yetone/mirdb"]');
            expect(githubLink).not.toBeNull();
        });

        test('GitHub link text should be "GitHub"', () => {
            const links = footer.querySelectorAll('a');
            const githubLink = Array.from(links).find(link =>
                link.textContent.trim() === 'GitHub' && link.href.includes('github')
            );
            expect(githubLink).not.toBeNull();
        });

        test('footer should contain a Documentation link', () => {
            const docLink = footer.querySelector('a[href*="readme"]');
            expect(docLink).not.toBeNull();
            expect(docLink.textContent.trim()).toBe('Documentation');
        });

        test('footer should contain an Issues link', () => {
            const issuesLink = footer.querySelector('a[href*="issues"]');
            expect(issuesLink).not.toBeNull();
            expect(issuesLink.textContent.trim()).toBe('Issues');
        });
    });

    describe('Test Case 3: Footer contains license information', () => {
        test('footer should contain license text', () => {
            const footerBottom = footer.querySelector('.footer-bottom');
            expect(footerBottom).not.toBeNull();
        });

        test('footer should display ISC license text', () => {
            const footerText = footer.textContent.toLowerCase();
            expect(footerText).toContain('isc');
        });

        test('license information should include "Licensed under"', () => {
            const footerBottom = footer.querySelector('.footer-bottom');
            const licenseText = footerBottom.textContent;
            expect(licenseText).toContain('Licensed under');
        });
    });

    describe('Test Case 4: External links have rel="noopener" security attribute', () => {
        test('all external links should have rel="noopener" attribute', () => {
            const externalLinks = footer.querySelectorAll('a[target="_blank"]');
            expect(externalLinks.length).toBeGreaterThan(0);

            externalLinks.forEach(link => {
                const rel = link.getAttribute('rel');
                expect(rel).not.toBeNull();
                expect(rel).toContain('noopener');
            });
        });

        test('GitHub link should have rel="noopener"', () => {
            const githubLink = footer.querySelector('a[href*="github.com/yetone/mirdb"]');
            expect(githubLink).not.toBeNull();
            expect(githubLink.getAttribute('rel')).toContain('noopener');
        });

        test('Documentation link should have rel="noopener"', () => {
            const docLink = footer.querySelector('a[href*="readme"]');
            expect(docLink).not.toBeNull();
            expect(docLink.getAttribute('rel')).toContain('noopener');
        });

        test('Issues link should have rel="noopener"', () => {
            const issuesLink = footer.querySelector('a[href*="issues"]');
            expect(issuesLink).not.toBeNull();
            expect(issuesLink.getAttribute('rel')).toContain('noopener');
        });

        test('all external links should have target="_blank"', () => {
            const externalLinks = footer.querySelectorAll('a[href^="https://"]');
            externalLinks.forEach(link => {
                expect(link.getAttribute('target')).toBe('_blank');
            });
        });
    });

    describe('Footer Content Structure', () => {
        test('footer should contain brand section with MirDB', () => {
            const brand = footer.querySelector('.footer-brand');
            expect(brand).not.toBeNull();
            expect(brand.textContent).toContain('MirDB');
        });

        test('footer should contain resources section', () => {
            const resources = footer.querySelector('.footer-links');
            expect(resources).not.toBeNull();
            expect(resources.textContent).toContain('Resources');
        });

        test('footer should contain project status section', () => {
            const status = footer.querySelector('.footer-status');
            expect(status).not.toBeNull();
            expect(status.textContent).toContain('Project Status');
        });

        test('footer should have description text', () => {
            const brand = footer.querySelector('.footer-brand');
            const description = brand.querySelector('p');
            expect(description).not.toBeNull();
            expect(description.textContent).toContain('persistent key-value store');
        });
    });
});
