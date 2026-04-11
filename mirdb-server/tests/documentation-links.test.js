/**
 * Documentation Links Tests
 * Owner: Scenario 10 - Documentation Links
 *
 * Tests for REQ-8: Homepage shall provide links to README, GitHub repository, and API documentation
 *
 * Test Cases:
 * 1. E2E: Footer contains GitHub link, license info, version number
 * 2. E2E: Links to README and API documentation are present
 * 3. Unit: Links to correct GitHub repository URL
 */

const fs = require('fs');
const path = require('path');

describe('Documentation Links - Scenario 10', () => {
    let htmlContent;

    beforeAll(() => {
        const htmlPath = path.join(__dirname, '../src/web/index.html');
        htmlContent = fs.readFileSync(htmlPath, 'utf-8');
    });

    beforeEach(() => {
        document.body.innerHTML = htmlContent;
    });

    describe('Test Case 1: Footer contains GitHub link, license info, version number', () => {
        test('footer element exists with data-testid', () => {
            const footer = document.querySelector('[data-testid="footer"]');
            expect(footer).not.toBeNull();
            expect(footer.tagName.toLowerCase()).toBe('footer');
        });

        test('footer contains GitHub link', () => {
            const githubLink = document.querySelector('[data-testid="github-link"]');
            expect(githubLink).not.toBeNull();
            expect(githubLink.textContent).toContain('GitHub');
            expect(githubLink.href).toContain('github.com');
        });

        test('footer contains license information', () => {
            const licenseLink = document.querySelector('[data-testid="license-link"]');
            expect(licenseLink).not.toBeNull();
            expect(licenseLink.textContent).toContain('MIT');

            const footerCopyright = document.querySelector('.footer-copyright');
            expect(footerCopyright).not.toBeNull();
            expect(footerCopyright.textContent).toContain('Licensed under');
        });

        test('footer contains version number', () => {
            const versionElement = document.querySelector('[data-testid="version-number"]');
            expect(versionElement).not.toBeNull();
            expect(versionElement.textContent).toMatch(/Version/i);

            const appVersion = document.getElementById('app-version');
            expect(appVersion).not.toBeNull();
            expect(appVersion.textContent).toMatch(/^\d+\.\d+\.\d+$/);
        });

        test('footer has proper structure with docs section', () => {
            const footerDocs = document.querySelector('[data-testid="footer-docs"]');
            expect(footerDocs).not.toBeNull();

            const footerNav = document.querySelector('.footer-nav');
            expect(footerNav).not.toBeNull();
            expect(footerNav.getAttribute('aria-label')).toBe('Documentation links');
        });
    });

    describe('Test Case 2: Documentation links presence check', () => {
        test('README link is present in footer', () => {
            const readmeLink = document.querySelector('[data-testid="readme-link"]');
            expect(readmeLink).not.toBeNull();
            expect(readmeLink.textContent).toContain('README');
        });

        test('API documentation link is present in footer', () => {
            const apiDocsLink = document.querySelector('[data-testid="api-docs-link"]');
            expect(apiDocsLink).not.toBeNull();
            expect(apiDocsLink.textContent).toContain('API Documentation');
        });

        test('all documentation links have correct href attributes', () => {
            const readmeLink = document.querySelector('[data-testid="readme-link"]');
            expect(readmeLink.href).toContain('github.com/yetone/mirdb');
            expect(readmeLink.href).toContain('#readme');

            const apiDocsLink = document.querySelector('[data-testid="api-docs-link"]');
            expect(apiDocsLink.href).toContain('/api-docs');
        });

        test('documentation section has heading', () => {
            const footerHeading = document.querySelector('.footer-heading');
            expect(footerHeading).not.toBeNull();
            expect(footerHeading.textContent).toContain('Documentation');
        });

        test('all footer links are accessible', () => {
            const footerLinks = document.querySelectorAll('.footer-link');
            expect(footerLinks.length).toBeGreaterThanOrEqual(3);

            footerLinks.forEach(link => {
                expect(link.tagName.toLowerCase()).toBe('a');
                expect(link.href).not.toBe('');
            });
        });
    });

    describe('Test Case 3: GitHub link href attribute', () => {
        test('GitHub link points to correct repository URL', () => {
            const githubLink = document.querySelector('[data-testid="github-link"]');
            expect(githubLink).not.toBeNull();
            expect(githubLink.href).toBe('https://github.com/yetone/mirdb');
        });

        test('README link points to GitHub README', () => {
            const readmeLink = document.querySelector('[data-testid="readme-link"]');
            expect(readmeLink).not.toBeNull();
            expect(readmeLink.href).toBe('https://github.com/yetone/mirdb#readme');
        });

        test('GitHub links use HTTPS protocol', () => {
            const githubLink = document.querySelector('[data-testid="github-link"]');
            const readmeLink = document.querySelector('[data-testid="readme-link"]');

            expect(githubLink.href).toMatch(/^https:\/\//);
            expect(readmeLink.href).toMatch(/^https:\/\//);
        });

        test('license link points to MIT license', () => {
            const licenseLink = document.querySelector('[data-testid="license-link"]');
            expect(licenseLink).not.toBeNull();
            expect(licenseLink.href).toContain('opensource.org/licenses/MIT');
        });
    });

    describe('Footer accessibility and styling', () => {
        test('footer nav has aria-label for accessibility', () => {
            const footerNav = document.querySelector('.footer-nav');
            expect(footerNav).not.toBeNull();
            expect(footerNav.getAttribute('aria-label')).toBeTruthy();
        });

        test('footer links have footer-link class for styling', () => {
            const footerLinks = document.querySelectorAll('.footer-link');
            expect(footerLinks.length).toBeGreaterThan(0);

            const testIds = ['readme-link', 'github-link', 'api-docs-link', 'license-link'];
            testIds.forEach(testId => {
                const link = document.querySelector(`[data-testid="${testId}"]`);
                expect(link.classList.contains('footer-link')).toBe(true);
            });
        });

        test('footer info section contains copyright', () => {
            const footerInfo = document.querySelector('.footer-info');
            expect(footerInfo).not.toBeNull();

            const copyright = footerInfo.querySelector('.footer-copyright');
            expect(copyright).not.toBeNull();
            expect(copyright.textContent).toMatch(/\d{4}/);
            expect(copyright.textContent).toContain('MirDB Project');
        });
    });
});
