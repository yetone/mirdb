/**
 * Footer Section Tests
 * Owner: Scenario 6 - Footer Section
 *
 * Tests:
 * - Footer element exists
 * - GitHub link present
 * - License information
 * - Copyright notice
 * - External link behavior (new tab)
 */

const { loadHTML, querySection } = require('../helpers/dom-utils');

describe('Footer Section', () => {
  beforeEach(() => {
    loadHTML('index.html');
  });

  describe('Test Case 1: Footer element exists at page bottom', () => {
    test('should have a footer section with id "footer"', () => {
      const footer = querySection('footer');
      expect(footer).toBeInTheDocument();
    });

    test('footer should be a footer element', () => {
      const footer = querySection('footer');
      expect(footer).toBeInTheDocument();
      expect(footer.tagName.toLowerCase()).toBe('footer');
    });

    test('footer should be visible and have content', () => {
      const footer = querySection('footer');
      expect(footer).toBeInTheDocument();
      expect(footer.textContent.trim().length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 2: Footer contains link to GitHub repository', () => {
    test('should have a GitHub link in the footer', () => {
      const footer = querySection('footer');
      expect(footer).toBeInTheDocument();

      const githubLink = footer.querySelector('#footer-github-link');
      expect(githubLink).toBeInTheDocument();
    });

    test('GitHub link should point to the correct repository', () => {
      const footer = querySection('footer');
      expect(footer).toBeInTheDocument();

      const githubLink = footer.querySelector('#footer-github-link');
      expect(githubLink).toBeInTheDocument();
      expect(githubLink.getAttribute('href')).toBe('https://github.com/yetone/mirdb');
    });

    test('GitHub link text should contain "GitHub"', () => {
      const footer = querySection('footer');
      expect(footer).toBeInTheDocument();

      const githubLink = footer.querySelector('#footer-github-link');
      expect(githubLink).toBeInTheDocument();
      expect(githubLink.textContent).toContain('GitHub');
    });
  });

  describe('Test Case 3: Footer mentions license or links to LICENSE file', () => {
    test('should have a license link in the footer', () => {
      const footer = querySection('footer');
      expect(footer).toBeInTheDocument();

      const licenseLink = footer.querySelector('#footer-license-link');
      expect(licenseLink).toBeInTheDocument();
    });

    test('license link should point to LICENSE file', () => {
      const footer = querySection('footer');
      expect(footer).toBeInTheDocument();

      const licenseLink = footer.querySelector('#footer-license-link');
      expect(licenseLink).toBeInTheDocument();
      expect(licenseLink.getAttribute('href')).toContain('LICENSE');
    });

    test('footer should mention license information', () => {
      const footer = querySection('footer');
      expect(footer).toBeInTheDocument();

      // Check footer mentions license in some form
      const footerText = footer.textContent.toLowerCase();
      expect(footerText).toMatch(/license|mit/i);
    });
  });

  describe('Test Case 4: Copyright text with year is present', () => {
    test('should have a copyright section', () => {
      const footer = querySection('footer');
      expect(footer).toBeInTheDocument();

      const copyrightSection = footer.querySelector('#footer-copyright');
      expect(copyrightSection).toBeInTheDocument();
    });

    test('copyright should contain the copyright symbol', () => {
      const footer = querySection('footer');
      expect(footer).toBeInTheDocument();

      const copyrightSection = footer.querySelector('#footer-copyright');
      expect(copyrightSection).toBeInTheDocument();
      expect(copyrightSection.textContent).toMatch(/©|copyright/i);
    });

    test('copyright should contain a year', () => {
      const footer = querySection('footer');
      expect(footer).toBeInTheDocument();

      const copyrightSection = footer.querySelector('#footer-copyright');
      expect(copyrightSection).toBeInTheDocument();
      // Check for a 4-digit year
      expect(copyrightSection.textContent).toMatch(/\d{4}/);
    });

    test('copyright should mention MirDB', () => {
      const footer = querySection('footer');
      expect(footer).toBeInTheDocument();

      const copyrightSection = footer.querySelector('#footer-copyright');
      expect(copyrightSection).toBeInTheDocument();
      expect(copyrightSection.textContent).toContain('MirDB');
    });
  });

  describe('Test Case 5: GitHub link opens in new tab', () => {
    test('GitHub link should have target="_blank"', () => {
      const footer = querySection('footer');
      expect(footer).toBeInTheDocument();

      const githubLink = footer.querySelector('#footer-github-link');
      expect(githubLink).toBeInTheDocument();
      expect(githubLink.getAttribute('target')).toBe('_blank');
    });

    test('GitHub link should have rel="noopener noreferrer" for security', () => {
      const footer = querySection('footer');
      expect(footer).toBeInTheDocument();

      const githubLink = footer.querySelector('#footer-github-link');
      expect(githubLink).toBeInTheDocument();
      const rel = githubLink.getAttribute('rel');
      expect(rel).toContain('noopener');
    });

    test('all external links in footer should open in new tab', () => {
      const footer = querySection('footer');
      expect(footer).toBeInTheDocument();

      const footerLinks = footer.querySelector('#footer-links');
      expect(footerLinks).toBeInTheDocument();

      const externalLinks = footerLinks.querySelectorAll('a[href^="http"]');
      externalLinks.forEach((link) => {
        expect(link.getAttribute('target')).toBe('_blank');
        expect(link.getAttribute('rel')).toContain('noopener');
      });
    });
  });

  describe('Additional Footer Tests', () => {
    test('footer should have contributing guidelines link', () => {
      const footer = querySection('footer');
      expect(footer).toBeInTheDocument();

      const contributingLink = footer.querySelector('#footer-contributing-link');
      expect(contributingLink).toBeInTheDocument();
      expect(contributingLink.getAttribute('href')).toContain('CONTRIBUTING');
    });

    test('footer links container should exist', () => {
      const footer = querySection('footer');
      expect(footer).toBeInTheDocument();

      const footerLinks = footer.querySelector('#footer-links');
      expect(footerLinks).toBeInTheDocument();
    });

    test('footer should have at least 3 navigation links', () => {
      const footer = querySection('footer');
      expect(footer).toBeInTheDocument();

      const footerLinks = footer.querySelector('#footer-links');
      expect(footerLinks).toBeInTheDocument();

      const links = footerLinks.querySelectorAll('a');
      expect(links.length).toBeGreaterThanOrEqual(3);
    });
  });
});
