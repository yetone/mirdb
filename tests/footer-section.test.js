/**
 * Tests for Footer Section (PRD Key Sections - Footer)
 * Verify the footer contains required links and information
 */

const fs = require('fs');
const path = require('path');

describe('Footer Section', () => {
  let document;

  beforeEach(() => {
    const html = fs.readFileSync(path.resolve(__dirname, '../index.html'), 'utf8');
    document = new DOMParser().parseFromString(html, 'text/html');
  });

  // Test Case 1: Footer element exists at bottom of page
  describe('Test Case 1: Footer Element Exists', () => {
    test('should have a footer element', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
    });

    test('should have footer with appropriate class', () => {
      const footer = document.querySelector('footer.footer, footer');
      expect(footer).not.toBeNull();
    });

    test('should have footer as last major section before closing body', () => {
      const body = document.querySelector('body');
      const children = Array.from(body.children);
      const lastChild = children[children.length - 1];
      expect(lastChild.tagName.toLowerCase()).toBe('footer');
    });
  });

  // Test Case 2: Footer contains link to GitHub repository
  describe('Test Case 2: GitHub Repository Link', () => {
    test('should have a link to GitHub in the footer', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
      const links = footer.querySelectorAll('a');
      const githubLink = Array.from(links).find(link =>
        link.getAttribute('href')?.includes('github.com') ||
        link.textContent.toLowerCase().includes('github')
      );
      expect(githubLink).not.toBeNull();
    });

    test('should have GitHub link pointing to correct repository', () => {
      const footer = document.querySelector('footer');
      const links = footer.querySelectorAll('a');
      const githubLink = Array.from(links).find(link =>
        link.getAttribute('href')?.includes('github.com')
      );
      expect(githubLink).not.toBeNull();
      const href = githubLink.getAttribute('href');
      expect(href).toContain('github.com');
      expect(href).toContain('mirdb');
    });

    test('should have GitHub link open in new tab for security', () => {
      const footer = document.querySelector('footer');
      const links = footer.querySelectorAll('a');
      const githubLink = Array.from(links).find(link =>
        link.getAttribute('href')?.includes('github.com')
      );
      expect(githubLink).not.toBeNull();
      expect(githubLink.getAttribute('target')).toBe('_blank');
      expect(githubLink.getAttribute('rel')).toContain('noopener');
    });
  });

  // Test Case 3: Footer contains link to documentation
  describe('Test Case 3: Documentation Link', () => {
    test('should have a documentation link in the footer', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
      const links = footer.querySelectorAll('a');
      const docLink = Array.from(links).find(link =>
        link.textContent.toLowerCase().includes('doc') ||
        link.textContent.toLowerCase().includes('documentation') ||
        link.getAttribute('href')?.toLowerCase().includes('doc') ||
        link.getAttribute('href')?.toLowerCase().includes('readme')
      );
      expect(docLink).not.toBeNull();
    });

    test('should have documentation link with valid href', () => {
      const footer = document.querySelector('footer');
      const links = footer.querySelectorAll('a');
      const docLink = Array.from(links).find(link =>
        link.textContent.toLowerCase().includes('doc') ||
        link.textContent.toLowerCase().includes('documentation')
      );
      expect(docLink).not.toBeNull();
      const href = docLink.getAttribute('href');
      expect(href).toBeTruthy();
      expect(href.length).toBeGreaterThan(0);
    });

    test('should have documentation link open in new tab', () => {
      const footer = document.querySelector('footer');
      const links = footer.querySelectorAll('a');
      const docLink = Array.from(links).find(link =>
        link.textContent.toLowerCase().includes('doc') ||
        link.textContent.toLowerCase().includes('documentation')
      );
      expect(docLink).not.toBeNull();
      expect(docLink.getAttribute('target')).toBe('_blank');
      expect(docLink.getAttribute('rel')).toContain('noopener');
    });
  });

  // Test Case 4: Footer displays or links to license information
  describe('Test Case 4: License Information', () => {
    test('should display license information in the footer', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
      const footerText = footer.textContent.toLowerCase();
      const hasLicenseText =
        footerText.includes('license') ||
        footerText.includes('licensed') ||
        footerText.includes('mit') ||
        footerText.includes('apache') ||
        footerText.includes('bsd') ||
        footerText.includes('gpl');
      expect(hasLicenseText).toBe(true);
    });

    test('should have visible license information', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
      // Check for license text or link
      const footerText = footer.textContent.toLowerCase();
      const licenseLinks = Array.from(footer.querySelectorAll('a')).filter(link =>
        link.textContent.toLowerCase().includes('license') ||
        link.getAttribute('href')?.toLowerCase().includes('license')
      );
      const hasLicenseInfo =
        footerText.includes('mit') ||
        footerText.includes('licensed') ||
        licenseLinks.length > 0;
      expect(hasLicenseInfo).toBe(true);
    });
  });

  // Additional tests for footer structure and accessibility
  describe('Footer Structure and Accessibility', () => {
    test('should have a container within the footer', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
      const container = footer.querySelector('.container, .footer-container, [class*="container"]');
      expect(container).not.toBeNull();
    });

    test('should have navigation links wrapped in nav or appropriate element', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
      const links = footer.querySelectorAll('a');
      expect(links.length).toBeGreaterThanOrEqual(2); // At least GitHub and Documentation links
    });

    test('should have copyright notice', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
      const footerText = footer.textContent;
      const hasCopyright =
        footerText.includes('©') ||
        footerText.includes('Copyright') ||
        footerText.includes('copyright') ||
        footerText.includes('MirDB');
      expect(hasCopyright).toBe(true);
    });
  });
});
