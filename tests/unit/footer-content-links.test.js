/**
 * Unit tests for Footer Content and Links
 * Scenario: Verify the footer contains required links to documentation, GitHub, license, and project status
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

describe('Footer Content and Links', () => {
  let document;
  let htmlContent;

  beforeAll(() => {
    const htmlPath = path.resolve(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(htmlContent);
    document = dom.window.document;
  });

  describe('Test Case 4: Footer uses semantic HTML', () => {
    test('should have a footer element', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
    });

    test('footer should wrap all footer content', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      // Footer should contain footer-container with links
      const footerContainer = footer.querySelector('.footer-container');
      expect(footerContainer).not.toBeNull();
    });

    test('footer should have proper aria-label for accessibility', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      const ariaLabel = footer.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
    });
  });

  describe('Test Case 1: Footer navigation links', () => {
    test('should have footer-links container', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      const footerLinks = footer.querySelector('.footer-links');
      expect(footerLinks).not.toBeNull();
    });

    test('should contain Documentation link', () => {
      const footer = document.querySelector('footer');
      const links = footer.querySelectorAll('.footer-links a');

      const docLink = Array.from(links).find(link =>
        link.textContent.toLowerCase().includes('documentation') ||
        link.textContent.toLowerCase().includes('docs')
      );

      expect(docLink).toBeDefined();
      expect(docLink.getAttribute('href')).toBeTruthy();
    });

    test('should contain GitHub link', () => {
      const footer = document.querySelector('footer');
      const links = footer.querySelectorAll('.footer-links a');

      const githubLink = Array.from(links).find(link =>
        link.textContent.toLowerCase().includes('github')
      );

      expect(githubLink).toBeDefined();
      expect(githubLink.getAttribute('href')).toBeTruthy();
    });

    test('should contain License link', () => {
      const footer = document.querySelector('footer');
      const links = footer.querySelectorAll('.footer-links a');

      const licenseLink = Array.from(links).find(link =>
        link.textContent.toLowerCase().includes('license')
      );

      expect(licenseLink).toBeDefined();
      expect(licenseLink.getAttribute('href')).toBeTruthy();
    });

    test('all footer links should have href attributes', () => {
      const footer = document.querySelector('footer');
      const links = footer.querySelectorAll('.footer-links a');

      expect(links.length).toBeGreaterThan(0);

      links.forEach(link => {
        expect(link.getAttribute('href')).toBeTruthy();
      });
    });

    test('external links should have proper rel attributes for security', () => {
      const footer = document.querySelector('footer');
      const links = footer.querySelectorAll('.footer-links a[target="_blank"]');

      links.forEach(link => {
        const rel = link.getAttribute('rel');
        expect(rel).toContain('noopener');
      });
    });
  });

  describe('Test Case 2: GitHub link destination', () => {
    test('GitHub link should point to valid GitHub URL', () => {
      const footer = document.querySelector('footer');
      const links = footer.querySelectorAll('.footer-links a');

      const githubLink = Array.from(links).find(link =>
        link.textContent.toLowerCase().includes('github')
      );

      expect(githubLink).toBeDefined();
      const href = githubLink.getAttribute('href');
      expect(href).toMatch(/^https:\/\/github\.com\/.+/);
    });

    test('GitHub link should point to mirdb repository', () => {
      const footer = document.querySelector('footer');
      const links = footer.querySelectorAll('.footer-links a');

      const githubLink = Array.from(links).find(link =>
        link.textContent.toLowerCase().includes('github')
      );

      expect(githubLink).toBeDefined();
      const href = githubLink.getAttribute('href').toLowerCase();
      expect(href).toContain('mirdb');
    });
  });

  describe('Test Case 3: Project status badge', () => {
    test('footer should display project status indicator or badge', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      // Check for status badge image or text indicator
      const statusBadge = footer.querySelector('.project-status, .status-badge, [class*="status"], [class*="badge"]');
      const statusImage = footer.querySelector('img[alt*="status"], img[alt*="badge"], img[src*="badge"], img[src*="shields.io"]');
      const statusText = footer.textContent.toLowerCase();

      // Either has a status badge element, status image, or mentions status/active/maintained in text
      const hasStatusIndicator =
        statusBadge !== null ||
        statusImage !== null ||
        statusText.includes('active') ||
        statusText.includes('maintained') ||
        statusText.includes('status') ||
        statusText.includes('development') ||
        statusText.includes('open source');

      expect(hasStatusIndicator).toBe(true);
    });

    test('project status should indicate active development or license status', () => {
      const footer = document.querySelector('footer');
      const footerText = footer.textContent.toLowerCase();

      // Footer should mention project status, license, or development status
      const hasStatusInfo =
        footerText.includes('mit') ||
        footerText.includes('license') ||
        footerText.includes('open source') ||
        footerText.includes('active') ||
        footerText.includes('maintained') ||
        footer.querySelector('.project-status') !== null ||
        footer.querySelector('img[src*="badge"]') !== null;

      expect(hasStatusInfo).toBe(true);
    });
  });
});
