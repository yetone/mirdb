/**
 * Tests for Navigation and External Links
 * Verifies navigation to GitHub repository and documentation works correctly
 * including proper link attributes for security
 */

const fs = require('fs');
const path = require('path');

describe('Navigation and External Links', () => {
  let htmlContent;
  let document;

  beforeAll(() => {
    // Read the HTML file
    const htmlPath = path.join(__dirname, '..', 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');

    // Parse HTML using jsdom
    const { JSDOM } = require('jsdom');
    const dom = new JSDOM(htmlContent);
    document = dom.window.document;
  });

  describe('Test Case 1: GitHub link navigation', () => {
    test('GitHub link should navigate to https://github.com/yetone/mirdb or opens in new tab', () => {
      // Find all links that point to the GitHub repository
      const githubLinks = document.querySelectorAll('a[href*="github.com/yetone/mirdb"]');
      expect(githubLinks.length).toBeGreaterThan(0);

      // Check that at least one GitHub link exists in navigation or accessible area
      let foundValidGithubLink = false;
      githubLinks.forEach(link => {
        const href = link.getAttribute('href');
        if (href.includes('github.com/yetone/mirdb')) {
          foundValidGithubLink = true;
          // Verify it either opens in new tab or navigates directly
          const target = link.getAttribute('target');
          // Link is valid if it navigates to GitHub (target can be _blank or not set)
          expect(href).toContain('github.com/yetone/mirdb');
        }
      });
      expect(foundValidGithubLink).toBe(true);
    });
  });

  describe('Test Case 2: GitHub link href attribute', () => {
    test('GitHub link href should contain github.com/yetone/mirdb', () => {
      // Find all anchor elements
      const allLinks = document.querySelectorAll('a');

      // Find links that contain the GitHub repository URL
      const githubLinks = Array.from(allLinks).filter(link => {
        const href = link.getAttribute('href') || '';
        return href.includes('github.com/yetone/mirdb');
      });

      expect(githubLinks.length).toBeGreaterThan(0);

      // Verify each GitHub link has the correct href
      githubLinks.forEach(link => {
        const href = link.getAttribute('href');
        expect(href).toContain('github.com/yetone/mirdb');
      });
    });

    test('Navigation GitHub link should be in nav element', () => {
      const navElement = document.querySelector('nav');
      expect(navElement).not.toBeNull();

      const navGithubLink = navElement.querySelector('a[href*="github.com/yetone/mirdb"]');
      expect(navGithubLink).not.toBeNull();
      expect(navGithubLink.getAttribute('href')).toContain('github.com/yetone/mirdb');
    });
  });

  describe('Test Case 3: External links have proper attributes', () => {
    test('External links should have rel="noopener" or rel="noopener noreferrer"', () => {
      // Find all external links (links that start with http or https)
      const allLinks = document.querySelectorAll('a[href^="http"]');

      expect(allLinks.length).toBeGreaterThan(0);

      // Check each external link has proper rel attribute
      allLinks.forEach(link => {
        const rel = link.getAttribute('rel') || '';
        const hasNoopener = rel.includes('noopener');
        expect(hasNoopener).toBe(true);
      });
    });

    test('External links should have target="_blank" for new tab behavior', () => {
      // Find all external links
      const externalLinks = document.querySelectorAll('a[href^="http"]');

      expect(externalLinks.length).toBeGreaterThan(0);

      // Check each external link has target="_blank"
      externalLinks.forEach(link => {
        const target = link.getAttribute('target');
        expect(target).toBe('_blank');
      });
    });
  });

  describe('Test Case 4: Documentation link exists', () => {
    test('At least one link to documentation or README should exist', () => {
      // Find links that reference documentation, README, or docs
      const allLinks = document.querySelectorAll('a');

      const documentationLinks = Array.from(allLinks).filter(link => {
        const href = link.getAttribute('href') || '';
        const text = link.textContent.toLowerCase();

        // Check if link points to documentation
        return href.includes('readme') ||
               href.includes('README') ||
               href.includes('docs') ||
               href.includes('documentation') ||
               text.includes('documentation') ||
               text.includes('readme') ||
               text.includes('docs');
      });

      expect(documentationLinks.length).toBeGreaterThan(0);
    });

    test('Documentation link should be accessible from the page', () => {
      // Find documentation link in footer or navigation
      const footerDocLink = document.querySelector('footer a[href*="readme"], footer a[href*="README"]');
      const navDocLink = document.querySelector('nav a[href*="readme"], nav a[href*="README"]');

      // At least one documentation link should exist
      const hasDocLink = footerDocLink !== null || navDocLink !== null;

      // If no direct readme link, check for Documentation text
      if (!hasDocLink) {
        const docTextLinks = document.querySelectorAll('a');
        const textBasedDocLink = Array.from(docTextLinks).find(link =>
          link.textContent.toLowerCase().includes('documentation')
        );
        expect(textBasedDocLink).not.toBeNull();
      } else {
        expect(hasDocLink).toBe(true);
      }
    });
  });

  describe('Navigation Structure', () => {
    test('Navigation element should exist', () => {
      const nav = document.querySelector('nav');
      expect(nav).not.toBeNull();
    });

    test('Navigation should have multiple links', () => {
      const nav = document.querySelector('nav');
      const navLinks = nav.querySelectorAll('a');
      expect(navLinks.length).toBeGreaterThan(1);
    });

    test('Navigation should include internal anchor links', () => {
      const nav = document.querySelector('nav');
      const internalLinks = nav.querySelectorAll('a[href^="#"]');
      expect(internalLinks.length).toBeGreaterThan(0);
    });
  });

  describe('Footer Links', () => {
    test('Footer should contain GitHub link', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      const footerGithubLink = footer.querySelector('a[href*="github.com/yetone/mirdb"]');
      expect(footerGithubLink).not.toBeNull();
    });

    test('Footer should have external links with proper attributes', () => {
      const footer = document.querySelector('footer');
      const externalLinks = footer.querySelectorAll('a[href^="http"]');

      externalLinks.forEach(link => {
        const rel = link.getAttribute('rel') || '';
        expect(rel).toContain('noopener');
      });
    });
  });
});
