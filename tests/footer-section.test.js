/**
 * Footer Section Tests
 * Scenario: Verify that footer contains GitHub repository link and project information (REQ-7)
 */

const fs = require('fs');
const path = require('path');

describe('Footer with Project Links', () => {
  let document;

  beforeAll(() => {
    // Load the index.html file
    const htmlPath = path.resolve(__dirname, '../index.html');
    const html = fs.readFileSync(htmlPath, 'utf8');
    document = new DOMParser().parseFromString(html, 'text/html');
  });

  describe('Test Case 1: Footer element exists', () => {
    test('should have a footer element', () => {
      // Query for footer element
      const footer = document.querySelector('footer');

      expect(footer).not.toBeNull();
      expect(footer.tagName.toLowerCase()).toBe('footer');
    });

    test('footer should be present at the bottom of the document', () => {
      const body = document.querySelector('body');
      const footer = document.querySelector('footer');

      expect(footer).not.toBeNull();

      // Footer should be a child of body or near the end
      const bodyChildren = Array.from(body.children);
      const footerIndex = bodyChildren.indexOf(footer);

      // Footer should exist as a direct child or within the main structure
      expect(footerIndex !== -1 || body.contains(footer)).toBe(true);
    });

    test('footer should have content', () => {
      const footer = document.querySelector('footer');

      expect(footer).not.toBeNull();
      expect(footer.textContent.trim().length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 2: GitHub link in footer', () => {
    test('should have a link to GitHub repository in footer', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      // Query for links containing github.com
      const links = footer.querySelectorAll('a');
      let githubLink = null;

      for (const link of links) {
        const href = link.getAttribute('href') || '';
        const text = link.textContent.toLowerCase();
        if (href.includes('github.com') || text.includes('github')) {
          githubLink = link;
          break;
        }
      }

      expect(githubLink).not.toBeNull();
    });

    test('GitHub link should point to a valid repository URL', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      const links = footer.querySelectorAll('a');
      let githubLink = null;

      for (const link of links) {
        const href = link.getAttribute('href') || '';
        if (href.includes('github.com')) {
          githubLink = link;
          break;
        }
      }

      expect(githubLink).not.toBeNull();

      const href = githubLink.getAttribute('href');
      // Check it's a valid GitHub URL pattern (https://github.com/owner/repo)
      expect(href).toMatch(/^https?:\/\/(www\.)?github\.com\/[\w-]+\/[\w-]+/);
    });

    test('GitHub link should include mirdb in the URL', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      const links = footer.querySelectorAll('a');
      let githubLink = null;

      for (const link of links) {
        const href = link.getAttribute('href') || '';
        if (href.includes('github.com')) {
          githubLink = link;
          break;
        }
      }

      expect(githubLink).not.toBeNull();

      const href = githubLink.getAttribute('href').toLowerCase();
      expect(href).toContain('mirdb');
    });
  });

  describe('Test Case 3: External links have target=_blank', () => {
    test('GitHub link should have target="_blank" attribute', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      const links = footer.querySelectorAll('a');
      let githubLink = null;

      for (const link of links) {
        const href = link.getAttribute('href') || '';
        if (href.includes('github.com')) {
          githubLink = link;
          break;
        }
      }

      expect(githubLink).not.toBeNull();
      expect(githubLink.getAttribute('target')).toBe('_blank');
    });

    test('external links should have rel="noopener noreferrer" for security', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      const externalLinks = footer.querySelectorAll('a[target="_blank"]');

      for (const link of externalLinks) {
        const rel = link.getAttribute('rel') || '';
        expect(rel).toContain('noopener');
        expect(rel).toContain('noreferrer');
      }
    });

    test('all external links in footer should open in new tab', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      const links = footer.querySelectorAll('a');

      for (const link of links) {
        const href = link.getAttribute('href') || '';
        // Check if it's an external link (starts with http:// or https://)
        if (href.startsWith('http://') || href.startsWith('https://')) {
          expect(link.getAttribute('target')).toBe('_blank');
        }
      }
    });
  });

  describe('Test Case 4: License information', () => {
    test('should display license information in footer', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      const footerText = footer.textContent.toLowerCase();

      // Check for common license types
      const hasLicense =
        footerText.includes('mit') ||
        footerText.includes('apache') ||
        footerText.includes('gpl') ||
        footerText.includes('bsd') ||
        footerText.includes('license');

      expect(hasLicense).toBe(true);
    });

    test('should include MIT License text', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      const footerText = footer.textContent.toLowerCase();

      // MirDB uses MIT License as per the existing implementation
      expect(footerText).toContain('mit');
      expect(footerText).toContain('license');
    });

    test('should include copyright information', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      const footerText = footer.textContent;

      // Check for copyright symbol or word
      const hasCopyright =
        footerText.includes('©') ||
        footerText.toLowerCase().includes('copyright') ||
        footerText.includes('(c)');

      expect(hasCopyright).toBe(true);
    });

    test('should include MirDB project name', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      const footerText = footer.textContent;

      expect(footerText).toContain('MirDB');
    });
  });
});
