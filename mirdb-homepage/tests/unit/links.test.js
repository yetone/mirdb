/**
 * Link Validation Unit Tests
 * Owner: Scenario 10 - External Link Verification (also used by Scenario 5)
 *
 * Unit tests for validating external link URLs, target attributes,
 * and security best practices using JSDOM for DOM parsing.
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

// Load HTML content
const htmlPath = path.join(__dirname, '../../index.html');
const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

describe('External Link Verification Tests', () => {
  let dom;
  let document;

  beforeAll(() => {
    dom = new JSDOM(htmlContent);
    document = dom.window.document;
  });

  describe('GitHub Repository Link', () => {
    test('GitHub link URL is correct', () => {
      // Test case 1: Link href equals 'https://github.com/yetone/mirdb'
      const githubLinks = document.querySelectorAll('a[href="https://github.com/yetone/mirdb"]');
      expect(githubLinks.length).toBeGreaterThanOrEqual(1);

      // Check that the main "View on GitHub" button in the hero section has the correct URL
      const heroGithubLink = document.querySelector('.cta-buttons a[href="https://github.com/yetone/mirdb"]');
      expect(heroGithubLink).not.toBeNull();
      expect(heroGithubLink.getAttribute('href')).toBe('https://github.com/yetone/mirdb');
    });

    test('GitHub link has target="_blank" for new tab', () => {
      // Test case 2: Link has target='_blank' for new tab
      const githubLinks = document.querySelectorAll('a[href="https://github.com/yetone/mirdb"]');

      githubLinks.forEach(link => {
        expect(link.getAttribute('target')).toBe('_blank');
      });
    });

    test('GitHub link has rel="noopener noreferrer" for security', () => {
      // Test case 3: Link has rel='noopener noreferrer' for security
      const githubLinks = document.querySelectorAll('a[href="https://github.com/yetone/mirdb"]');

      githubLinks.forEach(link => {
        const rel = link.getAttribute('rel');
        expect(rel).not.toBeNull();
        expect(rel).toContain('noopener');
        expect(rel).toContain('noreferrer');
      });
    });

    test('GitHub links in both hero and footer sections', () => {
      // Verify GitHub links exist in both hero and footer
      const heroGithubLink = document.querySelector('header a[href="https://github.com/yetone/mirdb"]');
      const footerGithubLink = document.querySelector('footer a[href="https://github.com/yetone/mirdb"]');

      expect(heroGithubLink).not.toBeNull();
      expect(footerGithubLink).not.toBeNull();
    });
  });

  describe('CircleCI Badge Link', () => {
    test('CircleCI badge is wrapped in link to CircleCI project', () => {
      // Test case 4: Badge is wrapped in link to CircleCI project (circleci.com/gh/yetone/mirdb)
      const circleCILink = document.querySelector('a[href*="circleci.com/gh/yetone/mirdb"]');
      expect(circleCILink).not.toBeNull();
      expect(circleCILink.getAttribute('href')).toBe('https://circleci.com/gh/yetone/mirdb');

      // Verify the badge image is inside the link
      const badgeImg = circleCILink.querySelector('img');
      expect(badgeImg).not.toBeNull();
      expect(badgeImg.getAttribute('alt')).toContain('CircleCI');
    });

    test('CircleCI link opens in new tab', () => {
      const circleCILink = document.querySelector('a[href*="circleci.com/gh/yetone/mirdb"]');
      expect(circleCILink.getAttribute('target')).toBe('_blank');
    });

    test('CircleCI link has security attributes', () => {
      const circleCILink = document.querySelector('a[href*="circleci.com/gh/yetone/mirdb"]');
      const rel = circleCILink.getAttribute('rel');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    });
  });

  describe('All External Links Security', () => {
    test('all external links have target="_blank"', () => {
      // Get all external links (starting with http:// or https://)
      const externalLinks = document.querySelectorAll('a[href^="http"]');

      externalLinks.forEach(link => {
        expect(link.getAttribute('target')).toBe('_blank');
      });
    });

    test('all external links have proper security attributes', () => {
      // Get all external links
      const externalLinks = document.querySelectorAll('a[href^="http"]');

      externalLinks.forEach(link => {
        const rel = link.getAttribute('rel');
        expect(rel).not.toBeNull();
        expect(rel).toContain('noopener');
        expect(rel).toContain('noreferrer');
      });
    });

    test('internal links do not have target="_blank"', () => {
      // Internal links (starting with # or relative paths) should not have target="_blank"
      const internalLinks = document.querySelectorAll('a[href^="#"]');

      internalLinks.forEach(link => {
        expect(link.getAttribute('target')).not.toBe('_blank');
      });
    });
  });

  describe('Author Link', () => {
    test('author link points to GitHub profile', () => {
      const authorLink = document.querySelector('.author a');
      expect(authorLink).not.toBeNull();
      expect(authorLink.getAttribute('href')).toBe('https://github.com/yetone');
    });

    test('author link has proper security attributes', () => {
      const authorLink = document.querySelector('.author a');
      expect(authorLink.getAttribute('target')).toBe('_blank');

      const rel = authorLink.getAttribute('rel');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    });
  });
});
