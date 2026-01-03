/**
 * Unit tests for GitHub Repository Links
 * Scenario: Verify the homepage provides working links to the source code repository
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

describe('GitHub Repository Links', () => {
  let document;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
  });

  // Test Case 1: Hero section contains link to GitHub repository
  describe('TC1: GitHub link in hero section', () => {
    test('hero section contains GitHub repository link', () => {
      const heroSection = document.querySelector('.hero');
      expect(heroSection).not.toBeNull();

      const githubLinks = heroSection.querySelectorAll('a[href*="github.com"]');
      expect(githubLinks.length).toBeGreaterThanOrEqual(1);

      // Verify it points to the MirDB repository
      const hasCorrectLink = Array.from(githubLinks).some((link) =>
        link.getAttribute('href').includes('github.com/yetone/mirdb')
      );
      expect(hasCorrectLink).toBe(true);
    });
  });

  // Test Case 2: Footer contains link to GitHub repository
  describe('TC2: GitHub link in footer', () => {
    test('footer contains GitHub repository link', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      const githubLinks = footer.querySelectorAll('a[href*="github.com"]');
      expect(githubLinks.length).toBeGreaterThanOrEqual(1);

      // Verify at least one link points to the main MirDB repository
      const hasMainRepoLink = Array.from(githubLinks).some((link) => {
        const href = link.getAttribute('href');
        return (
          href === 'https://github.com/yetone/mirdb' ||
          href.includes('github.com/yetone/mirdb')
        );
      });
      expect(hasMainRepoLink).toBe(true);
    });
  });

  // Test Case 3: Validate GitHub link URL format
  describe('TC3: GitHub link URL format validation', () => {
    test('all GitHub links match expected repository pattern', () => {
      const allGithubLinks = document.querySelectorAll('a[href*="github.com"]');
      expect(allGithubLinks.length).toBeGreaterThanOrEqual(1);

      // GitHub URL pattern: https://github.com/owner/repo with optional subpaths and fragments
      const githubUrlPattern =
        /^https:\/\/github\.com\/[a-zA-Z0-9_-]+\/[a-zA-Z0-9_-]+(\/[^#]*)?(#.*)?$/;

      allGithubLinks.forEach((link) => {
        const href = link.getAttribute('href');
        expect(href).toMatch(githubUrlPattern);
      });
    });

    test('MirDB repository links point to yetone/mirdb', () => {
      const mirdbLinks = document.querySelectorAll(
        'a[href*="github.com/yetone/mirdb"]'
      );
      expect(mirdbLinks.length).toBeGreaterThanOrEqual(2); // At least hero and footer

      mirdbLinks.forEach((link) => {
        const href = link.getAttribute('href');
        expect(href).toContain('github.com/yetone/mirdb');
      });
    });
  });

  // Test Case 4: Check all external links have proper attributes
  describe('TC4: External links security attributes', () => {
    test('all external links have target=_blank and rel=noopener', () => {
      // Get all external links (those with http/https that point outside)
      const allLinks = document.querySelectorAll('a[href^="http"]');
      expect(allLinks.length).toBeGreaterThanOrEqual(1);

      allLinks.forEach((link) => {
        const href = link.getAttribute('href');
        const target = link.getAttribute('target');
        const rel = link.getAttribute('rel');

        // External links should open in new tab
        expect(target).toBe('_blank');

        // External links should have noopener for security
        expect(rel).toContain('noopener');
      });
    });

    test('GitHub links in hero section have proper attributes', () => {
      const heroSection = document.querySelector('.hero');
      const githubLink = heroSection.querySelector('a[href*="github.com"]');

      expect(githubLink).not.toBeNull();
      expect(githubLink.getAttribute('target')).toBe('_blank');
      expect(githubLink.getAttribute('rel')).toContain('noopener');
    });

    test('GitHub links in footer have proper attributes', () => {
      const footer = document.querySelector('footer');
      const githubLinks = footer.querySelectorAll('a[href*="github.com"]');

      expect(githubLinks.length).toBeGreaterThanOrEqual(1);

      githubLinks.forEach((link) => {
        expect(link.getAttribute('target')).toBe('_blank');
        expect(link.getAttribute('rel')).toContain('noopener');
      });
    });
  });
});
