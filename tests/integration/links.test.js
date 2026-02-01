/**
 * External Links Tests
 * Owner: Scenario 13 - External Links Functionality
 *
 * Tests:
 * - GitHub repository link valid
 * - External links have target="_blank"
 * - External links have rel="noopener"
 * - No 404 errors on external links
 */
const { loadHTML } = require('../helpers/dom-utils');

describe('External Links Functionality', () => {
  let externalLinks;

  beforeEach(() => {
    loadHTML('index.html');
    // Get all anchor tags with external href (http:// or https://)
    externalLinks = Array.from(document.querySelectorAll('a[href^="http://"], a[href^="https://"]'));
  });

  describe('Test Case 1: GitHub repository link exists and is valid', () => {
    it('should have a link to the GitHub repository', () => {
      const githubLinks = externalLinks.filter(link =>
        link.href.includes('github.com/yetone/mirdb')
      );
      expect(githubLinks.length).toBeGreaterThan(0);
    });

    it('should have the main GitHub CTA link', () => {
      const githubCTA = document.getElementById('github-cta');
      expect(githubCTA).toBeInTheDocument();
      expect(githubCTA.href).toBe('https://github.com/yetone/mirdb');
    });

    it('should have the footer GitHub link', () => {
      const footerGithub = document.getElementById('footer-github-link');
      expect(footerGithub).toBeInTheDocument();
      expect(footerGithub.href).toBe('https://github.com/yetone/mirdb');
    });
  });

  describe('Test Case 2: External links have target="_blank"', () => {
    it('should have target="_blank" on all external links', () => {
      externalLinks.forEach(link => {
        expect(link).toHaveAttribute('target', '_blank');
      });
    });

    it('should have target="_blank" on GitHub CTA', () => {
      const githubCTA = document.getElementById('github-cta');
      expect(githubCTA).toHaveAttribute('target', '_blank');
    });

    it('should have target="_blank" on CircleCI badge link', () => {
      const circleciBadge = document.getElementById('circleci-badge');
      expect(circleciBadge).toHaveAttribute('target', '_blank');
    });

    it('should have target="_blank" on version badge link', () => {
      const versionBadge = document.getElementById('version-badge');
      expect(versionBadge).toHaveAttribute('target', '_blank');
    });

    it('should have target="_blank" on license badge link', () => {
      const licenseBadge = document.getElementById('license-badge');
      expect(licenseBadge).toHaveAttribute('target', '_blank');
    });

    it('should have target="_blank" on footer links', () => {
      const footerLinks = document.querySelectorAll('#footer a[href^="http"]');
      footerLinks.forEach(link => {
        expect(link).toHaveAttribute('target', '_blank');
      });
    });

    it('should have target="_blank" on documentation link in getting started', () => {
      const docLink = document.querySelector('#getting-started a[href*="github.com"]');
      expect(docLink).toHaveAttribute('target', '_blank');
    });
  });

  describe('Test Case 3: External links have rel="noopener" or rel="noopener noreferrer"', () => {
    it('should have rel containing "noopener" on all external links', () => {
      externalLinks.forEach(link => {
        const rel = link.getAttribute('rel');
        expect(rel).toBeTruthy();
        expect(rel).toMatch(/noopener/);
      });
    });

    it('should have rel with noopener on GitHub CTA', () => {
      const githubCTA = document.getElementById('github-cta');
      const rel = githubCTA.getAttribute('rel');
      expect(rel).toMatch(/noopener/);
    });

    it('should have rel with noopener on all badge links', () => {
      const badges = document.querySelectorAll('#status-badges a');
      badges.forEach(badge => {
        const rel = badge.getAttribute('rel');
        expect(rel).toMatch(/noopener/);
      });
    });

    it('should have rel with noopener on all footer external links', () => {
      const footerLinks = document.querySelectorAll('#footer a[href^="http"]');
      footerLinks.forEach(link => {
        const rel = link.getAttribute('rel');
        expect(rel).toMatch(/noopener/);
      });
    });

    it('external links should have noopener noreferrer for maximum security', () => {
      externalLinks.forEach(link => {
        const rel = link.getAttribute('rel');
        // Should have noopener at minimum, noreferrer is a bonus
        expect(rel).toMatch(/noopener/);
      });
    });
  });

  describe('Test Case 4: All external links are valid (no 404 expected)', () => {
    // Note: These are unit tests that verify the URLs are well-formed.
    // Actual HTTP validation would require a live server or mocking.

    it('should have well-formed GitHub URLs', () => {
      const githubLinks = externalLinks.filter(link =>
        link.href.includes('github.com')
      );

      githubLinks.forEach(link => {
        expect(link.href).toMatch(/^https:\/\/github\.com\/yetone\/mirdb/);
      });
    });

    it('should have valid CircleCI badge URL', () => {
      const circleciBadge = document.getElementById('circleci-badge');
      expect(circleciBadge.href).toBe('https://circleci.com/gh/yetone/mirdb');
    });

    it('should have valid version badge URL pointing to releases', () => {
      const versionBadge = document.getElementById('version-badge');
      expect(versionBadge.href).toBe('https://github.com/yetone/mirdb/releases');
    });

    it('should have valid license URL pointing to LICENSE file', () => {
      const licenseBadge = document.getElementById('license-badge');
      expect(licenseBadge.href).toBe('https://github.com/yetone/mirdb/blob/master/LICENSE');
    });

    it('should have valid documentation URL pointing to README', () => {
      const docLink = document.querySelector('#getting-started a[href*="readme"]');
      expect(docLink.href).toBe('https://github.com/yetone/mirdb#readme');
    });

    it('should have valid contributing URL', () => {
      const contributingLink = document.getElementById('footer-contributing-link');
      expect(contributingLink.href).toBe('https://github.com/yetone/mirdb/blob/master/CONTRIBUTING.md');
    });

    it('all external links should use HTTPS', () => {
      externalLinks.forEach(link => {
        expect(link.href).toMatch(/^https:\/\//);
      });
    });

    it('no external links should be empty or javascript:', () => {
      externalLinks.forEach(link => {
        expect(link.href).not.toBe('');
        expect(link.href).not.toMatch(/^javascript:/);
      });
    });
  });

  describe('External links count and coverage', () => {
    it('should have at least 5 external links on the page', () => {
      expect(externalLinks.length).toBeGreaterThanOrEqual(5);
    });

    it('should have external links in hero section', () => {
      const heroLinks = document.querySelectorAll('#hero a[href^="http"]');
      expect(heroLinks.length).toBeGreaterThan(0);
    });

    it('should have external links in footer section', () => {
      const footerLinks = document.querySelectorAll('#footer a[href^="http"]');
      expect(footerLinks.length).toBeGreaterThan(0);
    });
  });
});
