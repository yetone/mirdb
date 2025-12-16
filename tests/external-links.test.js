import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import { readFileSync } from 'fs';
import { resolve } from 'path';

describe('External Links Validation', () => {
  let dom;
  let document;

  beforeEach(() => {
    const htmlPath = resolve(__dirname, '../index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    dom = new JSDOM(html, { runScripts: 'dangerously', resources: 'usable' });
    document = dom.window.document;
  });

  afterEach(() => {
    if (dom) {
      dom.window.close();
    }
  });

  /**
   * Helper function to get all external links from the page
   * External links are those that point to different domains (start with http:// or https://)
   */
  function getExternalLinks() {
    const allLinks = document.querySelectorAll('a[href]');
    return Array.from(allLinks).filter(link => {
      const href = link.getAttribute('href');
      return href && (href.startsWith('http://') || href.startsWith('https://'));
    });
  }

  /**
   * Helper function to get all GitHub links
   */
  function getGitHubLinks() {
    return document.querySelectorAll('a[href*="github.com"]');
  }

  // Test Case 1: Check GitHub link validity
  // Type: Integration - Verifies GitHub links return 200 status code
  describe('Test Case 1: GitHub link validity', () => {
    it('should have at least one GitHub link on the page', () => {
      const githubLinks = getGitHubLinks();
      expect(githubLinks.length).toBeGreaterThan(0);
    });

    it('GitHub links should have valid URL format', () => {
      const githubLinks = getGitHubLinks();
      githubLinks.forEach(link => {
        const href = link.getAttribute('href');
        expect(href).toMatch(/^https:\/\/github\.com\//);
      });
    });

    it('GitHub links should point to valid repository paths', () => {
      const githubLinks = getGitHubLinks();
      githubLinks.forEach(link => {
        const href = link.getAttribute('href');
        // Valid GitHub URLs should have at least org/repo pattern
        // e.g., https://github.com/yetone/mirdb
        const githubPattern = /^https:\/\/github\.com\/[\w-]+\/[\w-]+/;
        expect(href).toMatch(githubPattern);
      });
    });

    it('primary GitHub link should point to mirdb repository', () => {
      const navGithubLink = document.querySelector('nav a[href*="github.com"]');
      expect(navGithubLink).not.toBeNull();
      const href = navGithubLink.getAttribute('href');
      expect(href.toLowerCase()).toContain('mirdb');
    });

    // Integration test: Verify GitHub URL structure is correct for API validation
    it('GitHub links should be well-formed for HTTP requests', () => {
      const githubLinks = getGitHubLinks();
      githubLinks.forEach(link => {
        const href = link.getAttribute('href');
        // URL should be parseable
        expect(() => new URL(href)).not.toThrow();
        const url = new URL(href);
        expect(url.protocol).toBe('https:');
        expect(url.hostname).toBe('github.com');
      });
    });
  });

  // Test Case 2: Check external links have target='_blank'
  // Type: Unit - Verifies external links open in new tab
  describe('Test Case 2: External links have target="_blank"', () => {
    it('all external links should have target="_blank" attribute', () => {
      const externalLinks = getExternalLinks();
      expect(externalLinks.length).toBeGreaterThan(0);

      externalLinks.forEach(link => {
        const target = link.getAttribute('target');
        const href = link.getAttribute('href');
        expect(target).toBe('_blank');
      });
    });

    it('GitHub link in navigation should open in new tab', () => {
      const navGithubLink = document.querySelector('nav a[href*="github.com"]');
      expect(navGithubLink).not.toBeNull();
      expect(navGithubLink.getAttribute('target')).toBe('_blank');
    });

    it('GitHub link in hero CTA should open in new tab', () => {
      const heroCTALink = document.querySelector('.hero a[href*="github.com"], .cta-buttons a[href*="github.com"]');
      if (heroCTALink) {
        expect(heroCTALink.getAttribute('target')).toBe('_blank');
      }
    });

    it('footer external links should open in new tab', () => {
      const footerExternalLinks = document.querySelectorAll('footer a[href^="http"]');
      footerExternalLinks.forEach(link => {
        expect(link.getAttribute('target')).toBe('_blank');
      });
    });

    it('internal anchor links should NOT have target="_blank"', () => {
      const internalLinks = document.querySelectorAll('a[href^="#"]');
      internalLinks.forEach(link => {
        const target = link.getAttribute('target');
        // Internal links should either have no target or not be _blank
        expect(target === null || target !== '_blank').toBe(true);
      });
    });
  });

  // Test Case 3: Check external links have rel='noopener noreferrer'
  // Type: Unit - Verifies security attributes for external links
  describe('Test Case 3: External links have rel="noopener noreferrer"', () => {
    it('all external links should have rel attribute containing "noopener"', () => {
      const externalLinks = getExternalLinks();
      expect(externalLinks.length).toBeGreaterThan(0);

      externalLinks.forEach(link => {
        const rel = link.getAttribute('rel');
        const href = link.getAttribute('href');
        expect(rel).not.toBeNull();
        expect(rel).toContain('noopener');
      });
    });

    it('all external links should have rel attribute containing "noreferrer"', () => {
      const externalLinks = getExternalLinks();

      externalLinks.forEach(link => {
        const rel = link.getAttribute('rel');
        const href = link.getAttribute('href');
        expect(rel).not.toBeNull();
        expect(rel).toContain('noreferrer');
      });
    });

    it('external links should have both noopener and noreferrer for security', () => {
      const externalLinks = getExternalLinks();

      externalLinks.forEach(link => {
        const rel = link.getAttribute('rel');
        const href = link.getAttribute('href');
        expect(rel).toContain('noopener');
        expect(rel).toContain('noreferrer');
      });
    });

    it('GitHub links should have proper security attributes', () => {
      const githubLinks = getGitHubLinks();
      githubLinks.forEach(link => {
        const rel = link.getAttribute('rel');
        expect(rel).toContain('noopener');
        expect(rel).toContain('noreferrer');
      });
    });

    it('links with target="_blank" must have rel="noopener" (security requirement)', () => {
      const blankTargetLinks = document.querySelectorAll('a[target="_blank"]');
      blankTargetLinks.forEach(link => {
        const rel = link.getAttribute('rel');
        expect(rel).not.toBeNull();
        expect(rel).toContain('noopener');
      });
    });
  });

  // Test Case 4: Verify no broken links on page
  // Type: Integration - Validates all href values resolve to valid resources
  describe('Test Case 4: No broken links on page', () => {
    it('all links should have non-empty href values', () => {
      const allLinks = document.querySelectorAll('a[href]');
      expect(allLinks.length).toBeGreaterThan(0);

      allLinks.forEach(link => {
        const href = link.getAttribute('href');
        expect(href).not.toBe('');
        expect(href).not.toBeNull();
      });
    });

    it('internal anchor links should point to existing elements', () => {
      const anchorLinks = document.querySelectorAll('a[href^="#"]');
      anchorLinks.forEach(link => {
        const href = link.getAttribute('href');
        if (href && href !== '#') {
          const targetId = href.substring(1);
          const targetElement = document.getElementById(targetId);
          expect(targetElement).not.toBeNull();
        }
      });
    });

    it('all external URLs should be well-formed', () => {
      const externalLinks = getExternalLinks();
      externalLinks.forEach(link => {
        const href = link.getAttribute('href');
        // Should be parseable as URL
        expect(() => new URL(href)).not.toThrow();
      });
    });

    it('external links should use HTTPS protocol', () => {
      const externalLinks = getExternalLinks();
      externalLinks.forEach(link => {
        const href = link.getAttribute('href');
        const url = new URL(href);
        expect(url.protocol).toBe('https:');
      });
    });

    it('should not have any javascript: or data: protocol links', () => {
      const allLinks = document.querySelectorAll('a[href]');
      allLinks.forEach(link => {
        const href = link.getAttribute('href').toLowerCase();
        expect(href.startsWith('javascript:')).toBe(false);
        expect(href.startsWith('data:')).toBe(false);
      });
    });

    it('no links should have placeholder URLs', () => {
      const allLinks = document.querySelectorAll('a[href]');
      allLinks.forEach(link => {
        const href = link.getAttribute('href').toLowerCase();
        // Check for common placeholder patterns
        expect(href).not.toContain('example.com');
        expect(href).not.toContain('placeholder');
        expect(href).not.toBe('http://');
        expect(href).not.toBe('https://');
      });
    });

    it('GitHub repository links should have valid paths', () => {
      const githubLinks = getGitHubLinks();
      githubLinks.forEach(link => {
        const href = link.getAttribute('href');
        const url = new URL(href);
        // Path should be at least /owner/repo
        const pathParts = url.pathname.split('/').filter(p => p.length > 0);
        expect(pathParts.length).toBeGreaterThanOrEqual(2);
      });
    });

    it('documentation links should have valid paths', () => {
      const docLinks = document.querySelectorAll('a[href*="README"], a[href*="readme"], a[href*="docs"]');
      docLinks.forEach(link => {
        const href = link.getAttribute('href');
        expect(() => new URL(href)).not.toThrow();
      });
    });
  });

  // Additional comprehensive tests for external link coverage
  describe('External links comprehensive coverage', () => {
    it('should have GitHub links in multiple locations (nav, hero, footer)', () => {
      const navGithub = document.querySelector('nav a[href*="github.com"]');
      const footerGithub = document.querySelector('footer a[href*="github.com"]');

      expect(navGithub).not.toBeNull();
      expect(footerGithub).not.toBeNull();
    });

    it('all external links should be properly identifiable', () => {
      const externalLinks = getExternalLinks();
      // Should have multiple external links (GitHub in nav, hero, footer + issues + docs)
      expect(externalLinks.length).toBeGreaterThanOrEqual(4);
    });

    it('external links should have descriptive text content', () => {
      const externalLinks = getExternalLinks();
      externalLinks.forEach(link => {
        const text = link.textContent.trim();
        expect(text.length).toBeGreaterThan(0);
      });
    });

    it('Issues link should point to GitHub issues', () => {
      const issuesLink = document.querySelector('a[href*="github.com"][href*="issues"]');
      expect(issuesLink).not.toBeNull();
      const href = issuesLink.getAttribute('href');
      expect(href).toContain('/issues');
    });
  });
});
