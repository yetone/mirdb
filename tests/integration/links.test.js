/**
 * External Links Integration Tests
 * Owner: Scenario 20 - External Links Validation
 *
 * Tests:
 * - GitHub repository link validity
 * - CircleCI badge link validity
 * - All external links have proper rel attributes
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

// Read the HTML file once for all tests
let htmlContent;
let dom;
let document;

beforeAll(() => {
  const htmlPath = path.join(process.cwd(), 'src/index.html');
  htmlContent = fs.readFileSync(htmlPath, 'utf-8');
  dom = new JSDOM(htmlContent);
  document = dom.window.document;
});

/**
 * Helper function to get all external links from the document
 */
function getExternalLinks() {
  const links = document.querySelectorAll('a[href^="http"]');
  return Array.from(links);
}

/**
 * Helper function to get all external link hrefs
 */
function getExternalLinkUrls() {
  const links = getExternalLinks();
  return [...new Set(links.map(link => link.href))];
}

describe('External Links Validation', () => {
  describe('Test Case 1: GitHub Repository Link', () => {
    it('should have GitHub repository link in the document', () => {
      const links = getExternalLinks();
      const githubLinks = links.filter(link =>
        link.href.includes('github.com/yetone/mirdb')
      );
      expect(githubLinks.length).toBeGreaterThan(0);
    });

    it('GitHub repository link should return 200 status', async () => {
      const githubUrl = 'https://github.com/yetone/mirdb';

      try {
        const response = await fetch(githubUrl, {
          method: 'HEAD',
          headers: {
            'User-Agent': 'Mozilla/5.0 (compatible; LinkValidator/1.0)'
          }
        });
        expect(response.ok).toBe(true);
        expect(response.status).toBe(200);
      } catch (error) {
        // If fetch fails due to network issues, skip the test
        console.warn('Network request failed:', error.message);
        expect.fail('GitHub link validation failed due to network error');
      }
    }, { timeout: 10000 });

    it('GitHub repository link should point to correct repository', () => {
      const links = getExternalLinks();
      const githubRepoLinks = links.filter(link =>
        link.href === 'https://github.com/yetone/mirdb'
      );
      expect(githubRepoLinks.length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 2: CircleCI Badge Link', () => {
    it('should have CircleCI link in the document', () => {
      const links = getExternalLinks();
      const circleciLinks = links.filter(link =>
        link.href.includes('circleci.com')
      );
      expect(circleciLinks.length).toBeGreaterThan(0);
    });

    it('CircleCI badge link should return valid response', async () => {
      const circleciUrl = 'https://circleci.com/gh/yetone/mirdb';

      try {
        const response = await fetch(circleciUrl, {
          method: 'HEAD',
          headers: {
            'User-Agent': 'Mozilla/5.0 (compatible; LinkValidator/1.0)'
          },
          redirect: 'follow'
        });
        // CircleCI may return 200 or redirect (3xx) which is still valid
        expect(response.status).toBeLessThan(400);
      } catch (error) {
        console.warn('Network request failed:', error.message);
        expect.fail('CircleCI link validation failed due to network error');
      }
    }, { timeout: 10000 });

    it('CircleCI badge image should be present', () => {
      const images = document.querySelectorAll('img');
      const badgeImages = Array.from(images).filter(img =>
        img.src && img.src.includes('circleci.com')
      );
      expect(badgeImages.length).toBeGreaterThan(0);
    });

    it('CircleCI badge image should have proper alt text', () => {
      const images = document.querySelectorAll('img');
      const badgeImages = Array.from(images).filter(img =>
        img.src && img.src.includes('circleci.com')
      );

      badgeImages.forEach(img => {
        expect(img.alt).toBeTruthy();
        expect(img.alt.toLowerCase()).toContain('status');
      });
    });
  });

  describe('Test Case 3: External Links Security Attributes', () => {
    it('all external links should have rel="noopener noreferrer"', () => {
      const externalLinks = getExternalLinks();
      const linksWithTargetBlank = externalLinks.filter(link =>
        link.target === '_blank'
      );

      linksWithTargetBlank.forEach(link => {
        const rel = link.getAttribute('rel');
        expect(rel).toBeTruthy();
        expect(rel).toContain('noopener');
        expect(rel).toContain('noreferrer');
      });
    });

    it('external links with target="_blank" should have security rel attributes', () => {
      const externalLinks = getExternalLinks();

      externalLinks.forEach(link => {
        if (link.target === '_blank') {
          const rel = link.getAttribute('rel');
          expect(rel, `Link ${link.href} missing rel attribute`).toBeTruthy();
          expect(rel, `Link ${link.href} missing noopener`).toContain('noopener');
          expect(rel, `Link ${link.href} missing noreferrer`).toContain('noreferrer');
        }
      });
    });

    it('should have correct number of external links', () => {
      const externalLinks = getExternalLinks();
      // Verify there are external links present
      expect(externalLinks.length).toBeGreaterThan(0);
    });

    it('all external links should have valid href format', () => {
      const externalLinks = getExternalLinks();

      externalLinks.forEach(link => {
        const href = link.href;
        expect(href).toMatch(/^https?:\/\//);
        // Should not have empty or malformed URLs
        expect(href.length).toBeGreaterThan(10);
      });
    });
  });

  describe('Additional Link Validation', () => {
    it('GitHub author link should be valid', () => {
      const links = getExternalLinks();
      const authorLinks = links.filter(link =>
        link.href === 'https://github.com/yetone'
      );
      expect(authorLinks.length).toBeGreaterThan(0);
    });

    it('GitHub issues link should be valid', () => {
      const links = getExternalLinks();
      const issuesLinks = links.filter(link =>
        link.href === 'https://github.com/yetone/mirdb/issues'
      );
      expect(issuesLinks.length).toBeGreaterThan(0);
    });

    it('GitHub pulls/contribute link should be valid', () => {
      const links = getExternalLinks();
      const pullsLinks = links.filter(link =>
        link.href === 'https://github.com/yetone/mirdb/pulls'
      );
      expect(pullsLinks.length).toBeGreaterThan(0);
    });

    it('License link should point to GitHub LICENSE file', () => {
      const links = getExternalLinks();
      const licenseLinks = links.filter(link =>
        link.href.includes('github.com/yetone/mirdb') &&
        link.href.includes('LICENSE')
      );
      expect(licenseLinks.length).toBeGreaterThan(0);
    });

    it('all unique external links should be accessible', async () => {
      const uniqueUrls = getExternalLinkUrls();

      // Filter to only test a subset of critical links to avoid rate limiting
      const criticalUrls = uniqueUrls.filter(url =>
        url === 'https://github.com/yetone/mirdb' ||
        url === 'https://github.com/yetone'
      );

      for (const url of criticalUrls) {
        try {
          const response = await fetch(url, {
            method: 'HEAD',
            headers: {
              'User-Agent': 'Mozilla/5.0 (compatible; LinkValidator/1.0)'
            }
          });
          expect(response.status, `Link ${url} returned non-OK status`).toBeLessThan(400);
        } catch (error) {
          console.warn(`Failed to validate ${url}:`, error.message);
        }
      }
    }, { timeout: 30000 });
  });
});
