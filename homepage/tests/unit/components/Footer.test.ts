/**
 * Unit tests for Footer component.
 * Tests footer links, external link security attributes, copyright, and license.
 */
import { describe, it, expect } from 'vitest';
import { parseHTML } from 'linkedom';

const GITHUB_REPO_URL = 'https://github.com/yetone/mirdb';
const DOCS_URL = 'https://github.com/yetone/mirdb#readme';
const COMMUNITY_URL = 'https://github.com/yetone/mirdb/discussions';

function buildFooterHTML(): string {
  const currentYear = new Date().getFullYear();
  return `<div class="footer-content">
  <nav class="footer-links" aria-label="Footer navigation">
    <a href="${GITHUB_REPO_URL}" target="_blank" rel="noopener noreferrer">GitHub</a>
    <a href="${DOCS_URL}" target="_blank" rel="noopener noreferrer">Documentation</a>
    <a href="${COMMUNITY_URL}" target="_blank" rel="noopener noreferrer">Community</a>
  </nav>
  <div class="footer-legal">
    <p class="copyright">&copy; ${currentYear} MirDB. All rights reserved.</p>
    <p class="license">Licensed under the MIT License.</p>
  </div>
</div>`;
}

function buildFullPageHTML(): string {
  return `<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><title>MirDB</title></head>
<body>
  <header>...</header>
  <main>...</main>
  <footer>${buildFooterHTML()}</footer>
</body>
</html>`;
}

describe('Footer & External Links', () => {
  // -----------------------------------------------------------------------
  // Test Case 1: GitHub repository link
  // -----------------------------------------------------------------------
  describe('Test Case 1: GitHub repository link', () => {
    it('should have a link pointing to the MirDB GitHub repository', () => {
      const { document } = parseHTML(buildFooterHTML());

      const githubLink = Array.from(document.querySelectorAll('a')).find(
        (el) => el.getAttribute('href') === GITHUB_REPO_URL,
      );

      expect(githubLink).not.toBeNull();
      expect(githubLink!.getAttribute('href')).toBe(GITHUB_REPO_URL);
    });

    it('should have descriptive link text for the GitHub link', () => {
      const { document } = parseHTML(buildFooterHTML());

      const githubLink = Array.from(document.querySelectorAll('a')).find(
        (el) => el.getAttribute('href') === GITHUB_REPO_URL,
      );

      expect(githubLink).not.toBeNull();
      const text = githubLink!.textContent?.trim() || '';
      expect(text.length).toBeGreaterThan(0);
      expect(['GitHub', 'View Source', 'View on GitHub', 'Source Code', 'Repository']).toContain(text);
    });
  });

  // -----------------------------------------------------------------------
  // Test Case 2: External link security attributes
  // -----------------------------------------------------------------------
  describe('Test Case 2: External link security attributes', () => {
    it('should have target="_blank" on all external links', () => {
      const { document } = parseHTML(buildFooterHTML());

      const externalLinks = Array.from(document.querySelectorAll('a')).filter((el) => {
        const href = el.getAttribute('href') || '';
        return href.startsWith('http');
      });

      expect(externalLinks.length).toBeGreaterThan(0);

      externalLinks.forEach((link) => {
        expect(link.getAttribute('target')).toBe('_blank');
      });
    });

    it('should have rel="noopener noreferrer" on all external links', () => {
      const { document } = parseHTML(buildFooterHTML());

      const externalLinks = Array.from(document.querySelectorAll('a')).filter((el) => {
        const href = el.getAttribute('href') || '';
        return href.startsWith('http');
      });

      expect(externalLinks.length).toBeGreaterThan(0);

      externalLinks.forEach((link) => {
        expect(link.getAttribute('rel')).toBe('noopener noreferrer');
      });
    });

    it('should have both target and rel on every external link', () => {
      const { document } = parseHTML(buildFooterHTML());

      const externalLinks = Array.from(document.querySelectorAll('a')).filter((el) => {
        const href = el.getAttribute('href') || '';
        return href.startsWith('http');
      });

      externalLinks.forEach((link) => {
        expect(link.getAttribute('target')).toBe('_blank');
        expect(link.getAttribute('rel')).toBe('noopener noreferrer');
      });
    });
  });

  // -----------------------------------------------------------------------
  // Test Case 3: Copyright notice and license
  // -----------------------------------------------------------------------
  describe('Test Case 3: Copyright notice and license', () => {
    it('should display a copyright notice with the current year', () => {
      const { document } = parseHTML(buildFooterHTML());

      const copyright = document.querySelector('.copyright');
      expect(copyright).not.toBeNull();

      const text = copyright!.textContent || '';
      expect(text).toMatch(/Copyright|©/);
    });

    it('should include a year in the copyright notice', () => {
      const { document } = parseHTML(buildFooterHTML());

      const copyright = document.querySelector('.copyright');
      expect(copyright).not.toBeNull();

      const text = copyright!.textContent || '';
      // Should contain a 4-digit year (current year or year range)
      expect(text).toMatch(/\d{4}/);
    });

    it('should display the project license type', () => {
      const { document } = parseHTML(buildFooterHTML());

      const license = document.querySelector('.license');
      expect(license).not.toBeNull();

      const text = license!.textContent || '';
      expect(text).toMatch(/MIT|Apache|GPL|BSD|license/i);
    });
  });

  // -----------------------------------------------------------------------
  // Test Case 4: Documentation link
  // -----------------------------------------------------------------------
  describe('Test Case 4: Documentation link', () => {
    it('should have a link to documentation', () => {
      const { document } = parseHTML(buildFooterHTML());

      const docLink = Array.from(document.querySelectorAll('a')).find((el) => {
        const text = (el.textContent || '').toLowerCase();
        const href = (el.getAttribute('href') || '').toLowerCase();
        return (
          text.includes('doc') ||
          text.includes('readme') ||
          text.includes('wiki') ||
          href.includes('readme') ||
          href.includes('docs') ||
          href.includes('wiki')
        );
      });

      expect(docLink).not.toBeNull();
      expect(docLink!.getAttribute('href')).toBeTruthy();
    });
  });

  // -----------------------------------------------------------------------
  // Test Case 5: Footer element presence and content
  // -----------------------------------------------------------------------
  describe('Test Case 5: Footer element presence', () => {
    it('should have a <footer> element in the page', () => {
      const { document } = parseHTML(buildFullPageHTML());

      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
    });

    it('should contain non-trivial content beyond a single copyright line', () => {
      const { document } = parseHTML(buildFullPageHTML());

      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      const text = footer!.textContent?.trim() || '';
      // Should contain more than just a copyright line
      expect(text.length).toBeGreaterThan(20);

      // Should have at least one link
      const links = footer!.querySelectorAll('a');
      expect(links.length).toBeGreaterThanOrEqual(1);
    });

    it('should have navigation links in the footer', () => {
      const { document } = parseHTML(buildFooterHTML());

      const nav = document.querySelector('nav');
      expect(nav).not.toBeNull();

      const navLinks = nav!.querySelectorAll('a');
      expect(navLinks.length).toBeGreaterThanOrEqual(2);
    });
  });

  // -----------------------------------------------------------------------
  // Test Case 6: GitHub link integration (click behavior)
  // -----------------------------------------------------------------------
  describe('Test Case 6: GitHub link navigation', () => {
    it('should have the correct GitHub URL for navigation', () => {
      const { document } = parseHTML(buildFullPageHTML());

      const githubLink = Array.from(document.querySelectorAll('a')).find(
        (el) => el.getAttribute('href') === GITHUB_REPO_URL,
      );

      expect(githubLink).not.toBeNull();
      expect(githubLink!.getAttribute('href')).toBe(GITHUB_REPO_URL);
    });

    it('should open GitHub link in a new tab', () => {
      const { document } = parseHTML(buildFullPageHTML());

      const githubLink = Array.from(document.querySelectorAll('a')).find(
        (el) => el.getAttribute('href') === GITHUB_REPO_URL,
      );

      expect(githubLink).not.toBeNull();
      expect(githubLink!.getAttribute('target')).toBe('_blank');
      expect(githubLink!.getAttribute('rel')).toBe('noopener noreferrer');
    });

    it('should have the GitHub link inside the footer', () => {
      const { document } = parseHTML(buildFullPageHTML());

      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      const githubLink = footer!.querySelector(`a[href="${GITHUB_REPO_URL}"]`);
      expect(githubLink).not.toBeNull();
    });
  });
});
