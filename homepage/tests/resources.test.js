/**
 * Documentation and resource links tests for MirDB homepage.
 * Owner: Scenario 5 - Documentation and Resource Links
 *
 * Test framework: Vitest + jsdom
 *
 * Test coverage:
 * - Documentation links exist (API docs, configuration guide, architecture overview)
 * - Project resource links exist (GitHub, releases, issues, contribution guide)
 * - Community/support link exists
 * - All external links have rel="noopener noreferrer"
 * - Link text is descriptive (not "click here")
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { readFileSync } from 'fs';
import { resolve } from 'path';
import { JSDOM } from 'jsdom';

const htmlPath = resolve(__dirname, '../index.html');
const cssPath = resolve(__dirname, '../css/resources.css');

let dom;
let document;

beforeAll(() => {
  const html = readFileSync(htmlPath, 'utf-8');

  dom = new JSDOM(html, { url: 'http://localhost:8080' });

  // Inject resources CSS into the DOM
  if (require('fs').existsSync(cssPath)) {
    const css = readFileSync(cssPath, 'utf-8');
    const styleEl = dom.window.document.createElement('style');
    styleEl.textContent = css;
    dom.window.document.head.appendChild(styleEl);
  }

  document = dom.window.document;
});

// Helper: get raw CSS content
function getRawCSS() {
  return readFileSync(cssPath, 'utf-8');
}

// Helper: get all resource links in the resources section
function getResourceLinks() {
  return document.querySelectorAll('#resources .resource-link');
}

describe('Documentation and Resource Links', () => {
  describe('Test Case 1: Documentation links presence and descriptive text', () => {
    it('should have a resources section', () => {
      const section = document.getElementById('resources');
      expect(section).not.toBeNull();
    });

    it('should have a link with text "API Documentation"', () => {
      const links = getResourceLinks();
      const apiLink = Array.from(links).find(
        (l) => l.textContent.trim() === 'API Documentation'
      );
      expect(apiLink).not.toBeNull();
    });

    it('should have a link with text "Configuration Guide"', () => {
      const links = getResourceLinks();
      const configLink = Array.from(links).find(
        (l) => l.textContent.trim() === 'Configuration Guide'
      );
      expect(configLink).not.toBeNull();
    });

    it('should have a link with text "Architecture Overview"', () => {
      const links = getResourceLinks();
      const archLink = Array.from(links).find(
        (l) => l.textContent.trim() === 'Architecture Overview'
      );
      expect(archLink).not.toBeNull();
    });

    it('should not use "click here" as link text', () => {
      const links = getResourceLinks();
      for (const link of links) {
        const text = link.textContent.trim().toLowerCase();
        expect(text).not.toMatch(/click here/);
      }
    });

    it('should have descriptive link text (at least 2 words)', () => {
      const links = getResourceLinks();
      for (const link of links) {
        const text = link.textContent.trim();
        const wordCount = text.split(/\s+/).length;
        expect(wordCount).toBeGreaterThanOrEqual(2);
      }
    });
  });

  describe('Test Case 2: Project resource links with correct URLs', () => {
    it('should have a "GitHub Repository" link pointing to github.com/yetone/mirdb', () => {
      const links = getResourceLinks();
      const ghLink = Array.from(links).find(
        (l) => l.textContent.trim() === 'GitHub Repository'
      );
      expect(ghLink).not.toBeNull();
      const href = ghLink.getAttribute('href');
      expect(href).toMatch(/github\.com\/yetone\/mirdb/);
    });

    it('should have a "Release Notes" link pointing to GitHub releases', () => {
      const links = getResourceLinks();
      const relLink = Array.from(links).find(
        (l) => l.textContent.trim() === 'Release Notes'
      );
      expect(relLink).not.toBeNull();
      const href = relLink.getAttribute('href');
      expect(href).toMatch(/github\.com\/yetone\/mirdb\/releases/);
    });

    it('should have an "Issue Tracker" link pointing to GitHub issues', () => {
      const links = getResourceLinks();
      const issueLink = Array.from(links).find(
        (l) => l.textContent.trim() === 'Issue Tracker'
      );
      expect(issueLink).not.toBeNull();
      const href = issueLink.getAttribute('href');
      expect(href).toMatch(/github\.com\/yetone\/mirdb\/issues/);
    });

    it('should have a "Contribution Guidelines" link pointing to CONTRIBUTING.md', () => {
      const links = getResourceLinks();
      const contribLink = Array.from(links).find(
        (l) => l.textContent.trim() === 'Contribution Guidelines'
      );
      expect(contribLink).not.toBeNull();
      const href = contribLink.getAttribute('href');
      expect(href).toMatch(/github\.com\/yetone\/mirdb/);
      expect(href).toMatch(/CONTRIBUTING/i);
    });
  });

  describe('Test Case 3: Community/support link', () => {
    it('should have at least one community/support link', () => {
      const communityLinks = document.querySelectorAll(
        '#resources .community-link'
      );
      expect(communityLinks.length).toBeGreaterThanOrEqual(1);
    });

    it('should have a "GitHub Discussions" link', () => {
      const links = getResourceLinks();
      const discussLink = Array.from(links).find(
        (l) => l.textContent.trim() === 'GitHub Discussions'
      );
      expect(discussLink).not.toBeNull();
    });

    it('community link should point to GitHub Discussions', () => {
      const discussLink = Array.from(getResourceLinks()).find(
        (l) => l.textContent.trim() === 'GitHub Discussions'
      );
      const href = discussLink.getAttribute('href');
      expect(href).toMatch(/github\.com\/yetone\/mirdb\/discussions/);
    });

    it('community link text should indicate support or community purpose', () => {
      const communityLinks = document.querySelectorAll(
        '#resources .community-link'
      );
      for (const link of communityLinks) {
        const text = link.textContent.toLowerCase();
        const isCommunityRelated =
          text.includes('discussion') ||
          text.includes('community') ||
          text.includes('support') ||
          text.includes('chat');
        expect(isCommunityRelated).toBe(true);
      }
    });
  });

  describe('Test Case 4: API Documentation link resolves to valid docs', () => {
    it('API Documentation link href should point to README.md documentation', () => {
      const links = getResourceLinks();
      const apiLink = Array.from(links).find(
        (l) => l.textContent.trim() === 'API Documentation'
      );
      expect(apiLink).not.toBeNull();
      const href = apiLink.getAttribute('href');
      expect(href).toMatch(/github\.com\/yetone\/mirdb/);
      expect(href).toMatch(/README/i);
    });

    it('API Documentation link should be a valid URL format', () => {
      const links = getResourceLinks();
      const apiLink = Array.from(links).find(
        (l) => l.textContent.trim() === 'API Documentation'
      );
      const href = apiLink.getAttribute('href');
      expect(() => new URL(href)).not.toThrow();
    });
  });

  describe('Test Case 5: Release Notes link resolves to GitHub releases', () => {
    it('Release Notes href should contain /releases', () => {
      const links = getResourceLinks();
      const relLink = Array.from(links).find(
        (l) => l.textContent.trim() === 'Release Notes'
      );
      expect(relLink).not.toBeNull();
      expect(relLink.getAttribute('href')).toMatch(/\/releases/);
    });

    it('Release Notes link should be a valid URL format', () => {
      const links = getResourceLinks();
      const relLink = Array.from(links).find(
        (l) => l.textContent.trim() === 'Release Notes'
      );
      const href = relLink.getAttribute('href');
      expect(() => new URL(href)).not.toThrow();
    });
  });

  describe('Test Case 6: Issue Tracker link resolves to GitHub issues', () => {
    it('Issue Tracker href should contain /issues', () => {
      const links = getResourceLinks();
      const issueLink = Array.from(links).find(
        (l) => l.textContent.trim() === 'Issue Tracker'
      );
      expect(issueLink).not.toBeNull();
      expect(issueLink.getAttribute('href')).toMatch(/\/issues/);
    });

    it('Issue Tracker link should be a valid URL format', () => {
      const links = getResourceLinks();
      const issueLink = Array.from(links).find(
        (l) => l.textContent.trim() === 'Issue Tracker'
      );
      const href = issueLink.getAttribute('href');
      expect(() => new URL(href)).not.toThrow();
    });
  });

  describe('Test Case 7: External link security attributes', () => {
    it('all external resource links should have rel="noopener noreferrer"', () => {
      const links = getResourceLinks();
      for (const link of links) {
        const rel = link.getAttribute('rel');
        expect(rel).not.toBeNull();
        expect(rel).toMatch(/noopener/);
        expect(rel).toMatch(/noreferrer/);
      }
    });

    it('all external resource links should have target="_blank"', () => {
      const links = getResourceLinks();
      for (const link of links) {
        expect(link.getAttribute('target')).toBe('_blank');
      }
    });

    it('internal links (href starting with #) should not have target="_blank"', () => {
      const allLinks = document.querySelectorAll('a[href^="#"]');
      for (const link of allLinks) {
        expect(link.getAttribute('target')).not.toBe('_blank');
      }
    });
  });

  describe('Resources section structure', () => {
    it('should have a section heading', () => {
      const heading = document.querySelector('#resources h2');
      expect(heading).not.toBeNull();
      expect(heading.textContent.trim().length).toBeGreaterThan(0);
    });

    it('should organize links into resource groups', () => {
      const groups = document.querySelectorAll('#resources .resource-group');
      expect(groups.length).toBeGreaterThanOrEqual(3);
    });

    it('each resource group should have a title', () => {
      const groups = document.querySelectorAll('#resources .resource-group');
      for (const group of groups) {
        const title = group.querySelector('.resource-group-title');
        expect(title).not.toBeNull();
        expect(title.textContent.trim().length).toBeGreaterThan(0);
      }
    });
  });

  describe('Resources CSS styles', () => {
    it('should define a .resources-grid class with grid display', () => {
      const css = getRawCSS();
      expect(css).toMatch(/\.resources-grid\s*\{[^}]*display:\s*grid/);
    });

    it('should define .resource-link hover styles', () => {
      const css = getRawCSS();
      expect(css).toMatch(/\.resource-link:hover/);
    });

    it('should define .resource-link focus styles', () => {
      const css = getRawCSS();
      expect(css).toMatch(/\.resource-link:focus/);
    });

    it('should define responsive breakpoints', () => {
      const css = getRawCSS();
      expect(css).toMatch(/@media[^{]*max-width:\s*767px/);
    });

    it('should have transition defined on resource links', () => {
      const css = getRawCSS();
      expect(css).toMatch(/transition/);
    });
  });
});
