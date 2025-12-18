import { describe, it, expect, beforeEach } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

describe('External Links Validation', () => {
  let document: Document;
  let externalLinks: NodeListOf<HTMLAnchorElement>;

  beforeEach(() => {
    // Load the actual HTML file
    const htmlPath = path.resolve(__dirname, '../website/index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;

    // Find all external links (links starting with http:// or https://)
    const allLinks = document.querySelectorAll('a[href^="http://"], a[href^="https://"]');
    externalLinks = allLinks as NodeListOf<HTMLAnchorElement>;
  });

  describe('TC3: External link security attributes', () => {
    it('should have external links present on the page', () => {
      expect(externalLinks.length).toBeGreaterThan(0);
    });

    it('all external links should have rel="noopener noreferrer" for security', () => {
      const linksWithoutSecurityAttrs: string[] = [];

      externalLinks.forEach((link) => {
        const rel = link.getAttribute('rel') || '';
        const hasNoopener = rel.includes('noopener');
        const hasNoreferrer = rel.includes('noreferrer');

        if (!hasNoopener || !hasNoreferrer) {
          linksWithoutSecurityAttrs.push(link.href);
        }
      });

      expect(linksWithoutSecurityAttrs).toEqual([]);
    });

    it('GitHub link in navigation should have proper security attributes', () => {
      const navGithubLink = document.querySelector('.nav-links a[href*="github.com"]') as HTMLAnchorElement;
      expect(navGithubLink).not.toBeNull();

      const rel = navGithubLink?.getAttribute('rel') || '';
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    });

    it('GitHub link in hero section should have proper security attributes', () => {
      const heroSection = document.querySelector('.hero');
      const heroGithubLink = heroSection?.querySelector('a[href*="github.com"]') as HTMLAnchorElement;
      expect(heroGithubLink).not.toBeNull();

      const rel = heroGithubLink?.getAttribute('rel') || '';
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    });

    it('external links should not have javascript: URLs', () => {
      const allLinks = document.querySelectorAll('a');
      const javascriptLinks: string[] = [];

      allLinks.forEach((link) => {
        const href = link.getAttribute('href') || '';
        if (href.toLowerCase().startsWith('javascript:')) {
          javascriptLinks.push(href);
        }
      });

      expect(javascriptLinks).toEqual([]);
    });
  });

  describe('TC4: External links open in new tab', () => {
    it('all external links should have target="_blank"', () => {
      const linksWithoutNewTab: string[] = [];

      externalLinks.forEach((link) => {
        const target = link.getAttribute('target');
        if (target !== '_blank') {
          linksWithoutNewTab.push(link.href);
        }
      });

      expect(linksWithoutNewTab).toEqual([]);
    });

    it('GitHub link in navigation should open in new tab', () => {
      const navGithubLink = document.querySelector('.nav-links a[href*="github.com"]') as HTMLAnchorElement;
      expect(navGithubLink).not.toBeNull();
      expect(navGithubLink?.getAttribute('target')).toBe('_blank');
    });

    it('GitHub link in hero section should open in new tab', () => {
      const heroSection = document.querySelector('.hero');
      const heroGithubLink = heroSection?.querySelector('a[href*="github.com"]') as HTMLAnchorElement;
      expect(heroGithubLink).not.toBeNull();
      expect(heroGithubLink?.getAttribute('target')).toBe('_blank');
    });

    it('internal links should NOT have target="_blank"', () => {
      const internalLinks = document.querySelectorAll('a[href^="#"]');
      const internalLinksWithNewTab: string[] = [];

      internalLinks.forEach((link) => {
        const target = link.getAttribute('target');
        if (target === '_blank') {
          internalLinksWithNewTab.push(link.getAttribute('href') || '');
        }
      });

      expect(internalLinksWithNewTab).toEqual([]);
    });
  });

  describe('External links URL structure', () => {
    it('all GitHub links should point to the same repository', () => {
      const githubLinks = document.querySelectorAll('a[href*="github.com"]');
      const uniqueUrls = new Set<string>();

      githubLinks.forEach((link) => {
        const href = link.getAttribute('href') || '';
        uniqueUrls.add(href);
      });

      // All GitHub links should point to the same URL
      expect(uniqueUrls.size).toBe(1);
      expect(Array.from(uniqueUrls)[0]).toBe('https://github.com/yetone/mirdb');
    });

    it('GitHub URL should be properly formatted', () => {
      const githubLink = document.querySelector('a[href*="github.com"]');
      const href = githubLink?.getAttribute('href') || '';

      // Check URL format
      expect(href).toMatch(/^https:\/\/github\.com\/[\w-]+\/[\w-]+$/);
    });
  });

  describe('Footer external links', () => {
    it('footer GitHub link should have proper security attributes', () => {
      const footerGithubLink = document.querySelector('[data-testid="footer-github-link"]') as HTMLAnchorElement;

      // Footer link may or may not have target="_blank" depending on design
      // But if it's external, it should have security attributes
      if (footerGithubLink) {
        const href = footerGithubLink.getAttribute('href') || '';
        if (href.startsWith('http://') || href.startsWith('https://')) {
          // External link should have security attributes
          const hasSecurityAttrs = footerGithubLink.getAttribute('rel')?.includes('noopener') ||
                                   footerGithubLink.getAttribute('target') !== '_blank';
          // If target is _blank, it must have noopener
          if (footerGithubLink.getAttribute('target') === '_blank') {
            expect(footerGithubLink.getAttribute('rel')).toContain('noopener');
          }
        }
      }
    });
  });
});
