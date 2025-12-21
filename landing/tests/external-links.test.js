import { describe, it, expect, beforeEach } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

describe('External Links and Repository Access', () => {
  let document;
  let heroSection;
  let footer;

  beforeEach(() => {
    const htmlPath = path.resolve(__dirname, '../index.html');
    const html = fs.readFileSync(htmlPath, 'utf8');
    const dom = new JSDOM(html);
    document = dom.window.document;
    heroSection = document.querySelector('.hero');
    footer = document.querySelector('footer');
  });

  // Test Case 1: GitHub link presence in hero section
  describe('Test Case 1: GitHub Link in Hero Section', () => {
    it('should have a GitHub repository link in the hero section', () => {
      const githubLink = heroSection.querySelector('a[href*="github.com"]');
      expect(githubLink).not.toBeNull();
    });

    it('should link to the GitHub repository', () => {
      const githubLink = heroSection.querySelector('a[href*="github.com"]');
      expect(githubLink.getAttribute('href')).toContain('github.com');
      expect(githubLink.getAttribute('href')).toContain('mirdb');
    });

    it('should display View on GitHub text', () => {
      const githubLink = heroSection.querySelector('a[href*="github.com"]');
      expect(githubLink.textContent).toContain('GitHub');
    });
  });

  // Test Case 2: GitHub link opens in new tab
  describe('Test Case 2: GitHub Link Opens in New Tab', () => {
    it('should have target="_blank" on GitHub link in hero section', () => {
      const githubLink = heroSection.querySelector('a[href*="github.com"]');
      expect(githubLink.getAttribute('target')).toBe('_blank');
    });

    it('should have target="_blank" on footer GitHub link', () => {
      const footerGithubLink = footer.querySelector('a[href*="github.com"]');
      expect(footerGithubLink.getAttribute('target')).toBe('_blank');
    });
  });

  // Test Case 3: GitHub link has proper rel attribute for security
  describe('Test Case 3: GitHub Link Security Attributes', () => {
    it('should have rel attribute containing noopener on hero GitHub link', () => {
      const githubLink = heroSection.querySelector('a[href*="github.com"]');
      expect(githubLink.getAttribute('rel')).toContain('noopener');
    });

    it('should have rel attribute containing noreferrer on hero GitHub link', () => {
      const githubLink = heroSection.querySelector('a[href*="github.com"]');
      expect(githubLink.getAttribute('rel')).toContain('noreferrer');
    });

    it('should have proper security attributes on footer GitHub link', () => {
      const footerGithubLink = footer.querySelector('a[href*="github.com"]');
      expect(footerGithubLink.getAttribute('rel')).toContain('noopener');
      expect(footerGithubLink.getAttribute('rel')).toContain('noreferrer');
    });

    it('should have both noopener and noreferrer for security', () => {
      const githubLink = heroSection.querySelector('a[href*="github.com"]');
      const relValue = githubLink.getAttribute('rel');
      expect(relValue).toBe('noopener noreferrer');
    });
  });

  // Test Case 4: Documentation link presence
  describe('Test Case 4: Documentation Link', () => {
    it('should have a documentation link on the page', () => {
      const docLinks = document.querySelectorAll('a');
      const hasDocLink = Array.from(docLinks).some(link => {
        const href = link.getAttribute('href') || '';
        const text = link.textContent.toLowerCase();
        return href.includes('docs') ||
               href.includes('documentation') ||
               href.includes('readme') ||
               text.includes('doc');
      });
      expect(hasDocLink).toBe(true);
    });

    it('should have documentation link in the footer', () => {
      const footerLinks = footer.querySelectorAll('a');
      const docLink = Array.from(footerLinks).find(link => {
        const href = link.getAttribute('href') || '';
        const text = link.textContent.toLowerCase();
        return href.includes('readme') ||
               href.includes('docs') ||
               text.includes('doc');
      });
      expect(docLink).not.toBeUndefined();
    });

    it('should have documentation link open in new tab', () => {
      const footerLinks = footer.querySelectorAll('a');
      const docLink = Array.from(footerLinks).find(link => {
        const href = link.getAttribute('href') || '';
        const text = link.textContent.toLowerCase();
        return href.includes('readme') ||
               href.includes('docs') ||
               text.includes('doc');
      });
      if (docLink) {
        expect(docLink.getAttribute('target')).toBe('_blank');
      }
    });
  });

  // Test Case 5: All links have valid href values
  describe('Test Case 5: Valid Link HREFs', () => {
    it('should not have any links with empty href', () => {
      const allLinks = document.querySelectorAll('a');
      const emptyHrefLinks = Array.from(allLinks).filter(link => {
        const href = link.getAttribute('href');
        return href === '' || href === null;
      });
      expect(emptyHrefLinks.length).toBe(0);
    });

    it('should not have any navigation links with only "#" as href', () => {
      const allLinks = document.querySelectorAll('a');
      // Exclude anchor links that navigate to sections on the page (like #get-started)
      const invalidHashLinks = Array.from(allLinks).filter(link => {
        const href = link.getAttribute('href');
        // Only flag '#' by itself as invalid, not valid anchor links like '#get-started'
        return href === '#';
      });
      expect(invalidHashLinks.length).toBe(0);
    });

    it('should have all external links with valid URLs', () => {
      const allLinks = document.querySelectorAll('a');
      const externalLinks = Array.from(allLinks).filter(link => {
        const href = link.getAttribute('href') || '';
        return href.startsWith('http://') || href.startsWith('https://');
      });

      externalLinks.forEach(link => {
        const href = link.getAttribute('href');
        expect(href).toMatch(/^https?:\/\/.+/);
      });
    });

    it('should have all internal anchor links point to valid section IDs', () => {
      const allLinks = document.querySelectorAll('a');
      const anchorLinks = Array.from(allLinks).filter(link => {
        const href = link.getAttribute('href') || '';
        return href.startsWith('#') && href.length > 1;
      });

      anchorLinks.forEach(link => {
        const href = link.getAttribute('href');
        const targetId = href.substring(1);
        const targetElement = document.getElementById(targetId);
        expect(targetElement).not.toBeNull();
      });
    });
  });

  // Additional tests for comprehensive link coverage
  describe('All External Links Security', () => {
    it('should have all external links with target="_blank"', () => {
      const allLinks = document.querySelectorAll('a');
      const externalLinks = Array.from(allLinks).filter(link => {
        const href = link.getAttribute('href') || '';
        return href.startsWith('http://') || href.startsWith('https://');
      });

      externalLinks.forEach(link => {
        expect(link.getAttribute('target')).toBe('_blank');
      });
    });

    it('should have all external links with proper rel attributes', () => {
      const allLinks = document.querySelectorAll('a');
      const externalLinks = Array.from(allLinks).filter(link => {
        const href = link.getAttribute('href') || '';
        return href.startsWith('http://') || href.startsWith('https://');
      });

      externalLinks.forEach(link => {
        const rel = link.getAttribute('rel');
        expect(rel).toContain('noopener');
      });
    });
  });

  // Footer links verification
  describe('Footer Links', () => {
    it('should have GitHub link in footer', () => {
      const githubLink = footer.querySelector('a[href*="github.com"]');
      expect(githubLink).not.toBeNull();
    });

    it('should have multiple navigation links in footer', () => {
      const footerLinks = footer.querySelectorAll('a');
      expect(footerLinks.length).toBeGreaterThanOrEqual(2);
    });
  });
});
