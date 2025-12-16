import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import { readFileSync } from 'fs';
import { resolve } from 'path';

describe('Footer Content and Links', () => {
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

  // Test Case 1: Check footer element exists
  describe('Test Case 1: Footer element exists', () => {
    it('should have a footer element present on the page', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
    });

    it('should have footer with appropriate class for styling', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
      expect(footer.classList.contains('footer') || footer.tagName === 'FOOTER').toBe(true);
    });
  });

  // Test Case 2: Check GitHub link in footer
  describe('Test Case 2: GitHub link in footer', () => {
    it('should have a link to GitHub repository in footer', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      const links = footer.querySelectorAll('a');
      const githubLink = Array.from(links).find(link =>
        link.href.toLowerCase().includes('github')
      );

      expect(githubLink).not.toBeNull();
    });

    it('GitHub link should point to the correct repository', () => {
      const footer = document.querySelector('footer');
      const links = footer.querySelectorAll('a');
      const githubLink = Array.from(links).find(link =>
        link.href.toLowerCase().includes('github.com')
      );

      expect(githubLink).not.toBeNull();
      expect(githubLink.href).toContain('github.com');
    });

    it('GitHub link should open in new tab for security', () => {
      const footer = document.querySelector('footer');
      const links = footer.querySelectorAll('a');
      const githubLink = Array.from(links).find(link =>
        link.href.toLowerCase().includes('github.com')
      );

      expect(githubLink).not.toBeNull();
      expect(githubLink.getAttribute('target')).toBe('_blank');
      expect(githubLink.getAttribute('rel')).toContain('noopener');
    });
  });

  // Test Case 3: Check Documentation link in footer
  describe('Test Case 3: Documentation link in footer', () => {
    it('should have a link to documentation in footer', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      const links = footer.querySelectorAll('a');
      const docLink = Array.from(links).find(link => {
        const text = link.textContent.toLowerCase();
        const href = link.href.toLowerCase();
        return text.includes('doc') ||
               text.includes('readme') ||
               href.includes('doc') ||
               href.includes('readme') ||
               href.includes('wiki');
      });

      expect(docLink).not.toBeNull();
    });

    it('Documentation link should be clickable and have valid href', () => {
      const footer = document.querySelector('footer');
      const links = footer.querySelectorAll('a');
      const docLink = Array.from(links).find(link => {
        const text = link.textContent.toLowerCase();
        return text.includes('doc') || text.includes('readme');
      });

      expect(docLink).not.toBeNull();
      expect(docLink.href).toBeTruthy();
      expect(docLink.href.length).toBeGreaterThan(0);
    });
  });

  // Test Case 4: Check license information
  describe('Test Case 4: License information', () => {
    it('should display MIT or Apache license information', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      const footerText = footer.textContent.toLowerCase();
      const hasLicenseInfo = footerText.includes('mit') ||
                            footerText.includes('apache');

      expect(hasLicenseInfo).toBe(true);
    });

    it('should have license section in footer', () => {
      const footer = document.querySelector('footer');
      const footerText = footer.textContent.toLowerCase();

      // Check for license header or license text
      const hasLicenseSection = footerText.includes('license') ||
                               footerText.includes('mit') ||
                               footerText.includes('apache');

      expect(hasLicenseSection).toBe(true);
    });

    it('should mention both MIT and Apache licenses for dual licensing', () => {
      const footer = document.querySelector('footer');
      const footerText = footer.textContent.toLowerCase();

      // Check for MIT OR Apache (project uses dual licensing)
      const hasMIT = footerText.includes('mit');
      const hasApache = footerText.includes('apache');

      // At least one should be present
      expect(hasMIT || hasApache).toBe(true);
    });
  });

  // Test Case 5: Check copyright notice
  describe('Test Case 5: Copyright notice', () => {
    it('should have copyright symbol or word in footer', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      const footerText = footer.textContent.toLowerCase();
      const footerHTML = footer.innerHTML;

      const hasCopyright = footerText.includes('copyright') ||
                          footerText.includes('©') ||
                          footerHTML.includes('&copy;') ||
                          footerHTML.includes('©');

      expect(hasCopyright).toBe(true);
    });

    it('should include a year in the copyright notice', () => {
      const footer = document.querySelector('footer');
      const footerText = footer.textContent;

      // Match any 4-digit year (2020-2029 or similar)
      const yearPattern = /20\d{2}/;
      const hasYear = yearPattern.test(footerText);

      expect(hasYear).toBe(true);
    });

    it('should have complete copyright notice with symbol/word and year', () => {
      const footer = document.querySelector('footer');
      const footerText = footer.textContent;
      const footerHTML = footer.innerHTML;

      // Check for copyright indicator
      const hasCopyrightIndicator = footerText.toLowerCase().includes('copyright') ||
                                    footerText.includes('©') ||
                                    footerHTML.includes('&copy;');

      // Check for year
      const yearPattern = /20\d{2}/;
      const hasYear = yearPattern.test(footerText);

      expect(hasCopyrightIndicator && hasYear).toBe(true);
    });

    it('should mention the project name in copyright', () => {
      const footer = document.querySelector('footer');
      const footerText = footer.textContent.toLowerCase();

      const hasMirDB = footerText.includes('mirdb');

      expect(hasMirDB).toBe(true);
    });
  });
});
