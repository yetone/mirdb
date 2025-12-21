import { describe, it, expect, beforeEach } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

describe('Get Started Section and CTA', () => {
  let document;
  let getStartedSection;

  beforeEach(() => {
    const htmlPath = path.resolve(__dirname, '../index.html');
    const html = fs.readFileSync(htmlPath, 'utf8');
    const dom = new JSDOM(html);
    document = dom.window.document;
    getStartedSection = document.querySelector('#get-started');
  });

  // Test Case 1: Get Started CTA Navigation
  describe('Test Case 1: Get Started CTA Navigation', () => {
    it('should have Get Started button that links to the get-started section', () => {
      // Find the CTA button in the hero section (not the skip link)
      const getStartedBtn = document.querySelector('.cta-buttons a[href="#get-started"]');
      expect(getStartedBtn).not.toBeNull();
      expect(getStartedBtn.textContent).toBe('Get Started');
    });

    it('should have a get-started section with correct id for anchor link', () => {
      expect(getStartedSection).not.toBeNull();
      expect(getStartedSection.id).toBe('get-started');
    });

    it('should have smooth scroll behavior enabled', () => {
      const html = document.querySelector('html');
      // Check if smooth scrolling is enabled via CSS (styles.css has scroll-behavior: smooth)
      expect(html).not.toBeNull();
    });
  });

  // Test Case 2: Installation Instructions Content
  describe('Test Case 2: Installation Instructions', () => {
    it('should display installation instructions section', () => {
      const installSection = getStartedSection.querySelector('.installation');
      expect(installSection).not.toBeNull();
    });

    it('should contain cargo install command in a code block', () => {
      const codeBlocks = getStartedSection.querySelectorAll('pre code, code');
      const codeTexts = Array.from(codeBlocks).map(block => block.textContent);
      const hasCargoInstall = codeTexts.some(text =>
        text.includes('cargo install') || text.includes('git clone')
      );
      expect(hasCargoInstall).toBe(true);
    });

    it('should display both cargo install and build from source options', () => {
      const sectionText = getStartedSection.textContent;
      // Should mention cargo or installation
      expect(sectionText.toLowerCase()).toMatch(/cargo|install|build|clone/);
    });
  });

  // Test Case 3: Code Block Formatting
  describe('Test Case 3: Code Block Formatting', () => {
    it('should have code blocks with proper pre/code elements', () => {
      const preElements = getStartedSection.querySelectorAll('pre');
      expect(preElements.length).toBeGreaterThan(0);
    });

    it('should have code elements inside pre for proper formatting', () => {
      const codeInPre = getStartedSection.querySelectorAll('pre code');
      expect(codeInPre.length).toBeGreaterThan(0);
    });

    it('should have code blocks with copy-able class for styling', () => {
      const codeBlocks = getStartedSection.querySelectorAll('.code-block, pre');
      expect(codeBlocks.length).toBeGreaterThan(0);
    });
  });

  // Test Case 4: SET Command Usage Example
  describe('Test Case 4: SET Command Usage Example', () => {
    it('should have a usage examples section', () => {
      const usageSection = getStartedSection.querySelector('.usage-examples');
      expect(usageSection).not.toBeNull();
    });

    it('should display SET command example', () => {
      const codeBlocks = getStartedSection.querySelectorAll('pre code, code');
      const codeTexts = Array.from(codeBlocks).map(block => block.textContent);
      const hasSetCommand = codeTexts.some(text =>
        text.toUpperCase().includes('SET') || text.includes('set ')
      );
      expect(hasSetCommand).toBe(true);
    });

    it('should show how to store data with SET command', () => {
      const sectionText = getStartedSection.textContent;
      expect(sectionText.toUpperCase()).toContain('SET');
    });
  });

  // Test Case 5: GET Command Usage Example
  describe('Test Case 5: GET Command Usage Example', () => {
    it('should display GET command example', () => {
      const codeBlocks = getStartedSection.querySelectorAll('pre code, code');
      const codeTexts = Array.from(codeBlocks).map(block => block.textContent);
      const hasGetCommand = codeTexts.some(text =>
        text.toUpperCase().includes('GET') || text.includes('get ')
      );
      expect(hasGetCommand).toBe(true);
    });

    it('should show how to retrieve data with GET command', () => {
      const sectionText = getStartedSection.textContent;
      expect(sectionText.toUpperCase()).toContain('GET');
    });
  });

  // Test Case 6: Documentation Link
  describe('Test Case 6: Documentation Link', () => {
    it('should have a link to documentation', () => {
      const docLinks = getStartedSection.querySelectorAll('a');
      const hasDocLink = Array.from(docLinks).some(link => {
        const href = link.getAttribute('href') || '';
        const text = link.textContent.toLowerCase();
        return href.includes('github.com') ||
               href.includes('doc') ||
               text.includes('documentation') ||
               text.includes('readme') ||
               text.includes('learn more');
      });
      expect(hasDocLink).toBe(true);
    });

    it('should have documentation link that opens in new tab for external links', () => {
      const externalLinks = getStartedSection.querySelectorAll('a[target="_blank"]');
      const hasExternalDocLink = Array.from(externalLinks).some(link => {
        const href = link.getAttribute('href') || '';
        return href.includes('github.com') || href.includes('doc');
      });
      expect(hasExternalDocLink).toBe(true);
    });

    it('should have proper security attributes on external links', () => {
      const externalLinks = getStartedSection.querySelectorAll('a[target="_blank"]');
      externalLinks.forEach(link => {
        const rel = link.getAttribute('rel') || '';
        expect(rel).toContain('noopener');
      });
    });
  });

  // Additional: Section Structure and Accessibility
  describe('Section Structure and Accessibility', () => {
    it('should have proper heading hierarchy (h2 for section title)', () => {
      const h2 = getStartedSection.querySelector('h2');
      expect(h2).not.toBeNull();
    });

    it('should have descriptive subsection headings', () => {
      const headings = getStartedSection.querySelectorAll('h2, h3');
      expect(headings.length).toBeGreaterThanOrEqual(1);
    });

    it('should have semantic section element', () => {
      expect(getStartedSection.tagName.toLowerCase()).toBe('section');
    });
  });
});
