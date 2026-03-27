/**
 * Footer Section Unit Tests
 * Owner: Scenario 16 - Footer Section
 *
 * Tests:
 * - Footer element presence with proper semantic markup
 * - Author credit 'yetone' is displayed
 * - Footer structure and accessibility
 */
import { describe, it, expect, beforeAll } from 'vitest';
import { JSDOM } from 'jsdom';
import { readFileSync } from 'fs';
import { resolve } from 'path';

describe('Footer Section', () => {
  let document;
  let footer;

  beforeAll(() => {
    const htmlPath = resolve(__dirname, '../../src/index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
    footer = document.querySelector('footer#footer');
  });

  describe('Footer Element Structure', () => {
    it('should have a footer element with proper semantic markup', () => {
      expect(footer).not.toBeNull();
      expect(footer.tagName.toLowerCase()).toBe('footer');
    });

    it('should have id="footer" for navigation targeting', () => {
      expect(footer.id).toBe('footer');
    });

    it('should have role="contentinfo" for accessibility', () => {
      expect(footer.getAttribute('role')).toBe('contentinfo');
    });

    it('should be placed at the end of the document body', () => {
      const body = document.body;
      const lastElement = body.lastElementChild;
      // The last element before script should be footer or the script itself
      const allChildren = Array.from(body.children);
      const footerIndex = allChildren.findIndex(el => el.id === 'footer');
      const scriptIndex = allChildren.findIndex(el => el.tagName.toLowerCase() === 'script');

      // Footer should exist and be near the end (before script)
      expect(footerIndex).toBeGreaterThan(-1);
      if (scriptIndex > -1) {
        expect(footerIndex).toBe(scriptIndex - 1);
      }
    });
  });

  describe('Author Credit', () => {
    it('should credit author yetone', () => {
      const footerText = footer.textContent.toLowerCase();
      expect(footerText).toContain('yetone');
    });

    it('should have a link to author GitHub profile', () => {
      const authorLink = footer.querySelector('[data-author="yetone"]');
      expect(authorLink).not.toBeNull();
      expect(authorLink.getAttribute('href')).toBe('https://github.com/yetone');
    });

    it('should display "Created by" text near author name', () => {
      const footerText = footer.textContent.toLowerCase();
      expect(footerText).toContain('created by');
    });
  });

  describe('Footer Content', () => {
    it('should contain MirDB project name', () => {
      const footerText = footer.textContent;
      expect(footerText).toContain('MirDB');
    });

    it('should have headings for sections', () => {
      const headings = footer.querySelectorAll('h3');
      expect(headings.length).toBeGreaterThanOrEqual(1);
    });

    it('should have proper text contrast classes', () => {
      // Footer should have dark background with light text
      expect(footer.classList.contains('bg-gray-900')).toBe(true);
      expect(footer.classList.contains('text-gray-300')).toBe(true);
    });
  });

  describe('Accessibility', () => {
    it('should have accessible links with proper attributes', () => {
      const externalLinks = footer.querySelectorAll('a[target="_blank"]');
      externalLinks.forEach(link => {
        expect(link.getAttribute('rel')).toContain('noopener');
      });
    });

    it('should not have empty links', () => {
      const links = footer.querySelectorAll('a');
      links.forEach(link => {
        const hasText = link.textContent.trim().length > 0;
        const hasAriaLabel = link.getAttribute('aria-label');
        expect(hasText || hasAriaLabel).toBe(true);
      });
    });
  });
});
