import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

/**
 * Navigation Header Functionality Tests
 * Testing REQ-3: Navigation header with logo, menu links, and CTA button
 */

describe('Navigation Header Functionality', () => {
  let dom;
  let document;
  let cssContent;

  beforeEach(() => {
    // Load the HTML file
    const htmlPath = path.resolve(__dirname, '../index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');
    dom = new JSDOM(htmlContent, {
      url: 'http://localhost:3000',
      runScripts: 'dangerously',
      resources: 'usable',
    });
    document = dom.window.document;

    // Load the CSS file
    const cssPath = path.resolve(__dirname, '../styles.css');
    cssContent = fs.readFileSync(cssPath, 'utf-8');
  });

  afterEach(() => {
    if (dom) {
      dom.window.close();
    }
  });

  /**
   * Test Case 1: Check header element exists
   */
  describe('Test Case 1: Header Element Presence', () => {
    it('should have a header element in the DOM', () => {
      const header = document.querySelector('header');
      expect(header).not.toBeNull();
      expect(header).toBeInTheDocument();
    });

    it('should have header with .header class', () => {
      const header = document.querySelector('header.header');
      expect(header).not.toBeNull();
    });

    it('should contain a navigation element', () => {
      const header = document.querySelector('header');
      const nav = header.querySelector('nav');
      expect(nav).not.toBeNull();
      expect(nav).toBeInTheDocument();
    });

    it('should have nav with .nav class', () => {
      const nav = document.querySelector('header nav.nav');
      expect(nav).not.toBeNull();
    });
  });

  /**
   * Test Case 2: Verify logo element and position
   */
  describe('Test Case 2: Logo Element and Position', () => {
    it('should have a logo element within header', () => {
      const header = document.querySelector('header');
      const logo = header.querySelector('.logo');
      expect(logo).not.toBeNull();
      expect(logo).toBeInTheDocument();
    });

    it('should have logo with text content', () => {
      const logo = document.querySelector('header .logo');
      const logoText = logo.querySelector('.logo-text');
      expect(logoText).not.toBeNull();
      expect(logoText.textContent.trim()).not.toBe('');
    });

    it('should have logo positioned first in nav (left-aligned)', () => {
      const nav = document.querySelector('header nav.nav');
      const firstChild = nav.firstElementChild;
      expect(firstChild.classList.contains('logo')).toBe(true);
    });

    it('should use flexbox for nav layout (left-align logo)', () => {
      // Check CSS for flexbox layout
      expect(cssContent).toMatch(/\.nav\s*\{[^}]*display:\s*flex/);
    });
  });

  /**
   * Test Case 3: Check navigation links count
   */
  describe('Test Case 3: Navigation Links Count', () => {
    it('should have at least 3 navigation links', () => {
      const navLinks = document.querySelectorAll('.nav-links a');
      expect(navLinks.length).toBeGreaterThanOrEqual(3);
    });

    it('should have navigation links in a list', () => {
      const navLinksList = document.querySelector('.nav-links');
      expect(navLinksList).not.toBeNull();
      expect(navLinksList.tagName.toLowerCase()).toBe('ul');
    });

    it('should have navigation links with href attributes', () => {
      const navLinks = document.querySelectorAll('.nav-links a');
      navLinks.forEach(link => {
        expect(link.hasAttribute('href')).toBe(true);
        expect(link.getAttribute('href')).not.toBe('');
      });
    });

    it('should have navigation links with text content', () => {
      const navLinks = document.querySelectorAll('.nav-links a');
      navLinks.forEach(link => {
        expect(link.textContent.trim()).not.toBe('');
      });
    });
  });

  /**
   * Test Case 4: Test sticky header CSS property
   */
  describe('Test Case 4: Sticky Header CSS Property', () => {
    it('should have header with position: fixed in CSS', () => {
      expect(cssContent).toMatch(/\.header\s*\{[^}]*position:\s*fixed/);
    });

    it('should have header positioned at top: 0', () => {
      expect(cssContent).toMatch(/\.header\s*\{[^}]*top:\s*0/);
    });

    it('should have header span full width (left: 0, right: 0)', () => {
      expect(cssContent).toMatch(/\.header\s*\{[^}]*left:\s*0/);
      expect(cssContent).toMatch(/\.header\s*\{[^}]*right:\s*0/);
    });

    it('should have header with z-index for proper stacking', () => {
      // Check for z-index definition
      const zIndexMatch = cssContent.match(/\.header\s*\{[^}]*z-index:\s*(\d+)/);
      expect(zIndexMatch).not.toBeNull();
      const zIndex = parseInt(zIndexMatch[1]);
      expect(zIndex).toBeGreaterThan(0);
    });

    it('should have header with defined height', () => {
      // Check for header height (either direct or via CSS variable)
      const hasHeaderHeight = cssContent.includes('--header-height') ||
                              cssContent.match(/\.header\s*\{[^}]*height:/);
      expect(hasHeaderHeight).toBe(true);
    });
  });

  /**
   * Test Case 5: Header CTA button visibility and distinctness
   */
  describe('Test Case 5: Header CTA Button', () => {
    it('should have a CTA button in the header', () => {
      const ctaButton = document.querySelector('header .nav-cta');
      expect(ctaButton).not.toBeNull();
      expect(ctaButton).toBeInTheDocument();
    });

    it('should have CTA button with text content', () => {
      const ctaButton = document.querySelector('header .nav-cta');
      expect(ctaButton.textContent.trim()).not.toBe('');
    });

    it('should have CTA button with href attribute', () => {
      const ctaButton = document.querySelector('header .nav-cta');
      expect(ctaButton.hasAttribute('href')).toBe(true);
    });

    it('should have CTA button with distinct background color', () => {
      // Check CSS for background color on nav-cta
      expect(cssContent).toMatch(/\.nav-cta\s*\{[^}]*background:\s*var\(--primary-color\)/);
    });

    it('should have CTA button with different styling from nav links', () => {
      // CTA has background, padding, and border-radius
      expect(cssContent).toMatch(/\.nav-cta\s*\{[^}]*padding:/);
      expect(cssContent).toMatch(/\.nav-cta\s*\{[^}]*border-radius:/);
    });

    it('should have CTA button with proper text color', () => {
      expect(cssContent).toMatch(/\.nav-cta\s*\{[^}]*color:\s*var\(--primary-text-on-button\)/);
    });
  });

  /**
   * Test Case 6: Navigation structure accessibility
   */
  describe('Navigation Accessibility', () => {
    it('should have header as semantic HTML element', () => {
      const header = document.querySelector('header');
      expect(header.tagName.toLowerCase()).toBe('header');
    });

    it('should have nav as semantic HTML element', () => {
      const nav = document.querySelector('header nav');
      expect(nav.tagName.toLowerCase()).toBe('nav');
    });

    it('should have navigation links with proper list structure', () => {
      const navList = document.querySelector('.nav-links');
      expect(navList.tagName.toLowerCase()).toBe('ul');

      const listItems = navList.querySelectorAll('li');
      expect(listItems.length).toBeGreaterThanOrEqual(3);

      listItems.forEach(item => {
        const link = item.querySelector('a');
        expect(link).not.toBeNull();
      });
    });
  });
});
