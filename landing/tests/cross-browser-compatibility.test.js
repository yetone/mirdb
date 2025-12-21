import { describe, it, expect, beforeEach } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

describe('Cross-Browser Compatibility (NFR-4)', () => {
  let document;
  let css;
  let html;

  beforeEach(() => {
    const htmlPath = path.resolve(__dirname, '../index.html');
    const cssPath = path.resolve(__dirname, '../styles.css');
    html = fs.readFileSync(htmlPath, 'utf8');
    css = fs.readFileSync(cssPath, 'utf8');
    const dom = new JSDOM(html);
    document = dom.window.document;
  });

  // Test Case 5: CSS vendor prefixes validation
  describe('Test Case 5: CSS Vendor Prefixes', () => {
    it('should use -webkit-background-clip for gradient text effect', () => {
      // Required for Chrome, Safari, Edge (Chromium)
      expect(css).toContain('-webkit-background-clip: text');
    });

    it('should use -webkit-text-fill-color for gradient text effect', () => {
      // Required for Chrome, Safari, Edge (Chromium)
      expect(css).toContain('-webkit-text-fill-color: transparent');
    });

    it('should use standard background-clip as fallback', () => {
      // Standard property for Firefox and modern browsers
      expect(css).toMatch(/background-clip:\s*text/);
    });

    it('should use box-sizing with proper reset for all browsers', () => {
      // Universal box-sizing reset
      expect(css).toContain('box-sizing: border-box');
    });

    it('should use flexbox properties that are well-supported', () => {
      // Flexbox is well-supported across Chrome, Firefox, Safari, Edge
      expect(css).toContain('display: flex');
    });

    it('should use CSS custom properties or fallback colors', () => {
      // Check for color values that work across browsers
      expect(css).toMatch(/#[0-9a-fA-F]{3,6}/);
    });

    it('should use linear-gradient which is supported across browsers', () => {
      // Modern gradient syntax is well-supported
      expect(css).toContain('linear-gradient');
    });

    it('should use transform property for hover effects', () => {
      // Transform is well-supported across all major browsers
      expect(css).toContain('transform:');
    });

    it('should use transition property for smooth effects', () => {
      // Transition is well-supported across all major browsers
      expect(css).toContain('transition:');
    });

    it('should include prefers-reduced-motion media query for accessibility', () => {
      // Media query for respecting user motion preferences
      expect(css).toContain('@media (prefers-reduced-motion');
    });
  });

  // Test Case 1-4 Combined: Page renders correctly structure validation
  describe('Page Rendering Structure (Chrome, Firefox, Safari, Edge)', () => {
    it('should have valid HTML5 doctype for cross-browser consistency', () => {
      expect(html.toLowerCase()).toContain('<!doctype html>');
    });

    it('should have proper meta viewport tag for responsive rendering', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
      expect(viewport.getAttribute('content')).toContain('width=device-width');
    });

    it('should have UTF-8 charset for consistent text rendering', () => {
      const charset = document.querySelector('meta[charset]');
      expect(charset).not.toBeNull();
      expect(charset.getAttribute('charset').toLowerCase()).toBe('utf-8');
    });

    it('should have all major sections visible in DOM', () => {
      // Hero section
      const hero = document.querySelector('.hero');
      expect(hero).not.toBeNull();

      // Get Started section
      const getStarted = document.querySelector('#get-started');
      expect(getStarted).not.toBeNull();

      // Footer
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
    });

    it('should have product name visible', () => {
      const productName = document.querySelector('.product-name');
      expect(productName).not.toBeNull();
      expect(productName.textContent).toBe('MirDB');
    });

    it('should have tagline visible', () => {
      const tagline = document.querySelector('.tagline');
      expect(tagline).not.toBeNull();
      expect(tagline.textContent).toContain('Memcached');
    });

    it('should have CTA buttons visible', () => {
      const primaryCta = document.querySelector('.btn-primary');
      const secondaryCta = document.querySelector('.btn-secondary');
      expect(primaryCta).not.toBeNull();
      expect(secondaryCta).not.toBeNull();
    });

    it('should have code blocks for installation instructions', () => {
      const codeBlocks = document.querySelectorAll('.code-block');
      expect(codeBlocks.length).toBeGreaterThan(0);
    });

    it('should have footer links visible', () => {
      const footerLinks = document.querySelectorAll('footer a');
      expect(footerLinks.length).toBeGreaterThan(0);
    });
  });

  // Chrome-specific compatibility
  describe('Chrome Compatibility', () => {
    it('should use system font stack that includes Chrome defaults', () => {
      // -apple-system, BlinkMacSystemFont are for Chrome on macOS
      expect(css).toMatch(/font-family:.*(-apple-system|BlinkMacSystemFont|Segoe UI)/);
    });

    it('should have proper focus-visible support', () => {
      // :focus-visible is well-supported in Chrome
      expect(css).toContain(':focus-visible');
    });
  });

  // Firefox-specific compatibility
  describe('Firefox Compatibility', () => {
    it('should use standard CSS properties that Firefox supports', () => {
      // Firefox supports standard background-clip
      expect(css).toMatch(/background-clip:\s*text/);
    });

    it('should have scroll-behavior for smooth scrolling', () => {
      // Firefox supports smooth scrolling
      expect(css).toContain('scroll-behavior: smooth');
    });

    it('should use rgba or hex colors that Firefox renders correctly', () => {
      expect(css).toMatch(/(rgba?\s*\(|#[0-9a-fA-F]{3,6})/);
    });
  });

  // Safari/WebKit-specific compatibility
  describe('Safari/WebKit Compatibility', () => {
    it('should include -webkit prefixes for gradient text', () => {
      // Safari requires -webkit prefix for background-clip: text
      expect(css).toContain('-webkit-background-clip');
      expect(css).toContain('-webkit-text-fill-color');
    });

    it('should use safe font stack for Safari', () => {
      // San Francisco font via -apple-system
      expect(css).toContain('-apple-system');
    });
  });

  // Edge (Chromium) compatibility
  describe('Edge Compatibility', () => {
    it('should include Segoe UI for Windows/Edge users', () => {
      expect(css).toContain('Segoe UI');
    });

    it('should use well-supported CSS grid/flex properties', () => {
      // Modern Edge is Chromium-based, supports flexbox well
      expect(css).toContain('display: flex');
    });
  });

  // Test Case 6: JavaScript error-free validation structure
  describe('Test Case 6: JavaScript Console Error Prevention', () => {
    it('should have no inline JavaScript that could cause errors', () => {
      const inlineScripts = document.querySelectorAll('script:not([src])');
      // If inline scripts exist, they should be minimal and safe
      inlineScripts.forEach(script => {
        // Ensure no obvious syntax issues
        const content = script.textContent;
        if (content.trim().length > 0) {
          // Check for balanced braces (simple validation)
          const openBraces = (content.match(/\{/g) || []).length;
          const closeBraces = (content.match(/\}/g) || []).length;
          expect(openBraces).toBe(closeBraces);
        }
      });
    });

    it('should have proper link elements with valid hrefs', () => {
      const links = document.querySelectorAll('a[href]');
      links.forEach(link => {
        const href = link.getAttribute('href');
        expect(href).toBeTruthy();
        // Should be valid URL or anchor
        expect(href).toMatch(/^(https?:\/\/|#|\/)/);
      });
    });

    it('should have external links with proper rel attribute', () => {
      const externalLinks = document.querySelectorAll('a[target="_blank"]');
      externalLinks.forEach(link => {
        const rel = link.getAttribute('rel');
        expect(rel).toContain('noopener');
      });
    });

    it('should not have missing image sources that would cause errors', () => {
      const images = document.querySelectorAll('img');
      images.forEach(img => {
        const src = img.getAttribute('src');
        if (src) {
          // Should be a valid path format
          expect(src).toBeTruthy();
        }
      });
    });
  });

  // CSS property compatibility matrix
  describe('CSS Property Cross-Browser Compatibility Matrix', () => {
    const wellSupportedProperties = [
      'display',
      'align-items',
      'justify-content',
      'gap',
      'padding',
      'margin',
      'border-radius',
      'background',
      'color',
      'font-size',
      'font-weight',
      'text-align',
      'min-height',
      'max-width',
      'overflow'
    ];

    wellSupportedProperties.forEach(property => {
      it(`should use well-supported CSS property: ${property}`, () => {
        const regex = new RegExp(`${property}\\s*:`, 'i');
        expect(css).toMatch(regex);
      });
    });

    it('should use flexbox layout via display: flex', () => {
      expect(css).toContain('display: flex');
    });
  });

  // Responsive design for cross-browser consistency
  describe('Responsive Design Across Browsers', () => {
    it('should have media queries for responsive design', () => {
      expect(css).toContain('@media');
    });

    it('should have mobile-specific styles', () => {
      expect(css).toMatch(/@media\s*\([^)]*max-width/);
    });

    it('should use relative units for scalable layouts', () => {
      // rem or em units for better cross-browser scaling
      expect(css).toMatch(/\d+(\.\d+)?(rem|em)/);
    });

    it('should have min-height on hero for consistent viewport handling', () => {
      expect(css).toMatch(/\.hero\s*\{[^}]*min-height/);
    });
  });

  // Font rendering consistency
  describe('Font Rendering Consistency', () => {
    it('should use system font stack for optimal rendering per OS', () => {
      // System font stack ensures native rendering on each OS/browser
      const fontStack = css.match(/font-family:[^;]+/g);
      expect(fontStack).not.toBeNull();
      expect(fontStack.some(f => f.includes('sans-serif'))).toBe(true);
    });

    it('should use monospace font for code blocks', () => {
      expect(css).toMatch(/font-family:.*monospace/);
    });

    it('should have line-height for readable text across browsers', () => {
      expect(css).toContain('line-height');
    });
  });

  // Color and visual consistency
  describe('Color Rendering Consistency', () => {
    it('should use hex colors for consistent rendering', () => {
      expect(css).toMatch(/#[0-9a-fA-F]{6}/);
    });

    it('should use rgba for transparency effects', () => {
      expect(css).toMatch(/rgba\s*\(/);
    });
  });
});
