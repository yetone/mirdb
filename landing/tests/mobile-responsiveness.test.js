import { describe, it, expect, beforeEach } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

/**
 * Unit tests for Mobile Responsiveness (Scenario 7)
 * Tests verify mobile responsive features including viewport meta tag
 * Requirements: NFR-2, US-6
 */

describe('Mobile Responsiveness - Unit Tests', () => {
  let document;

  beforeEach(() => {
    const htmlPath = path.resolve(__dirname, '../index.html');
    const html = fs.readFileSync(htmlPath, 'utf8');
    const dom = new JSDOM(html);
    document = dom.window.document;
  });

  // Test Case 5: Meta viewport tag
  describe('Test Case 5: Meta Viewport Tag', () => {
    it('should have meta viewport tag present', () => {
      const viewportMeta = document.querySelector('meta[name="viewport"]');
      expect(viewportMeta).not.toBeNull();
    });

    it('should have width=device-width in viewport meta', () => {
      const viewportMeta = document.querySelector('meta[name="viewport"]');
      const content = viewportMeta.getAttribute('content');
      expect(content).toContain('width=device-width');
    });

    it('should have initial-scale=1 in viewport meta', () => {
      const viewportMeta = document.querySelector('meta[name="viewport"]');
      const content = viewportMeta.getAttribute('content');
      expect(content).toContain('initial-scale=1');
    });

    it('should have complete viewport meta tag configuration', () => {
      const viewportMeta = document.querySelector('meta[name="viewport"]');
      const content = viewportMeta.getAttribute('content');
      expect(content).toBe('width=device-width, initial-scale=1.0');
    });
  });

  // Additional structural tests for mobile responsiveness
  describe('HTML Structure for Responsiveness', () => {
    it('should have proper DOCTYPE declaration', () => {
      const doctype = document.doctype;
      expect(doctype).not.toBeNull();
      expect(doctype.name).toBe('html');
    });

    it('should have lang attribute on html element', () => {
      const html = document.querySelector('html');
      expect(html.getAttribute('lang')).toBe('en');
    });

    it('should have stylesheet linked', () => {
      const stylesheet = document.querySelector('link[rel="stylesheet"]');
      expect(stylesheet).not.toBeNull();
    });

    it('should have semantic section elements for proper structure', () => {
      const header = document.querySelector('header.hero');
      const main = document.querySelector('main');
      const footer = document.querySelector('footer');

      expect(header).not.toBeNull();
      expect(main).not.toBeNull();
      expect(footer).not.toBeNull();
    });
  });

  // CSS class presence checks for responsive design
  describe('Responsive CSS Classes', () => {
    it('should have CTA buttons container for flex layout', () => {
      const ctaButtons = document.querySelector('.cta-buttons');
      expect(ctaButtons).not.toBeNull();
    });

    it('should have button elements with btn class', () => {
      const buttons = document.querySelectorAll('.btn');
      expect(buttons.length).toBeGreaterThan(0);
    });

    it('should have hero content wrapper for centering', () => {
      const heroContent = document.querySelector('.hero-content');
      expect(heroContent).not.toBeNull();
    });
  });
});
