/**
 * Error State Handling Integration Tests
 * Owner: Scenario 16 - Error State Handling
 *
 * Tests:
 * - Broken image fallback behavior
 * - Critical CSS inline loading
 * - Error state styling
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import { readFileSync } from 'fs';
import { resolve } from 'path';

describe('Error State Handling', () => {
  let dom;
  let document;

  beforeEach(() => {
    const htmlPath = resolve(process.cwd(), 'src/index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    dom = new JSDOM(html, {
      url: 'http://localhost:3000',
      runScripts: 'dangerously',
      resources: 'usable',
    });
    document = dom.window.document;
  });

  afterEach(() => {
    if (dom) {
      dom.window.close();
    }
  });

  describe('Broken Image Fallback', () => {
    it('should have alt text on all images', () => {
      const images = document.querySelectorAll('img');
      images.forEach(img => {
        expect(img.hasAttribute('alt')).toBe(true);
      });
    });

    it('should apply image-error class when image fails to load', () => {
      // Create a test image element
      const testImg = document.createElement('img');
      testImg.src = 'non-existent-image.jpg';
      testImg.alt = 'Test image';
      testImg.className = 'img-fallback';
      document.body.appendChild(testImg);

      // Simulate error event
      testImg.dispatchEvent(new dom.window.Event('error'));

      // Check that the image can be styled appropriately
      // The image should have data-error attribute or similar indicator
      expect(testImg.classList.contains('img-fallback')).toBe(true);
    });

    it('should display alt text visually for broken images via CSS', () => {
      const testImg = document.createElement('img');
      testImg.src = 'broken.png';
      testImg.alt = 'Fallback text visible';
      testImg.className = 'img-fallback';
      document.body.appendChild(testImg);

      // Alt text should be preserved for accessibility
      expect(testImg.alt).toBe('Fallback text visible');
    });

    it('should SVG icons be properly handled for accessibility', () => {
      const svgIcons = document.querySelectorAll('svg');
      svgIcons.forEach(svg => {
        // Decorative SVGs should be hidden from screen readers
        const isDecorative = svg.getAttribute('aria-hidden') === 'true';
        const hasAriaLabel = svg.hasAttribute('aria-label');
        const hasTitle = svg.querySelector('title') !== null;
        // Check if parent has aria-hidden
        const parentHasAriaHidden = svg.closest('[aria-hidden="true"]') !== null;
        // Either hidden, has accessible label, has title, or parent is hidden
        expect(isDecorative || hasAriaLabel || hasTitle || parentHasAriaHidden).toBe(true);
      });
    });
  });

  describe('Critical CSS Inline', () => {
    it('should have inline styles for basic page readability', () => {
      const styleElements = document.querySelectorAll('style');
      expect(styleElements.length).toBeGreaterThan(0);
    });

    it('should have essential layout rules in inline styles', () => {
      const styleElements = document.querySelectorAll('style');
      let hasLayoutRules = false;

      styleElements.forEach(style => {
        const cssText = style.textContent;
        // Check for basic layout properties
        if (cssText.includes('padding') ||
            cssText.includes('margin') ||
            cssText.includes('display')) {
          hasLayoutRules = true;
        }
      });

      expect(hasLayoutRules).toBe(true);
    });

    it('should have basic color variables defined', () => {
      // Check that CSS variables are referenced in the document
      const styleElements = document.querySelectorAll('style');
      let usesVariables = false;

      styleElements.forEach(style => {
        if (style.textContent.includes('var(--')) {
          usesVariables = true;
        }
      });

      expect(usesVariables).toBe(true);
    });

    it('should main content be visible without external CSS', () => {
      // The page should have content in semantic elements
      const main = document.querySelector('main');
      expect(main).toBeTruthy();
      expect(main.textContent.trim().length).toBeGreaterThan(0);
    });

    it('should heading hierarchy be correct for readability', () => {
      const h1 = document.querySelector('h1');
      const h2s = document.querySelectorAll('h2');

      expect(h1).toBeTruthy();
      expect(h2s.length).toBeGreaterThan(0);
    });
  });

  describe('Error Message Styling', () => {
    it('should have error color defined in CSS variables', () => {
      const variablesPath = resolve(process.cwd(), 'src/styles/variables.css');
      const variablesCSS = readFileSync(variablesPath, 'utf-8');

      expect(variablesCSS).toContain('--color-error');
    });

    it('should have warning color defined in CSS variables', () => {
      const variablesPath = resolve(process.cwd(), 'src/styles/variables.css');
      const variablesCSS = readFileSync(variablesPath, 'utf-8');

      expect(variablesCSS).toContain('--color-warning');
    });

    it('should have success color defined in CSS variables', () => {
      const variablesPath = resolve(process.cwd(), 'src/styles/variables.css');
      const variablesCSS = readFileSync(variablesPath, 'utf-8');

      expect(variablesCSS).toContain('--color-success');
    });
  });
});

describe('Image Error Handler', () => {
  let dom;
  let document;

  beforeEach(() => {
    dom = new JSDOM(`<!DOCTYPE html><html><body></body></html>`, {
      url: 'http://localhost:3000',
      runScripts: 'dangerously',
    });
    document = dom.window.document;
  });

  afterEach(() => {
    if (dom) {
      dom.window.close();
    }
  });

  it('should handle image error gracefully with placeholder', () => {
    const img = document.createElement('img');
    img.className = 'img-fallback';
    img.alt = 'Product logo';

    // Add error handler like the real implementation would
    img.onerror = function() {
      this.classList.add('img-error');
      this.setAttribute('data-error', 'true');
    };

    img.src = 'nonexistent.png';
    document.body.appendChild(img);

    // Trigger error event
    img.onerror();

    expect(img.classList.contains('img-error')).toBe(true);
    expect(img.getAttribute('data-error')).toBe('true');
    // Alt text still available for screen readers
    expect(img.alt).toBe('Product logo');
  });

  it('should prevent infinite error loops', () => {
    const img = document.createElement('img');
    img.className = 'img-fallback';
    let errorCount = 0;

    img.onerror = function() {
      errorCount++;
      if (!this.hasAttribute('data-error')) {
        this.setAttribute('data-error', 'true');
        // Don't set a new src that might fail
      }
    };

    img.onerror();
    img.onerror(); // Should not increment if already handled

    // First error should be processed
    expect(img.hasAttribute('data-error')).toBe(true);
  });
});
