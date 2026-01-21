import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

/**
 * Product Visual Showcase Tests
 * Testing REQ-6: Visual product showcase (screenshots, demo video, or product images)
 */

describe('Product Visual Showcase Section', () => {
  let dom;
  let document;

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
  });

  afterEach(() => {
    if (dom) {
      dom.window.close();
    }
  });

  /**
   * Test Case 1: Check product showcase section exists
   * Input: Check product showcase section exists
   * Expected: Product showcase section with images or video is present
   */
  describe('Test Case 1: Product Showcase Section Exists', () => {
    it('should have a product showcase section element', () => {
      const showcaseSection = document.querySelector('.product-showcase');
      expect(showcaseSection).not.toBeNull();
      expect(showcaseSection).toBeInTheDocument();
    });

    it('should have product showcase section with correct ID for navigation', () => {
      const showcaseSection = document.querySelector('#product-showcase');
      expect(showcaseSection).not.toBeNull();
    });

    it('should have product showcase section as a semantic section element', () => {
      const showcaseSection = document.querySelector('section.product-showcase');
      expect(showcaseSection).not.toBeNull();
      expect(showcaseSection.tagName.toLowerCase()).toBe('section');
    });

    it('should have product showcase section positioned after features section', () => {
      const main = document.querySelector('main');
      const sections = main.querySelectorAll('section');
      const sectionsArray = Array.from(sections);

      const featuresIndex = sectionsArray.findIndex(s => s.classList.contains('features'));
      const showcaseIndex = sectionsArray.findIndex(s => s.classList.contains('product-showcase'));

      expect(featuresIndex).toBeGreaterThan(-1);
      expect(showcaseIndex).toBeGreaterThan(-1);
      expect(showcaseIndex).toBeGreaterThan(featuresIndex);
    });

    it('should have a heading for the product showcase section', () => {
      const showcaseSection = document.querySelector('.product-showcase');
      const heading = showcaseSection.querySelector('h2');

      expect(heading).not.toBeNull();
      expect(heading.textContent.trim().length).toBeGreaterThan(0);
    });
  });

  /**
   * Test Case 2: Verify product images
   * Input: Verify product images
   * Expected: At least one product screenshot or visual is displayed
   */
  describe('Test Case 2: Product Images', () => {
    it('should have at least one product image or screenshot', () => {
      const showcaseSection = document.querySelector('.product-showcase');
      const images = showcaseSection.querySelectorAll('img');
      expect(images.length).toBeGreaterThanOrEqual(1);
    });

    it('should have product images within a showcase gallery or container', () => {
      const showcaseGallery = document.querySelector('.showcase-gallery, .showcase-images, .showcase-content');
      expect(showcaseGallery).not.toBeNull();
    });

    it('should have product images with src attribute', () => {
      const showcaseSection = document.querySelector('.product-showcase');
      const images = showcaseSection.querySelectorAll('img');

      expect(images.length).toBeGreaterThanOrEqual(1);
      images.forEach(image => {
        const src = image.getAttribute('src');
        expect(src).not.toBeNull();
        expect(src.trim().length).toBeGreaterThan(0);
      });
    });

    it('should have showcase images as img elements', () => {
      const showcaseSection = document.querySelector('.product-showcase');
      const images = showcaseSection.querySelectorAll('img');

      expect(images.length).toBeGreaterThanOrEqual(1);
      images.forEach(image => {
        expect(image.tagName.toLowerCase()).toBe('img');
      });
    });
  });

  /**
   * Test Case 3: Check image alt text
   * Input: Check image alt text
   * Expected: Product images have descriptive alt attributes
   */
  describe('Test Case 3: Image Alt Text', () => {
    it('should have all showcase images with alt attributes', () => {
      const showcaseSection = document.querySelector('.product-showcase');
      const images = showcaseSection.querySelectorAll('img');

      expect(images.length).toBeGreaterThanOrEqual(1);
      images.forEach(image => {
        const altText = image.getAttribute('alt');
        expect(altText).not.toBeNull();
      });
    });

    it('should have descriptive alt text (not empty)', () => {
      const showcaseSection = document.querySelector('.product-showcase');
      const images = showcaseSection.querySelectorAll('img');

      images.forEach(image => {
        const altText = image.getAttribute('alt');
        expect(altText.trim().length).toBeGreaterThan(0);
      });
    });

    it('should have meaningful alt text (not generic)', () => {
      const showcaseSection = document.querySelector('.product-showcase');
      const images = showcaseSection.querySelectorAll('img');

      images.forEach(image => {
        const altText = image.getAttribute('alt').toLowerCase();
        // Alt text should not be generic
        expect(altText).not.toBe('image');
        expect(altText).not.toBe('photo');
        expect(altText).not.toBe('screenshot');
        expect(altText).not.toBe('picture');
      });
    });

    it('should have alt text that describes the product visual', () => {
      const showcaseSection = document.querySelector('.product-showcase');
      const images = showcaseSection.querySelectorAll('img');

      images.forEach(image => {
        const altText = image.getAttribute('alt');
        // Alt text should be reasonably descriptive (at least 10 characters)
        expect(altText.length).toBeGreaterThanOrEqual(10);
      });
    });
  });

  /**
   * Image optimization tests for web performance
   */
  describe('Image Optimization', () => {
    it('should have images with loading attribute for lazy loading optimization', () => {
      const showcaseSection = document.querySelector('.product-showcase');
      const images = showcaseSection.querySelectorAll('img');

      // At least check that images exist
      expect(images.length).toBeGreaterThanOrEqual(1);

      // Images can have loading="lazy" or loading="eager" (or no attribute for eager)
      // For below-the-fold images, lazy loading is preferred
      images.forEach(image => {
        const loading = image.getAttribute('loading');
        // Loading attribute is optional but recommended
        if (loading) {
          expect(['lazy', 'eager']).toContain(loading);
        }
      });
    });

    it('should have images with width and height attributes to prevent layout shift', () => {
      const showcaseSection = document.querySelector('.product-showcase');
      const images = showcaseSection.querySelectorAll('img');

      images.forEach(image => {
        const width = image.getAttribute('width');
        const height = image.getAttribute('height');

        // Either have explicit width/height or CSS should handle aspect ratio
        // We check for at least one of these approaches
        const hasExplicitDimensions = width !== null && height !== null;
        const hasClass = image.classList.length > 0;

        expect(hasExplicitDimensions || hasClass).toBe(true);
      });
    });
  });

  /**
   * CSS styling tests for product showcase section
   */
  describe('Product Showcase Section Styling', () => {
    it('should have CSS styles defined for product showcase section', () => {
      const cssPath = path.resolve(__dirname, '../styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf-8');

      expect(cssContent).toContain('.product-showcase');
    });

    it('should have CSS styles for showcase gallery', () => {
      const cssPath = path.resolve(__dirname, '../styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf-8');

      // Should have styles for the image container
      expect(cssContent).toMatch(/\.showcase-(gallery|images|content)/);
    });

    it('should have images styled with appropriate sizing', () => {
      const cssPath = path.resolve(__dirname, '../styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf-8');

      // Should have styles that control image dimensions
      expect(cssContent).toMatch(/\.showcase-image|\.product-showcase\s+img/);
    });
  });

  /**
   * Accessibility tests for product showcase section
   */
  describe('Product Showcase Accessibility', () => {
    it('should have heading hierarchy maintained', () => {
      const showcaseSection = document.querySelector('.product-showcase');
      const heading = showcaseSection.querySelector('h2');

      expect(heading).not.toBeNull();
      // Should be h2 to maintain hierarchy (h1 is in hero)
      expect(heading.tagName.toLowerCase()).toBe('h2');
    });

    it('should have descriptive content around images', () => {
      const showcaseSection = document.querySelector('.product-showcase');

      // Should have either a heading, description, or figure with figcaption
      const hasHeading = showcaseSection.querySelector('h2') !== null;
      const hasDescription = showcaseSection.querySelector('p') !== null;
      const hasFigcaption = showcaseSection.querySelector('figcaption') !== null;

      expect(hasHeading || hasDescription || hasFigcaption).toBe(true);
    });
  });
});
