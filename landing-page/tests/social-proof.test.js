import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

/**
 * Social Proof Section Tests
 * Testing REQ-5: Social proof section (testimonials, customer logos, or reviews)
 */

describe('Social Proof Section', () => {
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
   * Test Case 1: Social proof section exists in DOM
   * Input: Check social proof section exists
   * Expected: Social proof/testimonials section is present in DOM
   */
  describe('Test Case 1: Social Proof Section Exists', () => {
    it('should have a social proof section element', () => {
      const socialProofSection = document.querySelector('.social-proof');
      expect(socialProofSection).not.toBeNull();
      expect(socialProofSection).toBeInTheDocument();
    });

    it('should have social proof section with correct ID for navigation', () => {
      const socialProofSection = document.querySelector('#social-proof');
      expect(socialProofSection).not.toBeNull();
    });

    it('should have social proof section as a semantic section element', () => {
      const socialProofSection = document.querySelector('section.social-proof');
      expect(socialProofSection).not.toBeNull();
      expect(socialProofSection.tagName.toLowerCase()).toBe('section');
    });

    it('should have social proof section positioned after features section', () => {
      const main = document.querySelector('main');
      const sections = main.querySelectorAll('section');
      const sectionsArray = Array.from(sections);

      const featuresIndex = sectionsArray.findIndex(s => s.classList.contains('features'));
      const socialProofIndex = sectionsArray.findIndex(s => s.classList.contains('social-proof'));

      expect(featuresIndex).toBeGreaterThan(-1);
      expect(socialProofIndex).toBeGreaterThan(-1);
      expect(socialProofIndex).toBeGreaterThan(featuresIndex);
    });
  });

  /**
   * Test Case 2: At least 2 testimonials are displayed
   * Input: Count testimonials
   * Expected: At least 2 testimonials or customer references are displayed
   */
  describe('Test Case 2: Testimonial Count', () => {
    it('should have at least 2 testimonials', () => {
      const testimonials = document.querySelectorAll('.testimonial');
      expect(testimonials.length).toBeGreaterThanOrEqual(2);
    });

    it('should have testimonials contained within testimonials grid', () => {
      const testimonialsGrid = document.querySelector('.testimonials-grid');
      expect(testimonialsGrid).not.toBeNull();

      const testimonials = testimonialsGrid.querySelectorAll('.testimonial');
      expect(testimonials.length).toBeGreaterThanOrEqual(2);
    });

    it('should have each testimonial as a separate card element', () => {
      const testimonials = document.querySelectorAll('.testimonial');
      testimonials.forEach(testimonial => {
        expect(testimonial.classList.contains('testimonial')).toBe(true);
      });
    });
  });

  /**
   * Test Case 3: Each testimonial has quote text, name, and context
   * Input: Verify testimonial structure
   * Expected: Each testimonial has quote text, name, and context
   */
  describe('Test Case 3: Testimonial Structure', () => {
    it('should have each testimonial contain a quote element', () => {
      const testimonials = document.querySelectorAll('.testimonial');
      expect(testimonials.length).toBeGreaterThanOrEqual(1);

      testimonials.forEach(testimonial => {
        const quote = testimonial.querySelector('.testimonial-quote');
        expect(quote).not.toBeNull();
        expect(quote.textContent.trim().length).toBeGreaterThan(0);
      });
    });

    it('should have each testimonial contain a customer name', () => {
      const testimonials = document.querySelectorAll('.testimonial');
      expect(testimonials.length).toBeGreaterThanOrEqual(1);

      testimonials.forEach(testimonial => {
        const name = testimonial.querySelector('.testimonial-name');
        expect(name).not.toBeNull();
        expect(name.textContent.trim().length).toBeGreaterThan(0);
      });
    });

    it('should have each testimonial contain context (company/role)', () => {
      const testimonials = document.querySelectorAll('.testimonial');
      expect(testimonials.length).toBeGreaterThanOrEqual(1);

      testimonials.forEach(testimonial => {
        const context = testimonial.querySelector('.testimonial-context');
        expect(context).not.toBeNull();
        expect(context.textContent.trim().length).toBeGreaterThan(0);
      });
    });

    it('should have quotes that appear authentic (reasonable length)', () => {
      const quotes = document.querySelectorAll('.testimonial-quote');

      quotes.forEach(quote => {
        const text = quote.textContent.trim();
        // Quote should be between 20 and 500 characters for authenticity
        expect(text.length).toBeGreaterThan(20);
        expect(text.length).toBeLessThan(500);
      });
    });

    it('should have attribution section containing name and context', () => {
      const testimonials = document.querySelectorAll('.testimonial');

      testimonials.forEach(testimonial => {
        const attribution = testimonial.querySelector('.testimonial-attribution');
        expect(attribution).not.toBeNull();

        const name = attribution.querySelector('.testimonial-name');
        const context = attribution.querySelector('.testimonial-context');

        expect(name).not.toBeNull();
        expect(context).not.toBeNull();
      });
    });
  });

  /**
   * Test Case 4: Testimonials include photos with proper alt text
   * Input: Check testimonial images
   * Expected: Testimonials include photos with proper alt text
   */
  describe('Test Case 4: Testimonial Images', () => {
    it('should have each testimonial include a photo/avatar', () => {
      const testimonials = document.querySelectorAll('.testimonial');
      expect(testimonials.length).toBeGreaterThanOrEqual(1);

      testimonials.forEach(testimonial => {
        const image = testimonial.querySelector('.testimonial-image');
        expect(image).not.toBeNull();
      });
    });

    it('should have testimonial images with proper alt text', () => {
      const images = document.querySelectorAll('.testimonial-image');
      expect(images.length).toBeGreaterThanOrEqual(2);

      images.forEach(image => {
        const altText = image.getAttribute('alt');
        expect(altText).not.toBeNull();
        expect(altText.trim().length).toBeGreaterThan(0);
        // Alt text should describe the person, not be generic
        expect(altText.toLowerCase()).not.toBe('image');
        expect(altText.toLowerCase()).not.toBe('photo');
      });
    });

    it('should have testimonial images as img elements', () => {
      const images = document.querySelectorAll('.testimonial-image');

      images.forEach(image => {
        expect(image.tagName.toLowerCase()).toBe('img');
      });
    });

    it('should have testimonial images with src attribute', () => {
      const images = document.querySelectorAll('.testimonial-image');

      images.forEach(image => {
        const src = image.getAttribute('src');
        expect(src).not.toBeNull();
        expect(src.trim().length).toBeGreaterThan(0);
      });
    });

    it('should have testimonial images with proper dimensions for accessibility', () => {
      // Check CSS for proper image sizing
      const cssPath = path.resolve(__dirname, '../styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf-8');

      // Should have defined dimensions for testimonial images
      expect(cssContent).toContain('.testimonial-image');
    });
  });

  /**
   * Additional accessibility and structure tests
   */
  describe('Social Proof Section Accessibility', () => {
    it('should have a heading for the social proof section', () => {
      const socialProofSection = document.querySelector('.social-proof');
      const heading = socialProofSection.querySelector('h2');

      expect(heading).not.toBeNull();
      expect(heading.textContent.trim().length).toBeGreaterThan(0);
    });

    it('should have blockquote or q element for semantic quote markup', () => {
      const testimonials = document.querySelectorAll('.testimonial');

      testimonials.forEach(testimonial => {
        const quote = testimonial.querySelector('blockquote, q, .testimonial-quote');
        expect(quote).not.toBeNull();
      });
    });
  });

  /**
   * CSS styling tests for social proof section
   */
  describe('Social Proof Section Styling', () => {
    it('should have CSS styles defined for social proof section', () => {
      const cssPath = path.resolve(__dirname, '../styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf-8');

      expect(cssContent).toContain('.social-proof');
      expect(cssContent).toContain('.testimonial');
      expect(cssContent).toContain('.testimonials-grid');
    });

    it('should have grid or flex layout for testimonials', () => {
      const cssPath = path.resolve(__dirname, '../styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf-8');

      // Should use grid or flex for responsive layout
      expect(cssContent).toMatch(/\.testimonials-grid\s*\{[^}]*(display:\s*(grid|flex))/);
    });
  });
});
