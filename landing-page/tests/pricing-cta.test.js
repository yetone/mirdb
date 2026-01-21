import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

/**
 * Pricing/CTA Section Tests
 * Testing REQ-7: Pricing section or call-to-action for pricing information
 */

describe('Pricing/CTA Section', () => {
  let dom;
  let document;

  beforeEach(() => {
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
   * Test Case 1: Pricing/CTA section exists
   * Input: Check pricing/CTA section exists
   * Expected: Pricing section or prominent CTA section is present
   */
  describe('Test Case 1: Pricing/CTA Section Exists', () => {
    it('should have a pricing or CTA section element', () => {
      const pricingSection = document.querySelector('.pricing-section, .cta-section, #pricing, #cta-section');
      expect(pricingSection).not.toBeNull();
      expect(pricingSection).toBeInTheDocument();
    });

    it('should have pricing/CTA section with correct ID for navigation', () => {
      const pricingSection = document.querySelector('#pricing, #cta-section');
      expect(pricingSection).not.toBeNull();
    });

    it('should have pricing/CTA section as a semantic section element', () => {
      const pricingSection = document.querySelector('section.pricing-section, section.cta-section');
      expect(pricingSection).not.toBeNull();
      expect(pricingSection.tagName.toLowerCase()).toBe('section');
    });

    it('should have pricing/CTA section positioned after social proof section', () => {
      const main = document.querySelector('main');
      const sections = main.querySelectorAll('section');
      const sectionsArray = Array.from(sections);

      const socialProofIndex = sectionsArray.findIndex(s => s.classList.contains('social-proof'));
      const pricingIndex = sectionsArray.findIndex(s =>
        s.classList.contains('pricing-section') || s.classList.contains('cta-section')
      );

      expect(socialProofIndex).toBeGreaterThan(-1);
      expect(pricingIndex).toBeGreaterThan(-1);
      expect(pricingIndex).toBeGreaterThan(socialProofIndex);
    });
  });

  /**
   * Test Case 2: Section provides clear action for visitor to take
   * Input: Verify conversion path
   * Expected: Section provides clear action for visitor to take
   */
  describe('Test Case 2: Conversion Path', () => {
    it('should have a heading in the pricing/CTA section', () => {
      const pricingSection = document.querySelector('.pricing-section, .cta-section');
      const heading = pricingSection.querySelector('h2');

      expect(heading).not.toBeNull();
      expect(heading.textContent.trim().length).toBeGreaterThan(0);
    });

    it('should have at least one CTA button in the section', () => {
      const pricingSection = document.querySelector('.pricing-section, .cta-section');
      const ctaButton = pricingSection.querySelector('a, button');

      expect(ctaButton).not.toBeNull();
    });

    it('should have a primary CTA button with clear action text', () => {
      const pricingSection = document.querySelector('.pricing-section, .cta-section');
      const ctaButton = pricingSection.querySelector('.cta-primary, .cta-button, a[class*="cta"], button[class*="cta"]');

      expect(ctaButton).not.toBeNull();
      const buttonText = ctaButton.textContent.trim();
      expect(buttonText.length).toBeGreaterThan(0);
    });

    it('should have CTA button with proper sizing for accessibility (min 44px touch target)', () => {
      const cssPath = path.resolve(__dirname, '../styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf-8');

      // Check that CTA buttons have minimum height defined
      expect(cssContent).toMatch(/min-height:\s*44px|min-height:\s*2\.75rem/);
    });

    it('should have descriptive text explaining what action to take', () => {
      const pricingSection = document.querySelector('.pricing-section, .cta-section');
      const description = pricingSection.querySelector('p');

      expect(description).not.toBeNull();
      expect(description.textContent.trim().length).toBeGreaterThan(20);
    });
  });

  /**
   * Additional structure and accessibility tests
   */
  describe('Pricing/CTA Section Structure', () => {
    it('should have proper visual hierarchy with heading followed by content', () => {
      const pricingSection = document.querySelector('.pricing-section, .cta-section');
      const heading = pricingSection.querySelector('h2');

      expect(heading).not.toBeNull();
      // Heading should be one of the first elements
      const firstElements = Array.from(pricingSection.children).slice(0, 3);
      const hasHeadingEarly = firstElements.some(el =>
        el.tagName.toLowerCase() === 'h2' || el.querySelector('h2')
      );
      expect(hasHeadingEarly).toBe(true);
    });

    it('should have CTA button with href or onclick handler', () => {
      const pricingSection = document.querySelector('.pricing-section, .cta-section');
      const ctaLink = pricingSection.querySelector('a[href], button[onclick], button');

      expect(ctaLink).not.toBeNull();
      if (ctaLink.tagName.toLowerCase() === 'a') {
        expect(ctaLink.getAttribute('href')).not.toBeNull();
      }
    });
  });

  /**
   * CSS styling tests for pricing/CTA section
   */
  describe('Pricing/CTA Section Styling', () => {
    it('should have CSS styles defined for pricing/CTA section', () => {
      const cssPath = path.resolve(__dirname, '../styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf-8');

      // Should have either pricing-section or cta-section styles
      const hasPricingStyles = cssContent.includes('.pricing-section') || cssContent.includes('.cta-section');
      expect(hasPricingStyles).toBe(true);
    });

    it('should have centered content layout', () => {
      const cssPath = path.resolve(__dirname, '../styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf-8');

      // Should have text-align center for the section
      expect(cssContent).toMatch(/\.(pricing-section|cta-section)[^{]*\{[^}]*(text-align:\s*center)/);
    });
  });
});
