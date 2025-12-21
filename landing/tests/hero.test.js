import { describe, it, expect, beforeEach } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

describe('Hero Section Display', () => {
  let document;
  let heroSection;

  beforeEach(() => {
    const htmlPath = path.resolve(__dirname, '../index.html');
    const html = fs.readFileSync(htmlPath, 'utf8');
    const dom = new JSDOM(html);
    document = dom.window.document;
    heroSection = document.querySelector('.hero');
  });

  // Test Case 1: Product name visibility
  describe('Test Case 1: Product Name Display', () => {
    it('should display MirDB product name in the hero section', () => {
      const productName = heroSection.querySelector('.product-name');
      expect(productName).not.toBeNull();
      expect(productName.textContent).toBe('MirDB');
    });

    it('should have product name visible and prominent as h1', () => {
      const h1 = heroSection.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.textContent).toContain('MirDB');
    });
  });

  // Test Case 2: Tagline display
  describe('Test Case 2: Tagline Display', () => {
    it('should display the tagline in the hero section', () => {
      const tagline = heroSection.querySelector('.tagline');
      expect(tagline).not.toBeNull();
      expect(tagline.textContent).toContain('Persistent Key-Value Storage');
      expect(tagline.textContent).toContain('Memcached Compatibility');
    });

    it('should have a complete tagline message', () => {
      const tagline = heroSection.querySelector('.tagline');
      expect(tagline.textContent).toBe('Persistent Key-Value Storage with Memcached Compatibility');
    });
  });

  // Test Case 3: Get Started button
  describe('Test Case 3: Get Started Primary CTA', () => {
    it('should have a Get Started button visible', () => {
      const getStartedBtn = heroSection.querySelector('a.btn-primary');
      expect(getStartedBtn).not.toBeNull();
      expect(getStartedBtn.textContent).toBe('Get Started');
    });

    it('should have proper styling class for primary CTA', () => {
      const getStartedBtn = heroSection.querySelector('a.btn-primary');
      expect(getStartedBtn.classList.contains('btn')).toBe(true);
      expect(getStartedBtn.classList.contains('btn-primary')).toBe(true);
    });

    it('should link to the get-started section', () => {
      const getStartedBtn = heroSection.querySelector('a.btn-primary');
      expect(getStartedBtn.getAttribute('href')).toBe('#get-started');
    });
  });

  // Test Case 4: View on GitHub secondary CTA
  describe('Test Case 4: View on GitHub Secondary CTA', () => {
    it('should have a View on GitHub button visible', () => {
      const githubBtn = heroSection.querySelector('a.btn-secondary');
      expect(githubBtn).not.toBeNull();
      expect(githubBtn.textContent).toBe('View on GitHub');
    });

    it('should link to the GitHub repository', () => {
      const githubBtn = heroSection.querySelector('a.btn-secondary');
      expect(githubBtn.getAttribute('href')).toContain('github.com');
      expect(githubBtn.getAttribute('href')).toContain('mirdb');
    });

    it('should open GitHub link in a new tab', () => {
      const githubBtn = heroSection.querySelector('a.btn-secondary');
      expect(githubBtn.getAttribute('target')).toBe('_blank');
      expect(githubBtn.getAttribute('rel')).toContain('noopener');
    });
  });

  // Test Case 5: Semantic HTML verification
  describe('Test Case 5: Semantic HTML Structure', () => {
    it('should use semantic header element for hero section', () => {
      const header = document.querySelector('header.hero');
      expect(header).not.toBeNull();
    });

    it('should have proper heading hierarchy with h1', () => {
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.textContent).toContain('MirDB');
    });

    it('should have role="banner" on the hero header', () => {
      const header = document.querySelector('header.hero');
      expect(header.getAttribute('role')).toBe('banner');
    });

    it('should have only one h1 on the page', () => {
      const h1Elements = document.querySelectorAll('h1');
      expect(h1Elements.length).toBe(1);
    });

    it('should have proper document structure with main element', () => {
      const main = document.querySelector('main');
      expect(main).not.toBeNull();
    });
  });

  // Additional tests for value proposition
  describe('Value Proposition', () => {
    it('should display a value proposition text', () => {
      const valueProp = heroSection.querySelector('.value-proposition');
      expect(valueProp).not.toBeNull();
      expect(valueProp.textContent.length).toBeGreaterThan(0);
    });

    it('should mention key benefits in value proposition', () => {
      const valueProp = heroSection.querySelector('.value-proposition');
      expect(valueProp.textContent.toLowerCase()).toContain('persistent');
    });
  });

  // CTA buttons container
  describe('CTA Buttons Container', () => {
    it('should have both CTA buttons in a container', () => {
      const ctaContainer = heroSection.querySelector('.cta-buttons');
      expect(ctaContainer).not.toBeNull();

      const buttons = ctaContainer.querySelectorAll('a.btn');
      expect(buttons.length).toBe(2);
    });
  });
});
