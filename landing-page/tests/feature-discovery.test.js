import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

/**
 * Feature Discovery User Journey Tests
 * Testing User Story 2: Feature Discovery
 * Scenario: Verify potential customer can explore product features and benefits
 */

describe('Feature Discovery User Journey', () => {
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
   * Test Case 2: Count visible features
   * Input: Count visible features
   * Expected: At least 3 key features/benefits are displayed
   */
  describe('Test Case 2: At Least 3 Features Displayed', () => {
    it('should have a features section in the DOM', () => {
      const featuresSection = document.querySelector('#features') ||
                             document.querySelector('.features') ||
                             document.querySelector('section.features');
      expect(featuresSection).not.toBeNull();
    });

    it('should display at least 3 feature cards', () => {
      const featuresSection = document.querySelector('#features') ||
                             document.querySelector('.features');
      expect(featuresSection).not.toBeNull();

      const featureCards = featuresSection.querySelectorAll('.feature-card');
      expect(featureCards.length).toBeGreaterThanOrEqual(3);
    });

    it('should have features section positioned after hero section', () => {
      const main = document.querySelector('main');
      const sections = main.querySelectorAll('section');
      const sectionsArray = Array.from(sections);

      const heroIndex = sectionsArray.findIndex(s => s.classList.contains('hero'));
      const featuresIndex = sectionsArray.findIndex(s => s.classList.contains('features'));

      expect(heroIndex).toBeGreaterThan(-1);
      expect(featuresIndex).toBeGreaterThan(-1);
      expect(featuresIndex).toBeGreaterThan(heroIndex);
    });

    it('should have a heading for the features section', () => {
      const featuresSection = document.querySelector('#features') ||
                             document.querySelector('.features');
      const heading = featuresSection.querySelector('h2');

      expect(heading).not.toBeNull();
      expect(heading.textContent.trim().length).toBeGreaterThan(0);
    });
  });

  /**
   * Test Case 3: Verify feature card completeness
   * Input: Verify feature card completeness
   * Expected: Each feature has icon, title, and description
   */
  describe('Test Case 3: Feature Card Completeness', () => {
    it('should have each feature card with an icon element', () => {
      const featureCards = document.querySelectorAll('.feature-card');
      expect(featureCards.length).toBeGreaterThan(0);

      featureCards.forEach((card) => {
        const icon = card.querySelector('.feature-icon') ||
                    card.querySelector('svg') ||
                    card.querySelector('img.icon') ||
                    card.querySelector('i');
        expect(icon).not.toBeNull();
      });
    });

    it('should have each feature card with a title (h3)', () => {
      const featureCards = document.querySelectorAll('.feature-card');
      expect(featureCards.length).toBeGreaterThan(0);

      featureCards.forEach((card) => {
        const title = card.querySelector('h3') || card.querySelector('h4');
        expect(title).not.toBeNull();
        expect(title.textContent.trim().length).toBeGreaterThan(0);
      });
    });

    it('should have each feature card with a description (p)', () => {
      const featureCards = document.querySelectorAll('.feature-card');
      expect(featureCards.length).toBeGreaterThan(0);

      featureCards.forEach((card) => {
        const description = card.querySelector('p');
        expect(description).not.toBeNull();
        expect(description.textContent.trim().length).toBeGreaterThan(0);
      });
    });

    it('should have feature icons with visible content', () => {
      const featureIcons = document.querySelectorAll('.feature-card .feature-icon');
      expect(featureIcons.length).toBeGreaterThan(0);

      featureIcons.forEach((icon) => {
        // Icon should have either text content (emoji), child elements (SVG), or be an img
        const hasContent = icon.textContent.trim().length > 0 ||
                          icon.children.length > 0 ||
                          icon.tagName.toLowerCase() === 'img';
        expect(hasContent).toBe(true);
      });
    });

    it('should have consistent structure across all feature cards', () => {
      const featureCards = document.querySelectorAll('.feature-card');
      expect(featureCards.length).toBeGreaterThan(0);

      featureCards.forEach((card) => {
        // Each card must have all three components
        const hasIcon = card.querySelector('.feature-icon') !== null;
        const hasTitle = card.querySelector('h3') !== null;
        const hasDescription = card.querySelector('p') !== null;

        expect(hasIcon).toBe(true);
        expect(hasTitle).toBe(true);
        expect(hasDescription).toBe(true);
      });
    });
  });

  /**
   * Test Case 4: Locate product visuals
   * Input: Locate product visuals
   * Expected: Product screenshots or images are present on page
   */
  describe('Test Case 4: Product Visuals Present', () => {
    it('should have a product showcase section', () => {
      const showcaseSection = document.querySelector('#product-showcase') ||
                              document.querySelector('.product-showcase');
      expect(showcaseSection).not.toBeNull();
    });

    it('should have at least one product image or screenshot', () => {
      const showcaseSection = document.querySelector('.product-showcase');
      expect(showcaseSection).not.toBeNull();

      const images = showcaseSection.querySelectorAll('img');
      expect(images.length).toBeGreaterThanOrEqual(1);
    });

    it('should have product images with valid src attributes', () => {
      const showcaseSection = document.querySelector('.product-showcase');
      const images = showcaseSection.querySelectorAll('img');

      expect(images.length).toBeGreaterThanOrEqual(1);
      images.forEach((image) => {
        const src = image.getAttribute('src');
        expect(src).not.toBeNull();
        expect(src.trim().length).toBeGreaterThan(0);
      });
    });

    it('should have product images with descriptive alt text', () => {
      const showcaseSection = document.querySelector('.product-showcase');
      const images = showcaseSection.querySelectorAll('img');

      expect(images.length).toBeGreaterThanOrEqual(1);
      images.forEach((image) => {
        const alt = image.getAttribute('alt');
        expect(alt).not.toBeNull();
        expect(alt.trim().length).toBeGreaterThan(10);
      });
    });

    it('should have showcase section positioned after features section', () => {
      const main = document.querySelector('main');
      const sections = main.querySelectorAll('section');
      const sectionsArray = Array.from(sections);

      const featuresIndex = sectionsArray.findIndex(s => s.classList.contains('features'));
      const showcaseIndex = sectionsArray.findIndex(s => s.classList.contains('product-showcase'));

      expect(featuresIndex).toBeGreaterThan(-1);
      expect(showcaseIndex).toBeGreaterThan(-1);
      expect(showcaseIndex).toBeGreaterThan(featuresIndex);
    });

    it('should have a heading for the showcase section', () => {
      const showcaseSection = document.querySelector('.product-showcase');
      const heading = showcaseSection.querySelector('h2');

      expect(heading).not.toBeNull();
      expect(heading.textContent.trim().length).toBeGreaterThan(0);
    });
  });

  /**
   * CSS Grid/Flexbox Layout Tests for Features Section
   */
  describe('Features Section Layout', () => {
    it('should have CSS grid layout for features-grid', () => {
      const cssPath = path.resolve(__dirname, '../styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf-8');

      // Check that features-grid uses CSS grid or flexbox
      const hasGridOrFlex = cssContent.includes('.features-grid') &&
                           (cssContent.includes('display: grid') ||
                            cssContent.includes('display: flex') ||
                            cssContent.includes('display:grid') ||
                            cssContent.includes('display:flex'));
      expect(hasGridOrFlex).toBe(true);
    });

    it('should have consistent styling for feature cards', () => {
      const cssPath = path.resolve(__dirname, '../styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf-8');

      // Check that feature-card has base styling
      expect(cssContent).toContain('.feature-card');

      const featureCardSection = cssContent.match(/\.feature-card\s*\{[^}]+\}/);
      expect(featureCardSection).not.toBeNull();

      const cardCSS = featureCardSection[0];
      expect(cardCSS).toContain('padding');
      expect(cardCSS).toContain('border-radius');
    });
  });
});
