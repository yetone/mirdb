/**
 * Benefits Section Unit Tests
 * Owner: Scenario 4 - Benefits Section
 *
 * Test cases:
 * - Test Case 1: Benefits section element exists with id or class
 * - Test Case 2: At least 2 benefit descriptions present
 * - Test Case 3: Section has h2 heading element
 * - Test Case 4: Each benefit has substantive description (>20 characters)
 */

const fs = require('fs');
const path = require('path');

describe('Benefits Section', () => {
  let document;
  let benefitsSection;

  beforeAll(() => {
    // Read the HTML file
    const htmlPath = path.join(__dirname, '../../index.html');
    const html = fs.readFileSync(htmlPath, 'utf8');

    // Create a DOM from the HTML
    document = new DOMParser().parseFromString(html, 'text/html');
    benefitsSection = document.getElementById('benefits');
  });

  describe('Test Case 1: Query for benefits section by id or class', () => {
    test('benefits section exists with id "benefits"', () => {
      expect(benefitsSection).not.toBeNull();
    });

    test('benefits section is a section element', () => {
      expect(benefitsSection.tagName.toLowerCase()).toBe('section');
    });

    test('benefits section has benefits class', () => {
      expect(benefitsSection.classList.contains('benefits')).toBe(true);
    });

    test('benefits section has aria-labelledby for accessibility', () => {
      expect(benefitsSection.getAttribute('aria-labelledby')).toBe('benefits-title');
    });
  });

  describe('Test Case 2: Query for benefit items within section', () => {
    test('benefit items exist within the section', () => {
      const benefitItems = benefitsSection.querySelectorAll('.benefit-item, .benefit-card, [class*="benefit"]');
      expect(benefitItems.length).toBeGreaterThan(0);
    });

    test('at least 2 benefit descriptions are present', () => {
      const benefitItems = benefitsSection.querySelectorAll('.benefit-item, .benefit-card');
      expect(benefitItems.length).toBeGreaterThanOrEqual(2);
    });
  });

  describe('Test Case 3: Check benefits section has heading', () => {
    test('benefits section has an h2 heading element', () => {
      const h2 = benefitsSection.querySelector('h2');
      expect(h2).not.toBeNull();
    });

    test('h2 heading has id "benefits-title"', () => {
      const h2 = benefitsSection.querySelector('h2');
      expect(h2.id).toBe('benefits-title');
    });

    test('h2 heading has non-empty text content', () => {
      const h2 = benefitsSection.querySelector('h2');
      expect(h2.textContent.trim().length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 4: Verify benefit text content length', () => {
    test('each benefit has a description element', () => {
      const benefitItems = benefitsSection.querySelectorAll('.benefit-item, .benefit-card');
      benefitItems.forEach((item) => {
        const description = item.querySelector('p, .benefit-description, .benefit-text');
        expect(description).not.toBeNull();
      });
    });

    test('each benefit description has substantive text (>20 characters)', () => {
      const benefitItems = benefitsSection.querySelectorAll('.benefit-item, .benefit-card');
      benefitItems.forEach((item, index) => {
        const description = item.querySelector('p, .benefit-description, .benefit-text');
        const text = description.textContent.trim();
        expect(text.length).toBeGreaterThan(20);
      });
    });

    test('each benefit has a title or heading', () => {
      const benefitItems = benefitsSection.querySelectorAll('.benefit-item, .benefit-card');
      benefitItems.forEach((item) => {
        const title = item.querySelector('h3, h4, .benefit-title');
        expect(title).not.toBeNull();
        expect(title.textContent.trim().length).toBeGreaterThan(0);
      });
    });
  });

  describe('Accessibility and Structure', () => {
    test('benefits section follows features section in DOM order', () => {
      const featuresSection = document.getElementById('features');
      const allSections = Array.from(document.querySelectorAll('main section'));
      const featuresIndex = allSections.indexOf(featuresSection);
      const benefitsIndex = allSections.indexOf(benefitsSection);

      expect(benefitsIndex).toBeGreaterThan(featuresIndex);
    });

    test('benefit items have semantic HTML structure', () => {
      const benefitItems = benefitsSection.querySelectorAll('.benefit-item, .benefit-card');
      benefitItems.forEach((item) => {
        // Should be an article or div with proper structure
        expect(['article', 'div', 'li'].includes(item.tagName.toLowerCase())).toBe(true);
      });
    });
  });
});
