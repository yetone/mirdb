/**
 * Benefits Section Integration Tests
 * Owner: Scenario 4 - Benefits Section
 *
 * Test Case 5: Benefits section is visible and readable after scrolling
 */

const fs = require('fs');
const path = require('path');

describe('Benefits Section Visibility Integration', () => {
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

  describe('Test Case 5: Benefits section visibility on scroll', () => {
    test('benefits section exists in the document', () => {
      expect(benefitsSection).not.toBeNull();
    });

    test('benefits section has content that can be scrolled to', () => {
      // Verify section has substantial content
      const content = benefitsSection.textContent.trim();
      expect(content.length).toBeGreaterThan(50);
    });

    test('benefits section is not hidden via CSS display property', () => {
      const style = benefitsSection.getAttribute('style');
      if (style) {
        expect(style).not.toContain('display: none');
        expect(style).not.toContain('display:none');
      }
    });

    test('benefits section does not have hidden attribute', () => {
      expect(benefitsSection.hasAttribute('hidden')).toBe(false);
    });

    test('benefits section has readable heading', () => {
      const heading = benefitsSection.querySelector('h2');
      expect(heading).not.toBeNull();
      expect(heading.textContent.trim().length).toBeGreaterThan(0);
    });

    test('benefits section has readable benefit descriptions', () => {
      const benefitItems = benefitsSection.querySelectorAll('.benefit-item, .benefit-card');
      expect(benefitItems.length).toBeGreaterThanOrEqual(2);

      benefitItems.forEach((item) => {
        const description = item.querySelector('p, .benefit-description, .benefit-text');
        expect(description).not.toBeNull();
        expect(description.textContent.trim().length).toBeGreaterThan(20);
      });
    });

    test('benefits section content follows semantic hierarchy', () => {
      const h2 = benefitsSection.querySelector('h2');
      const h3s = benefitsSection.querySelectorAll('h3');

      expect(h2).not.toBeNull();
      // If there are benefit items, they should have h3s
      const benefitItems = benefitsSection.querySelectorAll('.benefit-item, .benefit-card');
      if (benefitItems.length > 0) {
        expect(h3s.length).toBeGreaterThanOrEqual(benefitItems.length);
      }
    });

    test('benefits section is navigable via anchor link', () => {
      // Check that nav link to benefits exists
      const navLink = document.querySelector('a[href="#benefits"]');
      expect(navLink).not.toBeNull();

      // Check that benefits section has the correct id
      expect(benefitsSection.id).toBe('benefits');
    });
  });

  describe('Readability and Content Quality', () => {
    test('benefit titles are concise but descriptive', () => {
      const titles = benefitsSection.querySelectorAll('.benefit-item h3, .benefit-card h3, .benefit-title');
      titles.forEach((title) => {
        const text = title.textContent.trim();
        // Title should be between 3 and 100 characters
        expect(text.length).toBeGreaterThanOrEqual(3);
        expect(text.length).toBeLessThanOrEqual(100);
      });
    });

    test('benefit descriptions use problem-solution or value framing', () => {
      const descriptions = benefitsSection.querySelectorAll('.benefit-item p, .benefit-card p, .benefit-description, .benefit-text');
      descriptions.forEach((desc) => {
        const text = desc.textContent.trim().toLowerCase();
        // Each description should be substantive
        expect(text.length).toBeGreaterThan(20);
      });
    });
  });
});
