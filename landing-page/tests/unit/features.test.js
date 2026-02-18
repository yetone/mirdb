/**
 * Features Section Unit Tests
 * Owner: Scenario 3 - Features Section
 *
 * Test cases:
 * - Features section element exists with appropriate landmark
 * - Between 3 and 6 feature items present
 * - Each feature has h3 or equivalent heading with text
 * - Each feature has paragraph or description element with text
 * - Each feature has svg, img, or icon element
 * - Section has h2 heading, feature items have h3 or lower
 */

const fs = require('fs');
const path = require('path');

describe('Features Section', () => {
  let document;
  let featuresSection;

  beforeAll(() => {
    // Read the HTML file
    const htmlPath = path.join(__dirname, '../../index.html');
    const html = fs.readFileSync(htmlPath, 'utf8');

    // Create a DOM from the HTML
    document = new DOMParser().parseFromString(html, 'text/html');
    featuresSection = document.getElementById('features');
  });

  describe('Test Case 1: Features section element exists with appropriate landmark', () => {
    test('features section exists', () => {
      expect(featuresSection).not.toBeNull();
    });

    test('features section has correct tag name', () => {
      expect(featuresSection.tagName.toLowerCase()).toBe('section');
    });

    test('features section has aria-labelledby attribute', () => {
      expect(featuresSection.getAttribute('aria-labelledby')).toBe('features-title');
    });

    test('features section has features class', () => {
      expect(featuresSection.classList.contains('features')).toBe(true);
    });
  });

  describe('Test Case 2: Between 3 and 6 feature items present', () => {
    test('feature cards exist', () => {
      const featureCards = featuresSection.querySelectorAll('.feature-card');
      expect(featureCards.length).toBeGreaterThan(0);
    });

    test('feature cards count is at least 3', () => {
      const featureCards = featuresSection.querySelectorAll('.feature-card');
      expect(featureCards.length).toBeGreaterThanOrEqual(3);
    });

    test('feature cards count is at most 6', () => {
      const featureCards = featuresSection.querySelectorAll('.feature-card');
      expect(featureCards.length).toBeLessThanOrEqual(6);
    });
  });

  describe('Test Case 3: Each feature has h3 or equivalent heading with text', () => {
    test('each feature card has a heading element', () => {
      const featureCards = featuresSection.querySelectorAll('.feature-card');
      featureCards.forEach((card, index) => {
        const heading = card.querySelector('h3, .feature-title');
        expect(heading).not.toBeNull();
      });
    });

    test('each feature heading has non-empty text content', () => {
      const featureCards = featuresSection.querySelectorAll('.feature-card');
      featureCards.forEach((card, index) => {
        const heading = card.querySelector('h3, .feature-title');
        expect(heading.textContent.trim().length).toBeGreaterThan(0);
      });
    });
  });

  describe('Test Case 4: Each feature has paragraph or description element with text', () => {
    test('each feature card has a description element', () => {
      const featureCards = featuresSection.querySelectorAll('.feature-card');
      featureCards.forEach((card, index) => {
        const description = card.querySelector('p, .feature-description');
        expect(description).not.toBeNull();
      });
    });

    test('each feature description has non-empty text content', () => {
      const featureCards = featuresSection.querySelectorAll('.feature-card');
      featureCards.forEach((card, index) => {
        const description = card.querySelector('p, .feature-description');
        expect(description.textContent.trim().length).toBeGreaterThan(0);
      });
    });
  });

  describe('Test Case 5: Each feature has svg, img, or icon element', () => {
    test('each feature card has a visual element (svg, img, or icon)', () => {
      const featureCards = featuresSection.querySelectorAll('.feature-card');
      featureCards.forEach((card, index) => {
        const icon = card.querySelector('svg, img, .feature-icon, [class*="icon"]');
        expect(icon).not.toBeNull();
      });
    });

    test('each feature icon has aria-hidden for decorative icons', () => {
      const featureCards = featuresSection.querySelectorAll('.feature-card');
      featureCards.forEach((card, index) => {
        const iconContainer = card.querySelector('.feature-icon');
        if (iconContainer) {
          expect(iconContainer.getAttribute('aria-hidden')).toBe('true');
        }
      });
    });
  });

  describe('Test Case 6: Section has h2 heading, feature items have h3 or lower', () => {
    test('features section has an h2 heading', () => {
      const h2 = featuresSection.querySelector('h2');
      expect(h2).not.toBeNull();
    });

    test('features section h2 heading has id "features-title"', () => {
      const h2 = featuresSection.querySelector('h2');
      expect(h2.id).toBe('features-title');
    });

    test('features section h2 heading has non-empty text', () => {
      const h2 = featuresSection.querySelector('h2');
      expect(h2.textContent.trim().length).toBeGreaterThan(0);
    });

    test('feature items use h3 headings (not h1 or h2)', () => {
      const featureCards = featuresSection.querySelectorAll('.feature-card');
      featureCards.forEach((card, index) => {
        const h1InCard = card.querySelector('h1');
        const h2InCard = card.querySelector('h2');
        const h3InCard = card.querySelector('h3');

        expect(h1InCard).toBeNull();
        expect(h2InCard).toBeNull();
        expect(h3InCard).not.toBeNull();
      });
    });

    test('heading hierarchy is correct (section h2 contains feature h3s)', () => {
      const sectionH2 = featuresSection.querySelector('h2');
      const featureH3s = featuresSection.querySelectorAll('.feature-card h3');

      expect(sectionH2).not.toBeNull();
      expect(featureH3s.length).toBeGreaterThanOrEqual(3);
    });
  });
});
