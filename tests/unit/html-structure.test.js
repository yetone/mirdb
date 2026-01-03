/**
 * Unit test for HTML structure of the features section
 * TC6: Verify features section uses semantic markup with proper heading hierarchy
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

describe('Features Section HTML Structure', () => {
  let document;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
  });

  test('TC6: Features section uses semantic markup with proper heading hierarchy', () => {
    // Check for features section with semantic section element
    const featuresSection = document.querySelector('section#features');
    expect(featuresSection).not.toBeNull();

    // Check for aria-labelledby attribute for accessibility
    const ariaLabelledby = featuresSection.getAttribute('aria-labelledby');
    expect(ariaLabelledby).toBe('features-heading');

    // Check for proper h2 heading
    const h2Heading = featuresSection.querySelector('h2#features-heading');
    expect(h2Heading).not.toBeNull();
    expect(h2Heading.textContent).toBeTruthy();

    // Check for feature cards using article elements (semantic)
    const featureCards = featuresSection.querySelectorAll('article.feature-card');
    expect(featureCards.length).toBeGreaterThanOrEqual(4);

    // Check that each feature card has an h3 heading (proper hierarchy: h2 > h3)
    featureCards.forEach((card, index) => {
      const h3 = card.querySelector('h3');
      expect(h3).not.toBeNull();
      expect(h3.textContent).toBeTruthy();

      // Each card should have a description paragraph
      const paragraph = card.querySelector('p');
      expect(paragraph).not.toBeNull();
      expect(paragraph.textContent).toBeTruthy();
    });

    // Verify heading hierarchy: No h3 should appear before h2 in the section
    const allHeadings = featuresSection.querySelectorAll('h1, h2, h3, h4, h5, h6');
    const headingLevels = Array.from(allHeadings).map((h) =>
      parseInt(h.tagName.charAt(1))
    );

    // First heading should be h2
    expect(headingLevels[0]).toBe(2);

    // Subsequent headings should be h3 (children of the section)
    headingLevels.slice(1).forEach((level) => {
      expect(level).toBe(3);
    });
  });

  test('Features section has proper container structure', () => {
    const featuresSection = document.querySelector('section#features');
    const container = featuresSection.querySelector('.container');
    expect(container).not.toBeNull();

    const featuresGrid = container.querySelector('.features-grid');
    expect(featuresGrid).not.toBeNull();
  });

  test('Feature cards have expected class structure', () => {
    const featuresSection = document.querySelector('section#features');
    const featureCards = featuresSection.querySelectorAll('.feature-card');

    featureCards.forEach((card) => {
      // Check for icon element
      const icon = card.querySelector('.feature-icon');
      expect(icon).not.toBeNull();

      // Icon should have aria-hidden for accessibility
      expect(icon.getAttribute('aria-hidden')).toBe('true');
    });
  });
});
