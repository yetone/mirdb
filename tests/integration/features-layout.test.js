/**
 * Features Section Layout Integration Tests
 * Owner: Scenario 3 - Features Section
 *
 * Tests:
 * - Grid layout at desktop viewport (>=1024px)
 * - Grid layout at tablet viewport (768px-1023px)
 * - Grid layout at mobile viewport (<768px)
 * - Card hover effects exist
 */

const fs = require('fs');
const path = require('path');

const htmlPath = path.resolve(__dirname, '../../index.html');
const htmlContent = fs.readFileSync(htmlPath, 'utf-8');
const cssPath = path.resolve(__dirname, '../../css/features.css');
const cssContent = fs.readFileSync(cssPath, 'utf-8');

describe('Features Section - Responsive Grid Layout', () => {
  let featuresSection;
  let gridElement;

  beforeEach(() => {
    document.body.innerHTML = htmlContent;
    featuresSection = document.getElementById('features');
    gridElement = featuresSection.querySelector('.features-grid');
  });

  test('features-grid element exists in DOM', () => {
    expect(gridElement).not.toBeNull();
  });

  test('desktop viewport (>=1024px) has 4-column grid', () => {
    // Simulate desktop viewport
    window.innerWidth = 1200;
    window.innerHeight = 800;
    window.dispatchEvent(new Event('resize'));

    // Check CSS rule exists for 4-column grid
    expect(cssContent).toContain('grid-template-columns: repeat(4, 1fr)');
  });

  test('tablet viewport (768px-1023px) has 2-column grid', () => {
    expect(cssContent).toContain('grid-template-columns: repeat(2, 1fr)');
    expect(cssContent).toContain('max-width: 1023px');
  });

  test('mobile viewport (<768px) has 1-column grid', () => {
    expect(cssContent).toContain('grid-template-columns: 1fr');
    expect(cssContent).toContain('max-width: 767px');
  });

  test('feature cards are rendered in the grid', () => {
    const cards = gridElement.querySelectorAll('.feature-card');
    expect(cards).toHaveLength(4);
  });

  test('each card contains expected elements: icon, title, description', () => {
    const cards = gridElement.querySelectorAll('.feature-card');

    cards.forEach((card) => {
      const icon = card.querySelector('.feature-icon');
      const title = card.querySelector('.feature-title');
      const description = card.querySelector('.feature-description');
      const svg = card.querySelector('svg');

      expect(icon).not.toBeNull();
      expect(svg).not.toBeNull();
      expect(title).not.toBeNull();
      expect(title.tagName.toLowerCase()).toBe('h3');
      expect(description).not.toBeNull();
      expect(description.tagName.toLowerCase()).toBe('p');
    });
  });

  test('feature cards have hover effects defined', () => {
    expect(cssContent).toContain('.feature-card:hover');
    expect(cssContent).toContain('transition');
  });
});
