/**
 * Features Section Display Tests
 * Scenario: Verify product features/benefits section highlights key value propositions (REQ-2)
 */

const { getByTestId, getAllByTestId, queryByTestId } = require('@testing-library/dom');

describe('Features Section Display', () => {
  /**
   * Test Case 1: Check features section exists
   * Expected: Features section element is present in DOM
   */
  test('features section exists in DOM', () => {
    const featuresSection = document.querySelector('[data-testid="features-section"]') ||
                           document.querySelector('#features') ||
                           document.querySelector('.features-section') ||
                           document.querySelector('section.features');

    expect(featuresSection).toBeInTheDocument();
    expect(featuresSection).not.toBeNull();
  });

  /**
   * Test Case 2: Count feature items
   * Expected: At least 3 feature items are displayed
   */
  test('displays at least 3 feature items', () => {
    const featureItems = document.querySelectorAll('[data-testid="feature-card"]') ||
                        document.querySelectorAll('.feature-card') ||
                        document.querySelectorAll('.feature-item');

    // Should have at least 3 features as per PRD requirements
    expect(featureItems.length).toBeGreaterThanOrEqual(3);
  });

  /**
   * Test Case 3: Verify feature card structure
   * Expected: Each feature has icon, title, and description elements
   */
  test('each feature card has icon, title, and description', () => {
    const featureCards = document.querySelectorAll('[data-testid="feature-card"]');

    expect(featureCards.length).toBeGreaterThan(0);

    featureCards.forEach((card, index) => {
      // Check for icon (could be img, svg, or icon element)
      const icon = card.querySelector('[data-testid="feature-icon"]') ||
                  card.querySelector('.feature-icon') ||
                  card.querySelector('svg') ||
                  card.querySelector('img.icon') ||
                  card.querySelector('i');

      // Check for title
      const title = card.querySelector('[data-testid="feature-title"]') ||
                   card.querySelector('.feature-title') ||
                   card.querySelector('h3') ||
                   card.querySelector('h4');

      // Check for description
      const description = card.querySelector('[data-testid="feature-description"]') ||
                         card.querySelector('.feature-description') ||
                         card.querySelector('p');

      expect(icon).toBeInTheDocument();
      expect(title).toBeInTheDocument();
      expect(description).toBeInTheDocument();

      // Title should have text content
      expect(title.textContent.trim().length).toBeGreaterThan(0);

      // Description should have text content
      expect(description.textContent.trim().length).toBeGreaterThan(0);
    });
  });

  /**
   * Test Case 4: Check grid/flex layout
   * Expected: Features section uses CSS grid or flexbox layout
   */
  test('features section uses CSS grid or flexbox layout', () => {
    const fs = require('fs');
    const path = require('path');

    const featuresSection = document.querySelector('[data-testid="features-section"]') ||
                           document.querySelector('#features') ||
                           document.querySelector('.features-section');

    // Get the container that holds feature cards
    const featuresGrid = featuresSection.querySelector('[data-testid="features-grid"]') ||
                        featuresSection.querySelector('.features-grid') ||
                        featuresSection.querySelector('.features-container');

    // Verify the grid element exists
    expect(featuresGrid).toBeInTheDocument();

    // Read the CSS file and verify it defines grid or flex for features-grid
    const cssPath = path.resolve(__dirname, '../src/styles.css');
    const cssContent = fs.readFileSync(cssPath, 'utf8');

    // Check if CSS defines grid or flex layout for features-grid class
    const hasGridOrFlexLayout = cssContent.includes('.features-grid') &&
                                (cssContent.includes('display: grid') ||
                                 cssContent.includes('display: flex') ||
                                 cssContent.includes('display:grid') ||
                                 cssContent.includes('display:flex'));

    expect(hasGridOrFlexLayout).toBe(true);
  });

  /**
   * Test Case 5: Verify visual consistency
   * Expected: All feature cards have consistent styling and dimensions
   */
  test('all feature cards have consistent styling', () => {
    const fs = require('fs');
    const path = require('path');

    const featureCards = document.querySelectorAll('[data-testid="feature-card"]');

    expect(featureCards.length).toBeGreaterThan(0);

    // All feature cards should have the same class applied
    const allHaveSameClass = Array.from(featureCards).every(card => {
      return card.classList.contains('feature-card');
    });
    expect(allHaveSameClass).toBe(true);

    // Read CSS and verify there's a single .feature-card style rule
    // (consistent styling means all cards use the same CSS class)
    const cssPath = path.resolve(__dirname, '../src/styles.css');
    const cssContent = fs.readFileSync(cssPath, 'utf8');

    // Verify feature-card class has styling defined
    expect(cssContent).toContain('.feature-card');

    // Verify consistent styling properties are defined
    // (padding, background-color, border-radius are typical card styles)
    const featureCardSection = cssContent.match(/\.feature-card\s*\{[^}]+\}/);
    expect(featureCardSection).not.toBeNull();

    const cardCSS = featureCardSection[0];
    expect(cardCSS).toContain('padding');
    expect(cardCSS).toContain('border-radius');
    expect(cardCSS).toContain('background-color');
  });
});
