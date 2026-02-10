/**
 * Features Section Unit Tests
 * Owner: Scenario 2 - Features Section Display
 *
 * Unit tests for the features section HTML structure.
 *
 * Expected test coverage:
 * - Features section has an h2 or similar heading element
 *
 * Requirements traced:
 * - REQ-2: Homepage shall showcase key features and capabilities
 */

const fs = require('fs');
const path = require('path');

describe('Features Section Structure', () => {
  let htmlContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');

    // Set up document
    document.body.innerHTML = htmlContent;
  });

  // Test Case 5: Verify feature section has proper heading
  test('features section has an h2 or similar heading element', () => {
    const featuresSection = document.getElementById('features');
    expect(featuresSection).not.toBeNull();

    // Check for heading element (h2 is preferred, but h1, h3, h4 also acceptable)
    const heading = featuresSection.querySelector('h1, h2, h3, h4');
    expect(heading).not.toBeNull();

    // Verify the heading has some text content
    expect(heading.textContent.trim().length).toBeGreaterThan(0);
  });

  test('features section heading contains relevant text', () => {
    const featuresSection = document.getElementById('features');
    const heading = featuresSection.querySelector('h1, h2, h3, h4');

    // Heading should mention "features" or similar
    const headingText = heading.textContent.toLowerCase();
    const hasRelevantText =
      headingText.includes('feature') ||
      headingText.includes('capabilit') ||
      headingText.includes('what') ||
      headingText.includes('why');

    expect(hasRelevantText).toBeTruthy();
  });

  test('features section is accessible with proper semantic structure', () => {
    const featuresSection = document.getElementById('features');

    // Verify section element is used or has proper role
    expect(
      featuresSection.tagName === 'SECTION' ||
      featuresSection.getAttribute('role') === 'region'
    ).toBeTruthy();
  });
});
