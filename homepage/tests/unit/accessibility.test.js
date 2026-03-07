/**
 * Accessibility Tests
 * Owner: Scenario 11 - Accessibility Compliance
 *
 * Tests:
 * - Skip navigation link presence
 * - Alt text on all images
 * - Heading hierarchy
 * - Landmark regions
 * - ARIA labels on interactive elements
 */

const fs = require('fs');
const path = require('path');

// Read the HTML file
const htmlPath = path.join(__dirname, '../../index.html');
const htmlContent = fs.readFileSync(htmlPath, 'utf8');

// Set up DOM
document.documentElement.innerHTML = htmlContent;

/**
 * Test Case 1: Check for skip navigation link
 * Expected: Skip to main content link exists as first focusable element
 */
describe('Test Case 1: Skip Navigation Link', () => {
  test('Skip to main content link exists', () => {
    const skipLink = document.querySelector('.skip-link');
    expect(skipLink).not.toBeNull();
    expect(skipLink.tagName).toBe('A');
  });

  test('Skip link points to main content', () => {
    const skipLink = document.querySelector('.skip-link');
    expect(skipLink).not.toBeNull();
    expect(skipLink.getAttribute('href')).toBe('#main-content');
  });

  test('Skip link text is descriptive', () => {
    const skipLink = document.querySelector('.skip-link');
    expect(skipLink).not.toBeNull();
    const text = skipLink.textContent.toLowerCase();
    expect(text).toMatch(/skip|main|content/);
  });

  test('Main content target exists', () => {
    const mainContent = document.getElementById('main-content');
    expect(mainContent).not.toBeNull();
    expect(mainContent.tagName).toBe('MAIN');
  });

  test('Skip link is first focusable element in body', () => {
    // Get all focusable elements in body
    const focusableSelectors = 'a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])';
    const focusableElements = document.body.querySelectorAll(focusableSelectors);

    expect(focusableElements.length).toBeGreaterThan(0);

    // First focusable element should be the skip link
    const firstFocusable = focusableElements[0];
    expect(firstFocusable.classList.contains('skip-link')).toBe(true);
  });
});

/**
 * Test Case 2: Check all images for alt text
 * Expected: All img elements have descriptive alt attributes
 */
describe('Test Case 2: Image Alt Text', () => {
  let images;

  beforeAll(() => {
    images = document.querySelectorAll('img');
  });

  test('All images exist', () => {
    expect(images.length).toBeGreaterThan(0);
  });

  test('All images have alt attributes', () => {
    images.forEach((img, index) => {
      const alt = img.getAttribute('alt');
      expect(alt).not.toBeNull();
      expect(typeof alt).toBe('string');
    });
  });

  test('Alt attributes are descriptive (non-empty)', () => {
    images.forEach((img) => {
      const alt = img.getAttribute('alt');
      expect(alt).not.toBeNull();
      expect(alt.trim().length).toBeGreaterThan(0);
    });
  });

  test('Logo images have appropriate alt text', () => {
    const logoImages = document.querySelectorAll('img[src*="logo"]');
    logoImages.forEach((img) => {
      const alt = img.getAttribute('alt');
      expect(alt).not.toBeNull();
      expect(alt.toLowerCase()).toMatch(/logo|mirdb/i);
    });
  });

  test('Decorative SVGs have aria-hidden', () => {
    // Check that decorative SVG icons have aria-hidden="true"
    const featureIcons = document.querySelectorAll('.feature-card__icon svg');
    featureIcons.forEach((svg) => {
      expect(svg.getAttribute('aria-hidden')).toBe('true');
    });
  });
});

/**
 * Test Case 5: Verify heading hierarchy
 * Expected: Headings follow h1 > h2 > h3 hierarchy without skipping levels
 */
describe('Test Case 5: Heading Hierarchy', () => {
  let headings;

  beforeAll(() => {
    headings = Array.from(document.querySelectorAll('h1, h2, h3, h4, h5, h6'));
  });

  test('Page has exactly one H1 element', () => {
    const h1Elements = document.querySelectorAll('h1');
    expect(h1Elements.length).toBe(1);
  });

  test('H1 contains MirDB', () => {
    const h1 = document.querySelector('h1');
    expect(h1).not.toBeNull();
    expect(h1.textContent).toContain('MirDB');
  });

  test('Headings do not skip levels', () => {
    let previousLevel = 0;

    headings.forEach((heading) => {
      const level = parseInt(heading.tagName.charAt(1));

      // First heading should be H1
      if (previousLevel === 0) {
        expect(level).toBe(1);
      } else {
        // Current level should be same, +1, or less than previous (going back up)
        // Should not skip down more than 1 level (e.g., H2 to H4 is invalid)
        const isValidProgression = level <= previousLevel + 1;
        expect(isValidProgression).toBe(true);
      }

      previousLevel = level;
    });
  });

  test('H2 elements exist for main sections', () => {
    const h2Elements = document.querySelectorAll('h2');
    expect(h2Elements.length).toBeGreaterThanOrEqual(3); // Features, Usage, Quick Start, Architecture
  });

  test('Each section has an associated heading', () => {
    const sections = document.querySelectorAll('section[aria-labelledby]');

    sections.forEach((section) => {
      const labelledBy = section.getAttribute('aria-labelledby');
      expect(labelledBy).toBeTruthy();

      const heading = document.getElementById(labelledBy);
      expect(heading).not.toBeNull();
      expect(heading.tagName).toMatch(/^H[1-6]$/);
    });
  });
});

/**
 * Test Case 6: Check landmark regions
 * Expected: Page has header, main, and footer landmarks
 */
describe('Test Case 6: Landmark Regions', () => {
  test('Page has a header landmark', () => {
    const header = document.querySelector('header');
    expect(header).not.toBeNull();
    expect(header.tagName).toBe('HEADER');
  });

  test('Page has a main landmark', () => {
    const main = document.querySelector('main');
    expect(main).not.toBeNull();
    expect(main.tagName).toBe('MAIN');
  });

  test('Page has a footer landmark', () => {
    const footer = document.querySelector('footer');
    expect(footer).not.toBeNull();
    expect(footer.tagName).toBe('FOOTER');
  });

  test('Page has a nav landmark', () => {
    const nav = document.querySelector('nav');
    expect(nav).not.toBeNull();
    expect(nav.tagName).toBe('NAV');
  });

  test('Nav has aria-label for accessibility', () => {
    const nav = document.querySelector('nav');
    expect(nav).not.toBeNull();
    expect(nav.getAttribute('aria-label')).toBeTruthy();
  });

  test('Main content has unique ID for skip link target', () => {
    const main = document.querySelector('main');
    expect(main).not.toBeNull();
    expect(main.id).toBe('main-content');
  });

  test('Sections use proper semantic elements', () => {
    const sections = document.querySelectorAll('main > section');
    expect(sections.length).toBeGreaterThanOrEqual(4);
  });
});

/**
 * Additional accessibility checks
 */
describe('Additional Accessibility Checks', () => {
  test('HTML lang attribute is set in source HTML', () => {
    // Check the raw HTML content for lang attribute
    const htmlTag = document.querySelector('html');
    // The lang attribute should exist in the parsed HTML
    // We check for the presence of the lang="en" in the source
    expect(htmlContent).toContain('lang="en"');
  });

  test('All buttons have accessible names', () => {
    const buttons = document.querySelectorAll('button');
    buttons.forEach((button) => {
      const hasAriaLabel = button.getAttribute('aria-label');
      const hasTextContent = button.textContent.trim().length > 0;
      const hasAriaLabelledBy = button.getAttribute('aria-labelledby');

      const isAccessible = hasAriaLabel || hasTextContent || hasAriaLabelledBy;
      expect(isAccessible).toBeTruthy();
    });
  });

  test('All links have accessible names', () => {
    const links = document.querySelectorAll('a[href]');
    links.forEach((link) => {
      const hasAriaLabel = link.getAttribute('aria-label');
      const hasTextContent = link.textContent.trim().length > 0;
      const hasAriaLabelledBy = link.getAttribute('aria-labelledby');
      const hasImage = link.querySelector('img[alt]');

      const isAccessible = hasAriaLabel || hasTextContent || hasAriaLabelledBy || hasImage;
      expect(isAccessible).toBeTruthy();
    });
  });

  test('External links have appropriate attributes', () => {
    const externalLinks = document.querySelectorAll('a[target="_blank"]');
    externalLinks.forEach((link) => {
      const rel = link.getAttribute('rel');
      expect(rel).toContain('noopener');
    });
  });

  test('Form elements have labels', () => {
    // Check for any form inputs
    const inputs = document.querySelectorAll('input, select, textarea');
    inputs.forEach((input) => {
      const id = input.getAttribute('id');
      const ariaLabel = input.getAttribute('aria-label');
      const ariaLabelledBy = input.getAttribute('aria-labelledby');

      if (id) {
        const label = document.querySelector(`label[for="${id}"]`);
        const hasLabel = label || ariaLabel || ariaLabelledBy;
        expect(hasLabel).toBeTruthy();
      } else {
        expect(ariaLabel || ariaLabelledBy).toBeTruthy();
      }
    });
  });
});
