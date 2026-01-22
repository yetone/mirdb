// Unit test for skip link presence and accessibility
const fs = require('fs');
const path = require('path');

describe('TC4: Skip Link Accessibility', () => {
  let htmlContent;

  beforeAll(() => {
    // Read the main index.html from the project root
    const htmlPath = path.join(__dirname, '..', '..', 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');
  });

  beforeEach(() => {
    // Extract and load body content into jsdom
    const bodyMatch = htmlContent.match(/<body[^>]*>([\s\S]*)<\/body>/i);
    const bodyContent = bodyMatch ? bodyMatch[1] : '';
    document.body.innerHTML = bodyContent;

    // Also add the stylesheet link to get CSS variables
    const styleLink = document.createElement('link');
    styleLink.rel = 'stylesheet';
    styleLink.href = '../../styles.css';
    document.head.appendChild(styleLink);
  });

  test('Skip link element exists in the DOM', () => {
    const skipLink = document.querySelector('.skip-link');
    expect(skipLink).toBeInTheDocument();
  });

  test('Skip link has correct href pointing to main content', () => {
    const skipLink = document.querySelector('.skip-link');
    expect(skipLink).toHaveAttribute('href', '#main-content');
  });

  test('Skip link has accessible text for screen readers', () => {
    const skipLink = document.querySelector('.skip-link');
    expect(skipLink.textContent).toBe('Skip to main content');
  });

  test('Skip link is an anchor element', () => {
    const skipLink = document.querySelector('.skip-link');
    expect(skipLink.tagName.toLowerCase()).toBe('a');
  });

  test('Main content target element exists with correct id', () => {
    const mainContent = document.getElementById('main-content');
    expect(mainContent).toBeInTheDocument();
  });

  test('Main content is a semantic main element', () => {
    const mainContent = document.getElementById('main-content');
    expect(mainContent.tagName.toLowerCase()).toBe('main');
  });

  test('Skip link is first focusable element in DOM', () => {
    // Get all focusable elements
    const focusableSelector = 'a[href], button, input, textarea, select, [tabindex]:not([tabindex="-1"])';
    const focusableElements = document.querySelectorAll(focusableSelector);

    // Skip link should be the first focusable element
    expect(focusableElements[0]).toHaveClass('skip-link');
  });

  test('Skip link is positioned before the header in DOM order', () => {
    const skipLink = document.querySelector('.skip-link');
    const header = document.querySelector('header');

    // Compare positions in DOM
    const position = skipLink.compareDocumentPosition(header);

    // 4 means header is following (after) skip link
    expect(position & Node.DOCUMENT_POSITION_FOLLOWING).toBe(Node.DOCUMENT_POSITION_FOLLOWING);
  });

  test('Skip link text does not contain invalid characters', () => {
    const skipLink = document.querySelector('.skip-link');
    const text = skipLink.textContent;

    // Should be clean text without special characters
    expect(text).toMatch(/^[a-zA-Z\s]+$/);
    expect(text).not.toContain('<');
    expect(text).not.toContain('>');
  });

  test('Skip link is not hidden with aria-hidden', () => {
    const skipLink = document.querySelector('.skip-link');
    expect(skipLink).not.toHaveAttribute('aria-hidden', 'true');
  });

  test('Skip link does not have negative tabindex', () => {
    const skipLink = document.querySelector('.skip-link');
    const tabIndex = skipLink.getAttribute('tabindex');

    // tabindex should either not exist or be >= 0
    if (tabIndex !== null) {
      expect(parseInt(tabIndex, 10)).toBeGreaterThanOrEqual(0);
    }
  });

  test('Skip link purpose is clear from text', () => {
    const skipLink = document.querySelector('.skip-link');
    const text = skipLink.textContent.toLowerCase();

    // The text should indicate it's for skipping to content
    expect(text).toContain('skip');
    expect(text).toContain('content');
  });
});
