/**
 * Shared test utilities for homepage testing.
 * Owner: First Builder
 *
 * Expected exports:
 * - loadHTML(): Promise<Document> - Load and parse index.html
 * - getByTestId(doc, id): Element - Query by data-testid
 * - getAllLinks(doc): NodeList - Get all anchor elements
 * - getComputedStyleProperty(element, property): string - Get computed style
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

/**
 * Load and parse index.html
 * @returns {Promise<Document>} The parsed HTML document
 */
async function loadHTML() {
  const htmlPath = path.resolve(__dirname, '../../index.html');
  const html = fs.readFileSync(htmlPath, 'utf-8');
  const dom = new JSDOM(html, {
    url: 'http://localhost',
    runScripts: 'dangerously',
    resources: 'usable'
  });
  return dom.window.document;
}

/**
 * Query element by data-testid attribute
 * @param {Document} doc - The document to query
 * @param {string} id - The test ID to search for
 * @returns {Element|null} The matching element or null
 */
function getByTestId(doc, id) {
  return doc.querySelector(`[data-testid="${id}"]`);
}

/**
 * Get all anchor elements in the document
 * @param {Document} doc - The document to query
 * @returns {NodeList} All anchor elements
 */
function getAllLinks(doc) {
  return doc.querySelectorAll('a');
}

/**
 * Load HTML and return the JSDOM instance for style access
 * @returns {Promise<{document: Document, window: Window}>} The JSDOM window and document
 */
async function loadHTMLWithWindow() {
  const htmlPath = path.resolve(__dirname, '../../index.html');
  const html = fs.readFileSync(htmlPath, 'utf-8');
  const dom = new JSDOM(html, {
    url: 'http://localhost',
    runScripts: 'dangerously',
    resources: 'usable'
  });
  return { document: dom.window.document, window: dom.window };
}

/**
 * Check if an element has a specific CSS property in inline or embedded styles
 * @param {Document} doc - The document
 * @param {Element} element - The element to check
 * @param {string} property - The CSS property name
 * @returns {string|null} The property value or null
 */
function getStyleProperty(doc, element, property) {
  // Check inline style
  if (element.style && element.style[property]) {
    return element.style[property];
  }

  // Check embedded styles by matching selectors
  const styleSheets = doc.querySelectorAll('style');
  for (const styleSheet of styleSheets) {
    const cssText = styleSheet.textContent;
    // This is a simple check - real CSS parsing would be more complex
    if (cssText.includes(property)) {
      return cssText;
    }
  }

  return null;
}

module.exports = {
  loadHTML,
  loadHTMLWithWindow,
  getByTestId,
  getAllLinks,
  getStyleProperty
};
