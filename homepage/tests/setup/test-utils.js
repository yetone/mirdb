/**
 * Shared Test Utilities
 *
 * Common utilities used across unit and E2E tests.
 */

const fs = require('fs');
const path = require('path');

const VIEWPORT_SIZES = {
    mobile: { width: 375, height: 667 },
    tablet: { width: 768, height: 1024 },
    desktop: { width: 1280, height: 800 }
};

const BASE_URL = process.env.BASE_URL || 'http://localhost:3000';

/**
 * Get element by selector helper
 * @param {Document} doc - Document object
 * @param {string} selector - CSS selector
 * @returns {Element|null}
 */
function getElement(doc, selector) {
    return doc.querySelector(selector);
}

/**
 * Get all elements by selector helper
 * @param {Document} doc - Document object
 * @param {string} selector - CSS selector
 * @returns {NodeList}
 */
function getAllElements(doc, selector) {
    return doc.querySelectorAll(selector);
}

/**
 * Check if element contains text
 * @param {Element} element - DOM element
 * @param {string} text - Text to search for
 * @returns {boolean}
 */
function elementContainsText(element, text) {
    return element.textContent.toLowerCase().includes(text.toLowerCase());
}

/**
 * Load the homepage HTML file for unit testing
 * @returns {string} The HTML content of the homepage
 */
function loadHomepageHTML() {
    const htmlPath = path.join(__dirname, '../../index.html');
    return fs.readFileSync(htmlPath, 'utf-8');
}

module.exports = {
    VIEWPORT_SIZES,
    BASE_URL,
    getElement,
    getAllElements,
    elementContainsText,
    loadHomepageHTML
};
