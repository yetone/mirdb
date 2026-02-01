/**
 * DOM testing utilities shared across tests.
 * Owner: First Builder
 *
 * Expected exports:
 * - loadHTML(filePath): Load and parse HTML file
 * - querySection(sectionId): Query page section
 * - getComputedStyles(element): Get computed CSS
 */
const fs = require('fs');
const path = require('path');

/**
 * Load and parse HTML file into the document
 * @param {string} filePath - Path to the HTML file (relative to project root)
 * @returns {Document} - The parsed document
 */
function loadHTML(filePath) {
  const absolutePath = path.resolve(__dirname, '../../', filePath);
  const html = fs.readFileSync(absolutePath, 'utf8');
  document.documentElement.innerHTML = html;
  return document;
}

/**
 * Query a section by its ID
 * @param {string} sectionId - The ID of the section
 * @returns {Element|null} - The section element or null
 */
function querySection(sectionId) {
  return document.getElementById(sectionId);
}

/**
 * Get computed styles for an element
 * @param {Element} element - The DOM element
 * @returns {CSSStyleDeclaration} - The computed styles
 */
function getComputedStyles(element) {
  return window.getComputedStyle(element);
}

/**
 * Load HTML and return the body content
 * @param {string} filePath - Path to the HTML file
 * @returns {HTMLElement} - The document body
 */
function loadHTMLBody(filePath) {
  loadHTML(filePath);
  return document.body;
}

/**
 * Query all elements matching a selector within a section
 * @param {HTMLElement} section - The section to query within
 * @param {string} selector - CSS selector
 * @returns {NodeList} The matching elements
 */
function queryAllInSection(section, selector) {
  return section.querySelectorAll(selector);
}

/**
 * Check if element contains text
 * @param {HTMLElement} element - The element to check
 * @param {string} text - The text to search for
 * @returns {boolean} True if text is found
 */
function containsText(element, text) {
  return element.textContent.includes(text);
}

module.exports = {
  loadHTML,
  querySection,
  getComputedStyles,
  loadHTMLBody,
  queryAllInSection,
  containsText
};
