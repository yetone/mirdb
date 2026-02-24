/**
 * Shared Test Utilities
 * Owner: First scenario builder
 *
 * Provides common test helpers for DOM manipulation, mocking, and assertions.
 */

/**
 * Create DOM from HTML string for testing
 * @param {string} htmlString - HTML markup to render
 * @returns {Document} Parsed document
 */
export function renderHTML(htmlString) {
  const parser = new DOMParser();
  return parser.parseFromString(htmlString, 'text/html');
}

/**
 * Create a mock localStorage for testing
 * @returns {Object} Mock localStorage with get, set, clear methods
 */
export function mockLocalStorage() {
  let store = {};
  return {
    getItem: (key) => store[key] || null,
    setItem: (key, value) => { store[key] = String(value); },
    removeItem: (key) => { delete store[key]; },
    clear: () => { store = {}; },
    get length() { return Object.keys(store).length; },
    key: (i) => Object.keys(store)[i] || null,
  };
}

/**
 * Create a mock clipboard API for testing
 * @returns {Object} Mock clipboard with writeText method
 */
export function mockClipboard() {
  let clipboardContent = '';
  return {
    writeText: async (text) => {
      clipboardContent = text;
      return Promise.resolve();
    },
    readText: async () => {
      return Promise.resolve(clipboardContent);
    },
    getContent: () => clipboardContent,
  };
}

/**
 * Wait for CSS transitions to complete
 * @param {number} ms - Milliseconds to wait
 * @returns {Promise}
 */
export function waitForAnimation(ms = 300) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Create a minimal HTML document structure for testing
 * @returns {string} HTML document string
 */
export function createBaseDocument() {
  return `
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>MirDB - Test</title>
    </head>
    <body>
      <main></main>
    </body>
    </html>
  `;
}

/**
 * Simulate a click event on an element
 * @param {Element} element - Element to click
 */
export function simulateClick(element) {
  const event = new MouseEvent('click', {
    bubbles: true,
    cancelable: true,
    view: window,
  });
  element.dispatchEvent(event);
}

/**
 * Simulate keyboard event
 * @param {Element} element - Element to trigger event on
 * @param {string} key - Key to simulate
 * @param {string} type - Event type ('keydown', 'keyup', 'keypress')
 */
export function simulateKeyEvent(element, key, type = 'keydown') {
  const event = new KeyboardEvent(type, {
    key,
    bubbles: true,
    cancelable: true,
  });
  element.dispatchEvent(event);
}
