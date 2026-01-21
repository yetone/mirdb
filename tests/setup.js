const fs = require('fs');
const path = require('path');
require('@testing-library/jest-dom');

/**
 * Setup file for Jest tests
 * Loads the HTML content before each test
 */
beforeEach(() => {
  const htmlPath = path.resolve(__dirname, '../index.html');
  const html = fs.readFileSync(htmlPath, 'utf8');
  document.documentElement.innerHTML = html;
});
