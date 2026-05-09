/*
 * Owner: first builder.
 * Exports:
 *   loadHomepage(): Promise<JSDOM> - reads index.html into jsdom for integration tests.
 *   getDocument(): Document - convenience accessor.
 *   resetDom(): void - clears jsdom between tests.
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

const INDEX_PATH = path.resolve(__dirname, '../../index.html');

let currentDom = null;

function loadHomepage() {
  const html = fs.readFileSync(INDEX_PATH, 'utf-8');
  currentDom = new JSDOM(html, {
    url: 'http://localhost:3000',
    runScripts: 'dangerously',
    resources: 'usable',
  });
  return currentDom;
}

function getDocument() {
  if (!currentDom) {
    throw new Error('DOM not loaded. Call loadHomepage() first.');
  }
  return currentDom.window.document;
}

function resetDom() {
  currentDom = null;
}

module.exports = { loadHomepage, getDocument, resetDom };
