/**
 * Test Setup Utilities
 * Shared by: All scenarios
 *
 * Provides:
 * - DOM setup helpers
 * - Viewport simulation
 * - Common test utilities
 */

import { JSDOM } from 'jsdom';
import { readFileSync } from 'fs';
import { resolve } from 'path';

/**
 * Load the index.html file and create a JSDOM instance
 */
export function loadDocument() {
  const htmlPath = resolve(process.cwd(), 'src/index.html');
  const html = readFileSync(htmlPath, 'utf-8');
  const dom = new JSDOM(html, {
    url: 'http://localhost:3000',
    runScripts: 'dangerously',
    resources: 'usable',
  });
  return dom;
}

/**
 * Get the document from a JSDOM instance
 */
export function getDocument(dom) {
  return dom.window.document;
}

/**
 * Simulate viewport width for testing
 */
export function setViewportWidth(dom, width) {
  Object.defineProperty(dom.window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: width,
  });
  dom.window.dispatchEvent(new dom.window.Event('resize'));
}

/**
 * Wait for DOM to be ready
 */
export function waitForDOMReady(dom) {
  return new Promise((resolve) => {
    if (dom.window.document.readyState === 'complete') {
      resolve();
    } else {
      dom.window.document.addEventListener('DOMContentLoaded', resolve);
    }
  });
}

// Helper to load HTML content into the DOM
export function loadHTML(html) {
  document.body.innerHTML = html;
}

// Helper to wait for DOM updates
export function nextTick() {
  return new Promise(resolve => setTimeout(resolve, 0));
}

// Helper to simulate click events
export function click(element) {
  element.dispatchEvent(new MouseEvent('click', {
    bubbles: true,
    cancelable: true,
    view: window,
  }));
}

// Helper to simulate keyboard events
export function pressKey(element, key) {
  element.dispatchEvent(new KeyboardEvent('keydown', {
    key,
    bubbles: true,
    cancelable: true,
  }));
}
