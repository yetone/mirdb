/**
 * Jest Test Setup
 * Configures the testing environment for MirDB Homepage tests.
 */

import '@testing-library/jest-dom';
import { readFileSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

/**
 * Loads the homepage HTML into the document for testing
 */
export function loadHomepage() {
  const htmlPath = resolve(__dirname, '../index.html');
  const html = readFileSync(htmlPath, 'utf-8');
  document.documentElement.innerHTML = html;
}

/**
 * Clears the document body between tests
 */
export function clearDocument() {
  document.body.innerHTML = '';
  document.head.innerHTML = '';
}
