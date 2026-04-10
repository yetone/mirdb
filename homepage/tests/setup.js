/**
 * Test Setup and Configuration
 *
 * Common test utilities and configuration:
 * - Test framework setup (Vitest)
 * - DOM testing utilities
 * - Custom matchers
 * - Mock helpers for localStorage, Clipboard API
 */

import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// Load HTML fixture
export function loadHTML() {
  const htmlPath = path.resolve(__dirname, '../index.html');
  const html = fs.readFileSync(htmlPath, 'utf-8');
  const dom = new JSDOM(html, { runScripts: 'outside-only' });
  return dom;
}

// Mock localStorage
export function mockLocalStorage() {
  const store = {};
  return {
    getItem: (key) => store[key] || null,
    setItem: (key, value) => { store[key] = value; },
    removeItem: (key) => { delete store[key]; },
    clear: () => { Object.keys(store).forEach(key => delete store[key]); },
  };
}

// Mock Clipboard API
export function mockClipboard() {
  let content = '';
  return {
    writeText: async (text) => { content = text; return true; },
    readText: async () => content,
  };
}
