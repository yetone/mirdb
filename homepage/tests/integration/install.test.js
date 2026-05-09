/**
 * Scenario 3 - Installation Section Integration Tests
 * Owner: Scenario 3
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

const __dirname = path.dirname(new URL(import.meta.url).pathname);
const INDEX_PATH = path.resolve(__dirname, '../../index.html');

function loadHomepage() {
  const html = fs.readFileSync(INDEX_PATH, 'utf-8');
  return new JSDOM(html, {
    url: 'http://localhost:3000',
    runScripts: 'dangerously',
    resources: 'usable',
  });
}

describe('Installation Section', () => {
  let dom;
  let document;

  beforeEach(() => {
    dom = loadHomepage();
    document = dom.window.document;
  });

  afterEach(() => {
    dom = null;
    document = null;
  });

  it('has a section with id "install"', () => {
    const section = document.querySelector('#install');
    expect(section).not.toBeNull();
  });

  it('contains at least two code blocks', () => {
    const codeBlocks = document.querySelectorAll('#install pre');
    expect(codeBlocks.length).toBeGreaterThanOrEqual(2);
  });

  it('has a code block containing "cargo install" command', () => {
    const codeElements = document.querySelectorAll('#install code');
    const texts = Array.from(codeElements).map(el => el.textContent);
    const hasCargo = texts.some(text => text.includes('cargo install'));
    expect(hasCargo).toBe(true);
  });

  it('has a code block with pre-built binary download instructions', () => {
    const codeElements = document.querySelectorAll('#install code');
    const texts = Array.from(codeElements).map(el => el.textContent);
    const hasBinary = texts.some(text =>
      text.includes('curl') || text.includes('wget') || text.includes('releases')
    );
    expect(hasBinary).toBe(true);
  });

  it('has an equal number of copy buttons and code blocks', () => {
    const preBlocks = document.querySelectorAll('#install pre');
    const copyButtons = document.querySelectorAll('#install .copy-btn');
    expect(copyButtons.length).toBe(preBlocks.length);
  });

  it('every copy button is inside a code block wrapper', () => {
    const copyButtons = document.querySelectorAll('#install .copy-btn');
    copyButtons.forEach(btn => {
      const codeBlock = btn.closest('.code-block');
      expect(codeBlock).not.toBeNull();
      const pre = codeBlock.querySelector('pre');
      expect(pre).not.toBeNull();
    });
  });

  it('every code block has a data-copy attribute on its code element', () => {
    const codeBlocks = document.querySelectorAll('#install pre code');
    codeBlocks.forEach(code => {
      expect(code.hasAttribute('data-copy')).toBe(true);
    });
  });
});
