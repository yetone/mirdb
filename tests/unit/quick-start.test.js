/**
 * Quick Start Section Unit Tests
 * Owner: Scenario 3 - Quick Start Section
 *
 * Tests:
 * - Quick Start section heading exists
 * - Section structure validation
 */
import { describe, test, expect, beforeAll } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

describe('Quick Start Section Unit Tests', () => {
  let document;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../docs/index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
  });

  test('TC5: Section has heading with text "Quick Start"', () => {
    const section = document.querySelector('#quick-start');
    expect(section).not.toBeNull();

    const heading = section.querySelector('h2');
    expect(heading).not.toBeNull();
    expect(heading.textContent.toLowerCase()).toContain('quick start');
  });

  test('Quick Start section has proper aria-labelledby', () => {
    const section = document.querySelector('#quick-start');
    expect(section).not.toBeNull();

    const ariaLabelledBy = section.getAttribute('aria-labelledby');
    expect(ariaLabelledBy).toBe('quick-start-heading');

    const headingWithId = document.querySelector('#quick-start-heading');
    expect(headingWithId).not.toBeNull();
  });

  test('Section contains at least one code block', () => {
    const section = document.querySelector('#quick-start');
    const codeBlocks = section.querySelectorAll('.code-block');
    expect(codeBlocks.length).toBeGreaterThan(0);
  });

  test('Each code block has a copy button', () => {
    const section = document.querySelector('#quick-start');
    const codeBlocks = section.querySelectorAll('.code-block');

    codeBlocks.forEach((block) => {
      const copyBtn = block.querySelector('.copy-btn');
      expect(copyBtn).not.toBeNull();
      expect(copyBtn.getAttribute('type')).toBe('button');
    });
  });

  test('Code blocks contain pre and code elements', () => {
    const section = document.querySelector('#quick-start');
    const codeBlocks = section.querySelectorAll('.code-block');

    codeBlocks.forEach((block) => {
      expect(block.querySelector('pre')).not.toBeNull();
      expect(block.querySelector('code')).not.toBeNull();
    });
  });

  test('Code blocks have syntax highlighting spans', () => {
    const section = document.querySelector('#quick-start');
    const codeBlocks = section.querySelectorAll('.code-block');

    // Check that there are spans with syntax highlighting classes
    const highlightClasses = ['keyword', 'command', 'string', 'comment', 'number'];
    let hasHighlighting = false;

    codeBlocks.forEach((block) => {
      highlightClasses.forEach((className) => {
        if (block.querySelector(`.${className}`)) {
          hasHighlighting = true;
        }
      });
    });

    expect(hasHighlighting).toBe(true);
  });

  test('Server start command is present', () => {
    const section = document.querySelector('#quick-start');
    const codeContent = section.textContent;

    expect(codeContent).toContain('cargo run');
  });

  test('GET and SET commands are demonstrated', () => {
    const section = document.querySelector('#quick-start');
    const codeContent = section.textContent;

    expect(codeContent.toLowerCase()).toContain('set');
    expect(codeContent.toLowerCase()).toContain('get');
    expect(codeContent).toContain('STORED');
    expect(codeContent).toContain('VALUE');
  });
});
