/**
 * Tests for the Get Started code example (Scenario 3).
 * Covers test cases 1-4 and 8 from .something/scenario.json.
 */

const { loadFullPage } = require('../helpers/dom');

describe('code-example: quick-start section markup', () => {
  let doc;

  beforeEach(() => {
    doc = loadFullPage();
  });

  test('test case 1: #quick-start section exists', () => {
    const section = doc.getElementById('quick-start');
    expect(section).not.toBeNull();
    expect(section.tagName.toLowerCase()).toBe('section');
  });

  test('test case 2: pre > code uses a language- class for syntax highlighting', () => {
    const quickStart = doc.getElementById('quick-start');
    expect(quickStart).not.toBeNull();

    const codeElements = quickStart.querySelectorAll('pre > code');
    expect(codeElements.length).toBe(1);

    const code = codeElements[0];
    const hasLanguageClass = Array.from(code.classList).some((cls) =>
      cls.startsWith('language-')
    );
    expect(hasLanguageClass).toBe(true);
  });

  test('test case 3: code textContent includes at least one Memcached verb', () => {
    const quickStart = doc.getElementById('quick-start');
    const code = quickStart.querySelector('pre > code');
    const text = (code.textContent || '').toLowerCase();
    const verbs = ['set', 'get', 'add', 'replace', 'delete'];
    const found = verbs.filter((verb) =>
      new RegExp('\\b' + verb + '\\b').test(text)
    );
    expect(found.length).toBeGreaterThan(0);
  });

  test('test case 4: .copy-btn is a button with non-empty aria-label referencing copy', () => {
    const quickStart = doc.getElementById('quick-start');
    const button = quickStart.querySelector('.copy-btn');
    expect(button).not.toBeNull();
    expect(button.tagName.toLowerCase()).toBe('button');

    const ariaLabel = button.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel.length).toBeGreaterThan(0);
    expect(ariaLabel.toLowerCase()).toMatch(/copy/);
  });

  test('test case 8: .code-block uses the dark theme --color-code-bg token', () => {
    const path = require('path');
    const fs = require('fs');

    const themeCss = fs.readFileSync(
      path.resolve(__dirname, '..', '..', 'src', 'styles', 'theme.css'),
      'utf8'
    );
    const componentCss = fs.readFileSync(
      path.resolve(
        __dirname,
        '..',
        '..',
        'src',
        'components',
        'code-example',
        'code-example.css'
      ),
      'utf8'
    );

    // 1. The dark-theme token is defined.
    const tokenMatch = themeCss.match(
      /--color-code-bg:\s*(#[0-9a-fA-F]+)/
    );
    expect(tokenMatch).not.toBeNull();
    const hex = tokenMatch[1].toLowerCase();

    // 2. The token resolves to a visually dark color (low luminance).
    const r = parseInt(hex.slice(1, 3), 16);
    const g = parseInt(hex.slice(3, 5), 16);
    const b = parseInt(hex.slice(5, 7), 16);
    const luminance = 0.2126 * r + 0.7152 * g + 0.0722 * b;
    expect(luminance).toBeLessThan(64); // out of 255

    // 3. The code-block container in CSS uses the token for its background.
    expect(componentCss).toMatch(
      /\.code-block[\s\S]*background-color:\s*var\(--color-code-bg\)/
    );
  });
});
