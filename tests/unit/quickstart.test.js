/**
 * Quick Start Section Unit Tests
 * Owner: Scenario 4 - Quick Start Section
 *
 * Tests:
 * - Quick Start section DOM structure
 * - Terminal window chrome (title bar, buttons)
 * - Code block with memcached commands (SET, GET, DELETE)
 * - Syntax highlighting CSS classes
 * - Copy button presence and attributes
 * - Terminal styling (dark background, monospace font)
 * - Accessibility attributes
 */

const fs = require('fs');
const path = require('path');

const htmlPath = path.join(__dirname, '../../index.html');
const html = fs.readFileSync(htmlPath, 'utf-8');

const baseCssPath = path.join(__dirname, '../../css/base.css');
const quickstartCssPath = path.join(__dirname, '../../css/quickstart.css');
const baseCss = fs.readFileSync(baseCssPath, 'utf-8');
const quickstartCss = fs.readFileSync(quickstartCssPath, 'utf-8');

describe('Quick Start Section Structure', () => {
  beforeEach(() => {
    document.body.innerHTML = html;
  });

  test('quickstart section exists with correct id', () => {
    const quickstart = document.querySelector('section#quickstart');
    expect(quickstart).toBeTruthy();
  });

  test('quickstart section has a heading', () => {
    const quickstart = document.querySelector('section#quickstart');
    const heading = quickstart.querySelector('h2');
    expect(heading).toBeTruthy();
  });

  test('quickstart heading text contains "Quick Start"', () => {
    const quickstart = document.querySelector('section#quickstart');
    const heading = quickstart.querySelector('h2');
    expect(heading.textContent.trim()).toMatch(/Quick Start/i);
  });

  test('quickstart section has description text', () => {
    const quickstart = document.querySelector('section#quickstart');
    const desc = quickstart.querySelector('.quickstart-description');
    expect(desc).toBeTruthy();
  });

  test('quickstart description has non-empty text', () => {
    const quickstart = document.querySelector('section#quickstart');
    const desc = quickstart.querySelector('.quickstart-description');
    expect(desc.textContent.trim().length).toBeGreaterThan(0);
  });

  test('quickstart section contains a terminal window', () => {
    const quickstart = document.querySelector('section#quickstart');
    const terminal = quickstart.querySelector('.terminal-window');
    expect(terminal).toBeTruthy();
  });

  test('terminal window has role="region" and aria-label', () => {
    const quickstart = document.querySelector('section#quickstart');
    const terminal = quickstart.querySelector('.terminal-window');
    expect(terminal.getAttribute('role')).toBe('region');
    expect(terminal.getAttribute('aria-label')).toBeTruthy();
  });
});

describe('Terminal Window Chrome', () => {
  beforeEach(() => {
    document.body.innerHTML = html;
  });

  test('terminal has a header with buttons', () => {
    const quickstart = document.querySelector('section#quickstart');
    const header = quickstart.querySelector('.terminal-header');
    expect(header).toBeTruthy();
    const buttons = header.querySelector('.terminal-buttons');
    expect(buttons).toBeTruthy();
  });

  test('terminal has three colored buttons', () => {
    const quickstart = document.querySelector('section#quickstart');
    const header = quickstart.querySelector('.terminal-header');
    const redBtn = header.querySelector('.terminal-btn-red');
    const yellowBtn = header.querySelector('.terminal-btn-yellow');
    const greenBtn = header.querySelector('.terminal-btn-green');
    expect(redBtn).toBeTruthy();
    expect(yellowBtn).toBeTruthy();
    expect(greenBtn).toBeTruthy();
  });

  test('terminal buttons are hidden from screen readers', () => {
    const quickstart = document.querySelector('section#quickstart');
    const buttons = quickstart.querySelectorAll('.terminal-btn');
    buttons.forEach((btn) => {
      expect(btn.getAttribute('aria-hidden')).toBe('true');
    });
  });

  test('terminal has a title', () => {
    const quickstart = document.querySelector('section#quickstart');
    const title = quickstart.querySelector('.terminal-title');
    expect(title).toBeTruthy();
    expect(title.textContent.trim().length).toBeGreaterThan(0);
  });
});

describe('Code Block Content', () => {
  beforeEach(() => {
    document.body.innerHTML = html;
  });

  test('code block contains SET command', () => {
    const quickstart = document.querySelector('section#quickstart');
    const code = quickstart.querySelector('#quickstart-code');
    expect(code.textContent).toMatch(/set\s+mykey/i);
  });

  test('code block contains GET command', () => {
    const quickstart = document.querySelector('section#quickstart');
    const code = quickstart.querySelector('#quickstart-code');
    expect(code.textContent).toMatch(/get\s+mykey/i);
  });

  test('code block contains DELETE command', () => {
    const quickstart = document.querySelector('section#quickstart');
    const code = quickstart.querySelector('#quickstart-code');
    expect(code.textContent).toMatch(/delete\s+mykey/i);
  });

  test('code block shows STORED response', () => {
    const quickstart = document.querySelector('section#quickstart');
    const code = quickstart.querySelector('#quickstart-code');
    expect(code.textContent).toMatch(/STORED/);
  });

  test('code block shows DELETED response', () => {
    const quickstart = document.querySelector('section#quickstart');
    const code = quickstart.querySelector('#quickstart-code');
    expect(code.textContent).toMatch(/DELETED/);
  });

  test('code block has a pre element with code child', () => {
    const quickstart = document.querySelector('section#quickstart');
    const pre = quickstart.querySelector('.terminal-body pre');
    expect(pre).toBeTruthy();
    const code = pre.querySelector('code');
    expect(code).toBeTruthy();
  });
});

describe('Syntax Highlighting', () => {
  beforeEach(() => {
    document.body.innerHTML = html;
  });

  test('SET keyword has sh-keyword class', () => {
    const quickstart = document.querySelector('section#quickstart');
    const keyword = quickstart.querySelector('span.sh-keyword');
    expect(keyword).toBeTruthy();
  });

  test('prompt has sh-prompt class', () => {
    const quickstart = document.querySelector('section#quickstart');
    const prompt = quickstart.querySelector('span.sh-prompt');
    expect(prompt).toBeTruthy();
  });

  test('comment has sh-comment class', () => {
    const quickstart = document.querySelector('section#quickstart');
    const comment = quickstart.querySelector('span.sh-comment');
    expect(comment).toBeTruthy();
  });

  test('response has sh-response class', () => {
    const quickstart = document.querySelector('section#quickstart');
    const response = quickstart.querySelector('span.sh-response');
    expect(response).toBeTruthy();
  });
});

describe('Copy Button', () => {
  beforeEach(() => {
    document.body.innerHTML = html;
  });

  test('copy button exists inside terminal body', () => {
    const quickstart = document.querySelector('section#quickstart');
    const button = quickstart.querySelector('.copy-button');
    expect(button).toBeTruthy();
  });

  test('copy button has correct aria-label', () => {
    const quickstart = document.querySelector('section#quickstart');
    const button = quickstart.querySelector('.copy-button');
    expect(button.getAttribute('aria-label')).toMatch(/copy/i);
  });

  test('copy button has data-copy-target pointing to quickstart-code', () => {
    const quickstart = document.querySelector('section#quickstart');
    const button = quickstart.querySelector('.copy-button');
    expect(button.getAttribute('data-copy-target')).toBe('quickstart-code');
  });

  test('copy button has type="button"', () => {
    const quickstart = document.querySelector('section#quickstart');
    const button = quickstart.querySelector('.copy-button');
    expect(button.getAttribute('type')).toBe('button');
  });

  test('copy button contains copy icon svg', () => {
    const quickstart = document.querySelector('section#quickstart');
    const button = quickstart.querySelector('.copy-button');
    const icon = button.querySelector('.copy-icon');
    expect(icon).toBeTruthy();
  });

  test('copy button contains success icon svg', () => {
    const quickstart = document.querySelector('section#quickstart');
    const button = quickstart.querySelector('.copy-button');
    const icon = button.querySelector('.copy-success-icon');
    expect(icon).toBeTruthy();
  });

  test('copy button contains tooltip element', () => {
    const quickstart = document.querySelector('section#quickstart');
    const button = quickstart.querySelector('.copy-button');
    const tooltip = button.querySelector('.copy-tooltip');
    expect(tooltip).toBeTruthy();
    expect(tooltip.getAttribute('role')).toBe('status');
    expect(tooltip.getAttribute('aria-live')).toBe('polite');
  });
});

describe('Quick Start Terminal Styling', () => {
  test('CSS defines terminal-window with dark background', () => {
    expect(quickstartCss).toMatch(/\.terminal-window/);
    expect(quickstartCss).toMatch(/background-color:\s*#1e1e2e/);
  });

  test('CSS defines terminal header with buttons', () => {
    expect(quickstartCss).toMatch(/\.terminal-header/);
    expect(quickstartCss).toMatch(/\.terminal-btn/);
    expect(quickstartCss).toMatch(/\.terminal-btn-red/);
    expect(quickstartCss).toMatch(/\.terminal-btn-yellow/);
    expect(quickstartCss).toMatch(/\.terminal-btn-green/);
  });

  test('CSS uses monospace font for terminal body', () => {
    expect(quickstartCss).toMatch(/font-family:\s*var\(--font-mono/);
  });

  test('CSS defines syntax highlighting colors', () => {
    expect(quickstartCss).toMatch(/\.sh-keyword/);
    expect(quickstartCss).toMatch(/\.sh-prompt/);
    expect(quickstartCss).toMatch(/\.sh-comment/);
    expect(quickstartCss).toMatch(/\.sh-response/);
  });

  test('CSS defines copy button styles', () => {
    expect(quickstartCss).toMatch(/\.copy-button/);
    expect(quickstartCss).toMatch(/position:\s*absolute/);
  });

  test('CSS defines copy-success state for feedback', () => {
    expect(quickstartCss).toMatch(/\.copy-button\.copy-success/);
  });

  test('CSS defines tooltip styling', () => {
    expect(quickstartCss).toMatch(/\.copy-tooltip/);
  });

  test('CSS defines focus-visible outline for accessibility', () => {
    expect(quickstartCss).toMatch(/:focus-visible/);
  });
});
