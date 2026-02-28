/**
 * Clipboard & Quick Start Section Tests
 * Owner: Scenario 3 - Quick Start Code Examples
 *
 * Tests for:
 * - Quick Start section presence and structure
 * - Code block with SET/GET operations
 * - Copy button functionality
 * - Syntax highlighting
 * - Horizontal scroll behavior
 *
 * Requirements: REQ-3
 * @jest-environment jsdom
 */

import { jest } from '@jest/globals';
import { readFileSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import { initClipboard, copyToClipboard, showCopyFeedback } from '../../js/components/clipboard.js';

const __dirname = dirname(fileURLToPath(import.meta.url));

describe('Quick Start Code Examples (Scenario 3)', () => {
  beforeEach(() => {
    // Load the homepage HTML
    const htmlPath = resolve(__dirname, '../../index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    document.documentElement.innerHTML = html;

    // Mock navigator.clipboard
    Object.defineProperty(navigator, 'clipboard', {
      value: {
        writeText: jest.fn().mockResolvedValue(undefined),
      },
      writable: true,
      configurable: true,
    });
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  /**
   * Test Case 1: Quick Start section exists
   * Input: Query for Quick Start section
   * Expected: Section exists with id='quickstart' or heading containing 'Quick Start'
   */
  describe('Test Case 1: Quick Start Section Existence', () => {
    test('Quick Start section exists with id="quickstart"', () => {
      const quickstartSection = document.getElementById('quickstart');
      expect(quickstartSection).not.toBeNull();
      expect(quickstartSection).toBeInTheDocument();
    });

    test('Quick Start section is a semantic section element', () => {
      const quickstartSection = document.getElementById('quickstart');
      expect(quickstartSection.tagName.toLowerCase()).toBe('section');
    });

    test('Quick Start section has aria-labelledby for accessibility', () => {
      const quickstartSection = document.getElementById('quickstart');
      expect(quickstartSection.getAttribute('aria-labelledby')).toBe('quickstart-title');
    });

    test('Quick Start section has heading containing "Quick Start"', () => {
      const quickstartSection = document.getElementById('quickstart');
      const heading = quickstartSection.querySelector('h2');
      expect(heading).not.toBeNull();
      expect(heading.textContent).toContain('Quick Start');
    });
  });

  /**
   * Test Case 2: Code block element exists
   * Input: Query for code block element
   * Expected: Pre/code element exists within Quick Start section
   */
  describe('Test Case 2: Code Block Presence', () => {
    test('pre element exists within Quick Start section', () => {
      const quickstartSection = document.getElementById('quickstart');
      const preElement = quickstartSection.querySelector('pre');
      expect(preElement).not.toBeNull();
      expect(preElement).toBeInTheDocument();
    });

    test('code element exists within pre element', () => {
      const quickstartSection = document.getElementById('quickstart');
      const codeElement = quickstartSection.querySelector('pre code');
      expect(codeElement).not.toBeNull();
      expect(codeElement).toBeInTheDocument();
    });

    test('code element has proper class for styling', () => {
      const quickstartSection = document.getElementById('quickstart');
      const codeElement = quickstartSection.querySelector('pre code');
      expect(codeElement.classList.contains('quickstart__code')).toBe(true);
    });

    test('code wrapper has role="region" for accessibility', () => {
      const quickstartSection = document.getElementById('quickstart');
      const codeWrapper = quickstartSection.querySelector('.quickstart__code-wrapper');
      expect(codeWrapper).not.toBeNull();
      expect(codeWrapper.getAttribute('role')).toBe('region');
    });
  });

  /**
   * Test Case 3: Code contains SET operation
   * Input: Check code block content for SET operation
   * Expected: Code contains 'set' or 'SET' command syntax
   */
  describe('Test Case 3: SET Operation', () => {
    test('code contains "set" command', () => {
      const quickstartSection = document.getElementById('quickstart');
      const codeElement = quickstartSection.querySelector('pre code');
      const codeText = codeElement.textContent.toLowerCase();
      expect(codeText).toContain('set');
    });

    test('code demonstrates SET with key-value pair', () => {
      const quickstartSection = document.getElementById('quickstart');
      const codeElement = quickstartSection.querySelector('pre code');
      const codeText = codeElement.textContent;
      // Should have a set operation with a key and value
      expect(codeText).toMatch(/\.set\s*\(/);
    });
  });

  /**
   * Test Case 4: Code contains GET operation
   * Input: Check code block content for GET operation
   * Expected: Code contains 'get' or 'GET' command syntax
   */
  describe('Test Case 4: GET Operation', () => {
    test('code contains "get" command', () => {
      const quickstartSection = document.getElementById('quickstart');
      const codeElement = quickstartSection.querySelector('pre code');
      const codeText = codeElement.textContent.toLowerCase();
      expect(codeText).toContain('get');
    });

    test('code demonstrates GET with key', () => {
      const quickstartSection = document.getElementById('quickstart');
      const codeElement = quickstartSection.querySelector('pre code');
      const codeText = codeElement.textContent;
      // Should have a get operation with a key
      expect(codeText).toMatch(/\.get\s*\(/);
    });
  });

  /**
   * Test Case 5: Copy button exists
   * Input: Query for copy button near code block
   * Expected: Button element exists with copy functionality (aria-label or class indicating copy)
   */
  describe('Test Case 5: Copy Button Presence', () => {
    test('copy button exists within Quick Start section', () => {
      const quickstartSection = document.getElementById('quickstart');
      const copyButton = quickstartSection.querySelector('.quickstart__copy-btn');
      expect(copyButton).not.toBeNull();
      expect(copyButton).toBeInTheDocument();
    });

    test('copy button has aria-label for accessibility', () => {
      const quickstartSection = document.getElementById('quickstart');
      const copyButton = quickstartSection.querySelector('.quickstart__copy-btn');
      const ariaLabel = copyButton.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel.toLowerCase()).toContain('copy');
    });

    test('copy button has data-copy-target attribute', () => {
      const quickstartSection = document.getElementById('quickstart');
      const copyButton = quickstartSection.querySelector('.quickstart__copy-btn');
      expect(copyButton.getAttribute('data-copy-target')).toBeTruthy();
    });

    test('copy button is a button element', () => {
      const quickstartSection = document.getElementById('quickstart');
      const copyButton = quickstartSection.querySelector('.quickstart__copy-btn');
      expect(copyButton.tagName.toLowerCase()).toBe('button');
    });

    test('copy button has type="button"', () => {
      const quickstartSection = document.getElementById('quickstart');
      const copyButton = quickstartSection.querySelector('.quickstart__copy-btn');
      expect(copyButton.getAttribute('type')).toBe('button');
    });
  });

  /**
   * Test Case 6: Copy functionality works
   * Input: Click copy button and read clipboard
   * Expected: Clipboard contains the code snippet text
   */
  describe('Test Case 6: Copy Functionality', () => {
    test('copyToClipboard function copies text using Clipboard API', async () => {
      const testText = 'Hello, MirDB!';
      const success = await copyToClipboard(testText);

      expect(success).toBe(true);
      expect(navigator.clipboard.writeText).toHaveBeenCalledWith(testText);
    });

    test('initClipboard attaches click handlers to copy buttons', () => {
      initClipboard();

      const quickstartSection = document.getElementById('quickstart');
      const copyButton = quickstartSection.querySelector('.quickstart__copy-btn');
      const clickEvent = new Event('click', { bubbles: true });

      // Spy on clipboard API
      const clipboardSpy = jest.spyOn(navigator.clipboard, 'writeText');

      copyButton.dispatchEvent(clickEvent);

      // The click handler should have been called
      expect(clipboardSpy).toHaveBeenCalled();
    });

    test('clicking copy button copies code text', async () => {
      initClipboard();

      const quickstartSection = document.getElementById('quickstart');
      const copyButton = quickstartSection.querySelector('.quickstart__copy-btn');
      const codeElement = document.getElementById('quickstart-code');

      const clickEvent = new Event('click', { bubbles: true });
      copyButton.dispatchEvent(clickEvent);

      // Wait for async operation
      await new Promise(resolve => setTimeout(resolve, 10));

      expect(navigator.clipboard.writeText).toHaveBeenCalledWith(codeElement.textContent);
    });
  });

  /**
   * Test Case 7: Copy button feedback
   * Input: Verify copy button feedback
   * Expected: Button shows visual feedback (text change, icon change, or tooltip) after click
   */
  describe('Test Case 7: Copy Button Feedback', () => {
    test('showCopyFeedback adds success class to button', () => {
      const quickstartSection = document.getElementById('quickstart');
      const copyButton = quickstartSection.querySelector('.quickstart__copy-btn');

      showCopyFeedback(copyButton);

      expect(copyButton.classList.contains('quickstart__copy-btn--success')).toBe(true);
    });

    test('showCopyFeedback changes button text to "Copied!"', () => {
      const quickstartSection = document.getElementById('quickstart');
      const copyButton = quickstartSection.querySelector('.quickstart__copy-btn');
      const textElement = copyButton.querySelector('.quickstart__copy-text');

      showCopyFeedback(copyButton);

      expect(textElement.textContent).toBe('Copied!');
    });

    test('showCopyFeedback reverts after timeout', async () => {
      jest.useFakeTimers();

      const quickstartSection = document.getElementById('quickstart');
      const copyButton = quickstartSection.querySelector('.quickstart__copy-btn');
      const textElement = copyButton.querySelector('.quickstart__copy-text');
      const originalText = textElement.textContent;

      showCopyFeedback(copyButton);

      expect(textElement.textContent).toBe('Copied!');

      // Fast-forward 2 seconds
      jest.advanceTimersByTime(2000);

      expect(textElement.textContent).toBe(originalText);
      expect(copyButton.classList.contains('quickstart__copy-btn--success')).toBe(false);

      jest.useRealTimers();
    });
  });

  /**
   * Test Case 8: Syntax highlighting
   * Input: Check code block syntax highlighting
   * Expected: Code block has syntax highlighting classes or inline styles for different token types
   */
  describe('Test Case 8: Syntax Highlighting', () => {
    test('code block contains syntax highlighting token classes', () => {
      const quickstartSection = document.getElementById('quickstart');
      const codeElement = quickstartSection.querySelector('pre code');

      // Check for token classes
      const tokenClasses = ['token-comment', 'token-keyword', 'token-string', 'token-function', 'token-number'];
      const hasTokenClasses = tokenClasses.some(cls =>
        codeElement.querySelector(`.${cls}`) !== null
      );

      expect(hasTokenClasses).toBe(true);
    });

    test('code contains comment tokens', () => {
      const quickstartSection = document.getElementById('quickstart');
      const commentTokens = quickstartSection.querySelectorAll('.token-comment');
      expect(commentTokens.length).toBeGreaterThan(0);
    });

    test('code contains keyword tokens', () => {
      const quickstartSection = document.getElementById('quickstart');
      const keywordTokens = quickstartSection.querySelectorAll('.token-keyword');
      expect(keywordTokens.length).toBeGreaterThan(0);
    });

    test('code contains string tokens', () => {
      const quickstartSection = document.getElementById('quickstart');
      const stringTokens = quickstartSection.querySelectorAll('.token-string');
      expect(stringTokens.length).toBeGreaterThan(0);
    });

    test('code contains function tokens', () => {
      const quickstartSection = document.getElementById('quickstart');
      const functionTokens = quickstartSection.querySelectorAll('.token-function');
      expect(functionTokens.length).toBeGreaterThan(0);
    });
  });

  /**
   * Test Case 9: Horizontal scroll behavior
   * Input: Test code block horizontal scroll on narrow viewport
   * Expected: Long code lines scroll horizontally within container, no page scroll
   */
  describe('Test Case 9: Horizontal Scroll Behavior', () => {
    test('code block has overflow-x: auto style', () => {
      const quickstartSection = document.getElementById('quickstart');
      const preElement = quickstartSection.querySelector('pre');

      // Check class that should apply overflow-x: auto
      expect(preElement.classList.contains('quickstart__code-block')).toBe(true);
    });

    test('code uses white-space: pre to preserve formatting', () => {
      const quickstartSection = document.getElementById('quickstart');
      const codeElement = quickstartSection.querySelector('pre code');

      // Check class that should apply white-space: pre
      expect(codeElement.classList.contains('quickstart__code')).toBe(true);
    });

    test('code wrapper prevents overflow outside container', () => {
      const quickstartSection = document.getElementById('quickstart');
      const codeWrapper = quickstartSection.querySelector('.quickstart__code-wrapper');

      // The wrapper should have overflow: hidden at the border-radius level
      expect(codeWrapper).not.toBeNull();
    });
  });

  /**
   * Additional Tests: Code Content Validity
   */
  describe('Code Content Validity', () => {
    test('code demonstrates memcached client usage', () => {
      const quickstartSection = document.getElementById('quickstart');
      const codeElement = quickstartSection.querySelector('pre code');
      const codeText = codeElement.textContent.toLowerCase();

      expect(codeText).toContain('client');
    });

    test('code shows connection to localhost', () => {
      const quickstartSection = document.getElementById('quickstart');
      const codeElement = quickstartSection.querySelector('pre code');
      const codeText = codeElement.textContent;

      expect(codeText).toContain('localhost');
    });

    test('code has proper language indicator', () => {
      const quickstartSection = document.getElementById('quickstart');
      const langIndicator = quickstartSection.querySelector('.quickstart__code-lang');

      expect(langIndicator).not.toBeNull();
      expect(langIndicator.textContent.toLowerCase()).toContain('python');
    });

    test('code is syntactically valid Python structure', () => {
      const quickstartSection = document.getElementById('quickstart');
      const codeElement = quickstartSection.querySelector('pre code');
      const codeText = codeElement.textContent;

      // Basic Python structure validation
      expect(codeText).toContain('from');
      expect(codeText).toContain('import');
      expect(codeText).toMatch(/=\s*Client\(/);
    });
  });

  /**
   * Accessibility Tests
   */
  describe('Accessibility', () => {
    test('Quick Start section is within main element', () => {
      const main = document.querySelector('main');
      const quickstartSection = document.getElementById('quickstart');
      expect(main).not.toBeNull();
      expect(main.contains(quickstartSection)).toBe(true);
    });

    test('code wrapper has accessible label', () => {
      const quickstartSection = document.getElementById('quickstart');
      const codeWrapper = quickstartSection.querySelector('.quickstart__code-wrapper');
      expect(codeWrapper.getAttribute('aria-label')).toBeTruthy();
    });

    test('copy button icon is hidden from screen readers', () => {
      const quickstartSection = document.getElementById('quickstart');
      const copyIcon = quickstartSection.querySelector('.quickstart__copy-icon');
      expect(copyIcon.getAttribute('aria-hidden')).toBe('true');
    });
  });
});
