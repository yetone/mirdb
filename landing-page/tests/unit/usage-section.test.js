/**
 * Usage Section Unit Tests
 * Owner: Scenario 4 - Usage Section with Code Examples
 *
 * Tests for validating HTML structure of the Usage section
 */
const fs = require('fs');
const path = require('path');

describe('Usage Section HTML Structure', () => {
  let htmlContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');
  });

  describe('TC1: Usage section HTML structure', () => {
    test('Contains usage section with correct id', () => {
      expect(htmlContent).toMatch(/<section[^>]*id="usage"[^>]*class="usage/);
    });

    test('Contains section title "Simple as Memcached"', () => {
      expect(htmlContent).toMatch(/Simple as Memcached/);
    });

    test('Has usage__title class on section title', () => {
      expect(htmlContent).toMatch(/<h2[^>]*class="usage__title"[^>]*>/);
    });

    test('Contains code blocks with syntax highlighting structure', () => {
      // Should have code-block containers
      expect(htmlContent).toMatch(/<div[^>]*class="code-block"[^>]*>/);
      // Should have code elements with language class
      expect(htmlContent).toMatch(/<code[^>]*class="language-bash"[^>]*>/);
    });

    test('Contains copy buttons on code blocks', () => {
      // Should have copy buttons
      expect(htmlContent).toMatch(/<button[^>]*class="code-block__copy"[^>]*>/);
      // Buttons should have aria-label for accessibility
      expect(htmlContent).toMatch(/aria-label="Copy code to clipboard"/);
    });

    test('Contains code block header with language label', () => {
      expect(htmlContent).toMatch(/<div[^>]*class="code-block__header"[^>]*>/);
      expect(htmlContent).toMatch(/<span[^>]*class="code-block__language"[^>]*>/);
    });
  });

  describe('TC3: Code example content', () => {
    test('Shows Memcached SET operation', () => {
      expect(htmlContent).toMatch(/SET\s+\w+/);
      expect(htmlContent).toMatch(/STORED/);
    });

    test('Shows Memcached GET operation', () => {
      expect(htmlContent).toMatch(/GET\s+\w+/);
      expect(htmlContent).toMatch(/VALUE/);
      expect(htmlContent).toMatch(/END/);
    });

    test('Shows server start command', () => {
      expect(htmlContent).toMatch(/mirdb/);
      expect(htmlContent).toMatch(/--port/);
    });

    test('Shows telnet connection example', () => {
      expect(htmlContent).toMatch(/telnet\s+localhost\s+11211/);
    });
  });

  describe('TC4: Syntax highlighting library loaded', () => {
    test('Prism.js CSS is loaded', () => {
      expect(htmlContent).toMatch(/prism.*\.css/i);
    });

    test('Prism.js JavaScript is loaded', () => {
      expect(htmlContent).toMatch(/prism.*\.js/i);
    });

    test('Prism bash component is loaded', () => {
      expect(htmlContent).toMatch(/prism-bash.*\.js/i);
    });
  });

  describe('TC5: Usage GIF/animation present', () => {
    test('Contains usage demo container', () => {
      expect(htmlContent).toMatch(/<div[^>]*class="usage__demo"[^>]*>/);
    });

    test('Contains usage GIF image', () => {
      expect(htmlContent).toMatch(/<img[^>]*src="assets\/images\/usage\.gif"[^>]*>/);
    });

    test('Usage GIF has alt text for accessibility', () => {
      expect(htmlContent).toMatch(/<img[^>]*src="assets\/images\/usage\.gif"[^>]*alt="[^"]+"/);
    });

    test('Usage GIF has lazy loading attribute', () => {
      expect(htmlContent).toMatch(/<img[^>]*class="usage__gif"[^>]*loading="lazy"/);
    });
  });

  describe('Copy button structure', () => {
    test('Copy button has copy icon SVG', () => {
      expect(htmlContent).toMatch(/<svg[^>]*class="copy-icon"[^>]*>/);
    });

    test('Copy button has check icon SVG (initially hidden)', () => {
      expect(htmlContent).toMatch(/<svg[^>]*class="check-icon"[^>]*>/);
    });

    test('Copy button has text label', () => {
      expect(htmlContent).toMatch(/<span[^>]*class="copy-text"[^>]*>Copy<\/span>/);
    });
  });

  describe('Accessibility', () => {
    test('Code blocks use semantic pre/code elements', () => {
      expect(htmlContent).toMatch(/<pre><code/);
    });

    test('Copy buttons have type="button"', () => {
      expect(htmlContent).toMatch(/<button[^>]*class="code-block__copy"[^>]*type="button"/);
    });

    test('SVG icons are hidden from screen readers', () => {
      expect(htmlContent).toMatch(/aria-hidden="true"/);
    });
  });
});
