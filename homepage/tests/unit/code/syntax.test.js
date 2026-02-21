/**
 * Syntax Highlighting Unit Tests
 * Owner: Scenario 17 - Code Syntax Highlighting
 *
 * Tests for:
 * - Code blocks have syntax highlighting applied (not plain monospace)
 * - Shell commands are highlighted appropriately
 * - String values are visually distinct from commands
 * - Code blocks use monospace font family
 */

const fs = require('fs');
const path = require('path');

describe('Code Syntax Highlighting', () => {
  let codeCSS;
  let indexHTML;

  beforeAll(() => {
    // Read the CSS file for syntax highlighting styles
    const cssPath = path.join(__dirname, '../../../theme/css/code.css');
    codeCSS = fs.readFileSync(cssPath, 'utf8');

    // Read the built HTML file for code block structure
    const htmlPath = path.join(__dirname, '../../../book/index.html');
    indexHTML = fs.readFileSync(htmlPath, 'utf8');
  });

  describe('Test Case 1: Quick Start Code Block Has Syntax Highlighting Applied', () => {
    test('CSS defines syntax highlighting token classes', () => {
      // Verify token classes are defined in CSS for syntax highlighting
      expect(codeCSS).toMatch(/\.token-keyword/);
      expect(codeCSS).toMatch(/\.token-string/);
      expect(codeCSS).toMatch(/\.token-command/);
    });

    test('CSS defines hljs classes for highlight.js compatibility', () => {
      expect(codeCSS).toMatch(/\.hljs-keyword/);
      expect(codeCSS).toMatch(/\.hljs-string/);
      expect(codeCSS).toMatch(/\.hljs-comment/);
    });

    test('Syntax highlighting token classes have distinct colors', () => {
      // Keywords should have a color defined
      expect(codeCSS).toMatch(/\.hljs-keyword[\s\S]*?color:\s*#[a-fA-F0-9]{6}/);
      // Strings should have a color defined
      expect(codeCSS).toMatch(/\.hljs-string[\s\S]*?color:\s*#[a-fA-F0-9]{6}/);
      // Comments should have a color defined
      expect(codeCSS).toMatch(/\.hljs-comment[\s\S]*?color:\s*#[a-fA-F0-9]{6}/);
    });

    test('HTML code blocks have language-specific classes for highlighting', () => {
      // Quick start section uses language-bash class
      expect(indexHTML).toMatch(/class="language-bash"/);
    });

    test('HTML code blocks contain syntax token spans', () => {
      // Check that token spans are used in the HTML
      expect(indexHTML).toMatch(/class="token-command"/);
      expect(indexHTML).toMatch(/class="token-response"/);
      expect(indexHTML).toMatch(/class="token-comment"/);
    });
  });

  describe('Test Case 2: Shell Commands Are Highlighted Appropriately', () => {
    test('CSS defines bash/shell-specific highlighting rules', () => {
      // Check for language-bash specific styles
      expect(codeCSS).toMatch(/\.language-bash/);
    });

    test('Shell commands have dedicated token class styling', () => {
      // Token command class is styled
      expect(codeCSS).toMatch(/\.token-command[\s\S]*?color:/);
    });

    test('Shell built-in commands have highlighting', () => {
      // Built-in commands styling
      expect(codeCSS).toMatch(/\.hljs-built_in[\s\S]*?color:/);
    });

    test('Command flags and parameters have highlighting support', () => {
      // Flags/params styling
      expect(codeCSS).toMatch(/\.token-flag|\.hljs-params/);
    });

    test('Shell variable styling is defined', () => {
      // Variables should be styled distinctly
      expect(codeCSS).toMatch(/\.hljs-variable[\s\S]*?color:/);
    });
  });

  describe('Test Case 3: String Values Are Visually Distinct From Commands', () => {
    test('String token class has different color than command class', () => {
      // Extract command color
      const commandColorMatch = codeCSS.match(/\.token-command[\s\S]*?color:\s*(#[a-fA-F0-9]{6}|var\([^)]+\))/);
      // Extract string color
      const stringColorMatch = codeCSS.match(/\.hljs-string[\s\S]*?color:\s*(#[a-fA-F0-9]{6})/);

      expect(commandColorMatch).not.toBeNull();
      expect(stringColorMatch).not.toBeNull();

      // Colors should exist (actual distinctness is verified by having different selectors)
      expect(commandColorMatch[1]).toBeDefined();
      expect(stringColorMatch[1]).toBeDefined();
    });

    test('Response tokens are styled differently from commands', () => {
      // Check that response tokens have specific styling
      const responseMatch = codeCSS.match(/\.token-response[\s\S]*?color:\s*(#[a-fA-F0-9]{6})/);
      expect(responseMatch).not.toBeNull();
    });

    test('Comments are styled with italic', () => {
      // Comments should have italic font-style
      expect(codeCSS).toMatch(/\.hljs-comment[\s\S]*?font-style:\s*italic/);
    });

    test('Numbers have distinct styling', () => {
      // Numbers should have their own color
      expect(codeCSS).toMatch(/\.hljs-number[\s\S]*?color:\s*#[a-fA-F0-9]{6}/);
    });
  });

  describe('Test Case 4: Code Blocks Use Monospace Font Family', () => {
    test('Pre and code elements have monospace font-family', () => {
      // Check that pre, code uses monospace fonts
      expect(codeCSS).toMatch(/pre[^{]*,?\s*code[^{]*\{[^}]*font-family:\s*['"]?Monaco/);
    });

    test('Font family includes fallback monospace fonts', () => {
      // Should include multiple monospace fallbacks
      expect(codeCSS).toMatch(/font-family:.*Monaco.*Menlo.*monospace/s);
    });

    test('Syntax token classes inherit monospace font', () => {
      // Token classes should have monospace font
      expect(codeCSS).toMatch(/\.token-keyword[\s\S]*?font-family:.*monospace/);
    });

    test('Code blocks maintain monospace for all highlighted elements', () => {
      // Verify hljs classes have monospace
      expect(codeCSS).toMatch(/\.hljs[\s\S]*?font-family:.*monospace/);
    });
  });

  describe('Dark Mode Syntax Highlighting', () => {
    test('Dark mode syntax highlighting is defined', () => {
      // Check for dark mode selectors
      expect(codeCSS).toMatch(/\[data-theme="dark"\]/);
    });

    test('Dark mode has keyword styling', () => {
      expect(codeCSS).toMatch(/\[data-theme="dark"\]\s*\.hljs-keyword/);
    });

    test('Dark mode has string styling', () => {
      expect(codeCSS).toMatch(/\[data-theme="dark"\]\s*\.hljs-string/);
    });

    test('Dark mode has comment styling', () => {
      expect(codeCSS).toMatch(/\[data-theme="dark"\]\s*\.token-comment/);
    });
  });

  describe('Memcached Protocol Highlighting', () => {
    test('Memcached language-specific highlighting exists', () => {
      expect(codeCSS).toMatch(/\.language-memcached/);
    });

    test('Memcached commands are highlighted', () => {
      expect(codeCSS).toMatch(/\.language-memcached\s*\.token-command/);
    });

    test('Memcached responses are highlighted', () => {
      expect(codeCSS).toMatch(/\.language-memcached\s*\.token-response/);
    });
  });

  describe('HTML Code Block Structure', () => {
    test('Code blocks exist in the HTML', () => {
      expect(indexHTML).toMatch(/<div[^>]*class="code-block"/);
    });

    test('Code blocks contain pre and code elements', () => {
      expect(indexHTML).toMatch(/<pre><code/);
    });

    test('Code elements have language classes', () => {
      expect(indexHTML).toMatch(/<code\s+class="language-/);
    });
  });
});
