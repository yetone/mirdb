/**
 * Tests for Quick Start Section
 * Verifies REQ-4, REQ-5, US-3
 */

describe('Quick Start Section', () => {
  let quickStartSection;

  beforeAll(() => {
    quickStartSection = document.getElementById('quick-start') ||
                        document.querySelector('[data-section="quick-start"]') ||
                        document.querySelector('.quick-start');
  });

  describe('Test Case 1: Installation Instructions', () => {
    test('Quick start section exists and contains installation command', () => {
      expect(quickStartSection).not.toBeNull();

      const codeBlocks = quickStartSection.querySelectorAll('pre, code');

      let hasInstallCommand = false;
      codeBlocks.forEach(block => {
        const text = block.textContent.toLowerCase();
        if (text.includes('cargo install') ||
            text.includes('cargo run') ||
            text.includes('git clone') ||
            text.includes('install')) {
          hasInstallCommand = true;
        }
      });

      expect(hasInstallCommand).toBe(true);
    });
  });

  describe('Test Case 2: SET Operation Example', () => {
    test('At least one code block shows SET operation', () => {
      const codeBlocks = quickStartSection.querySelectorAll('pre, code');

      let hasSetOperation = false;
      codeBlocks.forEach(block => {
        const text = block.textContent;
        // Check for SET operation in various forms
        if (text.includes('set(') ||
            text.includes('.set(') ||
            text.includes('SET ') ||
            text.includes("set('") ||
            text.includes('set("') ||
            text.includes('set ')) {
          hasSetOperation = true;
        }
      });

      expect(hasSetOperation).toBe(true);
    });
  });

  describe('Test Case 3: GET Operation Example', () => {
    test('At least one code block shows GET operation', () => {
      const codeBlocks = quickStartSection.querySelectorAll('pre, code');

      let hasGetOperation = false;
      codeBlocks.forEach(block => {
        const text = block.textContent;
        // Check for GET operation in various forms
        if (text.includes('get(') ||
            text.includes('.get(') ||
            text.includes('GET ') ||
            text.includes("get('") ||
            text.includes('get("') ||
            text.includes('get ')) {
          hasGetOperation = true;
        }
      });

      expect(hasGetOperation).toBe(true);
    });
  });

  describe('Test Case 4: Code Block Formatting', () => {
    test('Code blocks use proper pre/code elements with syntax highlighting', () => {
      const codeBlocks = quickStartSection.querySelectorAll('pre code, pre[class*="language-"], code[class*="language-"]');

      expect(codeBlocks.length).toBeGreaterThan(0);

      // Check for syntax highlighting classes
      const allCodeElements = quickStartSection.querySelectorAll('pre, code');
      let hasSyntaxHighlighting = false;

      allCodeElements.forEach(el => {
        const classes = el.className;
        if (classes.includes('language-') ||
            classes.includes('hljs') ||
            classes.includes('highlight') ||
            classes.includes('code-block') ||
            classes.includes('prism')) {
          hasSyntaxHighlighting = true;
        }
      });

      expect(hasSyntaxHighlighting).toBe(true);
    });
  });

  describe('Test Case 5: Documentation Link', () => {
    test('Quick start section has link to documentation or learn more', () => {
      const links = quickStartSection.querySelectorAll('a');

      let hasDocLink = false;
      links.forEach(link => {
        const href = link.getAttribute('href') || '';
        const text = link.textContent.toLowerCase();

        if (href.includes('doc') ||
            href.includes('github') ||
            href.includes('readme') ||
            text.includes('documentation') ||
            text.includes('learn more') ||
            text.includes('full docs') ||
            text.includes('read more')) {
          hasDocLink = true;
        }
      });

      expect(hasDocLink).toBe(true);
    });
  });
});
