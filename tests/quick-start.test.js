/**
 * Quick-Start Guide Section Tests
 * Scenario: Verify that the quick-start section provides installation and basic usage instructions (REQ-3)
 */

const fs = require('fs');
const path = require('path');

// Helper to parse HTML and create a simple DOM-like interface
function parseHTML(html) {
  return {
    html,
    querySelector: function(selector) {
      // Handle id-based selectors with wildcards
      const idMatch = selector.match(/\[id\*="([^"]+)"\]/);
      if (idMatch) {
        const patterns = selector.split(',').map(s => {
          const m = s.match(/\[id\*="([^"]+)"\]/);
          return m ? m[1] : null;
        }).filter(Boolean);

        for (const pattern of patterns) {
          const regex = new RegExp(`id=["']([^"']*${pattern}[^"']*)["']`, 'i');
          if (regex.test(this.html)) {
            return { exists: true };
          }
        }
        return null;
      }
      return null;
    },
    querySelectorAll: function(selector) {
      const results = [];

      if (selector === 'h1, h2, h3, h4') {
        const headingRegex = /<(h[1-4])[^>]*>([\s\S]*?)<\/\1>/gi;
        let match;
        while ((match = headingRegex.exec(this.html)) !== null) {
          results.push({ textContent: match[2].replace(/<[^>]*>/g, '') });
        }
      } else if (selector === 'pre code, pre') {
        // Match <pre><code>...</code></pre> and <pre>...</pre>
        const preCodeRegex = /<pre[^>]*>\s*<code[^>]*>([\s\S]*?)<\/code>\s*<\/pre>/gi;
        let match;
        while ((match = preCodeRegex.exec(this.html)) !== null) {
          results.push({ textContent: match[1].replace(/<[^>]*>/g, '').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&') });
        }
      } else if (selector === 'pre code') {
        const codeRegex = /<pre[^>]*>\s*<code\s+class=["']([^"']+)["'][^>]*>([\s\S]*?)<\/code>\s*<\/pre>/gi;
        let match;
        while ((match = codeRegex.exec(this.html)) !== null) {
          results.push({
            className: match[1],
            textContent: match[2].replace(/<[^>]*>/g, '').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&')
          });
        }
      }

      return results;
    }
  };
}

describe('Quick-Start Guide Section', () => {
  let document;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '..', 'index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    document = parseHTML(html);
  });

  // Test Case 1: Quick-start section exists with appropriate heading
  test('Quick-start section exists with heading containing start, getting started, or installation', () => {
    // Look for section with id containing quick-start or getting-started
    const quickStartSection = document.querySelector(
      '[id*="quick-start"], [id*="quickstart"], [id*="getting-started"], [id*="installation"]'
    );

    // Alternatively, look for a heading with appropriate text
    const allHeadings = document.querySelectorAll('h1, h2, h3, h4');
    let foundHeading = false;
    let headingText = '';

    for (const heading of allHeadings) {
      const text = heading.textContent.toLowerCase();
      if (text.includes('start') || text.includes('getting started') || text.includes('installation')) {
        foundHeading = true;
        headingText = heading.textContent;
        break;
      }
    }

    const sectionExists = quickStartSection !== null || foundHeading;

    expect(sectionExists).toBe(true);
    if (foundHeading) {
      expect(headingText.toLowerCase()).toMatch(/start|getting started|installation/i);
    }
  });

  // Test Case 2: Installation code block contains cargo command
  test('Code block exists containing cargo command (build, install, or run)', () => {
    const codeBlocks = document.querySelectorAll('pre code, pre');
    let foundCargoCommand = false;
    let cargoCodeContent = '';

    for (const codeBlock of codeBlocks) {
      const text = codeBlock.textContent;
      if (text.includes('cargo') && (text.includes('build') || text.includes('install') || text.includes('run'))) {
        foundCargoCommand = true;
        cargoCodeContent = text;
        break;
      }
    }

    expect(foundCargoCommand).toBe(true);
    expect(cargoCodeContent).toMatch(/cargo\s+(build|install|run)/);
  });

  // Test Case 3: Usage example code block showing connection example
  test('Code block exists showing connection example (telnet, nc, or memcached client)', () => {
    const codeBlocks = document.querySelectorAll('pre code, pre');
    let foundConnectionExample = false;
    let connectionCode = '';

    for (const codeBlock of codeBlocks) {
      const text = codeBlock.textContent.toLowerCase();
      if (text.includes('telnet') || text.includes('nc ') || text.includes('netcat') ||
          text.includes('memcached') || text.includes('12333') || text.includes('set ') || text.includes('get ')) {
        foundConnectionExample = true;
        connectionCode = codeBlock.textContent;
        break;
      }
    }

    expect(foundConnectionExample).toBe(true);
    expect(connectionCode).toBeTruthy();
  });

  // Test Case 4: Code blocks have syntax highlighting (language class)
  test('Code blocks have appropriate language class (language-bash, language-rust, etc.)', () => {
    const codeBlocks = document.querySelectorAll('pre code');
    let hasLanguageClass = false;
    const languageClasses = [];

    for (const codeBlock of codeBlocks) {
      const classList = codeBlock.className;
      if (classList.includes('language-') || classList.includes('lang-')) {
        hasLanguageClass = true;
        languageClasses.push(classList);
      }
    }

    expect(hasLanguageClass).toBe(true);
    expect(languageClasses.length).toBeGreaterThan(0);

    // Check that at least one code block has a recognized language class
    const validLanguagePattern = /language-(bash|shell|rust|toml|sh|console)|lang-(bash|shell|rust|toml|sh|console)/;
    const hasValidLanguage = languageClasses.some(cls => validLanguagePattern.test(cls));
    expect(hasValidLanguage).toBe(true);
  });
});
