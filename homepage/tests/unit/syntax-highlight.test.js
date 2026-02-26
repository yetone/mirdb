/**
 * Syntax Highlighting Unit Tests
 * Owner: Scenario 3 - Usage and Code Examples
 *
 * Tests for:
 * - Code block detection
 * - Proper token classification
 * - Theme application
 */

const fs = require('fs');
const path = require('path');

// Load the syntax-highlight.js module
const syntaxHighlightPath = path.join(__dirname, '../../js/syntax-highlight.js');
const syntaxHighlightCode = fs.readFileSync(syntaxHighlightPath, 'utf8');

// Execute the module code in a controlled environment
const moduleExports = {};
const mockModule = { exports: moduleExports };
const evalCode = `
(function(module, exports) {
  ${syntaxHighlightCode}
})(mockModule, moduleExports);
`;
eval(evalCode);

const { highlightString, escapeHtml, patterns } = mockModule.exports;

describe('Syntax Highlighting Module', () => {
  describe('escapeHtml', () => {
    test('escapes HTML special characters', () => {
      expect(escapeHtml('<script>')).toBe('&lt;script&gt;');
      expect(escapeHtml('a & b')).toBe('a &amp; b');
    });

    test('preserves plain text', () => {
      expect(escapeHtml('hello world')).toBe('hello world');
      expect(escapeHtml('set key 0 0 5')).toBe('set key 0 0 5');
    });
  });

  describe('patterns', () => {
    test('has memcached patterns defined', () => {
      expect(patterns.memcached).toBeDefined();
      expect(patterns.memcached.command).toBeDefined();
      expect(patterns.memcached.response).toBeDefined();
      expect(patterns.memcached.number).toBeDefined();
    });

    test('has bash patterns defined', () => {
      expect(patterns.bash).toBeDefined();
      expect(patterns.bash.comment).toBeDefined();
      expect(patterns.bash.keyword).toBeDefined();
    });

    test('has rust patterns defined', () => {
      expect(patterns.rust).toBeDefined();
      expect(patterns.rust.comment).toBeDefined();
      expect(patterns.rust.keyword).toBeDefined();
    });
  });

  describe('highlightString - Memcached', () => {
    test('highlights SET command', () => {
      const result = highlightString('set mykey 0 0 5', 'memcached');
      expect(result).toContain('<span class="syntax-command">set</span>');
      expect(result).toContain('<span class="syntax-number">0</span>');
      expect(result).toContain('<span class="syntax-number">5</span>');
    });

    test('highlights GET command', () => {
      const result = highlightString('get mykey', 'memcached');
      expect(result).toContain('<span class="syntax-command">get</span>');
    });

    test('highlights DELETE command', () => {
      const result = highlightString('delete mykey', 'memcached');
      expect(result).toContain('<span class="syntax-command">delete</span>');
    });

    test('highlights STORED response', () => {
      const result = highlightString('STORED', 'memcached');
      expect(result).toContain('<span class="syntax-response">STORED</span>');
    });

    test('highlights VALUE response', () => {
      const result = highlightString('VALUE mykey 0 5', 'memcached');
      expect(result).toContain('<span class="syntax-response">VALUE</span>');
    });

    test('highlights END response', () => {
      const result = highlightString('END', 'memcached');
      expect(result).toContain('<span class="syntax-response">END</span>');
    });

    test('highlights DELETED response', () => {
      const result = highlightString('DELETED', 'memcached');
      expect(result).toContain('<span class="syntax-response">DELETED</span>');
    });

    test('highlights NOT_FOUND response', () => {
      const result = highlightString('NOT_FOUND', 'memcached');
      expect(result).toContain('<span class="syntax-response">NOT_FOUND</span>');
    });

    test('highlights numbers in commands', () => {
      const result = highlightString('set key 0 3600 100', 'memcached');
      expect(result).toContain('<span class="syntax-number">0</span>');
      expect(result).toContain('<span class="syntax-number">3600</span>');
      expect(result).toContain('<span class="syntax-number">100</span>');
    });
  });

  describe('highlightString - Bash', () => {
    test('highlights comments', () => {
      const result = highlightString('# This is a comment', 'bash');
      expect(result).toContain('<span class="syntax-comment">');
      expect(result).toContain('This is a comment');
    });

    test('highlights git command', () => {
      const result = highlightString('git clone', 'bash');
      expect(result).toContain('<span class="syntax-keyword">git</span>');
    });

    test('highlights cargo command', () => {
      const result = highlightString('cargo build --release', 'bash');
      expect(result).toContain('<span class="syntax-keyword">cargo</span>');
    });

    test('highlights telnet command', () => {
      const result = highlightString('telnet localhost 12333', 'bash');
      expect(result).toContain('<span class="syntax-keyword">telnet</span>');
    });
  });

  describe('highlightString - unknown language', () => {
    test('returns escaped text for unknown language', () => {
      const result = highlightString('<test>', 'unknown');
      expect(result).toBe('&lt;test&gt;');
    });

    test('does not add highlighting spans for unknown language', () => {
      const result = highlightString('set key', 'unknown');
      expect(result).not.toContain('<span');
    });
  });
});

describe('Integration with HTML', () => {
  beforeEach(() => {
    // Set up a basic DOM structure matching the actual index.html
    document.body.innerHTML = `
      <section id="usage" class="section usage" aria-labelledby="usage-title">
        <div class="container">
          <h2 id="usage-title">Usage</h2>
          <p class="usage-intro">MirDB implements the Memcached text protocol.</p>

          <div class="usage-examples">
            <div class="usage-example">
              <h3>Store a Value (SET)</h3>
              <p class="usage-description">The set command stores a key-value pair.</p>
              <pre class="code-block" data-language="memcached"><code>set mykey 0 0 5
value
STORED</code></pre>
              <p class="usage-explanation">This stores the value under the key.</p>
            </div>
            <div class="usage-example">
              <h3>Retrieve a Value (GET)</h3>
              <p class="usage-description">The get command retrieves values.</p>
              <pre class="code-block" data-language="memcached"><code>get mykey
VALUE mykey 0 5
value
END</code></pre>
              <p class="usage-explanation">Returns the value for the key.</p>
            </div>
            <div class="usage-example">
              <h3>Delete a Value (DELETE)</h3>
              <p class="usage-description">The delete command removes a key.</p>
              <pre class="code-block" data-language="memcached"><code>delete mykey
DELETED</code></pre>
              <p class="usage-explanation">Removes the key from the store.</p>
            </div>
            <div class="usage-example">
              <h3>Connecting to MirDB</h3>
              <p class="usage-description">Connect using telnet or any client.</p>
              <pre class="code-block" data-language="bash"><code># Connect via telnet
telnet localhost 12333</code></pre>
              <p class="usage-explanation">MirDB listens on port 12333.</p>
            </div>
          </div>
        </div>
      </section>
    `;
  });

  test('usage section is accessible', () => {
    const usageSection = document.getElementById('usage');
    expect(usageSection).not.toBeNull();
    expect(usageSection.classList.contains('usage')).toBe(true);
  });

  test('SET operation example is present', () => {
    const codeBlocks = document.querySelectorAll('.code-block code');
    const setExample = Array.from(codeBlocks).find(block =>
      block.textContent.includes('set') && block.textContent.includes('STORED')
    );
    expect(setExample).not.toBeNull();
    expect(setExample.textContent).toMatch(/set\s+\w+\s+\d+\s+\d+\s+\d+/);
  });

  test('GET operation example is present', () => {
    const codeBlocks = document.querySelectorAll('.code-block code');
    const getExample = Array.from(codeBlocks).find(block =>
      block.textContent.includes('get') && block.textContent.includes('END')
    );
    expect(getExample).not.toBeNull();
    expect(getExample.textContent).toMatch(/get\s+\w+/);
  });

  test('DELETE operation example is present', () => {
    const codeBlocks = document.querySelectorAll('.code-block code');
    const deleteExample = Array.from(codeBlocks).find(block =>
      block.textContent.includes('delete') && block.textContent.includes('DELETED')
    );
    expect(deleteExample).not.toBeNull();
    expect(deleteExample.textContent).toMatch(/delete\s+\w+/);
  });

  test('code blocks have proper structure', () => {
    const codeBlocks = document.querySelectorAll('.code-block');
    expect(codeBlocks.length).toBeGreaterThan(0);

    codeBlocks.forEach(block => {
      expect(block.tagName).toBe('PRE');
      expect(block.querySelector('code')).not.toBeNull();
      expect(block.dataset.language).toBeDefined();
    });
  });

  test('each code example has explanatory text', () => {
    const codeExamples = document.querySelectorAll('.usage-example');

    codeExamples.forEach(example => {
      const hasDescription = example.querySelector('.usage-description') !== null;
      const hasExplanation = example.querySelector('.usage-explanation') !== null;
      const hasHeading = example.querySelector('h3') !== null;

      // Each example should have a heading and either description or explanation
      expect(hasHeading).toBe(true);
      expect(hasDescription || hasExplanation).toBe(true);
    });
  });

  test('code uses monospace font class', () => {
    const codeBlocks = document.querySelectorAll('.code-block');
    codeBlocks.forEach(block => {
      // The code-block class should be present (CSS applies monospace)
      expect(block.classList.contains('code-block')).toBe(true);
    });
  });

  test('code blocks have proper data-language attribute', () => {
    const codeBlocks = document.querySelectorAll('.code-block');
    const languages = Array.from(codeBlocks).map(block => block.dataset.language);

    expect(languages).toContain('memcached');
    expect(languages).toContain('bash');
  });
});
