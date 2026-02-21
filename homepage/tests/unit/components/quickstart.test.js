/**
 * Quick Start Section Unit Tests
 * Owner: Scenario 4 - Quick Start Section
 *
 * Tests for:
 * - Installation command (cargo install mirdb)
 * - Server startup command (mirdb-server --addr ...)
 * - Set command example (set mykey 0 0 5)
 * - Get command example (get mykey)
 * - Code block syntax highlighting
 */

const fs = require('fs');
const path = require('path');

describe('Quick Start Section', () => {
  let indexContent;

  beforeAll(() => {
    const filePath = path.join(__dirname, '../../../src/index.md');
    indexContent = fs.readFileSync(filePath, 'utf8');
  });

  describe('Section Structure', () => {
    test('Quick Start section exists with correct id', () => {
      expect(indexContent).toMatch(/<section[^>]*id="quickstart"[^>]*class="quickstart"[^>]*>/);
    });

    test('Quick Start has h2 heading', () => {
      expect(indexContent).toMatch(/<h2>Quick Start<\/h2>/);
    });

    test('Quick Start has introduction paragraph', () => {
      expect(indexContent).toMatch(/<p[^>]*class="quickstart-intro"[^>]*>/);
    });
  });

  describe('Test Case 1: Installation Command', () => {
    test('Section displays cargo install mirdb command', () => {
      expect(indexContent).toMatch(/cargo install mirdb/);
    });

    test('Installation step has correct heading', () => {
      expect(indexContent).toMatch(/<h3>1\. Install MirDB<\/h3>/);
    });

    test('Installation command is in a code block', () => {
      // Verify the cargo install command is within a code element
      expect(indexContent).toMatch(/<code[^>]*>cargo install mirdb<\/code>/);
    });
  });

  describe('Test Case 2: Server Startup Command', () => {
    test('Section displays mirdb-server startup command with example flags', () => {
      expect(indexContent).toMatch(/mirdb-server --addr 0\.0\.0\.0:12333 --dir \/tmp\/mirdb/);
    });

    test('Server startup step has correct heading', () => {
      expect(indexContent).toMatch(/<h3>2\. Start the Server<\/h3>/);
    });

    test('Server command is in a code block', () => {
      expect(indexContent).toMatch(/<code[^>]*>mirdb-server --addr/);
    });
  });

  describe('Test Case 3: Set Command Example', () => {
    test('Section displays set command example: set mykey 0 0 5', () => {
      expect(indexContent).toMatch(/set mykey 0 0 5/);
    });

    test('Set command includes data value (hello)', () => {
      expect(indexContent).toMatch(/set mykey 0 0 5[\s\S]*?hello/);
    });

    test('Set command shows STORED response', () => {
      expect(indexContent).toMatch(/STORED/);
    });
  });

  describe('Test Case 4: Get Command Example', () => {
    test('Section displays get command example: get mykey', () => {
      expect(indexContent).toMatch(/get mykey/);
    });

    test('Get command shows VALUE response', () => {
      expect(indexContent).toMatch(/VALUE mykey 0 5/);
    });

    test('Get command shows END response', () => {
      expect(indexContent).toMatch(/END/);
    });
  });

  describe('Test Case 6: Code Blocks with Syntax Highlighting', () => {
    test('Code blocks have data-language attribute for syntax highlighting', () => {
      expect(indexContent).toMatch(/data-language="bash"/);
    });

    test('Code blocks have language-specific classes', () => {
      // Check for language classes on code elements
      expect(indexContent).toMatch(/class="language-bash"/);
    });

    test('Code blocks have syntax highlighting token spans', () => {
      // Check for token spans used in syntax highlighting
      expect(indexContent).toMatch(/class="token-command"/);
      expect(indexContent).toMatch(/class="token-response"/);
    });

    test('Comments have token-comment class', () => {
      expect(indexContent).toMatch(/class="token-comment"/);
    });
  });

  describe('Copy Button Structure', () => {
    test('Code blocks have copy buttons', () => {
      expect(indexContent).toMatch(/<button[^>]*class="copy-btn"[^>]*>/);
    });

    test('Copy buttons have aria-label for accessibility', () => {
      expect(indexContent).toMatch(/aria-label="Copy to clipboard"/);
    });

    test('Copy buttons have copy icon and text', () => {
      expect(indexContent).toMatch(/<span[^>]*class="copy-icon"[^>]*>/);
      expect(indexContent).toMatch(/<span[^>]*class="copy-text"[^>]*>Copy<\/span>/);
    });
  });

  describe('Code Block Container Structure', () => {
    test('Code blocks are wrapped in div.code-block', () => {
      expect(indexContent).toMatch(/<div[^>]*class="code-block"[^>]*>/);
    });

    test('Multiple code blocks exist for different steps', () => {
      const codeBlockMatches = indexContent.match(/<div[^>]*class="code-block"/g);
      expect(codeBlockMatches).not.toBeNull();
      expect(codeBlockMatches.length).toBeGreaterThanOrEqual(4);
    });
  });

  describe('Step-by-Step Structure', () => {
    test('Has quickstart-step divs for each step', () => {
      const stepMatches = indexContent.match(/<div[^>]*class="quickstart-step"/g);
      expect(stepMatches).not.toBeNull();
      expect(stepMatches.length).toBeGreaterThanOrEqual(4);
    });

    test('Steps are numbered correctly', () => {
      expect(indexContent).toMatch(/<h3>1\. Install MirDB<\/h3>/);
      expect(indexContent).toMatch(/<h3>2\. Start the Server<\/h3>/);
      expect(indexContent).toMatch(/<h3>3\. Connect and Use<\/h3>/);
      expect(indexContent).toMatch(/<h3>4\. Store and Retrieve Data<\/h3>/);
    });
  });

  describe('Telnet Connection Example', () => {
    test('Section includes telnet connection command', () => {
      expect(indexContent).toMatch(/telnet localhost 12333/);
    });
  });
});
