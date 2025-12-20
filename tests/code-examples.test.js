/**
 * Code Examples Display Tests
 *
 * These tests verify that the landing page contains code examples
 * demonstrating basic SET/GET operations with MirDB using the Memcached protocol.
 *
 * Test Cases:
 * 1. Page contains code examples section with syntax-highlighted code blocks
 * 2. Code examples include SET command usage
 * 3. Code examples include GET command usage
 * 4. Example shows connecting using standard memcached client
 *
 * @jest-environment jsdom
 */

const fs = require('fs');
const path = require('path');

describe('Code Examples Display', () => {
  let pageContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '..', 'index.html');
    const html = fs.readFileSync(htmlPath, 'utf8');
    document.documentElement.innerHTML = html;
    pageContent = document.body.textContent.toLowerCase();
  });

  /**
   * Test Case 1: Page contains code examples section with syntax-highlighted code blocks
   * Input: Search for code example section
   * Expected: Page contains code examples section with syntax-highlighted code blocks
   */
  test('TC1: should contain code examples section with syntax-highlighted code blocks', () => {
    // Check for code examples section
    const codeExamplesSection = document.querySelector('#code-examples') ||
                                document.querySelector('.code-examples') ||
                                document.querySelector('[data-section="code-examples"]') ||
                                document.querySelector('#quick-start') ||
                                document.querySelector('.quick-start');

    expect(codeExamplesSection).not.toBeNull();

    // Check for code blocks within the section
    const codeBlocks = codeExamplesSection.querySelectorAll('code, pre, .code-block, [class*="code"]');
    expect(codeBlocks.length).toBeGreaterThan(0);

    // Check for syntax highlighting elements (comments, keywords, etc.)
    const hasHighlighting = codeExamplesSection.querySelectorAll('.comment, .keyword, .string, [class*="syntax"], [class*="highlight"]').length > 0 ||
                           codeExamplesSection.innerHTML.includes('class="comment"') ||
                           codeExamplesSection.innerHTML.includes('class="keyword"');

    expect(hasHighlighting).toBe(true);
  });

  /**
   * Test Case 2: Code examples include SET command usage
   * Input: Verify SET command example
   * Expected: Code examples include SET command usage
   */
  test('TC2: should include SET command usage example', () => {
    // Look for SET command in code blocks
    const codeBlocks = document.querySelectorAll('code, pre, .code-block');
    let foundSetCommand = false;

    codeBlocks.forEach(block => {
      const blockText = block.textContent.toLowerCase();
      // SET command format: set <key> <flags> <exptime> <bytes>
      if (blockText.includes('set ') && (
        blockText.match(/set\s+\w+\s+\d+\s+\d+\s+\d+/) || // Full memcached SET syntax
        blockText.includes('set mykey') ||
        blockText.includes('set key')
      )) {
        foundSetCommand = true;
      }
    });

    expect(foundSetCommand).toBe(true);
  });

  /**
   * Test Case 3: Code examples include GET command usage
   * Input: Verify GET command example
   * Expected: Code examples include GET command usage
   */
  test('TC3: should include GET command usage example', () => {
    // Look for GET command in code blocks
    const codeBlocks = document.querySelectorAll('code, pre, .code-block');
    let foundGetCommand = false;

    codeBlocks.forEach(block => {
      const blockText = block.textContent.toLowerCase();
      // GET command format: get <key>
      if (blockText.includes('get ') && (
        blockText.match(/get\s+\w+/) ||
        blockText.includes('get mykey') ||
        blockText.includes('get key')
      )) {
        foundGetCommand = true;
      }
    });

    expect(foundGetCommand).toBe(true);
  });

  /**
   * Test Case 4: Example shows connecting using standard memcached client
   * Input: Verify memcached client connection example
   * Expected: Example shows connecting using standard memcached client
   */
  test('TC4: should show connecting using standard memcached client', () => {
    const codeBlocks = document.querySelectorAll('code, pre, .code-block');
    let foundConnectionExample = false;

    codeBlocks.forEach(block => {
      const blockText = block.textContent.toLowerCase();
      // Check for connection examples: telnet, memcached client, nc, etc.
      if (
        blockText.includes('telnet') ||
        blockText.includes('memcached') ||
        blockText.includes('localhost 12333') ||
        blockText.includes('127.0.0.1:12333') ||
        blockText.includes('nc ') ||
        blockText.includes('connect')
      ) {
        foundConnectionExample = true;
      }
    });

    expect(foundConnectionExample).toBe(true);
  });

  /**
   * Additional test: Verify code examples show the default port
   */
  test('should mention the default MirDB port (12333)', () => {
    expect(pageContent).toContain('12333');
  });

  /**
   * Additional test: Verify code examples include comments explaining the commands
   */
  test('should include comments explaining the code examples', () => {
    const commentElements = document.querySelectorAll('.comment, [class*="comment"]');
    expect(commentElements.length).toBeGreaterThan(0);

    // Check that comments explain SET and GET operations
    const allCommentText = Array.from(commentElements).map(el => el.textContent.toLowerCase()).join(' ');
    const hasSetComment = allCommentText.includes('set') || pageContent.includes('# set');
    const hasGetComment = allCommentText.includes('get') || pageContent.includes('# get');

    expect(hasSetComment || hasGetComment).toBe(true);
  });
});
