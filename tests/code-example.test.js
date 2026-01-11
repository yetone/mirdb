/**
 * Tests for Code Example Section
 * Verifies the code example section demonstrates memcached protocol usage correctly
 */

const fs = require('fs');
const path = require('path');

describe('Code Example Section', () => {
  let htmlContent;
  let document;

  beforeAll(() => {
    // Read the HTML file
    const htmlPath = path.join(__dirname, '..', 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');

    // Parse HTML using jsdom
    const { JSDOM } = require('jsdom');
    const dom = new JSDOM(htmlContent);
    document = dom.window.document;
  });

  describe('Test Case 1: Code block element presence', () => {
    test('should have a pre or code element with memcached command examples', () => {
      // Check for code-example section
      const codeExampleSection = document.querySelector('#code-example');
      expect(codeExampleSection).not.toBeNull();

      // Check for code block
      const codeBlock = document.querySelector('.code-block');
      expect(codeBlock).not.toBeNull();

      // Check for pre and code elements
      const preElement = document.querySelector('.code-content pre');
      expect(preElement).not.toBeNull();

      const codeElement = document.querySelector('.code-content code');
      expect(codeElement).not.toBeNull();

      // Verify code content exists
      const codeContent = codeElement.textContent;
      expect(codeContent.length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 2: Set command verification', () => {
    test('should contain set mykey or similar set command', () => {
      const codeElement = document.querySelector('.code-content code');
      expect(codeElement).not.toBeNull();

      const codeContent = codeElement.textContent;

      // Check for 'set' command in the code example
      const hasSetCommand = codeContent.includes('set mykey') ||
                           codeContent.includes('set ');
      expect(hasSetCommand).toBe(true);

      // More specifically, check for the memcached set syntax
      // set <key> <flags> <exptime> <bytes>
      const setPattern = /set\s+\w+\s+\d+\s+\d+\s+\d+/;
      expect(setPattern.test(codeContent)).toBe(true);
    });
  });

  describe('Test Case 3: Get command verification', () => {
    test('should contain get mykey or similar get command', () => {
      const codeElement = document.querySelector('.code-content code');
      expect(codeElement).not.toBeNull();

      const codeContent = codeElement.textContent;

      // Check for 'get' command in the code example
      const hasGetCommand = codeContent.includes('get mykey') ||
                           codeContent.includes('get ');
      expect(hasGetCommand).toBe(true);

      // Check for get command pattern
      const getPattern = /get\s+\w+/;
      expect(getPattern.test(codeContent)).toBe(true);
    });
  });

  describe('Test Case 4: STORED response verification', () => {
    test('should show STORED response after set command', () => {
      const codeElement = document.querySelector('.code-content code');
      expect(codeElement).not.toBeNull();

      const codeContent = codeElement.textContent;

      // Check for 'STORED' response
      expect(codeContent).toContain('STORED');

      // Verify the order: 'set' should appear before 'STORED'
      const setIndex = codeContent.indexOf('set mykey');
      const storedIndex = codeContent.indexOf('STORED');

      expect(setIndex).toBeGreaterThanOrEqual(0);
      expect(storedIndex).toBeGreaterThan(setIndex);
    });
  });

  describe('Test Case 5: Telnet connection example', () => {
    test('should include telnet localhost 12333 or similar connection command', () => {
      const codeElement = document.querySelector('.code-content code');
      expect(codeElement).not.toBeNull();

      const codeContent = codeElement.textContent;

      // Check for telnet connection example
      const hasTelnetCommand = codeContent.includes('telnet localhost 12333') ||
                               codeContent.includes('telnet localhost') ||
                               codeContent.includes('telnet 127.0.0.1');
      expect(hasTelnetCommand).toBe(true);

      // Verify the telnet command specifically includes the port
      const telnetPattern = /telnet\s+(localhost|127\.0\.0\.1)\s+\d+/;
      expect(telnetPattern.test(codeContent)).toBe(true);
    });
  });

  describe('Additional Code Example Validations', () => {
    test('should have proper code block styling elements', () => {
      // Check for code header with terminal dots
      const codeHeader = document.querySelector('.code-header');
      expect(codeHeader).not.toBeNull();

      const dots = document.querySelectorAll('.code-dot');
      expect(dots.length).toBe(3);
    });

    test('should display VALUE response for get command', () => {
      const codeElement = document.querySelector('.code-content code');
      const codeContent = codeElement.textContent;

      // Check for VALUE response which shows key-value retrieval
      expect(codeContent).toContain('VALUE');
      expect(codeContent).toContain('END');
    });

    test('should have syntax highlighting classes', () => {
      const commentElements = document.querySelectorAll('.code-comment');
      const commandElements = document.querySelectorAll('.code-command');
      const responseElements = document.querySelectorAll('.code-response');

      // Verify syntax highlighting is present
      expect(commentElements.length).toBeGreaterThan(0);
      expect(commandElements.length).toBeGreaterThan(0);
      expect(responseElements.length).toBeGreaterThan(0);
    });

    test('should be inside a section with proper id', () => {
      const section = document.querySelector('#code-example');
      expect(section).not.toBeNull();
      expect(section.classList.contains('code-example')).toBe(true);
    });
  });
});
