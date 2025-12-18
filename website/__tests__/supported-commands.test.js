/**
 * Supported Commands Section Tests
 * Tests for REQ-5 of MirDB Landing Page
 * Verifies that all supported memcached commands are displayed
 */

const fs = require('fs');
const path = require('path');

describe('Supported Commands Display', () => {
  let document;
  let commandsSection;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../src/index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf8');
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
    commandsSection = document.querySelector('.commands-section, #commands, [data-testid="commands-section"]');
  });

  // Test Case 1: GET command is listed in supported commands
  describe('TC-1: GET Command Display', () => {
    test('should display GET command in supported commands', () => {
      expect(commandsSection).not.toBeNull();
      const sectionText = commandsSection.textContent.toUpperCase();
      expect(sectionText).toContain('GET');
    });
  });

  // Test Case 2: SET command is listed in supported commands
  describe('TC-2: SET Command Display', () => {
    test('should display SET command in supported commands', () => {
      expect(commandsSection).not.toBeNull();
      const sectionText = commandsSection.textContent.toUpperCase();
      expect(sectionText).toContain('SET');
    });
  });

  // Test Case 3: DELETE command is listed in supported commands
  describe('TC-3: DELETE Command Display', () => {
    test('should display DELETE command in supported commands', () => {
      expect(commandsSection).not.toBeNull();
      const sectionText = commandsSection.textContent.toUpperCase();
      expect(sectionText).toContain('DELETE');
    });
  });

  // Test Case 4: ADD command is listed in supported commands
  describe('TC-4: ADD Command Display', () => {
    test('should display ADD command in supported commands', () => {
      expect(commandsSection).not.toBeNull();
      const sectionText = commandsSection.textContent.toUpperCase();
      expect(sectionText).toContain('ADD');
    });
  });

  // Test Case 5: REPLACE command is listed in supported commands
  describe('TC-5: REPLACE Command Display', () => {
    test('should display REPLACE command in supported commands', () => {
      expect(commandsSection).not.toBeNull();
      const sectionText = commandsSection.textContent.toUpperCase();
      expect(sectionText).toContain('REPLACE');
    });
  });

  // Test Case 6: APPEND command is listed in supported commands
  describe('TC-6: APPEND Command Display', () => {
    test('should display APPEND command in supported commands', () => {
      expect(commandsSection).not.toBeNull();
      const sectionText = commandsSection.textContent.toUpperCase();
      expect(sectionText).toContain('APPEND');
    });
  });

  // Test Case 7: PREPEND command is listed in supported commands
  describe('TC-7: PREPEND Command Display', () => {
    test('should display PREPEND command in supported commands', () => {
      expect(commandsSection).not.toBeNull();
      const sectionText = commandsSection.textContent.toUpperCase();
      expect(sectionText).toContain('PREPEND');
    });
  });
});

describe('Supported Commands Section Structure', () => {
  let document;
  let commandsSection;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../src/index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf8');
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
    commandsSection = document.querySelector('.commands-section, #commands, [data-testid="commands-section"]');
  });

  test('should have a commands section present on the page', () => {
    expect(commandsSection).not.toBeNull();
  });

  test('should have a heading for the commands section', () => {
    const heading = commandsSection.querySelector('h2, h3');
    expect(heading).not.toBeNull();
    const headingText = heading.textContent.toLowerCase();
    expect(headingText).toMatch(/command|supported/i);
  });

  test('should list commands in a visually organized manner', () => {
    // Commands should be listed in a list, grid, or similar structure
    const commandElements = commandsSection.querySelectorAll('.command-item, .command, li, .command-card, code');
    expect(commandElements.length).toBeGreaterThanOrEqual(7); // At least 7 commands
  });
});
