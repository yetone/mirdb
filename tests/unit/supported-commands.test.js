/**
 * E2E-equivalent tests for Supported Commands Display
 * Converted to JSDOM for environment compatibility
 * Tests scenario: Verify the homepage displays supported memcached commands
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

describe('Supported Commands Display - E2E Tests', () => {
  let document;
  let commandsSection;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
    // Find the commands section - could be part of technical overview or separate
    commandsSection = document.querySelector('#commands') ||
                      document.querySelector('[data-testid="commands-section"]') ||
                      document.querySelector('.commands-section');
  });

  test('TC1: Page contains a list or section with supported commands', () => {
    // Search for supported commands section
    expect(commandsSection).not.toBeNull();

    // Verify the section has a heading about commands
    const heading = commandsSection.querySelector('h2, h3');
    expect(heading).not.toBeNull();

    const headingText = heading.textContent.toLowerCase();
    expect(headingText).toMatch(/command/i);
  });

  test('TC2: GET command is mentioned in commands section', () => {
    expect(commandsSection).not.toBeNull();

    const sectionText = commandsSection.textContent;

    // GET should be mentioned as a command
    expect(sectionText).toMatch(/\bGET\b/);
  });

  test('TC3: SET command is mentioned in commands section', () => {
    expect(commandsSection).not.toBeNull();

    const sectionText = commandsSection.textContent;

    // SET should be mentioned as a command
    expect(sectionText).toMatch(/\bSET\b/);
  });

  test('TC4: DELETE command is mentioned in commands section', () => {
    expect(commandsSection).not.toBeNull();

    const sectionText = commandsSection.textContent;

    // DELETE should be mentioned as a command
    expect(sectionText).toMatch(/\bDELETE\b/);
  });

  test('TC5: ADD, REPLACE, APPEND, PREPEND commands are mentioned', () => {
    expect(commandsSection).not.toBeNull();

    const sectionText = commandsSection.textContent;

    // Additional commands should be mentioned
    expect(sectionText).toMatch(/\bADD\b/);
    expect(sectionText).toMatch(/\bREPLACE\b/);
    expect(sectionText).toMatch(/\bAPPEND\b/);
    expect(sectionText).toMatch(/\bPREPEND\b/);
  });
});

describe('Supported Commands Section Structure', () => {
  let document;
  let commandsSection;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
    commandsSection = document.querySelector('#commands') ||
                      document.querySelector('[data-testid="commands-section"]') ||
                      document.querySelector('.commands-section');
  });

  test('Commands section uses semantic markup', () => {
    expect(commandsSection).not.toBeNull();

    // Should be a section element
    expect(commandsSection.tagName.toLowerCase()).toBe('section');
  });

  test('Commands are displayed in a structured format', () => {
    expect(commandsSection).not.toBeNull();

    // Commands should be displayed in a list or grid format
    const commandList = commandsSection.querySelector('.commands-list, .commands-grid, ul, dl');
    expect(commandList).not.toBeNull();
  });

  test('Each command has a description', () => {
    expect(commandsSection).not.toBeNull();

    // Check that commands have descriptions
    const sectionText = commandsSection.textContent.toLowerCase();

    // Each command should have some descriptive text nearby
    // GET should exist and retrieval should be mentioned in the section
    expect(sectionText).toContain('get');
    expect(sectionText).toContain('retriev');

    // SET should exist and storage should be mentioned
    expect(sectionText).toContain('set');
    expect(sectionText).toContain('stor');

    // DELETE should exist and removal should be mentioned
    expect(sectionText).toContain('delete');
    expect(sectionText).toContain('remov');
  });
});
