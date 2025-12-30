/**
 * Tests for Supported Commands Display (REQ-4)
 * Verify display of supported memcached commands with examples
 */

const fs = require('fs');
const path = require('path');

describe('Supported Commands Display', () => {
  let document;

  beforeEach(() => {
    const html = fs.readFileSync(path.resolve(__dirname, '../index.html'), 'utf8');
    document = new DOMParser().parseFromString(html, 'text/html');
  });

  // Test Case 1: Storage commands are listed (SET, ADD, REPLACE, APPEND, PREPEND)
  describe('Test Case 1: Storage Commands Display', () => {
    test('should have a commands section in the page', () => {
      const commandsSection = document.querySelector('[data-section="commands"], #commands, .commands-section, section.commands');
      expect(commandsSection).not.toBeNull();
    });

    test('should list SET command', () => {
      const pageContent = document.body.textContent;
      expect(pageContent).toContain('SET');
    });

    test('should list ADD command', () => {
      const pageContent = document.body.textContent;
      expect(pageContent).toContain('ADD');
    });

    test('should list REPLACE command', () => {
      const pageContent = document.body.textContent;
      expect(pageContent).toContain('REPLACE');
    });

    test('should list APPEND command', () => {
      const pageContent = document.body.textContent;
      expect(pageContent).toContain('APPEND');
    });

    test('should list PREPEND command', () => {
      const pageContent = document.body.textContent;
      expect(pageContent).toContain('PREPEND');
    });

    test('should have storage commands grouped together or labeled', () => {
      const commandsSection = document.querySelector('[data-section="commands"], #commands, .commands-section, section.commands');
      expect(commandsSection).not.toBeNull();
      const storageSection = commandsSection.querySelector('[data-command-type="storage"], .storage-commands, .command-group');
      // Check either dedicated section exists or commands are within the commands section
      const sectionText = commandsSection.textContent;
      expect(sectionText).toContain('SET');
      expect(sectionText).toContain('ADD');
      expect(sectionText).toContain('REPLACE');
      expect(sectionText).toContain('APPEND');
      expect(sectionText).toContain('PREPEND');
    });
  });

  // Test Case 2: Retrieval commands are listed (GET, GETS)
  describe('Test Case 2: Retrieval Commands Display', () => {
    test('should list GET command', () => {
      const pageContent = document.body.textContent;
      expect(pageContent).toContain('GET');
    });

    test('should list GETS command', () => {
      const pageContent = document.body.textContent;
      expect(pageContent).toContain('GETS');
    });

    test('should have retrieval commands in the commands section', () => {
      const commandsSection = document.querySelector('[data-section="commands"], #commands, .commands-section, section.commands');
      expect(commandsSection).not.toBeNull();
      const sectionText = commandsSection.textContent;
      expect(sectionText).toContain('GET');
      expect(sectionText).toContain('GETS');
    });
  });

  // Test Case 3: DELETE command is listed
  describe('Test Case 3: DELETE Command Display', () => {
    test('should list DELETE command', () => {
      const pageContent = document.body.textContent;
      expect(pageContent).toContain('DELETE');
    });

    test('should have DELETE command in the commands section', () => {
      const commandsSection = document.querySelector('[data-section="commands"], #commands, .commands-section, section.commands');
      expect(commandsSection).not.toBeNull();
      const sectionText = commandsSection.textContent;
      expect(sectionText).toContain('DELETE');
    });
  });

  // Test Case 4: MirDB-specific commands are listed (INFO, MAJOR_COMPACTION)
  describe('Test Case 4: MirDB-Specific Commands Display', () => {
    test('should list INFO command', () => {
      const pageContent = document.body.textContent;
      expect(pageContent).toContain('INFO');
    });

    test('should list MAJOR_COMPACTION command', () => {
      const pageContent = document.body.textContent;
      expect(pageContent).toContain('MAJOR_COMPACTION');
    });

    test('should identify INFO and MAJOR_COMPACTION as MirDB-specific', () => {
      const commandsSection = document.querySelector('[data-section="commands"], #commands, .commands-section, section.commands');
      expect(commandsSection).not.toBeNull();

      // Check for MirDB-specific labeling
      const mirdbSection = commandsSection.querySelector('[data-command-type="mirdb-specific"], .mirdb-commands, .mirdb-specific');
      const sectionText = commandsSection.textContent.toLowerCase();

      // Either have a dedicated MirDB-specific section or mention "mirdb" in context with these commands
      const hasMirDBLabel = mirdbSection !== null || sectionText.includes('mirdb');
      expect(hasMirDBLabel).toBe(true);

      expect(sectionText).toContain('info');
      expect(sectionText).toContain('major_compaction');
    });

    test('should have MirDB-specific commands in the commands section', () => {
      const commandsSection = document.querySelector('[data-section="commands"], #commands, .commands-section, section.commands');
      expect(commandsSection).not.toBeNull();
      const sectionText = commandsSection.textContent;
      expect(sectionText).toContain('INFO');
      expect(sectionText).toContain('MAJOR_COMPACTION');
    });
  });

  // Additional integration tests
  describe('Commands Section Structure', () => {
    test('should have proper heading for commands section', () => {
      const commandsSection = document.querySelector('[data-section="commands"], #commands, .commands-section, section.commands');
      expect(commandsSection).not.toBeNull();
      const heading = commandsSection.querySelector('h2, h3');
      expect(heading).not.toBeNull();
    });

    test('should display all 10 supported commands', () => {
      const pageContent = document.body.textContent;
      const commands = ['SET', 'GET', 'DELETE', 'ADD', 'REPLACE', 'APPEND', 'PREPEND', 'GETS', 'INFO', 'MAJOR_COMPACTION'];
      commands.forEach(cmd => {
        expect(pageContent).toContain(cmd);
      });
    });
  });
});
