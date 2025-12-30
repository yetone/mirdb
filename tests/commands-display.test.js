/**
 * E2E Tests for Supported Commands Display (REQ-3)
 *
 * These tests verify that the MirDB homepage correctly displays
 * all supported memcached commands as specified in the PRD.
 */

const fs = require('fs');
const path = require('path');

describe('Supported Commands Display - REQ-3', () => {
  let document;
  let htmlContent;

  beforeAll(() => {
    // Load the HTML file
    const htmlPath = path.join(__dirname, '..', 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Parse HTML using jsdom
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  describe('Commands Section Structure', () => {
    test('should have a commands section with id "commands"', () => {
      const commandsSection = document.getElementById('commands');
      expect(commandsSection).toBeTruthy();
    });

    test('should have a section title for Supported Commands', () => {
      const commandsSection = document.getElementById('commands');
      const sectionTitle = commandsSection.querySelector('.section-title');
      expect(sectionTitle).toBeTruthy();
      expect(sectionTitle.textContent).toContain('Supported Commands');
    });
  });

  describe('Test Case 1: Storage Commands Display', () => {
    /**
     * Test Case ID: 1
     * Input: Check for storage commands in commands section
     * Expected: SET, ADD, REPLACE, APPEND, PREPEND commands are listed with descriptions
     */
    test('should display SET command with description', () => {
      const storageSection = document.querySelector('[data-testid="storage-commands"]');
      expect(storageSection).toBeTruthy();

      const setCommand = Array.from(storageSection.querySelectorAll('.command-name'))
        .find(el => el.textContent.includes('SET'));
      expect(setCommand).toBeTruthy();

      const setDescription = setCommand.closest('.command-item').querySelector('.command-description');
      expect(setDescription).toBeTruthy();
      expect(setDescription.textContent.length).toBeGreaterThan(0);
    });

    test('should display ADD command with description', () => {
      const storageSection = document.querySelector('[data-testid="storage-commands"]');

      const addCommand = Array.from(storageSection.querySelectorAll('.command-name'))
        .find(el => el.textContent.includes('ADD'));
      expect(addCommand).toBeTruthy();

      const addDescription = addCommand.closest('.command-item').querySelector('.command-description');
      expect(addDescription).toBeTruthy();
      expect(addDescription.textContent).toContain('exist');
    });

    test('should display REPLACE command with description', () => {
      const storageSection = document.querySelector('[data-testid="storage-commands"]');

      const replaceCommand = Array.from(storageSection.querySelectorAll('.command-name'))
        .find(el => el.textContent.includes('REPLACE'));
      expect(replaceCommand).toBeTruthy();

      const replaceDescription = replaceCommand.closest('.command-item').querySelector('.command-description');
      expect(replaceDescription).toBeTruthy();
      expect(replaceDescription.textContent.length).toBeGreaterThan(0);
    });

    test('should display APPEND command with description', () => {
      const storageSection = document.querySelector('[data-testid="storage-commands"]');

      const appendCommand = Array.from(storageSection.querySelectorAll('.command-name'))
        .find(el => el.textContent.includes('APPEND'));
      expect(appendCommand).toBeTruthy();

      const appendDescription = appendCommand.closest('.command-item').querySelector('.command-description');
      expect(appendDescription).toBeTruthy();
      expect(appendDescription.textContent.toLowerCase()).toContain('append');
    });

    test('should display PREPEND command with description', () => {
      const storageSection = document.querySelector('[data-testid="storage-commands"]');

      const prependCommand = Array.from(storageSection.querySelectorAll('.command-name'))
        .find(el => el.textContent.includes('PREPEND'));
      expect(prependCommand).toBeTruthy();

      const prependDescription = prependCommand.closest('.command-item').querySelector('.command-description');
      expect(prependDescription).toBeTruthy();
      expect(prependDescription.textContent.toLowerCase()).toContain('prepend');
    });

    test('should have Storage Commands category header', () => {
      const storageSection = document.querySelector('[data-testid="storage-commands"]');
      const header = storageSection.querySelector('.command-category-header');
      expect(header).toBeTruthy();
      expect(header.textContent).toContain('Storage');
    });
  });

  describe('Test Case 2: Retrieval Commands Display', () => {
    /**
     * Test Case ID: 2
     * Input: Check for retrieval commands in commands section
     * Expected: GET and GETS commands are listed with descriptions
     */
    test('should display GET command with description', () => {
      const retrievalSection = document.querySelector('[data-testid="retrieval-commands"]');
      expect(retrievalSection).toBeTruthy();

      const getCommand = Array.from(retrievalSection.querySelectorAll('.command-name'))
        .find(el => el.textContent === 'GET' || el.textContent.trim() === 'GET');
      expect(getCommand).toBeTruthy();

      const getDescription = getCommand.closest('.command-item').querySelector('.command-description');
      expect(getDescription).toBeTruthy();
      expect(getDescription.textContent.toLowerCase()).toContain('retrieve');
    });

    test('should display GETS command with description', () => {
      const retrievalSection = document.querySelector('[data-testid="retrieval-commands"]');

      const getsCommand = Array.from(retrievalSection.querySelectorAll('.command-name'))
        .find(el => el.textContent.includes('GETS'));
      expect(getsCommand).toBeTruthy();

      const getsDescription = getsCommand.closest('.command-item').querySelector('.command-description');
      expect(getsDescription).toBeTruthy();
      expect(getsDescription.textContent.toLowerCase()).toContain('cas');
    });

    test('should have Retrieval Commands category header', () => {
      const retrievalSection = document.querySelector('[data-testid="retrieval-commands"]');
      const header = retrievalSection.querySelector('.command-category-header');
      expect(header).toBeTruthy();
      expect(header.textContent).toContain('Retrieval');
    });
  });

  describe('Test Case 3: DELETE Command Display', () => {
    /**
     * Test Case ID: 3
     * Input: Check for DELETE command in commands section
     * Expected: DELETE command is listed with description
     */
    test('should display DELETE command with description', () => {
      const deletionSection = document.querySelector('[data-testid="deletion-commands"]');
      expect(deletionSection).toBeTruthy();

      const deleteCommand = Array.from(deletionSection.querySelectorAll('.command-name'))
        .find(el => el.textContent.includes('DELETE'));
      expect(deleteCommand).toBeTruthy();

      const deleteDescription = deleteCommand.closest('.command-item').querySelector('.command-description');
      expect(deleteDescription).toBeTruthy();
      expect(deleteDescription.textContent.toLowerCase()).toContain('remove');
    });

    test('should have Deletion Commands category header', () => {
      const deletionSection = document.querySelector('[data-testid="deletion-commands"]');
      const header = deletionSection.querySelector('.command-category-header');
      expect(header).toBeTruthy();
      expect(header.textContent).toContain('Deletion');
    });
  });

  describe('Test Case 4: MirDB-Specific Commands Display', () => {
    /**
     * Test Case ID: 4
     * Input: Check for MirDB-specific commands
     * Expected: INFO and MAJOR_COMPACTION commands are listed and identified as MirDB-specific
     */
    test('should display INFO command with description', () => {
      const mirdbSection = document.querySelector('[data-testid="mirdb-commands"]');
      expect(mirdbSection).toBeTruthy();

      const infoCommand = Array.from(mirdbSection.querySelectorAll('.command-name'))
        .find(el => el.textContent.includes('INFO'));
      expect(infoCommand).toBeTruthy();

      const infoDescription = infoCommand.closest('.command-item').querySelector('.command-description');
      expect(infoDescription).toBeTruthy();
      expect(infoDescription.textContent.toLowerCase()).toContain('status');
    });

    test('should display MAJOR_COMPACTION command with description', () => {
      const mirdbSection = document.querySelector('[data-testid="mirdb-commands"]');

      const compactionCommand = Array.from(mirdbSection.querySelectorAll('.command-name'))
        .find(el => el.textContent.includes('MAJOR_COMPACTION'));
      expect(compactionCommand).toBeTruthy();

      const compactionDescription = compactionCommand.closest('.command-item').querySelector('.command-description');
      expect(compactionDescription).toBeTruthy();
      expect(compactionDescription.textContent.toLowerCase()).toContain('compaction');
    });

    test('should have MirDB-Specific Commands category header', () => {
      const mirdbSection = document.querySelector('[data-testid="mirdb-commands"]');
      const header = mirdbSection.querySelector('.command-category-header');
      expect(header).toBeTruthy();
      expect(header.textContent).toContain('MirDB');
    });

    test('should identify INFO as MirDB-specific with badge', () => {
      const mirdbSection = document.querySelector('[data-testid="mirdb-commands"]');
      const infoCommand = Array.from(mirdbSection.querySelectorAll('.command-name'))
        .find(el => el.textContent.includes('INFO'));

      // Check for MirDB badge
      const badge = infoCommand.querySelector('.mirdb-badge');
      expect(badge).toBeTruthy();
      expect(badge.textContent).toContain('MirDB');
    });

    test('should identify MAJOR_COMPACTION as MirDB-specific with badge', () => {
      const mirdbSection = document.querySelector('[data-testid="mirdb-commands"]');
      const compactionCommand = Array.from(mirdbSection.querySelectorAll('.command-name'))
        .find(el => el.textContent.includes('MAJOR_COMPACTION'));

      // Check for MirDB badge
      const badge = compactionCommand.querySelector('.mirdb-badge');
      expect(badge).toBeTruthy();
      expect(badge.textContent).toContain('MirDB');
    });

    test('should visually distinguish MirDB-specific section', () => {
      const mirdbSection = document.querySelector('[data-testid="mirdb-commands"]');
      const header = mirdbSection.querySelector('.command-category-header');

      // Check that the header has the mirdb-specific class for visual distinction
      expect(header.classList.contains('mirdb-specific')).toBe(true);
    });
  });

  describe('All Commands Completeness', () => {
    test('should have all required storage commands', () => {
      const storageCommands = ['SET', 'ADD', 'REPLACE', 'APPEND', 'PREPEND'];
      const storageSection = document.querySelector('[data-testid="storage-commands"]');
      const commandNames = Array.from(storageSection.querySelectorAll('.command-name'))
        .map(el => el.textContent.trim());

      storageCommands.forEach(cmd => {
        expect(commandNames.some(name => name.includes(cmd))).toBe(true);
      });
    });

    test('should have all required retrieval commands', () => {
      const retrievalCommands = ['GET', 'GETS'];
      const retrievalSection = document.querySelector('[data-testid="retrieval-commands"]');
      const commandNames = Array.from(retrievalSection.querySelectorAll('.command-name'))
        .map(el => el.textContent.trim());

      retrievalCommands.forEach(cmd => {
        expect(commandNames.some(name => name.includes(cmd))).toBe(true);
      });
    });

    test('should have DELETE command', () => {
      const deletionSection = document.querySelector('[data-testid="deletion-commands"]');
      const commandNames = Array.from(deletionSection.querySelectorAll('.command-name'))
        .map(el => el.textContent.trim());

      expect(commandNames.some(name => name.includes('DELETE'))).toBe(true);
    });

    test('should have all required MirDB-specific commands', () => {
      const mirdbCommands = ['INFO', 'MAJOR_COMPACTION'];
      const mirdbSection = document.querySelector('[data-testid="mirdb-commands"]');
      const commandNames = Array.from(mirdbSection.querySelectorAll('.command-name'))
        .map(el => el.textContent.trim());

      mirdbCommands.forEach(cmd => {
        expect(commandNames.some(name => name.includes(cmd))).toBe(true);
      });
    });
  });
});
