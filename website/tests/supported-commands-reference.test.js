/**
 * Tests for Supported Commands Reference Section
 *
 * Scenario: Supported Commands Reference
 * Verifies that supported memcached commands are listed with examples
 * showing request/response format.
 *
 * Test Cases:
 * 1. Commands section lists: SET, GET, ADD, REPLACE, APPEND, PREPEND, DELETE, GETS (E2E)
 * 2. Commands section includes INFO and MAJOR_COMPACTION commands (E2E)
 * 3. At least SET and GET have example syntax with request/response format (E2E)
 * 4. Commands are organized in table or card format for easy scanning (Unit)
 *
 * @jest-environment jsdom
 */

const fs = require('fs');
const path = require('path');

// Load HTML content before tests
beforeEach(() => {
  // Load HTML for each test to ensure clean state
  const htmlPath = path.join(__dirname, '..', 'index.html');
  const html = fs.readFileSync(htmlPath, 'utf8');
  document.body.innerHTML = html;
});

describe('Supported Commands Reference', () => {
  /**
   * Test Case 1: Query DOM for commands section and extract command names
   * Expected: Commands section lists: SET, GET, ADD, REPLACE, APPEND, PREPEND, DELETE, GETS
   * Type: E2E
   */
  describe('Test Case 1: Standard Memcached Commands List', () => {
    const expectedCommands = ['SET', 'GET', 'ADD', 'REPLACE', 'APPEND', 'PREPEND', 'DELETE', 'GETS'];

    test('should have a commands section with proper ID', () => {
      const commandsSection = document.getElementById('commands');
      expect(commandsSection).toBeInTheDocument();
    });

    test('should list all standard memcached commands', () => {
      const commandsSection = document.getElementById('commands');
      expect(commandsSection).toBeInTheDocument();

      const commandRows = commandsSection.querySelectorAll('[data-command]');
      const commandNames = Array.from(commandRows).map((row) =>
        row.getAttribute('data-command')
      );

      expectedCommands.forEach((cmd) => {
        expect(commandNames).toContain(cmd);
      });
    });

    test('should have SET command in commands table', () => {
      const commandsSection = document.getElementById('commands');
      const setCommand = commandsSection.querySelector('[data-command="SET"]');
      expect(setCommand).toBeInTheDocument();

      const codeElement = setCommand.querySelector('code');
      expect(codeElement.textContent).toBe('SET');
    });

    test('should have GET command in commands table', () => {
      const commandsSection = document.getElementById('commands');
      const getCommand = commandsSection.querySelector('[data-command="GET"]');
      expect(getCommand).toBeInTheDocument();

      const codeElement = getCommand.querySelector('code');
      expect(codeElement.textContent).toBe('GET');
    });

    test('should have GETS command in commands table', () => {
      const commandsSection = document.getElementById('commands');
      const getsCommand = commandsSection.querySelector('[data-command="GETS"]');
      expect(getsCommand).toBeInTheDocument();

      const codeElement = getsCommand.querySelector('code');
      expect(codeElement.textContent).toBe('GETS');
    });

    test('should have ADD command in commands table', () => {
      const commandsSection = document.getElementById('commands');
      const addCommand = commandsSection.querySelector('[data-command="ADD"]');
      expect(addCommand).toBeInTheDocument();

      const codeElement = addCommand.querySelector('code');
      expect(codeElement.textContent).toBe('ADD');
    });

    test('should have REPLACE command in commands table', () => {
      const commandsSection = document.getElementById('commands');
      const replaceCommand = commandsSection.querySelector('[data-command="REPLACE"]');
      expect(replaceCommand).toBeInTheDocument();

      const codeElement = replaceCommand.querySelector('code');
      expect(codeElement.textContent).toBe('REPLACE');
    });

    test('should have APPEND command in commands table', () => {
      const commandsSection = document.getElementById('commands');
      const appendCommand = commandsSection.querySelector('[data-command="APPEND"]');
      expect(appendCommand).toBeInTheDocument();

      const codeElement = appendCommand.querySelector('code');
      expect(codeElement.textContent).toBe('APPEND');
    });

    test('should have PREPEND command in commands table', () => {
      const commandsSection = document.getElementById('commands');
      const prependCommand = commandsSection.querySelector('[data-command="PREPEND"]');
      expect(prependCommand).toBeInTheDocument();

      const codeElement = prependCommand.querySelector('code');
      expect(codeElement.textContent).toBe('PREPEND');
    });

    test('should have DELETE command in commands table', () => {
      const commandsSection = document.getElementById('commands');
      const deleteCommand = commandsSection.querySelector('[data-command="DELETE"]');
      expect(deleteCommand).toBeInTheDocument();

      const codeElement = deleteCommand.querySelector('code');
      expect(codeElement.textContent).toBe('DELETE');
    });

    test('should have at least 8 standard memcached commands', () => {
      const commandsSection = document.getElementById('commands');
      const commandRows = commandsSection.querySelectorAll('[data-command]');
      expect(commandRows.length).toBeGreaterThanOrEqual(8);
    });
  });

  /**
   * Test Case 2: Check for MirDB-specific commands
   * Expected: Commands section includes INFO and MAJOR_COMPACTION commands
   * Type: E2E
   */
  describe('Test Case 2: MirDB-Specific Commands', () => {
    test('should have INFO command in commands table', () => {
      const commandsSection = document.getElementById('commands');
      const infoCommand = commandsSection.querySelector('[data-command="INFO"]');
      expect(infoCommand).toBeInTheDocument();

      const codeElement = infoCommand.querySelector('code');
      expect(codeElement.textContent).toBe('INFO');
    });

    test('should have MAJOR_COMPACTION command in commands table', () => {
      const commandsSection = document.getElementById('commands');
      const compactionCommand = commandsSection.querySelector('[data-command="MAJOR_COMPACTION"]');
      expect(compactionCommand).toBeInTheDocument();

      const codeElement = compactionCommand.querySelector('code');
      expect(codeElement.textContent).toBe('MAJOR_COMPACTION');
    });

    test('INFO command should have description mentioning MirDB', () => {
      const commandsSection = document.getElementById('commands');
      const infoCommand = commandsSection.querySelector('[data-command="INFO"]');
      const description = infoCommand.textContent.toLowerCase();
      expect(description).toContain('mirdb');
    });

    test('MAJOR_COMPACTION command should have description mentioning MirDB', () => {
      const commandsSection = document.getElementById('commands');
      const compactionCommand = commandsSection.querySelector('[data-command="MAJOR_COMPACTION"]');
      const description = compactionCommand.textContent.toLowerCase();
      expect(description).toContain('mirdb');
    });

    test('should have both MirDB-specific commands listed', () => {
      const commandsSection = document.getElementById('commands');
      const mirdbCommands = ['INFO', 'MAJOR_COMPACTION'];

      mirdbCommands.forEach((cmd) => {
        const commandRow = commandsSection.querySelector(`[data-command="${cmd}"]`);
        expect(commandRow).toBeInTheDocument();
      });
    });
  });

  /**
   * Test Case 3: Verify command syntax examples are present
   * Expected: At least SET and GET have example syntax with request/response format
   * Type: E2E
   */
  describe('Test Case 3: Command Syntax Examples', () => {
    test('should have commands examples section', () => {
      const commandsSection = document.getElementById('commands');
      const examplesSection = commandsSection.querySelector('.commands-examples');
      expect(examplesSection).toBeInTheDocument();
    });

    test('should have SET command example', () => {
      const commandsSection = document.getElementById('commands');
      const setExample = commandsSection.querySelector('[data-command-example="SET"]');
      expect(setExample).toBeInTheDocument();
    });

    test('should have GET command example', () => {
      const commandsSection = document.getElementById('commands');
      const getExample = commandsSection.querySelector('[data-command-example="GET"]');
      expect(getExample).toBeInTheDocument();
    });

    test('SET example should have request format', () => {
      const commandsSection = document.getElementById('commands');
      const setExample = commandsSection.querySelector('[data-command-example="SET"]');
      const syntaxLabels = setExample.querySelectorAll('.syntax-label');
      const labelTexts = Array.from(syntaxLabels).map((label) => label.textContent.toLowerCase());
      expect(labelTexts.some((text) => text.includes('request'))).toBe(true);
    });

    test('SET example should have response format', () => {
      const commandsSection = document.getElementById('commands');
      const setExample = commandsSection.querySelector('[data-command-example="SET"]');
      const syntaxLabels = setExample.querySelectorAll('.syntax-label');
      const labelTexts = Array.from(syntaxLabels).map((label) => label.textContent.toLowerCase());
      expect(labelTexts.some((text) => text.includes('response'))).toBe(true);
    });

    test('GET example should have request format', () => {
      const commandsSection = document.getElementById('commands');
      const getExample = commandsSection.querySelector('[data-command-example="GET"]');
      const syntaxLabels = getExample.querySelectorAll('.syntax-label');
      const labelTexts = Array.from(syntaxLabels).map((label) => label.textContent.toLowerCase());
      expect(labelTexts.some((text) => text.includes('request'))).toBe(true);
    });

    test('GET example should have response format', () => {
      const commandsSection = document.getElementById('commands');
      const getExample = commandsSection.querySelector('[data-command-example="GET"]');
      const syntaxLabels = getExample.querySelectorAll('.syntax-label');
      const labelTexts = Array.from(syntaxLabels).map((label) => label.textContent.toLowerCase());
      expect(labelTexts.some((text) => text.includes('response'))).toBe(true);
    });

    test('SET example should contain STORED response', () => {
      const commandsSection = document.getElementById('commands');
      const setExample = commandsSection.querySelector('[data-command-example="SET"]');
      const codeBlocks = setExample.querySelectorAll('.code-block code');
      const allCodeText = Array.from(codeBlocks)
        .map((code) => code.textContent)
        .join('\n');
      expect(allCodeText).toContain('STORED');
    });

    test('GET example should contain VALUE response', () => {
      const commandsSection = document.getElementById('commands');
      const getExample = commandsSection.querySelector('[data-command-example="GET"]');
      const codeBlocks = getExample.querySelectorAll('.code-block code');
      const allCodeText = Array.from(codeBlocks)
        .map((code) => code.textContent)
        .join('\n');
      expect(allCodeText).toContain('VALUE');
    });

    test('GET example should contain END response', () => {
      const commandsSection = document.getElementById('commands');
      const getExample = commandsSection.querySelector('[data-command-example="GET"]');
      const codeBlocks = getExample.querySelectorAll('.code-block code');
      const allCodeText = Array.from(codeBlocks)
        .map((code) => code.textContent)
        .join('\n');
      expect(allCodeText).toContain('END');
    });

    test('command examples should have code blocks', () => {
      const commandsSection = document.getElementById('commands');
      const examples = commandsSection.querySelectorAll('.command-example');
      expect(examples.length).toBeGreaterThanOrEqual(2);

      examples.forEach((example) => {
        const codeBlocks = example.querySelectorAll('.code-block');
        expect(codeBlocks.length).toBeGreaterThanOrEqual(1);
      });
    });

    test('SET example should demonstrate complete syntax with key and value', () => {
      const commandsSection = document.getElementById('commands');
      const setExample = commandsSection.querySelector('[data-command-example="SET"]');
      const allText = setExample.textContent.toLowerCase();
      expect(allText).toMatch(/set\s+\w+/);
    });

    test('GET example should demonstrate complete syntax with key', () => {
      const commandsSection = document.getElementById('commands');
      const getExample = commandsSection.querySelector('[data-command-example="GET"]');
      const allText = getExample.textContent.toLowerCase();
      expect(allText).toMatch(/get\s+\w+/);
    });
  });

  /**
   * Test Case 4: Check commands are displayed in table or structured format
   * Expected: Commands are organized in table or card format for easy scanning
   * Type: Unit
   */
  describe('Test Case 4: Commands Table Structure', () => {
    test('should have commands displayed in a table element', () => {
      const commandsSection = document.getElementById('commands');
      const commandsTable = commandsSection.querySelector('.commands-table');
      expect(commandsTable).toBeInTheDocument();
      expect(commandsTable.tagName.toLowerCase()).toBe('table');
    });

    test('commands table should have proper testid for easy querying', () => {
      const commandsSection = document.getElementById('commands');
      const commandsTable = commandsSection.querySelector('[data-testid="commands-table"]');
      expect(commandsTable).toBeInTheDocument();
    });

    test('commands table should have table header', () => {
      const commandsSection = document.getElementById('commands');
      const tableHead = commandsSection.querySelector('.commands-table thead');
      expect(tableHead).toBeInTheDocument();
    });

    test('commands table should have header columns for Command and Description', () => {
      const commandsSection = document.getElementById('commands');
      const tableHeaders = commandsSection.querySelectorAll('.commands-table th');
      expect(tableHeaders.length).toBeGreaterThanOrEqual(2);

      const headerTexts = Array.from(tableHeaders).map((th) => th.textContent.toLowerCase());
      expect(headerTexts.some((text) => text.includes('command'))).toBe(true);
      expect(headerTexts.some((text) => text.includes('description'))).toBe(true);
    });

    test('commands table should have table body with rows', () => {
      const commandsSection = document.getElementById('commands');
      const tableBody = commandsSection.querySelector('.commands-table tbody');
      expect(tableBody).toBeInTheDocument();

      const tableRows = tableBody.querySelectorAll('tr');
      expect(tableRows.length).toBeGreaterThanOrEqual(10);
    });

    test('each command row should have command code element', () => {
      const commandsSection = document.getElementById('commands');
      const commandRows = commandsSection.querySelectorAll('.commands-table tbody tr');

      commandRows.forEach((row) => {
        const codeElement = row.querySelector('code');
        expect(codeElement).toBeInTheDocument();
        expect(codeElement.textContent.trim().length).toBeGreaterThan(0);
      });
    });

    test('each command row should have description', () => {
      const commandsSection = document.getElementById('commands');
      const commandRows = commandsSection.querySelectorAll('.commands-table tbody tr');

      commandRows.forEach((row) => {
        const cells = row.querySelectorAll('td');
        expect(cells.length).toBeGreaterThanOrEqual(2);
        expect(cells[1].textContent.trim().length).toBeGreaterThan(0);
      });
    });

    test('commands table should have wrapper for overflow handling', () => {
      const commandsSection = document.getElementById('commands');
      const tableWrapper = commandsSection.querySelector('.commands-table-wrapper');
      expect(tableWrapper).toBeInTheDocument();
    });

    test('commands table should be contained within commands container', () => {
      const commandsSection = document.getElementById('commands');
      const commandsContainer = commandsSection.querySelector('.commands-container');
      expect(commandsContainer).toBeInTheDocument();

      const tableWithinContainer = commandsContainer.querySelector('.commands-table');
      expect(tableWithinContainer).toBeInTheDocument();
    });
  });

  /**
   * Additional tests for commands section structure and accessibility
   */
  describe('Commands Section Structure and Accessibility', () => {
    test('commands section should have proper heading (h2)', () => {
      const commandsSection = document.getElementById('commands');
      const heading = commandsSection.querySelector('h2');
      expect(heading).toBeInTheDocument();
      expect(heading.textContent.toLowerCase()).toContain('command');
    });

    test('commands section should have aria-labelledby for accessibility', () => {
      const commandsSection = document.getElementById('commands');
      const labelledBy = commandsSection.getAttribute('aria-labelledby');
      expect(labelledBy).toBeTruthy();

      const heading = document.getElementById(labelledBy);
      expect(heading).toBeInTheDocument();
    });

    test('commands section should have section subtitle', () => {
      const commandsSection = document.getElementById('commands');
      const subtitle = commandsSection.querySelector('.section-subtitle');
      expect(subtitle).toBeInTheDocument();
      expect(subtitle.textContent.trim().length).toBeGreaterThan(0);
    });

    test('commands section should be navigable from navigation menu', () => {
      const navLink = document.querySelector('a[href="#commands"]');
      expect(navLink).toBeInTheDocument();
    });

    test('command examples should have proper heading hierarchy (h3, h4)', () => {
      const commandsSection = document.getElementById('commands');
      const examplesTitle = commandsSection.querySelector('.commands-examples-title');
      expect(examplesTitle.tagName.toLowerCase()).toBe('h3');

      const exampleNames = commandsSection.querySelectorAll('.command-example-name');
      exampleNames.forEach((name) => {
        expect(name.tagName.toLowerCase()).toBe('h4');
      });
    });

    test('command examples should have descriptions', () => {
      const commandsSection = document.getElementById('commands');
      const exampleDescriptions = commandsSection.querySelectorAll('.command-example-description');
      expect(exampleDescriptions.length).toBeGreaterThanOrEqual(2);

      exampleDescriptions.forEach((desc) => {
        expect(desc.textContent.trim().length).toBeGreaterThan(0);
      });
    });
  });
});
