import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import { readFileSync } from 'fs';
import { resolve } from 'path';

describe('Supported Memcached Commands Section', () => {
  let dom;
  let document;

  beforeEach(() => {
    const htmlPath = resolve(__dirname, '../index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    dom = new JSDOM(html, { runScripts: 'dangerously', resources: 'usable' });
    document = dom.window.document;
  });

  afterEach(() => {
    if (dom) {
      dom.window.close();
    }
  });

  // Test Case 1: Check commands section exists
  describe('Test Case 1: Commands section exists', () => {
    it('should have a commands section present', () => {
      const commandsSection = document.querySelector('#commands, [id*="command"], .commands');
      expect(commandsSection).not.toBeNull();
    });

    it('commands section should have a heading', () => {
      const commandsSection = document.querySelector('#commands, [id*="command"], .commands');
      expect(commandsSection).not.toBeNull();
      const heading = commandsSection.querySelector('h2, h3');
      expect(heading).not.toBeNull();
      expect(heading.textContent.toLowerCase()).toContain('command');
    });

    it('commands section should be navigable from header', () => {
      const nav = document.querySelector('nav');
      const commandsLink = nav.querySelector('a[href="#commands"], a[href*="command"]');
      expect(commandsLink).not.toBeNull();
    });

    it('commands section should have descriptive introductory text', () => {
      const commandsSection = document.querySelector('#commands, [id*="command"], .commands');
      expect(commandsSection).not.toBeNull();
      const introText = commandsSection.querySelector('p');
      expect(introText).not.toBeNull();
      expect(introText.textContent.toLowerCase()).toContain('memcached');
    });
  });

  // Test Case 2: Check 'get' command is listed
  describe('Test Case 2: Get command is listed', () => {
    it('should have get command in the commands section', () => {
      const commandsSection = document.querySelector('#commands, [id*="command"], .commands');
      expect(commandsSection).not.toBeNull();
      const commandCells = commandsSection.querySelectorAll('td code, code');
      const hasGetCommand = Array.from(commandCells).some(cell =>
        cell.textContent.toLowerCase() === 'get'
      );
      expect(hasGetCommand).toBe(true);
    });

    it('get command should be in a table row', () => {
      const commandsSection = document.querySelector('#commands, [id*="command"], .commands');
      const rows = commandsSection.querySelectorAll('tr');
      const hasGetRow = Array.from(rows).some(row => {
        const text = row.textContent.toLowerCase();
        return text.includes('get') && !text.includes('gets');
      });
      expect(hasGetRow).toBe(true);
    });

    it('get command should be categorized as getter command', () => {
      const commandsSection = document.querySelector('#commands, [id*="command"], .commands');
      const getterHeading = Array.from(commandsSection.querySelectorAll('h3')).find(h =>
        h.textContent.toLowerCase().includes('getter')
      );
      expect(getterHeading).not.toBeNull();

      // Check that 'get' appears after the getter heading
      const getterTable = getterHeading.nextElementSibling;
      expect(getterTable.textContent.toLowerCase()).toContain('get');
    });
  });

  // Test Case 3: Check 'set' command is listed
  describe('Test Case 3: Set command is listed', () => {
    it('should have set command in the commands section', () => {
      const commandsSection = document.querySelector('#commands, [id*="command"], .commands');
      expect(commandsSection).not.toBeNull();
      const commandCells = commandsSection.querySelectorAll('td code, code');
      const hasSetCommand = Array.from(commandCells).some(cell =>
        cell.textContent.toLowerCase() === 'set'
      );
      expect(hasSetCommand).toBe(true);
    });

    it('set command should be in a table row', () => {
      const commandsSection = document.querySelector('#commands, [id*="command"], .commands');
      const rows = commandsSection.querySelectorAll('tr');
      const hasSetRow = Array.from(rows).some(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length > 0) {
          const firstCellText = cells[0].textContent.toLowerCase().trim();
          return firstCellText === 'set';
        }
        return false;
      });
      expect(hasSetRow).toBe(true);
    });

    it('set command should be categorized as setter command', () => {
      const commandsSection = document.querySelector('#commands, [id*="command"], .commands');
      const setterHeading = Array.from(commandsSection.querySelectorAll('h3')).find(h =>
        h.textContent.toLowerCase().includes('setter')
      );
      expect(setterHeading).not.toBeNull();

      // Check that 'set' appears in the setter section
      const setterTable = setterHeading.nextElementSibling;
      const setCells = setterTable.querySelectorAll('td code, code');
      const hasSet = Array.from(setCells).some(cell =>
        cell.textContent.toLowerCase() === 'set'
      );
      expect(hasSet).toBe(true);
    });
  });

  // Test Case 4: Check 'delete' command is listed
  describe('Test Case 4: Delete command is listed', () => {
    it('should have delete command in the commands section', () => {
      const commandsSection = document.querySelector('#commands, [id*="command"], .commands');
      expect(commandsSection).not.toBeNull();
      const commandCells = commandsSection.querySelectorAll('td code, code');
      const hasDeleteCommand = Array.from(commandCells).some(cell =>
        cell.textContent.toLowerCase() === 'delete'
      );
      expect(hasDeleteCommand).toBe(true);
    });

    it('delete command should be in a table row', () => {
      const commandsSection = document.querySelector('#commands, [id*="command"], .commands');
      const rows = commandsSection.querySelectorAll('tr');
      const hasDeleteRow = Array.from(rows).some(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length > 0) {
          const firstCellText = cells[0].textContent.toLowerCase().trim();
          return firstCellText === 'delete';
        }
        return false;
      });
      expect(hasDeleteRow).toBe(true);
    });

    it('delete command should be in other commands category', () => {
      const commandsSection = document.querySelector('#commands, [id*="command"], .commands');
      const otherHeading = Array.from(commandsSection.querySelectorAll('h3')).find(h =>
        h.textContent.toLowerCase().includes('other')
      );
      expect(otherHeading).not.toBeNull();

      // Check that 'delete' appears in the other section
      const otherTable = otherHeading.nextElementSibling;
      const deleteCells = otherTable.querySelectorAll('td code, code');
      const hasDelete = Array.from(deleteCells).some(cell =>
        cell.textContent.toLowerCase() === 'delete'
      );
      expect(hasDelete).toBe(true);
    });
  });

  // Test Case 5: Check 'add' command is listed
  describe('Test Case 5: Add command is listed', () => {
    it('should have add command in the commands section', () => {
      const commandsSection = document.querySelector('#commands, [id*="command"], .commands');
      expect(commandsSection).not.toBeNull();
      const commandCells = commandsSection.querySelectorAll('td code, code');
      const hasAddCommand = Array.from(commandCells).some(cell =>
        cell.textContent.toLowerCase() === 'add'
      );
      expect(hasAddCommand).toBe(true);
    });

    it('add command should be in a table row', () => {
      const commandsSection = document.querySelector('#commands, [id*="command"], .commands');
      const rows = commandsSection.querySelectorAll('tr');
      const hasAddRow = Array.from(rows).some(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length > 0) {
          const firstCellText = cells[0].textContent.toLowerCase().trim();
          return firstCellText === 'add';
        }
        return false;
      });
      expect(hasAddRow).toBe(true);
    });

    it('add command should be categorized as setter command', () => {
      const commandsSection = document.querySelector('#commands, [id*="command"], .commands');
      const setterHeading = Array.from(commandsSection.querySelectorAll('h3')).find(h =>
        h.textContent.toLowerCase().includes('setter')
      );
      expect(setterHeading).not.toBeNull();

      // Check that 'add' appears in the setter section
      const setterTable = setterHeading.nextElementSibling;
      const addCells = setterTable.querySelectorAll('td code, code');
      const hasAdd = Array.from(addCells).some(cell =>
        cell.textContent.toLowerCase() === 'add'
      );
      expect(hasAdd).toBe(true);
    });
  });

  // Test Case 6: Check 'replace' command is listed
  describe('Test Case 6: Replace command is listed', () => {
    it('should have replace command in the commands section', () => {
      const commandsSection = document.querySelector('#commands, [id*="command"], .commands');
      expect(commandsSection).not.toBeNull();
      const commandCells = commandsSection.querySelectorAll('td code, code');
      const hasReplaceCommand = Array.from(commandCells).some(cell =>
        cell.textContent.toLowerCase() === 'replace'
      );
      expect(hasReplaceCommand).toBe(true);
    });

    it('replace command should be in a table row', () => {
      const commandsSection = document.querySelector('#commands, [id*="command"], .commands');
      const rows = commandsSection.querySelectorAll('tr');
      const hasReplaceRow = Array.from(rows).some(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length > 0) {
          const firstCellText = cells[0].textContent.toLowerCase().trim();
          return firstCellText === 'replace';
        }
        return false;
      });
      expect(hasReplaceRow).toBe(true);
    });

    it('replace command should be categorized as setter command', () => {
      const commandsSection = document.querySelector('#commands, [id*="command"], .commands');
      const setterHeading = Array.from(commandsSection.querySelectorAll('h3')).find(h =>
        h.textContent.toLowerCase().includes('setter')
      );
      expect(setterHeading).not.toBeNull();

      // Check that 'replace' appears in the setter section
      const setterTable = setterHeading.nextElementSibling;
      const replaceCells = setterTable.querySelectorAll('td code, code');
      const hasReplace = Array.from(replaceCells).some(cell =>
        cell.textContent.toLowerCase() === 'replace'
      );
      expect(hasReplace).toBe(true);
    });
  });

  // Test Case 7: Verify commands have descriptions
  describe('Test Case 7: Commands have descriptions', () => {
    it('each command row should have a description cell', () => {
      const commandsSection = document.querySelector('#commands, [id*="command"], .commands');
      expect(commandsSection).not.toBeNull();
      const tables = commandsSection.querySelectorAll('table');
      expect(tables.length).toBeGreaterThan(0);

      tables.forEach(table => {
        const dataRows = table.querySelectorAll('tbody tr');
        dataRows.forEach(row => {
          const cells = row.querySelectorAll('td');
          expect(cells.length).toBeGreaterThanOrEqual(2);
          // Second cell should contain description
          const descriptionCell = cells[1];
          expect(descriptionCell.textContent.trim().length).toBeGreaterThan(0);
        });
      });
    });

    it('get command should have a description about retrieving values', () => {
      const commandsSection = document.querySelector('#commands, [id*="command"], .commands');
      const rows = commandsSection.querySelectorAll('tr');
      const getRow = Array.from(rows).find(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length > 0) {
          return cells[0].textContent.toLowerCase().trim() === 'get';
        }
        return false;
      });
      expect(getRow).not.toBeNull();
      const description = getRow.querySelectorAll('td')[1].textContent.toLowerCase();
      expect(description).toMatch(/retrieve|get|fetch|read|value/);
    });

    it('set command should have a description about storing values', () => {
      const commandsSection = document.querySelector('#commands, [id*="command"], .commands');
      const rows = commandsSection.querySelectorAll('tr');
      const setRow = Array.from(rows).find(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length > 0) {
          return cells[0].textContent.toLowerCase().trim() === 'set';
        }
        return false;
      });
      expect(setRow).not.toBeNull();
      const description = setRow.querySelectorAll('td')[1].textContent.toLowerCase();
      expect(description).toMatch(/store|set|save|write|key|value/);
    });

    it('delete command should have a description about removing values', () => {
      const commandsSection = document.querySelector('#commands, [id*="command"], .commands');
      const rows = commandsSection.querySelectorAll('tr');
      const deleteRow = Array.from(rows).find(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length > 0) {
          return cells[0].textContent.toLowerCase().trim() === 'delete';
        }
        return false;
      });
      expect(deleteRow).not.toBeNull();
      const description = deleteRow.querySelectorAll('td')[1].textContent.toLowerCase();
      expect(description).toMatch(/remove|delete|discard/);
    });

    it('add command should have a description about conditional storage', () => {
      const commandsSection = document.querySelector('#commands, [id*="command"], .commands');
      const rows = commandsSection.querySelectorAll('tr');
      const addRow = Array.from(rows).find(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length > 0) {
          return cells[0].textContent.toLowerCase().trim() === 'add';
        }
        return false;
      });
      expect(addRow).not.toBeNull();
      const description = addRow.querySelectorAll('td')[1].textContent.toLowerCase();
      expect(description).toMatch(/store|add|only|exist|doesn't/);
    });

    it('replace command should have a description about conditional replacement', () => {
      const commandsSection = document.querySelector('#commands, [id*="command"], .commands');
      const rows = commandsSection.querySelectorAll('tr');
      const replaceRow = Array.from(rows).find(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length > 0) {
          return cells[0].textContent.toLowerCase().trim() === 'replace';
        }
        return false;
      });
      expect(replaceRow).not.toBeNull();
      const description = replaceRow.querySelectorAll('td')[1].textContent.toLowerCase();
      expect(description).toMatch(/store|replace|only|exist/);
    });

    it('tables should have proper headers with Command and Description columns', () => {
      const commandsSection = document.querySelector('#commands, [id*="command"], .commands');
      const tables = commandsSection.querySelectorAll('table');

      tables.forEach(table => {
        const headerRow = table.querySelector('thead tr');
        expect(headerRow).not.toBeNull();
        const headers = headerRow.querySelectorAll('th');
        expect(headers.length).toBeGreaterThanOrEqual(2);
        expect(headers[0].textContent.toLowerCase()).toContain('command');
        expect(headers[1].textContent.toLowerCase()).toContain('description');
      });
    });
  });

  // Additional tests for completeness
  describe('Additional command coverage', () => {
    it('should have gets command listed', () => {
      const commandsSection = document.querySelector('#commands, [id*="command"], .commands');
      const commandCells = commandsSection.querySelectorAll('td code, code');
      const hasGetsCommand = Array.from(commandCells).some(cell =>
        cell.textContent.toLowerCase() === 'gets'
      );
      expect(hasGetsCommand).toBe(true);
    });

    it('should have append command listed', () => {
      const commandsSection = document.querySelector('#commands, [id*="command"], .commands');
      const commandCells = commandsSection.querySelectorAll('td code, code');
      const hasAppendCommand = Array.from(commandCells).some(cell =>
        cell.textContent.toLowerCase() === 'append'
      );
      expect(hasAppendCommand).toBe(true);
    });

    it('should have prepend command listed', () => {
      const commandsSection = document.querySelector('#commands, [id*="command"], .commands');
      const commandCells = commandsSection.querySelectorAll('td code, code');
      const hasPrependCommand = Array.from(commandCells).some(cell =>
        cell.textContent.toLowerCase() === 'prepend'
      );
      expect(hasPrependCommand).toBe(true);
    });

    it('command tables should have proper styling class', () => {
      const commandsSection = document.querySelector('#commands, [id*="command"], .commands');
      const tables = commandsSection.querySelectorAll('table');
      tables.forEach(table => {
        expect(table.classList.contains('command-table') || table.className.includes('command')).toBe(true);
      });
    });
  });
});
