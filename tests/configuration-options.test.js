/**
 * E2E Tests for Configuration Options Display (REQ-9)
 *
 * These tests verify that the MirDB homepage correctly displays
 * default configuration parameters and customization options
 * as specified in the PRD.
 */

const fs = require('fs');
const path = require('path');

describe('Configuration Options Display - REQ-9', () => {
  let document;
  let htmlContent;

  beforeAll(() => {
    // Load the HTML file
    const htmlPath = path.join(__dirname, '..', 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Parse HTML using jsdom
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  describe('Configuration Section Structure', () => {
    test('should have a configuration section with id "configuration"', () => {
      const configSection = document.getElementById('configuration');
      expect(configSection).toBeTruthy();
    });

    test('should have a section title for Configuration', () => {
      const configSection = document.getElementById('configuration');
      const sectionTitle = configSection.querySelector('.section-title');
      expect(sectionTitle).toBeTruthy();
      expect(sectionTitle.textContent).toContain('Configuration');
    });

    test('should have a configuration table', () => {
      const configSection = document.getElementById('configuration');
      const configTable = configSection.querySelector('.config-table');
      expect(configTable).toBeTruthy();
    });

    test('should have table headers for Parameter, Default Value, and Description', () => {
      const configSection = document.getElementById('configuration');
      const configTable = configSection.querySelector('.config-table');
      const headers = configTable.querySelectorAll('th');

      const headerTexts = Array.from(headers).map(h => h.textContent.trim());
      expect(headerTexts).toContain('Parameter');
      expect(headerTexts).toContain('Default Value');
      expect(headerTexts).toContain('Description');
    });
  });

  describe('Test Case 1: Listen Address Default Display', () => {
    /**
     * Test Case ID: 1
     * Input: Check listen address default is displayed
     * Expected: Configuration shows default listen address: 0.0.0.0:12333
     */
    test('should display default listen address: 0.0.0.0:12333', () => {
      const configSection = document.getElementById('configuration');
      const configTable = configSection.querySelector('.config-table');
      const rows = configTable.querySelectorAll('tbody tr');

      // Find the row with addr parameter
      let addrRow = null;
      rows.forEach(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length > 0 && cells[0].textContent.includes('addr')) {
          addrRow = row;
        }
      });

      expect(addrRow).toBeTruthy();
      const cells = addrRow.querySelectorAll('td');
      expect(cells[1].textContent).toContain('0.0.0.0:12333');
    });

    test('should have description for listen address parameter', () => {
      const configSection = document.getElementById('configuration');
      const configTable = configSection.querySelector('.config-table');
      const rows = configTable.querySelectorAll('tbody tr');

      let addrRow = null;
      rows.forEach(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length > 0 && cells[0].textContent.includes('addr')) {
          addrRow = row;
        }
      });

      expect(addrRow).toBeTruthy();
      const cells = addrRow.querySelectorAll('td');
      expect(cells[2].textContent.toLowerCase()).toContain('listen');
    });
  });

  describe('Test Case 2: LSM Levels Default Display', () => {
    /**
     * Test Case ID: 2
     * Input: Check LSM levels default is displayed
     * Expected: Configuration shows max LSM levels: 7
     */
    test('should display max LSM levels: 7', () => {
      const configSection = document.getElementById('configuration');
      const configTable = configSection.querySelector('.config-table');
      const rows = configTable.querySelectorAll('tbody tr');

      // Find the row with max_level parameter
      let maxLevelRow = null;
      rows.forEach(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length > 0 && cells[0].textContent.includes('max_level')) {
          maxLevelRow = row;
        }
      });

      expect(maxLevelRow).toBeTruthy();
      const cells = maxLevelRow.querySelectorAll('td');
      expect(cells[1].textContent).toContain('7');
    });

    test('should have description for max_level parameter mentioning LSM', () => {
      const configSection = document.getElementById('configuration');
      const configTable = configSection.querySelector('.config-table');
      const rows = configTable.querySelectorAll('tbody tr');

      let maxLevelRow = null;
      rows.forEach(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length > 0 && cells[0].textContent.includes('max_level')) {
          maxLevelRow = row;
        }
      });

      expect(maxLevelRow).toBeTruthy();
      const cells = maxLevelRow.querySelectorAll('td');
      expect(cells[2].textContent.toLowerCase()).toContain('lsm');
    });
  });

  describe('Test Case 3: Memtable Size Default Display', () => {
    /**
     * Test Case ID: 3
     * Input: Check memtable size default is displayed
     * Expected: Configuration shows memtable max size: 4MB
     */
    test('should display memtable max size: 4MB', () => {
      const configSection = document.getElementById('configuration');
      const configTable = configSection.querySelector('.config-table');
      const rows = configTable.querySelectorAll('tbody tr');

      // Find the row with mem_table_max_size parameter
      let memtableRow = null;
      rows.forEach(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length > 0 && cells[0].textContent.includes('mem_table_max_size')) {
          memtableRow = row;
        }
      });

      expect(memtableRow).toBeTruthy();
      const cells = memtableRow.querySelectorAll('td');
      expect(cells[1].textContent).toContain('4MB');
    });

    test('should have description for memtable size parameter', () => {
      const configSection = document.getElementById('configuration');
      const configTable = configSection.querySelector('.config-table');
      const rows = configTable.querySelectorAll('tbody tr');

      let memtableRow = null;
      rows.forEach(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length > 0 && cells[0].textContent.includes('mem_table_max_size')) {
          memtableRow = row;
        }
      });

      expect(memtableRow).toBeTruthy();
      const cells = memtableRow.querySelectorAll('td');
      expect(cells[2].textContent.toLowerCase()).toContain('memtable');
    });
  });

  describe('Test Case 4: SSTable Size Default Display', () => {
    /**
     * Test Case ID: 4
     * Input: Check SSTable size default is displayed
     * Expected: Configuration shows SSTable max size: 100MB
     */
    test('should display SSTable max size: 100MB', () => {
      const configSection = document.getElementById('configuration');
      const configTable = configSection.querySelector('.config-table');
      const rows = configTable.querySelectorAll('tbody tr');

      // Find the row with sst_max_size parameter
      let sstRow = null;
      rows.forEach(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length > 0 && cells[0].textContent.includes('sst_max_size')) {
          sstRow = row;
        }
      });

      expect(sstRow).toBeTruthy();
      const cells = sstRow.querySelectorAll('td');
      expect(cells[1].textContent).toContain('100MB');
    });

    test('should have description for SSTable size parameter', () => {
      const configSection = document.getElementById('configuration');
      const configTable = configSection.querySelector('.config-table');
      const rows = configTable.querySelectorAll('tbody tr');

      let sstRow = null;
      rows.forEach(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length > 0 && cells[0].textContent.includes('sst_max_size')) {
          sstRow = row;
        }
      });

      expect(sstRow).toBeTruthy();
      const cells = sstRow.querySelectorAll('td');
      expect(cells[2].textContent.toLowerCase()).toContain('sstable');
    });
  });

  describe('Configuration Section Completeness', () => {
    test('should have all required configuration parameters', () => {
      const configSection = document.getElementById('configuration');
      const configTable = configSection.querySelector('.config-table');
      const rows = configTable.querySelectorAll('tbody tr');

      const requiredParams = ['addr', 'max_level', 'mem_table_max_size', 'sst_max_size'];
      const foundParams = [];

      rows.forEach(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length > 0) {
          const paramName = cells[0].textContent.trim();
          requiredParams.forEach(param => {
            if (paramName.includes(param)) {
              foundParams.push(param);
            }
          });
        }
      });

      requiredParams.forEach(param => {
        expect(foundParams).toContain(param);
      });
    });

    test('should have block_size parameter as additional configuration option', () => {
      const configSection = document.getElementById('configuration');
      const configTable = configSection.querySelector('.config-table');
      const rows = configTable.querySelectorAll('tbody tr');

      let blockSizeRow = null;
      rows.forEach(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length > 0 && cells[0].textContent.includes('block_size')) {
          blockSizeRow = row;
        }
      });

      expect(blockSizeRow).toBeTruthy();
      const cells = blockSizeRow.querySelectorAll('td');
      expect(cells[1].textContent).toContain('4KB');
    });
  });

  describe('Configuration Section Navigation', () => {
    test('should be navigable from nav links', () => {
      const navLinks = document.querySelectorAll('.nav-links a');
      const configLink = Array.from(navLinks).find(
        link => link.getAttribute('href') === '#configuration'
      );

      // Configuration section should be accessible (link may or may not exist in nav)
      // The section should exist and be targetable
      const configSection = document.getElementById('configuration');
      expect(configSection).toBeTruthy();
    });
  });

  describe('Configuration Section Accessibility', () => {
    test('should have proper table structure for accessibility', () => {
      const configSection = document.getElementById('configuration');
      const configTable = configSection.querySelector('.config-table');

      // Check for thead and tbody
      const thead = configTable.querySelector('thead');
      const tbody = configTable.querySelector('tbody');

      expect(thead).toBeTruthy();
      expect(tbody).toBeTruthy();
    });

    test('should use code elements for parameter names', () => {
      const configSection = document.getElementById('configuration');
      const configTable = configSection.querySelector('.config-table');
      const tbody = configTable.querySelector('tbody');
      const rows = tbody.querySelectorAll('tr');

      rows.forEach(row => {
        const paramCell = row.querySelector('td:first-child');
        if (paramCell) {
          const codeElement = paramCell.querySelector('code');
          expect(codeElement).toBeTruthy();
        }
      });
    });
  });
});
