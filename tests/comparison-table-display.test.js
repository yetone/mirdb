/**
 * E2E Tests for Comparison Table Display (Scenario 8, REQ-8)
 *
 * These tests verify that the comparison table with alternatives (Redis, Memcached)
 * is displayed as specified in REQ-8
 */

const fs = require('fs');
const path = require('path');

describe('Comparison Table Display - REQ-8', () => {
  let document;
  let htmlContent;

  beforeAll(() => {
    // Load the HTML file
    const htmlPath = path.join(__dirname, '..', 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Parse HTML using jsdom
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  describe('Comparison Section Structure', () => {
    test('should have a comparison section with id "comparison"', () => {
      const comparisonSection = document.getElementById('comparison');
      expect(comparisonSection).toBeTruthy();
    });

    test('should have a section title for comparison', () => {
      const comparisonSection = document.getElementById('comparison');
      const sectionTitle = comparisonSection.querySelector('.section-title');
      expect(sectionTitle).toBeTruthy();
      expect(sectionTitle.textContent.toLowerCase()).toMatch(/compare|comparison/);
    });

    test('should have a section subtitle describing the comparison', () => {
      const comparisonSection = document.getElementById('comparison');
      const sectionSubtitle = comparisonSection.querySelector('.section-subtitle');
      expect(sectionSubtitle).toBeTruthy();
    });

    test('should have a comparison table element', () => {
      const comparisonSection = document.getElementById('comparison');
      const comparisonTable = comparisonSection.querySelector('.comparison-table');
      expect(comparisonTable).toBeTruthy();
    });
  });

  describe('Test Case 1: Comparison table includes MirDB, Memcached, Redis', () => {
    /**
     * Test Case ID: 1
     * Input: Check comparison table includes MirDB, Memcached, Redis
     * Expected: Table has columns for all three products
     */
    test('should have table headers for MirDB, Memcached, and Redis', () => {
      const comparisonTable = document.querySelector('.comparison-table');
      const headers = comparisonTable.querySelectorAll('thead th');

      // Should have at least 4 columns: Feature, MirDB, Memcached, Redis
      expect(headers.length).toBeGreaterThanOrEqual(4);
    });

    test('should have MirDB column header', () => {
      const comparisonTable = document.querySelector('.comparison-table');
      const headers = comparisonTable.querySelectorAll('thead th');
      const headerTexts = Array.from(headers).map(h => h.textContent);

      expect(headerTexts.some(text => text.includes('MirDB'))).toBeTruthy();
    });

    test('should have Memcached column header', () => {
      const comparisonTable = document.querySelector('.comparison-table');
      const headers = comparisonTable.querySelectorAll('thead th');
      const headerTexts = Array.from(headers).map(h => h.textContent);

      expect(headerTexts.some(text => text.includes('Memcached'))).toBeTruthy();
    });

    test('should have Redis column header', () => {
      const comparisonTable = document.querySelector('.comparison-table');
      const headers = comparisonTable.querySelectorAll('thead th');
      const headerTexts = Array.from(headers).map(h => h.textContent);

      expect(headerTexts.some(text => text.includes('Redis'))).toBeTruthy();
    });

    test('should have Feature column header', () => {
      const comparisonTable = document.querySelector('.comparison-table');
      const headers = comparisonTable.querySelectorAll('thead th');
      const headerTexts = Array.from(headers).map(h => h.textContent);

      expect(headerTexts.some(text => text.includes('Feature'))).toBeTruthy();
    });

    test('should have columns in correct order (Feature, MirDB, Memcached, Redis)', () => {
      const comparisonTable = document.querySelector('.comparison-table');
      const headers = comparisonTable.querySelectorAll('thead th');
      const headerTexts = Array.from(headers).map(h => h.textContent.trim());

      expect(headerTexts[0]).toBe('Feature');
      expect(headerTexts[1]).toBe('MirDB');
      expect(headerTexts[2]).toBe('Memcached');
      expect(headerTexts[3]).toBe('Redis');
    });
  });

  describe('Test Case 2: Verify persistence comparison row', () => {
    /**
     * Test Case ID: 2
     * Input: Verify persistence comparison row
     * Expected: Shows MirDB: Yes (LSM), Memcached: No, Redis: Yes (RDB/AOF)
     */
    test('should have a persistence row in the comparison table', () => {
      const comparisonTable = document.querySelector('.comparison-table');
      const rows = comparisonTable.querySelectorAll('tbody tr');
      const rowTexts = Array.from(rows).map(r => r.textContent.toLowerCase());

      expect(rowTexts.some(text => text.includes('persistence'))).toBeTruthy();
    });

    test('should show MirDB persistence as "Yes (LSM)"', () => {
      const comparisonTable = document.querySelector('.comparison-table');
      const rows = comparisonTable.querySelectorAll('tbody tr');

      let persistenceRow = null;
      rows.forEach(row => {
        const firstCell = row.querySelector('td:first-child');
        if (firstCell && firstCell.textContent.toLowerCase().includes('persistence')) {
          persistenceRow = row;
        }
      });

      expect(persistenceRow).toBeTruthy();
      const cells = persistenceRow.querySelectorAll('td');
      // MirDB column (second cell, index 1)
      expect(cells[1].textContent).toContain('Yes');
      expect(cells[1].textContent).toContain('LSM');
    });

    test('should show Memcached persistence as "No"', () => {
      const comparisonTable = document.querySelector('.comparison-table');
      const rows = comparisonTable.querySelectorAll('tbody tr');

      let persistenceRow = null;
      rows.forEach(row => {
        const firstCell = row.querySelector('td:first-child');
        if (firstCell && firstCell.textContent.toLowerCase().includes('persistence')) {
          persistenceRow = row;
        }
      });

      expect(persistenceRow).toBeTruthy();
      const cells = persistenceRow.querySelectorAll('td');
      // Memcached column (third cell, index 2)
      expect(cells[2].textContent).toContain('No');
    });

    test('should show Redis persistence as "Yes (RDB/AOF)"', () => {
      const comparisonTable = document.querySelector('.comparison-table');
      const rows = comparisonTable.querySelectorAll('tbody tr');

      let persistenceRow = null;
      rows.forEach(row => {
        const firstCell = row.querySelector('td:first-child');
        if (firstCell && firstCell.textContent.toLowerCase().includes('persistence')) {
          persistenceRow = row;
        }
      });

      expect(persistenceRow).toBeTruthy();
      const cells = persistenceRow.querySelectorAll('td');
      // Redis column (fourth cell, index 3)
      expect(cells[3].textContent).toContain('Yes');
      expect(cells[3].textContent).toMatch(/RDB|AOF/);
    });
  });

  describe('Test Case 3: Verify memcached protocol comparison row', () => {
    /**
     * Test Case ID: 3
     * Input: Verify memcached protocol comparison row
     * Expected: Shows MirDB: Yes, Memcached: Native, Redis: Partial
     */
    test('should have a memcached protocol row in the comparison table', () => {
      const comparisonTable = document.querySelector('.comparison-table');
      const rows = comparisonTable.querySelectorAll('tbody tr');
      const rowTexts = Array.from(rows).map(r => r.textContent.toLowerCase());

      expect(rowTexts.some(text => text.includes('memcached protocol'))).toBeTruthy();
    });

    test('should show MirDB memcached protocol support as "Yes"', () => {
      const comparisonTable = document.querySelector('.comparison-table');
      const rows = comparisonTable.querySelectorAll('tbody tr');

      let protocolRow = null;
      rows.forEach(row => {
        const firstCell = row.querySelector('td:first-child');
        if (firstCell && firstCell.textContent.toLowerCase().includes('memcached protocol')) {
          protocolRow = row;
        }
      });

      expect(protocolRow).toBeTruthy();
      const cells = protocolRow.querySelectorAll('td');
      // MirDB column (second cell, index 1)
      expect(cells[1].textContent).toContain('Yes');
    });

    test('should show Memcached protocol support as "Native"', () => {
      const comparisonTable = document.querySelector('.comparison-table');
      const rows = comparisonTable.querySelectorAll('tbody tr');

      let protocolRow = null;
      rows.forEach(row => {
        const firstCell = row.querySelector('td:first-child');
        if (firstCell && firstCell.textContent.toLowerCase().includes('memcached protocol')) {
          protocolRow = row;
        }
      });

      expect(protocolRow).toBeTruthy();
      const cells = protocolRow.querySelectorAll('td');
      // Memcached column (third cell, index 2)
      expect(cells[2].textContent).toContain('Native');
    });

    test('should show Redis memcached protocol support as "Partial"', () => {
      const comparisonTable = document.querySelector('.comparison-table');
      const rows = comparisonTable.querySelectorAll('tbody tr');

      let protocolRow = null;
      rows.forEach(row => {
        const firstCell = row.querySelector('td:first-child');
        if (firstCell && firstCell.textContent.toLowerCase().includes('memcached protocol')) {
          protocolRow = row;
        }
      });

      expect(protocolRow).toBeTruthy();
      const cells = protocolRow.querySelectorAll('td');
      // Redis column (fourth cell, index 3)
      expect(cells[3].textContent).toContain('Partial');
    });
  });

  describe('Test Case 4: Comparison table is responsive', () => {
    /**
     * Test Case ID: 4
     * Input: Check comparison table is responsive
     * Expected: Table displays correctly on mobile (horizontal scroll or stacked layout)
     */
    test('should have table wrapper or responsive styles', () => {
      const comparisonTable = document.querySelector('.comparison-table');
      expect(comparisonTable).toBeTruthy();

      // Check if table has overflow handling in CSS
      // The table should either be in a scrollable container or have responsive styles
      const tableParent = comparisonTable.parentElement;
      expect(tableParent).toBeTruthy();
    });

    test('should have a responsive wrapper around the table', () => {
      const comparisonTable = document.querySelector('.comparison-table');
      const tableWrapper = comparisonTable.parentElement;
      // Check the table has responsive wrapper
      expect(tableWrapper.classList.contains('table-responsive')).toBeTruthy();
    });

    test('should have appropriate CSS class for responsive behavior', () => {
      const comparisonTable = document.querySelector('.comparison-table');
      // Check the table has proper class for styling
      expect(comparisonTable.classList.contains('comparison-table')).toBeTruthy();
    });

    test('should have CSS styles defined for comparison-table', () => {
      // Check that the HTML contains styling for the comparison table
      expect(htmlContent).toContain('.comparison-table');
    });

    test('should have CSS styles for table-responsive wrapper', () => {
      // Check that the HTML contains styling for the responsive wrapper
      expect(htmlContent).toContain('.table-responsive');
      expect(htmlContent).toContain('overflow-x');
    });

    test('should have responsive media query styles', () => {
      // Check for media query presence in the HTML styles
      expect(htmlContent).toContain('@media');
    });

    test('table should have width set to 100% for responsiveness', () => {
      // Verify CSS contains width: 100% for comparison-table
      expect(htmlContent).toMatch(/\.comparison-table[^}]*width:\s*100%/);
    });
  });

  describe('Additional Comparison Criteria', () => {
    test('should include comparison for written language (Rust vs C)', () => {
      const comparisonTable = document.querySelector('.comparison-table');
      const tableText = comparisonTable.textContent.toLowerCase();

      expect(tableText).toContain('written in');
      expect(tableText).toContain('rust');
    });

    test('should include comparison for compression support', () => {
      const comparisonTable = document.querySelector('.comparison-table');
      const tableText = comparisonTable.textContent.toLowerCase();

      expect(tableText).toContain('compression');
      expect(tableText).toContain('snappy');
    });

    test('should include comparison for clustering support', () => {
      const comparisonTable = document.querySelector('.comparison-table');
      const tableText = comparisonTable.textContent.toLowerCase();

      expect(tableText).toContain('clustering');
    });

    test('should have visual indicators for Yes/No values', () => {
      const comparisonTable = document.querySelector('.comparison-table');

      // Check for check/cross class indicators
      const checkIndicators = comparisonTable.querySelectorAll('.check');
      const crossIndicators = comparisonTable.querySelectorAll('.cross');

      expect(checkIndicators.length).toBeGreaterThan(0);
      expect(crossIndicators.length).toBeGreaterThan(0);
    });

    test('should have at least 5 comparison rows', () => {
      const comparisonTable = document.querySelector('.comparison-table');
      const rows = comparisonTable.querySelectorAll('tbody tr');

      // Should have persistence, protocol, language, compression, clustering
      expect(rows.length).toBeGreaterThanOrEqual(5);
    });
  });

  describe('Navigation to Comparison Section', () => {
    test('should be navigable via anchor link', () => {
      const comparisonSection = document.getElementById('comparison');
      expect(comparisonSection).toBeTruthy();
      expect(comparisonSection.tagName.toLowerCase()).toBe('section');
    });

    test('comparison section should be within main content flow', () => {
      const comparisonSection = document.getElementById('comparison');
      const body = document.body;

      expect(body.contains(comparisonSection)).toBeTruthy();
    });
  });
});
