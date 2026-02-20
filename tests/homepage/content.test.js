/**
 * Content Section Tests
 * Owner: Scenarios 2, 3, 4, 8 (shared)
 *
 * Tests for:
 * - Features section content and count
 * - Quick Start code blocks
 * - Comparison table content
 * - Project status checkmarks
 */

const { loadHTML, loadHTMLWithWindow } = require('./test-utils');

// Features Section Tests - Scenario 2
describe('Features Section Validation', () => {
  let document;

  beforeAll(async () => {
    document = await loadHTML();
  });

  // Test Case 1: Features section exists
  test('features section with id or class containing "features" exists', () => {
    const featuresSection = document.getElementById('features') ||
                           document.querySelector('.features') ||
                           document.querySelector('[class*="features"]');
    expect(featuresSection).not.toBeNull();
  });

  // Test Case 2: At least 4 feature items
  test('at least 4 feature items are present', () => {
    const featuresSection = document.getElementById('features');
    expect(featuresSection).not.toBeNull();

    const featureItems = featuresSection.querySelectorAll('.feature-card, .feature-item, [data-feature], article');
    expect(featureItems.length).toBeGreaterThanOrEqual(4);
  });

  // Test Case 3: Memcached compatibility feature
  test('feature mentioning Memcached protocol compatibility exists', () => {
    const featuresSection = document.getElementById('features');
    expect(featuresSection).not.toBeNull();

    const textContent = featuresSection.textContent.toLowerCase();
    expect(textContent).toMatch(/memcached/i);
  });

  // Test Case 4: Persistence feature
  test('feature mentioning data persistence exists', () => {
    const featuresSection = document.getElementById('features');
    expect(featuresSection).not.toBeNull();

    const textContent = featuresSection.textContent.toLowerCase();
    const hasPersistence = textContent.includes('persistence') ||
                          textContent.includes('persistent') ||
                          textContent.includes('persists');
    expect(hasPersistence).toBe(true);
  });

  // Test Case 5: LSM Tree feature
  test('feature mentioning LSM tree architecture exists', () => {
    const featuresSection = document.getElementById('features');
    expect(featuresSection).not.toBeNull();

    const textContent = featuresSection.textContent;
    const hasLSM = textContent.includes('LSM') ||
                   textContent.toLowerCase().includes('log-structured');
    expect(hasLSM).toBe(true);
  });

  // Test Case 6: Rust implementation feature
  test('feature mentioning Rust implementation exists', () => {
    const featuresSection = document.getElementById('features');
    expect(featuresSection).not.toBeNull();

    const textContent = featuresSection.textContent;
    expect(textContent).toMatch(/Rust/i);
  });

  // Test Case 7: Feature descriptions are 15 words or fewer
  test('each feature description contains 15 words or fewer', () => {
    const featuresSection = document.getElementById('features');
    expect(featuresSection).not.toBeNull();

    const featureItems = featuresSection.querySelectorAll('.feature-card, .feature-item, [data-feature], article');
    expect(featureItems.length).toBeGreaterThan(0);

    featureItems.forEach((item, index) => {
      // Get description paragraph (excluding the heading)
      const description = item.querySelector('p');
      if (description) {
        const words = description.textContent.trim().split(/\s+/).filter(w => w.length > 0);
        expect(words.length).toBeLessThanOrEqual(15);
      }
    });
  });

  // Test Case 8: SSTable or compaction mentioned
  test('features section mentions SSTables or compaction capabilities', () => {
    const featuresSection = document.getElementById('features');
    expect(featuresSection).not.toBeNull();

    const textContent = featuresSection.textContent;
    const hasSSTableOrCompaction = textContent.includes('SSTable') ||
                                   textContent.toLowerCase().includes('sstable') ||
                                   textContent.toLowerCase().includes('compaction');
    expect(hasSSTableOrCompaction).toBe(true);
  });
});

// Quick Start Section Tests - Scenario 3
describe('Quick Start Section Validation', () => {
  let document;

  beforeAll(async () => {
    document = await loadHTML();
  });

  describe('Test Case 1: Quick Start Section Exists', () => {
    test('A section with content about Quick Start exists', () => {
      // Look for a section with id "quick-start" or containing "Quick Start" text
      const quickStartSection = document.querySelector('#quick-start');
      expect(quickStartSection).not.toBeNull();

      // Also verify it has "Quick Start" in the heading
      const heading = quickStartSection.querySelector('h2');
      expect(heading).not.toBeNull();
      expect(heading.textContent.toLowerCase()).toContain('quick start');
    });

    test('Quick Start section has data-testid attribute', () => {
      const section = document.querySelector('[data-testid="quick-start-section"]');
      expect(section).not.toBeNull();
    });
  });

  describe('Test Case 2: Set Command Example', () => {
    test('A code or pre element contains set command syntax', () => {
      const quickStartSection = document.querySelector('#quick-start');
      const codeElements = quickStartSection.querySelectorAll('pre, code');

      let hasSetCommand = false;
      for (const el of codeElements) {
        const text = el.textContent;
        // Check for set command with memcached protocol format
        if (text.includes('set') && (text.includes('<key>') || text.includes('mykey'))) {
          hasSetCommand = true;
          break;
        }
      }

      expect(hasSetCommand).toBe(true);
    });

    test('Set command shows proper memcached format', () => {
      const quickStartSection = document.querySelector('#quick-start');
      const preElements = quickStartSection.querySelectorAll('pre');

      let hasProperFormat = false;
      for (const pre of preElements) {
        const text = pre.textContent;
        // Should contain format: set <key> <flags> <ttl> <bytes>
        if (text.includes('set') &&
            (text.includes('<flags>') || text.includes(' 0 0 '))) {
          hasProperFormat = true;
          break;
        }
      }

      expect(hasProperFormat).toBe(true);
    });
  });

  describe('Test Case 3: Get Command Example', () => {
    test('A code or pre element contains get command syntax', () => {
      const quickStartSection = document.querySelector('#quick-start');
      const codeElements = quickStartSection.querySelectorAll('pre, code');

      let hasGetCommand = false;
      for (const el of codeElements) {
        const text = el.textContent;
        // Check for get command
        if (text.includes('get') && (text.includes('<key>') || text.includes('mykey'))) {
          hasGetCommand = true;
          break;
        }
      }

      expect(hasGetCommand).toBe(true);
    });

    test('Get command shows proper memcached format', () => {
      const quickStartSection = document.querySelector('#quick-start');
      const preElements = quickStartSection.querySelectorAll('pre');

      let hasProperFormat = false;
      for (const pre of preElements) {
        const text = pre.textContent;
        // Should contain format: get <key>
        if (text.includes('get') && text.includes('mykey')) {
          hasProperFormat = true;
          break;
        }
      }

      expect(hasProperFormat).toBe(true);
    });
  });

  describe('Test Case 4: Monospace Font Styling', () => {
    test('Code blocks use font-family containing monospace', () => {
      // Check the embedded style for monospace font
      const styleElements = document.querySelectorAll('style');
      let hasMonospaceStyle = false;

      for (const style of styleElements) {
        const cssText = style.textContent;
        // Look for font-family declarations with monospace
        if (cssText.includes('monospace') &&
            (cssText.includes('pre') || cssText.includes('code'))) {
          hasMonospaceStyle = true;
          break;
        }
      }

      expect(hasMonospaceStyle).toBe(true);
    });

    test('Pre and code elements are present in Quick Start section', () => {
      const quickStartSection = document.querySelector('#quick-start');
      const preElements = quickStartSection.querySelectorAll('pre');
      const codeElements = quickStartSection.querySelectorAll('code');

      // Should have multiple code blocks
      expect(preElements.length).toBeGreaterThan(0);
      expect(codeElements.length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 5: Code Block Visual Distinction', () => {
    test('Code blocks have background color different from main content', () => {
      const styleElements = document.querySelectorAll('style');
      let hasDistinctBackground = false;
      let codeBackground = null;
      let bodyBackground = null;

      for (const style of styleElements) {
        const cssText = style.textContent;

        // Look for pre background color
        const preMatch = cssText.match(/pre[^{]*\{[^}]*background(?:-color)?:\s*([^;]+)/);
        if (preMatch) {
          codeBackground = preMatch[1].trim();
        }

        // Look for body background color
        const bodyMatch = cssText.match(/body[^{]*\{[^}]*background(?:-color)?:\s*([^;]+)/);
        if (bodyMatch) {
          bodyBackground = bodyMatch[1].trim();
        }

        // Also check CSS variables
        if (cssText.includes('--bg-code') && cssText.includes('--bg-primary')) {
          hasDistinctBackground = true;
        }
      }

      // Either have distinct backgrounds or CSS variables for different colors
      expect(hasDistinctBackground || (codeBackground && codeBackground !== bodyBackground)).toBe(true);
    });

    test('Code blocks have border or distinct styling', () => {
      const styleElements = document.querySelectorAll('style');
      let hasDistinctStyling = false;

      for (const style of styleElements) {
        const cssText = style.textContent;
        // Look for pre styling with border, border-radius, or padding
        if (cssText.includes('pre') &&
            (cssText.includes('border') ||
             cssText.includes('border-radius') ||
             cssText.includes('padding'))) {
          hasDistinctStyling = true;
          break;
        }
      }

      expect(hasDistinctStyling).toBe(true);
    });
  });

  describe('Test Case 6: Server Startup Instructions', () => {
    test('Instructions for running MirDB server are present', () => {
      const quickStartSection = document.querySelector('#quick-start');
      const textContent = quickStartSection.textContent.toLowerCase();

      // Should have instructions about starting/running the server
      const hasServerInstructions =
        textContent.includes('start') ||
        textContent.includes('run') ||
        textContent.includes('server');

      expect(hasServerInstructions).toBe(true);
    });

    test('Server startup command is shown in code block', () => {
      const quickStartSection = document.querySelector('#quick-start');
      const preElements = quickStartSection.querySelectorAll('pre');

      let hasServerCommand = false;
      for (const pre of preElements) {
        const text = pre.textContent;
        // Check for cargo run or server startup command
        if (text.includes('cargo run') ||
            text.includes('mirdb-server') ||
            text.includes('telnet')) {
          hasServerCommand = true;
          break;
        }
      }

      expect(hasServerCommand).toBe(true);
    });

    test('Default port or connection info is provided', () => {
      const quickStartSection = document.querySelector('#quick-start');
      const textContent = quickStartSection.textContent;

      // Should mention the port or connection details
      const hasConnectionInfo =
        textContent.includes('12333') ||
        textContent.includes('localhost') ||
        textContent.includes('telnet');

      expect(hasConnectionInfo).toBe(true);
    });
  });
});

// Comparison Section Tests - Scenario 4
describe('Comparison Section Validation', () => {
  let document;

  beforeAll(async () => {
    document = await loadHTML();
  });

  // Test Case 1: Comparison section or table exists
  describe('Test Case 1: Comparison Section Exists', () => {
    test('A section or table element for comparison exists', () => {
      const comparisonSection = document.getElementById('comparison') ||
                                document.querySelector('[data-testid="comparison-section"]');
      expect(comparisonSection).not.toBeNull();
    });

    test('Comparison section has a table element', () => {
      const comparisonSection = document.getElementById('comparison');
      expect(comparisonSection).not.toBeNull();

      const table = comparisonSection.querySelector('table');
      expect(table).not.toBeNull();
    });

    test('Comparison table has header row', () => {
      const comparisonSection = document.getElementById('comparison');
      const table = comparisonSection.querySelector('table');
      const headerRow = table.querySelector('thead tr') || table.querySelector('tr:first-child');
      expect(headerRow).not.toBeNull();
    });
  });

  // Test Case 2: MirDB is described as persistent
  describe('Test Case 2: Persistence Comparison', () => {
    test('MirDB is described as persistent in the comparison', () => {
      const comparisonSection = document.getElementById('comparison');
      expect(comparisonSection).not.toBeNull();

      const tableContent = comparisonSection.querySelector('table').textContent;
      const hasPersistent = tableContent.toLowerCase().includes('persistent');
      expect(hasPersistent).toBe(true);
    });

    test('Persistence is shown in MirDB column', () => {
      const comparisonSection = document.getElementById('comparison');
      const table = comparisonSection.querySelector('table');
      const rows = table.querySelectorAll('tbody tr');

      let mirdbHasPersistence = false;
      for (const row of rows) {
        const cells = row.querySelectorAll('td');
        if (cells.length >= 2) {
          // MirDB column (second column after feature name)
          const mirdbCell = cells[1];
          if (mirdbCell && mirdbCell.textContent.toLowerCase().includes('persistent')) {
            mirdbHasPersistence = true;
            break;
          }
        }
      }

      expect(mirdbHasPersistence).toBe(true);
    });
  });

  // Test Case 3: Memcached is described as in-memory
  describe('Test Case 3: In-Memory Storage', () => {
    test('Memcached is described as in-memory in the comparison', () => {
      const comparisonSection = document.getElementById('comparison');
      expect(comparisonSection).not.toBeNull();

      const tableContent = comparisonSection.querySelector('table').textContent.toLowerCase();
      const hasInMemory = tableContent.includes('in-memory') || tableContent.includes('inmemory');
      expect(hasInMemory).toBe(true);
    });

    test('In-memory is shown in Memcached column', () => {
      const comparisonSection = document.getElementById('comparison');
      const table = comparisonSection.querySelector('table');
      const rows = table.querySelectorAll('tbody tr');

      let memcachedHasInMemory = false;
      for (const row of rows) {
        const cells = row.querySelectorAll('td');
        if (cells.length >= 3) {
          // Memcached column (third column)
          const memcachedCell = cells[2];
          if (memcachedCell) {
            const cellText = memcachedCell.textContent.toLowerCase();
            if (cellText.includes('in-memory') || cellText.includes('inmemory')) {
              memcachedHasInMemory = true;
              break;
            }
          }
        }
      }

      expect(memcachedHasInMemory).toBe(true);
    });
  });

  // Test Case 4: MirDB storage engine is listed as LSM Tree
  describe('Test Case 4: LSM Tree Storage Engine', () => {
    test('MirDB storage engine is listed as LSM Tree', () => {
      const comparisonSection = document.getElementById('comparison');
      expect(comparisonSection).not.toBeNull();

      const tableContent = comparisonSection.querySelector('table').textContent;
      const hasLSM = tableContent.includes('LSM');
      expect(hasLSM).toBe(true);
    });

    test('LSM Tree is shown in MirDB column', () => {
      const comparisonSection = document.getElementById('comparison');
      const table = comparisonSection.querySelector('table');
      const rows = table.querySelectorAll('tbody tr');

      let mirdbHasLSM = false;
      for (const row of rows) {
        const cells = row.querySelectorAll('td');
        if (cells.length >= 2) {
          // Check for storage engine row with LSM in MirDB column
          const mirdbCell = cells[1];
          if (mirdbCell && mirdbCell.textContent.includes('LSM')) {
            mirdbHasLSM = true;
            break;
          }
        }
      }

      expect(mirdbHasLSM).toBe(true);
    });
  });

  // Test Case 5: Memcached storage is listed as Hash Table
  describe('Test Case 5: Hash Table Storage', () => {
    test('Memcached storage is listed as Hash Table', () => {
      const comparisonSection = document.getElementById('comparison');
      expect(comparisonSection).not.toBeNull();

      const tableContent = comparisonSection.querySelector('table').textContent;
      const hasHash = tableContent.toLowerCase().includes('hash');
      expect(hasHash).toBe(true);
    });

    test('Hash Table is shown in Memcached column', () => {
      const comparisonSection = document.getElementById('comparison');
      const table = comparisonSection.querySelector('table');
      const rows = table.querySelectorAll('tbody tr');

      let memcachedHasHash = false;
      for (const row of rows) {
        const cells = row.querySelectorAll('td');
        if (cells.length >= 3) {
          // Memcached column (third column)
          const memcachedCell = cells[2];
          if (memcachedCell && memcachedCell.textContent.toLowerCase().includes('hash')) {
            memcachedHasHash = true;
            break;
          }
        }
      }

      expect(memcachedHasHash).toBe(true);
    });
  });

  // Test Case 6: MirDB data is described as preserved on restart
  describe('Test Case 6: Data Preserved on Restart', () => {
    test('MirDB data is described as preserved on restart', () => {
      const comparisonSection = document.getElementById('comparison');
      expect(comparisonSection).not.toBeNull();

      const tableContent = comparisonSection.querySelector('table').textContent.toLowerCase();
      const hasPreserved = tableContent.includes('preserved') || tableContent.includes('persists');
      expect(hasPreserved).toBe(true);
    });

    test('Preserved is shown in MirDB restart behavior row', () => {
      const comparisonSection = document.getElementById('comparison');
      const table = comparisonSection.querySelector('table');
      const rows = table.querySelectorAll('tbody tr');

      let mirdbHasPreserved = false;
      for (const row of rows) {
        const cells = row.querySelectorAll('td');
        // Look for restart-related row
        const featureCell = cells[0];
        if (featureCell && featureCell.textContent.toLowerCase().includes('restart')) {
          const mirdbCell = cells[1];
          if (mirdbCell) {
            const cellText = mirdbCell.textContent.toLowerCase();
            if (cellText.includes('preserved') || cellText.includes('persists')) {
              mirdbHasPreserved = true;
              break;
            }
          }
        }
      }

      expect(mirdbHasPreserved).toBe(true);
    });
  });

  // Test Case 7: Memcached data is described as lost on restart
  describe('Test Case 7: Data Lost on Restart', () => {
    test('Memcached data is described as lost on restart', () => {
      const comparisonSection = document.getElementById('comparison');
      expect(comparisonSection).not.toBeNull();

      const tableContent = comparisonSection.querySelector('table').textContent.toLowerCase();
      const hasLost = tableContent.includes('lost') || tableContent.includes('cleared');
      expect(hasLost).toBe(true);
    });

    test('Lost is shown in Memcached restart behavior row', () => {
      const comparisonSection = document.getElementById('comparison');
      const table = comparisonSection.querySelector('table');
      const rows = table.querySelectorAll('tbody tr');

      let memcachedHasLost = false;
      for (const row of rows) {
        const cells = row.querySelectorAll('td');
        // Look for restart-related row
        const featureCell = cells[0];
        if (featureCell && featureCell.textContent.toLowerCase().includes('restart')) {
          const memcachedCell = cells[2];
          if (memcachedCell) {
            const cellText = memcachedCell.textContent.toLowerCase();
            if (cellText.includes('lost') || cellText.includes('cleared')) {
              memcachedHasLost = true;
              break;
            }
          }
        }
      }

      expect(memcachedHasLost).toBe(true);
    });
  });
});
