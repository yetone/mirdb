/**
 * Configuration Section Unit Tests
 * Owner: Scenario 7 - Configuration Reference
 *
 * Tests:
 * - Section displays listen_addr parameter with default 0.0.0.0:12333
 * - Section displays max_levels parameter with default 7
 * - Section displays work_dir parameter with default /tmp/mirdb
 * - Section displays sstable_max_size parameter with default 100MB
 * - Section displays memtable_max_size parameter with default 4MB
 * - Section displays block_size parameter with default 4KB
 * - Configuration parameters are displayed in a structured table format
 */

const fs = require('fs');
const path = require('path');

describe('Configuration Section', () => {
  let mdContent;

  beforeAll(() => {
    // Read the configuration.md file which contains the configuration section content
    const configPath = path.join(__dirname, '../../../src/configuration.md');
    mdContent = fs.readFileSync(configPath, 'utf8');
  });

  test('Section displays listen_addr parameter with default 0.0.0.0:12333 (test case 1)', () => {
    // Test case 1: Section displays listen_addr parameter with default 0.0.0.0:12333
    // Check for listen_addr in the table
    expect(mdContent).toMatch(/listen_addr/);
    expect(mdContent).toMatch(/0\.0\.0\.0:12333/);

    // Verify both appear in the same table row context
    const tableContent = mdContent.match(/\|[^|]*listen_addr[^|]*\|[^|]*0\.0\.0\.0:12333[^|]*\|/);
    expect(tableContent).not.toBeNull();
  });

  test('Section displays max_levels parameter with default 7 (test case 2)', () => {
    // Test case 2: Section displays max_levels parameter with default 7
    // Check for max_levels in the table
    expect(mdContent).toMatch(/max_levels/);

    // Verify max_levels with default value 7 in table row
    const tableContent = mdContent.match(/\|[^|]*max_levels[^|]*\|[^|]*7[^|]*\|/);
    expect(tableContent).not.toBeNull();
  });

  test('Section displays work_dir parameter with default /tmp/mirdb (test case 3)', () => {
    // Test case 3: Section displays work_dir parameter with default /tmp/mirdb
    // Check for work_dir in the table
    expect(mdContent).toMatch(/work_dir/);
    expect(mdContent).toMatch(/\/tmp\/mirdb/);

    // Verify both appear in the same table row context
    const tableContent = mdContent.match(/\|[^|]*work_dir[^|]*\|[^|]*\/tmp\/mirdb[^|]*\|/);
    expect(tableContent).not.toBeNull();
  });

  test('Section displays sstable_max_size parameter with default 100MB (test case 4)', () => {
    // Test case 4: Section displays sstable_max_size parameter with default 100MB
    // Check for sstable_max_size in the table
    expect(mdContent).toMatch(/sstable_max_size/);
    expect(mdContent).toMatch(/100MB/);

    // Verify both appear in the same table row context
    const tableContent = mdContent.match(/\|[^|]*sstable_max_size[^|]*\|[^|]*100MB[^|]*\|/);
    expect(tableContent).not.toBeNull();
  });

  test('Section displays memtable_max_size parameter with default 4MB (test case 5)', () => {
    // Test case 5: Section displays memtable_max_size parameter with default 4MB
    // Check for memtable_max_size in the table
    expect(mdContent).toMatch(/memtable_max_size/);
    expect(mdContent).toMatch(/4MB/);

    // Verify both appear in the same table row context
    const tableContent = mdContent.match(/\|[^|]*memtable_max_size[^|]*\|[^|]*4MB[^|]*\|/);
    expect(tableContent).not.toBeNull();
  });

  test('Section displays block_size parameter with default 4KB (test case 6)', () => {
    // Test case 6: Section displays block_size parameter with default 4KB
    // Check for block_size in the table
    expect(mdContent).toMatch(/block_size/);
    expect(mdContent).toMatch(/4KB/);

    // Verify both appear in the same table row context
    const tableContent = mdContent.match(/\|[^|]*block_size[^|]*\|[^|]*4KB[^|]*\|/);
    expect(tableContent).not.toBeNull();
  });

  test('Configuration parameters are displayed in a structured table format (test case 7)', () => {
    // Test case 7: Configuration parameters are displayed in a structured table format
    // Check for markdown table structure
    // Table header
    expect(mdContent).toMatch(/\| Parameter \| Default Value \| Description \|/);

    // Table separator
    expect(mdContent).toMatch(/\|[-]+\|[-]+\|[-]+\|/);

    // Verify all 6 configuration parameters are present in the table
    const parameterRows = mdContent.match(/\|[^|]+\|[^|]+\|[^|]+\|/g);
    expect(parameterRows).not.toBeNull();

    // Count data rows (excluding header and separator)
    const dataRows = parameterRows.filter(row =>
      !row.includes('Parameter') && !row.match(/^\|[-]+\|/)
    );
    expect(dataRows.length).toBeGreaterThanOrEqual(6);

    // Verify section has proper structure
    expect(mdContent).toMatch(/<section[^>]*id="configuration"[^>]*>/);
    expect(mdContent).toMatch(/class="configuration-section"/);
  });
});

describe('Configuration Section Additional Content', () => {
  let mdContent;

  beforeAll(() => {
    const configPath = path.join(__dirname, '../../../src/configuration.md');
    mdContent = fs.readFileSync(configPath, 'utf8');
  });

  test('Section has a main heading', () => {
    expect(mdContent).toMatch(/# Configuration Reference/);
  });

  test('Section includes configuration file example', () => {
    // Check for TOML code block
    expect(mdContent).toMatch(/```toml/);
    expect(mdContent).toMatch(/addr\s*=\s*"0\.0\.0\.0:12333"/);
  });

  test('Section documents size units', () => {
    // Check for size unit documentation
    expect(mdContent).toMatch(/Size Units/);
    expect(mdContent).toMatch(/Kilobytes/);
    expect(mdContent).toMatch(/Megabytes/);
  });

  test('Configuration section has example configuration', () => {
    // Check for example configuration section
    expect(mdContent).toMatch(/<section[^>]*id="example-configuration"[^>]*>/);
    expect(mdContent).toMatch(/## Example Configuration/);

    // Check for TOML code block
    expect(mdContent).toMatch(/```toml/);
  });
});

describe('Configuration Section Built HTML Tests', () => {
  let builtHtml;
  let htmlExists = false;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../../book/configuration.html');
    try {
      builtHtml = fs.readFileSync(htmlPath, 'utf8');
      htmlExists = true;
    } catch (e) {
      // Built HTML may not exist yet
      htmlExists = false;
    }
  });

  test('Built HTML contains configuration content (if built)', () => {
    if (!htmlExists) {
      console.log('Skipping built HTML test - book not built yet');
      return;
    }

    // Verify content is present in built HTML
    expect(builtHtml).toMatch(/Configuration Reference/);
    expect(builtHtml).toMatch(/listen_addr/);
    expect(builtHtml).toMatch(/0\.0\.0\.0:12333/);
  });

  test('Built HTML has configuration table with all parameters (if built)', () => {
    if (!htmlExists) {
      console.log('Skipping built HTML test - book not built yet');
      return;
    }

    // Verify all parameters are present
    expect(builtHtml).toMatch(/listen_addr/);
    expect(builtHtml).toMatch(/max_levels/);
    expect(builtHtml).toMatch(/work_dir/);
    expect(builtHtml).toMatch(/sstable_max_size/);
    expect(builtHtml).toMatch(/memtable_max_size/);
    expect(builtHtml).toMatch(/block_size/);
  });

  test('Built HTML has all parameter defaults (if built)', () => {
    if (!htmlExists) {
      console.log('Skipping built HTML test - book not built yet');
      return;
    }

    // Check all default values are present
    expect(builtHtml).toMatch(/0\.0\.0\.0:12333/);
    expect(builtHtml).toMatch(/\/tmp\/mirdb/);
    expect(builtHtml).toMatch(/100MB/);
    expect(builtHtml).toMatch(/4MB/);
    expect(builtHtml).toMatch(/4KB/);
  });
});
