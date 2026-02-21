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

describe('Configuration Section - Source Content', () => {
  let mdContent;

  beforeAll(() => {
    const configPath = path.join(__dirname, '../../../src/configuration.md');
    mdContent = fs.readFileSync(configPath, 'utf8');
  });

  test('Section displays listen_addr parameter with default 0.0.0.0:12333 (test case 1)', () => {
    // Test case 1: listen_addr parameter
    // Check for parameter name in table
    expect(mdContent).toMatch(/<code>listen_addr<\/code>/);

    // Check for default value
    expect(mdContent).toMatch(/<code>0\.0\.0\.0:12333<\/code>/);

    // Check for table row containing this parameter
    expect(mdContent).toMatch(/<tr[^>]*id="param-listen_addr"[^>]*>/);

    // Check for description
    expect(mdContent).toMatch(/listening address/i);
  });

  test('Section displays max_levels parameter with default 7 (test case 2)', () => {
    // Test case 2: max_levels parameter
    // Check for parameter name in table
    expect(mdContent).toMatch(/<code>max_levels<\/code>/);

    // Check for default value
    expect(mdContent).toMatch(/<code>7<\/code>/);

    // Check for table row containing this parameter
    expect(mdContent).toMatch(/<tr[^>]*id="param-max_levels"[^>]*>/);

    // Check for description about LSM tree levels
    expect(mdContent).toMatch(/LSM tree levels/i);
  });

  test('Section displays work_dir parameter with default /tmp/mirdb (test case 3)', () => {
    // Test case 3: work_dir parameter
    // Check for parameter name in table
    expect(mdContent).toMatch(/<code>work_dir<\/code>/);

    // Check for default value
    expect(mdContent).toMatch(/<code>\/tmp\/mirdb<\/code>/);

    // Check for table row containing this parameter
    expect(mdContent).toMatch(/<tr[^>]*id="param-work_dir"[^>]*>/);

    // Check for description about working directory
    expect(mdContent).toMatch(/Working directory/i);
  });

  test('Section displays sstable_max_size parameter with default 100MB (test case 4)', () => {
    // Test case 4: sstable_max_size parameter
    // Check for parameter name in table
    expect(mdContent).toMatch(/<code>sstable_max_size<\/code>/);

    // Check for default value
    expect(mdContent).toMatch(/<code>100MB<\/code>/);

    // Check for table row containing this parameter
    expect(mdContent).toMatch(/<tr[^>]*id="param-sstable_max_size"[^>]*>/);

    // Check for description about SSTable size
    expect(mdContent).toMatch(/SSTable/i);
  });

  test('Section displays memtable_max_size parameter with default 4MB (test case 5)', () => {
    // Test case 5: memtable_max_size parameter
    // Check for parameter name in table
    expect(mdContent).toMatch(/<code>memtable_max_size<\/code>/);

    // Check for default value
    expect(mdContent).toMatch(/<code>4MB<\/code>/);

    // Check for table row containing this parameter
    expect(mdContent).toMatch(/<tr[^>]*id="param-memtable_max_size"[^>]*>/);

    // Check for description about memtable size
    expect(mdContent).toMatch(/memtable size/i);
  });

  test('Section displays block_size parameter with default 4KB (test case 6)', () => {
    // Test case 6: block_size parameter
    // Check for parameter name in table
    expect(mdContent).toMatch(/<code>block_size<\/code>/);

    // Check for default value
    expect(mdContent).toMatch(/<code>4KB<\/code>/);

    // Check for table row containing this parameter
    expect(mdContent).toMatch(/<tr[^>]*id="param-block_size"[^>]*>/);

    // Check for description about block size
    expect(mdContent).toMatch(/block size/i);
  });

  test('Configuration parameters are displayed in a structured table format (test case 7)', () => {
    // Test case 7: Table structure verification
    // Check for table element with proper structure
    expect(mdContent).toMatch(/<table[^>]*class="[^"]*config-table[^"]*"[^>]*id="config-params-table"[^>]*>/);

    // Check for table header with required columns
    expect(mdContent).toMatch(/<th[^>]*class="[^"]*param-name[^"]*"[^>]*>Parameter<\/th>/);
    expect(mdContent).toMatch(/<th[^>]*class="[^"]*param-default[^"]*"[^>]*>Default Value<\/th>/);
    expect(mdContent).toMatch(/<th[^>]*class="[^"]*param-description[^"]*"[^>]*>Description<\/th>/);

    // Check for thead and tbody structure
    expect(mdContent).toMatch(/<thead>/);
    expect(mdContent).toMatch(/<\/thead>/);
    expect(mdContent).toMatch(/<tbody>/);
    expect(mdContent).toMatch(/<\/tbody>/);

    // Count table rows (should have 6 parameters)
    const tableRowMatches = mdContent.match(/<tr[^>]*id="param-[^"]*"[^>]*>/g);
    expect(tableRowMatches).not.toBeNull();
    expect(tableRowMatches.length).toBe(6);

    // Check for section container
    expect(mdContent).toMatch(/<section[^>]*id="configuration-table"[^>]*>/);
  });

  test('Configuration section has proper heading', () => {
    // Check for main heading
    expect(mdContent).toMatch(/# Configuration Reference/);

    // Check for parameters section heading
    expect(mdContent).toMatch(/## Configuration Parameters/);
  });

  test('Configuration section has example configuration', () => {
    // Check for example configuration section
    expect(mdContent).toMatch(/<section[^>]*id="example-configuration"[^>]*>/);
    expect(mdContent).toMatch(/## Example Configuration/);

    // Check for TOML code block
    expect(mdContent).toMatch(/```toml/);
  });
});

describe('Configuration Section - Built HTML Tests', () => {
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
    expect(builtHtml).toMatch(/max_levels/);
    expect(builtHtml).toMatch(/work_dir/);
    expect(builtHtml).toMatch(/sstable_max_size/);
    expect(builtHtml).toMatch(/memtable_max_size/);
    expect(builtHtml).toMatch(/block_size/);
  });

  test('Built HTML has configuration table (if built)', () => {
    if (!htmlExists) {
      console.log('Skipping built HTML test - book not built yet');
      return;
    }

    // Verify table structure is present
    expect(builtHtml).toMatch(/id="config-params-table"/);
    expect(builtHtml).toMatch(/0\.0\.0\.0:12333/);
    expect(builtHtml).toMatch(/\/tmp\/mirdb/);
  });

  test('Built HTML has all parameter defaults (if built)', () => {
    if (!htmlExists) {
      console.log('Skipping built HTML test - book not built yet');
      return;
    }

    // Check all default values are present
    expect(builtHtml).toMatch(/0\.0\.0\.0:12333/);
    expect(builtHtml).toMatch(/>7</);
    expect(builtHtml).toMatch(/\/tmp\/mirdb/);
    expect(builtHtml).toMatch(/100MB/);
    expect(builtHtml).toMatch(/4MB/);
    expect(builtHtml).toMatch(/4KB/);
  });
});
