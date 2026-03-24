/**
 * Content Validation Unit Tests
 * Owner: Scenario 6 - Configuration Reference / Scenario 14 - Content Accuracy
 *
 * Tests for:
 * - Configuration values match PRD specifications
 * - Content accuracy validation
 */

const { readFileSync } = require('fs');
const { resolve } = require('path');

describe('Configuration Values Content Validation', () => {
    let htmlContent;

    beforeAll(() => {
        // Read the HTML file
        const htmlPath = resolve(__dirname, '../../src/index.html');
        htmlContent = readFileSync(htmlPath, 'utf8');
    });

    // Test Case 2: Default listen address
    test('should contain default listen address 0.0.0.0:12333', () => {
        expect(htmlContent).toContain('0.0.0.0:12333');
    });

    // Test Case 3: Default max LSM levels
    test('should contain default max LSM levels (7)', () => {
        // Check for the value 7 in the context of max_lsm_levels
        expect(htmlContent).toContain('data-config="max-lsm-levels"');
        expect(htmlContent).toMatch(/>7</);
    });

    // Test Case 4: Default work directory
    test('should contain default work directory (/tmp/mirdb)', () => {
        expect(htmlContent).toContain('/tmp/mirdb');
    });

    // Test Case 5: Default SSTable max size
    test('should contain default SSTable max size (100MB)', () => {
        expect(htmlContent).toContain('100MB');
    });

    // Test Case 6: Default memtable max size
    test('should contain default memtable max size (4MB)', () => {
        expect(htmlContent).toContain('4MB');
    });

    // Additional content validation tests
    test('should have configuration section with id="config"', () => {
        expect(htmlContent).toContain('id="config"');
    });

    test('should have configuration table with proper structure', () => {
        expect(htmlContent).toContain('class="config-table"');
        expect(htmlContent).toContain('<thead>');
        expect(htmlContent).toContain('<tbody>');
    });

    test('should have listen_address parameter documented', () => {
        expect(htmlContent).toMatch(/listen_address/i);
    });

    test('should have max_lsm_levels parameter documented', () => {
        expect(htmlContent).toMatch(/max_lsm_levels/i);
    });

    test('should have work_directory parameter documented', () => {
        expect(htmlContent).toMatch(/work_directory/i);
    });

    test('should have sstable_max_size parameter documented', () => {
        expect(htmlContent).toMatch(/sstable_max_size/i);
    });

    test('should have memtable_max_size parameter documented', () => {
        expect(htmlContent).toMatch(/memtable_max_size/i);
    });

    test('should have block_size parameter documented', () => {
        expect(htmlContent).toMatch(/block_size/i);
    });

    test('block_size should have value 4KB', () => {
        expect(htmlContent).toContain('4KB');
    });
});
