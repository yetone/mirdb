/**
 * Roadmap Section Unit Tests
 * Owner: Scenario 6 - Feature Roadmap Display
 *
 * Tests:
 * - Tokio async networking is displayed as completed
 * - Skip-list memtable is displayed as completed
 * - Minor compaction is displayed as completed
 * - Major compaction is displayed as completed
 * - Raft consensus is displayed as planned/upcoming
 * - Completed and planned features have distinct visual styling
 */

const fs = require('fs');
const path = require('path');

describe('Roadmap Section - Source Content', () => {
  let htmlContent;

  beforeAll(() => {
    const indexPath = path.join(__dirname, '../../../src/index.md');
    htmlContent = fs.readFileSync(indexPath, 'utf8');
  });

  test('Section displays Tokio async networking as completed', () => {
    // Test case 1: Tokio async networking should be shown as completed
    expect(htmlContent).toMatch(/Tokio async networking/i);
    expect(htmlContent).toMatch(/<li[^>]*class="[^"]*roadmap-item[^"]*roadmap-completed[^"]*"[^>]*>[^<]*Tokio async networking/i);
  });

  test('Section displays skip-list memtable as completed', () => {
    // Test case 2: Skip-list memtable should be shown as completed
    expect(htmlContent).toMatch(/skip-list memtable/i);
    expect(htmlContent).toMatch(/<li[^>]*class="[^"]*roadmap-item[^"]*roadmap-completed[^"]*"[^>]*>[^<]*[Ss]kip-list memtable/i);
  });

  test('Section displays minor compaction as completed', () => {
    // Test case 3: Minor compaction should be shown as completed
    expect(htmlContent).toMatch(/[Mm]inor compaction/i);
    expect(htmlContent).toMatch(/<li[^>]*class="[^"]*roadmap-item[^"]*roadmap-completed[^"]*"[^>]*>[^<]*[Mm]inor compaction/i);
  });

  test('Section displays major compaction as completed', () => {
    // Test case 4: Major compaction should be shown as completed
    expect(htmlContent).toMatch(/[Mm]ajor compaction/i);
    expect(htmlContent).toMatch(/<li[^>]*class="[^"]*roadmap-item[^"]*roadmap-completed[^"]*"[^>]*>[^<]*[Mm]ajor compaction/i);
  });

  test('Section displays Raft consensus as planned/upcoming', () => {
    // Test case 5: Raft consensus should be shown as planned
    expect(htmlContent).toMatch(/Raft consensus/i);
    expect(htmlContent).toMatch(/<li[^>]*class="[^"]*roadmap-item[^"]*roadmap-planned[^"]*"[^>]*>[^<]*Raft consensus/i);
  });

  test('Completed and planned features have distinct visual styling classes', () => {
    // Test case 6: Both classes should exist for distinct styling
    expect(htmlContent).toMatch(/class="[^"]*roadmap-completed[^"]*"/);
    expect(htmlContent).toMatch(/class="[^"]*roadmap-planned[^"]*"/);

    // Verify the roadmap section exists with proper structure
    expect(htmlContent).toMatch(/<section[^>]*id="roadmap"[^>]*class="[^"]*roadmap[^"]*"[^>]*>/);
    expect(htmlContent).toMatch(/<ul[^>]*class="[^"]*roadmap-list[^"]*"[^>]*>/);
  });

  test('Roadmap section has proper heading', () => {
    expect(htmlContent).toMatch(/<h2[^>]*class="[^"]*roadmap-title[^"]*"[^>]*>/);
    expect(htmlContent).toMatch(/Roadmap|Feature Roadmap/i);
  });
});

describe('Roadmap Section - Built HTML Tests', () => {
  let builtHtml;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../../book/index.html');
    builtHtml = fs.readFileSync(htmlPath, 'utf8');
  });

  test('Built HTML has roadmap section', () => {
    expect(builtHtml).toMatch(/id="roadmap"/);
    expect(builtHtml).toMatch(/class="roadmap"/);
  });

  test('Built HTML shows all completed features', () => {
    expect(builtHtml).toMatch(/Tokio async networking/i);
    expect(builtHtml).toMatch(/skip-list memtable/i);
    expect(builtHtml).toMatch(/[Mm]inor compaction/);
    expect(builtHtml).toMatch(/[Mm]ajor compaction/);
  });

  test('Built HTML shows planned feature', () => {
    expect(builtHtml).toMatch(/Raft consensus/i);
  });

  test('Built HTML has distinct styling for completed and planned items', () => {
    expect(builtHtml).toMatch(/roadmap-completed/);
    expect(builtHtml).toMatch(/roadmap-planned/);
  });

  test('Built HTML roadmap section appears after quickstart section', () => {
    const quickstartIndex = builtHtml.indexOf('id="quickstart"');
    const roadmapIndex = builtHtml.indexOf('id="roadmap"');

    expect(quickstartIndex).toBeGreaterThan(-1);
    expect(roadmapIndex).toBeGreaterThan(-1);
    expect(roadmapIndex).toBeGreaterThan(quickstartIndex);
  });
});
