/**
 * Architecture Section Unit Tests
 * Owner: Scenario 5 - Architecture Overview
 *
 * Tests:
 * - Section contains description of memtables
 * - Section contains description of skip lists
 * - Section contains description of SSTables
 * - Section contains description of Write-Ahead Log (WAL)
 * - Section contains description of compaction process
 * - Section displays architecture diagram (SVG)
 * - Diagram has alt text for accessibility
 */

const fs = require('fs');
const path = require('path');

describe('Architecture Section', () => {
  let mdContent;

  beforeAll(() => {
    // Read the architecture.md file which contains the architecture section content
    const architecturePath = path.join(__dirname, '../../../src/architecture.md');
    mdContent = fs.readFileSync(architecturePath, 'utf8');
  });

  test('Section contains description of memtables (test case 1)', () => {
    // Test case 1: Section contains description of memtables
    // Check for memtable section heading
    expect(mdContent).toMatch(/## Memtables/);

    // Check for memtable description content
    expect(mdContent).toMatch(/memtable/i);
    expect(mdContent).toMatch(/in-memory data structure/i);
    expect(mdContent).toMatch(/write operations/i);

    // Verify section ID for navigation
    expect(mdContent).toMatch(/<section[^>]*id="memtables"[^>]*>/);
  });

  test('Section contains description of skip lists (test case 2)', () => {
    // Test case 2: Section contains description of skip lists
    // Check for skip list section heading
    expect(mdContent).toMatch(/## Skip Lists/);

    // Check for skip list description content
    expect(mdContent).toMatch(/skip list/i);
    expect(mdContent).toMatch(/probabilistic data structure/i);
    expect(mdContent).toMatch(/O\(log n\)/);

    // Verify section ID for navigation
    expect(mdContent).toMatch(/<section[^>]*id="skip-lists"[^>]*>/);
  });

  test('Section contains description of SSTables (test case 3)', () => {
    // Test case 3: Section contains description of SSTables
    // Check for SSTable section heading
    expect(mdContent).toMatch(/## SSTables/);

    // Check for SSTable description content
    expect(mdContent).toMatch(/Sorted String Tables/i);
    expect(mdContent).toMatch(/SSTable/);
    expect(mdContent).toMatch(/immutable/i);
    expect(mdContent).toMatch(/on-disk/i);

    // Verify section ID for navigation
    expect(mdContent).toMatch(/<section[^>]*id="sstables"[^>]*>/);
  });

  test('Section contains description of Write-Ahead Log (WAL) (test case 4)', () => {
    // Test case 4: Section contains description of Write-Ahead Log (WAL)
    // Check for WAL section heading
    expect(mdContent).toMatch(/## Write-Ahead Log \(WAL\)/);

    // Check for WAL description content
    expect(mdContent).toMatch(/Write-Ahead Log/);
    expect(mdContent).toMatch(/WAL/);
    expect(mdContent).toMatch(/durability/i);
    expect(mdContent).toMatch(/persisted/i);

    // Verify section ID for navigation
    expect(mdContent).toMatch(/<section[^>]*id="wal"[^>]*>/);
  });

  test('Section contains description of compaction process (test case 5)', () => {
    // Test case 5: Section contains description of compaction process
    // Check for compaction section heading
    expect(mdContent).toMatch(/## Compaction/);

    // Check for compaction description content
    expect(mdContent).toMatch(/compaction/i);
    expect(mdContent).toMatch(/Minor Compaction/);
    expect(mdContent).toMatch(/Major Compaction/);
    expect(mdContent).toMatch(/merge/i);

    // Verify section ID for navigation
    expect(mdContent).toMatch(/<section[^>]*id="compaction"[^>]*>/);
  });

  test('Section displays architecture diagram (SVG) (test case 6)', () => {
    // Test case 6: Section displays architecture diagram (SVG or rendered mermaid)
    // Check for SVG image reference
    expect(mdContent).toMatch(/<img[^>]*src="images\/architecture\.svg"[^>]*>/);

    // Check for diagram container
    expect(mdContent).toMatch(/class="architecture-diagram"/);

    // Verify diagram has an ID for reference
    expect(mdContent).toMatch(/id="architecture-diagram"/);
  });

  test('Diagram has alt text for accessibility (test case 7)', () => {
    // Test case 7: Diagram has alt text for accessibility
    // Check for alt text on the architecture diagram
    expect(mdContent).toMatch(/<img[^>]*alt="[^"]+"/);

    // Verify the alt text is descriptive (not empty or generic)
    const altMatch = mdContent.match(/<img[^>]*alt="([^"]+)"/);
    expect(altMatch).not.toBeNull();
    expect(altMatch[1].length).toBeGreaterThan(10); // Alt text should be meaningful
    expect(altMatch[1]).toMatch(/MirDB|architecture|diagram/i);
  });
});

describe('Architecture Section SVG Diagram', () => {
  let svgContent;

  beforeAll(() => {
    // Read the architecture.svg file
    const svgPath = path.join(__dirname, '../../../src/images/architecture.svg');
    svgContent = fs.readFileSync(svgPath, 'utf8');
  });

  test('SVG diagram exists and has valid structure', () => {
    // Verify SVG root element
    expect(svgContent).toMatch(/<svg[^>]*xmlns="http:\/\/www\.w3\.org\/2000\/svg"/);
    expect(svgContent).toMatch(/<\/svg>/);
  });

  test('SVG diagram has title element for accessibility', () => {
    // Check for title element (important for screen readers)
    expect(svgContent).toMatch(/<title[^>]*>[^<]+<\/title>/);
  });

  test('SVG diagram has desc element for accessibility', () => {
    // Check for desc element (provides longer description)
    expect(svgContent).toMatch(/<desc[^>]*>[^<]+<\/desc>/);
  });

  test('SVG diagram has role="img" for accessibility', () => {
    // Check for role attribute
    expect(svgContent).toMatch(/<svg[^>]*role="img"/);
  });

  test('SVG diagram shows key components', () => {
    // Verify diagram includes key architectural components
    expect(svgContent).toMatch(/Memtable/i);
    expect(svgContent).toMatch(/SSTable/i);
    expect(svgContent).toMatch(/WAL|Write-Ahead Log/i);
    expect(svgContent).toMatch(/Compaction/i);
  });

  test('SVG diagram shows data flow', () => {
    // Verify diagram shows Client and data flow
    expect(svgContent).toMatch(/Client/);
    expect(svgContent).toMatch(/Level/);
  });
});

describe('Architecture Section Built HTML Tests', () => {
  let builtHtml;
  let htmlExists = false;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../../book/architecture.html');
    try {
      builtHtml = fs.readFileSync(htmlPath, 'utf8');
      htmlExists = true;
    } catch (e) {
      // Built HTML may not exist yet
      htmlExists = false;
    }
  });

  test('Built HTML contains architecture content (if built)', () => {
    if (!htmlExists) {
      // Skip if not built yet
      console.log('Skipping built HTML test - book not built yet');
      return;
    }

    // Verify content is present in built HTML
    expect(builtHtml).toMatch(/Architecture Overview/);
    expect(builtHtml).toMatch(/memtable/i);
    expect(builtHtml).toMatch(/SSTable/i);
    expect(builtHtml).toMatch(/WAL/);
  });

  test('Built HTML has architecture diagram with alt text (if built)', () => {
    if (!htmlExists) {
      console.log('Skipping built HTML test - book not built yet');
      return;
    }

    // Verify diagram image is present with alt text
    expect(builtHtml).toMatch(/<img[^>]*src="images\/architecture\.svg"/);
    // Check that the architecture diagram has alt text containing MirDB
    expect(builtHtml).toMatch(/alt="MirDB LSM Tree Architecture Diagram/i);
  });
});
