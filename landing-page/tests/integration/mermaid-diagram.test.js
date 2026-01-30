/**
 * Mermaid Diagram Integration Tests
 * Owner: Scenario 5 - Architecture Section with Diagram
 *
 * Tests for validating Mermaid diagram rendering
 */
const fs = require('fs');
const path = require('path');

describe('Mermaid Diagram Integration Tests', () => {
  let htmlContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');
  });

  describe('TC2: Mermaid Diagram Configuration', () => {
    test('Mermaid library is loaded from CDN', () => {
      expect(htmlContent).toMatch(/cdn\.jsdelivr\.net\/npm\/mermaid/);
    });

    test('Mermaid container has correct class for rendering', () => {
      expect(htmlContent).toMatch(/class="architecture__diagram mermaid"/);
    });

    test('Diagram content uses flowchart syntax', () => {
      expect(htmlContent).toMatch(/flowchart TB/);
    });

    test('Mermaid theme is configured for dark mode', () => {
      expect(htmlContent).toMatch(/theme:\s*['"]dark['"]/);
    });

    test('Mermaid theme variables match design system colors', () => {
      // Primary background color
      expect(htmlContent).toMatch(/primaryColor:\s*['"]#0f3460['"]/);
      // Text color
      expect(htmlContent).toMatch(/primaryTextColor:\s*['"]#eaeaea['"]/);
      // Accent/border color
      expect(htmlContent).toMatch(/primaryBorderColor:\s*['"]#e94560['"]/);
    });

    test('Diagram contains subgraph definitions', () => {
      expect(htmlContent).toMatch(/subgraph WritePathSection/);
      expect(htmlContent).toMatch(/subgraph ReadPathSection/);
      expect(htmlContent).toMatch(/subgraph CompactionSection/);
    });
  });

  describe('Diagram Structure Validation', () => {
    test('Write path subgraph has correct label', () => {
      expect(htmlContent).toMatch(/WritePathSection\["Write Path"\]/);
    });

    test('Read path subgraph has correct label', () => {
      expect(htmlContent).toMatch(/ReadPathSection\["Read Path"\]/);
    });

    test('Compaction subgraph has correct label', () => {
      expect(htmlContent).toMatch(/CompactionSection\["Compaction Process"\]/);
    });

    test('Flowchart uses TB (top-to-bottom) direction', () => {
      expect(htmlContent).toMatch(/flowchart TB/);
    });

    test('Subgraphs use direction TB', () => {
      const directionMatches = htmlContent.match(/direction TB/g);
      expect(directionMatches).not.toBeNull();
      expect(directionMatches.length).toBeGreaterThanOrEqual(3);
    });
  });

  describe('Node Definitions', () => {
    test('Client nodes are defined', () => {
      expect(htmlContent).toMatch(/Client1\["Client"\]/);
      expect(htmlContent).toMatch(/Client2\["Client"\]/);
    });

    test('Memtable nodes are defined', () => {
      expect(htmlContent).toMatch(/Memtable\["Memtable \(In-Memory\)"\]/);
      expect(htmlContent).toMatch(/MemtableRead\["Memtable"\]/);
    });

    test('WAL node is defined', () => {
      expect(htmlContent).toMatch(/WAL\["WAL \(Write-Ahead Log\)"\]/);
    });

    test('SSTable nodes are defined', () => {
      expect(htmlContent).toMatch(/SSTable1\["SSTable \(Level 0\)"\]/);
      expect(htmlContent).toMatch(/SSTablesRead\["SSTables \(L0 → Ln\)"\]/);
    });

    test('Compaction level nodes are defined', () => {
      expect(htmlContent).toMatch(/SSTableL0\["Level 0 SSTables"\]/);
      expect(htmlContent).toMatch(/SSTableL1\["Level 1 SSTables"\]/);
      expect(htmlContent).toMatch(/SSTableLn\["Level N SSTables"\]/);
    });
  });

  describe('Edge/Connection Definitions', () => {
    test('Write path has correct flow connections', () => {
      // Client -> Memtable -> WAL -> SSTable
      // Note: In Mermaid, node IDs are reused after first definition
      expect(htmlContent).toContain('Client1["Client"] --> Memtable["Memtable (In-Memory)"]');
      expect(htmlContent).toContain('Memtable --> WAL["WAL (Write-Ahead Log)"]');
      expect(htmlContent).toContain('WAL --> SSTable1["SSTable (Level 0)"]');
    });

    test('Read path has correct flow connections', () => {
      // Client -> Memtable -> SSTables
      expect(htmlContent).toContain('Client2["Client"] --> MemtableRead["Memtable"]');
      expect(htmlContent).toContain('MemtableRead --> SSTablesRead["SSTables (L0');
    });

    test('Compaction path has correct flow connections', () => {
      // L0 -> Minor -> L1 -> Major -> Ln
      expect(htmlContent).toContain('SSTableL0["Level 0 SSTables"] --> MinorCompaction["Minor Compaction"]');
      expect(htmlContent).toContain('MinorCompaction --> SSTableL1["Level 1 SSTables"]');
      expect(htmlContent).toContain('SSTableL1 --> MajorCompaction["Major Compaction"]');
      expect(htmlContent).toContain('MajorCompaction --> SSTableLn["Level N SSTables"]');
    });
  });

  describe('Accessibility', () => {
    test('Diagram container allows horizontal scrolling on overflow', () => {
      const cssPath = path.join(__dirname, '../../css/components/architecture.css');
      const cssContent = fs.readFileSync(cssPath, 'utf-8');
      expect(cssContent).toMatch(/overflow-x:\s*auto/);
    });

    test('Diagram has minimum height for visibility', () => {
      const cssPath = path.join(__dirname, '../../css/components/architecture.css');
      const cssContent = fs.readFileSync(cssPath, 'utf-8');
      expect(cssContent).toMatch(/min-height:\s*300px/);
    });
  });
});
