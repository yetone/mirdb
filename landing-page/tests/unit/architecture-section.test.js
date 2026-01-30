/**
 * Architecture Section Unit Tests
 * Owner: Scenario 5 - Architecture Section with Diagram
 *
 * Tests for validating Architecture section HTML structure
 */
const fs = require('fs');
const path = require('path');

describe('Architecture Section Unit Tests', () => {
  let htmlContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');
  });

  describe('TC1: Architecture Section HTML Structure', () => {
    test('Contains architecture section with correct id', () => {
      expect(htmlContent).toMatch(/<section[^>]*id="architecture"[^>]*class="[^"]*architecture[^"]*"/);
    });

    test('Contains section title "How It Works"', () => {
      expect(htmlContent).toMatch(/<h2[^>]*class="architecture__title"[^>]*>How It Works<\/h2>/);
    });

    test('Contains Mermaid diagram container', () => {
      expect(htmlContent).toMatch(/<div[^>]*class="architecture__diagram-container"/);
    });

    test('Contains Mermaid diagram element with id', () => {
      expect(htmlContent).toMatch(/<div[^>]*class="architecture__diagram mermaid"[^>]*id="architecture-diagram"/);
    });

    test('Contains component descriptions container', () => {
      expect(htmlContent).toMatch(/<div[^>]*class="architecture__components"/);
    });

    test('Contains components grid', () => {
      expect(htmlContent).toMatch(/<div[^>]*class="architecture__components-grid"/);
    });

    test('Contains four component articles', () => {
      const componentMatches = htmlContent.match(/<article[^>]*class="architecture__component"[^>]*data-component="[^"]*"/g);
      expect(componentMatches).toHaveLength(4);
    });

    test('Contains intro paragraph', () => {
      expect(htmlContent).toMatch(/<p[^>]*class="architecture__intro"[^>]*>/);
      expect(htmlContent).toMatch(/LSM-tree/i);
    });
  });

  describe('TC3: Write Path in Diagram', () => {
    test('Diagram contains Client node', () => {
      expect(htmlContent).toMatch(/Client/);
    });

    test('Diagram contains Memtable node', () => {
      expect(htmlContent).toMatch(/Memtable/);
    });

    test('Diagram contains WAL (Write-Ahead Log) node', () => {
      expect(htmlContent).toMatch(/WAL/);
      expect(htmlContent).toMatch(/Write-Ahead Log/);
    });

    test('Diagram contains SSTable node', () => {
      expect(htmlContent).toMatch(/SSTable/);
    });

    test('Write path section is defined in diagram', () => {
      expect(htmlContent).toMatch(/WritePathSection\["Write Path"\]/);
    });

    test('Write path flow: Client -> Memtable', () => {
      expect(htmlContent).toMatch(/Client1\["Client"\]\s*-->\s*Memtable\["Memtable/);
    });

    test('Write path flow: Memtable -> WAL', () => {
      // In Mermaid, after first definition, nodes are referenced by their ID
      expect(htmlContent).toContain('Memtable --> WAL["WAL (Write-Ahead Log)"]');
    });

    test('Write path flow: WAL -> SSTable', () => {
      expect(htmlContent).toContain('WAL --> SSTable1["SSTable (Level 0)"]');
    });
  });

  describe('TC4: Read Path in Diagram', () => {
    test('Read path section is defined in diagram', () => {
      expect(htmlContent).toMatch(/ReadPathSection\["Read Path"\]/);
    });

    test('Read path flow: Client -> Memtable', () => {
      expect(htmlContent).toMatch(/Client2\["Client"\]\s*-->\s*MemtableRead\["Memtable"\]/);
    });

    test('Read path flow: Memtable -> SSTables', () => {
      // Note: The diagram uses unicode arrow → in "L0 → Ln"
      // Using includes instead of regex for simplicity with unicode
      expect(htmlContent).toContain('MemtableRead["Memtable"]');
      expect(htmlContent).toContain('SSTablesRead["SSTables (L0');
      expect(htmlContent).toMatch(/MemtableRead\s*-->\s*SSTablesRead/);
    });
  });

  describe('TC5: Component Explanations', () => {
    test('Contains Memtable component explanation', () => {
      expect(htmlContent).toMatch(/data-component="memtable"/);
      expect(htmlContent).toMatch(/<h4[^>]*class="architecture__component-title"[^>]*>Memtable<\/h4>/);
      expect(htmlContent).toMatch(/in-memory data structure/i);
    });

    test('Contains SSTables component explanation', () => {
      expect(htmlContent).toMatch(/data-component="sstables"/);
      expect(htmlContent).toMatch(/<h4[^>]*class="architecture__component-title"[^>]*>SSTables<\/h4>/);
      expect(htmlContent).toMatch(/Sorted String Tables/i);
    });

    test('Contains WAL component explanation', () => {
      expect(htmlContent).toMatch(/data-component="wal"/);
      expect(htmlContent).toMatch(/<h4[^>]*class="architecture__component-title"[^>]*>Write-Ahead Log \(WAL\)<\/h4>/);
      expect(htmlContent).toMatch(/durable log/i);
    });

    test('Contains Compaction component explanation', () => {
      expect(htmlContent).toMatch(/data-component="compaction"/);
      expect(htmlContent).toMatch(/<h4[^>]*class="architecture__component-title"[^>]*>Compaction<\/h4>/);
      expect(htmlContent).toMatch(/merges and sorts SSTables/i);
    });

    test('Each component has icon, title, and description', () => {
      // Check all components have the required structure
      const components = ['memtable', 'sstables', 'wal', 'compaction'];
      components.forEach(component => {
        const componentRegex = new RegExp(
          `data-component="${component}"[^>]*>[\\s\\S]*?` +
          `architecture__component-icon[\\s\\S]*?` +
          `architecture__component-title[\\s\\S]*?` +
          `architecture__component-description`,
          'i'
        );
        expect(htmlContent).toMatch(componentRegex);
      });
    });
  });

  describe('Mermaid Configuration', () => {
    test('Mermaid script is included from CDN', () => {
      expect(htmlContent).toMatch(/<script[^>]*src="https:\/\/cdn\.jsdelivr\.net\/npm\/mermaid/);
    });

    test('Mermaid is initialized with dark theme', () => {
      expect(htmlContent).toMatch(/mermaid\.initialize/);
      expect(htmlContent).toMatch(/theme:\s*['"]dark['"]/);
    });

    test('Mermaid is configured to start on load', () => {
      expect(htmlContent).toMatch(/startOnLoad:\s*true/);
    });
  });

  describe('Compaction Process in Diagram', () => {
    test('Compaction section is defined in diagram', () => {
      expect(htmlContent).toMatch(/CompactionSection\["Compaction Process"\]/);
    });

    test('Minor compaction is shown', () => {
      expect(htmlContent).toMatch(/MinorCompaction\["Minor Compaction"\]/);
    });

    test('Major compaction is shown', () => {
      expect(htmlContent).toMatch(/MajorCompaction\["Major Compaction"\]/);
    });
  });
});
