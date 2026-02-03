/**
 * Unit tests for Architecture component.
 * Owner: Scenario 4 - Architecture Overview Section
 */
import { describe, it, expect } from 'vitest';
import { readFileSync, existsSync } from 'fs';
import { join } from 'path';

const architectureComponent = readFileSync(
  join(__dirname, '../../../src/components/Architecture.astro'),
  'utf-8'
);

const architectureDiagramPath = join(__dirname, '../../../public/architecture-diagram.svg');
const architectureDiagramExists = existsSync(architectureDiagramPath);
const architectureDiagram = architectureDiagramExists
  ? readFileSync(architectureDiagramPath, 'utf-8')
  : '';

describe('Architecture Section', () => {
  describe('Architecture Component Structure', () => {
    it('should have a section with id="architecture"', () => {
      expect(architectureComponent).toContain('id="architecture"');
    });

    it('should have a section heading', () => {
      expect(architectureComponent).toContain('<h2');
      expect(architectureComponent).toContain('Architecture Overview');
    });

    it('should have responsive grid layout', () => {
      expect(architectureComponent).toContain('grid');
      expect(architectureComponent).toContain('grid-cols-1');
      expect(architectureComponent).toContain('lg:grid-cols-2');
    });

    it('should import GITHUB_URL from constants', () => {
      expect(architectureComponent).toContain("import { GITHUB_URL } from '../utils/constants'");
    });
  });

  describe('Data Flow Diagram Display', () => {
    it('should display the architecture diagram image', () => {
      expect(architectureComponent).toContain('<img');
      expect(architectureComponent).toContain('src="/architecture-diagram.svg"');
    });

    it('should have proper alt text describing the data flow for screen readers', () => {
      expect(architectureComponent).toContain('alt=');
      // Check that alt text contains key data flow elements
      expect(architectureComponent).toContain('Client');
      expect(architectureComponent).toContain('Memcached Protocol');
      expect(architectureComponent).toContain('Memtable');
      expect(architectureComponent).toContain('SSTable');
    });

    it('should have the diagram with lazy loading', () => {
      expect(architectureComponent).toContain('loading="lazy"');
    });

    it('should have a Data Flow heading', () => {
      expect(architectureComponent).toContain('Data Flow');
    });
  });

  describe('LSM-tree Architecture Explanation', () => {
    it('should have an LSM-tree Architecture heading', () => {
      expect(architectureComponent).toContain('LSM-tree Architecture');
    });

    it('should accurately describe memtable concept', () => {
      expect(architectureComponent).toContain('Memtable');
      expect(architectureComponent).toContain('Active');
      expect(architectureComponent).toContain('in-memory');
      expect(architectureComponent).toContain('skip list');
    });

    it('should accurately describe immutable memtable concept', () => {
      expect(architectureComponent).toContain('Immutable Memtable');
      expect(architectureComponent).toContain('immutable');
      expect(architectureComponent).toContain('compaction');
    });

    it('should accurately describe SSTable concept', () => {
      expect(architectureComponent).toContain('SSTable');
      expect(architectureComponent).toContain('Sorted String Table');
      expect(architectureComponent).toContain('disk');
      expect(architectureComponent).toContain('Level');
    });

    it('should explain minor and major compaction', () => {
      expect(architectureComponent.toLowerCase()).toContain('compaction');
      // Should mention both types of compaction
      expect(architectureComponent).toContain('Level 0');
      expect(architectureComponent).toContain('Major compaction');
    });
  });

  describe('Documentation Link', () => {
    it('should have a link to detailed architecture documentation', () => {
      expect(architectureComponent).toContain('architecture-doc-link');
      expect(architectureComponent).toContain('href={architectureDocUrl}');
    });

    it('should have external link security attributes', () => {
      expect(architectureComponent).toContain('target="_blank"');
      expect(architectureComponent).toContain('rel="noopener noreferrer"');
    });

    it('should have descriptive link text', () => {
      expect(architectureComponent).toContain('View detailed architecture documentation');
    });
  });

  describe('Dark Mode Support', () => {
    it('should have dark mode classes for section background', () => {
      expect(architectureComponent).toContain('dark:bg-gray-800');
    });

    it('should have dark mode classes for text colors', () => {
      expect(architectureComponent).toContain('dark:text-gray-100');
      expect(architectureComponent).toContain('dark:text-gray-400');
    });

    it('should have dark mode classes for card backgrounds', () => {
      expect(architectureComponent).toContain('dark:bg-gray-900');
    });
  });

  describe('Accessibility', () => {
    it('should have proper heading hierarchy', () => {
      expect(architectureComponent).toContain('<h2');
      expect(architectureComponent).toContain('<h3');
      expect(architectureComponent).toContain('<h4');
    });

    it('should have aria-hidden on decorative SVG icon', () => {
      expect(architectureComponent).toContain('aria-hidden="true"');
    });
  });
});

describe('Architecture Diagram SVG', () => {
  it('should exist in the public folder', () => {
    expect(architectureDiagramExists).toBe(true);
  });

  it('should have role="img" for accessibility', () => {
    expect(architectureDiagram).toContain('role="img"');
  });

  it('should have title and desc elements for screen readers', () => {
    expect(architectureDiagram).toContain('<title');
    expect(architectureDiagram).toContain('<desc');
    expect(architectureDiagram).toContain('aria-labelledby');
  });

  it('should have title describing the diagram', () => {
    expect(architectureDiagram).toContain('MirDB');
    expect(architectureDiagram).toContain('Data Flow');
  });

  it('should have desc with comprehensive description for screen readers', () => {
    expect(architectureDiagram).toContain('Client');
    expect(architectureDiagram).toContain('Memcached Protocol');
    expect(architectureDiagram).toContain('Memtable');
    expect(architectureDiagram).toContain('Immutable Memtable');
    expect(architectureDiagram).toContain('SSTable');
    expect(architectureDiagram).toContain('compaction');
  });

  it('should display Client component', () => {
    expect(architectureDiagram).toContain('Client');
  });

  it('should display Protocol Parser component', () => {
    expect(architectureDiagram).toContain('Protocol');
    expect(architectureDiagram).toContain('Parser');
  });

  it('should display Memtable component', () => {
    expect(architectureDiagram).toContain('Memtable');
    expect(architectureDiagram).toContain('Skip List');
  });

  it('should display Immutable Memtable component', () => {
    expect(architectureDiagram).toContain('Immutable Memtable');
  });

  it('should display SSTable levels', () => {
    expect(architectureDiagram).toContain('SSTable Level 0');
    expect(architectureDiagram).toContain('SSTable Level 1');
  });

  it('should show compaction labels', () => {
    expect(architectureDiagram).toContain('minor compaction');
    expect(architectureDiagram).toContain('major compaction');
  });
});
