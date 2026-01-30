/**
 * Architecture Component Unit Tests.
 * Owner: Scenario 4 - Architecture Diagram Section
 *
 * Tests:
 * - Architecture section renders
 * - Diagram image/SVG is present with alt text
 * - LSM-tree explanation is present
 * - Compaction explanation is present
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { renderArchitecture } from '../../../src/components/Architecture';

// Mock the SVG import
vi.mock('../../../src/assets/images/architecture-diagram.svg', () => ({
  default: '/assets/images/architecture-diagram.svg',
}));

describe('Architecture Section', () => {
  let architectureSection: HTMLElement;

  beforeEach(() => {
    architectureSection = renderArchitecture();
  });

  // Test Case 1: Section renders with diagram container and explanatory text
  describe('Section Rendering', () => {
    it('should render architecture section element', () => {
      expect(architectureSection).toBeDefined();
      expect(architectureSection.tagName.toLowerCase()).toBe('section');
    });

    it('should have correct id attribute', () => {
      expect(architectureSection.getAttribute('id')).toBe('architecture');
    });

    it('should have architecture-section class', () => {
      expect(architectureSection.classList.contains('architecture-section')).toBe(true);
    });

    it('should have proper aria-labelledby for accessibility', () => {
      expect(architectureSection.getAttribute('aria-labelledby')).toBe('architecture-heading');
    });

    it('should have a heading with correct id', () => {
      const heading = architectureSection.querySelector('#architecture-heading');
      expect(heading).not.toBeNull();
      expect(heading?.tagName.toLowerCase()).toBe('h2');
    });

    it('should contain diagram container', () => {
      const diagramContainer = architectureSection.querySelector('.architecture-diagram-container');
      expect(diagramContainer).not.toBeNull();
    });

    it('should contain explanation cards', () => {
      const explanationCards = architectureSection.querySelectorAll('.explanation-card');
      expect(explanationCards.length).toBeGreaterThanOrEqual(2);
    });
  });

  // Test Case 2: Architecture diagram image/SVG loads with proper alt text
  describe('Diagram Presence', () => {
    it('should render architecture diagram image', () => {
      const diagram = architectureSection.querySelector('.architecture-diagram');
      expect(diagram).not.toBeNull();
      expect(diagram?.tagName.toLowerCase()).toBe('img');
    });

    it('should have src attribute pointing to SVG file', () => {
      const diagram = architectureSection.querySelector('.architecture-diagram') as HTMLImageElement;
      expect(diagram?.getAttribute('src')).toContain('architecture-diagram.svg');
    });

    it('should have lazy loading attribute for performance', () => {
      const diagram = architectureSection.querySelector('.architecture-diagram') as HTMLImageElement;
      expect(diagram?.getAttribute('loading')).toBe('lazy');
    });
  });

  // Test Case 3: Text explains Log-Structured Merge-tree approach with memtables and SSTables
  describe('LSM-tree Explanation', () => {
    it('should contain LSM-tree explanation section', () => {
      const lsmExplanation = architectureSection.querySelector('.lsm-tree-explanation');
      expect(lsmExplanation).not.toBeNull();
    });

    it('should mention Log-Structured Merge-tree in content', () => {
      const textContent = architectureSection.textContent?.toLowerCase() || '';
      expect(textContent).toContain('log-structured merge-tree');
    });

    it('should explain memtable concept', () => {
      const textContent = architectureSection.textContent?.toLowerCase() || '';
      expect(textContent).toContain('memtable');
    });

    it('should explain SSTable concept', () => {
      const textContent = architectureSection.textContent || '';
      // SSTable or Sorted String Table
      expect(
        textContent.toLowerCase().includes('sstable') ||
        textContent.toLowerCase().includes('sorted string table')
      ).toBe(true);
    });

    it('should mention in-memory storage for memtable', () => {
      const lsmExplanation = architectureSection.querySelector('.lsm-tree-explanation');
      const textContent = lsmExplanation?.textContent?.toLowerCase() || '';
      expect(textContent).toContain('in-memory');
    });

    it('should mention skip list data structure', () => {
      const textContent = architectureSection.textContent?.toLowerCase() || '';
      expect(textContent).toContain('skip list');
    });
  });

  // Test Case 4: Text mentions minor compaction (memtable to SSTable) and major compaction (level compaction)
  describe('Compaction Explanation', () => {
    it('should contain compaction explanation section', () => {
      const compactionExplanation = architectureSection.querySelector('.compaction-explanation');
      expect(compactionExplanation).not.toBeNull();
    });

    it('should mention minor compaction', () => {
      const textContent = architectureSection.textContent?.toLowerCase() || '';
      expect(textContent).toContain('minor compaction');
    });

    it('should mention major compaction', () => {
      const textContent = architectureSection.textContent?.toLowerCase() || '';
      expect(textContent).toContain('major compaction');
    });

    it('should explain that minor compaction flushes memtable to disk', () => {
      const compactionExplanation = architectureSection.querySelector('.compaction-explanation');
      const textContent = compactionExplanation?.textContent?.toLowerCase() || '';
      // Should mention memtable being flushed to Level 0 or disk
      expect(
        textContent.includes('flush') ||
        textContent.includes('level 0')
      ).toBe(true);
    });

    it('should explain level compaction concept in major compaction', () => {
      const compactionExplanation = architectureSection.querySelector('.compaction-explanation');
      const textContent = compactionExplanation?.textContent?.toLowerCase() || '';
      // Should mention merging levels or level-based compaction
      expect(
        textContent.includes('level') &&
        (textContent.includes('merge') || textContent.includes('next level'))
      ).toBe(true);
    });

    it('should mention background processing for compaction', () => {
      const textContent = architectureSection.textContent?.toLowerCase() || '';
      expect(textContent).toContain('background');
    });
  });

  // Test Case 5: Diagram has appropriate alt text describing the architecture flow
  describe('Diagram Accessibility', () => {
    it('should have non-empty alt text on diagram image', () => {
      const diagram = architectureSection.querySelector('.architecture-diagram') as HTMLImageElement;
      const altText = diagram?.getAttribute('alt');
      expect(altText).toBeDefined();
      expect(altText).not.toBe('');
      expect(altText?.length).toBeGreaterThan(20);
    });

    it('should describe data flow in alt text (Client to Storage)', () => {
      const diagram = architectureSection.querySelector('.architecture-diagram') as HTMLImageElement;
      const altText = diagram?.getAttribute('alt')?.toLowerCase() || '';
      expect(altText).toContain('client');
      expect(altText).toContain('storage');
    });

    it('should mention Memcached Protocol in alt text', () => {
      const diagram = architectureSection.querySelector('.architecture-diagram') as HTMLImageElement;
      const altText = diagram?.getAttribute('alt')?.toLowerCase() || '';
      expect(altText).toContain('memcached protocol');
    });

    it('should mention Memtable in alt text', () => {
      const diagram = architectureSection.querySelector('.architecture-diagram') as HTMLImageElement;
      const altText = diagram?.getAttribute('alt')?.toLowerCase() || '';
      expect(altText).toContain('memtable');
    });

    it('should mention SSTable in alt text', () => {
      const diagram = architectureSection.querySelector('.architecture-diagram') as HTMLImageElement;
      const altText = diagram?.getAttribute('alt')?.toLowerCase() || '';
      expect(altText).toContain('sstable');
    });

    it('should mention compaction in alt text', () => {
      const diagram = architectureSection.querySelector('.architecture-diagram') as HTMLImageElement;
      const altText = diagram?.getAttribute('alt')?.toLowerCase() || '';
      expect(altText).toContain('compaction');
    });
  });

  // Additional tests for section structure and content
  describe('Section Structure', () => {
    it('should have a header with title', () => {
      const header = architectureSection.querySelector('.architecture-header');
      expect(header).not.toBeNull();

      const title = header?.querySelector('h2');
      expect(title).not.toBeNull();
      expect(title?.textContent?.toLowerCase()).toContain('architecture');
    });

    it('should have introductory text about LSM-tree', () => {
      const header = architectureSection.querySelector('.architecture-header');
      const introText = header?.querySelector('p');
      expect(introText).not.toBeNull();
      expect(introText?.textContent?.toLowerCase()).toContain('lsm');
    });

    it('should have key benefits section', () => {
      const highlights = architectureSection.querySelector('.architecture-highlights');
      expect(highlights).not.toBeNull();
    });

    it('should list high write throughput as a benefit', () => {
      const textContent = architectureSection.textContent?.toLowerCase() || '';
      expect(textContent).toContain('write throughput');
    });

    it('should have proper heading hierarchy', () => {
      const h2 = architectureSection.querySelectorAll('h2');
      const h3 = architectureSection.querySelectorAll('h3');

      expect(h2.length).toBe(1);
      expect(h3.length).toBeGreaterThanOrEqual(2);
    });
  });

  describe('Responsive Design Classes', () => {
    it('should have responsive grid for explanation cards', () => {
      const explanationsGrid = architectureSection.querySelector('.architecture-explanations');
      expect(explanationsGrid).not.toBeNull();
      expect(explanationsGrid?.classList.contains('grid')).toBe(true);
      expect(explanationsGrid?.classList.contains('md:grid-cols-2')).toBe(true);
    });

    it('should have container with max-width for proper layout', () => {
      const container = architectureSection.querySelector('.architecture-container');
      expect(container).not.toBeNull();
      expect(container?.classList.contains('max-w-6xl')).toBe(true);
    });
  });
});
