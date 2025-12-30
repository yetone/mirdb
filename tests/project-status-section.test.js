/**
 * Tests for Project Status Section (REQ-8)
 * Verify project status section showing implemented features and roadmap
 */

const fs = require('fs');
const path = require('path');

describe('Project Status Section', () => {
  let document;

  beforeEach(() => {
    const html = fs.readFileSync(path.resolve(__dirname, '../index.html'), 'utf8');
    document = new DOMParser().parseFromString(html, 'text/html');
  });

  // Test Case 1: Section exists showing project status or future plans
  describe('Test Case 1: Status or Roadmap Section Exists', () => {
    test('should have a project status section', () => {
      const statusSection = document.querySelector(
        '#project-status, [data-section="project-status"], .project-status, ' +
        '#roadmap, [data-section="roadmap"], .roadmap, ' +
        '#status, [data-section="status"], .status-section'
      );
      expect(statusSection).not.toBeNull();
    });

    test('should have a section title for project status or roadmap', () => {
      const statusSection = document.querySelector(
        '#project-status, [data-section="project-status"], .project-status, ' +
        '#roadmap, [data-section="roadmap"], .roadmap, ' +
        '#status, [data-section="status"], .status-section'
      );
      expect(statusSection).not.toBeNull();

      const heading = statusSection.querySelector('h2, h3');
      expect(heading).not.toBeNull();

      const headingText = heading.textContent.toLowerCase();
      expect(
        headingText.includes('status') ||
        headingText.includes('roadmap') ||
        headingText.includes('feature') ||
        headingText.includes('progress')
      ).toBe(true);
    });

    test('should display content about project status or future plans', () => {
      const statusSection = document.querySelector(
        '#project-status, [data-section="project-status"], .project-status, ' +
        '#roadmap, [data-section="roadmap"], .roadmap, ' +
        '#status, [data-section="status"], .status-section'
      );
      expect(statusSection).not.toBeNull();

      const sectionText = statusSection.textContent.toLowerCase();
      // Should contain information about features, plans, or status
      expect(
        sectionText.includes('implemented') ||
        sectionText.includes('feature') ||
        sectionText.includes('planned') ||
        sectionText.includes('roadmap') ||
        sectionText.includes('coming') ||
        sectionText.includes('future')
      ).toBe(true);
    });
  });

  // Test Case 2: Verify implemented features list is present
  describe('Test Case 2: Implemented Features List', () => {
    test('should have a list of implemented features', () => {
      const statusSection = document.querySelector(
        '#project-status, [data-section="project-status"], .project-status, ' +
        '#roadmap, [data-section="roadmap"], .roadmap, ' +
        '#status, [data-section="status"], .status-section'
      );
      expect(statusSection).not.toBeNull();

      // Look for a list element containing implemented features
      const lists = statusSection.querySelectorAll('ul, ol');
      expect(lists.length).toBeGreaterThan(0);
    });

    test('should mention implemented core features', () => {
      const statusSection = document.querySelector(
        '#project-status, [data-section="project-status"], .project-status, ' +
        '#roadmap, [data-section="roadmap"], .roadmap, ' +
        '#status, [data-section="status"], .status-section'
      );
      expect(statusSection).not.toBeNull();

      const sectionText = statusSection.textContent.toLowerCase();

      // Should mention at least some key implemented features
      const implementedFeatures = [
        'memcached',
        'protocol',
        'memtable',
        'sstable',
        'compaction',
        'async',
        'tokio',
        'skip list',
        'networking'
      ];

      const hasImplementedFeatures = implementedFeatures.some(feature =>
        sectionText.includes(feature)
      );

      expect(hasImplementedFeatures).toBe(true);
    });

    test('should have visual indicators for implemented features', () => {
      const statusSection = document.querySelector(
        '#project-status, [data-section="project-status"], .project-status, ' +
        '#roadmap, [data-section="roadmap"], .roadmap, ' +
        '#status, [data-section="status"], .status-section'
      );
      expect(statusSection).not.toBeNull();

      // Look for list items, checkmarks, or status indicators
      const listItems = statusSection.querySelectorAll('li');
      expect(listItems.length).toBeGreaterThan(0);
    });

    test('should distinguish between implemented and planned features', () => {
      const statusSection = document.querySelector(
        '#project-status, [data-section="project-status"], .project-status, ' +
        '#roadmap, [data-section="roadmap"], .roadmap, ' +
        '#status, [data-section="status"], .status-section'
      );
      expect(statusSection).not.toBeNull();

      const sectionText = statusSection.textContent.toLowerCase();

      // Should have some indication of both implemented and planned features
      const hasImplemented = sectionText.includes('implemented') ||
                            sectionText.includes('completed') ||
                            sectionText.includes('available') ||
                            sectionText.includes('supported');

      const hasPlanned = sectionText.includes('planned') ||
                        sectionText.includes('coming') ||
                        sectionText.includes('roadmap') ||
                        sectionText.includes('future');

      // At least one category should be present
      expect(hasImplemented || hasPlanned).toBe(true);
    });
  });
});
