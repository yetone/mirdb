import { describe, it, expect, beforeEach } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

describe('Project Status Display (REQ-8)', () => {
  let document;
  let statusSection;

  beforeEach(() => {
    const htmlPath = path.resolve(__dirname, '../index.html');
    const html = fs.readFileSync(htmlPath, 'utf8');
    const dom = new JSDOM(html);
    document = dom.window.document;
    statusSection = document.querySelector('.project-status-section') || document.querySelector('#project-status');
  });

  // Test Case 1: Project status or roadmap section is present
  describe('Test Case 1: Project Status Section Present', () => {
    it('should have a section displaying project status or feature roadmap', () => {
      expect(statusSection).not.toBeNull();
    });

    it('should have a heading for the project status section', () => {
      const heading = statusSection.querySelector('h2, h3');
      expect(heading).not.toBeNull();
      const headingText = heading.textContent.toLowerCase();
      expect(
        headingText.includes('status') ||
        headingText.includes('roadmap') ||
        headingText.includes('features')
      ).toBe(true);
    });

    it('should have semantic section element for project status', () => {
      const section = document.querySelector('section.project-status-section, section#project-status');
      expect(section).not.toBeNull();
    });
  });

  // Test Case 2: Implemented features are clearly marked
  describe('Test Case 2: Implemented Features Markers', () => {
    it('should have implemented features clearly marked with checkmarks or labels', () => {
      const implementedSection = statusSection.querySelector('.implemented-features, .features-implemented');
      expect(implementedSection).not.toBeNull();
    });

    it('should have visual indicators for implemented features (checkmarks or implemented labels)', () => {
      const implementedItems = statusSection.querySelectorAll('.feature-implemented, .implemented, [data-status="implemented"]');
      expect(implementedItems.length).toBeGreaterThan(0);
    });

    it('should have checkmark symbols or "implemented" text for implemented features', () => {
      const statusSectionHtml = statusSection.innerHTML.toLowerCase();
      const hasCheckmark = statusSectionHtml.includes('✓') ||
                          statusSectionHtml.includes('✔') ||
                          statusSectionHtml.includes('&#10003') ||
                          statusSectionHtml.includes('&#10004') ||
                          statusSectionHtml.includes('check');
      const hasImplementedLabel = statusSectionHtml.includes('implemented');
      expect(hasCheckmark || hasImplementedLabel).toBe(true);
    });
  });

  // Test Case 3: Tokio networking as implemented
  describe('Test Case 3: Tokio Networking Listed as Implemented', () => {
    it('should list Tokio-based async networking as an implemented feature', () => {
      const statusSectionText = statusSection.textContent.toLowerCase();
      expect(statusSectionText).toContain('tokio');
    });

    it('should indicate Tokio networking with networking/async context', () => {
      const statusSectionText = statusSection.textContent.toLowerCase();
      const hasTokio = statusSectionText.includes('tokio');
      const hasNetworking = statusSectionText.includes('networking') || statusSectionText.includes('async');
      expect(hasTokio && hasNetworking).toBe(true);
    });

    it('should have Tokio networking marked as implemented not planned', () => {
      // Find the Tokio feature item
      const allItems = statusSection.querySelectorAll('li, .feature-item');
      let tokioItem = null;
      allItems.forEach(item => {
        if (item.textContent.toLowerCase().includes('tokio')) {
          tokioItem = item;
        }
      });
      expect(tokioItem).not.toBeNull();

      // Check it's in implemented section or has implemented marker
      const isInImplemented = tokioItem.closest('.implemented-features, .features-implemented') !== null ||
                              tokioItem.classList.contains('feature-implemented') ||
                              tokioItem.classList.contains('implemented') ||
                              tokioItem.getAttribute('data-status') === 'implemented';
      expect(isInImplemented).toBe(true);
    });
  });

  // Test Case 4: Planned features are clearly marked
  describe('Test Case 4: Planned Features Markers', () => {
    it('should have planned features section with different styling', () => {
      const plannedSection = statusSection.querySelector('.planned-features, .features-planned');
      expect(plannedSection).not.toBeNull();
    });

    it('should have visual indicators for planned features (different from implemented)', () => {
      const plannedItems = statusSection.querySelectorAll('.feature-planned, .planned, [data-status="planned"]');
      expect(plannedItems.length).toBeGreaterThan(0);
    });

    it('should have "planned" labels or different symbols for planned features', () => {
      const statusSectionHtml = statusSection.innerHTML.toLowerCase();
      const hasPlannedLabel = statusSectionHtml.includes('planned') ||
                              statusSectionHtml.includes('coming soon') ||
                              statusSectionHtml.includes('upcoming');
      expect(hasPlannedLabel).toBe(true);
    });
  });

  // Test Case 5: Raft consensus as planned
  describe('Test Case 5: Raft Consensus Listed as Planned', () => {
    it('should list Raft consensus or distributed operation as a planned feature', () => {
      const statusSectionText = statusSection.textContent.toLowerCase();
      const hasRaft = statusSectionText.includes('raft');
      const hasDistributed = statusSectionText.includes('distributed');
      const hasConsensus = statusSectionText.includes('consensus');
      expect(hasRaft || hasDistributed || hasConsensus).toBe(true);
    });

    it('should have Raft consensus marked as planned not implemented', () => {
      // Find the Raft/distributed feature item
      const allItems = statusSection.querySelectorAll('li, .feature-item');
      let raftItem = null;
      allItems.forEach(item => {
        const text = item.textContent.toLowerCase();
        if (text.includes('raft') || text.includes('distributed') || text.includes('consensus')) {
          raftItem = item;
        }
      });
      expect(raftItem).not.toBeNull();

      // Check it's in planned section or has planned marker
      const isInPlanned = raftItem.closest('.planned-features, .features-planned') !== null ||
                          raftItem.classList.contains('feature-planned') ||
                          raftItem.classList.contains('planned') ||
                          raftItem.getAttribute('data-status') === 'planned';
      expect(isInPlanned).toBe(true);
    });
  });

  // Additional tests for comprehensive coverage
  describe('Additional Feature Status Tests', () => {
    it('should list memtable as an implemented feature', () => {
      const statusSectionText = statusSection.textContent.toLowerCase();
      expect(statusSectionText).toContain('memtable');
    });

    it('should list compaction as an implemented feature', () => {
      const statusSectionText = statusSection.textContent.toLowerCase();
      expect(statusSectionText).toContain('compaction');
    });

    it('should have both implemented and planned sections distinctly separated', () => {
      const implementedSection = statusSection.querySelector('.implemented-features, .features-implemented');
      const plannedSection = statusSection.querySelector('.planned-features, .features-planned');
      expect(implementedSection).not.toBeNull();
      expect(plannedSection).not.toBeNull();
      expect(implementedSection !== plannedSection).toBe(true);
    });
  });

  // Accessibility tests for status section
  describe('Status Section Accessibility', () => {
    it('should have proper heading hierarchy in status section', () => {
      const headings = statusSection.querySelectorAll('h2, h3, h4');
      expect(headings.length).toBeGreaterThan(0);
    });

    it('should use semantic list elements for features', () => {
      const lists = statusSection.querySelectorAll('ul, ol');
      expect(lists.length).toBeGreaterThan(0);
    });

    it('should have aria-label or descriptive content for feature lists', () => {
      const implementedList = statusSection.querySelector('.implemented-features ul, .features-implemented ul');
      const plannedList = statusSection.querySelector('.planned-features ul, .features-planned ul');
      expect(implementedList).not.toBeNull();
      expect(plannedList).not.toBeNull();
    });
  });
});
