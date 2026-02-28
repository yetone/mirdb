/**
 * Project Status Section Tests
 * Owner: Scenario 4 - Project Status Section
 *
 * Tests for:
 * - Status section visibility and structure
 * - Completed features with checkmark indicators
 * - Planned features with upcoming indicators
 * - Visual distinction between completed and planned features
 *
 * Requirements: REQ-4
 * @jest-environment jsdom
 */

import { readFileSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

describe('Project Status Section', () => {
  beforeEach(() => {
    // Load the homepage HTML
    const htmlPath = resolve(__dirname, '../../index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    document.documentElement.innerHTML = html;
  });

  /**
   * Test Case 1: Project status section exists
   * Input: Query for project status section
   * Expected: Section exists with id='status' or heading containing 'Status' or 'Roadmap'
   */
  describe('Test Case 1: Project Status Section Existence', () => {
    test('status section exists with id="status"', () => {
      const statusSection = document.getElementById('status');
      expect(statusSection).not.toBeNull();
      expect(statusSection).toBeInTheDocument();
    });

    test('status section has class containing "status"', () => {
      const statusSection = document.getElementById('status');
      expect(statusSection.classList.contains('status')).toBe(true);
    });

    test('status section is a semantic section element', () => {
      const statusSection = document.getElementById('status');
      expect(statusSection.tagName.toLowerCase()).toBe('section');
    });

    test('status section has aria-labelledby for accessibility', () => {
      const statusSection = document.getElementById('status');
      expect(statusSection.getAttribute('aria-labelledby')).toBe('status-title');
    });

    test('status section has heading containing "Status"', () => {
      const statusSection = document.getElementById('status');
      const heading = statusSection.querySelector('h2');
      expect(heading).not.toBeNull();
      expect(heading.textContent.toLowerCase()).toContain('status');
    });
  });

  /**
   * Test Case 2: Completed feature indicators exist
   * Input: Query for completed feature indicators
   * Expected: Elements exist with checkmark icons or 'completed'/'done' indicators
   */
  describe('Test Case 2: Completed Feature Indicators', () => {
    test('completed features list exists', () => {
      const statusSection = document.getElementById('status');
      const completedList = statusSection.querySelector('.status__list[aria-label="Completed features"]');
      expect(completedList).not.toBeNull();
      expect(completedList).toBeInTheDocument();
    });

    test('completed features have checkmark icons', () => {
      const statusSection = document.getElementById('status');
      const completedItems = statusSection.querySelectorAll('.status__item--completed');
      expect(completedItems.length).toBeGreaterThan(0);

      completedItems.forEach(item => {
        const checkIcon = item.querySelector('.status__icon--check');
        expect(checkIcon).not.toBeNull();
      });
    });

    test('completed items have data-status="completed" attribute', () => {
      const statusSection = document.getElementById('status');
      const completedItems = statusSection.querySelectorAll('[data-status="completed"]');
      expect(completedItems.length).toBeGreaterThan(0);
    });

    test('completed column has success-colored icon', () => {
      const statusSection = document.getElementById('status');
      const completedColumnIcon = statusSection.querySelector('.status__column-icon--completed');
      expect(completedColumnIcon).not.toBeNull();
    });
  });

  /**
   * Test Case 3: Tokio in completed features
   * Input: Check for 'Tokio' in completed features list
   * Expected: Item containing 'tokio' or 'Tokio' exists with completed indicator
   */
  describe('Test Case 3: Tokio Feature', () => {
    test('Tokio is listed in completed features', () => {
      const statusSection = document.getElementById('status');
      const completedItems = statusSection.querySelectorAll('.status__item--completed');

      const tokioItem = Array.from(completedItems).find(item =>
        item.textContent.toLowerCase().includes('tokio')
      );

      expect(tokioItem).toBeDefined();
      expect(tokioItem).not.toBeNull();
    });

    test('Tokio item has completed status attribute', () => {
      const statusSection = document.getElementById('status');
      const completedItems = statusSection.querySelectorAll('[data-status="completed"]');

      const tokioItem = Array.from(completedItems).find(item =>
        item.textContent.toLowerCase().includes('tokio')
      );

      expect(tokioItem).toBeDefined();
      expect(tokioItem.getAttribute('data-status')).toBe('completed');
    });

    test('Tokio item has checkmark icon', () => {
      const statusSection = document.getElementById('status');
      const completedItems = statusSection.querySelectorAll('.status__item--completed');

      const tokioItem = Array.from(completedItems).find(item =>
        item.textContent.toLowerCase().includes('tokio')
      );

      expect(tokioItem).toBeDefined();
      const checkIcon = tokioItem.querySelector('.status__icon--check');
      expect(checkIcon).not.toBeNull();
    });
  });

  /**
   * Test Case 4: Memtable/skiplist in completed features
   * Input: Check for 'memtable' or 'skiplist' in completed features
   * Expected: Item containing 'memtable' or 'skip-list' or 'skiplist' exists with completed indicator
   */
  describe('Test Case 4: Memtable/Skiplist Feature', () => {
    test('memtable or skiplist is listed in completed features', () => {
      const statusSection = document.getElementById('status');
      const completedItems = statusSection.querySelectorAll('.status__item--completed');

      const memtableItem = Array.from(completedItems).find(item => {
        const text = item.textContent.toLowerCase();
        return text.includes('memtable') || text.includes('skip-list') || text.includes('skiplist');
      });

      expect(memtableItem).toBeDefined();
      expect(memtableItem).not.toBeNull();
    });

    test('memtable item has completed status attribute', () => {
      const statusSection = document.getElementById('status');
      const completedItems = statusSection.querySelectorAll('[data-status="completed"]');

      const memtableItem = Array.from(completedItems).find(item => {
        const text = item.textContent.toLowerCase();
        return text.includes('memtable') || text.includes('skip-list') || text.includes('skiplist');
      });

      expect(memtableItem).toBeDefined();
      expect(memtableItem.getAttribute('data-status')).toBe('completed');
    });

    test('memtable item has checkmark icon', () => {
      const statusSection = document.getElementById('status');
      const completedItems = statusSection.querySelectorAll('.status__item--completed');

      const memtableItem = Array.from(completedItems).find(item => {
        const text = item.textContent.toLowerCase();
        return text.includes('memtable') || text.includes('skip-list') || text.includes('skiplist');
      });

      expect(memtableItem).toBeDefined();
      const checkIcon = memtableItem.querySelector('.status__icon--check');
      expect(checkIcon).not.toBeNull();
    });
  });

  /**
   * Test Case 5: Minor compaction in completed features
   * Input: Check for 'minor compaction' in completed features
   * Expected: Item containing 'minor compaction' exists with completed indicator
   */
  describe('Test Case 5: Minor Compaction Feature', () => {
    test('minor compaction is listed in completed features', () => {
      const statusSection = document.getElementById('status');
      const completedItems = statusSection.querySelectorAll('.status__item--completed');

      const minorCompactionItem = Array.from(completedItems).find(item =>
        item.textContent.toLowerCase().includes('minor compaction')
      );

      expect(minorCompactionItem).toBeDefined();
      expect(minorCompactionItem).not.toBeNull();
    });

    test('minor compaction item has completed status attribute', () => {
      const statusSection = document.getElementById('status');
      const completedItems = statusSection.querySelectorAll('[data-status="completed"]');

      const minorCompactionItem = Array.from(completedItems).find(item =>
        item.textContent.toLowerCase().includes('minor compaction')
      );

      expect(minorCompactionItem).toBeDefined();
      expect(minorCompactionItem.getAttribute('data-status')).toBe('completed');
    });

    test('minor compaction item has checkmark icon', () => {
      const statusSection = document.getElementById('status');
      const completedItems = statusSection.querySelectorAll('.status__item--completed');

      const minorCompactionItem = Array.from(completedItems).find(item =>
        item.textContent.toLowerCase().includes('minor compaction')
      );

      expect(minorCompactionItem).toBeDefined();
      const checkIcon = minorCompactionItem.querySelector('.status__icon--check');
      expect(checkIcon).not.toBeNull();
    });
  });

  /**
   * Test Case 6: Major compaction in completed features
   * Input: Check for 'major compaction' in completed features
   * Expected: Item containing 'major compaction' exists with completed indicator
   */
  describe('Test Case 6: Major Compaction Feature', () => {
    test('major compaction is listed in completed features', () => {
      const statusSection = document.getElementById('status');
      const completedItems = statusSection.querySelectorAll('.status__item--completed');

      const majorCompactionItem = Array.from(completedItems).find(item =>
        item.textContent.toLowerCase().includes('major compaction')
      );

      expect(majorCompactionItem).toBeDefined();
      expect(majorCompactionItem).not.toBeNull();
    });

    test('major compaction item has completed status attribute', () => {
      const statusSection = document.getElementById('status');
      const completedItems = statusSection.querySelectorAll('[data-status="completed"]');

      const majorCompactionItem = Array.from(completedItems).find(item =>
        item.textContent.toLowerCase().includes('major compaction')
      );

      expect(majorCompactionItem).toBeDefined();
      expect(majorCompactionItem.getAttribute('data-status')).toBe('completed');
    });

    test('major compaction item has checkmark icon', () => {
      const statusSection = document.getElementById('status');
      const completedItems = statusSection.querySelectorAll('.status__item--completed');

      const majorCompactionItem = Array.from(completedItems).find(item =>
        item.textContent.toLowerCase().includes('major compaction')
      );

      expect(majorCompactionItem).toBeDefined();
      const checkIcon = majorCompactionItem.querySelector('.status__icon--check');
      expect(checkIcon).not.toBeNull();
    });
  });

  /**
   * Test Case 7: Raft in planned features
   * Input: Check for 'Raft' in planned features
   * Expected: Item containing 'raft' or 'Raft' exists with planned/upcoming indicator
   */
  describe('Test Case 7: Raft Consensus Feature', () => {
    test('Raft is listed in planned features', () => {
      const statusSection = document.getElementById('status');
      const plannedItems = statusSection.querySelectorAll('.status__item--planned');

      const raftItem = Array.from(plannedItems).find(item =>
        item.textContent.toLowerCase().includes('raft')
      );

      expect(raftItem).toBeDefined();
      expect(raftItem).not.toBeNull();
    });

    test('Raft item has planned status attribute', () => {
      const statusSection = document.getElementById('status');
      const plannedItems = statusSection.querySelectorAll('[data-status="planned"]');

      const raftItem = Array.from(plannedItems).find(item =>
        item.textContent.toLowerCase().includes('raft')
      );

      expect(raftItem).toBeDefined();
      expect(raftItem.getAttribute('data-status')).toBe('planned');
    });

    test('Raft item has clock/planned icon', () => {
      const statusSection = document.getElementById('status');
      const plannedItems = statusSection.querySelectorAll('.status__item--planned');

      const raftItem = Array.from(plannedItems).find(item =>
        item.textContent.toLowerCase().includes('raft')
      );

      expect(raftItem).toBeDefined();
      const clockIcon = raftItem.querySelector('.status__icon--clock');
      expect(clockIcon).not.toBeNull();
    });

    test('planned features list exists', () => {
      const statusSection = document.getElementById('status');
      const plannedList = statusSection.querySelector('.status__list[aria-label="Planned features"]');
      expect(plannedList).not.toBeNull();
      expect(plannedList).toBeInTheDocument();
    });
  });

  /**
   * Test Case 8: Visual distinction between completed and planned
   * Input: Visual inspection of status indicators
   * Expected: Clear visual distinction between completed (checkmarks) and planned (different icon/style) features
   */
  describe('Test Case 8: Visual Distinction', () => {
    test('completed and planned items use different CSS classes', () => {
      const statusSection = document.getElementById('status');
      const completedItems = statusSection.querySelectorAll('.status__item--completed');
      const plannedItems = statusSection.querySelectorAll('.status__item--planned');

      expect(completedItems.length).toBeGreaterThan(0);
      expect(plannedItems.length).toBeGreaterThan(0);
    });

    test('completed items use check icons', () => {
      const statusSection = document.getElementById('status');
      const checkIcons = statusSection.querySelectorAll('.status__icon--check');
      expect(checkIcons.length).toBeGreaterThan(0);
    });

    test('planned items use clock icons', () => {
      const statusSection = document.getElementById('status');
      const clockIcons = statusSection.querySelectorAll('.status__icon--clock');
      expect(clockIcons.length).toBeGreaterThan(0);
    });

    test('completed and planned columns have different icon classes', () => {
      const statusSection = document.getElementById('status');
      const completedColumnIcon = statusSection.querySelector('.status__column-icon--completed');
      const plannedColumnIcon = statusSection.querySelector('.status__column-icon--planned');

      expect(completedColumnIcon).not.toBeNull();
      expect(plannedColumnIcon).not.toBeNull();
    });

    test('icons are hidden from screen readers', () => {
      const statusSection = document.getElementById('status');
      const icons = statusSection.querySelectorAll('.status__icon');

      icons.forEach(icon => {
        expect(icon.getAttribute('aria-hidden')).toBe('true');
      });
    });
  });

  /**
   * Accessibility Tests
   */
  describe('Accessibility', () => {
    test('status section is within main element', () => {
      const main = document.querySelector('main');
      const statusSection = document.getElementById('status');
      expect(main).not.toBeNull();
      expect(main.contains(statusSection)).toBe(true);
    });

    test('status lists have aria-label for screen readers', () => {
      const statusSection = document.getElementById('status');
      const lists = statusSection.querySelectorAll('.status__list');

      lists.forEach(list => {
        expect(list.getAttribute('aria-label')).toBeTruthy();
      });
    });

    test('column icons are hidden from screen readers', () => {
      const statusSection = document.getElementById('status');
      const columnIcons = statusSection.querySelectorAll('.status__column-icon');

      columnIcons.forEach(icon => {
        expect(icon.getAttribute('aria-hidden')).toBe('true');
      });
    });

    test('status section heading has proper id', () => {
      const statusSection = document.getElementById('status');
      const heading = statusSection.querySelector('#status-title');
      expect(heading).not.toBeNull();
      expect(heading.tagName.toLowerCase()).toBe('h2');
    });
  });

  /**
   * Structure Tests
   */
  describe('Structure', () => {
    test('status section has a grid layout with two columns', () => {
      const statusSection = document.getElementById('status');
      const grid = statusSection.querySelector('.status__grid');
      expect(grid).not.toBeNull();

      const columns = grid.querySelectorAll('.status__column');
      expect(columns.length).toBe(2);
    });

    test('each status item has text content', () => {
      const statusSection = document.getElementById('status');
      const items = statusSection.querySelectorAll('.status__item');

      items.forEach(item => {
        const text = item.querySelector('.status__item-text');
        expect(text).not.toBeNull();
        expect(text.textContent.trim().length).toBeGreaterThan(0);
      });
    });

    test('there are exactly 4 completed features', () => {
      const statusSection = document.getElementById('status');
      const completedItems = statusSection.querySelectorAll('.status__item--completed');
      expect(completedItems.length).toBe(4);
    });

    test('there is at least 1 planned feature', () => {
      const statusSection = document.getElementById('status');
      const plannedItems = statusSection.querySelectorAll('.status__item--planned');
      expect(plannedItems.length).toBeGreaterThanOrEqual(1);
    });
  });
});
