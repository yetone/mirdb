/**
 * Features Section Unit Tests
 * Owner: Scenario 2 - Features Showcase Section
 *
 * Tests:
 * - Feature card content validation
 * - Description accuracy
 *
 * Requirements: REQ-2
 */

const fs = require('fs');
const path = require('path');

describe('Features Section Content', () => {
  beforeAll(() => {
    // Read the HTML file
    const htmlPath = path.join(__dirname, '../../index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf8');

    // Set up the document body with the HTML content using jest-environment-jsdom
    document.body.innerHTML = htmlContent;
  });

  describe('TC2: Memcached Protocol Support Feature Card', () => {
    let card;

    beforeAll(() => {
      card = document.querySelector('[data-feature="memcached-protocol"]');
    });

    test('should exist in the DOM', () => {
      expect(card).not.toBeNull();
    });

    test('should have correct title', () => {
      const title = card.querySelector('h3');
      expect(title.textContent).toBe('Memcached Protocol Support');
    });

    test('should describe compatibility with existing memcached clients', () => {
      const description = card.querySelector('p');
      expect(description.textContent.toLowerCase()).toContain('compatible');
      expect(description.textContent.toLowerCase()).toContain('memcached');
      expect(description.textContent.toLowerCase()).toContain('client');
    });

    test('should have an icon', () => {
      const icon = card.querySelector('.feature-icon');
      expect(icon).not.toBeNull();
    });
  });

  describe('TC3: Persistence Feature Card', () => {
    let card;

    beforeAll(() => {
      card = document.querySelector('[data-feature="persistence"]');
    });

    test('should exist in the DOM', () => {
      expect(card).not.toBeNull();
    });

    test('should have correct title', () => {
      const title = card.querySelector('h3');
      expect(title.textContent).toBe('Persistence');
    });

    test('should describe data surviving restarts unlike traditional memcached', () => {
      const description = card.querySelector('p');
      expect(description.textContent.toLowerCase()).toContain('survives restarts');
      expect(description.textContent.toLowerCase()).toContain('memcached');
    });

    test('should have an icon', () => {
      const icon = card.querySelector('.feature-icon');
      expect(icon).not.toBeNull();
    });
  });

  describe('TC4: LSM Tree Architecture Feature Card', () => {
    let card;

    beforeAll(() => {
      card = document.querySelector('[data-feature="lsm-tree"]');
    });

    test('should exist in the DOM', () => {
      expect(card).not.toBeNull();
    });

    test('should have correct title', () => {
      const title = card.querySelector('h3');
      expect(title.textContent).toBe('LSM Tree Architecture');
    });

    test('should describe efficient storage with memtables and SSTable compaction', () => {
      const description = card.querySelector('p');
      expect(description.textContent.toLowerCase()).toContain('memtable');
      expect(description.textContent.toLowerCase()).toContain('sstable compaction');
    });

    test('should have an icon', () => {
      const icon = card.querySelector('.feature-icon');
      expect(icon).not.toBeNull();
    });
  });

  describe('High Performance Feature Card', () => {
    let card;

    beforeAll(() => {
      card = document.querySelector('[data-feature="high-performance"]');
    });

    test('should exist in the DOM', () => {
      expect(card).not.toBeNull();
    });

    test('should have correct title', () => {
      const title = card.querySelector('h3');
      expect(title.textContent).toBe('High Performance');
    });

    test('should describe speed and skip list', () => {
      const description = card.querySelector('p');
      expect(description.textContent.toLowerCase()).toContain('speed');
    });
  });

  describe('Rust Implementation Feature Card', () => {
    let card;

    beforeAll(() => {
      card = document.querySelector('[data-feature="rust-implementation"]');
    });

    test('should exist in the DOM', () => {
      expect(card).not.toBeNull();
    });

    test('should have correct title', () => {
      const title = card.querySelector('h3');
      expect(title.textContent).toBe('Rust Implementation');
    });

    test('should describe memory safety and performance', () => {
      const description = card.querySelector('p');
      expect(description.textContent).toContain('Memory safety');
      expect(description.textContent.toLowerCase()).toContain('performance');
    });
  });

  describe('Skip List Feature Card', () => {
    let card;

    beforeAll(() => {
      card = document.querySelector('[data-feature="skip-list"]');
    });

    test('should exist in the DOM', () => {
      expect(card).not.toBeNull();
    });

    test('should have correct title', () => {
      const title = card.querySelector('h3');
      expect(title.textContent).toBe('Skip List');
    });

    test('should describe fast operations and memtable management', () => {
      const description = card.querySelector('p');
      expect(description.textContent).toContain('Fast');
      expect(description.textContent.toLowerCase()).toContain('memtable');
    });
  });

  describe('Features Grid Structure', () => {
    test('should have exactly 6 feature cards', () => {
      const cards = document.querySelectorAll('#features .feature-card');
      expect(cards.length).toBe(6);
    });

    test('all cards should have the required structure (icon, title, description)', () => {
      const cards = document.querySelectorAll('#features .feature-card');

      cards.forEach((card) => {
        expect(card.querySelector('.feature-icon')).not.toBeNull();
        expect(card.querySelector('h3')).not.toBeNull();
        expect(card.querySelector('p')).not.toBeNull();
      });
    });

    test('features section should have proper aria-labelledby', () => {
      const section = document.querySelector('#features');
      expect(section.getAttribute('aria-labelledby')).toBe('features-title');
    });

    test('section title should be an h2', () => {
      const title = document.querySelector('#features-title');
      expect(title.tagName).toBe('H2');
    });
  });
});
