/**
 * Unit Tests for Features Section HTML Structure
 * Owner: Scenario 3 - Features Section Display
 *
 * Test Cases:
 * - TC1: Check Features section HTML structure (contains section title and 6 feature cards with icons and descriptions)
 * - TC5: Verify feature card content (Persistent Storage, Memcached Protocol, LSM-Tree Architecture, Compaction, Rust-Powered, Async I/O)
 */

const fs = require('fs');
const path = require('path');

describe('Features Section HTML Structure', () => {
  let htmlContent;
  let doc;

  beforeAll(() => {
    // Read the HTML file
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Use Jest's jsdom environment DOMParser
    const parser = new DOMParser();
    doc = parser.parseFromString(htmlContent, 'text/html');
  });

  describe('TC1: Features section HTML structure', () => {
    test('should have a features section with id="features"', () => {
      const featuresSection = doc.getElementById('features');
      expect(featuresSection).not.toBeNull();
      expect(featuresSection.tagName.toLowerCase()).toBe('section');
    });

    test('should have a section title "Why MirDB?"', () => {
      const featuresSection = doc.getElementById('features');
      const title = featuresSection.querySelector('.features__title');
      expect(title).not.toBeNull();
      expect(title.textContent.trim()).toBe('Why MirDB?');
    });

    test('should have exactly 6 feature cards', () => {
      const featuresSection = doc.getElementById('features');
      const cards = featuresSection.querySelectorAll('.feature-card');
      expect(cards.length).toBe(6);
    });

    test('each feature card should have an icon', () => {
      const featuresSection = doc.getElementById('features');
      const cards = featuresSection.querySelectorAll('.feature-card');

      cards.forEach((card, index) => {
        const icon = card.querySelector('.feature-card__icon');
        expect(icon).not.toBeNull();
        const svg = icon.querySelector('svg');
        expect(svg).not.toBeNull();
      });
    });

    test('each feature card should have a title', () => {
      const featuresSection = doc.getElementById('features');
      const cards = featuresSection.querySelectorAll('.feature-card');

      cards.forEach((card) => {
        const title = card.querySelector('.feature-card__title');
        expect(title).not.toBeNull();
        expect(title.textContent.trim().length).toBeGreaterThan(0);
      });
    });

    test('each feature card should have a description', () => {
      const featuresSection = doc.getElementById('features');
      const cards = featuresSection.querySelectorAll('.feature-card');

      cards.forEach((card) => {
        const description = card.querySelector('.feature-card__description');
        expect(description).not.toBeNull();
        expect(description.textContent.trim().length).toBeGreaterThan(0);
      });
    });

    test('features grid should have proper CSS class', () => {
      const featuresSection = doc.getElementById('features');
      const grid = featuresSection.querySelector('.features__grid');
      expect(grid).not.toBeNull();
    });
  });

  describe('TC5: Feature card content verification', () => {
    const expectedFeatures = [
      { title: 'Persistent Storage', descContains: 'survives restarts' },
      { title: 'Memcached Protocol', descContains: 'Drop-in replacement' },
      { title: 'LSM-Tree Architecture', descContains: 'write-heavy workloads' },
      { title: 'Minor/Major Compaction', descContains: 'storage optimization' },
      { title: 'Rust-Powered', descContains: 'Memory safety' },
      { title: 'Async I/O', descContains: 'Tokio' }
    ];

    test('should contain Persistent Storage feature', () => {
      const featuresSection = doc.getElementById('features');
      const titles = featuresSection.querySelectorAll('.feature-card__title');
      const titleTexts = Array.from(titles).map(t => t.textContent.trim());
      expect(titleTexts).toContain('Persistent Storage');
    });

    test('should contain Memcached Protocol feature', () => {
      const featuresSection = doc.getElementById('features');
      const titles = featuresSection.querySelectorAll('.feature-card__title');
      const titleTexts = Array.from(titles).map(t => t.textContent.trim());
      expect(titleTexts).toContain('Memcached Protocol');
    });

    test('should contain LSM-Tree Architecture feature', () => {
      const featuresSection = doc.getElementById('features');
      const titles = featuresSection.querySelectorAll('.feature-card__title');
      const titleTexts = Array.from(titles).map(t => t.textContent.trim());
      expect(titleTexts).toContain('LSM-Tree Architecture');
    });

    test('should contain Compaction feature', () => {
      const featuresSection = doc.getElementById('features');
      const titles = featuresSection.querySelectorAll('.feature-card__title');
      const titleTexts = Array.from(titles).map(t => t.textContent.trim());
      expect(titleTexts).toContain('Minor/Major Compaction');
    });

    test('should contain Rust-Powered feature', () => {
      const featuresSection = doc.getElementById('features');
      const titles = featuresSection.querySelectorAll('.feature-card__title');
      const titleTexts = Array.from(titles).map(t => t.textContent.trim());
      expect(titleTexts).toContain('Rust-Powered');
    });

    test('should contain Async I/O feature', () => {
      const featuresSection = doc.getElementById('features');
      const titles = featuresSection.querySelectorAll('.feature-card__title');
      const titleTexts = Array.from(titles).map(t => t.textContent.trim());
      expect(titleTexts).toContain('Async I/O');
    });

    test('each feature should have an accurate description', () => {
      const featuresSection = doc.getElementById('features');
      const cards = featuresSection.querySelectorAll('.feature-card');

      expectedFeatures.forEach(expected => {
        let found = false;
        cards.forEach(card => {
          const title = card.querySelector('.feature-card__title').textContent.trim();
          const desc = card.querySelector('.feature-card__description').textContent.trim();
          if (title === expected.title) {
            found = true;
            expect(desc.toLowerCase()).toContain(expected.descContains.toLowerCase());
          }
        });
        expect(found).toBe(true);
      });
    });
  });
});
