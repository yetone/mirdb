/**
 * Features Content Integration Tests
 * Owner: Scenario 4 - Features Section Display
 *
 * Tests:
 * - Features highlight key MirDB capabilities
 * - Persistent storage, Memcached protocol, skip list memtable, compaction
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { loadDocument, getDocument } from '../../setup.js';

describe('Test Case 4: MirDB Features Content', () => {
  let document;
  let dom;

  beforeEach(() => {
    dom = loadDocument();
    document = getDocument(dom);
  });

  describe('Features highlight key MirDB capabilities', () => {
    it('should have a feature about persistent storage', () => {
      const featureCards = document.querySelectorAll('.feature-card');
      const titles = Array.from(featureCards).map(card =>
        card.querySelector('.feature-card__title').textContent.toLowerCase()
      );
      const descriptions = Array.from(featureCards).map(card =>
        card.querySelector('.feature-card__description').textContent.toLowerCase()
      );

      const hasPersistentStorage = titles.some(title =>
        title.includes('persistent') || title.includes('storage')
      ) || descriptions.some(desc =>
        desc.includes('persistent') || desc.includes('disk')
      );

      expect(hasPersistentStorage).toBe(true);
    });

    it('should have a feature about Memcached protocol', () => {
      const featureCards = document.querySelectorAll('.feature-card');
      const titles = Array.from(featureCards).map(card =>
        card.querySelector('.feature-card__title').textContent.toLowerCase()
      );
      const descriptions = Array.from(featureCards).map(card =>
        card.querySelector('.feature-card__description').textContent.toLowerCase()
      );

      const hasMemcachedProtocol = titles.some(title =>
        title.includes('memcached') || title.includes('protocol')
      ) || descriptions.some(desc =>
        desc.includes('memcached') || desc.includes('protocol')
      );

      expect(hasMemcachedProtocol).toBe(true);
    });

    it('should have a feature about skip list memtable', () => {
      const featureCards = document.querySelectorAll('.feature-card');
      const titles = Array.from(featureCards).map(card =>
        card.querySelector('.feature-card__title').textContent.toLowerCase()
      );
      const descriptions = Array.from(featureCards).map(card =>
        card.querySelector('.feature-card__description').textContent.toLowerCase()
      );

      const hasSkipListMemtable = titles.some(title =>
        title.includes('skip list') || title.includes('memtable')
      ) || descriptions.some(desc =>
        desc.includes('skip list') || desc.includes('memtable')
      );

      expect(hasSkipListMemtable).toBe(true);
    });

    it('should have a feature about compaction', () => {
      const featureCards = document.querySelectorAll('.feature-card');
      const titles = Array.from(featureCards).map(card =>
        card.querySelector('.feature-card__title').textContent.toLowerCase()
      );
      const descriptions = Array.from(featureCards).map(card =>
        card.querySelector('.feature-card__description').textContent.toLowerCase()
      );

      const hasCompaction = titles.some(title =>
        title.includes('compaction') || title.includes('compact')
      ) || descriptions.some(desc =>
        desc.includes('compaction') || desc.includes('compact') || desc.includes('merges')
      );

      expect(hasCompaction).toBe(true);
    });

    it('should highlight all 4 key MirDB capabilities', () => {
      const featureCards = document.querySelectorAll('.feature-card');
      const allContent = Array.from(featureCards).map(card => {
        const title = card.querySelector('.feature-card__title').textContent.toLowerCase();
        const desc = card.querySelector('.feature-card__description').textContent.toLowerCase();
        return title + ' ' + desc;
      }).join(' ');

      // Verify all key capabilities are mentioned
      expect(allContent).toMatch(/persistent|storage|disk/i);
      expect(allContent).toMatch(/memcached|protocol/i);
      expect(allContent).toMatch(/skip\s*list|memtable/i);
      expect(allContent).toMatch(/compact|merges|sstable/i);
    });
  });

  describe('Feature descriptions are informative', () => {
    it('should have descriptions between 50 and 200 characters', () => {
      const descriptions = document.querySelectorAll('.feature-card__description');
      descriptions.forEach((desc) => {
        const length = desc.textContent.trim().length;
        expect(length).toBeGreaterThanOrEqual(50);
        expect(length).toBeLessThanOrEqual(200);
      });
    });

    it('should have titles between 10 and 50 characters', () => {
      const titles = document.querySelectorAll('.feature-card__title');
      titles.forEach((title) => {
        const length = title.textContent.trim().length;
        expect(length).toBeGreaterThanOrEqual(10);
        expect(length).toBeLessThanOrEqual(50);
      });
    });
  });
});
