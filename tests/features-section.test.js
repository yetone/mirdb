/**
 * Test Suite: Features Section Display
 * Scenario: Verify the features section showcases key MirDB capabilities
 * including Memcached compatibility, LSM-tree architecture, WAL durability,
 * and other technical features.
 *
 * These tests use JSDOM to parse and validate the HTML structure.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import { readFileSync } from 'fs';
import { resolve } from 'path';

describe('Features Section Display', () => {
  let dom;
  let document;

  beforeEach(() => {
    const htmlPath = resolve(__dirname, '../index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    dom = new JSDOM(html, { runScripts: 'dangerously', resources: 'usable' });
    document = dom.window.document;
  });

  afterEach(() => {
    if (dom) {
      dom.window.close();
    }
  });

  /**
   * Test Case 1: Check features section exists in DOM
   * Expected: Features section element with id='features' or class containing 'features' exists
   */
  describe('Test Case 1: Features section exists in DOM', () => {
    it('should have a section with id="features" or class containing "features"', () => {
      // Check for section with id="features"
      const sectionById = document.getElementById('features');

      // Check for section with class containing "features"
      const sectionByClass = document.querySelector('section.features');

      // At least one should exist
      const featuresSection = sectionById || sectionByClass;
      expect(featuresSection).not.toBeNull();
    });
  });

  /**
   * Test Case 2: Count feature cards/items in features section
   * Expected: At least 5 feature items are present
   */
  describe('Test Case 2: At least 5 feature items are present', () => {
    it('should have at least 5 feature cards in the features section', () => {
      const featuresSection = document.getElementById('features') || document.querySelector('section.features');
      expect(featuresSection).not.toBeNull();

      // Look for feature cards within the features section
      const featureCards = featuresSection.querySelectorAll('.feature-card, article.feature-card, .feature-item, [class*="feature-card"]');

      expect(featureCards.length).toBeGreaterThanOrEqual(5);
    });
  });

  /**
   * Test Case 3: Check for Memcached Protocol feature
   * Expected: Feature mentioning 'Memcached' protocol compatibility is displayed
   */
  describe('Test Case 3: Memcached Protocol feature is displayed', () => {
    it('should contain text mentioning Memcached in the features section', () => {
      const featuresSection = document.getElementById('features') || document.querySelector('section.features');
      expect(featuresSection).not.toBeNull();

      // Check if the features section contains text mentioning Memcached
      const featuresText = featuresSection.textContent;
      expect(featuresText.toLowerCase()).toContain('memcached');
    });

    it('should have Memcached mentioned within a feature card', () => {
      const featuresSection = document.getElementById('features') || document.querySelector('section.features');
      expect(featuresSection).not.toBeNull();

      // Verify it's within a feature card context
      const featureCards = featuresSection.querySelectorAll('.feature-card, article, [class*="feature-card"]');
      let memcachedFeatureFound = false;

      featureCards.forEach(card => {
        if (card.textContent.toLowerCase().includes('memcached')) {
          memcachedFeatureFound = true;
        }
      });

      expect(memcachedFeatureFound).toBe(true);
    });
  });

  /**
   * Test Case 4: Check for LSM-Tree feature
   * Expected: Feature mentioning 'LSM-tree' or 'LSM tree' architecture is displayed
   */
  describe('Test Case 4: LSM-Tree feature is displayed', () => {
    it('should contain text mentioning LSM-tree in the features section', () => {
      const featuresSection = document.getElementById('features') || document.querySelector('section.features');
      expect(featuresSection).not.toBeNull();

      // Check if the features section contains text mentioning LSM-tree (case insensitive)
      const featuresText = featuresSection.textContent.toLowerCase();
      const hasLSMTree = featuresText.includes('lsm-tree') || featuresText.includes('lsm tree') || featuresText.includes('log-structured merge');
      expect(hasLSMTree).toBe(true);
    });

    it('should have LSM-tree mentioned within a feature card', () => {
      const featuresSection = document.getElementById('features') || document.querySelector('section.features');
      expect(featuresSection).not.toBeNull();

      // Verify it's within a feature card context
      const featureCards = featuresSection.querySelectorAll('.feature-card, article, [class*="feature-card"]');
      let lsmFeatureFound = false;

      featureCards.forEach(card => {
        const cardText = card.textContent.toLowerCase();
        if (cardText.includes('lsm-tree') || cardText.includes('lsm tree') || cardText.includes('lsm')) {
          lsmFeatureFound = true;
        }
      });

      expect(lsmFeatureFound).toBe(true);
    });
  });

  /**
   * Test Case 5: Check for WAL/durability feature
   * Expected: Feature mentioning 'Write-Ahead Logging' or 'WAL' or 'durability' is displayed
   */
  describe('Test Case 5: WAL/durability feature is displayed', () => {
    it('should contain text mentioning WAL or durability in the features section', () => {
      const featuresSection = document.getElementById('features') || document.querySelector('section.features');
      expect(featuresSection).not.toBeNull();

      // Check if the features section contains text mentioning WAL, Write-Ahead Logging, or durability
      const featuresText = featuresSection.textContent.toLowerCase();
      const hasWAL = featuresText.includes('wal') ||
                     featuresText.includes('write-ahead log') ||
                     featuresText.includes('durability');
      expect(hasWAL).toBe(true);
    });

    it('should have WAL/durability mentioned within a feature card', () => {
      const featuresSection = document.getElementById('features') || document.querySelector('section.features');
      expect(featuresSection).not.toBeNull();

      // Verify it's within a feature card context
      const featureCards = featuresSection.querySelectorAll('.feature-card, article, [class*="feature-card"]');
      let walFeatureFound = false;

      featureCards.forEach(card => {
        const cardText = card.textContent.toLowerCase();
        if (cardText.includes('wal') || cardText.includes('write-ahead') || cardText.includes('durability')) {
          walFeatureFound = true;
        }
      });

      expect(walFeatureFound).toBe(true);
    });
  });

  /**
   * Test Case 6: Check for Skip List feature
   * Expected: Feature mentioning 'Skip List' or 'memtable' performance is displayed
   */
  describe('Test Case 6: Skip List/memtable feature is displayed', () => {
    it('should contain text mentioning Skip List or memtable in the features section', () => {
      const featuresSection = document.getElementById('features') || document.querySelector('section.features');
      expect(featuresSection).not.toBeNull();

      // Check if the features section contains text mentioning Skip List or memtable
      const featuresText = featuresSection.textContent.toLowerCase();
      const hasSkipList = featuresText.includes('skip list') || featuresText.includes('memtable');
      expect(hasSkipList).toBe(true);
    });

    it('should have Skip List/memtable mentioned within a feature card', () => {
      const featuresSection = document.getElementById('features') || document.querySelector('section.features');
      expect(featuresSection).not.toBeNull();

      // Verify it's within a feature card context
      const featureCards = featuresSection.querySelectorAll('.feature-card, article, [class*="feature-card"]');
      let skipListFeatureFound = false;

      featureCards.forEach(card => {
        const cardText = card.textContent.toLowerCase();
        if (cardText.includes('skip list') || cardText.includes('memtable')) {
          skipListFeatureFound = true;
        }
      });

      expect(skipListFeatureFound).toBe(true);
    });
  });

  /**
   * Test Case 7: Verify each feature has title and description
   * Expected: Each feature card contains a heading element and paragraph/description text
   */
  describe('Test Case 7: Each feature has title and description', () => {
    it('should have feature cards with headings and descriptions', () => {
      const featuresSection = document.getElementById('features') || document.querySelector('section.features');
      expect(featuresSection).not.toBeNull();

      // Get all feature cards
      const featureCards = featuresSection.querySelectorAll('.feature-card, article, [class*="feature-card"]');
      expect(featureCards.length).toBeGreaterThan(0);

      // Verify each feature card has a heading and description
      featureCards.forEach((card) => {
        // Check for heading (h1-h6)
        const heading = card.querySelector('h1, h2, h3, h4, h5, h6');
        expect(heading).not.toBeNull();

        // Check for description (p element)
        const description = card.querySelector('p');
        expect(description).not.toBeNull();

        // Verify heading has text content
        if (heading) {
          expect(heading.textContent.trim().length).toBeGreaterThan(0);
        }

        // Verify description has text content
        if (description) {
          expect(description.textContent.trim().length).toBeGreaterThan(0);
        }
      });
    });
  });
});
