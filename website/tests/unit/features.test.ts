/**
 * Features Section Unit Tests
 * Owner: Scenario 2 - Features Section Display
 *
 * Tests for:
 * - Features section rendering
 * - Feature cards structure and content
 * - Correct number of features
 */
import { describe, it, expect, beforeEach } from 'vitest';
import { JSDOM } from 'jsdom';
import { readFileSync } from 'fs';
import { resolve } from 'path';

// Expected feature data
const EXPECTED_FEATURES = [
  { id: 'persistence', title: 'Disk Persistence' },
  { id: 'lsm-tree', title: 'LSM Tree Architecture' },
  { id: 'memcached-protocol', title: 'Memcached Protocol' },
  { id: 'compression', title: 'Snappy Compression' },
  { id: 'bloom-filters', title: 'Bloom Filters' },
  { id: 'async-io', title: 'Tokio Async Runtime' },
];

describe('Features Component Unit Tests', () => {
  let document: Document;

  beforeEach(() => {
    // Load the HTML file
    const htmlPath = resolve(__dirname, '../../src/index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
  });

  it('TC4: Features component renders correct number of feature cards with proper content', () => {
    // Get features section
    const featuresSection = document.querySelector('#features');
    expect(featuresSection).not.toBeNull();

    // Get all feature cards
    const featureCards = document.querySelectorAll('.feature-card');
    expect(featureCards.length).toBe(EXPECTED_FEATURES.length);

    // Verify each expected feature exists
    for (const feature of EXPECTED_FEATURES) {
      const card = document.querySelector(`[data-feature="${feature.id}"]`);
      expect(card).not.toBeNull();

      // Check title content
      const title = card?.querySelector('.feature-title');
      expect(title?.textContent).toContain(feature.title);
    }
  });

  it('should have proper semantic structure', () => {
    // Check features is a section element
    const featuresSection = document.querySelector('#features');
    expect(featuresSection?.tagName.toLowerCase()).toBe('section');

    // Check h2 exists for section heading
    const h2 = featuresSection?.querySelector('h2');
    expect(h2).not.toBeNull();
    expect(h2?.textContent).toContain('Features');

    // Check feature cards use article elements
    const articles = featuresSection?.querySelectorAll('article.feature-card');
    expect(articles?.length).toBeGreaterThanOrEqual(4);

    // Check feature titles use h3 elements
    const h3Elements = featuresSection?.querySelectorAll('h3.feature-title');
    expect(h3Elements?.length).toBe(articles?.length);
  });

  it('should have icons that are hidden from screen readers', () => {
    const featureIcons = document.querySelectorAll('.feature-icon');

    for (const icon of featureIcons) {
      expect(icon.getAttribute('aria-hidden')).toBe('true');
    }
  });

  it('should have all required elements in each feature card', () => {
    const featureCards = document.querySelectorAll('.feature-card');

    for (const card of featureCards) {
      // Each card should have an icon
      const icon = card.querySelector('.feature-icon');
      expect(icon).not.toBeNull();

      // Each card should have a title
      const title = card.querySelector('.feature-title');
      expect(title).not.toBeNull();
      expect(title?.textContent?.trim().length).toBeGreaterThan(0);

      // Each card should have a description
      const description = card.querySelector('.feature-description');
      expect(description).not.toBeNull();
      expect(description?.textContent?.trim().length).toBeGreaterThan(10);
    }
  });

  it('should have features grid container', () => {
    const featuresGrid = document.querySelector('.features-grid');
    expect(featuresGrid).not.toBeNull();

    // Grid should contain all feature cards
    const cardsInGrid = featuresGrid?.querySelectorAll('.feature-card');
    expect(cardsInGrid?.length).toBe(EXPECTED_FEATURES.length);
  });

  it('should include core MirDB features', () => {
    const coreFeatures = ['persistence', 'lsm-tree', 'memcached-protocol', 'compression'];

    for (const featureId of coreFeatures) {
      const card = document.querySelector(`[data-feature="${featureId}"]`);
      expect(card).not.toBeNull();
    }
  });
});

describe('Features Data Unit Tests', () => {
  let featuresData: Array<{ id: string; title: string; description: string; icon: string }>;

  beforeEach(() => {
    // Load the features.json file
    const jsonPath = resolve(__dirname, '../../src/data/features.json');
    const jsonContent = readFileSync(jsonPath, 'utf-8');
    featuresData = JSON.parse(jsonContent);
  });

  it('should have at least 4 features in features.json', () => {
    expect(featuresData.length).toBeGreaterThanOrEqual(4);
  });

  it('should have required fields for each feature', () => {
    for (const feature of featuresData) {
      expect(feature.id).toBeDefined();
      expect(feature.title).toBeDefined();
      expect(feature.description).toBeDefined();
      expect(feature.icon).toBeDefined();

      expect(typeof feature.id).toBe('string');
      expect(typeof feature.title).toBe('string');
      expect(typeof feature.description).toBe('string');
      expect(typeof feature.icon).toBe('string');
    }
  });

  it('should include core MirDB features', () => {
    const featureIds = featuresData.map((f) => f.id);

    expect(featureIds).toContain('persistence');
    expect(featureIds).toContain('lsm-tree');
    expect(featureIds).toContain('memcached-protocol');
    expect(featureIds).toContain('compression');
  });
});
