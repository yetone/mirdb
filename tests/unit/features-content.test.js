/**
 * E2E-equivalent tests for Features Section Content
 * Converted to JSDOM for environment compatibility
 * Tests TC1-TC5: Feature cards and content validation
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

describe('Features Section Display - E2E Tests', () => {
  let document;
  let featuresSection;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
    featuresSection = document.querySelector('#features');
  });

  test('TC1: At least 4 feature cards are displayed', () => {
    // Navigate to features section - verify it exists
    expect(featuresSection).not.toBeNull();

    // Count feature cards
    const featureCards = featuresSection.querySelectorAll('.feature-card');
    const count = featureCards.length;

    expect(count).toBeGreaterThanOrEqual(4);
  });

  test('TC2: Memcached compatibility content is present', () => {
    expect(featuresSection).not.toBeNull();

    // Check for memcached compatibility content
    const featuresText = featuresSection.textContent.toLowerCase();

    // Content should mention 'memcached' and ('compatible' or 'protocol')
    const hasMemcached = featuresText.includes('memcached');
    const hasCompatibleOrProtocol =
      featuresText.includes('compatible') || featuresText.includes('protocol');

    expect(hasMemcached).toBe(true);
    expect(hasCompatibleOrProtocol).toBe(true);
  });

  test('TC3: Persistence content is present', () => {
    expect(featuresSection).not.toBeNull();

    const featuresText = featuresSection.textContent.toLowerCase();

    // Content should mention 'persistent', 'SSTable', or 'survives restarts'
    const hasPersistenceContent =
      featuresText.includes('persistent') ||
      featuresText.includes('sstable') ||
      featuresText.includes('survives restarts');

    expect(hasPersistenceContent).toBe(true);
  });

  test('TC4: LSM tree content is present', () => {
    expect(featuresSection).not.toBeNull();

    const featuresText = featuresSection.textContent.toLowerCase();

    // Content should mention 'LSM tree' or 'compaction'
    const hasLSMContent =
      featuresText.includes('lsm') || featuresText.includes('compaction');

    expect(hasLSMContent).toBe(true);
  });

  test('TC5: Rust implementation content is present', () => {
    expect(featuresSection).not.toBeNull();

    const featuresText = featuresSection.textContent.toLowerCase();

    // Content should mention 'Rust', 'memory-safe', or 'performance'
    const hasRustContent =
      featuresText.includes('rust') ||
      featuresText.includes('memory-safe') ||
      featuresText.includes('performance');

    expect(hasRustContent).toBe(true);
  });
});
