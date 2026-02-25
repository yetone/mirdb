/**
 * Features Section Unit Tests
 * Owner: Scenario 3 - Features Section Display
 *
 * Test cases:
 * - 4-6 feature cards are displayed
 * - Each required feature is present
 * - Feature cards have proper structure (icon, title, description)
 */

const fs = require('fs');
const path = require('path');
const { parseHTML } = require('linkedom');

describe('Features Section', () => {
  let document;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../_site/index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    const { document: doc } = parseHTML(html);
    document = doc;
  });

  test('should have features section', () => {
    const featuresSection = document.querySelector('#features');
    expect(featuresSection).not.toBeNull();
    expect(featuresSection.classList.contains('features')).toBe(true);
  });

  test('should display between 4 and 6 feature cards', () => {
    const featureCards = document.querySelectorAll('.feature-card');
    expect(featureCards.length).toBeGreaterThanOrEqual(4);
    expect(featureCards.length).toBeLessThanOrEqual(6);
  });

  test('should have Memcached protocol feature', () => {
    const featureCards = document.querySelectorAll('.feature-card');
    const hasMemcachedFeature = Array.from(featureCards).some(card => {
      const text = card.textContent.toLowerCase();
      return text.includes('memcached protocol') || text.includes('memcached compatible');
    });
    expect(hasMemcachedFeature).toBe(true);
  });

  test('should have disk persistence feature', () => {
    const featureCards = document.querySelectorAll('.feature-card');
    const hasPersistenceFeature = Array.from(featureCards).some(card => {
      const text = card.textContent.toLowerCase();
      return text.includes('disk persistence') || text.includes('persistent storage');
    });
    expect(hasPersistenceFeature).toBe(true);
  });

  test('should have LSM tree feature', () => {
    const featureCards = document.querySelectorAll('.feature-card');
    const hasLsmFeature = Array.from(featureCards).some(card => {
      const text = card.textContent.toLowerCase();
      return text.includes('lsm tree');
    });
    expect(hasLsmFeature).toBe(true);
  });

  test('should have multi-level compaction feature', () => {
    const featureCards = document.querySelectorAll('.feature-card');
    const hasCompactionFeature = Array.from(featureCards).some(card => {
      const text = card.textContent.toLowerCase();
      return text.includes('multi-level compaction') || text.includes('compaction');
    });
    expect(hasCompactionFeature).toBe(true);
  });

  test('should have async I/O feature', () => {
    const featureCards = document.querySelectorAll('.feature-card');
    const hasAsyncFeature = Array.from(featureCards).some(card => {
      const text = card.textContent.toLowerCase();
      return text.includes('async i/o') || text.includes('asynchronous');
    });
    expect(hasAsyncFeature).toBe(true);
  });

  test('each feature card should have an icon', () => {
    const featureCards = document.querySelectorAll('.feature-card');
    featureCards.forEach(card => {
      const icon = card.querySelector('.feature-icon');
      expect(icon).not.toBeNull();
      const svg = icon.querySelector('svg');
      expect(svg).not.toBeNull();
    });
  });

  test('each feature card should have a title', () => {
    const featureCards = document.querySelectorAll('.feature-card');
    featureCards.forEach(card => {
      const title = card.querySelector('.feature-title');
      expect(title).not.toBeNull();
      expect(title.textContent.trim().length).toBeGreaterThan(0);
    });
  });

  test('each feature card should have a description', () => {
    const featureCards = document.querySelectorAll('.feature-card');
    featureCards.forEach(card => {
      const description = card.querySelector('.feature-description');
      expect(description).not.toBeNull();
      expect(description.textContent.trim().length).toBeGreaterThan(0);
    });
  });
});

describe('Features CSS Styles', () => {
  let cssContent;

  beforeAll(() => {
    const cssPath = path.join(__dirname, '../../_site/assets/css/main.css');
    cssContent = fs.readFileSync(cssPath, 'utf-8');
  });

  test('should have feature-card hover transition defined', () => {
    // Check for hover styles in compiled CSS (transform and box-shadow transitions)
    expect(cssContent).toMatch(/\.feature-card/);
    expect(cssContent).toMatch(/transition/);
    expect(cssContent).toMatch(/transform/);
  });

  test('should have box-shadow styles for hover effect', () => {
    expect(cssContent).toMatch(/box-shadow/);
  });
});
