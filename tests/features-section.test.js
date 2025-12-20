/**
 * @jest-environment jsdom
 */

const fs = require('fs');
const path = require('path');

describe('Features Section Display', () => {
  let document;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '..', 'index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    document = new DOMParser().parseFromString(html, 'text/html');
  });

  test('TC1: Page contains a features section with identifiable feature cards/items', () => {
    // Check for features section element
    const featuresSection = document.querySelector('#features') ||
                            document.querySelector('.features') ||
                            document.querySelector('[data-section="features"]') ||
                            document.querySelector('section.features');

    expect(featuresSection).not.toBeNull();

    // Check for feature cards/items within the section
    const featureCards = featuresSection.querySelectorAll('.feature-card, .feature, .feature-item, [class*="feature"]');
    expect(featureCards.length).toBeGreaterThan(0);
  });

  test('TC2: Feature card mentions Memcached protocol compatibility/support', () => {
    const featuresSection = document.querySelector('#features') ||
                            document.querySelector('.features') ||
                            document.querySelector('[data-section="features"]');

    const featureCards = featuresSection.querySelectorAll('.feature-card, .feature, .feature-item, [class*="feature"]');
    const featureTexts = Array.from(featureCards).map(card => card.textContent.toLowerCase());

    const hasMemcachedFeature = featureTexts.some(text =>
      text.includes('memcached') && (text.includes('protocol') || text.includes('compatible') || text.includes('compatibility'))
    );

    expect(hasMemcachedFeature).toBe(true);
  });

  test('TC3: Feature card mentions data persistence with SSTables', () => {
    const featuresSection = document.querySelector('#features') ||
                            document.querySelector('.features') ||
                            document.querySelector('[data-section="features"]');

    const featureCards = featuresSection.querySelectorAll('.feature-card, .feature, .feature-item, [class*="feature"]');
    const featureTexts = Array.from(featureCards).map(card => card.textContent.toLowerCase());

    const hasPersistenceFeature = featureTexts.some(text =>
      text.includes('persist') && (text.includes('sstable') || text.includes('disk') || text.includes('durable'))
    );

    expect(hasPersistenceFeature).toBe(true);
  });

  test('TC4: Feature card mentions LSM Tree architecture', () => {
    const featuresSection = document.querySelector('#features') ||
                            document.querySelector('.features') ||
                            document.querySelector('[data-section="features"]');

    const featureCards = featuresSection.querySelectorAll('.feature-card, .feature, .feature-item, [class*="feature"]');
    const featureTexts = Array.from(featureCards).map(card => card.textContent.toLowerCase());

    const hasLSMFeature = featureTexts.some(text =>
      text.includes('lsm') && (text.includes('tree') || text.includes('architecture'))
    );

    expect(hasLSMFeature).toBe(true);
  });

  test('TC5: Feature mentions async networking or Tokio', () => {
    const featuresSection = document.querySelector('#features') ||
                            document.querySelector('.features') ||
                            document.querySelector('[data-section="features"]');

    const featureCards = featuresSection.querySelectorAll('.feature-card, .feature, .feature-item, [class*="feature"]');
    const featureTexts = Array.from(featureCards).map(card => card.textContent.toLowerCase());

    const hasAsyncFeature = featureTexts.some(text =>
      (text.includes('async') && text.includes('network')) || text.includes('tokio')
    );

    expect(hasAsyncFeature).toBe(true);
  });

  test('TC6: At least 3 feature cards are displayed', () => {
    const featuresSection = document.querySelector('#features') ||
                            document.querySelector('.features') ||
                            document.querySelector('[data-section="features"]');

    const featureCards = featuresSection.querySelectorAll('.feature-card, .feature, .feature-item, [class*="feature"]');

    expect(featureCards.length).toBeGreaterThanOrEqual(3);
  });
});
