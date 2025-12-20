/**
 * Value Proposition Communication Tests
 *
 * Scenario: Verify the core value proposition (persistent key-value store with
 * Memcached compatibility) is clearly communicated on the landing page.
 *
 * Test Cases:
 * 1. Search page content for persistence keywords
 * 2. Search page content for Memcached keywords
 * 3. Verify key differentiator is communicated
 *
 * @jest-environment jsdom
 */

const fs = require('fs');
const path = require('path');

describe('Value Proposition Communication', () => {
  let pageContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '..', 'index.html');
    const html = fs.readFileSync(htmlPath, 'utf8');
    document.documentElement.innerHTML = html;
    pageContent = document.body.textContent.toLowerCase();
  });

  describe('Test Case 1: Persistence Keywords', () => {
    /**
     * Input: Search page content for persistence keywords
     * Expected: Page contains words like 'persistent', 'persistence', 'durable', or 'disk'
     */
    test('should contain persistence-related keywords', () => {
      const persistenceKeywords = ['persistent', 'persistence', 'durable', 'durability', 'disk'];

      const foundKeywords = persistenceKeywords.filter(keyword =>
        pageContent.includes(keyword)
      );

      expect(foundKeywords.length).toBeGreaterThan(0);
    });

    test('should mention "persistent" in the tagline or hero section', () => {
      const heroSection = document.querySelector('.hero');
      expect(heroSection).not.toBeNull();

      const heroText = heroSection.textContent.toLowerCase();
      expect(heroText).toMatch(/persistent/i);
    });

    test('should mention data persisting to disk', () => {
      expect(pageContent).toMatch(/persist.*disk|disk.*persist/i);
    });
  });

  describe('Test Case 2: Memcached Keywords', () => {
    /**
     * Input: Search page content for Memcached keywords
     * Expected: Page contains 'Memcached', 'protocol', and 'compatible' or 'compatibility'
     */
    test('should contain "Memcached"', () => {
      expect(pageContent).toMatch(/memcached/i);
    });

    test('should contain "protocol"', () => {
      expect(pageContent).toMatch(/protocol/i);
    });

    test('should contain "compatible" or "compatibility"', () => {
      expect(pageContent).toMatch(/compatible|compatibility/i);
    });

    test('should contain all three Memcached-related keywords together', () => {
      const hasMemcached = pageContent.includes('memcached');
      const hasProtocol = pageContent.includes('protocol');
      const hasCompatible = pageContent.includes('compatible') || pageContent.includes('compatibility');

      expect(hasMemcached).toBe(true);
      expect(hasProtocol).toBe(true);
      expect(hasCompatible).toBe(true);
    });

    test('should mention Memcached protocol compatibility in the hero or features section', () => {
      const heroSection = document.querySelector('.hero');
      const featuresSection = document.querySelector('#features, .features');

      const heroText = heroSection ? heroSection.textContent.toLowerCase() : '';
      const featuresText = featuresSection ? featuresSection.textContent.toLowerCase() : '';
      const combinedText = heroText + featuresText;

      expect(combinedText).toMatch(/memcached/i);
      expect(combinedText).toMatch(/protocol/i);
    });
  });

  describe('Test Case 3: Key Differentiator Communication', () => {
    /**
     * Input: Verify key differentiator is communicated
     * Expected: Page explains that unlike Memcached, MirDB persists data to disk
     */
    test('should explain that MirDB persists data unlike traditional Memcached', () => {
      // The page should communicate the key difference: MirDB persists, Memcached does not
      // Look for text that contrasts MirDB with traditional Memcached
      const hasContrast = pageContent.includes('unlike') ||
                          pageContent.includes('traditional memcached') ||
                          pageContent.includes('unlike memcached') ||
                          (pageContent.includes('memcached') && pageContent.includes('persist'));

      expect(hasContrast).toBe(true);
    });

    test('should explain persistence in features section', () => {
      const featuresSection = document.querySelector('#features, .features');
      expect(featuresSection).not.toBeNull();

      const featureCards = featuresSection.querySelectorAll('.feature-card, [class*="feature"]');
      expect(featureCards.length).toBeGreaterThan(0);

      let foundPersistenceFeature = false;
      featureCards.forEach(card => {
        const cardText = card.textContent.toLowerCase();
        if (cardText.includes('persist')) {
          foundPersistenceFeature = true;
          // Should also mention disk or SSTable
          expect(cardText).toMatch(/disk|sstable/i);
        }
      });

      expect(foundPersistenceFeature).toBe(true);
    });

    test('should communicate that data survives restarts or crashes', () => {
      expect(pageContent).toMatch(/survives?\s*(restarts?|crashes?)/i);
    });

    test('should clearly differentiate from volatile Memcached', () => {
      // Check that the page mentions MirDB's persistence advantage over Memcached
      const differentiatorPatterns = [
        /unlike.*memcached/i,
        /traditional\s+memcached/i,
        /memcached.*volatile/i,
        /persist.*unlike/i,
        /survives/i
      ];

      const hasDifferentiator = differentiatorPatterns.some(pattern =>
        pageContent.match(pattern)
      );

      expect(hasDifferentiator).toBe(true);
    });
  });
});
