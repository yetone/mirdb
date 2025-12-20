/**
 * Memcached Comparison Tests
 *
 * Scenario: Verify comparison with plain Memcached highlighting persistence advantage
 *
 * Test Cases:
 * 1. Search for comparison content - Page mentions how MirDB differs from standard Memcached
 * 2. Verify persistence differentiator - Content explains MirDB persists data while Memcached is volatile/in-memory only
 * 3. Check for drop-in replacement messaging - Content indicates MirDB is compatible with existing Memcached clients
 *
 * @jest-environment jsdom
 */

const fs = require('fs');
const path = require('path');

describe('Memcached Comparison Section', () => {
  let document;
  let pageContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '..', 'index.html');
    const html = fs.readFileSync(htmlPath, 'utf8');
    const parser = new DOMParser();
    document = parser.parseFromString(html, 'text/html');
    pageContent = document.body.textContent.toLowerCase();
  });

  describe('Test Case 1: Search for comparison content', () => {
    /**
     * Input: Search for comparison content
     * Expected: Page mentions how MirDB differs from standard Memcached
     */
    test('should have a comparison section or content comparing MirDB to Memcached', () => {
      // Look for a dedicated comparison section
      const comparisonSection = document.querySelector('#comparison') ||
                                document.querySelector('.comparison') ||
                                document.querySelector('[data-section="comparison"]') ||
                                document.querySelector('.vs-memcached');

      // If no dedicated section, content should still compare
      const hasComparisonSection = comparisonSection !== null;
      const hasComparisonKeywords = pageContent.includes('vs') ||
                                    pageContent.includes('compared to') ||
                                    pageContent.includes('unlike') ||
                                    pageContent.includes('difference') ||
                                    pageContent.includes('standard memcached') ||
                                    pageContent.includes('traditional memcached') ||
                                    pageContent.includes('plain memcached');

      expect(hasComparisonSection || hasComparisonKeywords).toBe(true);
    });

    test('should explicitly mention how MirDB differs from standard Memcached', () => {
      // The page should explain the key difference between MirDB and Memcached
      const comparisonPatterns = [
        /unlike\s+(traditional\s+|standard\s+|plain\s+)?memcached/i,
        /memcached\s+(is\s+)?(volatile|in-memory|loses|lost)/i,
        /differs?\s+from\s+memcached/i,
        /compared\s+to\s+memcached/i,
        /mirdb\s+vs\.?\s+memcached/i,
        /memcached.*but.*mirdb/i,
        /mirdb.*but.*memcached/i,
        /while\s+memcached/i,
        /whereas\s+memcached/i
      ];

      const hasComparison = comparisonPatterns.some(pattern => pageContent.match(pattern));
      expect(hasComparison).toBe(true);
    });

    test('should mention that Memcached is volatile or in-memory only', () => {
      const volatilePatterns = [
        /memcached.*volatile/i,
        /memcached.*in-memory/i,
        /memcached.*memory.only/i,
        /volatile.*memcached/i,
        /in-memory.*memcached/i
      ];

      const mentionsVolatile = volatilePatterns.some(pattern => pageContent.match(pattern));
      expect(mentionsVolatile).toBe(true);
    });
  });

  describe('Test Case 2: Verify persistence differentiator', () => {
    /**
     * Input: Verify persistence differentiator
     * Expected: Content explains MirDB persists data while Memcached is volatile/in-memory only
     */
    test('should explain that MirDB persists data to disk', () => {
      const persistencePatterns = [
        /mirdb\s+persist/i,
        /persist.*data.*disk/i,
        /data.*persist.*disk/i,
        /persist.*to\s+disk/i,
        /survives?\s+(restarts?|crashes?|failures?)/i,
        /data\s+survives/i,
        /durable/i,
        /durability/i
      ];

      const mentionsPersistence = persistencePatterns.some(pattern => pageContent.match(pattern));
      expect(mentionsPersistence).toBe(true);
    });

    test('should contrast persistence with Memcached volatility', () => {
      // The page should have content that contrasts MirDB's persistence with Memcached's volatility
      const hasContrast = (
        // Pattern 1: Unlike Memcached, MirDB persists...
        (pageContent.includes('unlike') && pageContent.includes('memcached') && pageContent.includes('persist')) ||
        // Pattern 2: Memcached is volatile/in-memory, but MirDB persists
        (pageContent.includes('memcached') && (pageContent.includes('volatile') || pageContent.includes('in-memory')) && pageContent.includes('persist')) ||
        // Pattern 3: Data survives restarts (implicit contrast)
        (pageContent.includes('memcached') && pageContent.includes('survives'))
      );

      expect(hasContrast).toBe(true);
    });

    test('should highlight persistence as a key advantage', () => {
      // Look for text that emphasizes persistence as a benefit
      const advantagePatterns = [
        /never\s+(lose|forgets?)/i,
        /data\s+survives/i,
        /your\s+data\s+survives/i,
        /full\s+durability/i,
        /persists?\s+your\s+data/i,
        /advantages?\s+of/i,
        /key\s+benefit/i,
        /unlike.*your\s+data/i
      ];

      const highlightsAdvantage = advantagePatterns.some(pattern => pageContent.match(pattern)) ||
                                  (pageContent.includes('persist') && pageContent.includes('disk'));

      expect(highlightsAdvantage).toBe(true);
    });
  });

  describe('Test Case 3: Check for drop-in replacement messaging', () => {
    /**
     * Input: Check for drop-in replacement messaging
     * Expected: Content indicates MirDB is compatible with existing Memcached clients
     */
    test('should indicate compatibility with existing Memcached clients', () => {
      const compatibilityPatterns = [
        /existing\s+memcached\s+clients?/i,
        /memcached\s+clients?\s+(work|connect|compatible)/i,
        /compatible\s+with.*clients?/i,
        /use\s+your\s+(existing|current)/i,
        /drop-?in\s+replacement/i,
        /seamless(ly)?/i,
        /connect.*memcached\s+client/i
      ];

      const mentionsCompatibility = compatibilityPatterns.some(pattern => pageContent.match(pattern));
      expect(mentionsCompatibility).toBe(true);
    });

    test('should mention drop-in replacement or seamless migration', () => {
      const dropInPatterns = [
        /drop-?in/i,
        /seamless/i,
        /easy\s+(migration|switch)/i,
        /switch.*without/i,
        /no\s+code\s+changes?/i,
        /minimal\s+changes?/i,
        /existing.*seamless/i
      ];

      const mentionsDropIn = dropInPatterns.some(pattern => pageContent.match(pattern));
      expect(mentionsDropIn).toBe(true);
    });

    test('should emphasize protocol compatibility for easy adoption', () => {
      // The page should make it clear that using MirDB with existing Memcached code is easy
      const hasProtocolCompatibility = pageContent.includes('protocol') &&
                                       (pageContent.includes('compatible') || pageContent.includes('compatibility'));
      const hasExistingClientsMessage = pageContent.includes('existing') &&
                                        (pageContent.includes('client') || pageContent.includes('code'));

      expect(hasProtocolCompatibility || hasExistingClientsMessage).toBe(true);
    });
  });
});
