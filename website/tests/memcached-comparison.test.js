/**
 * Tests for Memcached Comparison and Persistence Advantage
 *
 * Scenario: Verify that the homepage clearly communicates the persistence
 * advantage over standard memcached through comparison content.
 *
 * Test Cases:
 * 1. Search page content for persistence-related keywords
 * 2. Search page content for memcached comparison
 * 3. Verify 'Why MirDB' or comparison section exists
 *
 * @jest-environment jsdom
 */

const fs = require('fs');
const path = require('path');

// Load HTML content before tests
beforeEach(() => {
  const htmlPath = path.join(__dirname, '..', 'index.html');
  const html = fs.readFileSync(htmlPath, 'utf8');
  document.body.innerHTML = html;
});

describe('Memcached Comparison and Persistence Advantage', () => {
  /**
   * Test Case 1: Search page content for persistence-related keywords
   * Expected: Page contains text explaining persistence advantage
   * (data survives restarts, durable storage)
   */
  describe('Test Case 1: Persistence-Related Content', () => {
    test('should contain persistence-related keywords in page content', () => {
      const pageText = document.body.textContent.toLowerCase();

      // Check for persistence-related keywords
      const persistenceKeywords = ['persist', 'persistent', 'durability', 'durable'];
      const hasPersistenceKeyword = persistenceKeywords.some(keyword =>
        pageText.includes(keyword)
      );

      expect(hasPersistenceKeyword).toBe(true);
    });

    test('should explain that data survives restarts', () => {
      const pageText = document.body.textContent.toLowerCase();

      // Check for "survives restarts" or similar phrasing
      const survivesRestarts = pageText.includes('survives restart') ||
                               pageText.includes('survives restarts') ||
                               pageText.includes('survive restarts') ||
                               pageText.includes('persist') && pageText.includes('restart');

      expect(survivesRestarts).toBe(true);
    });

    test('should mention durable storage concept', () => {
      const pageText = document.body.textContent.toLowerCase();

      // Check for durable storage concept - can be SSTables, disk storage, WAL, etc.
      const durableStorageConcepts = [
        'durable',
        'durability',
        'disk',
        'sstable',
        'write-ahead log',
        'wal',
        'persists to disk',
        'persistent storage'
      ];

      const hasDurableStorage = durableStorageConcepts.some(concept =>
        pageText.includes(concept)
      );

      expect(hasDurableStorage).toBe(true);
    });

    test('should clearly state persistence as a key differentiator', () => {
      // Look for persistence in prominent places (features, hero, or comparison)
      const persistentStorageFeature = document.querySelector('[data-feature="persistent-storage"]');
      const heroText = document.querySelector('.hero-description, .hero-tagline');
      const whySection = document.querySelector('#why-mirdb, #comparison, [data-section="why-mirdb"]');

      const hasPersisentFeature = persistentStorageFeature !== null;
      const heroMentionsPersistence = heroText && heroText.textContent.toLowerCase().includes('persist');
      const whySectionExists = whySection !== null;

      expect(hasPersisentFeature || heroMentionsPersistence || whySectionExists).toBe(true);
    });
  });

  /**
   * Test Case 2: Search page content for memcached comparison
   * Expected: Page contains comparison or mention of memcached compatibility/alternative
   */
  describe('Test Case 2: Memcached Comparison Content', () => {
    test('should mention memcached in page content', () => {
      const pageText = document.body.textContent.toLowerCase();

      expect(pageText).toContain('memcached');
    });

    test('should highlight memcached protocol compatibility', () => {
      const pageText = document.body.textContent.toLowerCase();

      // Should mention protocol compatibility
      const hasProtocolCompatibility =
        (pageText.includes('memcached') && pageText.includes('protocol')) ||
        (pageText.includes('memcached') && pageText.includes('compatible'));

      expect(hasProtocolCompatibility).toBe(true);
    });

    test('should communicate drop-in replacement potential', () => {
      const pageText = document.body.textContent.toLowerCase();

      // Should mention drop-in replacement or similar concept
      const dropInConcepts = [
        'drop-in',
        'drop in',
        'replacement',
        'existing clients',
        'without modification',
        'without any code changes',
        'no code changes'
      ];

      const hasDropInConcept = dropInConcepts.some(concept =>
        pageText.includes(concept)
      );

      expect(hasDropInConcept).toBe(true);
    });

    test('should contrast MirDB with standard memcached', () => {
      const pageText = document.body.textContent.toLowerCase();

      // Should have text that contrasts MirDB vs memcached
      const contrastPhrases = [
        'unlike memcached',
        'compared to memcached',
        'vs memcached',
        'memcached alternative',
        'memcached but',
        'memcached with persistence'
      ];

      const hasContrast = contrastPhrases.some(phrase =>
        pageText.includes(phrase)
      );

      expect(hasContrast).toBe(true);
    });

    test('should have memcached protocol feature card', () => {
      const memcachedFeature = document.querySelector('[data-feature="memcached-protocol"]');

      expect(memcachedFeature).toBeInTheDocument();

      const featureTitle = memcachedFeature.querySelector('.feature-title');
      expect(featureTitle.textContent.toLowerCase()).toContain('memcached');
    });
  });

  /**
   * Test Case 3: Verify 'Why MirDB' or comparison section exists
   * Expected: Dedicated section explaining MirDB's advantages over alternatives
   */
  describe('Test Case 3: Why MirDB / Comparison Section', () => {
    test('should have a dedicated comparison or "Why MirDB" section', () => {
      // Look for a comparison section by various possible IDs or data attributes
      const comparisonSection = document.querySelector(
        '#why-mirdb, #comparison, #advantages, ' +
        '[data-section="why-mirdb"], [data-section="comparison"], ' +
        'section.why-mirdb, section.comparison'
      );

      expect(comparisonSection).toBeInTheDocument();
    });

    test('comparison section should have appropriate heading', () => {
      const comparisonSection = document.querySelector(
        '#why-mirdb, #comparison, #advantages, ' +
        '[data-section="why-mirdb"], [data-section="comparison"]'
      );

      expect(comparisonSection).toBeInTheDocument();

      const heading = comparisonSection.querySelector('h2');
      expect(heading).toBeInTheDocument();

      const headingText = heading.textContent.toLowerCase();
      const validHeadings = ['why', 'comparison', 'advantage', 'vs', 'better', 'different'];
      const hasValidHeading = validHeadings.some(term => headingText.includes(term));

      expect(hasValidHeading).toBe(true);
    });

    test('comparison section should explain advantages over memcached', () => {
      const comparisonSection = document.querySelector(
        '#why-mirdb, #comparison, #advantages, ' +
        '[data-section="why-mirdb"], [data-section="comparison"]'
      );

      expect(comparisonSection).toBeInTheDocument();

      const sectionText = comparisonSection.textContent.toLowerCase();

      // Should mention both memcached and persistence
      expect(sectionText).toContain('memcached');
      expect(sectionText).toMatch(/persist|durable|survives/);
    });

    test('comparison section should be navigable from main navigation', () => {
      const navLink = document.querySelector(
        'nav a[href="#why-mirdb"], nav a[href="#comparison"], nav a[href="#advantages"]'
      );

      expect(navLink).toBeInTheDocument();
    });

    test('comparison section should have clear benefit statements', () => {
      const comparisonSection = document.querySelector(
        '#why-mirdb, #comparison, #advantages, ' +
        '[data-section="why-mirdb"], [data-section="comparison"]'
      );

      expect(comparisonSection).toBeInTheDocument();

      // Should have comparison items or benefit points
      const comparisonItems = comparisonSection.querySelectorAll(
        '.comparison-item, .advantage-item, .benefit-item, li, .comparison-row'
      );

      // Should have at least 2 benefit/comparison points
      expect(comparisonItems.length).toBeGreaterThanOrEqual(2);
    });
  });

  /**
   * Additional tests for comprehensive coverage
   */
  describe('Additional Comparison and Persistence Tests', () => {
    test('hero section should mention key differentiator', () => {
      const heroSection = document.querySelector('.hero, #hero, header');
      expect(heroSection).toBeInTheDocument();

      const heroText = heroSection.textContent.toLowerCase();

      // Hero should mention persistence or memcached compatibility
      const hasDifferentiator =
        heroText.includes('persist') ||
        heroText.includes('memcached');

      expect(hasDifferentiator).toBe(true);
    });

    test('page should have proper SEO meta description mentioning key benefits', () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      expect(metaDescription).toBeInTheDocument();

      const content = metaDescription.getAttribute('content').toLowerCase();

      // Meta description should mention key benefits
      expect(content).toMatch(/persist|memcached|key-value/);
    });

    test('should explain what happens to data on restart with memcached vs MirDB', () => {
      const pageText = document.body.textContent.toLowerCase();

      // Should communicate the restart behavior difference
      const explainsBehavior =
        pageText.includes('survives restart') ||
        pageText.includes('data survives') ||
        (pageText.includes('unlike memcached') && pageText.includes('persist')) ||
        pageText.includes('lost on restart') ||
        pageText.includes('data loss');

      expect(explainsBehavior).toBe(true);
    });
  });
});
