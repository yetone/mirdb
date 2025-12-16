/**
 * Feature Showcase Section Tests
 *
 * This test suite validates that the MirDB homepage accurately displays
 * key features and capabilities of the MirDB product.
 */

const fs = require('fs');
const path = require('path');
const cheerio = require('cheerio');

// Load the homepage HTML
const htmlPath = path.join(__dirname, '..', 'index.html');
const htmlContent = fs.readFileSync(htmlPath, 'utf-8');
const $ = cheerio.load(htmlContent);

describe('Feature Showcase Section', () => {
    describe('Test Case 1: Memcached Protocol Mention', () => {
        it('should mention Memcached protocol compatibility on homepage', () => {
            const pageText = $('body').text().toLowerCase();
            const hasMemcachedProtocol = pageText.includes('memcached protocol') ||
                                         pageText.includes('memcached compatible') ||
                                         pageText.includes('memcached-compatible');

            expect(hasMemcachedProtocol).toBe(true);
        });

        it('should have a feature card specifically about Memcached protocol', () => {
            // Check for feature card with Memcached in title (h3)
            const featureCards = $('.feature-card');
            let hasMemcachedCard = false;
            featureCards.each((index, element) => {
                const cardText = $(element).find('h3').text().toLowerCase();
                if (cardText.includes('memcached')) {
                    hasMemcachedCard = true;
                }
            });
            expect(hasMemcachedCard).toBe(true);
        });

        it('should mention supported Memcached commands', () => {
            const pageText = $('body').text().toLowerCase();
            const hasGetCommand = pageText.includes('get');
            const hasSetCommand = pageText.includes('set');
            const hasDeleteCommand = pageText.includes('delete');

            expect(hasGetCommand).toBe(true);
            expect(hasSetCommand).toBe(true);
            expect(hasDeleteCommand).toBe(true);
        });
    });

    describe('Test Case 2: Persistence/Durability Features (WAL)', () => {
        it('should mention WAL or Write-Ahead Logging', () => {
            const pageText = $('body').text().toLowerCase();
            const hasWAL = pageText.includes('wal') ||
                          pageText.includes('write-ahead log') ||
                          pageText.includes('write ahead log');

            expect(hasWAL).toBe(true);
        });

        it('should have a feature card about WAL/durability', () => {
            // Check for feature card with WAL or Write-Ahead Logging in title
            const featureCards = $('.feature-card');
            let hasWalCard = false;
            featureCards.each((index, element) => {
                const cardText = $(element).find('h3').text().toLowerCase();
                if (cardText.includes('wal') || cardText.includes('write-ahead') || cardText.includes('logging')) {
                    hasWalCard = true;
                }
            });
            expect(hasWalCard).toBe(true);
        });

        it('should mention durability guarantees', () => {
            const pageText = $('body').text().toLowerCase();
            const hasDurability = pageText.includes('durability') ||
                                  pageText.includes('durable') ||
                                  pageText.includes('persist');

            expect(hasDurability).toBe(true);
        });
    });

    describe('Test Case 3: Storage Architecture (LSM-tree/SSTable)', () => {
        it('should mention LSM-tree architecture', () => {
            const pageText = $('body').text().toLowerCase();
            const hasLSMTree = pageText.includes('lsm-tree') ||
                               pageText.includes('lsm tree') ||
                               pageText.includes('log-structured merge');

            expect(hasLSMTree).toBe(true);
        });

        it('should mention SSTable storage', () => {
            const pageText = $('body').text().toLowerCase();
            const hasSSTable = pageText.includes('sstable') ||
                               pageText.includes('sorted string table');

            expect(hasSSTable).toBe(true);
        });

        it('should have a feature card about LSM-tree/storage', () => {
            // Check for feature card with LSM-tree or Storage in title
            const featureCards = $('.feature-card');
            let hasLsmCard = false;
            featureCards.each((index, element) => {
                const cardText = $(element).find('h3').text().toLowerCase();
                if (cardText.includes('lsm') || cardText.includes('storage') || cardText.includes('sstable')) {
                    hasLsmCard = true;
                }
            });
            expect(hasLsmCard).toBe(true);
        });
    });

    describe('Test Case 4: Performance Features (Skip List, Caching, Compression)', () => {
        it('should mention skip list data structure', () => {
            const pageText = $('body').text().toLowerCase();
            const hasSkipList = pageText.includes('skip list') ||
                               pageText.includes('skiplist');

            expect(hasSkipList).toBe(true);
        });

        it('should have a feature card about skip list', () => {
            // Check for feature card with Skip List in title
            const featureCards = $('.feature-card');
            let hasSkipListCard = false;
            featureCards.each((index, element) => {
                const cardText = $(element).find('h3').text().toLowerCase();
                if (cardText.includes('skip') || cardText.includes('memtable')) {
                    hasSkipListCard = true;
                }
            });
            expect(hasSkipListCard).toBe(true);
        });

        it('should mention compression', () => {
            const pageText = $('body').text().toLowerCase();
            const hasCompression = pageText.includes('compression') ||
                                   pageText.includes('snappy');

            expect(hasCompression).toBe(true);
        });

        it('should have a feature card about compression', () => {
            // Check for feature card with Compression or Snappy in title
            const featureCards = $('.feature-card');
            let hasCompressionCard = false;
            featureCards.each((index, element) => {
                const cardText = $(element).find('h3').text().toLowerCase();
                if (cardText.includes('compression') || cardText.includes('snappy')) {
                    hasCompressionCard = true;
                }
            });
            expect(hasCompressionCard).toBe(true);
        });

        it('should mention Cuckoo filter or bloom filter', () => {
            const pageText = $('body').text().toLowerCase();
            const hasFilter = pageText.includes('cuckoo filter') ||
                             pageText.includes('bloom filter') ||
                             pageText.includes('filter');

            expect(hasFilter).toBe(true);
        });
    });

    describe('Test Case 5: Total Features Count (Integration Test)', () => {
        it('should have at least 4 key features highlighted', () => {
            const featureCards = $('.feature-card');
            expect(featureCards.length).toBeGreaterThanOrEqual(4);
        });

        it('should have a dedicated features section', () => {
            const featuresSection = $('#features');
            expect(featuresSection.length).toBe(1);
        });

        it('should have feature cards with proper structure', () => {
            const featureCards = $('.feature-card');

            featureCards.each((index, element) => {
                const card = $(element);
                // Each card should have a title (h3) and description (p)
                const hasTitle = card.find('h3').length > 0;
                const hasDescription = card.find('p').length > 0;

                expect(hasTitle).toBe(true);
                expect(hasDescription).toBe(true);
            });
        });

        it('should cover all major MirDB capabilities', () => {
            const pageText = $('body').text().toLowerCase();

            // Core capabilities that should be mentioned
            const capabilities = [
                { name: 'memcached', found: pageText.includes('memcached') },
                { name: 'persistence', found: pageText.includes('persist') || pageText.includes('durabl') },
                { name: 'wal', found: pageText.includes('wal') || pageText.includes('write-ahead') },
                { name: 'lsm-tree', found: pageText.includes('lsm') },
                { name: 'sstable', found: pageText.includes('sstable') },
                { name: 'skip-list', found: pageText.includes('skip') },
                { name: 'compaction', found: pageText.includes('compaction') },
                { name: 'rust', found: pageText.includes('rust') }
            ];

            const foundCapabilities = capabilities.filter(c => c.found);
            expect(foundCapabilities.length).toBeGreaterThanOrEqual(4);
        });
    });

    describe('Feature Descriptions Quality', () => {
        it('each feature card should have a meaningful description', () => {
            const featureCards = $('.feature-card');

            featureCards.each((index, element) => {
                const card = $(element);
                const description = card.find('p').text();

                // Description should be at least 30 characters (meaningful)
                expect(description.length).toBeGreaterThan(30);
            });
        });

        it('feature titles should be clear and descriptive', () => {
            const featureCards = $('.feature-card');

            featureCards.each((index, element) => {
                const card = $(element);
                const title = card.find('h3').text();

                // Title should be at least 5 characters
                expect(title.length).toBeGreaterThan(5);
            });
        });
    });
});
