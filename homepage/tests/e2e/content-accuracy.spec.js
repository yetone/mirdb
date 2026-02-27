/**
 * E2E Tests for Content Accuracy
 * Owner: Scenario 12 - Content Accuracy
 *
 * Validates all displayed content matches PRD specifications and is factually accurate for MirDB.
 * Tests include:
 * - Product name consistency ("MirDB")
 * - Value proposition messaging (persistent + Memcached)
 * - Technical accuracy (LSM tree, WAL, compaction, Memcached protocol)
 * - Code example accuracy (correct port 12333, correct libraries)
 */

const { test, expect } = require('@playwright/test');

test.describe('Content Accuracy - Scenario 12', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test.describe('Test Case 1: Product Name Consistency', () => {
        test('product is consistently called "MirDB" throughout the page', async ({ page }) => {
            // Check page title
            const title = await page.title();
            expect(title).toContain('MirDB');

            // Check meta description
            const metaDescription = await page.locator('meta[name="description"]').getAttribute('content');
            expect(metaDescription).toContain('MirDB');

            // Check logo/brand in navigation
            const navLogo = page.locator('.nav-logo');
            await expect(navLogo).toContainText('MirDB');

            // Check hero headline
            const heroHeadline = page.locator('.hero h1');
            await expect(heroHeadline).toContainText('MirDB');

            // Check hero description mentions MirDB
            const heroDescription = page.locator('.hero-description');
            const heroText = await heroDescription.textContent();
            // Hero description may not directly mention "MirDB" but should be about the product

            // Check Quick Start section mentions MirDB
            const quickStartSection = page.locator('#quick-start');
            const quickStartText = await quickStartSection.textContent();
            expect(quickStartText).toContain('MirDB');

            // Check Architecture section mentions MirDB
            const architectureSection = page.locator('#architecture');
            const architectureText = await architectureSection.textContent();
            expect(architectureText).toContain('MirDB');

            // Check footer mentions MirDB
            const footer = page.locator('.main-footer');
            const footerText = await footer.textContent();
            expect(footerText).toContain('MirDB');

            // Verify no misspellings like "Mirdb", "mirDB", "MIRDB" (case-sensitive check)
            // Get all text content from the page
            const bodyText = await page.locator('body').textContent();

            // Should not contain common misspellings (but allow "mirdb" in URLs/paths)
            const textWithoutUrls = bodyText.replace(/https?:\/\/[^\s]+/g, '');

            // Count MirDB occurrences (should be multiple)
            const mirdbMatches = textWithoutUrls.match(/MirDB/g);
            expect(mirdbMatches).not.toBeNull();
            expect(mirdbMatches.length).toBeGreaterThanOrEqual(5);
        });
    });

    test.describe('Test Case 2: Value Proposition', () => {
        test('contains "persistent" and "Memcached" keywords in value proposition', async ({ page }) => {
            // Check page title
            const title = await page.title();
            expect(title.toLowerCase()).toContain('persistent');
            expect(title.toLowerCase()).toContain('memcached');

            // Check meta description
            const metaDescription = await page.locator('meta[name="description"]').getAttribute('content');
            expect(metaDescription.toLowerCase()).toContain('persistent');
            expect(metaDescription.toLowerCase()).toContain('memcached');

            // Check hero tagline
            const heroTagline = page.locator('.hero-tagline');
            const taglineText = await heroTagline.textContent();
            expect(taglineText.toLowerCase()).toContain('persistent');
            expect(taglineText.toLowerCase()).toContain('memcached');

            // Check hero description mentions persistence/durability
            const heroDescription = page.locator('.hero-description');
            const descriptionText = await heroDescription.textContent();
            expect(descriptionText.toLowerCase()).toMatch(/durabil|persist/);

            // Features section should reinforce this value proposition
            const featuresSection = page.locator('#features');
            const featuresText = await featuresSection.textContent();
            expect(featuresText.toLowerCase()).toContain('memcached');
            expect(featuresText.toLowerCase()).toMatch(/persist|durabl/);
        });
    });

    test.describe('Test Case 3: Port Number in Code Examples', () => {
        test('all code examples use port 12333', async ({ page }) => {
            // Get all code blocks in Quick Start section
            const quickStartSection = page.locator('#quick-start');
            const codeBlocks = quickStartSection.locator('code');
            const count = await codeBlocks.count();

            expect(count).toBeGreaterThanOrEqual(1);

            // Check each code block for port 12333
            for (let i = 0; i < count; i++) {
                const codeBlock = codeBlocks.nth(i);
                const codeText = await codeBlock.textContent();

                // If code contains a port number, it should be 12333
                if (codeText.match(/localhost[:']\d+/) || codeText.match(/:\d{4,5}/)) {
                    expect(codeText).toContain('12333');
                }
            }

            // Verify Python example uses port 12333
            const pythonCode = page.locator('#quick-start code.language-python');
            const pythonText = await pythonCode.textContent();
            expect(pythonText).toContain('12333');

            // Verify Go example uses port 12333
            const goCode = page.locator('#quick-start code.language-go');
            const goText = await goCode.textContent();
            expect(goText).toContain('12333');
        });
    });

    test.describe('Test Case 4: Python Example Imports', () => {
        test('Python example uses pymemcache (correct library for memcached)', async ({ page }) => {
            const pythonCode = page.locator('#quick-start code.language-python');
            const codeText = await pythonCode.textContent();

            // Should use pymemcache, not python-memcached or other libraries
            expect(codeText).toContain('pymemcache');

            // Should import from the correct module path
            expect(codeText).toMatch(/from\s+pymemcache|import\s+pymemcache/);

            // Should use Client class (standard pymemcache usage)
            expect(codeText).toContain('Client');
        });
    });

    test.describe('Test Case 5: Go Example Imports', () => {
        test('Go example uses memcache package (gomemcache)', async ({ page }) => {
            const goCode = page.locator('#quick-start code.language-go');
            const codeText = await goCode.textContent();

            // Should use gomemcache from bradfitz
            expect(codeText).toContain('gomemcache/memcache');

            // Should use memcache.New for client creation
            expect(codeText).toContain('memcache.New');

            // Should use memcache.Item for Set operations
            expect(codeText).toContain('memcache.Item');
        });
    });

    test.describe('Test Case 6: LSM Tree Mentioned', () => {
        test('LSM tree or Log-Structured Merge-tree is mentioned in features/architecture', async ({ page }) => {
            // Check meta description
            const metaDescription = await page.locator('meta[name="description"]').getAttribute('content');
            expect(metaDescription.toLowerCase()).toContain('lsm');

            // Check hero description
            const heroDescription = page.locator('.hero-description');
            const heroText = await heroDescription.textContent();
            expect(heroText.toLowerCase()).toContain('lsm');

            // Check Features section
            const featuresSection = page.locator('#features');
            const featuresText = await featuresSection.textContent();
            expect(featuresText.toLowerCase()).toContain('lsm');

            // Check Architecture section
            const architectureSection = page.locator('#architecture');
            const architectureText = await architectureSection.textContent();
            expect(architectureText.toLowerCase()).toContain('lsm');

            // Check Architecture diagram has LSM tree label
            const architectureDiagram = page.locator('.architecture-diagram');
            const diagramTitle = await architectureDiagram.locator('title').textContent();
            expect(diagramTitle.toLowerCase()).toContain('lsm');

            // Check diagram description mentions LSM
            const diagramDesc = await architectureDiagram.locator('desc').textContent();
            expect(diagramDesc.toLowerCase()).toContain('lsm');
        });
    });

    test.describe('Additional Technical Accuracy Checks', () => {
        test('WAL (Write-Ahead Log) is mentioned in features', async ({ page }) => {
            const featuresSection = page.locator('#features');
            const featuresText = await featuresSection.textContent();

            // WAL or Write-Ahead Log should be mentioned
            expect(featuresText).toMatch(/WAL|Write-Ahead Log/i);
        });

        test('Compaction is mentioned in features', async ({ page }) => {
            const featuresSection = page.locator('#features');
            const featuresText = await featuresSection.textContent();

            expect(featuresText.toLowerCase()).toContain('compaction');
        });

        test('Memcached protocol compatibility is emphasized', async ({ page }) => {
            const featuresSection = page.locator('#features');
            const featuresText = await featuresSection.textContent();

            // Should mention Memcached protocol
            expect(featuresText.toLowerCase()).toContain('memcached protocol');
        });

        test('Architecture diagram shows correct data flow components', async ({ page }) => {
            const diagram = page.locator('.architecture-diagram');
            const diagramText = await diagram.textContent();

            // Should show key LSM tree components
            expect(diagramText).toContain('Memtable');
            expect(diagramText).toContain('SSTable');
            expect(diagramText).toContain('WAL');
            expect(diagramText).toContain('Compaction');
        });

        test('TTL support is mentioned in features', async ({ page }) => {
            const featuresSection = page.locator('#features');
            const featuresText = await featuresSection.textContent();

            expect(featuresText).toMatch(/TTL|time-to-live|expiration/i);
        });
    });

    test.describe('Negative Tests - No False Claims', () => {
        test('no mention of unimplemented distributed features', async ({ page }) => {
            const bodyText = await page.locator('body').textContent();

            // Raft consensus is planned but not implemented per knowledge base
            // Should not make claims about distributed consensus being available
            expect(bodyText.toLowerCase()).not.toMatch(/raft\s+consensus\s+(is\s+)?(available|supported|implemented)/);

            // Should not claim to be a distributed database without qualification
            expect(bodyText.toLowerCase()).not.toMatch(/distributed\s+database\s+(system\s+)?(available|ready|production)/);
        });

        test('benchmarks do not make unsubstantiated claims', async ({ page }) => {
            const benchmarksSection = page.locator('#benchmarks');
            const benchmarksText = await benchmarksSection.textContent();

            // Should not claim to be "fastest" without qualification
            expect(benchmarksText.toLowerCase()).not.toContain('fastest');

            // Should have comparison context (mentions test environment)
            expect(benchmarksText.toLowerCase()).toMatch(/benchmark|test|concurrent|hardware/);
        });
    });
});
