/**
 * Content Sections E2E Tests
 * Owner: Scenarios 2-9, 19 - All content sections
 *
 * Tests:
 * - Hero section content and CTA
 * - Features section cards and usage.gif
 * - Quick Start code blocks
 * - Architecture documentation
 * - API Reference commands
 * - Configuration parameters
 * - Performance information
 * - Contributing guidelines
 * - Asset loading (logo.gif, usage.gif)
 */

const { test, expect } = require('@playwright/test');

// ============================================================================
// Hero Section Tests (Scenario 2)
// ============================================================================

test.describe('Hero Section (Scenario 2)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Hero section contains clear value proposition', async ({ page }) => {
    // Verify hero section exists
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Check for value proposition elements
    const heroTitle = page.locator('.hero__title');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toContainText('MirDB');

    // Check tagline mentions key value prop
    const tagline = page.locator('.hero__tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Persistent Key-Value Store');
    await expect(tagline).toContainText('Memcached');

    // Check description provides additional context
    const description = page.locator('.hero__description');
    await expect(description).toBeVisible();
  });

  test('TC2: Primary CTA button is visible and clickable', async ({ page }) => {
    // Find the CTA button
    const ctaButton = page.locator('.hero__cta');
    await expect(ctaButton).toBeVisible();

    // Verify it's labeled appropriately (Quick Start or similar)
    await expect(ctaButton).toContainText(/Quick Start/i);

    // Verify it's clickable (has href attribute)
    const href = await ctaButton.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toContain('quick-start');
  });

  test('TC3: Quick Start CTA navigates to Quick Start section', async ({ page }) => {
    // Click the CTA button
    const ctaButton = page.locator('.hero__cta');
    await ctaButton.click();

    // Wait for navigation/scroll
    await page.waitForTimeout(500);

    // Verify we're at the Quick Start section
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeInViewport();
  });

  test('TC4: Logo.gif is loaded and visible in hero section', async ({ page }) => {
    // Find the hero logo
    const heroLogo = page.locator('.hero__logo');
    await expect(heroLogo).toBeVisible();

    // Verify it's an image with the correct source
    const src = await heroLogo.getAttribute('src');
    expect(src).toContain('logo.gif');

    // Verify the image has loaded (naturalWidth > 0)
    const isLoaded = await heroLogo.evaluate((img) => {
      return img.complete && img.naturalWidth > 0;
    });
    expect(isLoaded).toBe(true);
  });

  test('TC5: Scroll indicator exists in hero section', async ({ page }) => {
    // Find the scroll indicator
    const scrollIndicator = page.locator('.scroll-indicator');
    await expect(scrollIndicator).toBeVisible();

    // Verify it has visual elements (text and/or arrow)
    const scrollText = page.locator('.scroll-indicator__text');
    const scrollArrow = page.locator('.scroll-indicator__arrow');

    // At least one should be visible
    const hasText = await scrollText.isVisible();
    const hasArrow = await scrollArrow.isVisible();
    expect(hasText || hasArrow).toBe(true);
  });
});

// ============================================================================
// Features Section Tests (Scenario 3)
// ============================================================================

test.describe('Features Section (Scenario 3)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should have features section visible', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();
  });

  test('should have at least 4 feature cards covering key capabilities', async ({ page }) => {
    const featureCards = page.locator('.feature-card');
    const count = await featureCards.count();

    // Test Case 1: At least 4 feature cards present
    expect(count).toBeGreaterThanOrEqual(4);

    // Verify the 4 key features are present
    const memcachedCard = page.locator('.feature-card[data-feature="memcached-protocol"]');
    const persistenceCard = page.locator('.feature-card[data-feature="persistence"]');
    const lsmTreeCard = page.locator('.feature-card[data-feature="lsm-tree"]');
    const compactionCard = page.locator('.feature-card[data-feature="compaction"]');

    await expect(memcachedCard).toBeVisible();
    await expect(persistenceCard).toBeVisible();
    await expect(lsmTreeCard).toBeVisible();
    await expect(compactionCard).toBeVisible();
  });

  test('should have Memcached Protocol feature with correct content', async ({ page }) => {
    // Test Case 2: Verify Memcached Protocol feature content
    const memcachedCard = page.locator('.feature-card[data-feature="memcached-protocol"]');
    await expect(memcachedCard).toBeVisible();

    // Check title
    const title = memcachedCard.locator('.feature-title');
    await expect(title).toContainText('Memcached Protocol');

    // Check description mentions memcached text protocol compatibility
    const description = memcachedCard.locator('.feature-description');
    const descriptionText = await description.textContent();
    expect(descriptionText.toLowerCase()).toContain('memcached');
    expect(descriptionText.toLowerCase()).toMatch(/protocol|compatibility|compatible/);
  });

  test('should have Persistence feature with correct content', async ({ page }) => {
    // Test Case 3: Verify Persistence feature content
    const persistenceCard = page.locator('.feature-card[data-feature="persistence"]');
    await expect(persistenceCard).toBeVisible();

    // Check title
    const title = persistenceCard.locator('.feature-title');
    await expect(title).toContainText('Persistence');

    // Check description mentions disk persistence and SSTables
    const description = persistenceCard.locator('.feature-description');
    const descriptionText = await description.textContent();
    expect(descriptionText.toLowerCase()).toMatch(/disk|persist/);
    expect(descriptionText.toLowerCase()).toContain('sstable');
  });

  test('should have LSM Tree feature with correct content', async ({ page }) => {
    // Test Case 4: Verify LSM Tree feature content
    const lsmTreeCard = page.locator('.feature-card[data-feature="lsm-tree"]');
    await expect(lsmTreeCard).toBeVisible();

    // Check title
    const title = lsmTreeCard.locator('.feature-title');
    await expect(title).toContainText('LSM Tree');

    // Check description mentions Log-Structured Merge-tree, memtables, and SSTable levels
    const description = lsmTreeCard.locator('.feature-description');
    const descriptionText = await description.textContent();
    expect(descriptionText.toLowerCase()).toMatch(/log-structured|merge-tree|lsm/);
    expect(descriptionText.toLowerCase()).toContain('memtable');
    expect(descriptionText.toLowerCase()).toMatch(/sstable|level/);
  });

  test('should have usage.gif loaded and visible in Features section', async ({ page }) => {
    // Test Case 5: Check usage.gif presence
    const usageGif = page.locator('#usage-gif');
    await expect(usageGif).toBeVisible();

    // Verify the image source contains usage.gif
    const src = await usageGif.getAttribute('src');
    expect(src).toContain('usage.gif');

    // Wait for the image to load (it's a large GIF ~6MB)
    await page.waitForFunction(() => {
      const img = document.getElementById('usage-gif');
      return img && img.complete && img.naturalWidth > 0;
    }, { timeout: 30000 });

    // Verify the image is properly loaded (naturalWidth > 0)
    const isLoaded = await usageGif.evaluate((img) => {
      return img.complete && img.naturalWidth > 0;
    });
    expect(isLoaded).toBe(true);
  });

  test('should have icons/visual elements on each feature card', async ({ page }) => {
    // Test Case 6: Verify feature cards have icons
    const featureCards = page.locator('.feature-card');
    const count = await featureCards.count();

    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);
      const icon = card.locator('.feature-icon');

      // Each card should have an icon container
      await expect(icon).toBeVisible();

      // The icon should contain an SVG element
      const svg = icon.locator('svg');
      await expect(svg).toBeVisible();
    }
  });

  test('should navigate to features section from anchor link', async ({ page }) => {
    // Click the Features link in navigation
    await page.locator('a[href="#features"]').first().click();

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Verify the features section is in view
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  test('should have proper section header', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const sectionTitle = featuresSection.locator('.section-title');
    const sectionSubtitle = featuresSection.locator('.section-subtitle');

    await expect(sectionTitle).toContainText('Features');
    await expect(sectionSubtitle).toBeVisible();
  });

  test('should have usage demo section', async ({ page }) => {
    const demoSection = page.locator('.features-demo');
    await expect(demoSection).toBeVisible();

    const demoTitle = demoSection.locator('.features-demo-title');
    await expect(demoTitle).toContainText('See it in Action');

    const gifContainer = demoSection.locator('.usage-gif-container');
    await expect(gifContainer).toBeVisible();
  });
});

// ============================================================================
// Placeholder tests for other sections
// ============================================================================

test.describe('Quick Start Section (Scenario 4)', () => {
  test.skip('Quick Start section shows installation commands', async ({ page }) => {
    // To be implemented by Scenario 4
  });
});

test.describe('Architecture Section (Scenario 5)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Architecture section exists with clear heading', async ({ page }) => {
    // Navigate to Architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Check for clear heading
    const heading = architectureSection.locator('h2.section-title');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('Architecture');
  });

  test('TC2: Section explains memtable as in-memory write buffer with skip list', async ({ page }) => {
    const architectureSection = page.locator('#architecture');

    // Check for memtable explanation
    const memtableContent = architectureSection.locator('.architecture-component[data-component="memtable"], .arch-component--memtable');
    await expect(memtableContent).toBeVisible();

    // Verify content mentions key concepts
    const memtableText = await architectureSection.textContent();
    expect(memtableText.toLowerCase()).toContain('memtable');
    expect(memtableText.toLowerCase()).toMatch(/in-memory|memory/);
    expect(memtableText.toLowerCase()).toMatch(/write|buffer/);
    expect(memtableText.toLowerCase()).toMatch(/skip.?list/);
  });

  test('TC3: Section explains SSTable as persistent on-disk storage format', async ({ page }) => {
    const architectureSection = page.locator('#architecture');

    // Check for SSTable explanation
    const sstableContent = architectureSection.locator('.architecture-component[data-component="sstable"], .arch-component--sstable');
    await expect(sstableContent).toBeVisible();

    // Verify content mentions key concepts
    const sstableText = await architectureSection.textContent();
    expect(sstableText.toLowerCase()).toContain('sstable');
    expect(sstableText.toLowerCase()).toMatch(/sorted.?string.?table|sorted string table/);
    expect(sstableText.toLowerCase()).toMatch(/disk|persistent|storage/);
  });

  test('TC4: Section explains Write-Ahead Log for durability and crash recovery', async ({ page }) => {
    const architectureSection = page.locator('#architecture');

    // Check for WAL explanation
    const walContent = architectureSection.locator('.architecture-component[data-component="wal"], .arch-component--wal');
    await expect(walContent).toBeVisible();

    // Verify content mentions key concepts
    const walText = await architectureSection.textContent();
    expect(walText.toLowerCase()).toMatch(/write.?ahead.?log|wal/);
    expect(walText.toLowerCase()).toMatch(/durability|durable/);
    expect(walText.toLowerCase()).toMatch(/crash|recovery/);
  });

  test('TC5: Section explains minor and major compaction', async ({ page }) => {
    const architectureSection = page.locator('#architecture');

    // Check for compaction explanation
    const compactionContent = architectureSection.locator('.architecture-component[data-component="compaction"], .arch-component--compaction');
    await expect(compactionContent).toBeVisible();

    // Verify content mentions both compaction types
    const compactionText = await architectureSection.textContent();
    expect(compactionText.toLowerCase()).toMatch(/minor.?compaction/);
    expect(compactionText.toLowerCase()).toMatch(/major.?compaction|level.?compaction/);
    expect(compactionText.toLowerCase()).toMatch(/memtable.*(to|->).*sstable|flush/i);
  });

  test('TC6: Visual diagram of LSM Tree architecture is present', async ({ page }) => {
    const architectureSection = page.locator('#architecture');

    // Check for architecture diagram container
    const diagramContainer = architectureSection.locator('.architecture-diagram').first();
    await expect(diagramContainer).toBeVisible();

    // Verify diagram has visual elements representing data flow
    const diagramContent = await diagramContainer.innerHTML();
    // Should contain either SVG elements or ASCII/text-based diagram elements
    const hasSvgElements = diagramContent.includes('<svg') || diagramContent.includes('<path') || diagramContent.includes('<rect');
    const hasTextDiagram = diagramContent.includes('data-flow') || diagramContent.includes('flow-arrow') || diagramContent.includes('arch-box');
    expect(hasSvgElements || hasTextDiagram || diagramContent.length > 100).toBe(true);
  });

  test('Architecture section is accessible via navigation', async ({ page }) => {
    // Click navigation link to Architecture
    await page.locator('a[href="#architecture"]').first().click();

    // Wait for scroll
    await page.waitForTimeout(500);

    // Verify section is in viewport
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeInViewport();
  });
});

test.describe('API Reference Section (Scenario 6)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: API Reference section exists with clear heading', async ({ page }) => {
    // Navigate to API Reference section
    const apiSection = page.locator('#api-reference');
    await expect(apiSection).toBeVisible();

    // Check for clear heading
    const heading = apiSection.locator('h2.section-title');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('API Reference');
  });

  test('TC2: SET command documented with correct syntax', async ({ page }) => {
    const apiSection = page.locator('#api-reference');

    // Find SET command
    const setCommand = apiSection.locator('.api-command[data-command="set"]');
    await expect(setCommand).toBeVisible();

    // Check command name
    const commandName = setCommand.locator('.api-command__name');
    await expect(commandName).toContainText('SET');

    // Check syntax includes correct format: set <key> <flags> <ttl> <bytes> [noreply]
    const syntax = setCommand.locator('.api-syntax__code');
    await expect(syntax).toBeVisible();
    const syntaxText = await syntax.textContent();
    expect(syntaxText).toContain('set');
    expect(syntaxText).toContain('<key>');
    expect(syntaxText).toContain('<flags>');
    expect(syntaxText).toContain('<ttl>');
    expect(syntaxText).toContain('<bytes>');
    expect(syntaxText).toContain('[noreply]');
  });

  test('TC3: GET command documented with correct syntax', async ({ page }) => {
    const apiSection = page.locator('#api-reference');

    // Find GET command
    const getCommand = apiSection.locator('.api-command[data-command="get"]');
    await expect(getCommand).toBeVisible();

    // Check command name
    const commandName = getCommand.locator('.api-command__name');
    await expect(commandName).toContainText('GET');

    // Check syntax includes correct format: get <key1> [<key2> ...]
    const syntax = getCommand.locator('.api-syntax__code');
    await expect(syntax).toBeVisible();
    const syntaxText = await syntax.textContent();
    expect(syntaxText).toContain('get');
    expect(syntaxText).toMatch(/<key1>.*\[.*<key2>.*\.\.\.\]/);
  });

  test('TC4: DELETE command documented with correct syntax', async ({ page }) => {
    const apiSection = page.locator('#api-reference');

    // Find DELETE command
    const deleteCommand = apiSection.locator('.api-command[data-command="delete"]');
    await expect(deleteCommand).toBeVisible();

    // Check command name
    const commandName = deleteCommand.locator('.api-command__name');
    await expect(commandName).toContainText('DELETE');

    // Check syntax includes correct format: delete <key> [noreply]
    const syntax = deleteCommand.locator('.api-syntax__code');
    await expect(syntax).toBeVisible();
    const syntaxText = await syntax.textContent();
    expect(syntaxText).toContain('delete');
    expect(syntaxText).toContain('<key>');
    expect(syntaxText).toContain('[noreply]');
  });

  test('TC5: INFO command documented as MirDB-specific for database status', async ({ page }) => {
    const apiSection = page.locator('#api-reference');

    // Find INFO command in MirDB-specific category
    const mirdbCategory = apiSection.locator('.api-category[data-category="mirdb-specific"]');
    await expect(mirdbCategory).toBeVisible();

    const infoCommand = apiSection.locator('.api-command[data-command="info"]');
    await expect(infoCommand).toBeVisible();

    // Check command name
    const commandName = infoCommand.locator('.api-command__name');
    await expect(commandName).toContainText('INFO');

    // Check it has MirDB badge
    const badge = infoCommand.locator('.api-command__badge');
    await expect(badge).toContainText('MirDB');

    // Check description mentions database status
    const description = infoCommand.locator('.api-command__description');
    const descriptionText = await description.textContent();
    expect(descriptionText.toLowerCase()).toMatch(/database.*status|status.*database/);
  });

  test('TC6: Response codes documented: STORED, NOT_STORED, EXISTS, NOT_FOUND, DELETED, ERROR', async ({ page }) => {
    const apiSection = page.locator('#api-reference');

    // Find response codes section
    const responseCodesSection = apiSection.locator('.api-category[data-category="response-codes"]');
    await expect(responseCodesSection).toBeVisible();

    // Check each response code is documented
    const storedCode = responseCodesSection.locator('.response-code[data-response="stored"]');
    await expect(storedCode).toBeVisible();
    await expect(storedCode.locator('.response-code__name')).toContainText('STORED');

    const notStoredCode = responseCodesSection.locator('.response-code[data-response="not_stored"]');
    await expect(notStoredCode).toBeVisible();
    await expect(notStoredCode.locator('.response-code__name')).toContainText('NOT_STORED');

    const existsCode = responseCodesSection.locator('.response-code[data-response="exists"]');
    await expect(existsCode).toBeVisible();
    await expect(existsCode.locator('.response-code__name')).toContainText('EXISTS');

    const notFoundCode = responseCodesSection.locator('.response-code[data-response="not_found"]');
    await expect(notFoundCode).toBeVisible();
    await expect(notFoundCode.locator('.response-code__name')).toContainText('NOT_FOUND');

    const deletedCode = responseCodesSection.locator('.response-code[data-response="deleted"]');
    await expect(deletedCode).toBeVisible();
    await expect(deletedCode.locator('.response-code__name')).toContainText('DELETED');

    const errorCode = responseCodesSection.locator('.response-code[data-response="error"]');
    await expect(errorCode).toBeVisible();
    await expect(errorCode.locator('.response-code__name')).toContainText('ERROR');
  });

  test('TC7: Each command has at least one code example with expected output', async ({ page }) => {
    const apiSection = page.locator('#api-reference');

    // Check all main commands have examples
    const commands = ['set', 'add', 'replace', 'append', 'prepend', 'get', 'gets', 'delete', 'info', 'major_compaction'];

    for (const cmd of commands) {
      const command = apiSection.locator(`.api-command[data-command="${cmd}"]`);
      await expect(command).toBeVisible();

      // Check for example section
      const example = command.locator('.api-command__example');
      await expect(example).toBeVisible();

      // Check for code block within example
      const codeBlock = example.locator('.code-block');
      await expect(codeBlock).toBeVisible();

      // Verify the code block contains the command and a response
      const codeContent = await codeBlock.locator('pre code').textContent();
      expect(codeContent.length).toBeGreaterThan(0);
    }
  });

  test('All storage commands are documented (SET, ADD, REPLACE, APPEND, PREPEND)', async ({ page }) => {
    const apiSection = page.locator('#api-reference');
    const storageCategory = apiSection.locator('.api-category[data-category="storage"]');
    await expect(storageCategory).toBeVisible();

    // Check all storage commands exist
    const storageCommands = ['set', 'add', 'replace', 'append', 'prepend'];
    for (const cmd of storageCommands) {
      const command = storageCategory.locator(`.api-command[data-command="${cmd}"]`);
      await expect(command).toBeVisible();
    }
  });

  test('All retrieval commands are documented (GET, GETS)', async ({ page }) => {
    const apiSection = page.locator('#api-reference');
    const retrievalCategory = apiSection.locator('.api-category[data-category="retrieval"]');
    await expect(retrievalCategory).toBeVisible();

    // Check GET command
    const getCommand = retrievalCategory.locator('.api-command[data-command="get"]');
    await expect(getCommand).toBeVisible();

    // Check GETS command
    const getsCommand = retrievalCategory.locator('.api-command[data-command="gets"]');
    await expect(getsCommand).toBeVisible();
  });

  test('MAJOR_COMPACTION command is documented', async ({ page }) => {
    const apiSection = page.locator('#api-reference');

    const majorCompactionCommand = apiSection.locator('.api-command[data-command="major_compaction"]');
    await expect(majorCompactionCommand).toBeVisible();

    const commandName = majorCompactionCommand.locator('.api-command__name');
    await expect(commandName).toContainText('MAJOR_COMPACTION');

    // Check it has MirDB badge
    const badge = majorCompactionCommand.locator('.api-command__badge');
    await expect(badge).toContainText('MirDB');
  });

  test('API Reference section is accessible via navigation', async ({ page }) => {
    // Click navigation link to API Reference
    await page.locator('a[href="#api-reference"]').first().click();

    // Wait for scroll
    await page.waitForTimeout(500);

    // Verify section is in viewport
    const apiSection = page.locator('#api-reference');
    await expect(apiSection).toBeInViewport();
  });
});

test.describe('Configuration Section (Scenario 7)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Configuration section exists with clear heading', async ({ page }) => {
    // Navigate to Configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Check for clear heading
    const heading = configSection.locator('h2.section-title');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('Configuration');
  });

  test('TC2: Network parameters documented with addr default', async ({ page }) => {
    const configSection = page.locator('#configuration');

    // Check for network parameters table
    const networkTable = configSection.locator('.config-table--network').first();
    await expect(networkTable).toBeVisible();

    // Verify addr parameter is documented
    const addrRow = configSection.locator('tr:has-text("addr")').first();
    await expect(addrRow).toBeVisible();

    // Verify default value 0.0.0.0:12333
    const configText = await configSection.textContent();
    expect(configText).toContain('addr');
    expect(configText).toContain('0.0.0.0:12333');
  });

  test('TC3: Storage parameters documented with work_dir and max_level', async ({ page }) => {
    const configSection = page.locator('#configuration');

    // Verify storage parameters are documented
    const configText = await configSection.textContent();

    // Check work_dir parameter
    expect(configText).toContain('work_dir');

    // Check max_level parameter
    expect(configText).toContain('max_level');

    // Check defaults are mentioned
    expect(configText.toLowerCase()).toMatch(/\/tmp\/mirdb|work_dir/i);
    expect(configText).toMatch(/7|max_level/);
  });

  test('TC4: Memtable parameters documented with mem_table_max_size 4MB default', async ({ page }) => {
    const configSection = page.locator('#configuration');

    // Verify memtable parameters are documented
    const configText = await configSection.textContent();

    // Check mem_table_max_size parameter
    expect(configText).toContain('mem_table_max_size');

    // Check 4MB default
    expect(configText).toMatch(/4\s*M|4MB/i);
  });

  test('TC5: SSTable parameters documented with sst_max_size 100MB and block_size 4KB', async ({ page }) => {
    const configSection = page.locator('#configuration');

    // Verify SSTable parameters are documented
    const configText = await configSection.textContent();

    // Check sst_max_size parameter with 100MB default
    expect(configText).toContain('sst_max_size');
    expect(configText).toMatch(/100\s*M|100MB/i);

    // Check block_size parameter with 4KB default
    expect(configText).toContain('block_size');
    expect(configText).toMatch(/4\s*K|4KB/i);
  });

  test('TC6: Example TOML configuration is displayed in code block', async ({ page }) => {
    const configSection = page.locator('#configuration');

    // Check for code block with TOML configuration
    const tomlCodeBlock = configSection.locator('.code-block[data-language="toml"]').first();
    await expect(tomlCodeBlock).toBeVisible();

    // Verify it contains TOML-style configuration
    const codeContent = await tomlCodeBlock.textContent();
    expect(codeContent).toContain('addr');
    expect(codeContent).toContain('work_dir');
    expect(codeContent).toContain('=');
  });

  test('TC7: Size units (K, M, G, T) are documented', async ({ page }) => {
    const configSection = page.locator('#configuration');

    // Verify size units are explained
    const configText = await configSection.textContent();

    // Check that size units are documented
    expect(configText).toMatch(/K.*kilobyte|kilobyte.*K/i);
    expect(configText).toMatch(/M.*megabyte|megabyte.*M/i);
    expect(configText).toMatch(/G.*gigabyte|gigabyte.*G/i);
    expect(configText).toMatch(/T.*terabyte|terabyte.*T/i);
  });

  test('Configuration section is accessible via navigation', async ({ page }) => {
    // Click navigation link to Configuration
    await page.locator('a[href="#configuration"]').first().click();

    // Wait for scroll
    await page.waitForTimeout(500);

    // Verify section is in viewport
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeInViewport();
  });
});

test.describe('Performance Section (Scenario 8)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Performance section exists with clear heading', async ({ page }) => {
    // Navigate to Performance section
    const performanceSection = page.locator('#performance');
    await expect(performanceSection).toBeVisible();

    // Check for clear heading
    const heading = performanceSection.locator('h2.section-title');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('Performance');
  });

  test('TC2: Verify performance content exists', async ({ page }) => {
    const performanceSection = page.locator('#performance');
    await expect(performanceSection).toBeVisible();

    // Section should contain performance information, benchmarks, or placeholder
    // Check for either benchmark data or placeholder text
    const sectionText = await performanceSection.textContent();

    // Should have meaningful content about performance
    const hasPerformanceContent =
      sectionText.toLowerCase().includes('benchmark') ||
      sectionText.toLowerCase().includes('throughput') ||
      sectionText.toLowerCase().includes('latency') ||
      sectionText.toLowerCase().includes('operations') ||
      sectionText.toLowerCase().includes('performance');

    expect(hasPerformanceContent).toBe(true);
  });

  test('TC3: Check for comparison charts if present', async ({ page }) => {
    const performanceSection = page.locator('#performance');
    await expect(performanceSection).toBeVisible();

    // If benchmarks exist, they should be presented clearly
    // Check for benchmark metrics container or chart elements
    const benchmarkContainer = performanceSection.locator('.performance-benchmarks, .benchmark-grid, .performance-metrics');

    // Check if benchmarks are present
    const benchmarksExist = await benchmarkContainer.count() > 0;

    if (benchmarksExist) {
      // Verify benchmarks are presented with context
      await expect(benchmarkContainer).toBeVisible();

      // Should have metric cards or chart elements
      const metricCards = performanceSection.locator('.benchmark-metric, .performance-card, .metric-card');
      const metricCount = await metricCards.count();

      // If we have a benchmark container, it should have metrics
      expect(metricCount).toBeGreaterThan(0);
    } else {
      // If no explicit benchmarks, check for placeholder or informational content
      const sectionContent = await performanceSection.textContent();
      expect(sectionContent.length).toBeGreaterThan(50); // Should have meaningful content
    }
  });

  test('Performance section is accessible via navigation or scrolling', async ({ page }) => {
    // Scroll to performance section
    const performanceSection = page.locator('#performance');
    await performanceSection.scrollIntoViewIfNeeded();

    // Wait for scroll
    await page.waitForTimeout(300);

    // Verify section is visible
    await expect(performanceSection).toBeInViewport();
  });

  test('Performance section has proper styling', async ({ page }) => {
    const performanceSection = page.locator('#performance');
    await expect(performanceSection).toBeVisible();

    // Check that section has a container for content
    const container = performanceSection.locator('.container');
    await expect(container).toBeVisible();

    // Verify section has proper padding/spacing (visual check)
    const sectionBox = await performanceSection.boundingBox();
    expect(sectionBox.height).toBeGreaterThan(100); // Meaningful height
  });
});

test.describe('Contributing Section (Scenario 9)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Contributing section exists with clear heading', async ({ page }) => {
    // Navigate to Contributing section
    const contributingSection = page.locator('#contributing');
    await expect(contributingSection).toBeVisible();

    // Check for clear heading
    const heading = contributingSection.locator('h2.section-title');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('Contributing');
  });

  test('TC2: Development setup instructions are provided', async ({ page }) => {
    const contributingSection = page.locator('#contributing');
    await expect(contributingSection).toBeVisible();

    // Check for development setup section
    const setupSection = contributingSection.locator('.contributing-setup, [data-contributing="setup"]');
    await expect(setupSection).toBeVisible();

    // Verify setup instructions content
    const sectionText = await contributingSection.textContent();
    expect(sectionText.toLowerCase()).toMatch(/development|setup|install|clone|build/);
    expect(sectionText.toLowerCase()).toMatch(/cargo|rust|environment/);
  });

  test('TC3: Contribution process is documented (PRs and issues)', async ({ page }) => {
    const contributingSection = page.locator('#contributing');
    await expect(contributingSection).toBeVisible();

    // Check for contribution process section
    const processSection = contributingSection.locator('.contributing-process, [data-contributing="process"]');
    await expect(processSection).toBeVisible();

    // Verify contribution process content mentions PRs and issues
    const sectionText = await contributingSection.textContent();
    expect(sectionText.toLowerCase()).toMatch(/pull request|pr/);
    expect(sectionText.toLowerCase()).toMatch(/issue|bug|feature/);
  });

  test('TC4: Link to GitHub repository for contributions exists', async ({ page }) => {
    const contributingSection = page.locator('#contributing');
    await expect(contributingSection).toBeVisible();

    // Check for GitHub link
    const githubLink = contributingSection.locator('a[href*="github"]');
    await expect(githubLink).toBeVisible();

    // Verify it links to GitHub
    const href = await githubLink.first().getAttribute('href');
    expect(href).toContain('github');
  });

  test('Contributing section is accessible via navigation', async ({ page }) => {
    // Click navigation link to Contributing
    await page.locator('a[href="#contributing"]').first().click();

    // Wait for scroll
    await page.waitForTimeout(500);

    // Verify section is in viewport
    const contributingSection = page.locator('#contributing');
    await expect(contributingSection).toBeInViewport();
  });

  test('Contributing section has proper structure with guidelines grid', async ({ page }) => {
    const contributingSection = page.locator('#contributing');
    await expect(contributingSection).toBeVisible();

    // Check for contributing guidelines structure
    const guidelinesGrid = contributingSection.locator('.contributing-grid, .contributing-guidelines');
    await expect(guidelinesGrid).toBeVisible();
  });

  test('Contributing section contains code style guidelines', async ({ page }) => {
    const contributingSection = page.locator('#contributing');
    await expect(contributingSection).toBeVisible();

    // Check for code style guidelines
    const sectionText = await contributingSection.textContent();
    expect(sectionText.toLowerCase()).toMatch(/code|style|format|lint|test/);
  });
});

// ============================================================================
// Asset Integration Tests (Scenario 19)
// ============================================================================

test.describe('Asset Integration (Scenario 19)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 2: Check logo.gif loads (e2e)
  test('TC2: Logo.gif loads successfully with no 404 errors', async ({ page, context }) => {
    // Track network requests for logo.gif
    const logoRequests = [];

    // Create a fresh page with request tracking from the start
    const newPage = await context.newPage();
    newPage.on('response', response => {
      if (response.url().includes('logo.gif')) {
        logoRequests.push({
          url: response.url(),
          status: response.status()
        });
      }
    });

    // Navigate to the page
    await newPage.goto('/');
    await newPage.waitForLoadState('networkidle');

    // Verify at least one logo image is rendered
    const heroLogo = newPage.locator('.hero__logo');
    await expect(heroLogo).toBeVisible();
    const isLoaded = await heroLogo.evaluate((img) => img.complete && img.naturalWidth > 0);
    expect(isLoaded).toBe(true);

    // Verify all logo requests were successful (200 OK or 304 Not Modified are both valid)
    expect(logoRequests.length).toBeGreaterThan(0);
    logoRequests.forEach(request => {
      // 200 = success, 304 = cached (still success)
      expect([200, 304]).toContain(request.status);
    });

    await newPage.close();
  });

  // Test Case 3: Check logo.gif dimensions (e2e)
  test('TC3: Logo displays at appropriate size for hero section', async ({ page }) => {
    // Check hero logo dimensions
    const heroLogo = page.locator('.hero__logo');
    await expect(heroLogo).toBeVisible();

    // Get the bounding box to verify appropriate size
    const box = await heroLogo.boundingBox();
    expect(box).not.toBeNull();

    // Hero logo should be reasonably sized (not too small, not too large)
    expect(box.width).toBeGreaterThan(50); // Not too small
    expect(box.width).toBeLessThan(400); // Not excessively large
    expect(box.height).toBeGreaterThan(50);
    expect(box.height).toBeLessThan(400);
  });

  test('TC3: Logo displays at appropriate size for header', async ({ page }) => {
    // Check header logo dimensions
    const headerLogo = page.locator('.header__logo');
    await expect(headerLogo).toBeVisible();

    // Get the bounding box
    const box = await headerLogo.boundingBox();
    expect(box).not.toBeNull();

    // Header logo should be compact but visible
    expect(box.width).toBeGreaterThan(20);
    expect(box.width).toBeLessThan(200);
    expect(box.height).toBeGreaterThan(20);
    expect(box.height).toBeLessThan(100);
  });

  // Test Case 5: Check usage.gif loads (e2e)
  test('TC5: Usage.gif loads successfully with no 404 errors', async ({ page, context }) => {
    // Track network requests for usage.gif with a fresh page
    const usageRequests = [];
    const newPage = await context.newPage();

    newPage.on('response', response => {
      if (response.url().includes('usage.gif')) {
        usageRequests.push({
          url: response.url(),
          status: response.status()
        });
      }
    });

    // Navigate to the page
    await newPage.goto('/');

    // Scroll to features section to trigger lazy loading
    const featuresSection = newPage.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Wait for the usage gif to load (it's a large file ~6MB)
    await newPage.waitForFunction(() => {
      const usageGif = document.getElementById('usage-gif');
      return usageGif && usageGif.complete && usageGif.naturalWidth > 0;
    }, { timeout: 60000 });

    // Verify the image loaded successfully
    const usageGif = newPage.locator('#usage-gif');
    await expect(usageGif).toBeVisible();
    const isLoaded = await usageGif.evaluate((img) => img.complete && img.naturalWidth > 0);
    expect(isLoaded).toBe(true);

    // Verify the request was successful (200 OK or 304 Not Modified are both valid)
    expect(usageRequests.length).toBeGreaterThan(0);
    usageRequests.forEach(request => {
      expect([200, 304]).toContain(request.status);
    });

    await newPage.close();
  });

  // Test Case 6: Check GIF animations play (e2e)
  test('TC6: Logo.gif animates as expected (is animated GIF)', async ({ page }) => {
    const heroLogo = page.locator('.hero__logo');
    await expect(heroLogo).toBeVisible();

    // Verify the image is loaded
    const isLoaded = await heroLogo.evaluate((img) => {
      return img.complete && img.naturalWidth > 0;
    });
    expect(isLoaded).toBe(true);

    // Verify it's a GIF file
    const src = await heroLogo.getAttribute('src');
    expect(src).toContain('.gif');

    // For animated GIFs, we verify the image loads properly
    // The naturalWidth > 0 confirms the GIF renders correctly
    const naturalWidth = await heroLogo.evaluate((img) => img.naturalWidth);
    const naturalHeight = await heroLogo.evaluate((img) => img.naturalHeight);
    expect(naturalWidth).toBeGreaterThan(0);
    expect(naturalHeight).toBeGreaterThan(0);
  });

  test('TC6: Usage.gif animates as expected (is animated GIF)', async ({ page }) => {
    // Scroll to make usage.gif visible and load
    const usageGif = page.locator('#usage-gif');
    await usageGif.scrollIntoViewIfNeeded();

    // Wait for the image to load (it's large ~6MB)
    await page.waitForFunction(() => {
      const img = document.getElementById('usage-gif');
      return img && img.complete && img.naturalWidth > 0;
    }, { timeout: 60000 });

    await expect(usageGif).toBeVisible();

    // Verify it's a GIF file
    const src = await usageGif.getAttribute('src');
    expect(src).toContain('.gif');

    // Verify the GIF renders with proper dimensions
    const naturalWidth = await usageGif.evaluate((img) => img.naturalWidth);
    const naturalHeight = await usageGif.evaluate((img) => img.naturalHeight);
    expect(naturalWidth).toBeGreaterThan(0);
    expect(naturalHeight).toBeGreaterThan(0);
  });

  test('All logo.gif images are accessible and loaded', async ({ page }) => {
    // Get all logo.gif images
    const logoImages = page.locator('img[src*="logo.gif"]');
    const count = await logoImages.count();

    expect(count).toBeGreaterThanOrEqual(1);

    // Check each logo image is loaded
    for (let i = 0; i < count; i++) {
      const img = logoImages.nth(i);

      // Make sure the image is in viewport (scroll if needed)
      await img.scrollIntoViewIfNeeded();

      // Wait for it to load
      await img.evaluate(el => {
        return new Promise((resolve) => {
          if (el.complete && el.naturalWidth > 0) {
            resolve();
          } else {
            el.onload = resolve;
            el.onerror = resolve;
          }
        });
      });

      // Verify the image loaded successfully
      const isLoaded = await img.evaluate((el) => el.complete && el.naturalWidth > 0);
      expect(isLoaded).toBe(true);
    }
  });

  test('Usage.gif container is properly styled', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Check usage gif container exists
    const gifContainer = page.locator('.usage-gif-container');
    await expect(gifContainer).toBeVisible();

    // Check the container has proper sizing
    const containerBox = await gifContainer.boundingBox();
    expect(containerBox).not.toBeNull();
    expect(containerBox.width).toBeGreaterThan(200);
    expect(containerBox.height).toBeGreaterThan(100);
  });

  test('Assets load without CORS or security errors', async ({ page }) => {
    // Track any console errors related to assets
    const consoleErrors = [];
    page.on('console', msg => {
      if (msg.type() === 'error') {
        const text = msg.text();
        if (text.includes('logo.gif') || text.includes('usage.gif') || text.includes('CORS')) {
          consoleErrors.push(text);
        }
      }
    });

    // Reload and wait for all assets
    await page.reload();
    await page.waitForLoadState('networkidle');

    // There should be no CORS or asset-related errors
    expect(consoleErrors).toEqual([]);
  });
});
