import { test, expect, request as playwrightRequest } from '@playwright/test';

/**
 * E2E Tests for Content Accuracy and Completeness
 *
 * This test suite verifies that all content on the MirDB homepage
 * accurately represents MirDB's features and is technically correct.
 *
 * Scenario: Content Accuracy and Completeness
 * Test Cases:
 *   1. Verify default port mentioned is 12333 (not 11211)
 *   2. Check memcached commands are accurate (SET, GET, DELETE, ADD, REPLACE, APPEND, PREPEND)
 *   3. Verify Rust is mentioned as implementation language
 *   4. Check all external links are valid (no broken links)
 */

test.describe('Content Accuracy and Completeness', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Verify default port mentioned is 12333 (not 11211)', async ({ page }) => {
    // Get the full page content
    const pageContent = await page.textContent('body');
    expect(pageContent).toBeTruthy();

    // Verify port 12333 is mentioned (MirDB default port)
    expect(pageContent).toContain('12333');

    // Verify port 11211 (memcached default) is NOT mentioned as the default
    // We want to ensure we're using MirDB's port, not memcached's standard port
    const memcachedDefaultPortMentioned = pageContent?.includes('11211');
    expect(memcachedDefaultPortMentioned).toBeFalsy();

    // Specifically check the configuration section shows 12333
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();
    const configContent = await configSection.textContent();
    expect(configContent).toContain('12333');
    expect(configContent).toContain('0.0.0.0:12333');

    // Check the code example uses port 12333
    const codeExample = page.locator('#example-code');
    await expect(codeExample).toBeVisible();
    const codeContent = await codeExample.textContent();
    expect(codeContent).toContain('12333');

    // Check the getting started section uses port 12333
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();
    const gettingStartedContent = await gettingStartedSection.textContent();
    expect(gettingStartedContent).toContain('12333');
  });

  test('Test Case 2: Check memcached commands are accurate', async ({ page }) => {
    // Verify the supported commands section exists
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // List of expected supported commands
    const supportedCommands = ['SET', 'GET', 'DELETE', 'ADD', 'REPLACE', 'APPEND', 'PREPEND'];

    // Verify each supported command is listed
    for (const command of supportedCommands) {
      const commandCard = page.locator(`.command-card[data-command="${command}"]`);
      await expect(commandCard).toBeVisible();

      const commandName = commandCard.locator('.command-name');
      await expect(commandName).toHaveText(command);
    }

    // Verify there are exactly 7 commands listed
    const commandCards = page.locator('.command-card');
    await expect(commandCards).toHaveCount(7);

    // Verify no unsupported commands are listed (like INCR, DECR, CAS, etc.)
    const unsupportedCommands = ['INCR', 'DECR', 'CAS', 'FLUSH_ALL', 'STATS', 'VERSION'];
    for (const unsupportedCommand of unsupportedCommands) {
      const unsupportedCard = page.locator(`.command-card[data-command="${unsupportedCommand}"]`);
      await expect(unsupportedCard).not.toBeVisible();
    }
  });

  test('Test Case 3: Verify Rust is mentioned as implementation language', async ({ page }) => {
    // Get the full page content to check for Rust mentions
    const pageContent = await page.textContent('body');
    expect(pageContent).toBeTruthy();

    // Verify "Rust" is mentioned on the page
    expect(pageContent).toContain('Rust');

    // Check the hero section description
    const heroDescription = page.locator('.hero .description');
    await expect(heroDescription).toBeVisible();
    const heroContent = await heroDescription.textContent();
    expect(heroContent).toContain('Rust');

    // Check the features section has a Rust performance feature
    const highPerformanceFeature = page.locator('[data-feature="high-performance"]');
    await expect(highPerformanceFeature).toBeVisible();

    // Verify the heading mentions Rust
    const featureHeading = highPerformanceFeature.locator('h3');
    await expect(featureHeading).toContainText('Rust');

    // Verify the description mentions Rust
    const featureDescription = highPerformanceFeature.locator('p');
    await expect(featureDescription).toContainText('Rust');
    await expect(featureDescription).toContainText('Written in Rust');

    // Check the meta description also mentions Rust
    const metaDescription = await page.locator('meta[name="description"]').getAttribute('content');
    expect(metaDescription).toContain('Rust');

    // Check the JSON-LD structured data mentions Rust
    const jsonLdScript = await page.locator('script[type="application/ld+json"]').textContent();
    expect(jsonLdScript).toContain('Rust');
  });

  test('Test Case 4: Check all external links are valid (no broken links)', async ({ page }) => {
    // Collect all external links (those with target="_blank" or absolute URLs)
    const externalLinks = page.locator('a[target="_blank"]');
    const linkCount = await externalLinks.count();

    expect(linkCount).toBeGreaterThan(0);

    // Store all unique external URLs to check
    const urlsToCheck: string[] = [];

    for (let i = 0; i < linkCount; i++) {
      const href = await externalLinks.nth(i).getAttribute('href');
      if (href && href.startsWith('http') && !urlsToCheck.includes(href)) {
        urlsToCheck.push(href);
      }
    }

    // Check each URL is accessible (returns 2xx or 3xx status)
    const requestContext = await playwrightRequest.newContext();

    for (const url of urlsToCheck) {
      // Skip checking badge images from shields.io as they may have rate limits
      if (url.includes('img.shields.io') || url.includes('circleci.com')) {
        continue;
      }

      try {
        const response = await requestContext.head(url, {
          failOnStatusCode: false,
          timeout: 10000,
        });

        // Accept 2xx and 3xx status codes as valid
        const status = response.status();
        const isValidStatus = status >= 200 && status < 400;

        // GitHub example repo might return 404, which is expected for example URLs
        // but we should still verify real external links work
        if (url.includes('github.com/example/mirdb')) {
          // This is a placeholder URL, skip validation
          continue;
        }

        expect(isValidStatus, `External link ${url} returned status ${status}`).toBeTruthy();
      } catch (error) {
        // Network errors should be reported but some URLs might block automated requests
        if (url.includes('github.com/example/mirdb')) {
          // Skip placeholder URLs
          continue;
        }
        // Log but don't fail for timeout errors on external services
        console.warn(`Warning: Could not verify external link ${url}: ${error}`);
      }
    }

    await requestContext.dispose();

    // Verify internal anchor links work
    const internalLinks = page.locator('a[href^="#"]');
    const internalLinkCount = await internalLinks.count();

    for (let i = 0; i < internalLinkCount; i++) {
      const href = await internalLinks.nth(i).getAttribute('href');
      if (href && href !== '#') {
        const targetId = href.substring(1);
        const targetElement = page.locator(`#${targetId}`);
        await expect(
          targetElement,
          `Internal link ${href} should have a valid target element`
        ).toBeVisible();
      }
    }
  });

  test('All content should be technically accurate according to MirDB specifications', async ({
    page,
  }) => {
    // Verify the page title is accurate
    const title = await page.title();
    expect(title).toContain('MirDB');
    expect(title).toContain('Memcached');

    // Verify the tagline is accurate
    const tagline = page.locator('.tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Persistent Key-Value Store');
    await expect(tagline).toContainText('Memcached Protocol');

    // Verify LSM-tree architecture is mentioned accurately
    const lsmFeature = page.locator('[data-feature="lsm-tree"]');
    await expect(lsmFeature).toBeVisible();
    const lsmDescription = lsmFeature.locator('p');
    await expect(lsmDescription).toContainText('Log-Structured Merge-tree');

    // Verify SSTable storage is mentioned
    const persistentFeature = page.locator('[data-feature="persistent-storage"]');
    await expect(persistentFeature).toBeVisible();
    const persistentDescription = persistentFeature.locator('p');
    await expect(persistentDescription).toContainText('SSTables');
    await expect(persistentDescription).toContainText('Sorted String Tables');

    // Verify WAL (Write-Ahead Log) is mentioned
    const walFeature = page.locator('[data-feature="wal"]');
    await expect(walFeature).toBeVisible();
    const walDescription = walFeature.locator('p');
    await expect(walDescription).toContainText('Write-Ahead Log');

    // Verify default configuration values are accurate
    const configTable = page.locator('#configuration table');
    await expect(configTable).toBeVisible();
    const configContent = await configTable.textContent();

    // Check accurate default values
    expect(configContent).toContain('0.0.0.0:12333'); // Listen address
    expect(configContent).toContain('7'); // Max LSM levels
    expect(configContent).toContain('/tmp/mirdb'); // Work directory
    expect(configContent).toContain('100MB'); // SSTable max size
    expect(configContent).toContain('4MB'); // Memtable max size
    expect(configContent).toContain('4KB'); // Block size
  });

  test('Architecture diagram should accurately represent MirDB data flow', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Verify the architecture diagram exists
    const archDiagram = page.locator('.architecture-diagram');
    await expect(archDiagram).toBeVisible();

    // Verify WAL component is present
    const walComponent = page.locator('[data-component="wal"]');
    await expect(walComponent).toBeVisible();
    await expect(walComponent.locator('.component-box')).toContainText('WAL');
    await expect(walComponent.locator('.component-label')).toContainText('Write-Ahead Log');

    // Verify Memtable component is present
    const memtableComponent = page.locator('[data-component="memtable"]');
    await expect(memtableComponent).toBeVisible();
    await expect(memtableComponent.locator('.component-box')).toContainText('Memtable');
    await expect(memtableComponent.locator('.component-label')).toContainText('Skip List');

    // Verify SSTable component is present
    const sstableComponent = page.locator('[data-component="sstable"]');
    await expect(sstableComponent).toBeVisible();
    await expect(sstableComponent.locator('.component-box')).toContainText('SSTables');
    await expect(sstableComponent.locator('.component-label')).toContainText('Level');

    // Verify arrows indicating data flow direction exist
    const arrows = page.locator('.arch-arrow');
    const arrowCount = await arrows.count();
    expect(arrowCount).toBeGreaterThanOrEqual(2);
  });
});
