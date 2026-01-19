// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E tests for Content Accuracy
 * Scenario: Verify that all content accurately reflects product capabilities
 *
 * Tests verify that displayed technical specifications match product reality:
 * - Port: 12333
 * - LSM tree levels: 7-level
 * - Memtable size: 4MB skip list
 * - Compression: Snappy
 * - Version: 0.1.0
 */

test.describe('Content Accuracy - Technical Specifications', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Verify default port specification
   * Input: Verify default port specification
   * Expected: Page displays correct default port: 12333
   */
  test('TC1: Page displays correct default port: 12333', async ({ page }) => {
    // Port is displayed in multiple locations:
    // 1. Configuration example in Getting Started section
    // 2. Footer info section

    // Check in Getting Started configuration example
    const gettingStartedSection = page.locator('#getting-started');
    await gettingStartedSection.scrollIntoViewIfNeeded();
    await expect(gettingStartedSection).toBeVisible();

    // Find the configuration code block
    const configStep = gettingStartedSection.locator('[data-testid="step-configure"]');
    await expect(configStep).toBeVisible();

    const configCode = configStep.locator('pre code');
    const configContent = await configCode.textContent();

    // Verify port 12333 in configuration
    expect(configContent).toContain('0.0.0.0:12333');

    // Verify port in telnet example
    const clientStep = gettingStartedSection.locator('[data-testid="step-connect-client"]');
    await expect(clientStep).toBeVisible();

    const clientCode = clientStep.locator('pre code');
    const clientContent = await clientCode.textContent();
    expect(clientContent).toContain('telnet localhost 12333');

    // Check in footer info section
    const footerInfo = page.locator('[data-testid="footer-info"]');
    await footerInfo.scrollIntoViewIfNeeded();
    await expect(footerInfo).toBeVisible();

    const footerContent = await footerInfo.textContent();
    expect(footerContent).toContain('12333');
  });

  /**
   * Test Case 2: Verify LSM tree levels
   * Input: Verify LSM tree levels
   * Expected: Page displays correct LSM tree levels: 7-level
   */
  test('TC2: Page displays correct LSM tree levels: 7-level', async ({ page }) => {
    // LSM tree levels are displayed in multiple locations:
    // 1. Features section - Storage Levels card
    // 2. Architecture section - SSTables component
    // 3. Getting Started configuration example

    // Check in Features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    const storageLevelsCard = page.locator('[data-testid="feature-storage-levels"]');
    await expect(storageLevelsCard).toBeVisible();

    const storageLevelsText = await storageLevelsCard.textContent();
    expect(storageLevelsText).toContain('7-level');
    expect(storageLevelsText).toContain('LSM');

    // Check in Architecture section
    const architectureSection = page.locator('#architecture');
    await architectureSection.scrollIntoViewIfNeeded();
    await expect(architectureSection).toBeVisible();

    const sstableComponent = page.locator('.arch-box.sstable');
    await expect(sstableComponent).toBeVisible();

    const sstableDetail = sstableComponent.locator('.arch-detail');
    await expect(sstableDetail).toContainText('7 Levels');

    // Check in configuration example
    const configStep = page.locator('[data-testid="step-configure"]');
    const configCode = configStep.locator('pre code');
    const configContent = await configCode.textContent();
    expect(configContent).toContain('max_level = 7');
  });

  /**
   * Test Case 3: Verify memtable size
   * Input: Verify memtable size
   * Expected: Page displays correct memtable size: 4MB skip list
   */
  test('TC3: Page displays correct memtable size: 4MB skip list', async ({ page }) => {
    // Memtable size is displayed in multiple locations:
    // 1. Architecture section - Memtable component
    // 2. Architecture explanation
    // 3. Getting Started configuration example

    // Check in Architecture section - Memtable component
    const architectureSection = page.locator('#architecture');
    await architectureSection.scrollIntoViewIfNeeded();
    await expect(architectureSection).toBeVisible();

    const memtableComponent = page.locator('.arch-box.memtable');
    await expect(memtableComponent).toBeVisible();

    const memtableDetail = memtableComponent.locator('.arch-detail');
    await expect(memtableDetail).toContainText('4MB Skip List');

    // Check in Architecture explanation
    const archExplanation = page.locator('.arch-explanation');
    await expect(archExplanation).toBeVisible();

    const explanationText = await archExplanation.textContent();
    expect(explanationText).toContain('4MB');

    // Check in configuration example
    const configStep = page.locator('[data-testid="step-configure"]');
    await configStep.scrollIntoViewIfNeeded();

    const configCode = configStep.locator('pre code');
    const configContent = await configCode.textContent();
    expect(configContent).toContain('mem_table_max_size = "4M"');
  });

  /**
   * Test Case 4: Verify compression type
   * Input: Verify compression type
   * Expected: Page displays correct compression: Snappy
   */
  test('TC4: Page displays correct compression: Snappy', async ({ page }) => {
    // Compression type is displayed in Features section

    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Find the compression feature card
    const compressionCard = page.locator('[data-testid="feature-compression"]');
    await expect(compressionCard).toBeVisible();

    // Verify Snappy compression is mentioned
    const compressionTitle = compressionCard.locator('h3');
    await expect(compressionTitle).toContainText('Snappy Compression');

    const compressionDescription = compressionCard.locator('p');
    const descriptionText = await compressionDescription.textContent();
    expect(descriptionText.toLowerCase()).toContain('snappy');
    expect(descriptionText.toLowerCase()).toContain('compression');
  });

  /**
   * Test Case 5: Verify version number
   * Input: Verify version number
   * Expected: Page displays correct version: 0.1.0
   */
  test('TC5: Page displays correct version: 0.1.0', async ({ page }) => {
    // Version is displayed in footer

    const footer = page.locator('[data-testid="footer-section"]');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Find the version element
    const versionElement = page.locator('[data-testid="footer-version"]');
    await expect(versionElement).toBeVisible();

    // Verify version number
    const versionText = await versionElement.textContent();
    expect(versionText).toContain('Version');
    expect(versionText).toContain('0.1.0');
  });

  /**
   * Additional test: All technical specifications are consistent across the page
   */
  test('Technical specifications are consistent across all page sections', async ({ page }) => {
    // This test verifies that technical specifications are not contradictory
    // in different parts of the page

    // Collect all mentions of port 12333
    const pageContent = await page.content();

    // Verify port is mentioned consistently (12333, not other ports)
    const portMatches = pageContent.match(/:\d{5}/g) || [];
    const portNumbers = portMatches.map(m => m.slice(1));

    // All port references should be 12333
    for (const port of portNumbers) {
      if (port.match(/^12\d{3}$/)) {
        expect(port).toBe('12333');
      }
    }

    // Verify version 0.1.0 is mentioned in the footer
    const footerVersionElement = page.locator('[data-testid="footer-version"]');
    await expect(footerVersionElement).toContainText('0.1.0');
  });
});
