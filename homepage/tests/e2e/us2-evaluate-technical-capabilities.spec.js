// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for US-2: Evaluate Technical Capabilities
 * User Story: As a Technical Lead, I want to review MirDB's architecture and features,
 * so that I can assess its suitability for our infrastructure.
 *
 * Acceptance Criteria:
 * - Given I am on the homepage
 * - When I scroll to the features section
 * - Then I see detailed descriptions of LSM-tree storage, durability features, and performance characteristics
 * - And I can view an architecture diagram showing the data flow
 *
 * Related Requirements: REQ-2, REQ-5, REQ-9
 */

test.describe('US-2: Evaluate Technical Capabilities', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/');
  });

  /**
   * Test Case 1: Check for LSM-tree storage description
   * Input: Check for LSM-tree storage description
   * Expected: Detailed description of LSM-tree storage engine is present
   */
  test('TC1: LSM-tree storage description is present and detailed', async ({ page }) => {
    // Navigate to features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    // Check for LSM-tree storage feature card
    const persistentStorageCard = page.locator('[data-testid="feature-card-persistent"]');
    await expect(persistentStorageCard).toBeVisible();

    // Verify LSM-tree is mentioned in the feature description
    const featureDesc = page.locator('[data-testid="feature-desc-persistent"]');
    await expect(featureDesc).toContainText('LSM-tree');
    await expect(featureDesc).toContainText('SSTable');

    // Navigate to How It Works section for more detailed LSM-tree architecture
    await page.locator('#how-it-works').scrollIntoViewIfNeeded();

    // Verify the section description mentions LSM-tree architecture
    const howItWorksDesc = page.locator('[data-testid="how-it-works-description"]');
    await expect(howItWorksDesc).toBeVisible();
    await expect(howItWorksDesc).toContainText('Log-Structured Merge-tree');
    await expect(howItWorksDesc).toContainText('LSM-tree');

    // Verify write path explains the LSM-tree flow
    const writePathSteps = page.locator('[data-testid="write-path-steps"]');
    await expect(writePathSteps).toBeVisible();

    // Check that SSTable is mentioned in the write path (the flush step)
    const writeStep4 = page.locator('[data-testid="write-step-4"]');
    await expect(writeStep4).toContainText('SSTable');

    // Check that read path also references SSTable levels
    const readStep3 = page.locator('[data-testid="read-step-3"]');
    await expect(readStep3).toContainText('SSTable');
  });

  /**
   * Test Case 2: Check for durability features description
   * Input: Check for durability features description
   * Expected: WAL and durability features are clearly explained
   */
  test('TC2: WAL and durability features are clearly explained', async ({ page }) => {
    // Navigate to features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    // Check for Write-Ahead Logging feature card
    const walCard = page.locator('[data-testid="feature-card-wal"]');
    await expect(walCard).toBeVisible();

    // Verify WAL title
    const walTitle = page.locator('[data-testid="feature-title-wal"]');
    await expect(walTitle).toContainText('Write-Ahead Logging');

    // Verify WAL description mentions crash recovery and durability
    const walDesc = page.locator('[data-testid="feature-desc-wal"]');
    await expect(walDesc).toContainText('WAL-based');
    await expect(walDesc).toContainText('crash recovery');
    await expect(walDesc).toContainText('Never lose data');

    // Navigate to How It Works section for detailed write path explanation
    await page.locator('#how-it-works').scrollIntoViewIfNeeded();

    // Verify write path step 1 explains WAL for durability
    const writeStep1 = page.locator('[data-testid="write-step-1"]');
    await expect(writeStep1).toBeVisible();
    await expect(writeStep1).toContainText('Write-Ahead Log');
    await expect(writeStep1).toContainText('WAL');
    await expect(writeStep1).toContainText('durability');

    // Verify the architecture diagram shows WAL in the write path
    const diagram = page.locator('[data-testid="architecture-diagram"]');
    await expect(diagram).toBeVisible();
    const diagramContent = await diagram.textContent();
    expect(diagramContent).toContain('Write-Ahead Log');
  });

  /**
   * Test Case 3: Check for architecture diagram
   * Input: Check for architecture diagram
   * Expected: Visual architecture diagram showing data flow is present
   */
  test('TC3: Visual architecture diagram showing data flow is present', async ({ page }) => {
    // Navigate to How It Works section
    await page.locator('#how-it-works').scrollIntoViewIfNeeded();

    // Verify architecture diagram container exists
    const diagramContainer = page.locator('[data-testid="architecture-diagram-container"]');
    await expect(diagramContainer).toBeVisible();

    // Verify Mermaid diagram element exists
    const diagram = page.locator('[data-testid="architecture-diagram"]');
    await expect(diagram).toBeVisible();

    // Wait for Mermaid.js to potentially render the diagram
    await page.waitForTimeout(1500);

    // Verify diagram content shows data flow components
    const diagramContent = await diagram.innerHTML();

    // Check that either SVG is rendered or the flowchart text is present
    const hasDiagramContent = diagramContent.includes('<svg') ||
                               diagramContent.includes('flowchart') ||
                               diagramContent.includes('WriteFlow') ||
                               diagramContent.includes('ReadFlow');
    expect(hasDiagramContent).toBeTruthy();

    // Verify key data flow elements are in the diagram
    const diagramText = await diagram.textContent();
    expect(diagramText).toContain('Write');
    expect(diagramText).toContain('Read');
    expect(diagramText).toContain('Memtable');
    expect(diagramText).toContain('SSTable');

    // Verify write and read path cards explain the data flow
    const writePathCard = page.locator('[data-testid="write-path-card"]');
    const readPathCard = page.locator('[data-testid="read-path-card"]');
    await expect(writePathCard).toBeVisible();
    await expect(readPathCard).toBeVisible();

    // Verify diagram has accessibility attributes
    const diagramFigure = page.locator('.diagram-figure');
    await expect(diagramFigure).toHaveAttribute('role', 'img');

    const ariaLabel = await diagramFigure.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel).toContain('LSM-tree');
  });

  /**
   * Test Case 4: Check for performance characteristics
   * Input: Check for performance characteristics
   * Expected: Performance highlights or capabilities are documented
   */
  test('TC4: Performance highlights or capabilities are documented', async ({ page }) => {
    // Navigate to features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    // Check for High Performance feature card
    const performanceCard = page.locator('[data-testid="feature-card-performance"]');
    await expect(performanceCard).toBeVisible();

    // Verify performance title
    const performanceTitle = page.locator('[data-testid="feature-title-performance"]');
    await expect(performanceTitle).toContainText('High Performance');

    // Verify performance description mentions key performance features
    const performanceDesc = page.locator('[data-testid="feature-desc-performance"]');
    await expect(performanceDesc).toContainText('Skip list');
    await expect(performanceDesc).toContainText('memtables');
    await expect(performanceDesc).toContainText('LRU block cache');
    await expect(performanceDesc).toContainText('fast operations');

    // Check for compression feature (affects performance)
    const compressionCard = page.locator('[data-testid="feature-card-compression"]');
    await expect(compressionCard).toBeVisible();
    const compressionDesc = page.locator('[data-testid="feature-desc-compression"]');
    await expect(compressionDesc).toContainText('Snappy compression');

    // Check for smart filtering feature (affects read performance)
    const filteringCard = page.locator('[data-testid="feature-card-filtering"]');
    await expect(filteringCard).toBeVisible();
    const filteringDesc = page.locator('[data-testid="feature-desc-filtering"]');
    await expect(filteringDesc).toContainText('Cuckoo filters');
    await expect(filteringDesc).toContainText('disk reads');

    // Navigate to How It Works section to verify performance-related read path
    await page.locator('#how-it-works').scrollIntoViewIfNeeded();

    // Verify read path mentions Cuckoo filter for performance optimization
    const readStep4 = page.locator('[data-testid="read-step-4"]');
    await expect(readStep4).toContainText('Cuckoo');
    await expect(readStep4).toContainText('skip files');
  });

  /**
   * Additional Test: Verify technical leads can navigate from features to architecture
   */
  test('Technical lead workflow: Navigate from features to architecture', async ({ page }) => {
    // Step 1: Navigate to features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    // Verify features section is visible
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Verify section title
    const featuresTitle = page.locator('[data-testid="features-title"]');
    await expect(featuresTitle).toContainText('Key Features');

    // Verify all 6 key feature cards are visible
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(6);

    // Step 2: Review architecture diagram
    await page.locator('#how-it-works').scrollIntoViewIfNeeded();

    // Verify How It Works section is visible
    const howItWorksSection = page.locator('[data-testid="how-it-works-section"]');
    await expect(howItWorksSection).toBeVisible();

    // Verify architecture diagram is present
    const diagram = page.locator('[data-testid="architecture-diagram"]');
    await expect(diagram).toBeVisible();

    // Step 3: Find performance information - already covered by feature cards
    // Verify data paths grid is visible for detailed explanations
    const dataPathsGrid = page.locator('[data-testid="data-paths-grid"]');
    await expect(dataPathsGrid).toBeVisible();

    // Both write and read path explanations should be available
    const writePathCard = page.locator('[data-testid="write-path-card"]');
    const readPathCard = page.locator('[data-testid="read-path-card"]');
    await expect(writePathCard).toBeVisible();
    await expect(readPathCard).toBeVisible();
  });

  /**
   * Additional Test: Verify all technical details are comprehensive
   */
  test('Technical details are comprehensive for architecture evaluation', async ({ page }) => {
    // Check features section has all required technical features
    await page.locator('#features').scrollIntoViewIfNeeded();

    // Required technical features as per PRD
    const requiredFeatures = [
      { testId: 'feature-card-memcached', keyword: 'Memcached' },
      { testId: 'feature-card-persistent', keyword: 'LSM-tree' },
      { testId: 'feature-card-wal', keyword: 'WAL' },
      { testId: 'feature-card-performance', keyword: 'Performance' },
      { testId: 'feature-card-compression', keyword: 'Snappy' },
      { testId: 'feature-card-filtering', keyword: 'Cuckoo' }
    ];

    for (const feature of requiredFeatures) {
      const card = page.locator(`[data-testid="${feature.testId}"]`);
      await expect(card).toBeVisible();
      const cardText = await card.textContent();
      expect(cardText).toContain(feature.keyword);
    }

    // Check How It Works section has complete write path explanation
    await page.locator('#how-it-works').scrollIntoViewIfNeeded();

    const writeSteps = page.locator('[data-testid="write-path-steps"] li');
    await expect(writeSteps).toHaveCount(4);

    const readSteps = page.locator('[data-testid="read-path-steps"] li');
    await expect(readSteps).toHaveCount(4);
  });
});
