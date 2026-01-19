// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E tests for Architecture Overview Section
 * Scenario: Verify that the architecture section displays LSM tree visualization
 * and explanation as specified in REQ-4
 */

test.describe('Architecture Overview Section', () => {

  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage
    await page.goto('/');
  });

  /**
   * Test Case 1: Check for architecture diagram
   * Input: Check for architecture diagram
   * Expected: Diagram showing memtable -> immutable memtables -> SSTables flow is displayed
   */
  test('TC1: Architecture diagram showing data flow is displayed', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture, [data-testid="architecture-section"]');
    await expect(architectureSection).toBeVisible();

    // Check for the diagram container
    const diagram = page.locator('.architecture-diagram, [data-testid="architecture-diagram"]');
    await expect(diagram).toBeVisible();

    // Check for Memtable component
    const memtable = page.locator('.memtable, [data-testid="arch-memtable"]');
    await expect(memtable).toBeVisible();
    await expect(memtable).toContainText(/memtable/i);

    // Check for Immutable Memtables component
    const immutableMemtables = page.locator('.immutable, [data-testid="arch-immutable"]');
    await expect(immutableMemtables).toBeVisible();
    await expect(immutableMemtables).toContainText(/immutable/i);

    // Check for SSTables component
    const sstables = page.locator('.sstable, [data-testid="arch-sstable"]');
    await expect(sstables).toBeVisible();
    await expect(sstables).toContainText(/sstable/i);

    // Check for arrows indicating flow direction
    const arrows = page.locator('.arch-arrow');
    const arrowCount = await arrows.count();
    expect(arrowCount).toBeGreaterThanOrEqual(2); // At least 2 arrows for the flow
  });

  /**
   * Test Case 2: Check for LSM tree explanation
   * Input: Check for LSM tree explanation
   * Expected: Brief textual explanation of LSM tree benefits is present
   */
  test('TC2: LSM tree explanation is present', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture, [data-testid="architecture-section"]');
    await expect(architectureSection).toBeVisible();

    // Check for explanation section
    const explanation = page.locator('.arch-explanation, [data-testid="architecture-explanation"]');
    await expect(explanation).toBeVisible();

    // Check that the explanation contains key LSM tree concepts
    const explanationText = await explanation.textContent();

    // Verify Write Path is explained
    expect(explanationText.toLowerCase()).toContain('write');

    // Verify Flush process is explained
    expect(explanationText.toLowerCase()).toContain('flush');

    // Verify Compaction is explained
    expect(explanationText.toLowerCase()).toContain('compaction');

    // Verify Read Path is explained
    expect(explanationText.toLowerCase()).toContain('read');
  });

  /**
   * Test Case 3: Verify diagram has alt text
   * Input: Verify diagram has alt text
   * Expected: Architecture diagram has appropriate alt text for accessibility
   */
  test('TC3: Architecture diagram has appropriate alt text for accessibility', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture, [data-testid="architecture-section"]');
    await expect(architectureSection).toBeVisible();

    // The diagram is implemented using div elements with text, which is accessible
    // Check that the diagram container has appropriate ARIA labeling or the diagram
    // components have accessible text
    const diagram = page.locator('.architecture-diagram, [data-testid="architecture-diagram"]');
    await expect(diagram).toBeVisible();

    // Check for aria-label on the diagram
    const ariaLabel = await diagram.getAttribute('aria-label');
    const role = await diagram.getAttribute('role');

    // Verify the diagram has ARIA attributes for accessibility
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel.toLowerCase()).toContain('lsm');
    expect(ariaLabel.toLowerCase()).toContain('memtable');
    expect(ariaLabel.toLowerCase()).toContain('sstable');

    // Verify the role attribute for screen readers
    expect(role).toBe('img');

    // Also check that text content is accessible (visible text in components)
    const memtableText = await page.locator('.memtable .arch-label, .memtable, [data-testid="arch-memtable"]').first().textContent();
    const immutableText = await page.locator('.immutable .arch-label, .immutable, [data-testid="arch-immutable"]').first().textContent();
    const sstableText = await page.locator('.sstable .arch-label, .sstable, [data-testid="arch-sstable"]').first().textContent();

    // Verify all components have meaningful accessible text
    expect(memtableText.toLowerCase()).toContain('memtable');
    expect(immutableText.toLowerCase()).toContain('immutable');
    expect(sstableText.toLowerCase()).toContain('sstable');
  });

  /**
   * Additional test: Architecture section is properly positioned after Features section
   */
  test('Architecture section appears in the correct page order', async ({ page }) => {
    // Get the y-position of features and architecture sections
    const featuresSection = page.locator('#features');
    const architectureSection = page.locator('#architecture');

    await expect(featuresSection).toBeVisible();
    await expect(architectureSection).toBeVisible();

    const featuresBox = await featuresSection.boundingBox();
    const archBox = await architectureSection.boundingBox();

    // Architecture section should come after features section
    expect(archBox.y).toBeGreaterThan(featuresBox.y);
  });

});
