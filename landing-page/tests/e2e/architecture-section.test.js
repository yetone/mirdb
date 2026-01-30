/**
 * MirDB Landing Page - Architecture Section E2E Tests
 * Owner: Scenario 4 - Architecture Overview Section
 *
 * Tests for:
 * - LSM-tree architecture diagram presence
 * - Memtable component in diagram
 * - SSTable components in diagram
 * - WAL explanation text
 * - Compaction explanation text
 * - Interactive hover states on diagram components
 */

const { test, expect } = require('@playwright/test');
const { setupPage, waitForAnimations } = require('../helpers/test-utils');

test.describe('Architecture Section', () => {
  test.beforeEach(async ({ page }) => {
    await setupPage(page);
    // Scroll to architecture section
    await page.locator('#architecture').scrollIntoViewIfNeeded();
    await waitForAnimations(page, 500);
  });

  test('architecture diagram is present', async ({ page }) => {
    // Test Case 1: Check for architecture diagram
    const diagram = page.locator('.architecture-diagram');
    await expect(diagram).toBeVisible();

    // Verify it has the role of img with proper aria-label
    await expect(diagram).toHaveAttribute('role', 'img');
    const ariaLabel = await diagram.getAttribute('aria-label');
    expect(ariaLabel).toContain('LSM-tree');
  });

  test('diagram shows memtable component', async ({ page }) => {
    // Test Case 2: Verify diagram shows memtable component
    const memtableNode = page.locator('[data-component="memtable"]');
    await expect(memtableNode).toBeVisible();

    // Check that memtable label is present
    const memtableLabel = memtableNode.locator('.node-label');
    await expect(memtableLabel).toContainText('Memtable');

    // Check that skip list detail is mentioned
    const memtableDetail = memtableNode.locator('.node-detail');
    await expect(memtableDetail).toContainText('Skip List');
  });

  test('diagram shows SSTable components', async ({ page }) => {
    // Test Case 3: Verify diagram shows SSTables and multiple levels
    // Check for Level 0 SSTables
    const level0Node = page.locator('[data-component="level0"]');
    await expect(level0Node).toBeVisible();
    const level0Label = level0Node.locator('.node-label');
    await expect(level0Label).toContainText('Level 0 SSTables');

    // Check for Level 1+ SSTables
    const levelsNode = page.locator('[data-component="levels"]');
    await expect(levelsNode).toBeVisible();
    const levelsLabel = levelsNode.locator('.node-label');
    await expect(levelsLabel).toContainText('Level 1+ SSTables');
  });

  test('WAL explanation is present', async ({ page }) => {
    // Test Case 4: Check for WAL explanation
    const walExplanation = page.locator('#explanation-wal');
    await expect(walExplanation).toBeVisible();

    // Check for heading
    const walHeading = walExplanation.locator('h3');
    await expect(walHeading).toContainText('Write-Ahead Log');

    // Check for durability keyword
    const walContent = await walExplanation.textContent();
    expect(walContent.toLowerCase()).toContain('durability');

    // WAL node should also be in diagram
    const walNode = page.locator('[data-component="wal"]');
    await expect(walNode).toBeVisible();
  });

  test('compaction explanation is present', async ({ page }) => {
    // Test Case 5: Check for compaction explanation
    const compactionExplanation = page.locator('#explanation-compaction');
    await expect(compactionExplanation).toBeVisible();

    // Check for heading
    const compactionHeading = compactionExplanation.locator('h3');
    await expect(compactionHeading).toContainText('Compaction');

    // Check for minor and major compaction mentions
    const compactionContent = await compactionExplanation.textContent();
    expect(compactionContent.toLowerCase()).toContain('minor');
    expect(compactionContent.toLowerCase()).toContain('major');

    // Verify minor compaction explanation
    expect(compactionContent).toContain('Flushes immutable memtables');

    // Verify major compaction explanation
    expect(compactionContent).toContain('Merges Level N SSTables');
  });

  test('diagram has interactive hover states', async ({ page }) => {
    // Test Case 6: Hovering over diagram components shows additional information

    // Test memtable hover
    const memtableNode = page.locator('[data-component="memtable"]');
    const memtableTooltip = page.locator('#tooltip-memtable');

    // Initially tooltip should not be visible
    await expect(memtableTooltip).not.toBeVisible();

    // Hover over the memtable node
    await memtableNode.hover();
    await waitForAnimations(page, 300);

    // Tooltip should now be visible
    await expect(memtableTooltip).toBeVisible();

    // Tooltip should contain additional information
    const tooltipText = await memtableTooltip.textContent();
    expect(tooltipText).toContain('in-memory');
    expect(tooltipText).toContain('skip list');

    // Test WAL hover
    const walNode = page.locator('[data-component="wal"]');
    const walTooltip = page.locator('#tooltip-wal');

    await walNode.hover();
    await waitForAnimations(page, 300);
    await expect(walTooltip).toBeVisible();

    // WAL tooltip should mention durability/recovery
    const walTooltipText = await walTooltip.textContent();
    expect(walTooltipText.toLowerCase()).toContain('durability');
  });

  test('diagram components are keyboard accessible', async ({ page }) => {
    // Additional test: keyboard navigation and focus states
    const memtableNode = page.locator('[data-component="memtable"]');
    const memtableTooltip = page.locator('#tooltip-memtable');

    // Tab to the memtable node (it should have tabindex="0")
    await memtableNode.focus();
    await waitForAnimations(page, 300);

    // Check that the node is focused
    const isFocused = await memtableNode.evaluate((el) => document.activeElement === el);
    expect(isFocused).toBe(true);

    // Tooltip should be visible on focus
    await expect(memtableTooltip).toBeVisible();
  });

  test('architecture section has proper heading', async ({ page }) => {
    // Verify section structure
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    const heading = page.locator('#architecture-title');
    await expect(heading).toContainText('Architecture');
    await expect(heading).toHaveAttribute('id', 'architecture-title');
  });

  test('diagram shows data flow from writes to storage', async ({ page }) => {
    // Verify the complete data flow is represented
    const writeNode = page.locator('[data-component="write"]');
    const walNode = page.locator('[data-component="wal"]');
    const memtableNode = page.locator('[data-component="memtable"]');
    const immNode = page.locator('[data-component="imm"]');
    const level0Node = page.locator('[data-component="level0"]');
    const levelsNode = page.locator('[data-component="levels"]');

    // All nodes should be visible
    await expect(writeNode).toBeVisible();
    await expect(walNode).toBeVisible();
    await expect(memtableNode).toBeVisible();
    await expect(immNode).toBeVisible();
    await expect(level0Node).toBeVisible();
    await expect(levelsNode).toBeVisible();

    // Check arrows indicating data flow
    const arrows = page.locator('.diagram-arrow');
    const arrowCount = await arrows.count();
    expect(arrowCount).toBeGreaterThanOrEqual(5); // At least 5 arrows for the flow
  });

  test('compaction labels are visible in diagram', async ({ page }) => {
    // Check that compaction process labels are in the diagram
    const minorLabel = page.locator('.arrow-label:has-text("Minor Compaction")');
    const majorLabel = page.locator('.arrow-label:has-text("Major Compaction")');

    await expect(minorLabel).toBeVisible();
    await expect(majorLabel).toBeVisible();
  });
});
