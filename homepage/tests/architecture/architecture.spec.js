/**
 * Architecture Section E2E Tests
 * Owner: Scenario 4 - Architecture Section
 *
 * Test coverage:
 * - Architecture section heading and SVG diagram visible
 * - SVG shows memtable, SSTables, and WAL with labels
 * - SVG has title and desc elements for accessibility
 * - Write path explanation text
 * - Read path explanation text
 */

const { test, expect } = require('@playwright/test');

test.describe('Architecture Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('displays section heading and SVG diagram', async ({ page }) => {
    // Test case 1: Load architecture section and verify heading and SVG are visible
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Check section heading
    const heading = page.locator('#architecture-heading');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Architecture');

    // Check SVG diagram is visible
    const svgDiagram = page.locator('[data-testid="architecture-svg"]');
    await expect(svgDiagram).toBeVisible();
  });

  test('SVG diagram shows memtable, SSTables, and WAL components with labels', async ({ page }) => {
    // Test case 2: Check SVG diagram components
    const svgDiagram = page.locator('[data-testid="architecture-svg"]');
    await expect(svgDiagram).toBeVisible();

    // Check WAL component
    const walComponent = svgDiagram.locator('[data-component="wal"]');
    await expect(walComponent).toBeVisible();
    const walLabel = svgDiagram.locator('[data-label="wal"]');
    await expect(walLabel).toHaveText('WAL');

    // Check Memtable component
    const memtableComponent = svgDiagram.locator('[data-component="memtable"]');
    await expect(memtableComponent).toBeVisible();
    const memtableLabel = svgDiagram.locator('[data-label="memtable"]');
    await expect(memtableLabel).toHaveText('Memtable');

    // Check SSTable components (L0, L1, L2)
    const sstableL0 = svgDiagram.locator('[data-component="sstable-l0"]');
    await expect(sstableL0).toBeVisible();

    const sstableL1 = svgDiagram.locator('[data-component="sstable-l1"]');
    await expect(sstableL1).toBeVisible();

    const sstableL2 = svgDiagram.locator('[data-component="sstable-l2"]');
    await expect(sstableL2).toBeVisible();

    // Check SSTable labels
    const sstableLabels = svgDiagram.locator('[data-label="sstable"]');
    await expect(sstableLabels).toHaveCount(3);
  });

  test('SVG has title and desc elements for screen readers', async ({ page }) => {
    // Test case 3: Check SVG accessibility
    const svgDiagram = page.locator('[data-testid="architecture-svg"]');
    await expect(svgDiagram).toBeVisible();

    // Check role="img" attribute
    await expect(svgDiagram).toHaveAttribute('role', 'img');

    // Check aria-labelledby points to title and desc
    await expect(svgDiagram).toHaveAttribute('aria-labelledby', 'arch-title arch-desc');

    // Check title element exists and has content
    const title = svgDiagram.locator('title#arch-title');
    await expect(title).toBeAttached();
    const titleText = await title.textContent();
    expect(titleText).toContain('MirDB');
    expect(titleText).toContain('LSM-tree');

    // Check desc element exists and has content
    const desc = svgDiagram.locator('desc#arch-desc');
    await expect(desc).toBeAttached();
    const descText = await desc.textContent();
    expect(descText).toContain('Write-Ahead Log');
    expect(descText).toContain('Memtable');
    expect(descText).toContain('SSTable');
  });

  test('write path explanation describes WAL, memtable, and SSTable flow', async ({ page }) => {
    // Test case 4: Read write path explanation
    const writePath = page.locator('[data-testid="write-path"]');
    await expect(writePath).toBeVisible();

    // Check heading
    const heading = writePath.locator('.architecture__path-heading');
    await expect(heading).toContainText('Write Path');

    // Check description explains WAL -> memtable -> SSTables flow
    const description = writePath.locator('.architecture__path-description');
    const descText = await description.textContent();

    // Verify it mentions WAL first
    expect(descText).toContain('Write-Ahead Log');
    expect(descText).toContain('WAL');

    // Verify it mentions memtable
    expect(descText).toContain('Memtable');

    // Verify it mentions flush to SSTables
    expect(descText).toContain('SSTable');
    expect(descText).toContain('flushed');
  });

  test('read path explanation describes checking memtable first, then SSTables', async ({ page }) => {
    // Test case 5: Read read path explanation
    const readPath = page.locator('[data-testid="read-path"]');
    await expect(readPath).toBeVisible();

    // Check heading
    const heading = readPath.locator('.architecture__path-heading');
    await expect(heading).toContainText('Read Path');

    // Check description explains reads check memtable first, then SSTables
    const description = readPath.locator('.architecture__path-description');
    const descText = await description.textContent();

    // Verify it mentions checking memtable first
    expect(descText).toContain('Memtable');
    expect(descText.toLowerCase()).toMatch(/first|check/);

    // Verify it mentions SSTables for further lookup
    expect(descText).toContain('SSTable');
  });
});
