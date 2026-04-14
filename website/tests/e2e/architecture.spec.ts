/**
 * Architecture Section E2E Tests
 * Owner: Scenario 4 - Architecture Section with LSM Tree Diagram
 *
 * Tests for:
 * - Architecture diagram visibility
 * - Diagram component presence (WAL, Memtable, Immutable Memtable, SSTables)
 * - Component descriptions
 * - Mermaid diagram rendering
 */
import { test, expect } from '@playwright/test';
import { SELECTORS } from '../fixtures/test-data';

test.describe('Architecture Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for mermaid to render
    await page.waitForSelector('.architecture', { state: 'visible' });
  });

  test('architecture diagram is visible showing data flow from WAL to memtable to SSTables', async ({ page }) => {
    // Navigate to architecture section
    await page.locator(SELECTORS.architecture).scrollIntoViewIfNeeded();

    // Check that the architecture section is visible
    const architectureSection = page.locator(SELECTORS.architecture);
    await expect(architectureSection).toBeVisible();

    // Check that the mermaid diagram container exists and rendered
    const diagramContainer = page.locator('.architecture-diagram');
    await expect(diagramContainer).toBeVisible();

    // Wait for mermaid to render (SVG should be present)
    const mermaidSvg = page.locator('.architecture-diagram svg');
    await expect(mermaidSvg).toBeVisible({ timeout: 10000 });

    // Verify data flow components are visible in the diagram
    // Mermaid renders text as elements within SVG
    const diagramText = await page.locator('.architecture-diagram svg').textContent();
    expect(diagramText).toContain('WAL');
    expect(diagramText).toContain('Memtable');
    expect(diagramText).toContain('SSTable');
  });

  test('diagram includes labeled components: WAL, Memtable, Immutable Memtable, SSTables', async ({ page }) => {
    await page.locator(SELECTORS.architecture).scrollIntoViewIfNeeded();

    // Wait for mermaid diagram to render
    const mermaidSvg = page.locator('.architecture-diagram svg');
    await expect(mermaidSvg).toBeVisible({ timeout: 10000 });

    // Get the SVG text content which contains all labels
    const diagramText = await mermaidSvg.textContent();

    // Verify all required components are present
    expect(diagramText).toContain('WAL');
    expect(diagramText).toContain('Memtable');
    expect(diagramText).toContain('Immutable');
    expect(diagramText).toContain('SSTable');
  });

  test('brief descriptions explain the purpose of each architecture component', async ({ page }) => {
    await page.locator(SELECTORS.architecture).scrollIntoViewIfNeeded();

    // Check component explanation cards exist
    const explanationCards = page.locator('.architecture-components .component-card');
    await expect(explanationCards).toHaveCount(4);

    // Verify WAL description
    const walCard = page.locator('.component-card[data-component="wal"]');
    await expect(walCard).toBeVisible();
    await expect(walCard.locator('.component-title')).toContainText('WAL');
    await expect(walCard.locator('.component-description')).toBeVisible();

    // Verify Memtable description
    const memtableCard = page.locator('.component-card[data-component="memtable"]');
    await expect(memtableCard).toBeVisible();
    await expect(memtableCard.locator('.component-title')).toContainText('Memtable');
    await expect(memtableCard.locator('.component-description')).toBeVisible();

    // Verify Immutable Memtable description
    const immutableCard = page.locator('.component-card[data-component="immutable"]');
    await expect(immutableCard).toBeVisible();
    await expect(immutableCard.locator('.component-title')).toContainText('Immutable');
    await expect(immutableCard.locator('.component-description')).toBeVisible();

    // Verify SSTable description
    const sstableCard = page.locator('.component-card[data-component="sstable"]');
    await expect(sstableCard).toBeVisible();
    await expect(sstableCard.locator('.component-title')).toContainText('SSTable');
    await expect(sstableCard.locator('.component-description')).toBeVisible();
  });

  test('Mermaid diagram renders correctly without errors', async ({ page }) => {
    await page.locator(SELECTORS.architecture).scrollIntoViewIfNeeded();

    // Check that no error message is displayed
    const errorMessage = page.locator('.architecture-diagram .mermaid-error');
    await expect(errorMessage).toHaveCount(0);

    // Check that the mermaid diagram rendered as SVG
    const mermaidSvg = page.locator('.architecture-diagram svg');
    await expect(mermaidSvg).toBeVisible({ timeout: 10000 });

    // Verify the SVG has proper structure (nodes and edges)
    const svgContent = await mermaidSvg.innerHTML();
    expect(svgContent.length).toBeGreaterThan(100); // SVG should have substantial content

    // Check for mermaid-specific classes or structures
    const hasNodes = await page.locator('.architecture-diagram svg .node, .architecture-diagram svg .nodeLabel, .architecture-diagram svg [class*="node"]').count();
    expect(hasNodes).toBeGreaterThan(0);
  });
});
