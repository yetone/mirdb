// @ts-check
const { test, expect } = require('@playwright/test');
const { setupPage } = require('../helpers/test-utils');

/**
 * Architecture Section E2E Tests
 * Owner: Scenario 4 - Architecture Overview Section
 *
 * Tests verify:
 * - Presence of LSM-tree architecture diagram (Mermaid.js or SVG)
 * - Diagram shows memtable component in data flow
 * - Diagram shows SSTable components and levels
 * - WAL explanation text is present
 * - Compaction explanation (minor and major) is present
 * - Interactive hover states on diagram components
 */

test.describe('Architecture Section', () => {
  test.beforeEach(async ({ page }) => {
    await setupPage(page);
  });

  test('architecture diagram is present', async ({ page }) => {
    // Navigate to architecture section
    await page.locator('#architecture').scrollIntoViewIfNeeded();

    // Check for architecture diagram - could be Mermaid.js (.mermaid), SVG, or a custom diagram
    const diagramContainer = page.locator('#architecture .architecture-diagram, #architecture svg, #architecture .mermaid');
    await expect(diagramContainer.first()).toBeVisible();

    // Verify the diagram shows LSM-tree architecture concept
    const architectureSection = page.locator('#architecture');
    const sectionText = await architectureSection.textContent();

    // The diagram or surrounding text should reference LSM-tree
    expect(sectionText?.toLowerCase()).toMatch(/lsm|log-structured|storage/i);
  });

  test('diagram shows memtable component', async ({ page }) => {
    await page.locator('#architecture').scrollIntoViewIfNeeded();

    // Check that memtable is shown in the diagram or described
    const architectureSection = page.locator('#architecture');

    // Look for memtable in diagram elements or text
    const memtableElement = page.locator('#architecture [data-component="memtable"], #architecture .memtable, #architecture text:has-text("Memtable"), #architecture :has-text("memtable")').first();

    // Either the diagram element exists or the text mentions memtable
    const sectionText = await architectureSection.textContent();
    const hasMemtableText = sectionText?.toLowerCase().includes('memtable');
    const hasMemtableElement = await memtableElement.count() > 0;

    expect(hasMemtableText || hasMemtableElement).toBeTruthy();
  });

  test('diagram shows SSTable components and levels', async ({ page }) => {
    await page.locator('#architecture').scrollIntoViewIfNeeded();

    const architectureSection = page.locator('#architecture');
    const sectionText = await architectureSection.textContent();

    // Check for SSTable mention
    const hasSSTables = sectionText?.toLowerCase().includes('sstable') ||
                        sectionText?.toLowerCase().includes('sst') ||
                        sectionText?.toLowerCase().includes('sorted string table');
    expect(hasSSTables).toBeTruthy();

    // Check for levels mention (Level 0, Level 1, etc.)
    const hasLevels = sectionText?.toLowerCase().includes('level') ||
                      sectionText?.match(/l[0-9]/i);
    expect(hasLevels).toBeTruthy();
  });

  test('WAL explanation is present', async ({ page }) => {
    await page.locator('#architecture').scrollIntoViewIfNeeded();

    const architectureSection = page.locator('#architecture');
    const sectionText = await architectureSection.textContent();

    // Check for WAL (Write-Ahead Log) mention and durability explanation
    const hasWAL = sectionText?.toLowerCase().includes('wal') ||
                   sectionText?.toLowerCase().includes('write-ahead log') ||
                   sectionText?.toLowerCase().includes('write ahead log');
    expect(hasWAL).toBeTruthy();

    // Check for durability explanation
    const hasDurability = sectionText?.toLowerCase().includes('durability') ||
                          sectionText?.toLowerCase().includes('persist') ||
                          sectionText?.toLowerCase().includes('crash') ||
                          sectionText?.toLowerCase().includes('recovery');
    expect(hasDurability).toBeTruthy();
  });

  test('compaction explanation is present', async ({ page }) => {
    await page.locator('#architecture').scrollIntoViewIfNeeded();

    const architectureSection = page.locator('#architecture');
    const sectionText = await architectureSection.textContent();

    // Check for compaction mention
    const hasCompaction = sectionText?.toLowerCase().includes('compaction');
    expect(hasCompaction).toBeTruthy();

    // Check for minor and major compaction specifically
    const hasMinorCompaction = sectionText?.toLowerCase().includes('minor');
    const hasMajorCompaction = sectionText?.toLowerCase().includes('major');

    // Both minor and major compaction should be explained
    expect(hasMinorCompaction).toBeTruthy();
    expect(hasMajorCompaction).toBeTruthy();
  });

  test('diagram has interactive hover states', async ({ page }) => {
    await page.locator('#architecture').scrollIntoViewIfNeeded();

    // Find interactive diagram components
    const diagramComponents = page.locator('#architecture [data-component], #architecture .diagram-node, #architecture .architecture-component');

    // Ensure there are hoverable components
    const componentCount = await diagramComponents.count();

    if (componentCount > 0) {
      // Hover over the first component and check for tooltip or hover state
      const firstComponent = diagramComponents.first();
      await firstComponent.hover();

      // Check for tooltip, title attribute, or CSS hover effect
      const hasTooltip = await page.locator('#architecture .tooltip:visible, #architecture [role="tooltip"]:visible').count() > 0;
      const hasTitleAttr = await firstComponent.getAttribute('title');
      const hasDataTooltip = await firstComponent.getAttribute('data-tooltip');

      // At least one hover indicator should be present
      expect(hasTooltip || hasTitleAttr || hasDataTooltip).toBeTruthy();
    } else {
      // Alternative: Check for SVG elements with hover capability
      const svgElements = page.locator('#architecture svg [data-component], #architecture svg .hoverable, #architecture svg g[data-tooltip]');
      const svgCount = await svgElements.count();

      if (svgCount > 0) {
        const firstSvgElement = svgElements.first();
        await firstSvgElement.hover();

        const hasTitle = await firstSvgElement.locator('title').count() > 0;
        const hasDataTooltip = await firstSvgElement.getAttribute('data-tooltip');
        expect(hasTitle || hasDataTooltip).toBeTruthy();
      } else {
        // Check for CSS-based hover interactions with description cards
        const hoverCards = page.locator('#architecture .component-card, #architecture .hover-info');
        const cardCount = await hoverCards.count();
        expect(cardCount).toBeGreaterThan(0);
      }
    }
  });
});
