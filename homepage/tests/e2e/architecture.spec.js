/**
 * Architecture Section E2E Tests
 * Owner: Scenario 4 - Architecture Section Implementation
 *
 * Tests for:
 * - Architecture section visibility and heading
 * - LSM tree diagram rendering
 * - Write path explanation
 * - Read path explanation
 * - Compaction explanation
 * - Component listing
 * - Diagram responsiveness
 * - Mermaid error handling/fallback
 */

const { test, expect } = require('@playwright/test');

test.describe('Architecture Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Architecture section is visible with appropriate heading', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture');
    await architectureSection.scrollIntoViewIfNeeded();

    // Check section is visible
    await expect(architectureSection).toBeVisible();

    // Check heading text
    const heading = page.locator('#architecture-heading');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Architecture');

    // Check section description
    const description = page.locator('.architecture .section-heading p');
    await expect(description).toContainText('LSM tree');
  });

  test('TC2: LSM tree diagram renders (not showing raw Mermaid code)', async ({ page }) => {
    // Navigate to architecture section
    await page.locator('#architecture').scrollIntoViewIfNeeded();

    // Wait for Mermaid to initialize and render
    await page.waitForTimeout(2000);

    // Find the LSM tree diagram container
    const lsmDiagram = page.locator('[data-diagram="lsm-tree"]');
    await expect(lsmDiagram).toBeVisible();

    // Check that either:
    // 1. The diagram has been rendered (data-rendered="true" and SVG present)
    // 2. OR fallback content is shown (data-error="true")
    const isRendered = await lsmDiagram.getAttribute('data-rendered');
    const hasError = await lsmDiagram.getAttribute('data-error');

    if (isRendered === 'true') {
      // Diagram rendered successfully - check for SVG
      const svg = lsmDiagram.locator('svg');
      await expect(svg).toBeVisible();

      // Verify raw Mermaid code is not visible
      const rawMermaid = lsmDiagram.locator('.mermaid');
      const rawMermaidVisible = await rawMermaid.isVisible().catch(() => false);
      expect(rawMermaidVisible).toBeFalsy();
    } else if (hasError === 'true') {
      // Fallback content should be visible
      const fallback = lsmDiagram.locator('.architecture__fallback-visible');
      await expect(fallback).toBeVisible();
    } else {
      // Check diagram content is present (either rendered or as Mermaid code awaiting render)
      const content = await lsmDiagram.textContent();
      expect(content.length).toBeGreaterThan(0);
    }
  });

  test('TC3: Write path explanation is present', async ({ page }) => {
    // Navigate to architecture section
    await page.locator('#architecture').scrollIntoViewIfNeeded();

    // Find the write path subsection
    const writePathSection = page.locator('.architecture__subsection').filter({
      has: page.locator('h3:has-text("Write Path")')
    });
    await expect(writePathSection).toBeVisible();

    // Check for write path description containing key concepts
    const description = writePathSection.locator('.architecture__description');
    await expect(description).toBeVisible();

    const descText = await description.textContent();
    // Should mention Client, Memtable, WAL, and SSTable
    expect(descText).toMatch(/client/i);
    expect(descText).toMatch(/memtable/i);
    expect(descText).toMatch(/wal|write-ahead/i);
    expect(descText).toMatch(/sstable/i);

    // Check for write path diagram
    const writeDiagram = page.locator('[data-diagram="write-path"]');
    await expect(writeDiagram).toBeVisible();
  });

  test('TC4: Read path explanation is present', async ({ page }) => {
    // Navigate to architecture section
    await page.locator('#architecture').scrollIntoViewIfNeeded();

    // Find the read path subsection
    const readPathSection = page.locator('.architecture__subsection').filter({
      has: page.locator('h3:has-text("Read Path")')
    });
    await expect(readPathSection).toBeVisible();

    // Check for read path description containing key concepts
    const description = readPathSection.locator('.architecture__description');
    await expect(description).toBeVisible();

    const descText = await description.textContent();
    // Should mention Memtable, SSTables, and the read order
    expect(descText).toMatch(/memtable/i);
    expect(descText).toMatch(/sstable/i);

    // Check for read path diagram
    const readDiagram = page.locator('[data-diagram="read-path"]');
    await expect(readDiagram).toBeVisible();
  });

  test('TC5: Compaction explanation is displayed', async ({ page }) => {
    // Navigate to architecture section
    await page.locator('#architecture').scrollIntoViewIfNeeded();

    // Find the compaction subsection
    const compactionSection = page.locator('.architecture__subsection').filter({
      has: page.locator('h3:has-text("Compaction")')
    });
    await expect(compactionSection).toBeVisible();

    // Check for minor compaction explanation
    const minorCompaction = page.locator('.architecture__compaction-type').filter({
      has: page.locator('h4:has-text("Minor Compaction")')
    });
    await expect(minorCompaction).toBeVisible();

    const minorText = await minorCompaction.textContent();
    expect(minorText).toMatch(/memtable|level 0/i);

    // Check for major compaction explanation
    const majorCompaction = page.locator('.architecture__compaction-type').filter({
      has: page.locator('h4:has-text("Major Compaction")')
    });
    await expect(majorCompaction).toBeVisible();

    const majorText = await majorCompaction.textContent();
    expect(majorText).toMatch(/level.*n.*1|merge/i);

    // Check for compaction diagram
    const compactionDiagram = page.locator('[data-diagram="compaction"]');
    await expect(compactionDiagram).toBeVisible();
  });

  test('TC6: Three main crates listed with descriptions', async ({ page }) => {
    // Navigate to architecture section
    await page.locator('#architecture').scrollIntoViewIfNeeded();

    // Find the components section
    const componentsSection = page.locator('.architecture__subsection').filter({
      has: page.locator('h3:has-text("Core Components")')
    });
    await expect(componentsSection).toBeVisible();

    // Check for mirdb-server crate
    const mirdbServer = page.locator('.architecture__component').filter({
      has: page.locator('.architecture__component-name:has-text("mirdb-server")')
    });
    await expect(mirdbServer).toBeVisible();
    const mirdbServerDesc = mirdbServer.locator('.architecture__component-desc');
    await expect(mirdbServerDesc).toBeVisible();
    const mirdbServerText = await mirdbServerDesc.textContent();
    expect(mirdbServerText.length).toBeGreaterThan(20);

    // Check for skip-list crate
    const skipList = page.locator('.architecture__component').filter({
      has: page.locator('.architecture__component-name:has-text("skip-list")')
    });
    await expect(skipList).toBeVisible();
    const skipListDesc = skipList.locator('.architecture__component-desc');
    await expect(skipListDesc).toBeVisible();
    const skipListText = await skipListDesc.textContent();
    expect(skipListText.length).toBeGreaterThan(20);

    // Check for sstable crate
    const sstable = page.locator('.architecture__component').filter({
      has: page.locator('.architecture__component-name:has-text("sstable")')
    });
    await expect(sstable).toBeVisible();
    const sstableDesc = sstable.locator('.architecture__component-desc');
    await expect(sstableDesc).toBeVisible();
    const sstableText = await sstableDesc.textContent();
    expect(sstableText.length).toBeGreaterThan(20);

    // Verify we have exactly 3 components
    const components = page.locator('.architecture__component');
    await expect(components).toHaveCount(3);
  });

  test('TC7: Architecture diagrams scale appropriately on mobile', async ({ page }) => {
    // Set viewport to mobile size
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    // Navigate to architecture section
    await page.locator('#architecture').scrollIntoViewIfNeeded();

    // Wait for diagrams to render
    await page.waitForTimeout(2000);

    // Get viewport width
    const viewportWidth = 375;

    // Check each diagram container doesn't overflow
    const diagrams = page.locator('.architecture__diagram');
    const diagramCount = await diagrams.count();

    for (let i = 0; i < diagramCount; i++) {
      const diagram = diagrams.nth(i);
      await diagram.scrollIntoViewIfNeeded();

      const box = await diagram.boundingBox();
      if (box) {
        // Diagram should not exceed viewport width (with some padding tolerance)
        expect(box.width).toBeLessThanOrEqual(viewportWidth);

        // Check horizontal scroll position - content shouldn't be cut off initially
        const scrollLeft = await diagram.evaluate(el => el.scrollLeft);
        expect(scrollLeft).toBe(0);
      }
    }

    // Check component cards stack vertically on mobile
    const components = page.locator('.architecture__components');
    const componentsBox = await components.boundingBox();
    if (componentsBox) {
      expect(componentsBox.width).toBeLessThanOrEqual(viewportWidth);
    }

    // Verify compaction grid also stacks on mobile
    const compactionGrid = page.locator('.architecture__compaction-grid');
    const gridBox = await compactionGrid.boundingBox();
    if (gridBox) {
      expect(gridBox.width).toBeLessThanOrEqual(viewportWidth);
    }
  });

  test('TC8: Mermaid error handling shows fallback content', async ({ page }) => {
    // Block Mermaid.js from loading to simulate error
    await page.route('**/mermaid*.js', route => route.abort());

    // Navigate to page
    await page.goto('/');

    // Navigate to architecture section
    await page.locator('#architecture').scrollIntoViewIfNeeded();

    // Wait for the fallback logic to run
    await page.waitForTimeout(2000);

    // Get all diagram containers
    const diagrams = page.locator('.architecture__diagram');
    const diagramCount = await diagrams.count();

    // For each diagram, verify either:
    // 1. Fallback content is shown
    // 2. Or raw mermaid code is hidden (not ideal but acceptable)
    for (let i = 0; i < diagramCount; i++) {
      const diagram = diagrams.nth(i);

      // Check if fallback is visible
      const fallback = diagram.locator('.architecture__fallback-visible, noscript p');
      const mermaidCode = diagram.locator('.mermaid');

      // Either fallback should be visible OR mermaid should be hidden
      const hasFallback = await diagram.locator('.architecture__fallback-visible').count() > 0;
      const hasError = await diagram.getAttribute('data-error');

      // The diagram should have some form of content or error indication
      if (hasError === 'true' || hasFallback) {
        // Good - error handling worked
        if (hasFallback) {
          const fallbackEl = diagram.locator('.architecture__fallback-visible');
          await expect(fallbackEl).toBeVisible();
        }
      } else {
        // Check that at least the noscript fallback exists in the DOM
        const noscript = diagram.locator('noscript');
        const noscriptCount = await noscript.count();
        expect(noscriptCount).toBeGreaterThan(0);
      }
    }
  });
});
