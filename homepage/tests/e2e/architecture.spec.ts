/**
 * Architecture Diagram E2E Tests
 * Owner: Scenario 5 - Architecture Diagram
 *
 * Tests for the LSM-tree architecture diagram section:
 * - Diagram presence and visibility
 * - Component labels (memtable, SSTable, compaction)
 * - Accessibility (alt text, ARIA labels)
 */

import { test, expect } from '@playwright/test';

test.describe('Architecture Diagram Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Architecture section and diagram are present', async ({ page }) => {
    // Check architecture section exists
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Check section title
    const sectionTitle = architectureSection.locator('.section-title');
    await expect(sectionTitle).toContainText('LSM-Tree Architecture');

    // Check diagram container exists
    const diagramContainer = architectureSection.locator('.architecture-diagram-container');
    await expect(diagramContainer).toBeVisible();

    // Check the diagram image/SVG is present
    const diagram = architectureSection.locator('.architecture-diagram img');
    await expect(diagram).toBeVisible();

    // Verify the diagram source points to the architecture SVG
    const imgSrc = await diagram.getAttribute('src');
    expect(imgSrc).toContain('architecture.svg');
  });

  test('TC2: Memtable component is referenced in the diagram and content', async ({ page }) => {
    const architectureSection = page.locator('#architecture');

    // Check for memtable component description
    const memtableComponent = architectureSection.locator('#memtable-desc');
    await expect(memtableComponent).toBeVisible();

    // Check memtable heading
    const memtableHeading = memtableComponent.locator('h3');
    await expect(memtableHeading).toContainText('Memtable');

    // Check memtable description mentions skip list
    const memtableDescription = memtableComponent.locator('p');
    await expect(memtableDescription).toContainText('skip list');

    // Check the diagram alt text mentions memtable
    const diagramImg = architectureSection.locator('.architecture-diagram img');
    const altText = await diagramImg.getAttribute('alt');
    expect(altText?.toLowerCase()).toContain('memtable');
  });

  test('TC3: SSTable component is referenced in the diagram and content', async ({ page }) => {
    const architectureSection = page.locator('#architecture');

    // Check for SSTable component description
    const sstableComponent = architectureSection.locator('#sstable-desc');
    await expect(sstableComponent).toBeVisible();

    // Check SSTable heading
    const sstableHeading = sstableComponent.locator('h3');
    await expect(sstableHeading).toContainText('SSTable');

    // Check SSTable description mentions sorted and levels
    const sstableDescription = sstableComponent.locator('p');
    await expect(sstableDescription).toContainText('Sorted String Tables');
    await expect(sstableDescription).toContainText('levels');

    // Check the diagram alt text mentions SSTable
    const diagramImg = architectureSection.locator('.architecture-diagram img');
    const altText = await diagramImg.getAttribute('alt');
    expect(altText?.toLowerCase()).toContain('sstable');
  });

  test('TC4: Compaction process is visualized and explained', async ({ page }) => {
    const architectureSection = page.locator('#architecture');

    // Check for compaction component description
    const compactionComponent = architectureSection.locator('#compaction-desc');
    await expect(compactionComponent).toBeVisible();

    // Check compaction heading
    const compactionHeading = compactionComponent.locator('h3');
    await expect(compactionHeading).toContainText('Compaction');

    // Check compaction description explains the process
    const compactionDescription = compactionComponent.locator('p');
    await expect(compactionDescription).toContainText('merges');
    await expect(compactionDescription).toContainText('Minor compaction');
    await expect(compactionDescription).toContainText('major compaction');

    // Check the diagram alt text mentions compaction
    const diagramImg = architectureSection.locator('.architecture-diagram img');
    const altText = await diagramImg.getAttribute('alt');
    expect(altText?.toLowerCase()).toContain('compaction');
  });

  test('TC5: Diagram has proper accessibility attributes', async ({ page }) => {
    const architectureSection = page.locator('#architecture');

    // Check section has aria-labelledby pointing to the title
    const ariaLabelledBy = await architectureSection.getAttribute('aria-labelledby');
    expect(ariaLabelledBy).toBe('architecture-title');

    // Check the title element exists with correct id
    const titleElement = architectureSection.locator('#architecture-title');
    await expect(titleElement).toBeVisible();

    // Check the diagram figure has proper role
    const diagramFigure = architectureSection.locator('.architecture-diagram');
    const figureRole = await diagramFigure.getAttribute('role');
    expect(figureRole).toBe('figure');

    // Check the diagram figure has aria-label
    const figureAriaLabel = await diagramFigure.getAttribute('aria-label');
    expect(figureAriaLabel).toBeTruthy();
    expect(figureAriaLabel?.toLowerCase()).toContain('architecture');

    // Check the diagram image has alt text
    const diagramImg = architectureSection.locator('.architecture-diagram img');
    const altText = await diagramImg.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText!.length).toBeGreaterThan(50); // Alt text should be descriptive

    // Check alt text includes key components
    const altLower = altText?.toLowerCase() || '';
    expect(altLower).toContain('memtable');
    expect(altLower).toContain('sstable');
    expect(altLower).toContain('compaction');
  });

  test('Diagram is responsive and displays correctly', async ({ page }) => {
    const architectureSection = page.locator('#architecture');
    const diagramContainer = architectureSection.locator('.architecture-diagram-container');
    const diagramImg = architectureSection.locator('.architecture-diagram img');

    // Check diagram container is visible
    await expect(diagramContainer).toBeVisible();

    // Check image has width and height attributes
    const width = await diagramImg.getAttribute('width');
    const height = await diagramImg.getAttribute('height');
    expect(width).toBe('800');
    expect(height).toBe('500');

    // Check image loads properly (has natural dimensions)
    const boundingBox = await diagramImg.boundingBox();
    expect(boundingBox).not.toBeNull();
    expect(boundingBox!.width).toBeGreaterThan(0);
    expect(boundingBox!.height).toBeGreaterThan(0);
  });

  test('Component cards display correctly', async ({ page }) => {
    const architectureSection = page.locator('#architecture');
    const componentCards = architectureSection.locator('.architecture-component');

    // Should have 3 component cards (memtable, sstable, compaction)
    await expect(componentCards).toHaveCount(3);

    // Check each card has an icon, heading, and description
    for (let i = 0; i < 3; i++) {
      const card = componentCards.nth(i);
      await expect(card.locator('.component-icon')).toBeVisible();
      await expect(card.locator('h3')).toBeVisible();
      await expect(card.locator('p')).toBeVisible();
    }
  });

  test('Architecture section is navigable via anchor link', async ({ page }) => {
    // Navigate to the architecture section using anchor
    await page.goto('/#architecture');

    // Check that the architecture section is in view
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeInViewport();
  });
});
