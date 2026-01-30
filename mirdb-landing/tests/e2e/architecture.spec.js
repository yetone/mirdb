/**
 * Architecture Section E2E Tests
 * Owner: Scenario 5 - Technical Architecture Section
 *
 * Tests:
 * - Diagram visibility and accessibility
 * - Component explanations
 * - Responsive diagram behavior
 */

import { test, expect } from '@playwright/test';

test.describe('Architecture Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Navigate to Architecture section - displays heading, diagram, and component breakdown
  test('TC1: Section displays heading, architecture diagram, and component breakdown', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture');
    await architectureSection.scrollIntoViewIfNeeded();

    // Verify section is visible
    await expect(architectureSection).toBeVisible();

    // Verify heading exists
    const heading = page.locator('#architecture-heading');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Technical Architecture');

    // Verify diagram container exists and is visible
    const diagramContainer = page.locator('.architecture__diagram-container');
    await expect(diagramContainer).toBeVisible();

    // Verify diagram image exists
    const diagramImg = page.locator('.architecture__diagram-img');
    await expect(diagramImg).toBeVisible();

    // Verify component breakdown exists - at least 5 component cards
    const componentCards = page.locator('.component-card');
    await expect(componentCards).toHaveCount(5);

    // Verify data flow section exists
    const dataflowSection = page.locator('.architecture__dataflow');
    await expect(dataflowSection).toBeVisible();
  });

  // Test Case 2: Verify architecture diagram content shows all required components
  test('TC2: Diagram shows Network Layer, Memtable, Immutable Memtable, SSTables, and Compaction Engine', async ({ page }) => {
    const diagramImg = page.locator('.architecture__diagram-img');
    await expect(diagramImg).toBeVisible();

    // Check the SVG source path
    const src = await diagramImg.getAttribute('src');
    expect(src).toContain('architecture-diagram.svg');

    // Verify alt text contains all required components
    const altText = await diagramImg.getAttribute('alt');
    expect(altText).toContain('Network Layer');
    expect(altText).toContain('Tokio');
    expect(altText).toContain('Memtable');
    expect(altText).toContain('Skip List');
    expect(altText).toContain('Immutable Memtable');
    expect(altText).toContain('SSTable');
    expect(altText).toContain('L0');
    expect(altText).toContain('L6');
    expect(altText).toContain('Compaction Engine');
  });

  // Test Case 3: Check diagram accessibility - has descriptive alt text or aria-label
  test('TC3: Diagram has descriptive alt text explaining the architecture', async ({ page }) => {
    const diagramImg = page.locator('.architecture__diagram-img');

    // Check for alt attribute
    const altText = await diagramImg.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText.length).toBeGreaterThan(50); // Should be descriptive

    // Verify figcaption exists (for screen readers)
    const figcaption = page.locator('.architecture__diagram figcaption');
    await expect(figcaption).toBeAttached();

    // Check aria-labelledby on section
    const section = page.locator('#architecture');
    const ariaLabelledby = await section.getAttribute('aria-labelledby');
    expect(ariaLabelledby).toBe('architecture-heading');
  });

  // Test Case 4: Verify memtable component explanation
  test('TC4: Memtable explanation includes skip list structure, 4MB size, and write buffering', async ({ page }) => {
    // Find the Memtable component card
    const memtableCard = page.locator('.component-card', { has: page.locator('#component-memtable') });
    await expect(memtableCard).toBeVisible();

    // Get the description text
    const description = memtableCard.locator('.component-card__description');
    const descriptionText = await description.textContent();

    // Verify it contains required information
    expect(descriptionText).toContain('skip list');
    expect(descriptionText).toContain('4MB');
    expect(descriptionText).toMatch(/write|buffer/i);
  });

  // Test Case 5: Verify SSTable component explanation
  test('TC5: SSTable explanation includes sorted string tables, 100MB size, block-based structure, and multi-level organization', async ({ page }) => {
    // Find the SSTable component card
    const sstableCard = page.locator('.component-card', { has: page.locator('#component-sstable') });
    await expect(sstableCard).toBeVisible();

    // Get the description text
    const description = sstableCard.locator('.component-card__description');
    const descriptionText = await description.textContent();

    // Verify it contains required information
    expect(descriptionText).toMatch(/sorted.*string.*table/i);
    expect(descriptionText).toContain('100MB');
    expect(descriptionText).toMatch(/block/i);
    expect(descriptionText).toMatch(/level|L0|L6/i);
  });

  // Test Case 6: Verify compaction explanation
  test('TC6: Compaction explains minor compaction (memtable to SSTable) and major compaction (level merging)', async ({ page }) => {
    // Find the Compaction component card
    const compactionCard = page.locator('.component-card', { has: page.locator('#component-compaction') });
    await expect(compactionCard).toBeVisible();

    // Get the description text
    const description = compactionCard.locator('.component-card__description');
    const descriptionText = await description.textContent();

    // Verify it contains required information about minor compaction
    expect(descriptionText).toMatch(/minor.*compaction/i);
    expect(descriptionText).toMatch(/memtable|Level 0|SSTable/i);

    // Verify it contains required information about major compaction
    expect(descriptionText).toMatch(/major.*compaction/i);
    expect(descriptionText).toMatch(/merge|level/i);
  });

  // Test Case 7: Mobile responsive behavior
  test('TC7: On mobile, diagram scrolls horizontally and component cards stack vertically', async ({ page }) => {
    // Set viewport to mobile size
    await page.setViewportSize({ width: 375, height: 667 });

    // Navigate to architecture section
    const architectureSection = page.locator('#architecture');
    await architectureSection.scrollIntoViewIfNeeded();

    // Check diagram container allows horizontal scrolling
    const diagramContainer = page.locator('.architecture__diagram-container');
    await expect(diagramContainer).toBeVisible();

    // Verify overflow-x is set to auto or scroll
    const overflowX = await diagramContainer.evaluate(el => {
      return window.getComputedStyle(el).overflowX;
    });
    expect(['auto', 'scroll']).toContain(overflowX);

    // Check component cards are stacking vertically (grid becomes single column)
    const componentsGrid = page.locator('.architecture__components');
    const gridColumns = await componentsGrid.evaluate(el => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });
    // On mobile, should be a single column (fr value or single pixel width)
    expect(gridColumns).not.toContain(' '); // Should be single column, not multiple

    // Verify component cards are visible and stacked
    const componentCards = page.locator('.component-card');
    const count = await componentCards.count();
    expect(count).toBe(5);

    // Check that cards are stacked (compare y positions of first two cards)
    const firstCard = componentCards.first();
    const secondCard = componentCards.nth(1);

    const firstBox = await firstCard.boundingBox();
    const secondBox = await secondCard.boundingBox();

    // Second card should be below first card (stacked vertically)
    expect(secondBox.y).toBeGreaterThan(firstBox.y + firstBox.height - 10);
  });

  // Additional accessibility tests
  test('All component cards have accessible headings', async ({ page }) => {
    const componentCards = page.locator('.component-card');
    const count = await componentCards.count();

    for (let i = 0; i < count; i++) {
      const card = componentCards.nth(i);
      const heading = card.locator('[id^="component-"]');
      await expect(heading).toBeAttached();

      // Check aria-labelledby is set
      const ariaLabelledby = await card.getAttribute('aria-labelledby');
      expect(ariaLabelledby).toBeTruthy();
    }
  });

  // Test data flow visualization
  test('Data flow paths are visible and contain correct steps', async ({ page }) => {
    // Check write path
    const writePath = page.locator('.dataflow-path--write');
    await expect(writePath).toBeVisible();

    const writeSteps = writePath.locator('.dataflow-step');
    await expect(writeSteps).toHaveCount(5);

    // Check read path
    const readPath = page.locator('.dataflow-path--read');
    await expect(readPath).toBeVisible();

    const readSteps = readPath.locator('.dataflow-step');
    await expect(readSteps).toHaveCount(5);

    // Verify write path mentions key concepts
    const writePathText = await writePath.textContent();
    expect(writePathText).toMatch(/SET|write/i);
    expect(writePathText).toMatch(/WAL|Write-Ahead Log/i);
    expect(writePathText).toMatch(/Memtable/i);
    expect(writePathText).toMatch(/SSTable|Level 0/i);

    // Verify read path mentions key concepts
    const readPathText = await readPath.textContent();
    expect(readPathText).toMatch(/GET|read/i);
    expect(readPathText).toMatch(/Memtable/i);
    expect(readPathText).toMatch(/SSTable|L0|L1/i);
  });
});
