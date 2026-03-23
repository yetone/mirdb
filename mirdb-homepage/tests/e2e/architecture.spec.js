/**
 * Architecture Section Tests
 * Owner: Scenario 4 - Architecture Diagram Section
 *
 * Test cases:
 * - Architecture diagram present (SVG or image)
 * - WAL component with explanation
 * - Memtable component with explanation
 * - SSTables component with explanation
 * - Data flow arrows visible
 * - Alt text for accessibility
 */
const { test, expect } = require('@playwright/test');

test.describe('Architecture Diagram Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('http://localhost:3000');
  });

  test('TC1: Architecture section contains a visual diagram (SVG, image, or CSS-based)', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Check for the diagram container with role="img"
    const diagram = architectureSection.locator('.architecture__diagram');
    await expect(diagram).toBeVisible();

    // Verify it has role="img" for semantic meaning
    const role = await diagram.getAttribute('role');
    expect(role).toBe('img');

    // Verify diagram contains visual components (SVGs for arrows or component boxes)
    const diagramContainer = architectureSection.locator('.architecture__diagram-container');
    await expect(diagramContainer).toBeVisible();

    // Check for presence of visual elements (arrows with SVG)
    const arrowSvgs = diagramContainer.locator('.architecture__arrow svg');
    const arrowCount = await arrowSvgs.count();
    expect(arrowCount).toBeGreaterThanOrEqual(1);
  });

  test('TC2: Diagram includes Write-Ahead Log (WAL) with brief explanation of durability role', async ({ page }) => {
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Check for WAL component in the diagram
    const walComponent = architectureSection.locator('[data-component="wal"]');
    await expect(walComponent).toBeVisible();

    // Verify WAL title is present
    const walTitle = walComponent.locator('.architecture__component-title');
    const walTitleText = await walTitle.textContent();
    expect(walTitleText.toLowerCase()).toContain('write-ahead log');

    // Check for WAL explanation
    const walExplanation = architectureSection.locator('#wal-explanation');
    await expect(walExplanation).toBeVisible();

    // Verify explanation mentions durability
    const explanationText = await walExplanation.textContent();
    expect(explanationText.toLowerCase()).toContain('durability');
  });

  test('TC3: Diagram includes Memtable with explanation of in-memory storage and skip list structure', async ({ page }) => {
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Check for Memtable component in the diagram
    const memtableComponent = architectureSection.locator('[data-component="memtable"]');
    await expect(memtableComponent).toBeVisible();

    // Verify Memtable title is present
    const memtableTitle = memtableComponent.locator('.architecture__component-title');
    const memtableTitleText = await memtableTitle.textContent();
    expect(memtableTitleText.toLowerCase()).toContain('memtable');

    // Check for Memtable explanation
    const memtableExplanation = architectureSection.locator('#memtable-explanation');
    await expect(memtableExplanation).toBeVisible();

    // Verify explanation mentions in-memory storage and skip list
    const explanationText = await memtableExplanation.textContent();
    expect(explanationText.toLowerCase()).toContain('in-memory');
    expect(explanationText.toLowerCase()).toContain('skip list');
  });

  test('TC4: Diagram includes SSTables with explanation of persistent storage and levels', async ({ page }) => {
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Check for SSTables component in the diagram
    const sstablesComponent = architectureSection.locator('[data-component="sstables"]');
    await expect(sstablesComponent).toBeVisible();

    // Verify SSTables title is present
    const sstablesTitle = sstablesComponent.locator('.architecture__component-title');
    const sstablesTitleText = await sstablesTitle.textContent();
    expect(sstablesTitleText.toLowerCase()).toContain('sstable');

    // Check for level indicators
    const levels = sstablesComponent.locator('.architecture__level');
    const levelCount = await levels.count();
    expect(levelCount).toBeGreaterThanOrEqual(2);

    // Check for SSTables explanation
    const sstablesExplanation = architectureSection.locator('#sstables-explanation');
    await expect(sstablesExplanation).toBeVisible();

    // Verify explanation mentions persistent storage and levels
    const explanationText = await sstablesExplanation.textContent();
    expect(explanationText.toLowerCase()).toContain('persistent');
    expect(explanationText.toLowerCase()).toContain('level');
  });

  test('TC5: Diagram shows directional flow: Write -> WAL -> Memtable -> SSTables', async ({ page }) => {
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    const diagramContainer = architectureSection.locator('.architecture__diagram-container');
    await expect(diagramContainer).toBeVisible();

    // Verify Write component exists at the start
    const writeComponent = diagramContainer.locator('.architecture__component--write');
    await expect(writeComponent).toBeVisible();
    await expect(writeComponent).toContainText('Write');

    // Verify arrows exist to show flow
    const arrows = diagramContainer.locator('.architecture__arrow');
    const arrowCount = await arrows.count();
    expect(arrowCount).toBeGreaterThanOrEqual(3); // Write->WAL, WAL->Memtable, Memtable->SSTables

    // Verify SVG arrows have path elements indicating direction
    const arrowPaths = diagramContainer.locator('.architecture__arrow svg path');
    const pathCount = await arrowPaths.count();
    expect(pathCount).toBeGreaterThanOrEqual(3);

    // Verify the order of components (Write, WAL, Memtable, SSTables)
    const components = await diagramContainer.locator('.architecture__component').all();
    expect(components.length).toBeGreaterThanOrEqual(4);

    // First component should be Write
    const firstComponentText = await components[0].textContent();
    expect(firstComponentText.toLowerCase()).toContain('write');

    // WAL should come after Write
    const walComponentText = await components[1].textContent();
    expect(walComponentText.toLowerCase()).toContain('wal');

    // Memtable should come after WAL
    const memtableComponentText = await components[2].textContent();
    expect(memtableComponentText.toLowerCase()).toContain('memtable');

    // SSTables should come after Memtable
    const sstablesComponentText = await components[3].textContent();
    expect(sstablesComponentText.toLowerCase()).toContain('sstable');
  });

  test('TC6: Diagram has alt text or aria labels for accessibility', async ({ page }) => {
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Check diagram has role="img" and aria-label
    const diagram = architectureSection.locator('.architecture__diagram');
    await expect(diagram).toBeVisible();

    const role = await diagram.getAttribute('role');
    expect(role).toBe('img');

    const ariaLabel = await diagram.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel.toLowerCase()).toContain('lsm');
    expect(ariaLabel.toLowerCase()).toContain('architecture');

    // Check for figcaption (even if visually hidden)
    const figcaption = diagram.locator('figcaption');
    await expect(figcaption).toBeAttached();
    const figcaptionText = await figcaption.textContent();
    expect(figcaptionText.length).toBeGreaterThan(10); // Has meaningful description

    // Verify decorative icons are marked with aria-hidden
    const decorativeElements = architectureSection.locator('[aria-hidden="true"]');
    const decorativeCount = await decorativeElements.count();
    expect(decorativeCount).toBeGreaterThanOrEqual(1); // At least some elements marked as decorative
  });
});
