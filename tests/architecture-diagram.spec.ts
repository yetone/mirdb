import { test, expect } from '@playwright/test';

test.describe('Architecture Diagram', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('page contains an architecture diagram image or SVG', async ({ page }) => {
    // Test Case 1: Search for architecture diagram image or SVG
    // Expected: Page contains an image or SVG showing architecture diagram

    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeVisible();

    // Check for the architecture diagram figure with SVG
    const architectureDiagram = page.locator('[data-testid="architecture-diagram"]');
    await expect(architectureDiagram).toBeVisible();

    // Verify SVG exists within the diagram
    const svg = architectureDiagram.locator('svg');
    await expect(svg).toBeVisible();

    // Verify the SVG has role="img" for accessibility
    await expect(svg).toHaveAttribute('role', 'img');
  });

  test('architecture diagram has appropriate alt text for accessibility', async ({ page }) => {
    // Test Case 2: Verify diagram has alt text or accessible description
    // Expected: Architecture diagram has appropriate alt text for accessibility

    const architectureDiagram = page.locator('[data-testid="architecture-diagram"]');
    await expect(architectureDiagram).toBeVisible();

    const svg = architectureDiagram.locator('svg');

    // Check for aria-labelledby attribute pointing to title and desc
    await expect(svg).toHaveAttribute('aria-labelledby', 'architecture-diagram-title architecture-diagram-desc');

    // Verify title element exists with meaningful content (title is not visually visible, check for attached)
    const title = svg.locator('title#architecture-diagram-title');
    await expect(title).toBeAttached();
    const titleText = await title.textContent();
    expect(titleText).toContain('Architecture');
    expect(titleText).toContain('Diagram');

    // Verify desc element exists with meaningful description
    const desc = svg.locator('desc#architecture-diagram-desc');
    await expect(desc).toBeAttached();
    const descText = await desc.textContent();
    expect(descText).toContain('WAL');
    expect(descText).toContain('Memtable');
    expect(descText).toContain('SSTable');
  });

  test('diagram or its description references WAL, Memtable, SSTable', async ({ page }) => {
    // Test Case 3: Check diagram mentions key components
    // Expected: Diagram or its description references WAL, Memtable, SSTable

    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeVisible();

    // Check that the SVG contains text elements for each component
    const architectureDiagram = page.locator('[data-testid="architecture-diagram"]');
    const svg = architectureDiagram.locator('svg');

    // Check for WAL component in the diagram
    const walText = svg.locator('text:has-text("WAL")');
    await expect(walText.first()).toBeVisible();

    // Check for Memtable component in the diagram
    const memtableText = svg.locator('text:has-text("Memtable")');
    await expect(memtableText.first()).toBeVisible();

    // Check for SSTable component in the diagram
    const sstableText = svg.locator('text:has-text("SSTable")');
    await expect(sstableText.first()).toBeVisible();

    // Also verify the architecture description section mentions all components
    const descriptionSection = architectureSection.locator('.architecture-description');
    await expect(descriptionSection).toBeVisible();

    const descriptionText = await descriptionSection.textContent();
    expect(descriptionText).toContain('WAL');
    expect(descriptionText).toContain('Memtable');
    expect(descriptionText).toContain('SSTable');
  });

  test('architecture section has proper semantic structure', async ({ page }) => {
    // Additional test for semantic HTML structure
    const architectureSection = page.locator('section.architecture');
    await expect(architectureSection).toBeVisible();

    // Verify h2 exists within architecture section
    const h2 = architectureSection.locator('h2');
    await expect(h2).toBeVisible();
    await expect(h2).toContainText('Architecture');

    // Verify figure element is used for the diagram
    const figure = architectureSection.locator('figure');
    await expect(figure).toBeVisible();
  });

  test('diagram shows data flow direction with arrows', async ({ page }) => {
    // Verify the diagram shows data flow with visual indicators
    const architectureDiagram = page.locator('[data-testid="architecture-diagram"]');
    const svg = architectureDiagram.locator('svg');

    // Check for path elements (arrows) in the SVG
    const paths = svg.locator('path');
    const pathCount = await paths.count();
    expect(pathCount).toBeGreaterThanOrEqual(2); // At least arrows between components

    // Check for arrowhead marker definition (marker elements are in defs, not visually visible)
    const marker = svg.locator('marker#arrowhead');
    await expect(marker).toBeAttached();
  });

  test('diagram components have data attributes for identification', async ({ page }) => {
    // Verify each component box has identifying data attributes
    const architectureDiagram = page.locator('[data-testid="architecture-diagram"]');
    const svg = architectureDiagram.locator('svg');

    // Check for WAL component rect
    const walRect = svg.locator('rect[data-component="wal"]');
    await expect(walRect).toBeVisible();

    // Check for Memtable component rect
    const memtableRect = svg.locator('rect[data-component="memtable"]');
    await expect(memtableRect).toBeVisible();

    // Check for SSTable component rect
    const sstableRect = svg.locator('rect[data-component="sstable"]');
    await expect(sstableRect).toBeVisible();
  });
});
