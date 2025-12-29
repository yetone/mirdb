// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.join(__dirname, '..', 'index.html');

test.describe('Architecture Overview Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  test('TC1: Architecture section exists with heading and content', async ({ page }) => {
    // Locate architecture section on page
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Verify section has a heading containing "Architecture"
    const heading = architectureSection.locator('h2');
    await expect(heading).toBeVisible();
    const headingText = await heading.textContent();
    expect(headingText?.toLowerCase()).toContain('architecture');

    // Verify section has content
    const content = architectureSection.locator('p, div, pre');
    const contentCount = await content.count();
    expect(contentCount).toBeGreaterThan(0);
  });

  test('TC2: Visual diagram or illustration showing data flow is displayed', async ({ page }) => {
    // Verify architecture diagram presence
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Check for visual diagram (can be img, svg, or ASCII diagram in pre/code block)
    const diagram = architectureSection.locator('img, svg, .diagram, .architecture-diagram, pre, .data-flow');
    await expect(diagram.first()).toBeVisible();

    // Verify the diagram shows data flow (WAL -> Memtable -> SSTable)
    const diagramContent = await architectureSection.textContent();
    expect(diagramContent).toMatch(/WAL|Memtable|SSTable/i);
  });

  test('TC3: Architecture diagram has appropriate alt text for accessibility', async ({ page }) => {
    // Check diagram has alt text
    const architectureSection = page.locator('#architecture');

    // Look for img elements with alt text
    const img = architectureSection.locator('img');
    const imgCount = await img.count();

    if (imgCount > 0) {
      const altText = await img.first().getAttribute('alt');
      expect(altText).toBeTruthy();
      expect(altText?.toLowerCase()).toMatch(/architecture|data flow|diagram/i);
    } else {
      // If using ASCII diagram or SVG, check for aria-label or title
      const svgOrDiagram = architectureSection.locator('svg, .diagram, .architecture-diagram');
      const svgCount = await svgOrDiagram.count();

      if (svgCount > 0) {
        const ariaLabel = await svgOrDiagram.first().getAttribute('aria-label');
        const title = await svgOrDiagram.first().getAttribute('title');
        const role = await svgOrDiagram.first().getAttribute('role');
        expect(ariaLabel || title || role).toBeTruthy();
      } else {
        // ASCII diagram in pre/code block should have aria-label or be described
        const preDiagram = architectureSection.locator('pre, .data-flow');
        const preCount = await preDiagram.count();
        if (preCount > 0) {
          const ariaLabel = await preDiagram.first().getAttribute('aria-label');
          const role = await preDiagram.first().getAttribute('role');
          // ASCII diagrams should have aria-label for accessibility
          expect(ariaLabel || role).toBeTruthy();
        }
      }
    }
  });

  test('TC4: Write-Ahead Log (WAL) is mentioned and explained for durability', async ({ page }) => {
    // Verify WAL component explanation
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Check that WAL is mentioned
    const walContent = await architectureSection.textContent();
    expect(walContent).toMatch(/WAL|Write-Ahead Log/i);

    // Check that durability is explained
    expect(walContent?.toLowerCase()).toMatch(/durability|persist|crash|recovery/i);
  });

  test('TC5: Memtable and SSTable components are explained', async ({ page }) => {
    // Verify Memtable and SSTable explanations
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    const content = await architectureSection.textContent();

    // Check Memtable is mentioned and explained (in-memory)
    expect(content).toMatch(/Memtable/i);
    expect(content?.toLowerCase()).toMatch(/memory|in-memory/i);

    // Check SSTable is mentioned and explained (on-disk)
    expect(content).toMatch(/SSTable/i);
    expect(content?.toLowerCase()).toMatch(/disk|storage|persist/i);
  });
});
