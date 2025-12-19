import { test, expect } from '@playwright/test';

test.describe('Architecture Overview Section', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage
    await page.goto('file://' + process.cwd() + '/public/index.html');
  });

  test('TC1: Architecture section is visible with visual diagram', async ({ page }) => {
    // Navigate to the architecture section by scrolling
    const architectureSection = page.getByTestId('architecture-section');
    await architectureSection.scrollIntoViewIfNeeded();

    // Verify architecture section is visible
    await expect(architectureSection).toBeVisible();

    // Verify the section has a proper heading
    const heading = architectureSection.locator('h2');
    await expect(heading).toContainText('How It Works');

    // Verify visual diagram is present
    const diagram = page.getByTestId('architecture-diagram');
    await expect(diagram).toBeVisible();
  });

  test('TC2: SVG diagram is rendered without errors', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.getByTestId('architecture-section');
    await architectureSection.scrollIntoViewIfNeeded();

    // Verify the architecture diagram is an SVG element
    const diagram = page.getByTestId('architecture-diagram');
    await expect(diagram).toBeVisible();

    // Verify it's an SVG element
    const tagName = await diagram.evaluate((el) => el.tagName.toLowerCase());
    expect(tagName).toBe('svg');

    // Verify the SVG has a viewBox attribute (properly configured)
    const viewBox = await diagram.getAttribute('viewBox');
    expect(viewBox).toBeTruthy();
    expect(viewBox).toBe('0 0 800 400');

    // Verify diagram container exists and is styled properly
    const container = page.locator('.architecture-diagram-container');
    await expect(container).toBeVisible();

    // Verify diagram has proper dimensions (not collapsed)
    const boundingBox = await diagram.boundingBox();
    expect(boundingBox).toBeTruthy();
    expect(boundingBox!.width).toBeGreaterThan(0);
    expect(boundingBox!.height).toBeGreaterThan(0);
  });

  test('TC3: Diagram shows WAL (Write-Ahead Log) component in data flow', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.getByTestId('architecture-section');
    await architectureSection.scrollIntoViewIfNeeded();

    // Verify WAL component exists in the diagram
    const walComponent = page.getByTestId('diagram-wal');
    await expect(walComponent).toBeVisible();

    // Verify the WAL text is visible in the SVG
    const diagram = page.getByTestId('architecture-diagram');
    const svgContent = await diagram.innerHTML();
    expect(svgContent).toContain('WAL');
    expect(svgContent).toContain('Write-Ahead');

    // Verify WAL is explained in the text explanations
    const explanations = page.getByTestId('architecture-explanations');
    const walExplanation = explanations.locator('text=Write-Ahead Log');
    await expect(walExplanation).toBeVisible();
  });

  test('TC4: Diagram shows Memtable component in data flow', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.getByTestId('architecture-section');
    await architectureSection.scrollIntoViewIfNeeded();

    // Verify Memtable component exists in the diagram
    const memtableComponent = page.getByTestId('diagram-memtable');
    await expect(memtableComponent).toBeVisible();

    // Verify the Memtable text is visible in the SVG
    const diagram = page.getByTestId('architecture-diagram');
    const svgContent = await diagram.innerHTML();
    expect(svgContent).toContain('Memtable');
    expect(svgContent).toContain('In-Memory');

    // Verify Memtable is explained in the text explanations
    const explanations = page.getByTestId('architecture-explanations');
    const memtableExplanation = explanations.locator('h3:has-text("Memtable")');
    await expect(memtableExplanation).toBeVisible();
  });

  test('TC5: Diagram shows SSTable levels in data flow', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.getByTestId('architecture-section');
    await architectureSection.scrollIntoViewIfNeeded();

    // Verify SSTable component exists in the diagram
    const sstableComponent = page.getByTestId('diagram-sstable');
    await expect(sstableComponent).toBeVisible();

    // Verify the SSTable levels are visible in the SVG
    const diagram = page.getByTestId('architecture-diagram');
    const svgContent = await diagram.innerHTML();
    expect(svgContent).toContain('SSTable');
    expect(svgContent).toContain('Level 0');
    expect(svgContent).toContain('Level 1');
    expect(svgContent).toContain('Level 2');

    // Verify SSTable levels are explained in the text explanations
    const explanations = page.getByTestId('architecture-explanations');
    const sstableExplanation = explanations.locator('h3:has-text("SSTable")');
    await expect(sstableExplanation).toBeVisible();
  });

  test('TC6: Diagram has descriptive alt text for accessibility', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.getByTestId('architecture-section');
    await architectureSection.scrollIntoViewIfNeeded();

    // Verify the SVG has an accessible role
    const diagram = page.getByTestId('architecture-diagram');
    const role = await diagram.getAttribute('role');
    expect(role).toBe('img');

    // Verify the SVG has an aria-label
    const ariaLabel = await diagram.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel?.toLowerCase()).toContain('lsm');
    expect(ariaLabel?.toLowerCase()).toContain('wal');
    expect(ariaLabel?.toLowerCase()).toContain('memtable');
    expect(ariaLabel?.toLowerCase()).toContain('sstable');

    // Verify the SVG has a title element (title is hidden by default in SVG, so we check for existence)
    const titleElement = diagram.locator('title');
    await expect(titleElement).toHaveCount(1);
    const titleText = await titleElement.textContent();
    expect(titleText?.toLowerCase()).toContain('lsm');

    // Verify the SVG has a desc element (desc is hidden by default in SVG, so we check for existence)
    const descElement = diagram.locator('desc');
    await expect(descElement).toHaveCount(1);
    const descText = await descElement.textContent();
    expect(descText?.toLowerCase()).toContain('wal');
    expect(descText?.toLowerCase()).toContain('memtable');
    expect(descText?.toLowerCase()).toContain('sstable');
  });

  test('Architecture section is accessible via navigation link', async ({ page }) => {
    // Find the architecture navigation link
    const architectureLink = page.locator('a[href="#architecture"]');
    await expect(architectureLink).toBeVisible();

    // Click the architecture link
    await architectureLink.click();

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Verify architecture section is now in view
    const architectureSection = page.getByTestId('architecture-section');
    await expect(architectureSection).toBeInViewport();
  });

  test('Architecture section has explanatory text accompanying the diagram', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.getByTestId('architecture-section');
    await architectureSection.scrollIntoViewIfNeeded();

    // Verify explanations container exists
    const explanations = page.getByTestId('architecture-explanations');
    await expect(explanations).toBeVisible();

    // Verify explanation cards exist with headings and descriptions
    const explanationCards = explanations.locator('.explanation-card');
    const cardCount = await explanationCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(3);

    // Verify each card has a heading and description
    for (let i = 0; i < cardCount; i++) {
      const card = explanationCards.nth(i);
      const heading = card.locator('h3');
      await expect(heading).toBeVisible();

      const description = card.locator('p');
      await expect(description).toBeVisible();
      const descText = await description.textContent();
      expect(descText?.trim().length).toBeGreaterThan(0);
    }
  });

  test('Diagram shows complete data flow: Write -> WAL -> Memtable -> SSTable', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.getByTestId('architecture-section');
    await architectureSection.scrollIntoViewIfNeeded();

    const diagram = page.getByTestId('architecture-diagram');
    const svgContent = await diagram.innerHTML();

    // Verify all components of the data flow are present
    expect(svgContent).toContain('Write');
    expect(svgContent).toContain('WAL');
    expect(svgContent).toContain('Memtable');
    expect(svgContent).toContain('SSTable');

    // Verify arrows/connections exist (arrow markers)
    expect(svgContent).toContain('arrowhead');
    expect(svgContent).toContain('marker-end');

    // Verify flush label exists showing data movement from Memtable to SSTable
    expect(svgContent).toContain('Flush');

    // Verify compaction between levels
    expect(svgContent).toContain('Compact');
  });

  test('Architecture section is responsive on mobile viewport', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('file://' + process.cwd() + '/public/index.html');

    // Navigate to architecture section
    const architectureSection = page.getByTestId('architecture-section');
    await architectureSection.scrollIntoViewIfNeeded();

    // Verify architecture section is visible
    await expect(architectureSection).toBeVisible();

    // Verify diagram container allows horizontal scroll on mobile
    const diagramContainer = page.locator('.architecture-diagram-container');
    await expect(diagramContainer).toBeVisible();

    // Verify explanation cards are visible on mobile
    const explanationCards = page.locator('.explanation-card');
    const cardCount = await explanationCards.count();
    for (let i = 0; i < cardCount; i++) {
      await expect(explanationCards.nth(i)).toBeVisible();
    }
  });
});
