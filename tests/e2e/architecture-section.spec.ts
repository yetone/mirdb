import { test, expect } from '@playwright/test';

/**
 * E2E Tests for Architecture Overview Section
 * Tests validate that the architecture section displays LSM tree diagram
 * and data flow explanation as per PRD requirements.
 */

test.describe('Architecture Overview Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Check for architecture diagram presence', async ({ page }) => {
    // Navigate to the architecture section
    const architectureSection = page.locator('#architecture');
    await architectureSection.scrollIntoViewIfNeeded();
    await expect(architectureSection).toBeVisible();

    // Check for SVG or image diagram of LSM tree architecture
    const diagramWrapper = architectureSection.locator('[data-testid="architecture-diagram"]');
    await expect(diagramWrapper).toBeVisible();

    // Verify the SVG diagram exists inside the wrapper
    const svgDiagram = diagramWrapper.locator('svg.architecture-diagram');
    await expect(svgDiagram).toBeVisible();
  });

  test('TC2: Verify diagram is an SVG element', async ({ page }) => {
    // Navigate to the architecture section
    const architectureSection = page.locator('#architecture');
    await architectureSection.scrollIntoViewIfNeeded();

    // Architecture diagram uses SVG format for scalability
    const svgDiagram = architectureSection.locator('svg.architecture-diagram, [data-testid="architecture-diagram"] svg, svg[data-testid="architecture-diagram"]');
    await expect(svgDiagram).toBeVisible();

    // Verify it's actually an SVG element
    const tagName = await svgDiagram.evaluate((el) => el.tagName.toLowerCase());
    expect(tagName).toBe('svg');
  });

  test('TC3: Check write path content', async ({ page }) => {
    // Navigate to the architecture section
    const architectureSection = page.locator('#architecture');
    await architectureSection.scrollIntoViewIfNeeded();

    // Check for write path explanation
    const writePathSection = architectureSection.locator('[data-testid="write-path"], .write-path, .data-flow').filter({
      has: page.locator('text=/write path/i')
    });
    await expect(writePathSection).toBeVisible();

    // Write path mentions WAL, Memtable, and SSTables
    const writePathContent = await writePathSection.textContent();
    expect(writePathContent).toMatch(/WAL/i);
    expect(writePathContent).toMatch(/Memtable/i);
    expect(writePathContent).toMatch(/SST|SSTable/i);
  });

  test('TC4: Check read path content', async ({ page }) => {
    // Navigate to the architecture section
    const architectureSection = page.locator('#architecture');
    await architectureSection.scrollIntoViewIfNeeded();

    // Check for read path explanation
    const readPathSection = architectureSection.locator('[data-testid="read-path"], .read-path, .data-flow').filter({
      has: page.locator('text=/read path/i')
    });
    await expect(readPathSection).toBeVisible();

    // Read path explains lookup from memtable through SST levels
    const readPathContent = await readPathSection.textContent();
    expect(readPathContent).toMatch(/Memtable/i);
    expect(readPathContent).toMatch(/L0|L1|level|SST/i);
  });

  test('Architecture section is navigable from navigation link', async ({ page }) => {
    // Click on the Architecture link in navigation if exists
    const architectureLink = page.locator('a[href="#architecture"]');
    if (await architectureLink.count() > 0) {
      await architectureLink.click();
      const architectureSection = page.locator('#architecture');
      await expect(architectureSection).toBeInViewport();
    }
  });

  test('Architecture section has proper heading', async ({ page }) => {
    // Navigate to the architecture section
    const architectureSection = page.locator('#architecture');
    await architectureSection.scrollIntoViewIfNeeded();

    // Check for Architecture heading
    const heading = architectureSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText(/Architecture/i);
  });
});
