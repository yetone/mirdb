// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '..', 'index.html');

test.describe('Architecture Diagram Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  /**
   * Test Case 1: Architecture section exists with header/title
   */
  test('TC1: architecture section exists with header/title', async ({ page }) => {
    // Verify architecture section exists
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Verify section has a title/header
    const sectionTitle = architectureSection.locator('h2, .section-title');
    await expect(sectionTitle).toBeVisible();
    const titleText = await sectionTitle.textContent();
    expect(titleText.toLowerCase()).toMatch(/architecture/i);
  });

  /**
   * Test Case 2: Visual diagram element (img or svg) exists in architecture section
   */
  test('TC2: architecture diagram image or SVG exists', async ({ page }) => {
    // Verify architecture section exists
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Check for visual diagram - either img or svg element
    const diagram = architectureSection.locator('img, svg.architecture-diagram, .architecture-diagram svg, .architecture-diagram img');
    await expect(diagram.first()).toBeVisible();
  });

  /**
   * Test Case 3: Architecture section mentions 'WAL' or 'Write-Ahead Log'
   */
  test('TC3: architecture section mentions WAL', async ({ page }) => {
    // Verify architecture section exists
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Check that WAL is mentioned in the architecture section
    const sectionText = await architectureSection.textContent();
    expect(sectionText).toMatch(/WAL|Write-Ahead Log/i);
  });

  /**
   * Test Case 4: Architecture section mentions 'Memtable' or 'memory table'
   */
  test('TC4: architecture section mentions Memtable', async ({ page }) => {
    // Verify architecture section exists
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Check that Memtable is mentioned in the architecture section
    const sectionText = await architectureSection.textContent();
    expect(sectionText).toMatch(/Memtable|memory table/i);
  });

  /**
   * Test Case 5: Architecture section mentions 'SSTable' or 'Sorted String Table'
   */
  test('TC5: architecture section mentions SSTable', async ({ page }) => {
    // Verify architecture section exists
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Check that SSTable is mentioned in the architecture section
    const sectionText = await architectureSection.textContent();
    expect(sectionText).toMatch(/SSTable|Sorted String Table/i);
  });
});
