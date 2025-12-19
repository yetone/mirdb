// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

// Base URL for the static HTML file
const BASE_URL = 'file://' + path.resolve(__dirname, '../dist/index.html');

test.describe('Architecture Overview Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(BASE_URL);
  });

  test('Test Case 1: Check architecture diagram visibility', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeVisible();

    // Check that the diagram container exists
    const diagramContainer = page.locator('[data-testid="architecture-diagram"]');
    await expect(diagramContainer).toBeVisible();

    // Verify data flow components are present: WAL, Memtable, SSTable
    const walComponent = page.locator('[data-testid="diagram-wal"]');
    await expect(walComponent).toBeVisible();
    const walText = await walComponent.textContent();
    expect(walText).toContain('WAL');

    const memtableComponent = page.locator('[data-testid="diagram-memtable"]');
    await expect(memtableComponent).toBeVisible();
    const memtableText = await memtableComponent.textContent();
    expect(memtableText).toContain('Memtable');

    const sstableComponent = page.locator('[data-testid="diagram-sstable"]');
    await expect(sstableComponent).toBeVisible();
    const sstableText = await sstableComponent.textContent();
    expect(sstableText).toContain('SSTable');

    // Verify flow arrows or connectors exist
    const flowArrows = page.locator('[data-testid^="flow-arrow"]');
    await expect(flowArrows.first()).toBeVisible();
  });

  test('Test Case 2: Check LSM tree explanation', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeVisible();

    // Check that the LSM tree explanation exists
    const explanationSection = page.locator('[data-testid="lsm-explanation"]');
    await expect(explanationSection).toBeVisible();

    // Verify explanation content mentions LSM tree benefits
    const explanationText = await explanationSection.textContent();
    expect(explanationText.toLowerCase()).toContain('lsm');

    // Check that benefits are mentioned (write-optimized, persistence, etc.)
    const benefitsList = page.locator('[data-testid="lsm-benefits"]');
    await expect(benefitsList).toBeVisible();
  });

  test('Test Case 3: Check architecture documentation link', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeVisible();

    // Check that the documentation link exists
    const docsLink = page.locator('[data-testid="architecture-docs-link"]');
    await expect(docsLink).toBeVisible();

    // Verify the link has proper attributes
    const href = await docsLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href.length).toBeGreaterThan(0);

    // Verify link text indicates it leads to architecture documentation
    const linkText = await docsLink.textContent();
    expect(linkText.toLowerCase()).toMatch(/architecture|documentation|learn more|read more/);
  });
});
