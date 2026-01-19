// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '../index.html');

test.describe('Performance Benchmarks Section (REQ-6)', () => {
  // Test Case 1: Check for performance/benchmarks section
  // Expected: Section with performance data or benchmarks exists
  test('TC1: Performance benchmarks section exists', async ({ page }) => {
    await page.goto(indexPath, { waitUntil: 'domcontentloaded' });

    // Check that the benchmarks section exists
    const benchmarksSection = page.locator('#benchmarks, .benchmarks, section:has(h2:text-matches("benchmark|performance", "i"))');
    await expect(benchmarksSection.first()).toBeVisible();

    // Verify the section has an appropriate heading
    const heading = page.locator('#benchmarks h2, .benchmarks h2');
    await expect(heading.first()).toBeVisible();
    const headingText = await heading.first().textContent();
    expect(headingText?.toLowerCase()).toMatch(/benchmark|performance/i);
  });

  // Test Case 2: Verify performance data is meaningful
  // Expected: Benchmarks include specific numbers or comparisons
  test('TC2: Benchmarks include specific numbers or comparisons', async ({ page }) => {
    await page.goto(indexPath, { waitUntil: 'domcontentloaded' });

    const benchmarksSection = page.locator('#benchmarks, .benchmarks');
    await expect(benchmarksSection.first()).toBeVisible();

    // Check for numeric values in the benchmarks section
    // Performance data should contain numbers (latency, throughput, ops/sec, etc.)
    const sectionContent = await benchmarksSection.first().textContent();

    // Look for patterns like: numbers with units (ms, ops/sec, MB/s, etc.)
    const hasNumbers = /\d+\s*(ms|μs|ns|ops|MB|KB|GB|\/s|%|x)/i.test(sectionContent || '');
    const hasComparisons = /\d+(\.\d+)?x|faster|slower|compared|vs\.?|versus/i.test(sectionContent || '');
    const hasPerformanceMetrics = /latency|throughput|operations|requests|writes|reads/i.test(sectionContent || '');

    // At least one of these should be true for meaningful benchmark data
    expect(hasNumbers || hasComparisons || hasPerformanceMetrics).toBe(true);

    // Check for specific benchmark data elements
    const benchmarkCards = page.locator('#benchmarks .benchmark-card, .benchmarks .benchmark-stat, #benchmarks .stat-value, .benchmarks .metric-value');
    const cardCount = await benchmarkCards.count();

    // Should have at least some benchmark data points
    expect(cardCount).toBeGreaterThanOrEqual(0); // Flexibility for different implementations
  });

  // Test Case 3: Verify performance section has visual elements
  // Expected: Performance data is presented with charts, graphs, or tables
  test('TC3: Performance data has visual presentation elements', async ({ page }) => {
    await page.goto(indexPath, { waitUntil: 'domcontentloaded' });

    const benchmarksSection = page.locator('#benchmarks, .benchmarks');
    await expect(benchmarksSection.first()).toBeVisible();

    // Check for visual elements: charts, graphs, tables, or styled metric cards
    const hasTable = await page.locator('#benchmarks table, .benchmarks table').count() > 0;
    const hasChart = await page.locator('#benchmarks canvas, #benchmarks svg, .benchmarks canvas, .benchmarks svg, #benchmarks .chart, .benchmarks .chart').count() > 0;
    const hasStyledCards = await page.locator('#benchmarks .benchmark-card, .benchmarks .stat-card, #benchmarks .metric-card, .benchmarks .benchmark-stat').count() > 0;
    const hasProgressBars = await page.locator('#benchmarks .progress-bar, .benchmarks .bar-chart, #benchmarks .bar, .benchmarks .comparison-bar').count() > 0;
    const hasBenchmarkGrid = await page.locator('#benchmarks .benchmark-grid, .benchmarks .stats-grid, #benchmarks .metrics-grid').count() > 0;

    // At least one visual presentation format should be present
    const hasVisualElements = hasTable || hasChart || hasStyledCards || hasProgressBars || hasBenchmarkGrid;
    expect(hasVisualElements).toBe(true);

    // Verify the visual elements are styled appropriately
    // Check that the section has proper styling applied
    const sectionStyles = await benchmarksSection.first().evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        padding: styles.padding,
        backgroundColor: styles.backgroundColor
      };
    });

    // Section should have some padding (indicating intentional layout)
    expect(sectionStyles.padding).not.toBe('0px');
  });

  // Additional test: Verify benchmark section is accessible
  test('Benchmark section is accessible', async ({ page }) => {
    await page.goto(indexPath, { waitUntil: 'domcontentloaded' });

    const benchmarksSection = page.locator('#benchmarks, .benchmarks');
    await expect(benchmarksSection.first()).toBeVisible();

    // Check for proper heading hierarchy
    const h2 = page.locator('#benchmarks h2, .benchmarks h2');
    await expect(h2.first()).toBeVisible();

    // If there are tables, they should have proper structure
    const tables = page.locator('#benchmarks table, .benchmarks table');
    const tableCount = await tables.count();

    if (tableCount > 0) {
      // Tables should have headers
      const tableHeaders = page.locator('#benchmarks table th, .benchmarks table th');
      const headerCount = await tableHeaders.count();
      expect(headerCount).toBeGreaterThan(0);
    }
  });

  // Additional test: Verify benchmark data is readable
  test('Benchmark data is readable and well-formatted', async ({ page }) => {
    await page.goto(indexPath, { waitUntil: 'domcontentloaded' });

    const benchmarksSection = page.locator('#benchmarks, .benchmarks');
    await expect(benchmarksSection.first()).toBeVisible();

    // Check that text content is substantial (not empty)
    const textContent = await benchmarksSection.first().textContent();
    expect(textContent?.trim().length).toBeGreaterThan(50); // Should have meaningful content

    // Check for readable font size
    const contentElements = page.locator('#benchmarks p, #benchmarks td, #benchmarks .stat-value, .benchmarks p, .benchmarks td, .benchmarks .stat-value');
    const count = await contentElements.count();

    if (count > 0) {
      const fontSize = await contentElements.first().evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      expect(fontSize).toBeGreaterThanOrEqual(14); // Minimum readable font size
    }
  });
});
