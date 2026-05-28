/**
 * E2E Tests: Performance Benchmarks
 * Owner: Scenario 6 - Performance Benchmarks
 */

const { test, expect } = require('@playwright/test');

test.describe('Benchmarks Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('comparison table with MirDB vs alternatives is present', async ({ page }) => {
    const section = page.locator('#benchmarks');
    await expect(section).toBeVisible();

    const table = section.locator('table.benchmark-table');
    await expect(table).toBeVisible();
  });

  test('table uses semantic HTML markup', async ({ page }) => {
    const table = page.locator('table.benchmark-table');
    await expect(table).toBeVisible();

    const thead = table.locator('thead');
    await expect(thead).toBeVisible();

    const tbody = table.locator('tbody');
    await expect(tbody).toBeVisible();

    const headerCells = table.locator('thead th');
    await expect(headerCells).toHaveCount(4);

    const rowHeaders = table.locator('tbody th[scope="row"]');
    await expect(rowHeaders).toHaveCount(5);

    const columnHeaders = table.locator('thead th[scope="col"]');
    await expect(columnHeaders).toHaveCount(4);
  });

  test('memcached appears as an alternative in the comparison', async ({ page }) => {
    const table = page.locator('table.benchmark-table');
    await expect(table).toContainText('memcached');

    const headers = table.locator('thead th');
    const headerTexts = await headers.allTextContents();
    expect(headerTexts.some(text => text.toLowerCase().includes('memcached'))).toBe(true);
  });

  test('Redis appears as an alternative in the comparison', async ({ page }) => {
    const table = page.locator('table.benchmark-table');
    await expect(table).toContainText('Redis');

    const headers = table.locator('thead th');
    const headerTexts = await headers.allTextContents();
    expect(headerTexts.some(text => text.toLowerCase().includes('redis'))).toBe(true);
  });

  test('at least one row shows MirDB meeting or exceeding alternative performance', async ({ page }) => {
    const table = page.locator('table.benchmark-table');
    const rows = table.locator('tbody tr');
    const rowCount = await rows.count();

    let foundCompetitive = false;
    for (let i = 0; i < rowCount; i++) {
      const cells = rows.nth(i).locator('td');
      const cellCount = await cells.count();
      if (cellCount < 3) continue;

      const mirdbValue = await cells.nth(0).textContent();
      const memcachedValue = await cells.nth(1).textContent();
      const redisValue = await cells.nth(2).textContent();

      const mirdbNum = parseFloat(mirdbValue.replace(/[^0-9.]/g, ''));
      const memcachedNum = parseFloat(memcachedValue.replace(/[^0-9.]/g, ''));
      const redisNum = parseFloat(redisValue.replace(/[^0-9.]/g, ''));

      if (!isNaN(mirdbNum) && !isNaN(memcachedNum) && !isNaN(redisNum)) {
        if (mirdbNum >= memcachedNum || mirdbNum >= redisNum) {
          foundCompetitive = true;
          break;
        }
      }
    }

    expect(foundCompetitive).toBe(true);
  });

  test('benchmark methodology text is present', async ({ page }) => {
    const section = page.locator('#benchmarks');
    await expect(section).toContainText('Benchmark Methodology');
    await expect(section).toContainText('Hardware');
    await expect(section).toContainText('Dataset');
    await expect(section).toContainText('Measurement');
  });

  test('benchmark table is accessible with proper headers for screen readers', async ({ page }) => {
    const table = page.locator('table.benchmark-table');
    await expect(table).toBeVisible();

    const thElements = table.locator('th');
    const count = await thElements.count();
    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const scope = await thElements.nth(i).getAttribute('scope');
      expect(scope).toMatch(/^(col|row)$/);
    }
  });

  test('benchmark section remains readable at mobile viewport', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    const section = page.locator('#benchmarks');
    await expect(section).toBeVisible();

    const table = section.locator('table.benchmark-table');
    await expect(table).toBeVisible();

    const tableWrapper = section.locator('.benchmark-table-wrapper');
    const hasOverflow = await tableWrapper.evaluate(el => {
      return getComputedStyle(el).overflowX === 'auto' ||
             getComputedStyle(el).overflowX === 'scroll';
    });
    expect(hasOverflow).toBe(true);
  });

  test('table includes read and write ops/sec metrics', async ({ page }) => {
    const table = page.locator('table.benchmark-table');

    await expect(table).toContainText('Read ops/sec');
    await expect(table).toContainText('Write ops/sec');
  });

  test('MirDB column header is present', async ({ page }) => {
    const table = page.locator('table.benchmark-table');
    await expect(table).toContainText('MirDB');

    const headers = table.locator('thead th');
    const headerTexts = await headers.allTextContents();
    expect(headerTexts.some(text => text.includes('MirDB'))).toBe(true);
  });
});
