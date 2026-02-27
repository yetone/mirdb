// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Performance Benchmarks Section', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test('TC1: Benchmarks section has a table element with data', async ({ page }) => {
        // Navigate to benchmarks section
        const benchmarksSection = page.locator('#benchmarks');
        await expect(benchmarksSection).toBeVisible();

        // Check that table element exists
        const table = benchmarksSection.locator('table.benchmarks-table');
        await expect(table).toBeVisible();

        // Verify table has data rows (thead + tbody with rows)
        const rows = table.locator('tbody tr');
        await expect(rows).toHaveCount(3); // MirDB, Memcached, Redis
    });

    test('TC2: Table has proper headers', async ({ page }) => {
        const table = page.locator('#benchmarks table.benchmarks-table');

        // Check for Database header
        const databaseHeader = table.locator('thead th', { hasText: 'Database' });
        await expect(databaseHeader).toBeVisible();

        // Check for Operations/sec header (or similar)
        const opsHeader = table.locator('thead th', { hasText: /Operations.*sec/i });
        await expect(opsHeader).toBeVisible();

        // Check for latency headers
        const latencyHeaders = table.locator('thead th', { hasText: /Latency/i });
        await expect(latencyHeaders).toHaveCount(6); // Read (p50, p95, p99) + Write (p50, p95, p99)
    });

    test('TC3: MirDB row exists with metric values', async ({ page }) => {
        const table = page.locator('#benchmarks table.benchmarks-table');

        // Find row with MirDB
        const mirdbRow = table.locator('tbody tr', { hasText: 'MirDB' });
        await expect(mirdbRow).toBeVisible();

        // Check MirDB row has ops/sec value
        const mirdbCells = mirdbRow.locator('td');
        await expect(mirdbCells).toHaveCount(7); // 7 metrics columns

        // Verify first cell has numeric value (ops/sec)
        const opsValue = mirdbCells.first();
        await expect(opsValue).toHaveText(/[\d,]+/);
    });

    test('TC4: Memcached comparison row exists', async ({ page }) => {
        const table = page.locator('#benchmarks table.benchmarks-table');

        // Find row with Memcached
        const memcachedRow = table.locator('tbody tr', { hasText: 'Memcached' });
        await expect(memcachedRow).toBeVisible();

        // Check Memcached row has metric values
        const memcachedCells = memcachedRow.locator('td');
        await expect(memcachedCells).toHaveCount(7);

        // Verify it has latency metrics with 'ms' unit
        const latencyCell = memcachedCells.nth(1); // Read p50
        await expect(latencyCell).toHaveText(/[\d.]+ms/);
    });

    test('TC5: Redis comparison row exists', async ({ page }) => {
        const table = page.locator('#benchmarks table.benchmarks-table');

        // Find row with Redis
        const redisRow = table.locator('tbody tr', { hasText: 'Redis' });
        await expect(redisRow).toBeVisible();

        // Check Redis row has metric values
        const redisCells = redisRow.locator('td');
        await expect(redisCells).toHaveCount(7);

        // Verify it has numeric/latency data
        const opsValue = redisCells.first();
        await expect(opsValue).toHaveText(/[\d,]+/);
    });

    test('TC6: Table has proper scope attributes for accessibility', async ({ page }) => {
        const table = page.locator('#benchmarks table.benchmarks-table');

        // Check column headers have scope="col"
        const colHeaders = table.locator('thead th[scope="col"]');
        const colHeaderCount = await colHeaders.count();
        expect(colHeaderCount).toBeGreaterThan(0);

        // Verify all thead th have scope="col"
        const allTheadTh = table.locator('thead th');
        const allTheadThCount = await allTheadTh.count();
        expect(colHeaderCount).toBe(allTheadThCount);

        // Check row headers have scope="row"
        const rowHeaders = table.locator('tbody th[scope="row"]');
        const rowHeaderCount = await rowHeaders.count();
        expect(rowHeaderCount).toBe(3); // MirDB, Memcached, Redis
    });

    test('TC7: Latency metrics include percentiles (p50, p95, p99)', async ({ page }) => {
        const table = page.locator('#benchmarks table.benchmarks-table');

        // Check for p50 in headers
        const p50Header = table.locator('thead th', { hasText: 'p50' });
        await expect(p50Header.first()).toBeVisible();

        // Check for p95 in headers
        const p95Header = table.locator('thead th', { hasText: 'p95' });
        await expect(p95Header.first()).toBeVisible();

        // Check for p99 in headers
        const p99Header = table.locator('thead th', { hasText: 'p99' });
        await expect(p99Header.first()).toBeVisible();

        // Verify we have both read and write latency percentiles
        const readLatencyHeaders = table.locator('thead th', { hasText: /Read Latency/i });
        await expect(readLatencyHeaders).toHaveCount(3); // p50, p95, p99

        const writeLatencyHeaders = table.locator('thead th', { hasText: /Write Latency/i });
        await expect(writeLatencyHeaders).toHaveCount(3); // p50, p95, p99
    });
});
