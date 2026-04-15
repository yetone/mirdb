/**
 * Technical Specifications Section E2E Tests
 * Owner: Scenario 5 - Configuration Display
 *
 * Tests for:
 * - TechSpecs section visibility
 * - Default port display (12333)
 * - Work directory display (/tmp/mirdb)
 * - SSTable and memtable size limits display
 * - All configuration values present
 */

import { test, expect } from '@playwright/test';
import { SELECTORS, TECHSPECS_CONFIG } from '../fixtures/test-data';

test.describe('Technical Specifications - Configuration Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('techspecs section exists with configuration information', async ({ page }) => {
    const techspecsSection = page.locator(SELECTORS.TECHSPECS.SECTION);
    await expect(techspecsSection).toBeVisible();

    // Verify it has a title
    const title = page.locator(SELECTORS.TECHSPECS.TITLE);
    await expect(title).toBeVisible();
    await expect(title).toContainText('Configuration');

    // Verify there are configuration items
    const items = page.locator(SELECTORS.TECHSPECS.ITEM);
    const itemCount = await items.count();
    expect(itemCount).toBeGreaterThanOrEqual(4);
  });

  test('default port 12333 is visible in specs section', async ({ page }) => {
    const techspecsSection = page.locator(SELECTORS.TECHSPECS.SECTION);
    await expect(techspecsSection).toBeVisible();

    // Check that port value is displayed
    const sectionText = await techspecsSection.textContent();
    expect(sectionText).toContain(TECHSPECS_CONFIG.PORT);

    // Verify the port value element specifically
    const portValue = techspecsSection.locator(SELECTORS.TECHSPECS.VALUE, {
      hasText: TECHSPECS_CONFIG.PORT,
    });
    await expect(portValue).toBeVisible();
  });

  test('work directory path /tmp/mirdb is visible', async ({ page }) => {
    const techspecsSection = page.locator(SELECTORS.TECHSPECS.SECTION);
    await expect(techspecsSection).toBeVisible();

    // Check that work directory path is displayed
    const sectionText = await techspecsSection.textContent();
    expect(sectionText).toContain(TECHSPECS_CONFIG.WORK_DIR);

    // Verify the work directory value element specifically
    const workDirValue = techspecsSection.locator(SELECTORS.TECHSPECS.VALUE, {
      hasText: TECHSPECS_CONFIG.WORK_DIR,
    });
    await expect(workDirValue).toBeVisible();
  });

  test('SSTable and memtable size limits are visible', async ({ page }) => {
    const techspecsSection = page.locator(SELECTORS.TECHSPECS.SECTION);
    await expect(techspecsSection).toBeVisible();

    const sectionText = await techspecsSection.textContent();

    // Check SSTable max size (100 MB or 100MB)
    expect(sectionText).toMatch(/100\s*MB/i);

    // Check memtable max size (4 MB or 4MB)
    expect(sectionText).toMatch(/4\s*MB/i);

    // Verify the specific values are visible
    const sstableValue = techspecsSection.locator(SELECTORS.TECHSPECS.VALUE, {
      hasText: /100\s*MB/,
    });
    await expect(sstableValue).toBeVisible();

    const memtableValue = techspecsSection.locator(SELECTORS.TECHSPECS.VALUE, {
      hasText: /4\s*MB/,
    });
    await expect(memtableValue).toBeVisible();
  });

  test('all configuration values are displayed in grid/cards', async ({ page }) => {
    const techspecsSection = page.locator(SELECTORS.TECHSPECS.SECTION);
    await expect(techspecsSection).toBeVisible();

    // Verify grid container exists
    const grid = page.locator(SELECTORS.TECHSPECS.GRID);
    await expect(grid).toBeVisible();

    // Verify all expected configuration items are present
    const configItems = [
      { label: 'Port', value: TECHSPECS_CONFIG.PORT },
      { label: 'Work Directory', value: TECHSPECS_CONFIG.WORK_DIR },
      { label: 'SSTable', value: /100\s*MB/ },
      { label: 'Memtable', value: /4\s*MB/ },
    ];

    for (const config of configItems) {
      const item = techspecsSection.locator(SELECTORS.TECHSPECS.ITEM, {
        hasText: config.label,
      });
      await expect(item).toBeVisible();

      if (typeof config.value === 'string') {
        await expect(item).toContainText(config.value);
      } else {
        const itemText = await item.textContent();
        expect(itemText).toMatch(config.value);
      }
    }
  });

  test('techspecs section can be navigated to from header', async ({ page }) => {
    // Click on the Specs link in navigation
    const specsLink = page.locator('.nav__link', { hasText: 'Specs' });
    await expect(specsLink).toBeVisible();
    await specsLink.click();

    // Wait for scroll and verify section is in view
    await page.waitForTimeout(500); // Allow smooth scroll

    const techspecsSection = page.locator(SELECTORS.TECHSPECS.SECTION);
    await expect(techspecsSection).toBeInViewport();
  });

  test('techspecs has proper semantic structure', async ({ page }) => {
    const techspecsSection = page.locator(SELECTORS.TECHSPECS.SECTION);

    // Verify section has proper heading
    const heading = techspecsSection.locator('h2');
    await expect(heading).toBeVisible();

    // Verify definition list structure (dt/dd pairs)
    const labels = techspecsSection.locator('dt');
    const values = techspecsSection.locator('dd');

    const labelCount = await labels.count();
    const valueCount = await values.count();

    // Each label should have a corresponding value
    expect(labelCount).toBe(valueCount);
    expect(labelCount).toBeGreaterThanOrEqual(4);
  });

  test('block size and LSM levels are displayed', async ({ page }) => {
    const techspecsSection = page.locator(SELECTORS.TECHSPECS.SECTION);
    await expect(techspecsSection).toBeVisible();

    const sectionText = await techspecsSection.textContent();

    // Check block size (4 KB or 4KB)
    expect(sectionText).toMatch(/4\s*KB/i);

    // Check max LSM levels (7)
    expect(sectionText).toContain(TECHSPECS_CONFIG.MAX_LSM_LEVELS);
  });
});
