/**
 * Performance Section E2E Tests
 * Owner: Scenario 6 - Performance Section
 *
 * Tests:
 * - Performance section exists and contains configuration options
 * - Default port documented (0.0.0.0:12333)
 * - SSTable size documented (100MB)
 * - Memtable size documented (4MB)
 * - LSM levels documented (7)
 */

const { test, expect } = require('@playwright/test');
const { loadPage } = require('../utils/test-helpers');

test.describe('Performance Section', () => {
  test.beforeEach(async ({ page }) => {
    await loadPage(page);
  });

  test('TC1: Section contains performance-related information and configuration options', async ({ page }) => {
    // Navigate to performance section
    const performanceSection = page.locator('#performance');
    await expect(performanceSection).toBeVisible();

    // Check section has heading
    const heading = performanceSection.locator('h2');
    await expect(heading).toHaveText('Performance');

    // Check for configuration table or list
    const configTable = performanceSection.locator('.config-table, table');
    await expect(configTable).toBeVisible();

    // Check for performance notes section
    const performanceNotes = performanceSection.locator('.performance-notes, .perf-notes');
    await expect(performanceNotes).toBeVisible();
  });

  test('TC2: Default listen address 0.0.0.0:12333 documented', async ({ page }) => {
    const performanceSection = page.locator('#performance');

    // Check for the default listen address
    const sectionText = await performanceSection.textContent();
    expect(sectionText).toContain('0.0.0.0:12333');
  });

  test('TC3: SSTable max size 100MB documented', async ({ page }) => {
    const performanceSection = page.locator('#performance');

    // Check for SSTable size configuration
    const sectionText = await performanceSection.textContent();
    expect(sectionText).toContain('100MB');

    // Verify it's associated with SSTable
    expect(sectionText.toLowerCase()).toContain('sstable');
  });

  test('TC4: Memtable max size 4MB documented', async ({ page }) => {
    const performanceSection = page.locator('#performance');

    // Check for memtable size configuration
    const sectionText = await performanceSection.textContent();
    expect(sectionText).toContain('4MB');

    // Verify it's associated with memtable
    expect(sectionText.toLowerCase()).toContain('memtable');
  });

  test('TC5: Max LSM levels 7 documented', async ({ page }) => {
    const performanceSection = page.locator('#performance');

    // Check for LSM levels configuration
    const sectionText = await performanceSection.textContent();

    // Should mention 7 levels for LSM tree (can have "LSM" between 7 and levels)
    expect(sectionText).toMatch(/7\s*(LSM\s*)?levels?/i);
    expect(sectionText.toLowerCase()).toContain('lsm');
    expect(sectionText).toContain('max_level');
  });

  test('Performance section is navigable via anchor link', async ({ page }) => {
    // Click on performance link if exists in nav
    const perfLink = page.locator('a[href="#performance"]').first();
    if (await perfLink.isVisible()) {
      await perfLink.click();
    }

    // Verify the section is in view
    const performanceSection = page.locator('#performance');
    await expect(performanceSection).toBeVisible();
  });
});
